//! Reading the archive's TFRecord-framed `TimestampedFrame` segments back from an object store
//! (S3, `file://`, or a bare local path) — the same backends the connector's uploader writes to.
//! Finalized segments are gzipped whole-file; a still-open `.partial` (only ever seen when
//! replaying a live local archive dir — the uploader never ships partials) is an uncompressed
//! record stream, so gzip is sniffed from the content, never inferred from the key.

use std::io::Read;
use std::sync::Arc;

use anyhow::{Context, anyhow, bail};
use flate2::read::GzDecoder;
use futures::StreamExt;
use nordpool_connector::proto::archive::{SegmentHeader, TimestampedFrame};
use nordpool_connector::tfrecord;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use prost::Message;
use tokio::runtime::Runtime;

/// Pick the backend for `input` and return the store plus the base [`ObjectPath`] within it.
/// Accepts `s3://bucket/prefix` (region + credential chain from the environment, exactly like the
/// connector's uploader), `file:///abs/path`, or a bare local path (e.g. `/tmp/archive` or a
/// relative dir, resolved to an absolute filesystem location).
pub fn build_object_store(input: &str) -> anyhow::Result<(Arc<dyn ObjectStore>, ObjectPath)> {
    if let Ok(url) = url::Url::parse(input) {
        match url.scheme() {
            "s3" => {
                let store = AmazonS3Builder::from_env()
                    .with_url(input)
                    .build()
                    .context("building S3 object store")?;
                let prefix = ObjectPath::from(url.path().trim_start_matches('/'));
                return Ok((Arc::new(store), prefix));
            }
            "file" => {
                let prefix = ObjectPath::from(url.path().trim_start_matches('/'));
                return Ok((Arc::new(LocalFileSystem::new()), prefix));
            }
            // Anything else (incl. a bare path, which fails to parse as a URL) falls through to
            // the local-path handling below.
            _ => {}
        }
    }
    let abs = std::path::absolute(input).with_context(|| format!("resolving path {input:?}"))?;
    let prefix = ObjectPath::from_absolute_path(&abs)
        .map_err(|e| anyhow!("path {abs:?} is not a valid object path: {e}"))?;
    Ok((Arc::new(LocalFileSystem::new()), prefix))
}

/// Archive segment keys under `prefix`, sorted by key. The archive names segments
/// `<type>-<YYYYMMDDThhmmss.sssZ>-<seg>.<ext>` under `<type>/<date>/`, so lexical order of
/// the full key is chronological. Returns both finalized `.tfrecord.gz` segments and a
/// still-open `.tfrecord.partial` (only present when replaying a live local archive dir; the
/// uploader never ships partials): the partial carries the latest start timestamp, so it
/// sorts last and replays after the finalized segments. The listing recurses into whatever
/// falls under `prefix` (one day when `--date` is given, all days otherwise).
pub fn list_segments(
    rt: &Runtime,
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> anyhow::Result<Vec<ObjectPath>> {
    let mut paths = rt.block_on(async {
        let mut out = Vec::new();
        let mut listing = store.list(Some(prefix));
        while let Some(meta) = listing.next().await {
            let loc = meta.context("listing object store")?.location;
            if loc
                .filename()
                .is_some_and(|n| n.ends_with(".tfrecord.gz") || n.ends_with(".tfrecord.partial"))
            {
                out.push(loc);
            }
        }
        Ok::<_, anyhow::Error>(out)
    })?;
    paths.sort();
    Ok(paths)
}

/// The trailing segment counter parsed from a segment filename, e.g.
/// `localview-20260624T190535.345Z-000.tfrecord.gz` (or its `.partial`) → `0`. The archive resets this
/// counter to 0 whenever it opens a snapshot-led segment, so index 0 marks a segment that begins
/// with a full snapshot. `None` if the name carries no parseable counter. Relies on the only `-`
/// separators being `<type>-<timestamp>-<seg>` (type labels use `_`, the timestamp is ISO basic).
pub fn segment_index(name: &str) -> Option<u32> {
    let after_last_dash = name.rsplit_once('-')?.1;
    let digits: String = after_last_dash
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    digits.parse().ok()
}

/// The tail of `segments` from the last snapshot-led segment (counter 000): the latest
/// self-contained run, which is all a current-state summary needs to read. Errors when the
/// listing holds no snapshot-led segment at all — the tail would begin mid-stream.
pub fn latest_snapshot_run(mut segments: Vec<ObjectPath>) -> anyhow::Result<Vec<ObjectPath>> {
    let start = segments
        .iter()
        .rposition(|p| segment_index(p.filename().unwrap_or_default()) == Some(0))
        .ok_or_else(|| anyhow!("no snapshot-led segment (counter 000) in the listing"))?;
    segments.drain(..start);
    Ok(segments)
}

/// Decode every `TimestampedFrame` in one segment object, in write order, returning the frames
/// and the number of stored bytes read. Fetches the whole object into memory — segments are
/// bounded by the archive's `block_size`. Gzip is detected by **sniffing the magic bytes**, not
/// the key's extension, so a finalized `.gz` and a raw local `.partial` read through one path
/// with the content as the authority. The sniff is unambiguous — a raw segment starts with the
/// header record's u64 LE length, and `1f 8b` there would mean a ~35KB `SegmentHeader`, which
/// cannot exist. The leading [`SegmentHeader`] record is checked for the supported format
/// version, then skipped. A truncated or CRC-failing tail record (possible on a hard-killed
/// partial, or mid-file corruption — the CRCs make both detectable) stops decoding with a
/// warning rather than failing the run.
pub fn read_segment(
    rt: &Runtime,
    store: &dyn ObjectStore,
    path: &ObjectPath,
) -> anyhow::Result<(Vec<TimestampedFrame>, u64)> {
    let bytes = rt.block_on(async {
        let object = store
            .get(path)
            .await
            .with_context(|| format!("getting {path}"))?;
        object
            .bytes()
            .await
            .with_context(|| format!("reading {path}"))
    })?;
    let read = bytes.len() as u64;
    let raw: Vec<u8> = if bytes.starts_with(&[0x1f, 0x8b]) {
        let mut out = Vec::new();
        GzDecoder::new(bytes.as_ref())
            .read_to_end(&mut out)
            .with_context(|| format!("gunzipping {path}"))?;
        out
    } else {
        bytes.to_vec()
    };

    let mut buf: &[u8] = &raw;
    if buf.is_empty() {
        // A 0-byte partial (hard kill before the first sync) recovers to an empty segment.
        tracing::warn!(path = %path, "empty segment, skipping");
        return Ok((Vec::new(), read));
    }
    // `decode_record` returns `None` only on empty input, which was handled above.
    let Some(header_bytes) = tfrecord::decode_record(&mut buf)
        .with_context(|| format!("reading segment header record of {path}"))?
    else {
        return Ok((Vec::new(), read));
    };
    let header = SegmentHeader::decode(header_bytes)
        .with_context(|| format!("decoding segment header of {path}"))?;
    if header.version != tfrecord::FORMAT_VERSION {
        bail!(
            "segment {path} has format version {} (supported: {})",
            header.version,
            tfrecord::FORMAT_VERSION
        );
    }

    let mut frames = Vec::new();
    loop {
        match tfrecord::decode_record(&mut buf) {
            Ok(Some(payload)) => match TimestampedFrame::decode(payload) {
                Ok(frame) => frames.push(frame),
                Err(e) => {
                    tracing::warn!(error = %e, path = %path, "stopping at undecodable record");
                    break;
                }
            },
            Ok(None) => break,
            Err(e) => {
                tracing::warn!(error = %e, path = %path, "stopping at truncated or corrupt tail record");
                break;
            }
        }
    }
    Ok((frames, read))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    fn runtime() -> Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn temp_dir() -> std::path::PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("nordpool-reader-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    use prost::Message;

    #[test]
    fn segment_index_reads_the_trailing_counter() {
        assert_eq!(
            segment_index("localview-20260624T190535.345Z-000.tfrecord.gz"),
            Some(0)
        );
        assert_eq!(
            segment_index("public_statistic-20260624T190535.345Z-012.tfrecord.partial"),
            Some(12)
        );
        assert_eq!(segment_index("localview.tfrecord.gz"), None);
    }

    #[test]
    fn latest_snapshot_run_keeps_the_tail_from_the_last_counter_zero_segment() {
        // Given a sorted listing spanning two snapshot-led runs, the second continuing into
        // the next day's partition.
        let paths: Vec<ObjectPath> = [
            "delivery_area/2026-01-01/delivery_area-20260101T000000.000Z-000.tfrecord.gz",
            "delivery_area/2026-01-01/delivery_area-20260101T060000.000Z-001.tfrecord.gz",
            "delivery_area/2026-01-01/delivery_area-20260101T120000.000Z-000.tfrecord.gz",
            "delivery_area/2026-01-02/delivery_area-20260102T000000.000Z-001.tfrecord.gz",
        ]
        .map(ObjectPath::from)
        .to_vec();

        // When trimming to the latest snapshot-led run.
        let run = latest_snapshot_run(paths.clone()).unwrap();

        // Then only the last counter-000 segment and its continuations survive.
        assert_eq!(run, paths[2..]);

        // And a listing with no snapshot-led segment at all errors rather than starting
        // mid-stream.
        let err = latest_snapshot_run(paths[1..2].to_vec()).unwrap_err();
        assert!(err.to_string().contains("snapshot-led"), "{err}");
    }

    #[test]
    fn list_segments_includes_open_partial_last() {
        // Given a finalized segment, a non-segment file, and a still-open partial whose start
        // timestamp is later than the finalized one, all under one day's partition directory.
        let dir = temp_dir();
        let day = dir.join("localview/2026-01-01");
        std::fs::create_dir_all(&day).unwrap();
        std::fs::write(
            day.join("localview-20260101T000000.000Z-000.tfrecord.gz"),
            b"",
        )
        .unwrap();
        std::fs::write(
            day.join("localview-20260101T000010.000Z-001.tfrecord.partial"),
            b"",
        )
        .unwrap();
        std::fs::write(day.join("notes.txt"), b"").unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());

        // When listing the segments under the type prefix.
        let paths =
            list_segments(&runtime(), store.as_ref(), &ObjectPath::from("localview")).unwrap();

        // Then the partial is included and sorts after the finalized segment; the unrelated file
        // is excluded.
        assert_eq!(
            paths,
            vec![
                ObjectPath::from(
                    "localview/2026-01-01/localview-20260101T000000.000Z-000.tfrecord.gz"
                ),
                ObjectPath::from(
                    "localview/2026-01-01/localview-20260101T000010.000Z-001.tfrecord.partial"
                ),
            ]
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// The raw record stream of one segment: a version-1 header, then `frames`.
    fn raw_segment(frames: &[&[u8]]) -> Vec<u8> {
        let header = SegmentHeader {
            version: tfrecord::FORMAT_VERSION,
            frame_type: "localview".into(),
            opened_at: None,
            seq: 0,
        };
        let mut buf = tfrecord::encode_record(&header.encode_to_vec());
        for frame in frames {
            let record = TimestampedFrame {
                timestamp: None,
                frame: frame.to_vec(),
            };
            buf.extend_from_slice(&tfrecord::encode_record(&record.encode_to_vec()));
        }
        buf
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut enc, bytes).unwrap();
        enc.finish().unwrap()
    }

    #[test]
    fn read_segment_gunzips_skips_the_header_and_tolerates_a_torn_tail() {
        // Given a finalized .gz segment holding a header and two frames, with its tail record
        // truncated mid-payload — as a preview upload of a partially flushed partial can be.
        let dir = temp_dir();
        let mut raw = raw_segment(&[b"first", b"second"]);
        raw.truncate(raw.len() - 3);
        std::fs::write(dir.join("localview-1-000.tfrecord.gz"), gzip(&raw)).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());

        // When the segment is read back.
        let (frames, read) = read_segment(
            &runtime(),
            store.as_ref(),
            &ObjectPath::from("localview-1-000.tfrecord.gz"),
        )
        .unwrap();

        // Then the header record is validated and skipped (it is not returned as a frame), the
        // intact frame survives, and the CRC-failing torn tail stops decoding without an error.
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].frame, b"first");
        assert!(read > 0);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_segment_sniffs_a_raw_partial_and_parses_it_uncompressed() {
        // Given a raw (uncompressed) still-open partial, as found when replaying a live local
        // archive dir — compression only happens at finalize.
        let dir = temp_dir();
        std::fs::write(
            dir.join("localview-1-000.tfrecord.partial"),
            raw_segment(&[b"open"]),
        )
        .unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());

        // When the segment is read back.
        let (frames, _) = read_segment(
            &runtime(),
            store.as_ref(),
            &ObjectPath::from("localview-1-000.tfrecord.partial"),
        )
        .unwrap();

        // Then the reader detects the missing gzip magic and parses the records raw — the
        // content, not the key name, decides the decoding path.
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].frame, b"open");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_segment_rejects_an_unsupported_format_version() {
        // Given a segment whose header claims a future format version.
        let dir = temp_dir();
        let header = SegmentHeader {
            version: tfrecord::FORMAT_VERSION + 1,
            frame_type: "localview".into(),
            opened_at: None,
            seq: 0,
        };
        let raw = tfrecord::encode_record(&header.encode_to_vec());
        std::fs::write(dir.join("localview-1-000.tfrecord.gz"), gzip(&raw)).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());

        // When the segment is read back, then the version check fails loudly rather than
        // mis-decoding records written by a newer format.
        let err = read_segment(
            &runtime(),
            store.as_ref(),
            &ObjectPath::from("localview-1-000.tfrecord.gz"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("format version"), "{err}");

        std::fs::remove_dir_all(&dir).ok();
    }
}
