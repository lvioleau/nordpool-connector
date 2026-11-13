//! Raw-frame disk archive. Wraps each frame's verbatim wire `ServerFrame` bytes — together
//! with the wall-clock instant it was received — in a [`TimestampedFrame`] and writes those
//! to local disk as TFRecord-framed records (per-record masked CRC32C, see
//! [`tfrecord`](crate::tfrecord)), each segment led by a [`SegmentHeader`] record that makes
//! the file self-describing. The inner `frame` bytes are never re-encoded (the field is
//! wire-compatible with `ServerFrame`, so a reader may decode it as one), preserving fields
//! our compiled proto doesn't know about. One rotating segment per frame type. Rotation is
//! decided when the next frame arrives: the arriving frame first sweeps **every** type's open
//! segment and finalizes any open longer than `segment_max_age` (the busy feeds close the
//! quiet feeds' aged segments; the age close opens nothing in its place — the type's next
//! frame reopens lazily, a delta continuing the seq chain at count+1, a snapshot starting
//! fresh at 0, so silence never produces empty segments), then its own segment is finalized
//! if it has reached `block_size`, or a snapshot starts a fresh file. Finalizing gzips the
//! raw `.partial` into its final `.tfrecord.gz` (via a `.tmp` the uploader ignores), so
//! readers/uploaders only ever see complete compressed blocks; the open `.partial` itself
//! stays uncompressed so records can be appended and fsync'd incrementally and a torn tail
//! stays recoverable.
//!
//! A snapshot always starts a fresh segment so a file boundary lines up with a self-contained
//! book state; a type's first frame must therefore be a snapshot (a delta with no open segment
//! has no preceding block to anchor it and is rejected). Segments are named
//! `<label>-<YYYYMMDDThhmmss.sssZ>-<seg>.tfrecord.gz`, with the ISO 8601 basic UTC time the
//! segment opened and a per-snapshot segment counter that starts at 0 on the snapshot segment
//! and increments on each `block_size` rotation until the next snapshot resets it. The open
//! time is nudged to at least 1ms past the previous segment's, so per type the names are
//! unique and lexical order is strictly chronological.
//!
//! Built to run behind a [`QueuedSink`](super::QueuedSink): the disk I/O is blocking, so it
//! lives on the queue's worker thread, single-threaded with no internal locking. Rotation
//! (age sweep included) and the fsync cadence are event-time, keyed off the receive timestamp
//! threaded through [`handle`](ArchiveSink::handle).

mod writer;

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufWriter};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use anyhow::Context;
use chrono::{DateTime, Utc};
use flate2::Compression;
use flate2::write::GzEncoder;

use crate::config::ArchiveConfig;
use crate::driver::shutdown_requested;
use crate::proto::archive::{SegmentHeader, TimestampedFrame};
use crate::proto::pmd::server_frame::Payload as ServerPayload;
use crate::tfrecord;

use self::writer::SegmentWriter;
use super::FrameSink;

pub struct ArchiveSink {
    dir: PathBuf,
    block_size: usize,
    segment_max_age: Duration,
    fsync_interval: Duration,
    /// The currently open segment per frame type — an entry exists iff that type has an open
    /// `.partial`. Opened lazily on the type's first (snapshot) frame.
    segments: HashMap<&'static str, Segment>,
    /// The last segment opened per type (seq + open time), kept even after that segment
    /// closes: the anchor for the filename-timestamp nudge and for continuing the seq chain
    /// when a delta arrives after an age-based close left no segment open.
    last_opened: HashMap<&'static str, (u64, SystemTime)>,
    /// Free-space floor below which the staging disk is reported as low; 0 never reports.
    min_free_space: u64,
    /// Event time of the last free-space check, so a `statvfs` runs once per `fsync_interval`
    /// rather than once per frame. `UNIX_EPOCH` makes the first frame check.
    last_space_check: SystemTime,
    /// Whether the last check found the disk low, so the warning is logged on the way in (and
    /// the recovery on the way out) instead of on every check while it stays there.
    low_space: bool,
    /// How long to keep retrying a refusing disk: the budget spent on one frame, and the wait
    /// before probing again after giving one up.
    write_retry: Duration,
    /// Event time the next frame may attempt the disk again, set once a frame has been given up;
    /// `None` means healthy. Without it every frame would spend the whole retry budget against a
    /// disk already known to be down, and each attempt that opens a segment before failing to
    /// write it leaves an orphaned `.partial`. Doubles as the "currently failing" latch.
    write_retry_at: Option<SystemTime>,
}

/// Granularity of the retry wait: the budget is slept in slices this size so a shutdown lands
/// promptly instead of waiting out the last (longest) doubling.
const RETRY_STEP: Duration = Duration::from_millis(200);

/// One open `.partial` segment file being appended to.
struct Segment {
    writer: SegmentWriter,
    /// The `.partial` path; finalized by stripping that extension on rotation.
    partial_path: PathBuf,
    /// The (possibly nudged, see [`unique_open_time`]) instant stamped into the segment's
    /// filename — the floor for the next segment's open time.
    opened_at: SystemTime,
    bytes_written: usize,
    last_sync: SystemTime,
    /// Per-snapshot segment counter: 0 on the snapshot segment, +1 on each `block_size`
    /// rotation until the next snapshot resets it.
    seq: u64,
}

impl ArchiveSink {
    /// Create the base directory up front (fail fast), recover any `.partial` segments a
    /// previous crash left behind, and start with no open segments; per-type
    /// subdirectories and segments are opened lazily on first frame.
    pub fn new(cfg: &ArchiveConfig) -> anyhow::Result<Self> {
        fs::create_dir_all(&cfg.dir)
            .with_context(|| format!("creating archive dir {:?}", cfg.dir))?;
        recover_partials(&cfg.dir);
        Ok(Self {
            dir: cfg.dir.clone(),
            block_size: cfg.block_size,
            segment_max_age: cfg.segment_max_age,
            fsync_interval: cfg.fsync_interval,
            segments: HashMap::new(),
            last_opened: HashMap::new(),
            min_free_space: cfg.min_free_space,
            last_space_check: SystemTime::UNIX_EPOCH,
            low_space: false,
            write_retry: cfg.write_retry,
            write_retry_at: None,
        })
    }

    /// One frame's trip to disk: everything that can fail on the way to a durable record.
    ///
    /// Split out so [`handle`](Self::handle) can decide what a failure means. An archive fault
    /// must not reach the driver: the gRPC feeds never touch this disk, and ending the session
    /// over an IO error would turn an archive outage into a client outage without saving a
    /// single frame. Reconnecting would not help either — the socket is fine, the disk is not.
    fn archive(
        &mut self,
        label: &'static str,
        payload: &ServerPayload,
        bytes: &[u8],
        received_at: SystemTime,
    ) -> anyhow::Result<()> {
        // Every frame sweeps every type's aged segments first — including its own, so an
        // over-age segment is closed before segment_for decides where this frame lands.
        self.close_aged(received_at)?;
        self.check_free_space(received_at);
        let mut segment = self.segment_for(label, payload, received_at)?;
        let written = write_frame(&mut segment, bytes, received_at)?;

        metrics::counter!("nordpool.archive.frames_written", "type" => label).increment(1);
        metrics::counter!("nordpool.archive.bytes_written", "type" => label)
            .increment(written as u64);

        self.sync_if_due(&mut segment, payload, received_at)?;
        self.segments.insert(label, segment);
        Ok(())
    }

    /// Write one frame, waiting out a disk that is refusing before giving that frame up.
    ///
    /// The waiting belongs here, on the queue's worker thread, because here it is nearly free:
    /// frames arriving meanwhile collect in the `QueuedSink`'s bounded queue — deep enough to
    /// hold well over a minute of feed — and are written as soon as the disk comes back, so a
    /// fault shorter than `write_retry` costs nothing at all. Returning immediately instead
    /// would throw away every frame in the window even when the disk recovered a millisecond
    /// later. The driver is never the one waiting: past `enqueue_timeout` its own enqueue drops
    /// and latches, which is the queue doing its job.
    ///
    /// Attempts are spaced by doubling rather than evenly, because an attempt that opens a
    /// segment and then fails to write it leaves an orphaned `.partial` behind — the spacing is
    /// what stops a long fault from littering the staging dir with them.
    fn write_with_retry(
        &mut self,
        label: &'static str,
        payload: &ServerPayload,
        bytes: &[u8],
        received_at: SystemTime,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + self.write_retry;
        let mut wait = RETRY_STEP;
        loop {
            let error = match self.archive(label, payload, bytes, received_at) {
                Ok(()) => {
                    self.note_write_recovered();
                    return Ok(());
                }
                // A full disk, or a failure that is not a disk fault at all: waiting cannot
                // help either, so neither is retried.
                Err(e) if !is_survivable(&e) => return Err(e),
                Err(e) => e,
            };
            // The last wait is clamped to what remains of the budget rather than skipped, so
            // the final attempt lands at the deadline itself: the frame is only lost to a fault
            // that outlasted the whole budget, not to where the doubling happened to fall.
            let step = wait.min(deadline.saturating_duration_since(Instant::now()));
            if step.is_zero() || !sleep_unless_stopping(step) {
                self.give_up_on(&error, label, received_at);
                return Ok(());
            }
            wait *= 2;
        }
    }

    /// Whether this frame falls inside the wait that follows a frame already given up. Attempting
    /// would spend the whole budget again against a disk known to be down, and leave another
    /// orphaned `.partial` for every attempt that got as far as opening one.
    fn write_deferred(&self, received_at: SystemTime) -> bool {
        self.write_retry_at
            .is_some_and(|retry_at| received_at < retry_at)
    }

    /// Give up on one frame: count the loss, say so once, and hold the disk off for one
    /// `write_retry` before probing it again.
    fn give_up_on(&mut self, error: &anyhow::Error, label: &'static str, received_at: SystemTime) {
        metrics::counter!("nordpool.archive.write_failures", "type" => label).increment(1);
        self.drop_frame(label);
        // Logged on the way in only: at feed rate a stuck disk would otherwise write the error
        // log faster than it failed to write the archive.
        if self.write_retry_at.is_none() {
            tracing::error!(
                error = format!("{error:#}"),
                label,
                retry_secs = self.write_retry.as_secs(),
                "archive write failed after retrying; frames are being lost while the feeds keep serving"
            );
        }
        self.write_retry_at = Some(received_at + self.write_retry);
    }

    /// Count a frame that never reached disk — the only remaining trace that it existed.
    fn drop_frame(&self, label: &'static str) {
        metrics::counter!("nordpool.archive.frames_dropped", "type" => label).increment(1);
    }

    /// Clear the failure latch once a frame lands again, and say the archive is back.
    fn note_write_recovered(&mut self) {
        if self.write_retry_at.take().is_some() {
            tracing::info!("archive writing again");
        }
    }

    /// Publish how much room the staging disk has left, and say so when it is running out.
    ///
    /// Nothing bounds the staging dir. If uploads stop shipping, the backlog grows until a write
    /// fails for want of space — and that, alone among archive failures, is still fatal to the
    /// process, taking the live feeds with it.
    ///
    /// Crossing the floor is therefore **only** a warning. It does not stop, throttle, or delete
    /// anything: deleting undelivered data to save the process would be worse (the object store
    /// may never have received it), and stopping early would spend the one thing the warning is
    /// for, which is the room still left to act in. The process ends at ENOSPC or not at all, so
    /// nothing is gained by pre-empting it here.
    ///
    /// Throttled to one `statvfs` per `fsync_interval` of event time — the syscall is cheap but not
    /// worth doing at the frame rate — and logged only on a transition, so a disk sitting low says
    /// so once rather than on every check.
    fn check_free_space(&mut self, now: SystemTime) {
        if now
            .duration_since(self.last_space_check)
            .is_ok_and(|since| since < self.fsync_interval)
        {
            return;
        }
        self.last_space_check = now;
        let Some(free) = free_space(&self.dir) else {
            return;
        };
        metrics::gauge!("nordpool.archive.disk_free_bytes").set(free as f64);

        let low = free < self.min_free_space;
        if low && !self.low_space {
            tracing::warn!(
                dir = %self.dir.display(),
                free_mib = free / (1024 * 1024),
                min_free_mib = self.min_free_space / (1024 * 1024),
                "archive staging disk is running low — this is lead time, not an intervention: writing continues until the volume is genuinely full, and only that ends the process. Drain the upload backlog or grow the volume"
            );
        } else if !low && self.low_space {
            tracing::info!(
                dir = %self.dir.display(),
                free_mib = free / (1024 * 1024),
                "archive staging disk has recovered"
            );
        }
        self.low_space = low;
    }

    /// Return the segment this frame should append to, rotating first when the previous block
    /// has ended. A snapshot always opens a fresh segment at count 0, finalizing any open one;
    /// otherwise a delta keeps appending to the open segment, rolling it over to count+1 once
    /// it reached `block_size` on a prior write — or, when an age-based close left no segment
    /// open, resuming the chain at the closed segment's count+1. The first frame for a type
    /// must be a snapshot — a delta with no open segment *and no prior chain* has no preceding
    /// block state and is an error.
    fn segment_for(
        &mut self,
        label: &'static str,
        payload: &ServerPayload,
        received_at: SystemTime,
    ) -> anyhow::Result<Segment> {
        let existing = self.segments.remove(label);
        let open_time = match self.last_opened.get(label) {
            Some((_, opened_at)) => unique_open_time(received_at, *opened_at),
            None => received_at,
        };

        // snapshot
        if is_snapshot(payload) {
            if let Some(segment) = existing {
                finalize_segment(label, "snapshot", segment)?;
            }
            return self.open_segment(label, 0, open_time);
        }

        // delta
        let Some(segment) = existing else {
            // No open segment: continue the chain an age-based close ended, or reject a
            // delta that has no snapshot-led chain at all.
            return match self.last_opened.get(label) {
                Some((seq, _)) => self.open_segment(label, seq + 1, open_time),
                None => Err(anyhow::anyhow!(
                    "archive received a {label} delta before any snapshot"
                )),
            };
        };

        if segment.bytes_written >= self.block_size {
            let next_seq = segment.seq + 1;
            finalize_segment(label, "size", segment)?;
            return self.open_segment(label, next_seq, open_time);
        }

        Ok(segment)
    }

    /// Open a fresh segment via the free [`open_segment`] and record it as the type's latest
    /// open — the anchor that survives the segment's close for seq continuation and the
    /// filename-timestamp nudge.
    fn open_segment(
        &mut self,
        label: &'static str,
        seq: u64,
        open_time: SystemTime,
    ) -> anyhow::Result<Segment> {
        let segment = open_segment(&self.dir, label, seq, open_time)?;
        self.last_opened.insert(label, (seq, open_time));
        Ok(segment)
    }

    /// Flush+fsync the open segment on event-time cadence to bound the hard-kill loss window
    /// while a segment fills slowly. A snapshot is always synced: it opens a fresh segment and
    /// resets `last_sync`, so the cadence can never fire for it, yet on a sparse stream it may
    /// be the only frame for a long time — leaving it buffered means a 0-byte partial and total
    /// loss on a hard kill until the next frame arrives.
    fn sync_if_due(
        &self,
        segment: &mut Segment,
        payload: &ServerPayload,
        received_at: SystemTime,
    ) -> anyhow::Result<()> {
        if is_snapshot(payload)
            || received_at
                .duration_since(segment.last_sync)
                .unwrap_or_default()
                >= self.fsync_interval
        {
            sync_segment(segment, received_at)?;
        }
        Ok(())
    }

    /// Finalize every open segment — any type's, not just the arriving frame's — older than
    /// `segment_max_age`, so no frame sits in a not-yet-shipped segment longer than that: the
    /// loss bound that lets the uploader ship only complete segments. Swept at the top of
    /// every [`handle`](ArchiveSink::handle) with the frame's receive time (event time, like
    /// all rotation), so the busy feeds close the quiet feeds' aged segments; only total
    /// silence across all subscriptions defers the close (to the next frame or shutdown), and
    /// then nothing new is accumulating anyway. Deliberately opens no replacement — the next
    /// frame for the type does that lazily (delta ⇒ count+1 continuation, snapshot ⇒ fresh
    /// count 0), so a quiet feed produces no empty segments.
    fn close_aged(&mut self, now: SystemTime) -> anyhow::Result<()> {
        let expired: Vec<&'static str> = self
            .segments
            .iter()
            .filter(|(_, segment)| {
                now.duration_since(segment.opened_at).unwrap_or_default() >= self.segment_max_age
            })
            .map(|(label, _)| *label)
            .collect();
        for label in expired {
            if let Some(segment) = self.segments.remove(label) {
                finalize_segment(label, "age", segment)?;
            }
        }
        Ok(())
    }
}

impl FrameSink for ArchiveSink {
    fn handle(
        &mut self,
        payload: &ServerPayload,
        bytes: &[u8],
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        // Below this line the archive works in `SystemTime`: its segment names, rotation ages and
        // fsync intervals are wall-clock bookkeeping about files, not market-data timestamps.
        let received_at = SystemTime::from(received_at);
        let Some(label) = type_dir_name(payload) else {
            // Heartbeat/Connected/Error are handled by the driver and never reach sinks;
            // ignore defensively if one ever does.
            return Ok(());
        };

        if self.write_deferred(received_at) {
            self.drop_frame(label);
            return Ok(());
        }
        self.write_with_retry(label, payload, bytes, received_at)
    }
}

/// Sleep `total` in [`RETRY_STEP`] slices, returning `false` the moment a shutdown is requested.
/// The worker must not hold the process open for the rest of a wait that has already stopped
/// being worth anything.
fn sleep_unless_stopping(total: Duration) -> bool {
    let mut slept = Duration::ZERO;
    while slept < total {
        if shutdown_requested() {
            return false;
        }
        let step = RETRY_STEP.min(total - slept);
        std::thread::sleep(step);
        slept += step;
    }
    !shutdown_requested()
}

/// Whether an archive failure is one to keep serving through: a disk fault, and not the disk
/// being full. Matched on the OS code rather than a message, anywhere in the cause chain, since
/// every disk call here wraps its `io::Error` in context.
fn is_survivable(error: &anyhow::Error) -> bool {
    let mut disk_fault = false;
    for cause in error.chain().filter_map(|c| c.downcast_ref::<io::Error>()) {
        if cause.raw_os_error() == Some(libc::ENOSPC) {
            return false;
        }
        disk_fault = true;
    }
    disk_fault
}

impl Drop for ArchiveSink {
    fn drop(&mut self) {
        // Finalize every open `.partial` so a clean shutdown leaves no dangling partial.
        for (label, segment) in self.segments.drain() {
            if let Err(e) = finalize_segment(label, "shutdown", segment) {
                tracing::error!(
                    error = format!("{e:#}"),
                    label,
                    "finalizing archive segment on drop failed"
                );
            }
        }
    }
}

/// Filesystem-safe subdirectory name per archived frame type. A dedicated mapping (rather
/// than reusing `driver::describe`) keeps the names lowercase and static. `None` for
/// non-market variants, which never reach a sink.
fn type_dir_name(payload: &ServerPayload) -> Option<&'static str> {
    match payload {
        ServerPayload::LocalView(_) => Some("localview"),
        ServerPayload::DeliveryArea(_) => Some("delivery_area"),
        ServerPayload::Contract(_) => Some("contract"),
        ServerPayload::Ticker(_) => Some("ticker"),
        ServerPayload::AtcCapacity(_) => Some("atc_capacity"),
        ServerPayload::H2hCapacity(_) => Some("h2h_capacity"),
        ServerPayload::PublicStatistic(_) => Some("public_statistic"),
        _ => None,
    }
}

/// Whether `payload` is a full snapshot (vs an incremental delta). A snapshot rotates to a
/// fresh archive segment. Mirrors the variants [`type_dir_name`] archives; non-market
/// variants never reach a sink, so they are treated as non-snapshots.
fn is_snapshot(payload: &ServerPayload) -> bool {
    match payload {
        ServerPayload::LocalView(p) => p.is_snapshot,
        ServerPayload::DeliveryArea(p) => p.is_snapshot,
        ServerPayload::Contract(p) => p.is_snapshot,
        ServerPayload::Ticker(p) => p.is_snapshot,
        ServerPayload::AtcCapacity(p) => p.is_snapshot,
        ServerPayload::H2hCapacity(p) => p.is_snapshot,
        ServerPayload::PublicStatistic(p) => p.is_snapshot,
        _ => false,
    }
}

/// The timestamp for a new segment's filename: the frame's receive time, nudged forward to at
/// least 1ms past the previous segment's open time. The name's timestamp has millisecond
/// precision, so without the nudge two snapshot-led segments opened within one millisecond
/// (e.g. per-area LocalView snapshots arriving back-to-back on resubscribe) would both be named
/// `<label>-<ts>-000` — and the second's finalize rename would silently overwrite the first.
/// Strictly increasing open times per type also keep lexical filename order chronological even
/// when a rotation and a snapshot share a millisecond.
fn unique_open_time(received_at: SystemTime, prev_open: SystemTime) -> SystemTime {
    received_at.max(prev_open + Duration::from_millis(1))
}

/// Open a fresh `<dir>/<label>/<date>/<label>-<YYYYMMDDThhmmss.sssZ>-<seg>.tfrecord.partial`
/// segment, stamped with its open time (the opening frame's UTC receive time, uniquified via
/// [`unique_open_time`]), creating the per-type/per-day directory on first use, and write its
/// leading [`SegmentHeader`] record. The `<date>` partition (the segment's open day) is the
/// layout the uploader mirrors verbatim into the object store and `depth-replay` lists under —
/// it must stay in step with [`segment_prefix`](crate::upload::segment_prefix)
/// (`<type>/<date>`).
fn open_segment(
    dir: &Path,
    label: &str,
    seq: u64,
    open_time: SystemTime,
) -> anyhow::Result<Segment> {
    let day_dir = dir.join(label).join(utc_date(open_time));
    fs::create_dir_all(&day_dir)
        .with_context(|| format!("creating archive segment dir {day_dir:?}"))?;
    let name = format!(
        "{label}-{}-{seq:03}.tfrecord.partial",
        utc_timestamp(open_time)
    );
    let partial_path = day_dir.join(name);
    let file = File::create(&partial_path)
        .with_context(|| format!("creating archive segment {partial_path:?}"))?;
    let mut writer = SegmentWriter::new(file);
    let header = SegmentHeader {
        version: tfrecord::FORMAT_VERSION,
        frame_type: label.to_string(),
        opened_at: Some(open_time.into()),
        seq,
    };
    let header_bytes = writer
        .write_record(&header)
        .with_context(|| format!("writing segment header {partial_path:?}"))?;
    Ok(Segment {
        writer,
        partial_path,
        opened_at: open_time,
        bytes_written: header_bytes,
        last_sync: open_time,
        seq,
    })
}

/// Checkpoint the open segment's durability without rotating it, then record the sync time.
fn sync_segment(segment: &mut Segment, received_at: SystemTime) -> anyhow::Result<()> {
    segment.writer.sync()?;
    segment.last_sync = received_at;
    Ok(())
}

/// Finalize (compress) any `.partial` segments a previous crash/kill left behind, so their
/// data lands as a complete block instead of being stranded. Best-effort: a stale orphan must
/// not block startup, so failures are logged, not propagated. The tail record may be truncated
/// mid-record; the reader's per-record CRCs detect that and it stops there. Walks the archive
/// tree recursively, so it finds partials at any depth (`<type>/<date>/`).
fn recover_partials(dir: &Path) {
    let mut recovered = 0u64;
    recover_partials_in(dir, &mut recovered);
    if recovered > 0 {
        metrics::counter!("nordpool.archive.recovered_segments").increment(recovered);
        tracing::info!(recovered, "recovered orphaned archive segments");
    }
}

fn recover_partials_in(dir: &Path, recovered: &mut u64) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            recover_partials_in(&path, recovered);
        } else if path.extension().is_some_and(|x| x == "partial") {
            match compress_segment(&path) {
                Ok(_) => *recovered += 1,
                Err(e) => {
                    tracing::warn!(
                        error = format!("{e:#}"),
                        path = ?path,
                        "recovering archive segment failed"
                    )
                }
            }
        }
    }
}

/// Append one length-delimited `TimestampedFrame` record: the verbatim frame bytes paired
/// with their receive time, prefixed by a varint length. Returns the number of bytes written
/// (prefix + message).
fn write_frame(
    segment: &mut Segment,
    bytes: &[u8],
    received_at: SystemTime,
) -> anyhow::Result<usize> {
    let record = TimestampedFrame {
        timestamp: Some(received_at.into()),
        frame: bytes.to_vec(),
    };
    let written = segment.writer.write_record(&record)?;
    segment.bytes_written += written;
    Ok(written)
}

/// Flush and `fsync` the raw segment, gzip it into its final `.tfrecord.gz`, and count it —
/// with its raw and compressed sizes — under its frame type and rotation reason
/// (`snapshot` / `size` / `shutdown`). Only complete compressed blocks are ever visible under
/// the final name.
fn finalize_segment(
    label: &'static str,
    reason: &'static str,
    segment: Segment,
) -> anyhow::Result<()> {
    let Segment {
        writer,
        partial_path,
        bytes_written,
        seq,
        ..
    } = segment;

    writer.finalize()?;
    let final_path = compress_segment(&partial_path)?;
    let compressed_bytes = fs::metadata(&final_path).map(|m| m.len()).unwrap_or(0);

    metrics::counter!("nordpool.archive.segments_finalized", "type" => label, "reason" => reason)
        .increment(1);
    metrics::histogram!("nordpool.archive.segment_raw_bytes", "type" => label, "reason" => reason)
        .record(bytes_written as f64);
    metrics::histogram!("nordpool.archive.segment_compressed_bytes", "type" => label, "reason" => reason)
        .record(compressed_bytes as f64);

    tracing::info!(
        frame_type = label,
        reason,
        seq,
        bytes = bytes_written,
        compressed_bytes,
        path = %final_path.display(),
        "finalized archive segment"
    );

    Ok(())
}

/// Gzip the raw `.partial` into its final `.tfrecord.gz` sibling, then delete the partial.
/// Goes through a fsynced `.tmp` (an extension the uploader ignores) so a crash mid-compress
/// never exposes a half-written final segment. Idempotent across every crash point: re-running
/// on the surviving partial recreates the `.tmp`, overwrites the final, and re-deletes — which
/// is exactly what startup recovery does.
fn compress_segment(partial_path: &Path) -> anyhow::Result<PathBuf> {
    let final_path = partial_path.with_extension("gz");
    let tmp_path = partial_path.with_extension("tmp");

    let src = File::open(partial_path)
        .with_context(|| format!("opening archive segment {partial_path:?}"))?;
    let dst = File::create(&tmp_path)
        .with_context(|| format!("creating compressed segment {tmp_path:?}"))?;
    let mut encoder = GzEncoder::new(BufWriter::new(dst), Compression::default());
    io::copy(&mut io::BufReader::new(src), &mut encoder)
        .with_context(|| format!("compressing archive segment {partial_path:?}"))?;
    let dst = encoder
        .finish()
        .context("finishing gzip stream")?
        .into_inner()
        .map_err(|e| anyhow::anyhow!("flushing compressed segment {tmp_path:?}: {e}"))?;
    dst.sync_all()
        .with_context(|| format!("fsync compressed segment {tmp_path:?}"))?;

    fs::rename(&tmp_path, &final_path)
        .with_context(|| format!("finalizing archive segment {final_path:?}"))?;
    fs::remove_file(partial_path)
        .with_context(|| format!("removing archive partial {partial_path:?}"))?;
    Ok(final_path)
}

/// Format `time` as the ISO 8601 basic UTC form `YYYYMMDDThhmmss.sssZ` for the segment
/// filename — colon-free (so portable as a filename) and lexically chronological (UTC has no
/// DST fold to repeat an hour).
fn utc_timestamp(time: SystemTime) -> String {
    let utc: DateTime<Utc> = time.into();
    utc.format("%Y%m%dT%H%M%S%.3fZ").to_string()
}

/// Bytes still writable by this process on the filesystem holding `dir`, or `None` if the query
/// failed (a path that has gone, a filesystem that won't answer) — nothing the archive can act on,
/// so the caller simply reports nothing this round.
///
/// `statvfs` because std has no free-space API. `f_bavail` rather than `f_bfree`: the difference is
/// the root-only reserve, which this process never gets to write into, so `f_bfree` would promise
/// room the writer cannot use.
fn free_space(dir: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;

    let path = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
    // SAFETY: `path` outlives the call and is NUL-terminated by CString; `statvfs` only writes into
    // `stats`, which is a correctly sized zeroed struct of its own output type. A non-zero return
    // means it wrote nothing worth reading, so nothing reads `stats` on that path.
    unsafe {
        let mut stats: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(path.as_ptr(), &mut stats) != 0 {
            return None;
        }
        Some(widen(stats.f_bavail).saturating_mul(widen(stats.f_frsize)))
    }
}

/// Widen one of `statvfs`'s counts, which are `u32` on some targets and already `u64` on others —
/// going through a generic keeps both spellings correct without a platform-dependent cast.
fn widen(count: impl Into<u64>) -> u64 {
    count.into()
}

/// The open time [`utc_timestamp`] wrote into `file_name`, or `None` if it holds no such stamp.
/// The inverse of the naming above, and deliberately next to it: a segment's name is the only
/// record of when its data starts once the file is closed, and the uploader reads it to say how
/// much history is still only on the staging disk (and to ship the oldest of it first).
///
/// A `None` means the file did not come from [`open_segment`] — nothing else in the archive dir is
/// named this way — so callers treat it as carrying no history rather than as an error.
pub(crate) fn segment_open_time(file_name: &str) -> Option<SystemTime> {
    // `<label>-<YYYYMMDDThhmmss.sssZ>-<seq>.tfrecord.…`. Read from the right: a label may hold a
    // `-` (none does today), the stamp never does, so it is the field just past the sequence.
    let mut fields = file_name.rsplit('-');
    fields.next()?;
    let stamp = fields.next()?;
    let naive = chrono::NaiveDateTime::parse_from_str(stamp, "%Y%m%dT%H%M%S%.3fZ").ok()?;
    Some(naive.and_utc().into())
}

/// The UTC calendar day `YYYY-MM-DD` of `time` — the per-day partition directory a segment opens
/// under. Matches the `date` level of [`segment_prefix`](crate::upload::segment_prefix) and the
/// `--date` argument `depth-replay` takes.
fn utc_date(time: SystemTime) -> String {
    let utc: DateTime<Utc> = time.into();
    utc.format("%Y-%m-%d").to_string()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io;
    use std::io::Read;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, SystemTime};

    use chrono::{DateTime, TimeDelta, Utc};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use prost::Message;

    use super::ArchiveSink;
    use super::FrameSink;
    use super::{RETRY_STEP, free_space, is_survivable, segment_open_time};
    use crate::config::ArchiveConfig;
    use crate::proto::archive::{SegmentHeader, TimestampedFrame};
    use crate::proto::pmd::server_frame::Payload as ServerPayload;
    use crate::proto::pmd::{ContractPayload, LocalViewPayload};
    use crate::tfrecord;

    /// Fixed receive time, so the serialized timestamp is deterministic to assert against.
    fn received_at() -> DateTime<Utc> {
        at("2023-11-14T22:13:20.123Z")
    }

    fn temp_dir(tag: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("nordpool-archive-{tag}-{}-{n}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// `block_size` no test frame comes near, so a segment rotates only when the test drives it —
    /// a snapshot, the age sweep, or shutdown.
    const BLOCK_SIZE_NO_ROTATION: usize = 1_000_000;
    /// `block_size` every frame passes, so each frame finalizes the segment it lands in.
    const BLOCK_SIZE_ROTATE_EVERY_FRAME: usize = 1;
    /// `min_free_space` (bytes, as the config resolves `min_free_space_mib`) that no filesystem
    /// can clear, so the check always reports the staging disk low.
    const MIN_FREE_SPACE_ALWAYS_LOW: u64 = u64::MAX;
    /// `min_free_space` of one MiB — a temp dir always has that much, so the check never reports
    /// low. Far below the shipped 2 GiB, which a CI disk could plausibly be under.
    const MIN_FREE_SPACE_NEVER_LOW: u64 = 1024 * 1024;

    fn cfg(dir: PathBuf, block_size: usize) -> ArchiveConfig {
        cfg_with_space_floor(dir, block_size, 0)
    }

    /// `cfg` with a free-space floor, for the disk-space checks. A floor of 0 never reports low,
    /// which is what keeps every other test blind to the disk the suite happens to run on.
    fn cfg_with_space_floor(dir: PathBuf, block_size: usize, min_free_space: u64) -> ArchiveConfig {
        ArchiveConfig {
            dir,
            block_size,
            // Long enough that age-based rotation never fires unless a test drives it.
            segment_max_age: Duration::from_secs(900),
            fsync_interval: Duration::from_secs(3600),
            min_free_space,
            // One RETRY_STEP: long enough to gate a following frame in event time, short enough
            // that a failing write costs a test one clamped 200ms sleep before giving up.
            write_retry: RETRY_STEP,
            queue_capacity: 8,
            enqueue_timeout: Duration::from_millis(250),
            poll_interval: Duration::from_millis(10),
            upload: None,
        }
    }

    /// A frame receive time, written the way the segment names spell it — so a test's input and
    /// the file name it expects line up by eye.
    fn at(rfc3339: &str) -> DateTime<Utc> {
        rfc3339.parse().expect("a valid RFC 3339 instant")
    }

    /// A segment's open time as [`segment_open_time`] reads it back out of a file name: the
    /// `SystemTime` the archive's own file bookkeeping speaks, not a frame's receive time.
    fn open_time(rfc3339: &str) -> SystemTime {
        at(rfc3339).into()
    }

    fn local_view() -> ServerPayload {
        ServerPayload::LocalView(LocalViewPayload::default())
    }

    fn local_view_snapshot() -> ServerPayload {
        ServerPayload::LocalView(LocalViewPayload {
            is_snapshot: true,
            ..Default::default()
        })
    }

    /// The `-<seg>` suffix of a finalized segment's name (extensions stripped).
    fn seq_suffix(path: &Path) -> String {
        let name = path.file_name().unwrap().to_str().unwrap();
        let stem = name.strip_suffix(".tfrecord.gz").unwrap();
        stem.rsplit_once('-').unwrap().1.to_string()
    }

    /// All files with extension `ext` under `dir`, found recursively (the archive nests segments
    /// under `<type>/<date>/`), sorted by path.
    fn files_with_ext(dir: &Path, ext: &str) -> Vec<PathBuf> {
        fn walk(dir: &Path, ext: &str, out: &mut Vec<PathBuf>) {
            for entry in fs::read_dir(dir).into_iter().flatten().flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, ext, out);
                } else if path.extension().is_some_and(|x| x == ext) {
                    out.push(path);
                }
            }
        }
        let mut files = Vec::new();
        walk(dir, ext, &mut files);
        files.sort();
        files
    }

    /// Finalized (non-`.partial`) `.tfrecord.gz` segments under a type directory, sorted.
    fn finalized(dir: &Path, label: &str) -> Vec<PathBuf> {
        files_with_ext(&dir.join(label), "gz")
    }

    /// Gunzip a finalized segment file back to its raw record stream.
    fn gunzip(path: &Path) -> Vec<u8> {
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(fs::File::open(path).unwrap())
            .read_to_end(&mut out)
            .unwrap();
        out
    }

    /// Parse a segment's raw (uncompressed) record stream: the leading header, then every frame.
    fn parse_records(mut buf: &[u8]) -> (SegmentHeader, Vec<TimestampedFrame>) {
        let header_bytes = tfrecord::decode_record(&mut buf)
            .unwrap()
            .expect("missing segment header record");
        let header = SegmentHeader::decode(header_bytes).unwrap();
        let mut frames = Vec::new();
        while let Some(payload) = tfrecord::decode_record(&mut buf).unwrap() {
            frames.push(TimestampedFrame::decode(payload).unwrap());
        }
        (header, frames)
    }

    #[test]
    fn finalizes_on_block_size_into_a_headed_gzipped_tfrecord_segment() {
        // Given a block size of 1 byte, so any single frame already pushes the segment past the
        // rotation threshold — the next frame's arrival will finalize it.
        let dir = temp_dir("size");
        let mut sink = ArchiveSink::new(&cfg(dir.clone(), BLOCK_SIZE_ROTATE_EVERY_FRAME)).unwrap();

        // When a snapshot frame opens and fills the segment, then a delta arrives — rotation is
        // decided on the next frame, so the delta is what rolls the now-over-size segment over.
        sink.handle(&local_view_snapshot(), b"hello", received_at())
            .unwrap();
        sink.handle(&local_view(), b"world", received_at()).unwrap();

        // Then the first, over-size segment is finalized as a gzipped TFRecord stream: a
        // self-describing SegmentHeader record, then the TimestampedFrame with the verbatim wire
        // bytes (unchanged) paired with the exact receive time.
        let files = finalized(&dir, "localview");
        assert_eq!(files.len(), 1);
        let (header, frames) = parse_records(&gunzip(&files[0]));
        assert_eq!(header.version, tfrecord::FORMAT_VERSION);
        assert_eq!(header.frame_type, "localview");
        assert_eq!(header.seq, 0);
        let opened_at = header.opened_at.unwrap();
        assert_eq!(opened_at.seconds, 1_700_000_000);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].frame, b"hello");
        let ts = frames[0].timestamp.unwrap();
        assert_eq!(ts.seconds, 1_700_000_000);
        assert_eq!(ts.nanos, 123_000_000);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn emits_write_and_finalize_counters() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let dir = temp_dir("metrics");
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        // When a single frame is archived and the sink is then dropped (which finalizes it).
        metrics::with_local_recorder(&recorder, || {
            let mut sink =
                ArchiveSink::new(&cfg(dir.clone(), BLOCK_SIZE_ROTATE_EVERY_FRAME)).unwrap();
            sink.handle(&local_view_snapshot(), b"hello", received_at())
                .unwrap();
        });

        // Then the per-frame counters (one frame written, byte count = the full TFRecord-framed
        // record size, header excluded) and the segment counter (one segment finalized, by drop)
        // are all recorded, so the archive's observability covers both the write path and rotation.
        let snapshot = snapshotter.snapshot().into_vec();
        let counter = |name: &str| {
            let (_, _, _, value) = snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("counter {name} not recorded"));
            match value {
                DebugValue::Counter(v) => *v,
                other => panic!("expected counter, got {other:?}"),
            }
        };
        assert_eq!(counter("nordpool.archive.frames_written"), 1);
        // The whole TFRecord-framed TimestampedFrame record (framing + envelope).
        let record = TimestampedFrame {
            timestamp: Some(SystemTime::from(received_at()).into()),
            frame: b"hello".to_vec(),
        };
        let expected = tfrecord::encode_record(&record.encode_to_vec()).len() as u64;
        assert_eq!(counter("nordpool.archive.bytes_written"), expected);
        assert_eq!(counter("nordpool.archive.segments_finalized"), 1);

        // And the finalize histograms carry the segment's sizes: raw = the whole raw file
        // (header record included, unlike the per-frame counter), compressed = a non-empty gzip.
        let histogram = |name: &str| {
            let (_, _, _, value) = snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("histogram {name} not recorded"));
            match value {
                DebugValue::Histogram(values) => {
                    assert_eq!(values.len(), 1, "{name}: one segment finalized");
                    values[0].into_inner()
                }
                other => panic!("expected histogram, got {other:?}"),
            }
        };
        let header = SegmentHeader {
            version: tfrecord::FORMAT_VERSION,
            frame_type: "localview".to_string(),
            opened_at: Some(SystemTime::from(received_at()).into()),
            seq: 0,
        };
        let header_len = tfrecord::encode_record(&header.encode_to_vec()).len() as u64;
        assert_eq!(
            histogram("nordpool.archive.segment_raw_bytes"),
            (header_len + expected) as f64
        );
        assert!(histogram("nordpool.archive.segment_compressed_bytes") > 0.0);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn segment_name_carries_utc_time_and_zero_count() {
        // Given a block size of 1 so the single frame's segment finalizes the moment it closes
        // (here on drop), giving us a named final file to inspect.
        let dir = temp_dir("name-utc");
        let mut sink = ArchiveSink::new(&cfg(dir.clone(), BLOCK_SIZE_ROTATE_EVERY_FRAME)).unwrap();

        // When a snapshot is archived — its receive time is the instant that stamps the segment
        // filename — and the sink is dropped to finalize it.
        sink.handle(&local_view_snapshot(), b"x", at("2023-11-14T22:13:20.123Z"))
            .unwrap();
        drop(sink);

        // Then the file name encodes that receive time in ISO 8601 basic UTC (colon-free, lexically
        // sortable) and segment count 000, the count a fresh snapshot segment always opens at.
        let files = finalized(&dir, "localview");
        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0].file_name().unwrap().to_str().unwrap(),
            "localview-20231114T221320.123Z-000.tfrecord.gz"
        );

        // And that stamp reads back out of the name: once the file is closed its name is the only
        // record of when its data starts, which is what the uploader reports as undelivered history
        // and orders its uploads by.
        assert_eq!(
            segment_open_time(files[0].file_name().unwrap().to_str().unwrap()),
            Some(open_time("2023-11-14T22:13:20.123Z"))
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn free_space_answers_for_a_real_dir_and_declines_for_a_missing_one() {
        let dir = temp_dir("free-space");
        assert!(
            free_space(&dir).is_some_and(|free| free > 0),
            "a writable temp dir should report room"
        );
        // A path that isn't there can't be measured, and that is not an archive error: the caller
        // reports nothing this round rather than treating it as a full disk.
        assert_eq!(free_space(&dir.join("nope/nor/this")), None);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_low_staging_disk_is_published_and_flagged() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        // Given a floor no real filesystem can clear, so the check always finds the disk low —
        // standing in for a staging volume the upload backlog has nearly filled.
        let dir = temp_dir("disk-low");
        let mut sink = ArchiveSink::new(&cfg_with_space_floor(
            dir.clone(),
            BLOCK_SIZE_NO_ROTATION,
            MIN_FREE_SPACE_ALWAYS_LOW,
        ))
        .unwrap();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        // When a frame is archived — the check rides the frame path, like the age sweep.
        metrics::with_local_recorder(&recorder, || {
            sink.handle(&local_view_snapshot(), b"x", at("2023-11-14T22:13:20Z"))
                .unwrap();
        });

        // Then the free space is published, so a monitor can alert on the trend rather than on our
        // threshold, and the sink remembers it is low so the warning is not repeated per frame.
        let (_, _, _, value) = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .find(|(key, ..)| key.key().name() == "nordpool.archive.disk_free_bytes")
            .expect("free space published");
        let DebugValue::Gauge(free) = value else {
            panic!("expected a gauge, got {value:?}")
        };
        assert!(free.into_inner() > 0.0, "{free:?}");
        assert!(sink.low_space, "a disk under the floor should be flagged");

        // And the archive kept writing regardless: a low disk is a warning, not a failure — the
        // frames still have to land somewhere, and dropping them would be the worse outcome.
        assert_eq!(finalized(&dir, "localview").len(), 0);
        drop(sink);
        assert_eq!(finalized(&dir, "localview").len(), 1);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_healthy_staging_disk_is_not_flagged() {
        // Given a floor a normal temp dir clears easily.
        let dir = temp_dir("disk-ok");
        let mut sink = ArchiveSink::new(&cfg_with_space_floor(
            dir.clone(),
            BLOCK_SIZE_NO_ROTATION,
            MIN_FREE_SPACE_NEVER_LOW,
        ))
        .unwrap();

        // When a frame is archived.
        sink.handle(&local_view_snapshot(), b"x", at("2023-11-14T22:13:20Z"))
            .unwrap();

        // Then nothing is flagged, so the warning stays quiet on a healthy disk.
        assert!(!sink.low_space);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn segment_open_time_ignores_names_the_writer_did_not_make() {
        // A partial is named the same way, so its stamp reads back too — the uploader skips those
        // for other reasons.
        assert_eq!(
            segment_open_time("localview-20231114T221320.123Z-000.tfrecord.partial"),
            Some(open_time("2023-11-14T22:13:20.123Z"))
        );
        // Anything else carries no open time rather than a wrong one: too few fields, a stamp that
        // is not one, or a name with no stamp at all.
        for name in [
            "localview-000.tfrecord.gz",
            "localview-not-a-stamp-000.tfrecord.gz",
            "notes.txt",
            "",
        ] {
            assert_eq!(segment_open_time(name), None, "{name}");
        }
    }

    #[test]
    fn segment_is_written_under_a_per_day_directory() {
        // Given a sink whose single frame arrives on 2023-11-14 UTC.
        let dir = temp_dir("day-dir");
        let mut sink = ArchiveSink::new(&cfg(dir.clone(), BLOCK_SIZE_NO_ROTATION)).unwrap();

        // When a snapshot is archived and the sink dropped to finalize it.
        sink.handle(&local_view_snapshot(), b"x", at("2023-11-14T22:13:20.123Z"))
            .unwrap();
        drop(sink);

        // Then the segment lands under `<dir>/<type>/<date>/` — the per-day partition the uploader
        // mirrors verbatim into the object store, derived from the segment's open day.
        let files = finalized(&dir, "localview");
        assert_eq!(files.len(), 1);
        let parent = files[0].parent().unwrap();
        assert_eq!(parent, dir.join("localview").join("2023-11-14"));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_snapshot_starts_a_fresh_segment_at_count_zero() {
        // Given a block size large enough that size-based rotation never triggers, so a snapshot
        // and a following delta accumulate in one still-open segment (nothing finalized yet).
        let dir = temp_dir("snapshot-rotate");
        let mut sink = ArchiveSink::new(&cfg(dir.clone(), BLOCK_SIZE_NO_ROTATION)).unwrap();
        sink.handle(&local_view_snapshot(), b"s1", at("2023-11-14T22:13:20Z"))
            .unwrap();
        sink.handle(&local_view(), b"d1", at("2023-11-14T22:13:21Z"))
            .unwrap();
        assert_eq!(finalized(&dir, "localview").len(), 0);

        // When a second snapshot arrives — a snapshot always forces a fresh segment so a file
        // boundary lines up with a self-contained book state, regardless of size.
        sink.handle(&local_view_snapshot(), b"s2", at("2023-11-14T22:13:22Z"))
            .unwrap();

        // Then the first snapshot's segment is finalized despite being under the block size, and
        // the new snapshot opens its own segment, so each archive file starts from a full snapshot.
        let files = finalized(&dir, "localview");
        assert_eq!(files.len(), 1);
        drop(sink);
        let files = finalized(&dir, "localview");
        assert_eq!(files.len(), 2);
        // Both segments are count 0 (each snapshot resets the counter), kept distinct and ordered
        // by their wall-clock timestamps.
        assert_eq!(
            files.iter().map(|p| seq_suffix(p)).collect::<Vec<_>>(),
            ["000", "000"]
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn snapshots_in_the_same_millisecond_get_distinct_segment_names() {
        // Given two snapshot frames received within the same millisecond — e.g. per-area
        // LocalView snapshots arriving back-to-back on resubscribe. Both open a seq-000 segment,
        // so with millisecond filename precision they would otherwise collide and the second's
        // finalize rename would overwrite the first.
        let dir = temp_dir("same-ms");
        let mut sink = ArchiveSink::new(&cfg(dir.clone(), BLOCK_SIZE_NO_ROTATION)).unwrap();

        // When both are archived with an identical receive time and the sink is dropped so the
        // second segment finalizes too.
        sink.handle(
            &local_view_snapshot(),
            b"s1",
            at("2023-11-14T22:13:20.123Z"),
        )
        .unwrap();
        sink.handle(
            &local_view_snapshot(),
            b"s2",
            at("2023-11-14T22:13:20.123Z"),
        )
        .unwrap();
        drop(sink);

        // Then both segments survive: the second's open time is nudged 1ms past the first's, so
        // the names stay unique and lexically ordered in arrival order.
        let files = finalized(&dir, "localview");
        let names: Vec<_> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap())
            .collect();
        assert_eq!(
            names,
            [
                "localview-20231114T221320.123Z-000.tfrecord.gz",
                "localview-20231114T221320.124Z-000.tfrecord.gz"
            ]
        );

        fs::remove_dir_all(&dir).ok();
    }

    /// The single open `.partial` segment under a type directory.
    fn open_partial(dir: &Path, label: &str) -> PathBuf {
        let mut files = files_with_ext(&dir.join(label), "partial");
        assert_eq!(files.len(), 1, "expected exactly one open partial");
        files.pop().unwrap()
    }

    #[test]
    fn a_snapshot_is_flushed_to_disk_without_waiting_for_finalize() {
        // Given a block size large enough that nothing rotates and an fsync interval long
        // enough that the event-time cadence never fires during the test.
        let dir = temp_dir("snapshot-sync");
        let mut sink = ArchiveSink::new(&cfg(dir.clone(), BLOCK_SIZE_NO_ROTATION)).unwrap();

        // When a snapshot is archived (the segment stays open — no finalize, no drop).
        sink.handle(&local_view_snapshot(), b"snap", received_at())
            .unwrap();

        // Then its bytes are already durable in the open partial (an uncompressed record
        // stream: header then frame), not stranded in the buffer.
        let partial = open_partial(&dir, "localview");
        let snap_bytes = fs::read(&partial).unwrap();
        let (header, frames) = parse_records(&snap_bytes);
        assert_eq!(header.frame_type, "localview");
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].frame, b"snap");

        // Whereas a following delta stays buffered (the cadence is not yet due), so the open
        // partial is unchanged on disk until the next sync or finalize.
        sink.handle(&local_view(), b"delta", received_at()).unwrap();
        assert_eq!(
            fs::read(&partial).unwrap(),
            snap_bytes,
            "the delta should not yet be on disk"
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn segment_count_increments_between_snapshots_then_resets() {
        // Given a block size of 1 so every frame fills its segment and the next frame rotates it,
        // making the per-snapshot segment counter advance once per frame — easy to read off.
        let dir = temp_dir("snapshot-count");
        let mut sink = ArchiveSink::new(&cfg(dir.clone(), BLOCK_SIZE_ROTATE_EVERY_FRAME)).unwrap();

        // When a snapshot, two deltas, then another snapshot are archived — deltas keep appending
        // (incrementing the counter), while the second snapshot starts a new self-contained file.
        sink.handle(&local_view_snapshot(), b"a", at("2023-11-14T22:13:20Z"))
            .unwrap();
        sink.handle(&local_view(), b"b", at("2023-11-14T22:13:21Z"))
            .unwrap();
        sink.handle(&local_view(), b"c", at("2023-11-14T22:13:22Z"))
            .unwrap();
        sink.handle(&local_view_snapshot(), b"d", at("2023-11-14T22:13:23Z"))
            .unwrap();
        // The last snapshot's segment stays open until shutdown closes it.
        drop(sink);

        // Then the counter climbs 000→001→002 across the snapshot and its deltas, then resets to
        // 000 on the next snapshot — so a segment's sequence number locates it within its snapshot.
        let files = finalized(&dir, "localview");
        assert_eq!(
            files.iter().map(|p| seq_suffix(p)).collect::<Vec<_>>(),
            ["000", "001", "002", "000"]
        );

        fs::remove_dir_all(&dir).ok();
    }

    fn contract_snapshot() -> ServerPayload {
        ServerPayload::Contract(ContractPayload {
            is_snapshot: true,
            ..Default::default()
        })
    }

    #[test]
    fn any_frame_closes_other_types_aged_segments_and_opens_no_replacement() {
        // Given an open localview segment whose snapshot arrived at t0, with the default
        // 15-minute segment_max_age from cfg().
        let dir = temp_dir("age");
        let mut sink = ArchiveSink::new(&cfg(dir.clone(), BLOCK_SIZE_NO_ROTATION)).unwrap();
        sink.handle(&local_view_snapshot(), b"s", at("2023-11-14T22:13:20Z"))
            .unwrap();

        // When frames of a DIFFERENT type arrive — one just before the localview segment's
        // age bound, one just past it. The age sweep runs on every frame, whatever its type.
        sink.handle(
            &contract_snapshot(),
            b"c1",
            at("2023-11-14T22:13:20Z") + TimeDelta::seconds(899),
        )
        .unwrap();
        assert_eq!(finalized(&dir, "localview").len(), 0, "not yet aged");
        sink.handle(
            &contract_snapshot(),
            b"c2",
            at("2023-11-14T22:13:20Z") + TimeDelta::seconds(900),
        )
        .unwrap();

        // Then the quiet type's aged segment is finalized (reason: age) by the busy type's
        // traffic, and nothing new is opened in its place — silence on a type must not create
        // files, so no localview .partial exists until localview data arrives again.
        assert_eq!(finalized(&dir, "localview").len(), 1);
        assert!(files_with_ext(&dir.join("localview"), "partial").is_empty());

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_delta_after_an_age_close_continues_the_seq_chain() {
        // Given a snapshot-led localview segment (count 000) opened at t0, and a
        // segment_max_age of 15 minutes.
        let dir = temp_dir("age-resume");
        let mut sink = ArchiveSink::new(&cfg(dir.clone(), BLOCK_SIZE_NO_ROTATION)).unwrap();
        sink.handle(&local_view_snapshot(), b"s", at("2023-11-14T22:13:20Z"))
            .unwrap();

        // When the next localview delta arrives past the age bound: its own sweep closes the
        // aged segment first, then the delta needs somewhere to land — previously an error
        // ("delta before any snapshot"), but here the chain exists, just not an open file.
        sink.handle(
            &local_view(),
            b"d",
            at("2023-11-14T22:13:20Z") + TimeDelta::seconds(901),
        )
        .unwrap();
        drop(sink);

        // Then the delta opened a continuation segment at count 001: replay still sees one
        // snapshot-led chain (000 then 001), split across the age boundary.
        let files = finalized(&dir, "localview");
        assert_eq!(
            files.iter().map(|p| seq_suffix(p)).collect::<Vec<_>>(),
            ["000", "001"]
        );
        // And the continuation holds exactly the delta — its own header, then the frame.
        let (header, frames) = parse_records(&gunzip(&files[1]));
        assert_eq!(header.seq, 1);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].frame, b"d");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn new_recovers_orphaned_partial_segments_from_a_previous_crash() {
        // Given a stale .partial segment left behind in a type directory, simulating a segment a
        // hard kill (SIGKILL) abandoned before it could finalize — its data is otherwise stranded.
        let dir = temp_dir("recover");
        let type_dir = dir.join("localview");
        fs::create_dir_all(&type_dir).unwrap();
        let orphan = type_dir.join("localview-123-000.tfrecord.partial");
        fs::write(&orphan, b"leftover").unwrap();

        // When a new sink starts up, running its startup recovery pass over the archive dir.
        let _sink = ArchiveSink::new(&cfg(dir.clone(), BLOCK_SIZE_NO_ROTATION)).unwrap();

        // Then recovery finalizes the orphan by compressing it to its final .tfrecord.gz (no
        // .partial remains), and its bytes round-trip through the gzip untouched — the leftover
        // lands as a complete block, not lost. Recovery never parses the records, so even a
        // torn tail survives to be handled by the reader's CRCs.
        assert!(!orphan.exists());
        let files = finalized(&dir, "localview");
        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0].file_name().unwrap().to_str().unwrap(),
            "localview-123-000.tfrecord.gz"
        );
        assert_eq!(gunzip(&files[0]), b"leftover");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn writes_a_separate_segment_per_type_and_drop_finalizes_partials() {
        // Given a LocalView snapshot and a Contract snapshot — two different frame types — with a
        // block size neither reaches, so each leaves an open segment in its own type directory.
        let dir = temp_dir("types");
        let mut sink = ArchiveSink::new(&cfg(dir.clone(), BLOCK_SIZE_NO_ROTATION)).unwrap();
        sink.handle(&local_view_snapshot(), b"a", received_at())
            .unwrap();
        sink.handle(
            &ServerPayload::Contract(ContractPayload {
                is_snapshot: true,
                ..Default::default()
            }),
            b"bb",
            received_at(),
        )
        .unwrap();

        // When the sink is dropped (clean shutdown), which finalizes every open partial.
        drop(sink);

        // Then each frame type has its own finalized segment under its own directory, confirming
        // the archive keeps one independent rotating segment per type rather than interleaving them.
        assert_eq!(finalized(&dir, "localview").len(), 1);
        assert_eq!(finalized(&dir, "contract").len(), 1);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_delta_before_any_snapshot_is_an_error() {
        // Given a fresh sink with no open segment for the type — it has seen no snapshot, so there
        // is no block to anchor an incremental update to.
        let dir = temp_dir("delta-first");
        let mut sink = ArchiveSink::new(&cfg(dir.clone(), BLOCK_SIZE_NO_ROTATION)).unwrap();

        // When a delta (non-snapshot) arrives first — a protocol violation for the archive, which
        // requires a type's first frame to be a self-contained snapshot.
        let result = sink.handle(&local_view(), b"d", received_at());

        // Then it is rejected as an error — unlike a disk fault this one still ends the process,
        // because a segment stream with no snapshot to anchor it would not replay — and no
        // segment was opened, so a stray delta can't start a headless archive.
        assert!(result.is_err());
        assert_eq!(finalized(&dir, "localview").len(), 0);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn only_a_disk_fault_short_of_a_full_disk_is_survivable() {
        // A full disk ends the process: nothing here frees space, so every later frame fails the
        // same way and the free-space warning has already had its chance to be acted on.
        let full =
            anyhow::Error::from(io::Error::from_raw_os_error(libc::ENOSPC)).context("writing");
        assert!(!is_survivable(&full));

        // Any other disk fault is ridden out — the feeds never touch this disk.
        let denied = anyhow::Error::from(io::Error::from(io::ErrorKind::PermissionDenied))
            .context("opening segment");
        assert!(is_survivable(&denied));

        // And a failure that is not a disk fault at all is not something to keep serving through.
        assert!(!is_survivable(&anyhow::anyhow!(
            "delta before any snapshot"
        )));
    }

    #[test]
    fn a_fault_that_clears_within_the_budget_costs_no_frames() {
        // Given an archive blocked by a regular file where its type directory belongs, and a
        // budget long enough to outlast the fault.
        let dir = temp_dir("write-retry");
        let mut archive_cfg = cfg(dir.clone(), BLOCK_SIZE_NO_ROTATION);
        archive_cfg.write_retry = Duration::from_secs(5);
        let mut sink = ArchiveSink::new(&archive_cfg).unwrap();
        let blocker = dir.join("localview");
        fs::write(&blocker, b"x").unwrap();

        // And something clearing that fault shortly after the first attempt fails, the way a
        // transient disk fault clears on its own.
        let clearing = std::thread::spawn({
            let blocker = blocker.clone();
            move || {
                std::thread::sleep(Duration::from_millis(50));
                fs::remove_file(&blocker).unwrap();
            }
        });

        // When the frame arrives while the disk is still refusing.
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let result = metrics::with_local_recorder(&recorder, || {
            sink.handle(&local_view_snapshot(), b"f", at("1970-01-01T00:00:01Z"))
        });
        clearing.join().unwrap();

        // Then the frame is written rather than lost: the wait happens on the queue's worker
        // thread, where frames arriving meanwhile collect in the queue instead of being thrown
        // away, so a fault shorter than the budget costs nothing at all.
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(files_with_ext(&dir.join("localview"), "partial").len(), 1);
        assert!(
            sink.write_retry_at.is_none(),
            "should not be in a failed state"
        );

        // And nothing is reported lost, because nothing was.
        let lost = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .find(|(key, ..)| {
                matches!(
                    key.key().name(),
                    "nordpool.archive.frames_dropped" | "nordpool.archive.write_failures"
                )
            });
        assert!(lost.is_none(), "nothing should be reported lost: {lost:?}");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_disk_fault_loses_frames_rather_than_the_process() {
        // Given an archive that cannot create its type directory — a regular file sits where
        // `localview/` needs to go. Stands in for any disk fault short of a full disk, and
        // unlike a permissions test it fails the same way for root.
        let dir = temp_dir("write-fault");
        let mut sink = ArchiveSink::new(&cfg(dir.clone(), BLOCK_SIZE_NO_ROTATION)).unwrap();
        fs::write(dir.join("localview"), b"x").unwrap();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        // When a frame arrives and the write fails.
        let result = metrics::with_local_recorder(&recorder, || {
            sink.handle(&local_view_snapshot(), b"f", at("1970-01-01T00:00:01Z"))
        });

        // Then the sink reports success anyway. An archive fault reaching the driver would end
        // the session and then the process, taking gRPC feeds that never touch this disk with
        // it — and no frame is saved by doing so. A reconnect would not help either: the socket
        // is fine, the disk is not.
        assert!(result.is_ok(), "{result:?}");

        // And the loss is counted, since it is now the only thing that says a frame existed.
        let (_, _, _, failures) = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .find(|(key, ..)| key.key().name() == "nordpool.archive.write_failures")
            .expect("a lost frame must be counted");
        assert_eq!(failures, DebugValue::Counter(1));

        // And the next frame inside the retry window does not touch the disk at all: a failed
        // frame drops its open segment, so retrying at feed rate would leave one fresh `.partial`
        // behind for every frame a stuck disk refuses.
        assert!(
            sink.handle(&local_view_snapshot(), b"f", at("1970-01-01T00:00:01.001Z"))
                .is_ok()
        );
        assert!(sink.write_retry_at.is_some(), "should be holding off");

        // And once the fault clears and the pause has elapsed, archiving resumes on its own —
        // nothing has to restart the process to get the archive back.
        fs::remove_file(dir.join("localview")).unwrap();
        sink.handle(&local_view_snapshot(), b"f", at("1970-01-01T01:00:01.001Z"))
            .unwrap();
        assert!(sink.write_retry_at.is_none(), "should have recovered");
        assert_eq!(files_with_ext(&dir.join("localview"), "partial").len(), 1);

        fs::remove_dir_all(&dir).ok();
    }
}
