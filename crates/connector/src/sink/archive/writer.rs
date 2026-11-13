//! Buffered, TFRecord-framed record writer over one open segment file. Knows how to append a
//! record, checkpoint durability (`sync`), and persist the file's bytes (`finalize`) — but not
//! the segment's path or its finalize-time compression and rename, which stay with the
//! archive sink.

use std::fs::File;
use std::io::{BufWriter, Write};

use anyhow::Context;
use prost::Message;

use crate::tfrecord;

/// A `BufWriter<File>` for one segment, speaking TFRecord-framed protobuf records — a
/// `SegmentHeader` first, then `TimestampedFrame`s.
pub(super) struct SegmentWriter {
    inner: BufWriter<File>,
}

impl SegmentWriter {
    pub(super) fn new(file: File) -> Self {
        Self {
            inner: BufWriter::new(file),
        }
    }

    /// Append one TFRecord-framed record (length + CRC + message + CRC). Returns the number of
    /// bytes written (framing + message).
    pub(super) fn write_record(&mut self, record: &impl Message) -> anyhow::Result<usize> {
        let framed = tfrecord::encode_record(&record.encode_to_vec());
        self.inner.write_all(&framed).context("writing record")?;
        Ok(framed.len())
    }

    /// Flush buffered bytes and `sync_data` without closing — a durability checkpoint that
    /// bounds the hard-kill loss window while the segment fills. `sync_data` (not `sync_all`)
    /// suffices: an append changes only the data and the file size, both of which it persists.
    pub(super) fn sync(&mut self) -> anyhow::Result<()> {
        self.inner.flush().context("flushing archive segment")?;
        self.inner
            .get_ref()
            .sync_data()
            .context("fsync archive segment")?;
        Ok(())
    }

    /// Flush, `sync_all`, and consume the writer so the caller can rename the file. `sync_all`
    /// persists metadata too, since the segment is now complete.
    pub(super) fn finalize(self) -> anyhow::Result<()> {
        let file = self
            .inner
            .into_inner()
            .map_err(|e| anyhow::anyhow!("flushing archive segment: {e}"))?;
        file.sync_all().context("fsync archive segment")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::proto::archive::TimestampedFrame;

    fn temp_file(tag: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "nordpool-segwriter-{tag}-{}-{n}.binpb",
            std::process::id()
        ))
    }

    fn record(frame: &str) -> TimestampedFrame {
        TimestampedFrame {
            timestamp: None,
            frame: frame.as_bytes().to_vec(),
        }
    }

    #[test]
    fn write_record_returns_byte_count_and_writes_a_decodable_record() {
        let path = temp_file("write");
        let mut writer = SegmentWriter::new(File::create(&path).unwrap());

        // When one TFRecord-framed record is appended and the writer finalized (flush + sync,
        // closing the segment so everything buffered reaches disk).
        let written = writer.write_record(&record("hello")).unwrap();
        writer.finalize().unwrap();

        // Then the byte count write_record reported equals the segment's on-disk size (the caller
        // relies on this to track bytes_written for rotation), and those bytes decode back to the
        // original frame round-trip through the TFRecord framing.
        let data = fs::read(&path).unwrap();
        assert_eq!(written, data.len());
        let mut buf = data.as_slice();
        let payload = tfrecord::decode_record(&mut buf).unwrap().unwrap();
        let decoded = TimestampedFrame::decode(payload).unwrap();
        assert_eq!(decoded.frame, b"hello");
        assert!(buf.is_empty());

        fs::remove_file(&path).ok();
    }

    #[test]
    fn sync_flushes_buffered_bytes_and_keeps_the_writer_usable() {
        let path = temp_file("sync");
        let mut writer = SegmentWriter::new(File::create(&path).unwrap());

        // When a record is written and then synced — a durability checkpoint (flush + sync_data)
        // that bounds the hard-kill loss window without closing the segment.
        let first = writer.write_record(&record("a")).unwrap();
        writer.sync().unwrap();

        // Then the record is already durable on disk even though the writer is still open,
        assert_eq!(fs::read(&path).unwrap().len(), first);

        // and sync left the writer usable, so a further record appends on top of the synced one
        // rather than the checkpoint having consumed or reset the writer.
        let second = writer.write_record(&record("bb")).unwrap();
        writer.finalize().unwrap();
        assert_eq!(fs::read(&path).unwrap().len(), first + second);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn finalize_flushes_buffered_bytes_to_disk() {
        let path = temp_file("finalize");
        let mut writer = SegmentWriter::new(File::create(&path).unwrap());

        // Given a record handed to write_record but still sitting in the BufWriter — without an
        // explicit sync or finalize it has not reached the file yet (the file is still empty).
        let written = writer.write_record(&record("hello")).unwrap();
        assert_eq!(fs::read(&path).unwrap().len(), 0);

        // When the writer is finalized — the segment-complete path (flush + sync_all + consume).
        writer.finalize().unwrap();

        // Then finalize drained the buffer so the whole record is on disk, confirming no data is
        // lost between the last write and the rename that finalize precedes.
        assert_eq!(fs::read(&path).unwrap().len(), written);

        fs::remove_file(&path).ok();
    }
}
