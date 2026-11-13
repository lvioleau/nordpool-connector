//! TFRecord framing for archive segments — the single authority for the record format, shared
//! by the archive writer and `depth-replay`'s reader. Each record is
//!
//! ```text
//! uint64  length                        (little-endian)
//! uint32  masked_crc32c(length bytes)   (little-endian)
//! bytes   payload[length]
//! uint32  masked_crc32c(payload)        (little-endian)
//! ```
//!
//! byte-compatible with TensorFlow's TFRecord, so standard tooling (`tf.data.TFRecordDataset`
//! with `compression_type="GZIP"`, Beam/Spark TFRecord connectors) can iterate and
//! integrity-check segments without knowing the payload schema. The CRCs make corruption
//! detectable rather than silently mis-decoded: a flipped bit in a length word fails the
//! length CRC instead of mis-framing everything after it.

use anyhow::{bail, ensure};

/// Version stamped into every segment's leading `SegmentHeader` record.
/// 1 = TFRecord framing, whole-file gzip on finalize.
pub const FORMAT_VERSION: u32 = 1;

/// Framing overhead per record: length prefix + its CRC + the payload CRC.
pub const RECORD_OVERHEAD: usize = 8 + 4 + 4;

/// TFRecord masks CRCs so that CRCs of CRC-bearing data stay well distributed.
const CRC_MASK_DELTA: u32 = 0xa282_ead8;

fn masked_crc32c(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
        .rotate_right(15)
        .wrapping_add(CRC_MASK_DELTA)
}

/// One TFRecord-framed record holding `payload`, ready to append to a segment.
pub fn encode_record(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + RECORD_OVERHEAD);
    let len = (payload.len() as u64).to_le_bytes();
    out.extend_from_slice(&len);
    out.extend_from_slice(&masked_crc32c(&len).to_le_bytes());
    out.extend_from_slice(payload);
    out.extend_from_slice(&masked_crc32c(payload).to_le_bytes());
    out
}

/// Decode the next record from the front of `buf`, advancing it past the record. Returns
/// `Ok(None)` on clean end-of-input (empty `buf`); errors on a truncated record or a CRC
/// mismatch — a truncated tail is expected on a partial recovered from a hard kill, so
/// readers treat an error here as "stop", not "fail".
pub fn decode_record<'a>(buf: &mut &'a [u8]) -> anyhow::Result<Option<&'a [u8]>> {
    if buf.is_empty() {
        return Ok(None);
    }
    ensure!(buf.len() >= 12, "truncated record header");
    let (len_bytes, rest) = buf.split_at(8);
    let (len_crc, rest) = rest.split_at(4);
    if u32::from_le_bytes(len_crc.try_into().unwrap()) != masked_crc32c(len_bytes) {
        bail!("record length CRC mismatch");
    }
    let len = u64::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
    ensure!(rest.len() >= len + 4, "truncated record payload");
    let (payload, rest) = rest.split_at(len);
    let (payload_crc, rest) = rest.split_at(4);
    if u32::from_le_bytes(payload_crc.try_into().unwrap()) != masked_crc32c(payload) {
        bail!("record payload CRC mismatch");
    }
    *buf = rest;
    Ok(Some(payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip_in_order() {
        // Given two records appended back to back, as the segment writer lays them out.
        let mut data = encode_record(b"hello");
        data.extend_from_slice(&encode_record(b""));

        // When decoding from the front of the stream.
        let mut buf = data.as_slice();

        // Then both payloads come back in write order (an empty payload is a valid record)
        // and the stream ends cleanly.
        assert_eq!(decode_record(&mut buf).unwrap(), Some(&b"hello"[..]));
        assert_eq!(decode_record(&mut buf).unwrap(), Some(&b""[..]));
        assert_eq!(decode_record(&mut buf).unwrap(), None);
    }

    #[test]
    fn known_answer_vector_pins_tfrecord_compatibility() {
        // Computed by an independent CRC32C + TFRecord-masking implementation (not this
        // module), so a mask, polynomial, or endianness bug can't hide behind a symmetric
        // encode/decode round trip. If this vector ever fails, the files are no longer
        // TFRecord-compatible — fix the code, never the vector.
        let expected = [
            0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // length 3, little-endian
            0xb0, 0x99, 0x49, 0x0e, // masked crc32c(length)
            0x61, 0x62, 0x63, // "abc"
            0x6e, 0x57, 0xf1, 0x21, // masked crc32c(payload)
        ];
        assert_eq!(encode_record(b"abc"), expected);
    }

    #[test]
    fn encoded_record_has_tfrecord_layout() {
        // Then the record is length + length-CRC + payload + payload-CRC, with the length
        // little-endian — the exact TFRecord layout external tooling expects.
        let record = encode_record(b"abc");
        assert_eq!(record.len(), 3 + RECORD_OVERHEAD);
        assert_eq!(u64::from_le_bytes(record[..8].try_into().unwrap()), 3);
        assert_eq!(&record[12..15], b"abc");
    }

    #[test]
    fn a_flipped_payload_bit_fails_the_crc() {
        // Given a record with one corrupted payload byte — without CRCs this would decode as
        // a plausible-looking record.
        let mut record = encode_record(b"hello");
        record[13] ^= 0x01;

        // Then decoding detects the corruption instead of returning the payload.
        let mut buf = record.as_slice();
        let err = decode_record(&mut buf).unwrap_err();
        assert!(err.to_string().contains("payload CRC"));
    }

    #[test]
    fn a_flipped_length_bit_fails_the_length_crc() {
        // Given a record whose length prefix is corrupted — the failure mode that mis-frames
        // every following record in unchecksummed framing.
        let mut record = encode_record(b"hello");
        record[0] ^= 0x01;

        // Then the length CRC catches it before the bogus length is trusted.
        let mut buf = record.as_slice();
        let err = decode_record(&mut buf).unwrap_err();
        assert!(err.to_string().contains("length CRC"));
    }

    #[test]
    fn a_truncated_tail_is_an_error_not_a_bogus_record() {
        // Given a record cut mid-payload, as a hard kill leaves a partial's tail.
        let record = encode_record(b"hello");
        let mut buf = &record[..record.len() - 6];

        // Then decoding reports truncation (the reader treats this as "stop here").
        assert!(decode_record(&mut buf).is_err());
    }
}
