//! Record batches (v0.7): the unit that is produced, stored, replicated and delivered.
//!
//! One batch holds the messages of one publish request for one partition. The producer
//! encodes the **records section** once. The broker wraps it in an envelope and writes it
//! as is. Consumers on protocol v4 receive stored envelopes verbatim
//! (`ServerFrame::DeliverRecords`), so the broker does no per-message work on the fast path.
//! Every reliability feature still works: per-message delivery ids, individual ack,
//! redelivery, nack and DLQ (see `docs/design/v0.7.md`).
//!
//! ## Envelope (on disk, in replication and in `DeliverRecords`; little endian)
//!
//! ```text
//! offset size field
//! 0      4    len_flag          bit 31 = 1 (batch); bits 0..31 = bytes after this field
//! 4      4    crc32c            CRC-32C (Castagnoli) of bytes [8, 4 + len)
//! 8      8    base_offset       offset of the first record
//! 16     8    base_timestamp_ms broker-assigned; record timestamps are base + delta
//! 24     4    record_count      >= 1
//! 28     1    attributes        bits 0-2: Compression; other bits 0 (reserved)
//! 29     3    reserved          0
//! 32     ..   records section   (compressed as a whole when attributes say so)
//! ```
//!
//! **Telling batches from legacy records.** A legacy single record (v0.1–v0.6) starts with a
//! `u32` length below `MAX_RECORD_LEN` (64 MiB), so bit 31 is 0. A batch sets bit 31. One
//! segment can mix both, and readers dispatch on that bit. Downgrading to a broker that
//! predates v0.7 after batches were written is not supported (it would see a corrupt record).
//!
//! **Records section (uncompressed),** `record_count` records back to back:
//!
//! ```text
//! varint offset_delta        record offset = base_offset + offset_delta (0, 1, 2, ... in order)
//! varint timestamp_delta     record timestamp = base_timestamp_ms + timestamp_delta
//! varint key_len_plus_one    0 = no key; n + 1 = key of n bytes (UTF-8)
//! bytes  key
//! varint header_count
//!   repeated: varint k_len, k bytes (UTF-8), varint v_len, v bytes (UTF-8)
//! varint payload_len
//! bytes  payload
//! ```
//!
//! Varints are unsigned LEB128 (at most 10 bytes). Producers write `offset_delta = index`
//! and `timestamp_delta = 0`. The broker fills `base_offset` and `base_timestamp_ms` in
//! the envelope, so it never rewrites records.
//!
//! **Integrity.** The CRC covers everything after itself. The broker verifies it on
//! append (it computed it) and on recovery. Replicas verify it on fetch. Clients may
//! verify it (`DeliverRecords`). A torn batch at the tail of a log was never acked and is
//! truncated as a unit.

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Bit 31 of the first `u32` marks a batch envelope.
pub const BATCH_FLAG: u32 = 1 << 31;
/// Envelope header size (fields before the records section).
pub const ENVELOPE_HEADER_LEN: usize = 32;
/// Upper bound for one envelope (same as one frame body, so a batch always fits a frame).
pub const MAX_BATCH_LEN: usize = crate::wire::MAX_FRAME_LEN - 1024;
/// Upper bound for records in one batch.
pub const MAX_BATCH_RECORDS: u32 = 1 << 20;

/// Per-batch compression, set by the producer and stored as is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum Compression {
    #[default]
    None,
    Lz4,
    Zstd,
}

impl Compression {
    pub fn to_bits(self) -> u8 {
        match self {
            Compression::None => 0,
            Compression::Lz4 => 1,
            Compression::Zstd => 2,
        }
    }

    pub fn from_bits(bits: u8) -> Option<Self> {
        match bits & 0b111 {
            0 => Some(Compression::None),
            1 => Some(Compression::Lz4),
            2 => Some(Compression::Zstd),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Lz4 => "lz4",
            Compression::Zstd => "zstd",
        }
    }
}

/// Decoded envelope header (`len` = bytes after the `len_flag` field).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvelopeHeader {
    pub len: u32,
    pub crc32c: u32,
    pub base_offset: u64,
    pub base_timestamp_ms: u64,
    pub record_count: u32,
    pub compression: Compression,
}

impl EnvelopeHeader {
    /// Total envelope size in bytes, including `len_flag`.
    pub fn total_len(&self) -> usize {
        4 + self.len as usize
    }

    /// Offset after the last record.
    pub fn next_offset(&self) -> u64 {
        self.base_offset + u64::from(self.record_count)
    }

    /// Writes the 32-byte header. `crc32c` must already cover bytes [8, total_len).
    pub fn write(&self, out: &mut [u8; ENVELOPE_HEADER_LEN]) {
        out[0..4].copy_from_slice(&(self.len | BATCH_FLAG).to_le_bytes());
        out[4..8].copy_from_slice(&self.crc32c.to_le_bytes());
        out[8..16].copy_from_slice(&self.base_offset.to_le_bytes());
        out[16..24].copy_from_slice(&self.base_timestamp_ms.to_le_bytes());
        out[24..28].copy_from_slice(&self.record_count.to_le_bytes());
        out[28] = self.compression.to_bits();
        out[29..32].fill(0);
    }

    /// Parses a header. Doesn't check the CRC (the caller hashes the body). Returns `None`
    /// when bit 31 isn't set (a legacy record) or the fields are invalid.
    pub fn parse(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < ENVELOPE_HEADER_LEN {
            return None;
        }
        let len_flag = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
        if len_flag & BATCH_FLAG == 0 {
            return None;
        }
        let len = len_flag & !BATCH_FLAG;
        if (len as usize) < ENVELOPE_HEADER_LEN - 4 || len as usize > MAX_BATCH_LEN {
            return None;
        }
        let record_count = u32::from_le_bytes(bytes[24..28].try_into().ok()?);
        if record_count == 0 || record_count > MAX_BATCH_RECORDS || bytes[28] & !0b111 != 0 {
            return None;
        }
        Some(Self {
            len,
            crc32c: u32::from_le_bytes(bytes[4..8].try_into().ok()?),
            base_offset: u64::from_le_bytes(bytes[8..16].try_into().ok()?),
            base_timestamp_ms: u64::from_le_bytes(bytes[16..24].try_into().ok()?),
            record_count,
            compression: Compression::from_bits(bytes[28])?,
        })
    }
}

/// Parses a [`crate::model::headers::PRODUCER_SEQ`] value: `<producer id hex>.<sequence>`.
pub fn parse_pseq(v: &str) -> Option<(u64, u64)> {
    let (pid, seq) = v.split_once('.')?;
    Some((u64::from_str_radix(pid, 16).ok()?, seq.parse().ok()?))
}

/// The producer id and batch sequence of a records section (uncompressed), read from its first
/// record's headers; `None` when it carries none or is malformed.
pub fn section_pseq(section: &[u8], record_count: u32) -> Option<(u64, u64)> {
    let first = Records::new(section, record_count).next()?.ok()?;
    first
        .headers
        .iter()
        .find(|(k, _)| *k == crate::model::headers::PRODUCER_SEQ)
        .and_then(|(_, v)| parse_pseq(v))
}

/// The priority level of a records section (uncompressed) on a `Priority` channel with
/// `levels` levels: every record's [`crate::model::headers::PRIORITY`] level must be the same
/// (a batch goes to one partition = one level). `Err` names the first mismatch.
pub fn section_priority(section: &[u8], record_count: u32, levels: u32) -> Result<u32, String> {
    let mut level = None;
    for (i, r) in Records::new(section, record_count).enumerate() {
        let r = r.map_err(|e| format!("record {i}: {e}"))?;
        let v = r
            .headers
            .iter()
            .find(|(k, _)| *k == crate::model::headers::PRIORITY)
            .map(|(_, v)| *v);
        let l = crate::model::priority_level_str(v, levels);
        match level {
            None => level = Some(l),
            Some(prev) if prev != l => {
                return Err(format!(
                    "record {i} has priority {l} but record 0 has {prev}: a batch holds one priority level"
                ));
            }
            _ => {}
        }
    }
    Ok(level.unwrap_or(0))
}

/// The channel key hash (contract, shared by broker, cluster and SDK):
/// `crc32_ieee(key bytes) % partitions`. Identical to v0.1–v0.6 `Channel::partition_for`.
pub fn key_partition(key: &str, partitions: u32) -> u32 {
    crc32fast::hash(key.as_bytes()) % partitions.max(1)
}

/// True when `first_u32_le` (the first 4 bytes of an entry) marks a batch envelope.
pub fn is_batch(first_u32_le: u32) -> bool {
    first_u32_le & BATCH_FLAG != 0
}

/// One decoded record; `key` / `headers` / `payload` borrow from the records section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordView<'a> {
    pub offset_delta: u64,
    pub timestamp_delta: u64,
    pub key: Option<&'a str>,
    pub headers: Vec<(&'a str, &'a str)>,
    pub payload: &'a [u8],
}

impl RecordView<'_> {
    /// Owned headers in the shape of [`crate::model::Message::headers`].
    pub fn headers_map(&self) -> BTreeMap<String, String> {
        self.headers
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BatchError {
    #[error("truncated records section")]
    Truncated,
    #[error("malformed records section: {0}")]
    Malformed(&'static str),
}

/// Appends one record to a records section (producer side; `index` = position in the batch).
pub fn encode_record(
    out: &mut Vec<u8>,
    index: u32,
    key: Option<&str>,
    headers: &BTreeMap<String, String>,
    payload: &[u8],
) {
    put_varint(out, u64::from(index));
    put_varint(out, 0);
    match key {
        None => put_varint(out, 0),
        Some(k) => {
            put_varint(out, k.len() as u64 + 1);
            out.extend_from_slice(k.as_bytes());
        }
    }
    put_varint(out, headers.len() as u64);
    for (k, v) in headers {
        put_varint(out, k.len() as u64);
        out.extend_from_slice(k.as_bytes());
        put_varint(out, v.len() as u64);
        out.extend_from_slice(v.as_bytes());
    }
    put_varint(out, payload.len() as u64);
    out.extend_from_slice(payload);
}

/// Upper bound of [`encode_record`]'s output, for pre-sizing buffers.
pub fn record_size_hint(
    key: Option<&str>,
    headers: &BTreeMap<String, String>,
    payload_len: usize,
) -> usize {
    let h: usize = headers.iter().map(|(k, v)| k.len() + v.len() + 20).sum();
    10 * 4 + key.map_or(0, str::len) + h + payload_len
}

/// Iterates the records of an **uncompressed** records section.
pub struct Records<'a> {
    rest: &'a [u8],
    remaining: u32,
}

impl<'a> Records<'a> {
    pub fn new(section: &'a [u8], record_count: u32) -> Self {
        Self {
            rest: section,
            remaining: record_count,
        }
    }
}

impl<'a> Iterator for Records<'a> {
    type Item = Result<RecordView<'a>, BatchError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return if self.rest.is_empty() {
                None
            } else {
                self.rest = &[];
                Some(Err(BatchError::Malformed(
                    "trailing bytes after last record",
                )))
            };
        }
        self.remaining -= 1;
        let r = decode_record(&mut self.rest);
        if r.is_err() {
            self.remaining = 0;
            self.rest = &[];
        }
        Some(r)
    }
}

fn decode_record<'a>(buf: &mut &'a [u8]) -> Result<RecordView<'a>, BatchError> {
    let offset_delta = get_varint(buf)?;
    let timestamp_delta = get_varint(buf)?;
    let key = match get_varint(buf)? {
        0 => None,
        n => Some(get_str(buf, (n - 1) as usize)?),
    };
    let header_count = get_varint(buf)?;
    if header_count > buf.len() as u64 {
        return Err(BatchError::Malformed("header count exceeds section"));
    }
    let mut headers = Vec::with_capacity(header_count as usize);
    for _ in 0..header_count {
        let kl = get_varint(buf)? as usize;
        let k = get_str(buf, kl)?;
        let vl = get_varint(buf)? as usize;
        let v = get_str(buf, vl)?;
        headers.push((k, v));
    }
    let pl = get_varint(buf)? as usize;
    let payload = take(buf, pl)?;
    Ok(RecordView {
        offset_delta,
        timestamp_delta,
        key,
        headers,
        payload,
    })
}

/// Unsigned LEB128.
pub fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Unsigned LEB128; advances `buf`.
pub fn get_varint(buf: &mut &[u8]) -> Result<u64, BatchError> {
    let mut v = 0u64;
    for i in 0..10 {
        let Some((&b, rest)) = buf.split_first() else {
            return Err(BatchError::Truncated);
        };
        *buf = rest;
        if i == 9 && b > 1 {
            return Err(BatchError::Malformed("varint overflow"));
        }
        v |= u64::from(b & 0x7f) << (7 * i);
        if b & 0x80 == 0 {
            return Ok(v);
        }
    }
    Err(BatchError::Malformed("varint too long"))
}

fn take<'a>(buf: &mut &'a [u8], n: usize) -> Result<&'a [u8], BatchError> {
    if buf.len() < n {
        return Err(BatchError::Truncated);
    }
    let (head, rest) = buf.split_at(n);
    *buf = rest;
    Ok(head)
}

fn get_str<'a>(buf: &mut &'a [u8], n: usize) -> Result<&'a str, BatchError> {
    std::str::from_utf8(take(buf, n)?).map_err(|_| BatchError::Malformed("invalid UTF-8"))
}

/// A records section built by a producer, ready for `ClientRequest::PublishBatch`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RecordsSection {
    pub compression: Compression,
    pub record_count: u32,
    /// Exactly the "records section" bytes above, compressed as a whole if `compression`
    /// isn't `None`.
    pub body: Bytes,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        for v in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut b = Vec::new();
            put_varint(&mut b, v);
            let mut s = b.as_slice();
            assert_eq!(get_varint(&mut s).unwrap(), v);
            assert!(s.is_empty());
        }
        let mut s: &[u8] = &[0x80];
        assert_eq!(get_varint(&mut s), Err(BatchError::Truncated));
    }

    #[test]
    fn pseq_parses_and_is_read_from_the_first_record_only() {
        assert_eq!(parse_pseq("00000000000000ff.42"), Some((255, 42)));
        assert_eq!(parse_pseq("ff.1"), Some((255, 1)));
        assert_eq!(parse_pseq("zz.1"), None);
        assert_eq!(parse_pseq("ff"), None);
        let mut h = BTreeMap::new();
        h.insert(
            crate::model::headers::PRODUCER_SEQ.to_string(),
            "a.7".to_string(),
        );
        let mut sec = Vec::new();
        encode_record(&mut sec, 0, None, &h, b"x");
        encode_record(&mut sec, 1, None, &BTreeMap::new(), b"y");
        assert_eq!(section_pseq(&sec, 2), Some((10, 7)));
        let mut sec2 = Vec::new();
        encode_record(&mut sec2, 0, None, &BTreeMap::new(), b"x");
        encode_record(&mut sec2, 1, None, &h, b"y");
        assert_eq!(section_pseq(&sec2, 2), None);
    }

    #[test]
    fn records_roundtrip() {
        let mut h = BTreeMap::new();
        h.insert("trace".to_string(), "abc".to_string());
        let mut sec = Vec::new();
        encode_record(&mut sec, 0, Some("k1"), &h, b"hello");
        encode_record(&mut sec, 1, None, &BTreeMap::new(), b"");
        let recs: Vec<_> = Records::new(&sec, 2).collect::<Result<_, _>>().unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].key, Some("k1"));
        assert_eq!(recs[0].headers, vec![("trace", "abc")]);
        assert_eq!(recs[0].payload, b"hello");
        assert_eq!(recs[1].offset_delta, 1);
        assert_eq!(recs[1].key, None);
        assert!(recs[1].payload.is_empty());
        // Wrong count: trailing bytes are an error, missing records are truncation.
        assert!(Records::new(&sec, 1).any(|r| r.is_err()));
        assert!(Records::new(&sec, 3).any(|r| r == Err(BatchError::Truncated)));
    }

    #[test]
    fn envelope_header_roundtrip_and_legacy_detection() {
        let h = EnvelopeHeader {
            len: 100,
            crc32c: 0xdead_beef,
            base_offset: 42,
            base_timestamp_ms: 1_700_000_000_000,
            record_count: 3,
            compression: Compression::Lz4,
        };
        let mut out = [0u8; ENVELOPE_HEADER_LEN];
        h.write(&mut out);
        assert!(is_batch(u32::from_le_bytes(out[0..4].try_into().unwrap())));
        assert_eq!(EnvelopeHeader::parse(&out), Some(h));
        assert_eq!(h.total_len(), 104);
        assert_eq!(h.next_offset(), 45);
        // A legacy record length (bit 31 clear) is not a batch.
        let mut legacy = out;
        legacy[0..4].copy_from_slice(&100u32.to_le_bytes());
        assert_eq!(EnvelopeHeader::parse(&legacy), None);
    }
}

/// Shared batch codec: compression and CRC-32C (feature `codec`).
///
/// Byte formats (contract):
/// * `Lz4`: the LZ4 **block** format, prefixed with the uncompressed length as `u32` little
///   endian (`lz4_flex::compress_prepend_size`).
/// * `Zstd`: a standard zstd frame with the content size included. Producers use level 1 by
///   default.
/// * The compressed bytes are the whole records section. If compression doesn't make it
///   smaller, producers send `Compression::None`.
/// * Decoders refuse output larger than `max_len` (use [`MAX_BATCH_LEN`]).
/// * CRC-32C (Castagnoli), hardware accelerated, covers envelope bytes `[8, total_len)`.
#[cfg(feature = "codec")]
pub mod codec {
    use super::{BatchError, Compression, ENVELOPE_HEADER_LEN, EnvelopeHeader, MAX_BATCH_LEN};

    /// Default zstd level for producers.
    pub const ZSTD_LEVEL: i32 = 1;

    /// CRC-32C of `bytes`.
    pub fn crc32c(bytes: &[u8]) -> u32 {
        crc32c::crc32c(bytes)
    }

    /// Continues a CRC-32C over another chunk (for header + body written separately).
    pub fn crc32c_append(crc: u32, bytes: &[u8]) -> u32 {
        crc32c::crc32c_append(crc, bytes)
    }

    /// Compresses a records section. Returns `None` when compression doesn't help (send
    /// `Compression::None` with the original bytes) or `c` is `None`.
    pub fn compress(c: Compression, section: &[u8]) -> Option<Vec<u8>> {
        let out = match c {
            Compression::None => return None,
            Compression::Lz4 => lz4_flex::compress_prepend_size(section),
            Compression::Zstd => zstd::bulk::compress(section, ZSTD_LEVEL).ok()?,
        };
        (out.len() < section.len()).then_some(out)
    }

    /// Decompresses a records section, refusing more than `max_len` bytes of output.
    pub fn decompress(c: Compression, data: &[u8], max_len: usize) -> Result<Vec<u8>, BatchError> {
        match c {
            Compression::None => Ok(data.to_vec()),
            Compression::Lz4 => {
                let declared = data
                    .get(..4)
                    .map(|b| u32::from_le_bytes(b.try_into().unwrap()) as usize)
                    .ok_or(BatchError::Truncated)?;
                if declared > max_len {
                    return Err(BatchError::Malformed("lz4 section exceeds limit"));
                }
                lz4_flex::decompress_size_prepended(data)
                    .map_err(|_| BatchError::Malformed("bad lz4 section"))
            }
            Compression::Zstd => zstd::bulk::decompress(data, max_len)
                .map_err(|_| BatchError::Malformed("bad zstd section or exceeds limit")),
        }
    }

    /// Builds a complete envelope around a records section (broker side). The section is
    /// copied once into the output; `crc32c` is computed over bytes `[8, total)`.
    pub fn build_envelope(
        base_offset: u64,
        base_timestamp_ms: u64,
        record_count: u32,
        compression: Compression,
        section: &[u8],
    ) -> Result<Vec<u8>, BatchError> {
        let total = ENVELOPE_HEADER_LEN + section.len();
        if total > MAX_BATCH_LEN || record_count == 0 {
            return Err(BatchError::Malformed("batch too large or empty"));
        }
        let mut header = EnvelopeHeader {
            len: (total - 4) as u32,
            crc32c: 0,
            base_offset,
            base_timestamp_ms,
            record_count,
            compression,
        };
        let mut h = [0u8; ENVELOPE_HEADER_LEN];
        header.write(&mut h);
        header.crc32c = crc32c_append(crc32c(&h[8..]), section);
        header.write(&mut h);
        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&h);
        out.extend_from_slice(section);
        Ok(out)
    }

    /// Verifies a complete envelope (header + body) and returns its header.
    pub fn verify_envelope(bytes: &[u8]) -> Result<EnvelopeHeader, BatchError> {
        let h = EnvelopeHeader::parse(bytes).ok_or(BatchError::Malformed("bad envelope header"))?;
        let total = h.total_len();
        if bytes.len() < total {
            return Err(BatchError::Truncated);
        }
        if crc32c(&bytes[8..total]) != h.crc32c {
            return Err(BatchError::Malformed("envelope CRC-32C mismatch"));
        }
        Ok(h)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::batch::{Records, encode_record};
        use std::collections::BTreeMap;

        #[test]
        fn compress_roundtrip_and_skip_when_useless() {
            let section = vec![7u8; 10_000];
            for c in [Compression::Lz4, Compression::Zstd] {
                let z = compress(c, &section).expect("compressible");
                assert!(z.len() < section.len());
                assert_eq!(decompress(c, &z, MAX_BATCH_LEN).unwrap(), section);
                assert!(decompress(c, &z, 100).is_err(), "limit enforced");
            }
            assert!(compress(Compression::Lz4, b"x").is_none());
        }

        #[test]
        fn envelope_build_verify_and_corruption() {
            let mut sec = Vec::new();
            encode_record(&mut sec, 0, Some("k"), &BTreeMap::new(), b"payload");
            let env = build_envelope(10, 1_000, 1, Compression::None, &sec).unwrap();
            let h = verify_envelope(&env).unwrap();
            assert_eq!((h.base_offset, h.record_count), (10, 1));
            let recs: Vec<_> = Records::new(&env[ENVELOPE_HEADER_LEN..], 1)
                .collect::<Result<_, _>>()
                .unwrap();
            assert_eq!(recs[0].payload, b"payload");
            let mut bad = env.clone();
            *bad.last_mut().unwrap() ^= 1;
            assert!(verify_envelope(&bad).is_err());
            assert_eq!(
                verify_envelope(&env[..env.len() - 1]),
                Err(BatchError::Truncated)
            );
        }
    }
}
