//! Raw fetch payloads (v0.7): legacy single records and batch envelopes back to back,
//! exactly as stored in the leader's segment. Replication never decodes records: it walks
//! the entry framing, verifies batch envelopes (CRC-32C) and hands the bytes to the writer.
//!
//! Both entry kinds share the framing used here (little endian):
//!
//! ```text
//! 0   4  len      legacy: bytes after this field; batch: bit 31 set, bits 0..31 = bytes after
//! 4   4  crc      legacy: CRC-32 IEEE of bytes [8, end); batch: CRC-32C of bytes [8, end)
//! 8   8  offset   legacy: the record's offset; batch: base offset
//! 24  4  count    batch only: record count
//! ```
//!
//! See `fluxmq_proto::batch` for the envelope and `fluxmq_core::storage::record` for the
//! legacy record.

use fluxmq_proto::batch::{self, BATCH_FLAG, EnvelopeHeader};

/// Smallest legacy record (`storage::record::FIXED_OVERHEAD`).
const LEGACY_MIN: usize = 32;
/// Largest legacy record (`storage::record::MAX_RECORD_LEN`).
const LEGACY_MAX: usize = 64 * 1024 * 1024;

/// Summary of a verified chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkInfo {
    /// Offset of the first record (base offset of the first entry).
    pub first_offset: u64,
    /// One past the last record.
    pub next_offset: u64,
    /// Legacy records in the chunk.
    pub legacy: u64,
    /// Batch envelopes in the chunk.
    pub batches: u64,
}

impl ChunkInfo {
    pub fn records(&self) -> u64 {
        self.next_offset - self.first_offset
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChunkError {
    #[error("entry at byte {at}: truncated")]
    Truncated { at: usize },
    #[error("entry at byte {at}: {why}")]
    Malformed { at: usize, why: &'static str },
    #[error("entry at byte {at}: envelope CRC-32C mismatch")]
    Crc { at: usize },
    #[error("entry at byte {at}: offset {got}, expected {expected} (not contiguous)")]
    Gap { at: usize, got: u64, expected: u64 },
}

/// One entry's framing.
#[derive(Debug, Clone, Copy)]
struct Entry {
    total: usize,
    base: u64,
    count: u64,
    batch: bool,
}

fn entry_at(bytes: &[u8], at: usize) -> Result<Entry, ChunkError> {
    let rest = &bytes[at..];
    if rest.len() < 16 {
        return Err(ChunkError::Truncated { at });
    }
    let len_flag = u32::from_le_bytes(rest[0..4].try_into().expect("4 bytes"));
    let base = u64::from_le_bytes(rest[8..16].try_into().expect("8 bytes"));
    if batch::is_batch(len_flag) {
        let h = EnvelopeHeader::parse(rest).ok_or(if rest.len() < batch::ENVELOPE_HEADER_LEN {
            ChunkError::Truncated { at }
        } else {
            ChunkError::Malformed {
                at,
                why: "invalid envelope header",
            }
        })?;
        if rest.len() < h.total_len() {
            return Err(ChunkError::Truncated { at });
        }
        Ok(Entry {
            total: h.total_len(),
            base,
            count: u64::from(h.record_count),
            batch: true,
        })
    } else {
        let len = (len_flag & !BATCH_FLAG) as usize;
        if !(LEGACY_MIN - 4..=LEGACY_MAX).contains(&len) {
            return Err(ChunkError::Malformed {
                at,
                why: "invalid record length",
            });
        }
        if rest.len() < 4 + len {
            return Err(ChunkError::Truncated { at });
        }
        Ok(Entry {
            total: 4 + len,
            base,
            count: 1,
            batch: false,
        })
    }
}

/// Follower side: checks the framing of every entry, that offsets are contiguous, and every
/// checksum (CRC-32C of a batch envelope, CRC-32 of a legacy record), so corruption on the
/// wire is told apart from log divergence and refetched. Nothing is decoded or decompressed.
/// (The core writer verifies again on append: the last line of defence for the disk.)
pub fn verify(bytes: &[u8]) -> Result<ChunkInfo, ChunkError> {
    let mut at = 0usize;
    let mut info: Option<ChunkInfo> = None;
    while at < bytes.len() {
        let e = entry_at(bytes, at)?;
        let body = &bytes[at + 8..at + e.total];
        let crc_ok = if e.batch {
            batch::codec::crc32c(body) == crc_of(bytes, at)
        } else {
            crc32fast::hash(body) == crc_of(bytes, at)
        };
        if !crc_ok {
            return Err(ChunkError::Crc { at });
        }
        let i = info.get_or_insert(ChunkInfo {
            first_offset: e.base,
            next_offset: e.base,
            legacy: 0,
            batches: 0,
        });
        if e.base != i.next_offset {
            return Err(ChunkError::Gap {
                at,
                got: e.base,
                expected: i.next_offset,
            });
        }
        i.next_offset = e.base + e.count;
        if e.batch {
            i.batches += 1;
        } else {
            i.legacy += 1;
        }
        at += e.total;
    }
    info.ok_or(ChunkError::Truncated { at: 0 })
}

fn crc_of(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at + 4..at + 8].try_into().expect("4 bytes"))
}

/// Leader side, for a follower that can't read batches (a node older than v0.7): the length
/// of the leading run of legacy records and their count. Stops at the first envelope.
/// Framing errors end the prefix (the leader's own bytes; the follower verifies anyway).
pub fn legacy_prefix(bytes: &[u8]) -> (usize, u64) {
    let mut at = 0usize;
    let mut count = 0u64;
    while at < bytes.len() {
        match entry_at(bytes, at) {
            Ok(e) if !e.batch => {
                at += e.total;
                count += 1;
            }
            _ => break,
        }
    }
    (at, count)
}

/// True when the chunk holds at least one batch envelope.
pub fn has_batches(bytes: &[u8]) -> bool {
    legacy_prefix(bytes).0 < bytes.len()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use fluxmq_proto::batch::{Compression, codec, encode_record};
    use std::collections::BTreeMap;

    /// A legacy record (no key, no headers) with a valid CRC-32.
    pub(crate) fn legacy(offset: u64, payload: &[u8]) -> Vec<u8> {
        let len = 28 + payload.len();
        let mut b = Vec::new();
        b.extend_from_slice(&(len as u32).to_le_bytes());
        b.extend_from_slice(&0u32.to_le_bytes());
        b.extend_from_slice(&offset.to_le_bytes());
        b.extend_from_slice(&0u64.to_le_bytes());
        b.extend_from_slice(&u32::MAX.to_le_bytes());
        b.extend_from_slice(&0u32.to_le_bytes());
        b.extend_from_slice(payload);
        let crc = crc32fast::hash(&b[8..]);
        b[4..8].copy_from_slice(&crc.to_le_bytes());
        b
    }

    pub(crate) fn envelope(base: u64, n: u32, c: Compression) -> Vec<u8> {
        let mut sec = Vec::new();
        for i in 0..n {
            encode_record(&mut sec, i, None, &BTreeMap::new(), &[b'x'; 200]);
        }
        let (c, body) = match codec::compress(c, &sec) {
            Some(z) => (c, z),
            None => (Compression::None, sec),
        };
        codec::build_envelope(base, 1, n, c, &body).unwrap()
    }

    #[test]
    fn mixed_chunk_verifies_and_counts() {
        let mut b = legacy(10, b"a");
        b.extend(legacy(11, b"bb"));
        b.extend(envelope(12, 5, Compression::None));
        b.extend(envelope(17, 3, Compression::Lz4));
        b.extend(legacy(20, b""));
        b.extend(envelope(21, 4, Compression::Zstd));
        let i = verify(&b).unwrap();
        assert_eq!(
            i,
            ChunkInfo {
                first_offset: 10,
                next_offset: 25,
                legacy: 3,
                batches: 3
            }
        );
        assert_eq!(i.records(), 15);
        let (len, n) = legacy_prefix(&b);
        assert_eq!(n, 2);
        assert_eq!(verify(&b[..len]).unwrap().next_offset, 12);
        assert!(has_batches(&b));
        assert!(!has_batches(&b[..len]));
    }

    #[test]
    fn corruption_gaps_and_truncation_are_detected() {
        let mut b = envelope(0, 3, Compression::None);
        b.extend(envelope(3, 3, Compression::None));
        // A flipped payload bit inside the second envelope.
        let mut bad = b.clone();
        let last = bad.len() - 5;
        bad[last] ^= 0x10;
        assert!(matches!(verify(&bad), Err(ChunkError::Crc { at }) if at > 0));
        // A flipped base offset is covered by the CRC too.
        let mut bad = b.clone();
        bad[9] ^= 1;
        assert_eq!(verify(&bad), Err(ChunkError::Crc { at: 0 }));
        // Torn tail.
        assert!(matches!(
            verify(&b[..b.len() - 1]),
            Err(ChunkError::Truncated { .. })
        ));
        // Gap between entries.
        let mut gap = envelope(0, 3, Compression::None);
        gap.extend(envelope(4, 1, Compression::None));
        assert!(matches!(
            verify(&gap),
            Err(ChunkError::Gap {
                got: 4,
                expected: 3,
                ..
            })
        ));
        let mut gap = legacy(0, b"x");
        gap.extend(legacy(2, b"x"));
        assert!(matches!(verify(&gap), Err(ChunkError::Gap { .. })));
        // A legacy record's CRC-32 is checked too.
        let mut bad = legacy(0, b"hello");
        bad[34] ^= 1;
        assert_eq!(verify(&bad), Err(ChunkError::Crc { at: 0 }));
        assert!(verify(&[]).is_err());
    }
}
