//! Sparse offset/time index of a segment.
//!
//! One entry is written for the first record of a segment and then roughly every
//! [`INDEX_INTERVAL_BYTES`] of log data. Each entry maps a relative offset to a byte position
//! and carries the record's timestamp. Because the broker stamps timestamps monotonically per
//! partition, the same entries serve offset seeks and time seeks.
//!
//! File format: a flat array of 16-byte little-endian entries
//! `rel_offset: u32 | position: u32 | timestamp_ms: u64`. The index is derived data: it is not
//! fsynced on every commit, and it is validated (and rebuilt from the log if needed) on open.

use parking_lot::RwLock;

pub const ENTRY_SIZE: usize = 16;
pub const INDEX_INTERVAL_BYTES: u64 = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub rel_offset: u32,
    pub position: u32,
    pub timestamp_ms: u64,
}

impl IndexEntry {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.rel_offset.to_le_bytes());
        out.extend_from_slice(&self.position.to_le_bytes());
        out.extend_from_slice(&self.timestamp_ms.to_le_bytes());
    }

    fn decode(b: &[u8]) -> IndexEntry {
        IndexEntry {
            rel_offset: u32::from_le_bytes(b[0..4].try_into().expect("4-byte slice")),
            position: u32::from_le_bytes(b[4..8].try_into().expect("4-byte slice")),
            timestamp_ms: u64::from_le_bytes(b[8..16].try_into().expect("8-byte slice")),
        }
    }
}

/// Parses an index file. Returns `None` when the content is structurally invalid
/// (wrong size, non-monotonic, positions beyond `log_len`).
pub fn parse_index(bytes: &[u8], log_len: u64) -> Option<Vec<IndexEntry>> {
    if !bytes.len().is_multiple_of(ENTRY_SIZE) {
        return None;
    }
    let entries: Vec<IndexEntry> = bytes
        .as_chunks::<ENTRY_SIZE>()
        .0
        .iter()
        .map(|c| IndexEntry::decode(c))
        .collect();
    if let Some(first) = entries.first()
        && (first.rel_offset != 0 || first.position != 0)
    {
        return None;
    }
    for w in entries.windows(2) {
        if w[1].rel_offset <= w[0].rel_offset
            || w[1].position <= w[0].position
            || w[1].timestamp_ms < w[0].timestamp_ms
        {
            return None;
        }
    }
    if let Some(last) = entries.last()
        && u64::from(last.position) >= log_len
    {
        return None;
    }
    Some(entries)
}

/// In-memory sparse index shared between the writer (appends) and readers (lookups).
#[derive(Debug, Default)]
pub struct SparseIndex {
    entries: RwLock<Vec<IndexEntry>>,
}

impl SparseIndex {
    pub fn new(entries: Vec<IndexEntry>) -> Self {
        Self {
            entries: RwLock::new(entries),
        }
    }

    pub fn push(&self, e: IndexEntry) {
        self.entries.write().push(e);
    }

    /// Drops entries at or beyond `position` (writer rollback).
    pub fn truncate_from_position(&self, position: u64) -> usize {
        let mut e = self.entries.write();
        e.retain(|e| u64::from(e.position) < position);
        e.len()
    }

    pub fn first(&self) -> Option<IndexEntry> {
        self.entries.read().first().copied()
    }

    pub fn last(&self) -> Option<IndexEntry> {
        self.entries.read().last().copied()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let entries = self.entries.read();
        let mut out = Vec::with_capacity(entries.len() * ENTRY_SIZE);
        for e in entries.iter() {
            e.encode(&mut out);
        }
        out
    }

    /// Encoded entries whose position is below `len` (the index of a segment prefix).
    pub fn bytes_below(&self, len: u64) -> Vec<u8> {
        let entries = self.entries.read();
        let mut out = Vec::new();
        for e in entries.iter().take_while(|e| u64::from(e.position) < len) {
            e.encode(&mut out);
        }
        out
    }

    /// Last entry with `rel_offset <= rel`; `(rel_offset, position)`, or `(0, 0)`.
    pub fn lookup_offset(&self, rel: u32) -> (u32, u32) {
        let entries = self.entries.read();
        let idx = entries.partition_point(|e| e.rel_offset <= rel);
        if idx == 0 {
            (0, 0)
        } else {
            (entries[idx - 1].rel_offset, entries[idx - 1].position)
        }
    }

    /// Last entry whose timestamp is strictly below `ts` (every record before it is older than
    /// `ts`, so a forward scan from it finds the first record with timestamp >= `ts`).
    pub fn lookup_time(&self, ts: u64) -> (u32, u32) {
        let entries = self.entries.read();
        let idx = entries.partition_point(|e| e.timestamp_ms < ts);
        if idx == 0 {
            (0, 0)
        } else {
            (entries[idx - 1].rel_offset, entries[idx - 1].position)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(r: u32, p: u32, t: u64) -> IndexEntry {
        IndexEntry {
            rel_offset: r,
            position: p,
            timestamp_ms: t,
        }
    }

    #[test]
    fn lookups() {
        let idx = SparseIndex::new(vec![e(0, 0, 10), e(10, 4100, 20), e(20, 8200, 30)]);
        assert_eq!(idx.lookup_offset(0), (0, 0));
        assert_eq!(idx.lookup_offset(9), (0, 0));
        assert_eq!(idx.lookup_offset(10), (10, 4100));
        assert_eq!(idx.lookup_offset(1000), (20, 8200));
        assert_eq!(idx.lookup_time(5), (0, 0));
        assert_eq!(idx.lookup_time(10), (0, 0));
        assert_eq!(idx.lookup_time(11), (0, 0));
        assert_eq!(idx.lookup_time(21), (10, 4100));
        assert_eq!(idx.lookup_time(1000), (20, 8200));
    }

    #[test]
    fn parse_validation() {
        let idx = SparseIndex::new(vec![e(0, 0, 10), e(10, 4100, 20)]);
        let bytes = idx.to_bytes();
        assert_eq!(parse_index(&bytes, 5000).unwrap().len(), 2);
        assert!(parse_index(&bytes, 4100).is_none());
        assert!(parse_index(&bytes[..20], 5000).is_none());
        let bad = SparseIndex::new(vec![e(0, 0, 10), e(0, 4100, 20)]).to_bytes();
        assert!(parse_index(&bad, 5000).is_none());
        assert_eq!(parse_index(&[], 0).unwrap().len(), 0);
    }
}
