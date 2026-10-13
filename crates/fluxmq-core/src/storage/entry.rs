//! Log entries (v0.7): a segment holds a sequence of entries, each either a legacy single
//! record (v0.1–v0.6, see [`super::record`]) or a batch envelope ([`fluxmq_proto::batch`]).
//! Readers dispatch on bit 31 of the first `u32`. Both kinds are self-delimiting and
//! self-verifying: CRC-32 (IEEE) for legacy records, CRC-32C for envelopes.
//!
//! An envelope covers `record_count` consecutive offsets from `base_offset`; a legacy
//! record covers one. Envelopes are written whole and atomically become visible, so every
//! offset boundary a reader, a follower or a truncation sees between entries is an entry
//! boundary (the high watermark included: batches are acked whole).

use bytes::Bytes;
use fluxmq_proto::batch::{
    self, BATCH_FLAG, Compression, ENVELOPE_HEADER_LEN, EnvelopeHeader, MAX_BATCH_LEN,
    MAX_BATCH_RECORDS, RecordsSection, codec,
};
use fluxmq_proto::model::{Message, PublishMessage};

use super::record::{self, DecodeError, Record};
use crate::error::CoreError;

/// Upper bound for a decompressed records section (same as one legacy record).
pub const MAX_SECTION_LEN: usize = record::MAX_RECORD_LEN;

/// What a reader needs to know about one entry without decoding its records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryMeta {
    /// Offset of the first (legacy: only) record.
    pub base_offset: u64,
    /// Records in the entry (legacy: 1).
    pub count: u32,
    /// Timestamp of the first record. Envelopes written by this broker carry one timestamp
    /// for all their records (`timestamp_delta` is 0, enforced on append).
    pub timestamp_ms: u64,
    /// Bytes of the whole entry.
    pub len: usize,
    /// `Some` for a batch envelope.
    pub compression: Option<Compression>,
}

impl EntryMeta {
    pub fn is_batch(&self) -> bool {
        self.compression.is_some()
    }

    /// Offset after the last record of the entry.
    pub fn next_offset(&self) -> u64 {
        self.base_offset + u64::from(self.count)
    }

    pub fn contains(&self, offset: u64) -> bool {
        offset >= self.base_offset && offset < self.next_offset()
    }
}

/// One entry read from a segment: its byte position and its bytes (a zero-copy slice of
/// the read buffer).
#[derive(Debug, Clone)]
pub struct Entry {
    pub pos: u64,
    pub meta: EntryMeta,
    pub bytes: Bytes,
}

/// Total size of the entry starting at `buf` (needs only its first 4 bytes).
pub fn peek_entry_len(buf: &[u8]) -> Result<usize, DecodeError> {
    if buf.len() < 4 {
        return Err(DecodeError::Incomplete { needed: 4 });
    }
    let first = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if first & BATCH_FLAG == 0 {
        return record::peek_total_len(buf);
    }
    let len = (first & !BATCH_FLAG) as usize;
    if len < ENVELOPE_HEADER_LEN - 4 || len + 4 > MAX_BATCH_LEN {
        return Err(DecodeError::Corrupt(format!(
            "invalid envelope length {len}"
        )));
    }
    Ok(len + 4)
}

/// Parses the metadata of the complete entry at the start of `buf` (which holds at least
/// the whole entry). With `verify`, the CRC is checked too (recovery, replication); readers
/// of the visible log skip it for entries they don't decode.
pub fn parse_meta(buf: &[u8], verify: bool) -> Result<EntryMeta, DecodeError> {
    let total = peek_entry_len(buf)?;
    if buf.len() < total {
        return Err(DecodeError::Incomplete { needed: total });
    }
    if batch::is_batch(u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]])) {
        let h = EnvelopeHeader::parse(buf)
            .ok_or_else(|| DecodeError::Corrupt("invalid envelope header".into()))?;
        if verify && codec::crc32c(&buf[8..total]) != h.crc32c {
            return Err(DecodeError::Corrupt("envelope crc32c mismatch".into()));
        }
        return Ok(EntryMeta {
            base_offset: h.base_offset,
            count: h.record_count,
            timestamp_ms: h.base_timestamp_ms,
            len: total,
            compression: Some(h.compression),
        });
    }
    let (offset, ts) = if verify {
        let (o, t, _) = record::decode_meta(buf)?;
        (o, t)
    } else {
        (
            u64::from_le_bytes(buf[8..16].try_into().expect("8-byte slice")),
            u64::from_le_bytes(buf[16..24].try_into().expect("8-byte slice")),
        )
    };
    Ok(EntryMeta {
        base_offset: offset,
        count: 1,
        timestamp_ms: ts,
        len: total,
        compression: None,
    })
}

/// Splits a buffer of whole entries into their metadata (CRC verified), checking that
/// offsets are contiguous from `expect`. Used for replicated chunks and verification.
pub fn split_entries(buf: &[u8], mut expect: u64) -> Result<Vec<(usize, EntryMeta)>, String> {
    let mut out = Vec::new();
    let mut at = 0usize;
    while at < buf.len() {
        let m = parse_meta(&buf[at..], true).map_err(|e| format!("entry at byte {at}: {e:?}"))?;
        if m.base_offset != expect {
            return Err(format!(
                "entries not contiguous: got offset {}, expected {expect}",
                m.base_offset
            ));
        }
        expect = m.next_offset();
        out.push((at, m));
        at += m.len;
    }
    Ok(out)
}

fn corrupt(e: impl std::fmt::Display) -> DecodeError {
    DecodeError::Corrupt(e.to_string())
}

struct BatchCursor {
    section: Bytes,
    at: usize,
    base_offset: u64,
    base_ts: u64,
    index: u32,
    count: u32,
}

/// Iterates the records of a buffer of whole entries (legacy records and envelopes, any
/// compression). Every entry's CRC is verified before its records are yielded. Payloads are
/// zero-copy slices of the buffer (or of the decompressed section). Stops after the first
/// error.
pub struct ChunkRecords {
    chunk: Bytes,
    pos: usize,
    cur: Option<BatchCursor>,
    failed: bool,
}

impl ChunkRecords {
    pub fn new(chunk: Bytes) -> Self {
        Self {
            chunk,
            pos: 0,
            cur: None,
            failed: false,
        }
    }

    fn next_inner(&mut self) -> Option<Result<Record, DecodeError>> {
        loop {
            if let Some(c) = &mut self.cur {
                if c.index < c.count {
                    return Some(next_batch_record(c));
                }
                if c.at != c.section.len() {
                    return Some(Err(corrupt("trailing bytes after the last batch record")));
                }
                self.cur = None;
            }
            if self.pos >= self.chunk.len() {
                return None;
            }
            let rest = &self.chunk[self.pos..];
            let total = match peek_entry_len(rest) {
                Ok(t) if t <= rest.len() => t,
                Ok(t) => return Some(Err(DecodeError::Incomplete { needed: t })),
                Err(e) => return Some(Err(e)),
            };
            let entry = self.chunk.slice(self.pos..self.pos + total);
            self.pos += total;
            if !batch::is_batch(u32::from_le_bytes([entry[0], entry[1], entry[2], entry[3]])) {
                return Some(record::decode_shared(&entry).map(|(r, _)| r));
            }
            match open_envelope(&entry) {
                Ok(c) => self.cur = Some(c),
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// Verifies an envelope and returns a cursor over its (decompressed) records section.
fn open_envelope(entry: &Bytes) -> Result<BatchCursor, DecodeError> {
    let h = codec::verify_envelope(entry).map_err(corrupt)?;
    let body = entry.slice(ENVELOPE_HEADER_LEN..h.total_len());
    let section = match h.compression {
        Compression::None => body,
        c => Bytes::from(codec::decompress(c, &body, MAX_SECTION_LEN).map_err(corrupt)?),
    };
    Ok(BatchCursor {
        section,
        at: 0,
        base_offset: h.base_offset,
        base_ts: h.base_timestamp_ms,
        index: 0,
        count: h.record_count,
    })
}

fn next_batch_record(c: &mut BatchCursor) -> Result<Record, DecodeError> {
    let rest = &c.section[c.at..];
    let v = match batch::Records::new(rest, 1).next() {
        Some(Ok(v)) => v,
        Some(Err(e)) => {
            c.index = c.count;
            return Err(corrupt(e));
        }
        None => unreachable!("Records yields one item for count 1"),
    };
    if v.offset_delta != u64::from(c.index) {
        c.index = c.count;
        return Err(corrupt(format!(
            "batch record {} has offset delta {}",
            c.index, v.offset_delta
        )));
    }
    // The payload is the record's last field: its end is the record's end.
    let end = v.payload.as_ptr() as usize + v.payload.len() - c.section.as_ptr() as usize;
    let rec = Record {
        offset: c.base_offset + v.offset_delta,
        timestamp_ms: c.base_ts + v.timestamp_delta,
        key: v.key.map(str::to_owned),
        headers: v.headers_map(),
        payload: c.section.slice_ref(v.payload),
    };
    c.at = end;
    c.index += 1;
    Ok(rec)
}

impl Iterator for ChunkRecords {
    type Item = Result<Record, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        let r = self.next_inner();
        if matches!(r, Some(Err(_))) {
            self.failed = true;
        }
        r
    }
}

/// Decodes a buffer of whole entries — an [`EnvelopeChunk`](super::EnvelopeChunk)'s bytes,
/// a `DeliverRecords` body — into messages of `partition`: legacy records and
/// compressed/uncompressed batches, CRC verified, zero-copy payloads.
pub fn decode_chunk(
    partition: u32,
    chunk: &Bytes,
) -> impl Iterator<Item = crate::error::Result<Message>> + use<> {
    ChunkRecords::new(chunk.clone()).map(move |r| {
        r.map(|r| r.into_message(partition))
            .map_err(|e| CoreError::Corrupt(format!("{e:?}")))
    })
}

/// Where the keys of a batch route it (`PublishBatch` with `partition: None`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyRoute {
    /// No record has a key: sticky partition.
    Keyless,
    /// Every keyed record maps to this partition.
    Partition(u32),
    /// Keyed records map to more than one partition.
    Spans,
}

/// Result of [`validate_section`].
#[derive(Debug, Clone, Copy)]
pub struct SectionInfo {
    pub route: KeyRoute,
    /// Uncompressed section bytes.
    pub raw_len: usize,
    /// v0.12: the first record's `flux-pseq` mark (producer id, batch sequence).
    pub producer: Option<(u64, u64)>,
    /// v0.12 `Priority` channels (`levels` given): the batch's one priority level.
    pub priority: Option<u32>,
}

/// Validates a producer's records section before it is stored verbatim: count and size
/// limits, decompression (with a size cap), every record well formed (UTF-8 keys and
/// headers), `offset_delta == index` and `timestamp_delta == 0` (the broker assigns one
/// timestamp per batch). `partitions` is used to compute the key route.
pub fn validate_section(sec: &RecordsSection, partitions: u32) -> Result<SectionInfo, String> {
    validate_section_with(sec, partitions, None)
}

/// [`validate_section`] for a `Priority` channel with `levels` levels (`Some`): also checks that
/// every record has the same priority level and reports it.
pub fn validate_section_with(
    sec: &RecordsSection,
    partitions: u32,
    levels: Option<u32>,
) -> Result<SectionInfo, String> {
    if sec.record_count == 0 || sec.record_count > MAX_BATCH_RECORDS {
        return Err(format!(
            "record_count {} outside 1..={MAX_BATCH_RECORDS}",
            sec.record_count
        ));
    }
    if ENVELOPE_HEADER_LEN + sec.body.len() > MAX_BATCH_LEN {
        return Err(format!(
            "batch of {} bytes exceeds the {MAX_BATCH_LEN} byte limit",
            sec.body.len()
        ));
    }
    let owned;
    let section: &[u8] = match sec.compression {
        Compression::None => &sec.body,
        c => {
            owned = codec::decompress(c, &sec.body, MAX_SECTION_LEN)
                .map_err(|e| format!("{} section: {e}", c.as_str()))?;
            &owned
        }
    };
    let mut route = KeyRoute::Keyless;
    let mut producer = None;
    for (i, r) in batch::Records::new(section, sec.record_count).enumerate() {
        let r = r.map_err(|e| format!("record {i}: {e}"))?;
        if i == 0 {
            producer = r
                .headers
                .iter()
                .find(|(k, _)| *k == fluxmq_proto::model::headers::PRODUCER_SEQ)
                .and_then(|(_, v)| batch::parse_pseq(v));
        }
        if r.offset_delta != i as u64 {
            return Err(format!(
                "record {i}: offset_delta {} != {i}",
                r.offset_delta
            ));
        }
        if r.timestamp_delta != 0 {
            return Err(format!("record {i}: timestamp_delta must be 0"));
        }
        if let Some(k) = r.key {
            let p = batch::key_partition(k, partitions.max(1));
            route = match route {
                KeyRoute::Keyless => KeyRoute::Partition(p),
                KeyRoute::Partition(q) if q == p => route,
                _ => KeyRoute::Spans,
            };
        }
    }
    let priority = match levels {
        Some(l) => Some(batch::section_priority(section, sec.record_count, l)?),
        None => None,
    };
    Ok(SectionInfo {
        route,
        raw_len: section.len(),
        producer,
        priority,
    })
}

/// The messages of a (valid) records section, payloads sharing the section's bytes — for
/// writing it as legacy records (`batch_writes` off during a rolling upgrade).
pub fn section_messages(sec: &RecordsSection) -> Result<Vec<PublishMessage>, String> {
    let section = match sec.compression {
        Compression::None => sec.body.clone(),
        c => Bytes::from(
            codec::decompress(c, &sec.body, MAX_SECTION_LEN).map_err(|e| e.to_string())?,
        ),
    };
    let mut out = Vec::with_capacity(sec.record_count as usize);
    for r in batch::Records::new(&section, sec.record_count) {
        let r = r.map_err(|e| e.to_string())?;
        out.push(PublishMessage {
            key: r.key.map(str::to_owned),
            headers: r.headers_map(),
            payload: section.slice_ref(r.payload),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn section(n: u32, key: impl Fn(u32) -> Option<String>) -> Vec<u8> {
        let mut out = Vec::new();
        for i in 0..n {
            let mut h = BTreeMap::new();
            if i % 2 == 0 {
                h.insert("h".to_string(), format!("v{i}"));
            }
            batch::encode_record(
                &mut out,
                i,
                key(i).as_deref(),
                &h,
                format!("p{i}").as_bytes(),
            );
        }
        out
    }

    #[test]
    fn chunk_decode_mixed_and_compressed() {
        let sec = section(3, |i| (i == 1).then(|| "k".into()));
        let mut chunk = codec::build_envelope(10, 500, 3, Compression::None, &sec).unwrap();
        record::encode_into(&mut chunk, 13, 501, None, &BTreeMap::new(), b"legacy");
        let big = section(200, |_| None);
        for c in [Compression::Lz4, Compression::Zstd] {
            let z = codec::compress(c, &big).unwrap();
            let base = if c == Compression::Lz4 { 14 } else { 214 };
            chunk.extend(codec::build_envelope(base, 502, 200, c, &z).unwrap());
        }
        let chunk = Bytes::from(chunk);
        let msgs: Vec<Message> = decode_chunk(7, &chunk).collect::<Result<_, _>>().unwrap();
        assert_eq!(msgs.len(), 404);
        for (i, m) in msgs.iter().enumerate() {
            assert_eq!(m.offset, 10 + i as u64);
            assert_eq!(m.partition, 7);
        }
        assert_eq!(msgs[1].key.as_deref(), Some("k"));
        assert_eq!(msgs[0].headers.get("h").map(String::as_str), Some("v0"));
        assert_eq!(&msgs[3].payload[..], b"legacy");
        assert_eq!(msgs[3].timestamp_ms, 501);
        assert_eq!(&msgs[403].payload[..], b"p199");
        // Corruption is detected and stops the iteration.
        let mut bad = chunk.to_vec();
        bad[40] ^= 1;
        let res: Vec<_> = decode_chunk(0, &Bytes::from(bad)).collect();
        assert_eq!(res.len(), 1);
        assert!(res[0].is_err());
    }

    #[test]
    fn validation_and_routing() {
        let ok = RecordsSection {
            compression: Compression::None,
            record_count: 4,
            body: Bytes::from(section(4, |_| None)),
        };
        assert_eq!(validate_section(&ok, 8).unwrap().route, KeyRoute::Keyless);
        let keyed = RecordsSection {
            body: Bytes::from(section(4, |i| (i % 2 == 0).then(|| "same".into()))),
            ..ok.clone()
        };
        let p = batch::key_partition("same", 8);
        assert_eq!(
            validate_section(&keyed, 8).unwrap().route,
            KeyRoute::Partition(p)
        );
        let spans = RecordsSection {
            body: Bytes::from(section(64, |i| Some(format!("k{i}")))),
            record_count: 64,
            ..ok.clone()
        };
        assert_eq!(validate_section(&spans, 8).unwrap().route, KeyRoute::Spans);
        // Wrong count, bad deltas, garbage compression.
        assert!(
            validate_section(
                &RecordsSection {
                    record_count: 5,
                    ..ok.clone()
                },
                8
            )
            .is_err()
        );
        let mut bad_delta = Vec::new();
        batch::encode_record(&mut bad_delta, 3, None, &BTreeMap::new(), b"x");
        assert!(
            validate_section(
                &RecordsSection {
                    record_count: 1,
                    body: Bytes::from(bad_delta),
                    ..ok.clone()
                },
                8
            )
            .is_err()
        );
        assert!(
            validate_section(
                &RecordsSection {
                    compression: Compression::Lz4,
                    ..ok.clone()
                },
                8
            )
            .is_err()
        );
        let msgs = section_messages(&keyed).unwrap();
        assert_eq!(msgs.len(), 4);
        assert_eq!(msgs[0].key.as_deref(), Some("same"));
        assert_eq!(&msgs[3].payload[..], b"p3");
    }
}
