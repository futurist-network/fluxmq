//! Property / fuzz-style tests for the v0.7 batch format (`fluxmq_proto::batch`) and the v4
//! frames that carry it.
//!
//! No extra dependencies (the workspace has no proptest / cargo-fuzz and the crate's
//! manifest stays untouched): a small deterministic PRNG (splitmix64) drives thousands of
//! generated cases per property. Every failure prints the case seed; re-run one case with
//! `BATCH_PROPS_SEED=<seed> cargo test -p fluxmq-proto --test batch_props`. Scale the case
//! count with `BATCH_PROPS_CASES` (default 2000 per property; CI keeps the default, a soak
//! run can use 1_000_000).
//!
//! Properties:
//! * `EnvelopeHeader::parse` never panics on arbitrary bytes (random, flag-biased, all
//!   lengths 0..64); whatever it accepts satisfies the documented invariants and re-encodes
//!   to the same 29 meaningful bytes; `write` → `parse` is the identity for valid headers.
//! * `get_varint` never panics, consumes at most 10 bytes and roundtrips `put_varint`.
//! * `Records` over arbitrary bytes and arbitrary counts never panics, yields at most
//!   `min(count, len / 5) + 1` items, and is fused after the first error.
//! * Encode → decode roundtrip of random records (keys, UTF-8 headers, binary payloads).
//! * Every strict prefix of a valid section is reported as an error (a torn batch can never
//!   decode as complete), and a section with extra bytes is an error.
//! * Random byte flips / insertions / deletions in valid sections never panic.
//! * Hostile length fields (huge header counts, key / payload lengths near u64::MAX) are
//!   rejected without allocating.
//! * v4 frames (`PublishBatch`, `Commit`, `AckRanges`, `DeliverRecords`, `PublishBatchOk`)
//!   roundtrip through `encode_frame` / `decode_body`, and `decode_body` never panics on
//!   arbitrary or mutated bodies.

use std::collections::BTreeMap;

use bytes::Bytes;
use fluxmq_proto::batch::{
    BATCH_FLAG, BatchError, Compression, ENVELOPE_HEADER_LEN, EnvelopeHeader, MAX_BATCH_LEN,
    MAX_BATCH_RECORDS, Records, RecordsSection, encode_record, get_varint, is_batch, put_varint,
    record_size_hint,
};
use fluxmq_proto::wire::{
    ClientFrame, ClientRequest, DeliveryRange, ServerFrame, ServerResponse, decode_body,
    encode_frame,
};

// ---------------------------------------------------------------------------------------
// Deterministic generator

#[derive(Clone)]
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // splitmix64
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
    fn range(&mut self, lo: usize, hi_incl: usize) -> usize {
        lo + self.below((hi_incl - lo + 1) as u64) as usize
    }
    fn chance(&mut self, num: u64, den: u64) -> bool {
        self.below(den) < num
    }
    fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| self.next() as u8).collect()
    }
    fn bytes_in(&mut self, lo: usize, hi_incl: usize) -> Vec<u8> {
        let n = self.range(lo, hi_incl);
        self.bytes(n)
    }
    /// Valid UTF-8 with some multi-byte characters.
    fn string(&mut self, max_chars: usize) -> String {
        const POOL: &[char] = &[
            'a', 'z', '0', '-', '_', ' ', 'é', 'Ж', '€', '中', '🦀', '\0',
        ];
        let n = self.range(0, max_chars);
        (0..n)
            .map(|_| {
                if self.chance(1, 3) {
                    POOL[self.below(POOL.len() as u64) as usize]
                } else {
                    char::from(b'a' + self.below(26) as u8)
                }
            })
            .collect()
    }
    /// Interesting u64 values: boundaries of every varint length, plus random ones.
    fn edge_u64(&mut self) -> u64 {
        match self.below(4) {
            0 => {
                let bits = self.range(0, 64) as u32;
                let base = if bits == 64 {
                    u64::MAX
                } else {
                    (1u64 << bits).wrapping_sub(1)
                };
                base.wrapping_add(self.below(3)).wrapping_sub(1)
            }
            1 => self.below(300),
            2 => u64::MAX - self.below(3),
            _ => self.next(),
        }
    }
}

fn cases() -> u64 {
    std::env::var("BATCH_PROPS_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000)
}

/// Runs `prop` for every case seed (or only `BATCH_PROPS_SEED`), naming the seed on panic.
fn check(name: &str, prop: impl Fn(&mut Rng) + std::panic::RefUnwindSafe) {
    let base = 0xF1_u64 << 56 | fnv(name);
    let seeds: Vec<u64> = match std::env::var("BATCH_PROPS_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
    {
        Some(s) => vec![s],
        None => (0..cases()).map(|i| base.wrapping_add(i)).collect(),
    };
    for seed in seeds {
        let r = std::panic::catch_unwind(|| prop(&mut Rng(seed)));
        if let Err(e) = r {
            let msg = e
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            panic!("property `{name}` failed for BATCH_PROPS_SEED={seed}: {msg}");
        }
    }
}

fn fnv(s: &str) -> u64 {
    s.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    }) & 0x00FF_FFFF_FFFF_FFFF
}

// ---------------------------------------------------------------------------------------
// Generated records

#[derive(Debug, Clone, PartialEq)]
struct Rec {
    key: Option<String>,
    headers: BTreeMap<String, String>,
    payload: Vec<u8>,
}

fn gen_rec(r: &mut Rng) -> Rec {
    let key = r.chance(1, 2).then(|| r.string(12));
    let mut headers = BTreeMap::new();
    for _ in 0..r.range(0, 4) {
        headers.insert(r.string(8), r.string(16));
    }
    let len = match r.below(10) {
        0 => 0,
        1 => r.range(1000, 5000),
        _ => r.range(0, 200),
    };
    Rec {
        key,
        headers,
        payload: r.bytes(len),
    }
}

fn encode(recs: &[Rec]) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, rec) in recs.iter().enumerate() {
        let before = out.len();
        encode_record(
            &mut out,
            i as u32,
            rec.key.as_deref(),
            &rec.headers,
            &rec.payload,
        );
        let hint = record_size_hint(rec.key.as_deref(), &rec.headers, rec.payload.len());
        assert!(
            out.len() - before <= hint,
            "record_size_hint {hint} < encoded {}",
            out.len() - before
        );
    }
    out
}

/// Drains a `Records` iterator, asserting the structural guarantees that hold for any
/// input: bounded item count, fused after the first error. Returns (ok records, error).
fn drain(section: &[u8], count: u32) -> (usize, Option<BatchError>) {
    let mut it = Records::new(section, count);
    // Each record needs >= 5 bytes (five 1-byte varints), so at most len/5 Ok items.
    let bound = (count as usize).min(section.len() / 5) + 1;
    let mut ok = 0usize;
    let mut err = None;
    let mut items = 0usize;
    for item in it.by_ref() {
        items += 1;
        assert!(items <= bound, "iterator yielded more than {bound} items");
        match item {
            Ok(v) => {
                ok += 1;
                assert!(v.payload.len() <= section.len());
            }
            Err(e) => {
                err = Some(e);
                break;
            }
        }
    }
    if err.is_some() {
        assert!(it.next().is_none(), "Records not fused after an error");
        assert!(it.next().is_none());
    } else {
        assert_eq!(ok, count as usize, "clean end before record_count");
    }
    (ok, err)
}

// ---------------------------------------------------------------------------------------
// EnvelopeHeader

#[test]
fn envelope_parse_never_panics_and_accepts_only_valid() {
    check("envelope_parse_arbitrary", |r| {
        let n = r.range(0, 64);
        let mut b = r.bytes(n);
        // Bias towards inputs that get past the first checks.
        if n >= 4 && r.chance(3, 4) {
            let len = match r.below(4) {
                0 => r.below(64) as u32,
                1 => (ENVELOPE_HEADER_LEN - 4) as u32 + r.below(4096) as u32,
                2 => MAX_BATCH_LEN as u32 - 2 + r.below(4) as u32,
                _ => r.next() as u32 & !BATCH_FLAG,
            };
            b[0..4].copy_from_slice(&(len | BATCH_FLAG).to_le_bytes());
        }
        if n >= 29 && r.chance(1, 2) {
            let c = match r.below(4) {
                0 => 0,
                1 => 1 + r.below(10) as u32,
                2 => MAX_BATCH_RECORDS - 1 + r.below(3) as u32,
                _ => r.next() as u32,
            };
            b[24..28].copy_from_slice(&c.to_le_bytes());
            b[28] = if r.chance(3, 4) {
                r.below(8) as u8
            } else {
                r.next() as u8
            };
        }
        let parsed = EnvelopeHeader::parse(&b);
        let Some(h) = parsed else { return };
        assert!(b.len() >= ENVELOPE_HEADER_LEN);
        assert!(is_batch(u32::from_le_bytes(b[0..4].try_into().unwrap())));
        assert!(h.len as usize >= ENVELOPE_HEADER_LEN - 4);
        assert!(h.len as usize <= MAX_BATCH_LEN);
        assert!(h.total_len() >= ENVELOPE_HEADER_LEN);
        assert!(h.record_count >= 1 && h.record_count <= MAX_BATCH_RECORDS);
        assert_eq!(b[28] & !0b111, 0);
        assert_eq!(Compression::from_bits(b[28]), Some(h.compression));
        // next_offset must not overflow silently for offsets the broker can assign; it is
        // only defined below u64::MAX - record_count.
        if h.base_offset <= u64::MAX - u64::from(h.record_count) {
            assert_eq!(h.next_offset(), h.base_offset + u64::from(h.record_count));
        }
        // Re-encoding reproduces the meaningful 29 bytes (29..32 are reserved, written 0).
        let mut out = [0u8; ENVELOPE_HEADER_LEN];
        h.write(&mut out);
        assert_eq!(&out[..29], &b[..29]);
        assert_eq!(&out[29..], &[0, 0, 0]);
        assert_eq!(EnvelopeHeader::parse(&out), Some(h));
        // A legacy record (bit 31 clear) with the same bytes is never a batch.
        let mut legacy = out;
        legacy[3] &= 0x7f;
        assert_eq!(EnvelopeHeader::parse(&legacy), None);
        // Truncated headers are rejected.
        for cut in [0, 1, 4, 28, ENVELOPE_HEADER_LEN - 1] {
            assert_eq!(EnvelopeHeader::parse(&out[..cut]), None);
        }
    });
}

#[test]
fn envelope_write_parse_roundtrip() {
    check("envelope_roundtrip", |r| {
        let h = EnvelopeHeader {
            len: r.range(ENVELOPE_HEADER_LEN - 4, MAX_BATCH_LEN) as u32,
            crc32c: r.next() as u32,
            base_offset: r.edge_u64(),
            base_timestamp_ms: r.edge_u64(),
            record_count: r.range(1, MAX_BATCH_RECORDS as usize) as u32,
            compression: [Compression::None, Compression::Lz4, Compression::Zstd]
                [r.below(3) as usize],
        };
        let mut out = [0u8; ENVELOPE_HEADER_LEN];
        h.write(&mut out);
        assert_eq!(EnvelopeHeader::parse(&out), Some(h));
        // Trailing bytes (the records section) don't affect the header.
        let mut long = out.to_vec();
        long.extend(r.bytes_in(0, 64));
        assert_eq!(EnvelopeHeader::parse(&long), Some(h));
        // Unknown compression bits / reserved attribute bits are rejected.
        let mut bad = out;
        bad[28] = 3 + r.below(5) as u8;
        assert_eq!(EnvelopeHeader::parse(&bad), None);
        bad[28] = h.compression.to_bits() | (1 << (3 + r.below(5)));
        assert_eq!(EnvelopeHeader::parse(&bad), None);
        // Zero records is invalid.
        let mut zero = out;
        zero[24..28].fill(0);
        assert_eq!(EnvelopeHeader::parse(&zero), None);
    });
}

#[test]
fn compression_bits_roundtrip() {
    for c in [Compression::None, Compression::Lz4, Compression::Zstd] {
        assert_eq!(Compression::from_bits(c.to_bits()), Some(c));
    }
    for b in 0u8..=255 {
        let _ = Compression::from_bits(b); // never panics
        assert_eq!(Compression::from_bits(b), Compression::from_bits(b & 0b111));
    }
}

// ---------------------------------------------------------------------------------------
// Varints

#[test]
fn varint_roundtrip_and_arbitrary_bytes() {
    check("varint", |r| {
        let v = r.edge_u64();
        let mut b = Vec::new();
        put_varint(&mut b, v);
        assert!(b.len() <= 10);
        let tail = r.bytes_in(0, 4);
        b.extend_from_slice(&tail);
        let mut s = b.as_slice();
        assert_eq!(get_varint(&mut s), Ok(v));
        assert_eq!(s, tail.as_slice());

        let junk = r.bytes_in(0, 16);
        let mut s = junk.as_slice();
        let res = get_varint(&mut s);
        let used = junk.len() - s.len();
        assert!(used <= 10);
        if let Ok(x) = res {
            // Canonical or not, re-decoding the consumed bytes gives the same value.
            let mut again = &junk[..used];
            assert_eq!(get_varint(&mut again), Ok(x));
        }
    });
}

// ---------------------------------------------------------------------------------------
// Records

#[test]
fn records_roundtrip() {
    check("records_roundtrip", |r| {
        let n = r.range(1, 40);
        let recs: Vec<Rec> = (0..n).map(|_| gen_rec(r)).collect();
        let sec = encode(&recs);
        let decoded: Vec<_> = Records::new(&sec, n as u32)
            .collect::<Result<_, _>>()
            .expect("valid section decodes");
        assert_eq!(decoded.len(), n);
        for (i, (d, want)) in decoded.iter().zip(&recs).enumerate() {
            assert_eq!(d.offset_delta, i as u64);
            assert_eq!(d.timestamp_delta, 0);
            assert_eq!(d.key, want.key.as_deref());
            assert_eq!(d.headers_map(), want.headers);
            assert_eq!(d.payload, want.payload.as_slice());
        }
        // Wrong counts: fewer -> trailing bytes, more -> truncated.
        if n > 1 {
            let (_, e) = drain(&sec, n as u32 - 1);
            assert!(matches!(e, Some(BatchError::Malformed(_))), "{e:?}");
        }
        let (ok, e) = drain(&sec, n as u32 + 1 + r.below(5) as u32);
        assert_eq!(ok, n);
        assert_eq!(e, Some(BatchError::Truncated));
    });
}

#[test]
fn torn_section_is_never_complete() {
    check("torn_section", |r| {
        let n = r.range(1, 12);
        let recs: Vec<Rec> = (0..n).map(|_| gen_rec(r)).collect();
        let sec = encode(&recs);
        // A random strict prefix, plus the boundary cases.
        let mut cuts = vec![0, sec.len() - 1, r.below(sec.len() as u64) as usize];
        cuts.push(sec.len() / 2);
        for cut in cuts {
            let (ok, e) = drain(&sec[..cut], n as u32);
            assert!(ok < n, "prefix {cut}/{} decoded all {n} records", sec.len());
            assert!(e.is_some(), "prefix {cut} ended without an error");
        }
        // Extra bytes after a complete section are rejected.
        let mut long = sec.clone();
        long.extend(r.bytes_in(1, 8));
        let (ok, e) = drain(&long, n as u32);
        assert_eq!(ok, n);
        assert!(matches!(e, Some(BatchError::Malformed(_))));
    });
}

#[test]
fn records_never_panic_on_arbitrary_bytes() {
    check("records_arbitrary", |r| {
        let len = match r.below(3) {
            0 => r.range(0, 8),
            1 => r.range(0, 64),
            _ => r.range(0, 2048),
        };
        let b = r.bytes(len);
        let count = match r.below(4) {
            0 => 0,
            1 => r.range(1, 8) as u32,
            2 => u32::MAX,
            _ => r.next() as u32,
        };
        drain(&b, count);
    });
}

#[test]
fn records_never_panic_on_mutated_sections() {
    check("records_mutated", |r| {
        let n = r.range(1, 10);
        let recs: Vec<Rec> = (0..n).map(|_| gen_rec(r)).collect();
        let mut sec = encode(&recs);
        for _ in 0..r.range(1, 4) {
            match r.below(4) {
                0 if !sec.is_empty() => {
                    let i = r.below(sec.len() as u64) as usize;
                    sec[i] ^= 1 << r.below(8);
                }
                1 if !sec.is_empty() => {
                    let i = r.below(sec.len() as u64) as usize;
                    sec[i] = r.next() as u8;
                }
                2 => {
                    let i = r.below(sec.len() as u64 + 1) as usize;
                    sec.insert(i, r.next() as u8);
                }
                _ if !sec.is_empty() => {
                    let i = r.below(sec.len() as u64) as usize;
                    sec.remove(i);
                }
                _ => {}
            }
        }
        let count = if r.chance(3, 4) {
            n as u32
        } else {
            r.next() as u32
        };
        drain(&sec, count);
    });
}

#[test]
fn hostile_length_fields_are_rejected_cheaply() {
    // offset_delta, timestamp_delta, key_len+1, header_count, ... with absurd values.
    let huge = [
        u64::MAX,
        u64::MAX - 1,
        1 << 62,
        u32::MAX as u64 + 1,
        1 << 40,
    ];
    for &h in &huge {
        // Huge key length.
        let mut s = Vec::new();
        put_varint(&mut s, 0);
        put_varint(&mut s, 0);
        put_varint(&mut s, h);
        s.extend_from_slice(b"abc");
        assert!(matches!(drain(&s, 1).1, Some(BatchError::Truncated)));
        // Huge header count (must not pre-allocate h entries).
        let mut s = Vec::new();
        put_varint(&mut s, 0);
        put_varint(&mut s, 0);
        put_varint(&mut s, 0);
        put_varint(&mut s, h);
        s.extend_from_slice(&[0; 16]);
        assert!(drain(&s, 1).1.is_some());
        // Huge header key / value length.
        let mut s = Vec::new();
        for v in [0, 0, 0, 1] {
            put_varint(&mut s, v);
        }
        put_varint(&mut s, h);
        assert!(drain(&s, 1).1.is_some());
        // Huge payload length.
        let mut s = Vec::new();
        for v in [0, 0, 0, 0] {
            put_varint(&mut s, v);
        }
        put_varint(&mut s, h);
        s.extend_from_slice(b"payload");
        assert_eq!(drain(&s, 1).1, Some(BatchError::Truncated));
    }
    // Invalid UTF-8 in a key / header is Malformed, not a panic.
    let mut s = Vec::new();
    for v in [0, 0, 3] {
        put_varint(&mut s, v);
    }
    s.extend_from_slice(&[0xff, 0xfe]);
    put_varint(&mut s, 0);
    put_varint(&mut s, 0);
    assert!(matches!(drain(&s, 1).1, Some(BatchError::Malformed(_))));
    // Overlong varint (11 continuation bytes).
    let s = [0x80u8; 11];
    assert!(matches!(drain(&s, 1).1, Some(BatchError::Malformed(_))));
    // Max record count over a tiny buffer terminates immediately.
    let mut s = Vec::new();
    encode_record(&mut s, 0, None, &BTreeMap::new(), b"x");
    let (ok, e) = drain(&s, u32::MAX);
    assert_eq!((ok, e), (1, Some(BatchError::Truncated)));
}

// ---------------------------------------------------------------------------------------
// v4 frames carrying batches

fn gen_section(r: &mut Rng) -> RecordsSection {
    let n = r.range(1, 20);
    let recs: Vec<Rec> = (0..n).map(|_| gen_rec(r)).collect();
    RecordsSection {
        compression: [Compression::None, Compression::Lz4, Compression::Zstd][r.below(3) as usize],
        record_count: n as u32,
        body: Bytes::from(encode(&recs)),
    }
}

fn body_of(frame: &Bytes) -> &[u8] {
    let len = u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize;
    assert_eq!(len, frame.len() - 4);
    &frame[4..]
}

#[test]
fn v4_frames_roundtrip() {
    check("v4_frames", |r| {
        let client = ClientFrame {
            req_id: r.next(),
            body: match r.below(3) {
                0 => ClientRequest::PublishBatch {
                    channel: r.string(20),
                    partition: r.chance(3, 4).then(|| r.next() as u32),
                    records: gen_section(r),
                },
                1 => ClientRequest::Commit {
                    subscription_id: r.next(),
                    partition: r.next() as u32,
                    offset: r.edge_u64(),
                },
                _ => ClientRequest::AckRanges {
                    subscription_id: r.next(),
                    ranges: (0..r.range(0, 10))
                        .map(|_| DeliveryRange {
                            first: r.edge_u64(),
                            count: r.next() as u32,
                        })
                        .collect(),
                },
            },
        };
        let f = encode_frame(&client).expect("encode");
        assert_eq!(
            decode_body::<ClientFrame>(body_of(&f)).expect("decode"),
            client
        );

        let server = match r.below(2) {
            0 => ServerFrame::DeliverRecords {
                subscription_id: r.next(),
                partition: r.next() as u32,
                first_delivery_id: r.next(),
                from_offset: r.edge_u64(),
                count: r.next() as u32,
                envelopes: gen_section(r).body,
            },
            _ => ServerFrame::Response {
                req_id: r.next(),
                result: Ok(ServerResponse::PublishBatchOk {
                    partition: r.next() as u32,
                    base_offset: r.edge_u64(),
                    count: r.next() as u32,
                }),
            },
        };
        let f = encode_frame(&server).expect("encode");
        assert_eq!(
            decode_body::<ServerFrame>(body_of(&f)).expect("decode"),
            server
        );
    });
}

#[test]
fn frame_decode_never_panics() {
    check("frame_decode", |r| {
        // Arbitrary bodies and mutated valid bodies.
        let body = if r.chance(1, 2) {
            r.bytes_in(0, 256)
        } else {
            let f = ClientFrame {
                req_id: r.next(),
                body: ClientRequest::PublishBatch {
                    channel: r.string(10),
                    partition: Some(r.below(8) as u32),
                    records: gen_section(r),
                },
            };
            let mut b = body_of(&encode_frame(&f).unwrap()).to_vec();
            for _ in 0..r.range(1, 3) {
                let i = r.below(b.len() as u64) as usize;
                b[i] = r.next() as u8;
            }
            if r.chance(1, 4) {
                b.truncate(r.below(b.len() as u64) as usize);
            }
            b
        };
        let _ = decode_body::<ClientFrame>(&body);
        let _ = decode_body::<ServerFrame>(&body);
        // A decoded PublishBatch's section is then validated by the broker; that must not
        // panic either, whatever the frame said.
        if let Ok(ClientFrame {
            body: ClientRequest::PublishBatch { records, .. },
            ..
        }) = decode_body::<ClientFrame>(&body)
            && records.compression == Compression::None
        {
            drain(&records.body, records.record_count);
        }
    });
}

// ---------------------------------------------------------------------------------------
// codec (feature `codec`: compression, CRC-32C, envelopes). Runs with
// `cargo test -p fluxmq-proto --features codec --test batch_props` and in workspace runs
// (the SDK and core enable the feature).

#[cfg(feature = "codec")]
mod codec_props {
    use super::*;
    use fluxmq_proto::batch::codec::{
        build_envelope, compress, crc32c, crc32c_append, decompress, verify_envelope,
    };

    const CODECS: [Compression; 3] = [Compression::None, Compression::Lz4, Compression::Zstd];

    #[test]
    fn compress_decompress_roundtrip_and_limit() {
        check("codec_roundtrip", |r| {
            let n = r.range(1, 30);
            let recs: Vec<Rec> = (0..n).map(|_| gen_rec(r)).collect();
            let mut sec = encode(&recs);
            if r.chance(1, 2) {
                // Make it compressible.
                let rep = sec.clone();
                for _ in 0..r.range(1, 4) {
                    sec.extend_from_slice(&rep);
                }
            }
            for c in CODECS {
                match compress(c, &sec) {
                    None => {} // not smaller (or `None`): the producer sends it uncompressed
                    Some(z) => {
                        assert_ne!(c, Compression::None);
                        assert!(z.len() < sec.len(), "compress returned a larger body");
                        assert_eq!(decompress(c, &z, MAX_BATCH_LEN).as_deref(), Ok(&sec[..]));
                        // The output limit is enforced.
                        assert!(decompress(c, &z, sec.len() - 1).is_err());
                    }
                }
            }
        });
    }

    #[test]
    fn decompress_never_panics_on_arbitrary_bytes() {
        check("codec_decompress_arbitrary", |r| {
            let mut b = r.bytes_in(0, 512);
            if b.len() >= 4 && r.chance(1, 3) {
                // Plausible / hostile lz4 size prefixes.
                let n = [0u32, 1, 64, 1 << 20, u32::MAX][r.below(5) as usize];
                b[..4].copy_from_slice(&n.to_le_bytes());
            }
            if b.len() >= 4 && r.chance(1, 4) {
                b[..4].copy_from_slice(&[0x28, 0xb5, 0x2f, 0xfd]); // zstd magic
            }
            let limit = [0usize, 16, 4096, MAX_BATCH_LEN][r.below(4) as usize];
            for c in [Compression::Lz4, Compression::Zstd] {
                if let Ok(out) = decompress(c, &b, limit) {
                    assert!(out.len() <= limit, "{c:?} exceeded the output limit");
                }
            }
        });
    }

    #[test]
    fn decompress_never_panics_on_mutated_streams() {
        check("codec_decompress_mutated", |r| {
            let n = r.range(1, 10);
            let recs: Vec<Rec> = (0..n).map(|_| gen_rec(r)).collect();
            let sec = encode(&recs).repeat(3);
            for c in [Compression::Lz4, Compression::Zstd] {
                let Some(mut z) = compress(c, &sec) else {
                    continue;
                };
                let i = r.below(z.len() as u64) as usize;
                z[i] ^= 1 << r.below(8);
                if r.chance(1, 3) {
                    z.truncate(r.below(z.len() as u64) as usize);
                }
                if let Ok(out) = decompress(c, &z, MAX_BATCH_LEN) {
                    // A corrupt stream that still decodes must not break the record parser.
                    drain(&out, n as u32);
                }
            }
        });
    }

    #[test]
    fn envelope_build_verify_and_every_corruption_detected() {
        check("codec_envelope", |r| {
            let n = r.range(1, 20);
            let recs: Vec<Rec> = (0..n).map(|_| gen_rec(r)).collect();
            let sec = encode(&recs);
            let c = CODECS[r.below(3) as usize];
            let (c, body) = match compress(c, &sec) {
                Some(z) => (c, z),
                None => (Compression::None, sec.clone()),
            };
            let base = r.edge_u64() >> 1;
            let env = build_envelope(base, r.next() >> 24, n as u32, c, &body).expect("build");
            let h = verify_envelope(&env).expect("verify");
            assert_eq!(h.total_len(), env.len());
            assert_eq!(
                (h.base_offset, h.record_count, h.compression),
                (base, n as u32, c)
            );
            let plain = decompress(c, &env[ENVELOPE_HEADER_LEN..], MAX_BATCH_LEN).unwrap();
            assert_eq!(plain, sec);
            // CRC-32C over [8, total) incrementally equals one pass.
            assert_eq!(
                crc32c_append(
                    crc32c(&env[8..ENVELOPE_HEADER_LEN]),
                    &env[ENVELOPE_HEADER_LEN..]
                ),
                h.crc32c
            );
            // Any single bit flip anywhere is detected (header checks or CRC), never accepted.
            let i = r.below(env.len() as u64) as usize;
            let mut bad = env.clone();
            bad[i] ^= 1 << r.below(8);
            match verify_envelope(&bad) {
                Err(_) => {}
                Ok(h2) => panic!("bit flip at byte {i} accepted: {h2:?}"),
            }
            // Every strict prefix (a torn write) is rejected.
            let cut = r.below(env.len() as u64) as usize;
            assert!(
                verify_envelope(&env[..cut]).is_err(),
                "prefix {cut} accepted"
            );
            // Trailing bytes after the envelope (the next entry in a segment) are ignored.
            let mut long = env.clone();
            long.extend(r.bytes_in(0, 40));
            assert_eq!(verify_envelope(&long), Ok(h));
        });
    }

    #[test]
    fn verify_envelope_never_panics_on_arbitrary_bytes() {
        check("codec_verify_arbitrary", |r| {
            let mut b = r.bytes_in(0, 256);
            if b.len() >= 4 && r.chance(3, 4) {
                let len = (r.range(28, b.len().max(28) + 8) as u32) | BATCH_FLAG;
                b[0..4].copy_from_slice(&len.to_le_bytes());
            }
            if b.len() >= 29 {
                b[24..28].copy_from_slice(&(1 + r.below(4) as u32).to_le_bytes());
                b[28] = r.below(3) as u8;
            }
            let _ = verify_envelope(&b);
        });
    }

    #[test]
    fn build_envelope_rejects_empty_and_oversized() {
        assert!(build_envelope(0, 0, 0, Compression::None, b"x").is_err());
        let big = vec![0u8; MAX_BATCH_LEN];
        assert!(build_envelope(0, 0, 1, Compression::None, &big).is_err());
    }
}
