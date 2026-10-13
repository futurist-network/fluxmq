//! Protocol v4 record batches on the client: building `PublishBatch` records sections
//! (producer) and decoding `DeliverRecords` frames lazily (consumer).
//!
//! Formats are defined in [`fluxmq_proto::batch`]; compression and CRC-32C come from its shared
//! codec, so the SDK, the broker and replication can't disagree on bytes.

use std::collections::BTreeMap;

use bytes::Bytes;
use fluxmq_proto::batch::{
    self, Compression, ENVELOPE_HEADER_LEN, EnvelopeHeader, MAX_BATCH_LEN, MAX_BATCH_RECORDS,
    RecordView, Records, RecordsSection, codec,
};
use fluxmq_proto::wire::Delivery as RawDelivery;
use fluxmq_proto::{Message, PublishMessage};

use crate::error::{ClientError, Result};

/// Largest uncompressed records section a producer builds: the envelope (32-byte header +
/// section) must fit [`MAX_BATCH_LEN`], and the frame carrying it must fit the frame limit.
pub(crate) const MAX_SECTION_LEN: usize = MAX_BATCH_LEN - ENVELOPE_HEADER_LEN;

/// Encodes one record like [`batch::encode_record`], with an optional extra header that is
/// written as if it were in `headers` (the `flux-producer` stamp) without inserting it into the
/// message's map (saves two `String`s and a map node per message). Records-section headers are
/// unordered, so appending it is equivalent.
/// Optionally also the `flux-pseq` mark (idempotent publishing; only on a batch's first record).
pub(crate) fn encode_record_marked(
    out: &mut Vec<u8>,
    index: u32,
    msg: &PublishMessage,
    extra: Option<(&str, &str)>,
    mark: Option<&str>,
) {
    if extra.is_none() && mark.is_none() {
        batch::encode_record(out, index, msg.key.as_deref(), &msg.headers, &msg.payload);
        return;
    }
    batch::put_varint(out, u64::from(index));
    batch::put_varint(out, 0);
    match msg.key.as_deref() {
        None => batch::put_varint(out, 0),
        Some(key) => {
            batch::put_varint(out, key.len() as u64 + 1);
            out.extend_from_slice(key.as_bytes());
        }
    }
    let n = msg.headers.len() + usize::from(extra.is_some()) + usize::from(mark.is_some());
    batch::put_varint(out, n as u64);
    for (hk, hv) in &msg.headers {
        put_str(out, hk);
        put_str(out, hv);
    }
    if let Some((k, v)) = extra {
        put_str(out, k);
        put_str(out, v);
    }
    if let Some(m) = mark {
        put_str(out, fluxmq_proto::model::headers::PRODUCER_SEQ);
        put_str(out, m);
    }
    batch::put_varint(out, msg.payload.len() as u64);
    out.extend_from_slice(&msg.payload);
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    batch::put_varint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

/// Upper bound of [`encode_record_marked`]'s output (without the mark).
pub(crate) fn size_hint(msg: &PublishMessage, extra: Option<(&str, &str)>) -> usize {
    batch::record_size_hint(msg.key.as_deref(), &msg.headers, msg.payload.len())
        + extra.map_or(0, |(k, v)| k.len() + v.len() + 20)
}

/// A records section being built while a batch lingers: records are appended to one buffer
/// as they arrive, so dispatching a batch costs no further encoding.
#[derive(Default)]
pub(crate) struct RecordsBuilder {
    buf: Vec<u8>,
    count: u32,
    has_keys: bool,
}

impl RecordsBuilder {
    pub(crate) fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap),
            count: 0,
            has_keys: false,
        }
    }

    /// [`RecordsBuilder::push`] with a `flux-pseq` mark (the batch's first record).
    pub(crate) fn push_marked(
        &mut self,
        msg: &PublishMessage,
        extra: Option<(&str, &str)>,
        mark: Option<&str>,
    ) {
        // Only stamp / mark when the message doesn't carry the header itself.
        let extra = extra.filter(|(k, _)| !msg.headers.contains_key(*k));
        let mark = mark.filter(|_| {
            !msg.headers
                .contains_key(fluxmq_proto::model::headers::PRODUCER_SEQ)
        });
        self.buf
            .reserve(size_hint(msg, extra) + mark.map_or(0, |m| m.len() + 20));
        encode_record_marked(&mut self.buf, self.count, msg, extra, mark);
        self.count += 1;
        self.has_keys |= msg.key.is_some();
    }

    pub(crate) fn len(&self) -> usize {
        self.buf.len()
    }

    /// The finished section, compressed when that makes it smaller.
    pub(crate) fn finish(self, compression: Compression) -> Result<EncodedRecords> {
        if self.buf.len() > MAX_SECTION_LEN || self.count > MAX_BATCH_RECORDS {
            return Err(ClientError::Protocol(format!(
                "batch of {} records / {} bytes exceeds the batch limit ({} records / {} bytes)",
                self.count,
                self.buf.len(),
                MAX_BATCH_RECORDS,
                MAX_SECTION_LEN
            )));
        }
        let (compression, body) = match codec::compress(compression, &self.buf) {
            Some(z) => (compression, Bytes::from(z)),
            None => (Compression::None, Bytes::from(self.buf)),
        };
        Ok(EncodedRecords {
            section: RecordsSection {
                compression,
                record_count: self.count,
                body,
            },
            has_keys: self.has_keys,
        })
    }
}

/// A finished records section plus what routing needs to know about it.
#[derive(Debug, Clone)]
pub(crate) struct EncodedRecords {
    pub(crate) section: RecordsSection,
    /// Some record has a key (a batch without partition must then be split by the broker,
    /// which v4 refuses: such batches go out as `Publish` when the partition is unknown).
    pub(crate) has_keys: bool,
}

impl EncodedRecords {
    pub(crate) fn from_messages(
        msgs: &[PublishMessage],
        compression: Compression,
        mark: Option<&str>,
    ) -> Result<Self> {
        let cap = msgs.iter().map(|m| size_hint(m, None)).sum::<usize>();
        let mut b = RecordsBuilder::with_capacity(cap.min(MAX_SECTION_LEN + 1024));
        for (i, m) in msgs.iter().enumerate() {
            b.push_marked(m, None, if i == 0 { mark } else { None });
        }
        b.finish(compression)
    }

    /// The messages back (fallback to `Publish` for a v2/v3 connection). Rare: copies.
    pub(crate) fn to_messages(&self) -> Result<Vec<PublishMessage>> {
        let s = &self.section;
        let plain = section_bytes(s.compression, &s.body)?;
        Records::new(&plain, s.record_count)
            .map(|r| {
                let r = r.map_err(|e| ClientError::Protocol(e.to_string()))?;
                Ok(PublishMessage {
                    key: r.key.map(str::to_owned),
                    headers: r.headers_map(),
                    payload: plain.slice_ref(r.payload),
                })
            })
            .collect()
    }
}

/// The uncompressed records section (zero-copy when not compressed).
fn section_bytes(c: Compression, body: &Bytes) -> Result<Bytes> {
    match c {
        Compression::None => Ok(body.clone()),
        c => codec::decompress(c, body, MAX_BATCH_LEN)
            .map(Bytes::from)
            .map_err(|e| ClientError::Protocol(format!("{} batch: {e}", c.as_str()))),
    }
}

/// One `ServerFrame::DeliverRecords`, decoded lazily by the consumer task.
#[derive(Debug)]
pub(crate) struct RecordsFrame {
    pub(crate) partition: u32,
    pub(crate) first_delivery_id: u64,
    pub(crate) from_offset: u64,
    pub(crate) count: u32,
    /// Slice of the received frame (no copy).
    pub(crate) envelopes: Bytes,
}

/// Iterates the deliveries of a [`RecordsFrame`] one by one: envelopes are verified (CRC-32C,
/// optional) and decompressed when the cursor reaches them; payloads are slices of the frame
/// (or of the decompressed section). Records outside `from_offset .. from_offset + count` are
/// skipped.
pub(crate) struct RecordsCursor {
    frame: RecordsFrame,
    verify_crc: bool,
    /// Position of the next envelope in `frame.envelopes`.
    pos: usize,
    /// Current envelope: uncompressed section, read position, records left, header.
    section: Bytes,
    sec_pos: usize,
    left: u32,
    header: Option<EnvelopeHeader>,
    /// Deliveries yielded so far.
    yielded: u32,
    failed: bool,
}

impl RecordsCursor {
    pub(crate) fn new(frame: RecordsFrame, verify_crc: bool) -> Self {
        Self {
            frame,
            verify_crc,
            pos: 0,
            section: Bytes::new(),
            sec_pos: 0,
            left: 0,
            header: None,
            yielded: 0,
            failed: false,
        }
    }

    fn end(&self) -> u64 {
        self.frame.from_offset + u64::from(self.frame.count)
    }

    fn fail(&mut self, what: impl std::fmt::Display) -> Option<Result<RawDelivery>> {
        self.failed = true;
        Some(Err(ClientError::Protocol(format!(
            "bad DeliverRecords frame (partition {}, offset {}): {what}",
            self.frame.partition, self.frame.from_offset
        ))))
    }

    /// Loads the next envelope. `Ok(false)` = no more envelopes.
    fn next_envelope(&mut self) -> std::result::Result<bool, String> {
        let env = &self.frame.envelopes;
        if self.pos >= env.len() {
            return Ok(false);
        }
        let rest = &env[self.pos..];
        let h = if self.verify_crc {
            codec::verify_envelope(rest).map_err(|e| e.to_string())?
        } else {
            let h = EnvelopeHeader::parse(rest).ok_or("bad envelope header")?;
            if rest.len() < h.total_len() {
                return Err("truncated envelope".into());
            }
            h
        };
        let body = env.slice(self.pos + ENVELOPE_HEADER_LEN..self.pos + h.total_len());
        self.pos += h.total_len();
        self.section = section_bytes(h.compression, &body).map_err(|e| e.to_string())?;
        self.sec_pos = 0;
        self.left = h.record_count;
        self.header = Some(h);
        Ok(true)
    }

    /// Next delivery; `Some(Err)` once on a malformed / corrupt frame, then `None`.
    pub(crate) fn next_delivery(&mut self) -> Option<Result<RawDelivery>> {
        if self.failed {
            return None;
        }
        loop {
            if self.yielded >= self.frame.count {
                return None;
            }
            if self.left == 0 {
                match self.next_envelope() {
                    Ok(true) => continue,
                    Ok(false) => {
                        let msg =
                            format!("{} of {} records present", self.yielded, self.frame.count);
                        return self.fail(msg);
                    }
                    Err(e) => return self.fail(e),
                }
            }
            let h = self.header.expect("envelope loaded");
            let rest = &self.section[self.sec_pos..];
            let view: RecordView<'_> = match Records::new(rest, 1).next() {
                Some(Ok(v)) => v,
                Some(Err(e)) => return self.fail(e),
                None => return self.fail("missing record"),
            };
            // The payload is a record's last field: everything up to its end is consumed.
            let consumed =
                view.payload.as_ptr() as usize + view.payload.len() - rest.as_ptr() as usize;
            self.left -= 1;
            let offset = h.base_offset + view.offset_delta;
            let in_range = offset >= self.frame.from_offset && offset < self.end();
            let delivery = in_range.then(|| RawDelivery {
                delivery_id: self.frame.first_delivery_id + (offset - self.frame.from_offset),
                attempt: 1,
                message: Message {
                    partition: self.frame.partition,
                    offset,
                    key: view.key.map(str::to_owned),
                    headers: headers_map(&view),
                    timestamp_ms: h.base_timestamp_ms + view.timestamp_delta,
                    payload: self.section.slice_ref(view.payload),
                },
            });
            self.sec_pos += consumed;
            if let Some(d) = delivery {
                self.yielded += 1;
                return Some(Ok(d));
            }
        }
    }
}

fn headers_map(v: &RecordView<'_>) -> BTreeMap<String, String> {
    if v.headers.is_empty() {
        BTreeMap::new()
    } else {
        v.headers_map()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotence_mark_goes_on_the_first_record_only() {
        let msgs: Vec<PublishMessage> = (0..3)
            .map(|i| PublishMessage {
                key: None,
                headers: BTreeMap::new(),
                payload: Bytes::from(format!("m{i}")),
            })
            .collect();
        let enc =
            EncodedRecords::from_messages(&msgs, Compression::Lz4, Some("00000000000000ab.12"))
                .unwrap();
        let section = match enc.section.compression {
            Compression::None => enc.section.body.to_vec(),
            c => codec::decompress(c, &enc.section.body, 1 << 20).unwrap(),
        };
        assert_eq!(batch::section_pseq(&section, 3), Some((0xab, 12)));
        let marked = Records::new(&section, 3)
            .filter(|r| {
                r.as_ref()
                    .unwrap()
                    .headers
                    .iter()
                    .any(|(k, _)| *k == fluxmq_proto::model::headers::PRODUCER_SEQ)
            })
            .count();
        assert_eq!(marked, 1);
        let plain = EncodedRecords::from_messages(&msgs, Compression::None, None).unwrap();
        assert_eq!(batch::section_pseq(&plain.section.body, 3), None);
    }

    fn msg(i: usize, key: bool) -> PublishMessage {
        PublishMessage {
            key: key.then(|| format!("k{i}")),
            headers: [("flux-type".to_string(), format!("T{i}"))].into(),
            payload: Bytes::from(vec![(i % 251) as u8; i % 300]),
        }
    }

    /// Our stamped encoding decodes with the proto reference decoder exactly like a message
    /// that carries the header in its map, and the plain path is byte-identical to proto's.
    #[test]
    fn stamped_encoding_matches_reference() {
        for i in 0..50 {
            let m = msg(i, i % 2 == 0);
            let mut ours = Vec::new();
            encode_record_marked(&mut ours, i as u32, &m, Some(("flux-producer", "p1")), None);
            let mut with = m.clone();
            with.headers.insert("flux-producer".into(), "p1".into());
            let a = Records::new(&ours, 1).next().unwrap().unwrap();
            assert_eq!(a.key, with.key.as_deref());
            assert_eq!(a.headers_map(), with.headers);
            assert_eq!(a.payload, &with.payload[..]);
            assert_eq!(a.offset_delta, i as u64);
            assert!(ours.len() <= size_hint(&m, Some(("flux-producer", "p1"))));

            let mut plain = Vec::new();
            encode_record_marked(&mut plain, i as u32, &m, None, None);
            let mut reference = Vec::new();
            batch::encode_record(
                &mut reference,
                i as u32,
                m.key.as_deref(),
                &m.headers,
                &m.payload,
            );
            assert_eq!(plain, reference);
        }
    }

    #[test]
    fn builder_roundtrips_with_every_compression() {
        let msgs: Vec<_> = (0..500).map(|i| msg(i, i % 3 == 0)).collect();
        for c in [Compression::None, Compression::Lz4, Compression::Zstd] {
            let enc = EncodedRecords::from_messages(&msgs, c, None).unwrap();
            assert_eq!(enc.section.record_count, 500);
            assert!(enc.has_keys);
            if c != Compression::None {
                assert_eq!(enc.section.compression, c, "repetitive data compresses");
            }
            assert_eq!(enc.to_messages().unwrap(), msgs);
        }
        // Incompressible data goes out uncompressed.
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let noise: Vec<u8> = (0..4096)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 24) as u8
            })
            .collect();
        let m = PublishMessage {
            payload: Bytes::from(noise),
            ..Default::default()
        };
        let enc = EncodedRecords::from_messages(&[m], Compression::Lz4, None).unwrap();
        assert_eq!(enc.section.compression, Compression::None);
        assert!(!enc.has_keys);
    }

    #[test]
    fn builder_rejects_oversized_batches() {
        let big = PublishMessage {
            payload: Bytes::from(vec![0u8; MAX_SECTION_LEN]),
            ..Default::default()
        };
        assert!(EncodedRecords::from_messages(&[big], Compression::None, None).is_err());
        let fits = PublishMessage {
            payload: Bytes::from(vec![0u8; MAX_SECTION_LEN - 16]),
            ..Default::default()
        };
        assert!(EncodedRecords::from_messages(&[fits], Compression::None, None).is_ok());
    }

    /// Envelopes of records `base .. base + n` built with the shared codec.
    fn envelope(base: u64, msgs: &[PublishMessage], c: Compression) -> Vec<u8> {
        let enc = EncodedRecords::from_messages(msgs, c, None).unwrap();
        codec::build_envelope(
            base,
            1_000,
            msgs.len() as u32,
            enc.section.compression,
            &enc.section.body,
        )
        .unwrap()
    }

    fn frame(envs: &[Vec<u8>], from: u64, count: u32) -> RecordsFrame {
        RecordsFrame {
            partition: 2,
            first_delivery_id: 500,
            from_offset: from,
            count,
            envelopes: Bytes::from(envs.concat()),
        }
    }

    fn drain(c: &mut RecordsCursor) -> Vec<Result<RawDelivery>> {
        std::iter::from_fn(|| c.next_delivery()).collect()
    }

    #[test]
    fn cursor_decodes_partial_ranges_across_envelopes() {
        let msgs: Vec<_> = (0..30).map(|i| msg(i, i % 2 == 1)).collect();
        for c in [Compression::None, Compression::Lz4, Compression::Zstd] {
            // Envelopes [100, 110), [110, 130); deliver [104, 125).
            let envs = vec![envelope(100, &msgs[..10], c), envelope(110, &msgs[10..], c)];
            let f = frame(&envs, 104, 21);
            let body_range =
                f.envelopes.as_ptr() as usize..f.envelopes.as_ptr() as usize + f.envelopes.len();
            let mut cur = RecordsCursor::new(f, true);
            let got: Vec<RawDelivery> = drain(&mut cur).into_iter().map(|r| r.unwrap()).collect();
            assert_eq!(got.len(), 21);
            for (j, d) in got.iter().enumerate() {
                let o = 104 + j as u64;
                assert_eq!(d.message.offset, o);
                assert_eq!(d.delivery_id, 500 + j as u64);
                assert_eq!(d.attempt, 1);
                assert_eq!(d.message.partition, 2);
                assert_eq!(d.message.timestamp_ms, 1_000);
                let src = &msgs[(o - 100) as usize];
                assert_eq!(d.message.key, src.key);
                assert_eq!(d.message.headers, src.headers);
                assert_eq!(d.message.payload, src.payload);
                if c == Compression::None && !d.message.payload.is_empty() {
                    assert!(body_range.contains(&(d.message.payload.as_ptr() as usize)));
                }
            }
        }
    }

    #[test]
    fn cursor_reports_corruption_and_missing_records() {
        let msgs: Vec<_> = (0..5).map(|i| msg(i, false)).collect();
        let mut env = envelope(0, &msgs, Compression::None);
        let last = env.len() - 1;
        env[last] ^= 0xff;
        let mut cur = RecordsCursor::new(frame(std::slice::from_ref(&env), 0, 5), true);
        let r = drain(&mut cur);
        assert_eq!(r.len(), 1);
        assert!(r[0].is_err(), "CRC mismatch detected");
        // Without verification the (payload) corruption goes unnoticed.
        let mut cur = RecordsCursor::new(frame(&[env], 0, 5), false);
        assert!(drain(&mut cur).iter().all(Result::is_ok));
        // Fewer records than announced.
        let good = envelope(0, &msgs, Compression::None);
        let mut cur = RecordsCursor::new(frame(&[good], 0, 7), true);
        let r = drain(&mut cur);
        assert_eq!(r.len(), 6);
        assert!(r[5].is_err());
    }
}
