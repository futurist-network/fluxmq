//! Wire codec: `u32` big-endian length prefix + postcard body (see `fluxmq_proto::wire`).
//!
//! Generic over direction so tests (and tools) can speak the client side of the protocol
//! with the same implementation: [`ServerCodec`] decodes `ClientFrame`s and encodes
//! `ServerFrame`s; [`ClientCodec`] is the mirror image.

use std::collections::BTreeMap;
use std::marker::PhantomData;

use bytes::{BufMut, Bytes, BytesMut};
use fluxmq_core::SharedDelivery;
use fluxmq_proto::PublishMessage;
use fluxmq_proto::batch::{Compression, RecordsSection};
use fluxmq_proto::wire::{self, ClientFrame, ClientRequest, MAX_FRAME_LEN, ServerFrame};
use serde::ser::{SerializeStruct, SerializeStructVariant};
use serde::{Deserialize, Serialize, Serializer};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

/// Codec errors. Any of them is a protocol error that closes the connection.
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Codec(#[from] wire::CodecError),
}

/// Decodes `In` frames, encodes `Out` frames.
pub struct FrameCodec<In, Out> {
    inner: LengthDelimitedCodec,
    _marker: PhantomData<fn(Out) -> In>,
}

/// Broker side: reads [`ClientFrame`], writes [`ServerFrame`].
pub type ServerCodec = FrameCodec<ClientFrame, ServerFrame>;
/// Broker side for client connections: reads [`ClientFrame`], writes [`Outbound`].
pub type ServerOutCodec = FrameCodec<ClientFrame, Outbound>;
/// Client side: reads [`ServerFrame`], writes [`ClientFrame`].
pub type ClientCodec = FrameCodec<ServerFrame, ClientFrame>;

impl<In, Out> FrameCodec<In, Out> {
    pub fn new() -> Self {
        let inner = LengthDelimitedCodec::builder()
            .length_field_type::<u32>()
            .big_endian()
            .max_frame_length(MAX_FRAME_LEN)
            .new_codec();
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<In, Out> Default for FrameCodec<In, Out> {
    fn default() -> Self {
        Self::new()
    }
}

/// How a frame type is decoded from its body.
pub trait FrameDecode: Sized {
    fn decode_frame(body: Bytes) -> Result<Self, wire::CodecError>;
}

impl FrameDecode for ServerFrame {
    fn decode_frame(body: Bytes) -> Result<Self, wire::CodecError> {
        wire::decode_body(&body)
    }
}

/// postcard variant indices of [`ClientRequest::Publish`] / [`ClientRequest::PublishBatch`]
/// (checked against the reference decoder by the tests).
const PUBLISH_VARIANT: u8 = 2;
const PUBLISH_BATCH_VARIANT: u8 = 10;

/// Borrowing mirror of [`RecordsSection`] (same fields, same order): the body stays a slice
/// of the frame.
#[derive(Deserialize)]
struct RecordsSectionRef<'a> {
    compression: Compression,
    record_count: u32,
    #[serde(borrow)]
    body: &'a [u8],
}

/// Fields of `ClientRequest::PublishBatch`, in order.
#[derive(Deserialize)]
struct PublishBatchRef<'a> {
    channel: String,
    partition: Option<u32>,
    #[serde(borrow)]
    records: RecordsSectionRef<'a>,
}

/// Borrowing mirror of [`PublishMessage`] (same fields, same order, so the same bytes); the
/// payload stays a slice of the frame.
#[derive(Deserialize)]
struct PublishMessageRef<'a> {
    key: Option<String>,
    headers: BTreeMap<String, String>,
    #[serde(borrow)]
    payload: &'a [u8],
}

/// Fields of `ClientRequest::Publish`, in order.
#[derive(Deserialize)]
struct PublishRef<'a> {
    channel: String,
    partition: Option<u32>,
    #[serde(borrow)]
    messages: Vec<PublishMessageRef<'a>>,
}

impl FrameDecode for ClientFrame {
    /// `Publish` frames are decoded without copying payloads (each message's payload is a
    /// slice of the frame buffer), `PublishBatch` without copying its records section.
    /// Everything else goes through the reference decoder.
    fn decode_frame(body: Bytes) -> Result<Self, wire::CodecError> {
        // `ClientFrame` = varint(req_id) ++ ClientRequest; a u64 varint is at most 10 bytes.
        let id_len = body
            .iter()
            .take(10)
            .position(|b| b & 0x80 == 0)
            .map(|i| i + 1);
        if let Some(n) = id_len
            && body.get(n) == Some(&PUBLISH_BATCH_VARIANT)
        {
            let req_id: u64 = postcard::from_bytes(&body[..n])?;
            let p: PublishBatchRef<'_> = postcard::from_bytes(&body[n + 1..])?;
            return Ok(ClientFrame {
                req_id,
                body: ClientRequest::PublishBatch {
                    channel: p.channel,
                    partition: p.partition,
                    records: RecordsSection {
                        compression: p.records.compression,
                        record_count: p.records.record_count,
                        body: body.slice_ref(p.records.body),
                    },
                },
            });
        }
        if let Some(n) = id_len
            && body.get(n) == Some(&PUBLISH_VARIANT)
        {
            let req_id: u64 = postcard::from_bytes(&body[..n])?;
            let p: PublishRef<'_> = postcard::from_bytes(&body[n + 1..])?;
            let messages = p
                .messages
                .into_iter()
                .map(|m| PublishMessage {
                    key: m.key,
                    headers: m.headers,
                    payload: body.slice_ref(m.payload),
                })
                .collect();
            return Ok(ClientFrame {
                req_id,
                body: ClientRequest::Publish {
                    channel: p.channel,
                    partition: p.partition,
                    messages,
                },
            });
        }
        wire::decode_body(&body)
    }
}

impl<In: FrameDecode, Out> Decoder for FrameCodec<In, Out> {
    type Item = In;
    type Error = FrameError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<In>, FrameError> {
        match self.inner.decode(src)? {
            Some(body) => Ok(Some(In::decode_frame(body.freeze())?)),
            None => Ok(None),
        }
    }
}

/// What a client connection's writer sends: a [`ServerFrame`], or deliveries of one
/// subscription encoded straight from the group's shared messages (no deep copy). One
/// delivery goes out as `ServerFrame::Deliver`, several as `ServerFrame::DeliverBatch` —
/// byte-identical to encoding those frames.
#[derive(Debug)]
pub enum Outbound {
    Frame(ServerFrame),
    Deliveries {
        subscription_id: u64,
        /// Never empty.
        deliveries: Vec<SharedDelivery>,
    },
}

impl From<ServerFrame> for Outbound {
    fn from(f: ServerFrame) -> Self {
        Outbound::Frame(f)
    }
}

impl Outbound {
    pub fn is_goodbye(&self) -> bool {
        matches!(self, Outbound::Frame(ServerFrame::Goodbye { .. }))
    }
}

/// postcard variant indices of `ServerFrame::Deliver` / `ServerFrame::DeliverBatch`
/// (checked against the reference encoder by the tests).
const DELIVER_VARIANT: u32 = 1;
const DELIVER_BATCH_VARIANT: u32 = 4;

/// Serializes like [`wire::Delivery`].
struct DeliveryOut<'a>(&'a SharedDelivery);

impl Serialize for DeliveryOut<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut st = s.serialize_struct("Delivery", 3)?;
        st.serialize_field("delivery_id", &self.0.delivery_id)?;
        st.serialize_field("attempt", &self.0.attempt)?;
        st.serialize_field("message", &*self.0.message)?;
        st.end()
    }
}

struct DeliveriesOut<'a>(&'a [SharedDelivery]);

impl Serialize for DeliveriesOut<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(self.0.iter().map(DeliveryOut))
    }
}

impl Serialize for Outbound {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Outbound::Frame(f) => f.serialize(s),
            Outbound::Deliveries {
                subscription_id,
                deliveries,
            } if deliveries.len() == 1 => {
                let mut v =
                    s.serialize_struct_variant("ServerFrame", DELIVER_VARIANT, "Deliver", 2)?;
                v.serialize_field("subscription_id", subscription_id)?;
                v.serialize_field("delivery", &DeliveryOut(&deliveries[0]))?;
                v.end()
            }
            Outbound::Deliveries {
                subscription_id,
                deliveries,
            } => {
                let mut v = s.serialize_struct_variant(
                    "ServerFrame",
                    DELIVER_BATCH_VARIANT,
                    "DeliverBatch",
                    2,
                )?;
                v.serialize_field("subscription_id", subscription_id)?;
                v.serialize_field("deliveries", &DeliveriesOut(deliveries))?;
                v.end()
            }
        }
    }
}

impl<In, Out: Serialize> Encoder<Out> for FrameCodec<In, Out> {
    type Error = FrameError;

    /// Serializes straight into the write buffer behind a placeholder length prefix that is
    /// patched afterwards: no intermediate allocation and no second copy of the body (which
    /// matters for `DeliverBatch` frames carrying up to `MAX_DELIVER_BATCH_BYTES`).
    fn encode(&mut self, item: Out, dst: &mut BytesMut) -> Result<(), FrameError> {
        let start = dst.len();
        dst.put_u32(0);
        if let Err(e) = postcard::serialize_with_flavor(&item, BufFlavor(dst)) {
            dst.truncate(start);
            return Err(FrameError::Codec(wire::CodecError::Decode(e)));
        }
        let len = dst.len() - start - 4;
        if len > MAX_FRAME_LEN {
            dst.truncate(start);
            return Err(FrameError::Codec(wire::CodecError::FrameTooLarge(len)));
        }
        dst[start..start + 4].copy_from_slice(&(len as u32).to_be_bytes());
        Ok(())
    }
}

/// postcard variant index of `ServerFrame::DeliverRecords` (checked by the tests).
const DELIVER_RECORDS_VARIANT: u32 = 5;
/// Envelope buffers at least this long go to the socket as their own slice (no copy);
/// shorter ones are copied next to the frame header (an extra iovec costs more).
const EXT_MIN_ENVELOPES: usize = 256;
/// Same for message payloads of `Deliver`/`DeliverBatch` frames: a payload slice per message
/// only pays off for large payloads (one `writev` takes at most [`MAX_IOV`] slices).
const EXT_MIN_PAYLOAD: usize = 2048;
/// Fresh scratch allocations for frame headers and small frames (headers of thousands of
/// `DeliverRecords` frames fit in one).
const SCRATCH_BYTES: usize = 64 * 1024;
/// Slices per vectored write.
pub const MAX_IOV: usize = 128;

/// One piece of an encoded frame: bytes in the scratch buffer, or a shared buffer that goes
/// to the socket as it is.
enum Piece {
    Own(usize),
    Ext(Bytes),
}

/// Zero-copy encoder of the broker's outgoing frames.
///
/// Every frame is byte-identical to `wire::encode_frame` of the equivalent [`ServerFrame`]
/// (the tests check it), but stored envelopes (`DeliverRecords`) and large payloads are not
/// copied: the frame becomes a list of [`Bytes`] chunks, the frame header and small fields
/// in a scratch buffer (allocated in [`SCRATCH_BYTES`] blocks, never grown by copying) and
/// the envelope/payload buffers shared with the store, which the writer hands to the socket
/// with vectored writes.
pub struct OutboundEncoder {
    scratch: BytesMut,
    pieces: Vec<Piece>,
}

impl Default for OutboundEncoder {
    fn default() -> Self {
        Self::new()
    }
}

fn put_varint(buf: &mut BytesMut, mut v: u64) {
    while v >= 0x80 {
        buf.put_u8((v as u8) | 0x80);
        v >>= 7;
    }
    buf.put_u8(v as u8);
}

impl OutboundEncoder {
    pub fn new() -> Self {
        Self {
            scratch: BytesMut::new(),
            pieces: Vec::new(),
        }
    }

    /// Makes room for at least `n` more scratch bytes. The scratch is empty between frames
    /// (everything is split off), so this never copies.
    fn reserve(&mut self, n: usize) {
        if self.scratch.capacity() - self.scratch.len() < n {
            self.scratch.reserve(n.max(SCRATCH_BYTES));
        }
    }

    /// Ends the scratch piece written so far and appends `b` (or copies `b` if short).
    fn ext(&mut self, b: &Bytes, min: usize, own_start: &mut usize) {
        if b.len() < min {
            self.scratch.extend_from_slice(b);
            return;
        }
        let n = self.scratch.len() - *own_start;
        if n > 0 {
            self.pieces.push(Piece::Own(n));
        }
        self.pieces.push(Piece::Ext(b.clone()));
        *own_start = self.scratch.len();
    }

    /// Appends the encoded `item` to `out` as one or more chunks; returns its total length.
    pub fn encode(
        &mut self,
        item: Outbound,
        out: &mut std::collections::VecDeque<Bytes>,
    ) -> Result<usize, FrameError> {
        debug_assert!(self.scratch.is_empty() && self.pieces.is_empty());
        let r = self.encode_pieces(&item);
        if let Err(e) = r {
            self.scratch.clear();
            self.pieces.clear();
            return Err(e);
        }
        let mut total = 0;
        for p in self.pieces.drain(..) {
            match p {
                Piece::Own(n) => {
                    total += n;
                    out.push_back(self.scratch.split_to(n).freeze());
                }
                Piece::Ext(b) => {
                    total += b.len();
                    out.push_back(b);
                }
            }
        }
        debug_assert!(self.scratch.is_empty());
        Ok(total)
    }

    /// Writes the frame into scratch + `pieces` and patches its length prefix.
    fn encode_pieces(&mut self, item: &Outbound) -> Result<(), FrameError> {
        let mut own_start = 0usize;
        match item {
            Outbound::Frame(ServerFrame::DeliverRecords {
                subscription_id,
                partition,
                first_delivery_id,
                from_offset,
                count,
                envelopes,
            }) => {
                self.reserve(4 + 6 * 10);
                self.scratch.put_u32(0);
                put_varint(&mut self.scratch, u64::from(DELIVER_RECORDS_VARIANT));
                put_varint(&mut self.scratch, *subscription_id);
                put_varint(&mut self.scratch, u64::from(*partition));
                put_varint(&mut self.scratch, *first_delivery_id);
                put_varint(&mut self.scratch, *from_offset);
                put_varint(&mut self.scratch, u64::from(*count));
                put_varint(&mut self.scratch, envelopes.len() as u64);
                if envelopes.len() < EXT_MIN_ENVELOPES {
                    self.reserve(envelopes.len());
                }
                self.ext(envelopes, EXT_MIN_ENVELOPES, &mut own_start);
            }
            Outbound::Deliveries {
                subscription_id,
                deliveries,
            } => {
                // Pre-size for everything copied (large payloads go out as their own slices).
                let est: usize = deliveries
                    .iter()
                    .map(|d| {
                        let m = &d.message;
                        let p = if m.payload.len() >= EXT_MIN_PAYLOAD {
                            0
                        } else {
                            m.payload.len()
                        };
                        delivery_overhead(m) + p
                    })
                    .sum();
                self.reserve(4 + 30 + est);
                self.scratch.put_u32(0);
                if deliveries.len() == 1 {
                    put_varint(&mut self.scratch, u64::from(DELIVER_VARIANT));
                    put_varint(&mut self.scratch, *subscription_id);
                } else {
                    put_varint(&mut self.scratch, u64::from(DELIVER_BATCH_VARIANT));
                    put_varint(&mut self.scratch, *subscription_id);
                    put_varint(&mut self.scratch, deliveries.len() as u64);
                }
                for d in deliveries {
                    let m = &*d.message;
                    put_varint(&mut self.scratch, d.delivery_id);
                    put_varint(&mut self.scratch, u64::from(d.attempt));
                    // `Message` fields before `payload`, serialized like the struct (postcard
                    // structs and tuples are both the plain concatenation of their fields).
                    postcard::serialize_with_flavor(
                        &(m.partition, m.offset, &m.key, &m.headers, m.timestamp_ms),
                        BufFlavor(&mut self.scratch),
                    )
                    .map_err(|e| FrameError::Codec(wire::CodecError::Decode(e)))?;
                    put_varint(&mut self.scratch, m.payload.len() as u64);
                    self.ext(&m.payload, EXT_MIN_PAYLOAD, &mut own_start);
                }
            }
            Outbound::Frame(f) => {
                self.reserve(256);
                self.scratch.put_u32(0);
                postcard::serialize_with_flavor(f, BufFlavor(&mut self.scratch))
                    .map_err(|e| FrameError::Codec(wire::CodecError::Decode(e)))?;
            }
        }
        let tail = self.scratch.len() - own_start;
        if tail > 0 {
            self.pieces.push(Piece::Own(tail));
        }
        let total: usize = self
            .pieces
            .iter()
            .map(|p| match p {
                Piece::Own(n) => *n,
                Piece::Ext(b) => b.len(),
            })
            .sum();
        let len = total - 4;
        if len > MAX_FRAME_LEN {
            return Err(FrameError::Codec(wire::CodecError::FrameTooLarge(len)));
        }
        self.scratch[..4].copy_from_slice(&(len as u32).to_be_bytes());
        Ok(())
    }
}

/// Encoded size of one delivery without its payload bytes (over-estimate is fine: it only
/// pre-sizes the scratch buffer).
fn delivery_overhead(m: &fluxmq_proto::Message) -> usize {
    let key = m.key.as_ref().map_or(1, |k| k.len() + 6);
    let headers: usize = m.headers.iter().map(|(k, v)| k.len() + v.len() + 10).sum();
    48 + key + headers
}

/// postcard output flavor appending to a `BytesMut`.
struct BufFlavor<'a>(&'a mut BytesMut);

impl postcard::ser_flavors::Flavor for BufFlavor<'_> {
    type Output = ();

    fn try_push(&mut self, data: u8) -> postcard::Result<()> {
        self.0.put_u8(data);
        Ok(())
    }

    fn try_extend(&mut self, data: &[u8]) -> postcard::Result<()> {
        self.0.extend_from_slice(data);
        Ok(())
    }

    fn finalize(self) -> postcard::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use fluxmq_proto::wire::{ClientRequest, ServerResponse};

    use super::*;

    #[test]
    fn client_to_server_roundtrip_matches_proto_framing() {
        let frame = ClientFrame {
            req_id: 9,
            body: ClientRequest::Ping,
        };
        let mut buf = BytesMut::new();
        ClientCodec::new().encode(frame.clone(), &mut buf).unwrap();
        // Byte-identical to the reference encoder in fluxmq-proto.
        assert_eq!(&buf[..], &wire::encode_frame(&frame).unwrap()[..]);
        let decoded = ServerCodec::new().decode(&mut buf).unwrap();
        assert_eq!(decoded, Some(frame));
        assert!(buf.is_empty());
    }

    #[test]
    fn server_frames_match_proto_framing_and_roundtrip() {
        let delivery = |id: u64| fluxmq_proto::wire::Delivery {
            delivery_id: id,
            attempt: 1,
            message: fluxmq_proto::Message {
                partition: 2,
                offset: id,
                key: Some("k".into()),
                headers: [("h".to_string(), "v".to_string())].into(),
                timestamp_ms: 1,
                payload: bytes::Bytes::from(vec![7u8; 300]),
            },
        };
        let frames = [
            ServerFrame::Deliver {
                subscription_id: 3,
                delivery: delivery(1),
            },
            ServerFrame::DeliverBatch {
                subscription_id: 3,
                deliveries: (1..=50).map(delivery).collect(),
            },
        ];
        let mut buf = BytesMut::new();
        let mut enc = ServerCodec::new();
        for f in &frames {
            let before = buf.len();
            enc.encode(f.clone(), &mut buf).unwrap();
            assert_eq!(&buf[before..], &wire::encode_frame(f).unwrap()[..]);
        }
        let mut dec = ClientCodec::new();
        for f in &frames {
            assert_eq!(dec.decode(&mut buf).unwrap().as_ref(), Some(f));
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn partial_frames_wait_for_more_bytes() {
        let frame = ServerFrame::Response {
            req_id: 1,
            result: Ok(ServerResponse::Pong),
        };
        let bytes = wire::encode_frame(&frame).unwrap();
        let mut codec = ClientCodec::new();
        let mut buf = BytesMut::from(&bytes[..3]);
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        buf.extend_from_slice(&bytes[3..]);
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(frame));
    }

    #[test]
    fn oversized_and_garbage_frames_are_errors() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&((MAX_FRAME_LEN as u32) + 1).to_be_bytes());
        assert!(ServerCodec::new().decode(&mut buf).is_err());

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&3u32.to_be_bytes());
        buf.extend_from_slice(&[0xff, 0xff, 0xff]);
        assert!(ServerCodec::new().decode(&mut buf).is_err());
    }

    fn shared(id: u64, n: usize) -> SharedDelivery {
        SharedDelivery {
            delivery_id: id,
            attempt: 1 + (id % 3) as u32,
            message: std::sync::Arc::new(fluxmq_proto::Message {
                partition: 2,
                offset: id,
                key: id.is_multiple_of(2).then(|| format!("k{id}")),
                headers: [("h".to_string(), "v".to_string())].into(),
                timestamp_ms: 1,
                payload: Bytes::from(vec![id as u8; n]),
            }),
        }
    }

    /// `Outbound::Deliveries` must be byte-identical to the equivalent `ServerFrame`.
    #[test]
    fn outbound_deliveries_encode_like_server_frames() {
        let mut enc = ServerOutCodec::new();
        for n in [1usize, 2, 50] {
            let ds: Vec<SharedDelivery> = (0..n as u64).map(|i| shared(i + 300, 100)).collect();
            let reference = if n == 1 {
                ServerFrame::Deliver {
                    subscription_id: 77,
                    delivery: ds[0].clone().into_wire(),
                }
            } else {
                ServerFrame::DeliverBatch {
                    subscription_id: 77,
                    deliveries: ds.iter().cloned().map(SharedDelivery::into_wire).collect(),
                }
            };
            let mut buf = BytesMut::new();
            enc.encode(
                Outbound::Deliveries {
                    subscription_id: 77,
                    deliveries: ds,
                },
                &mut buf,
            )
            .unwrap();
            assert_eq!(
                &buf[..],
                &wire::encode_frame(&reference).unwrap()[..],
                "n={n}"
            );
            assert_eq!(
                ClientCodec::new().decode(&mut buf).unwrap(),
                Some(reference)
            );
        }
        let mut buf = BytesMut::new();
        let f = ServerFrame::Goodbye { reason: "x".into() };
        enc.encode(Outbound::from(f.clone()), &mut buf).unwrap();
        assert_eq!(&buf[..], &wire::encode_frame(&f).unwrap()[..]);
    }

    /// The zero-copy `PublishBatch` decoder agrees with the reference decoder and the records
    /// section points into the frame buffer.
    #[test]
    fn publish_batch_decodes_without_copy_like_reference() {
        for (req_id, compression, len) in [
            (0u64, Compression::None, 0usize),
            (1, Compression::Lz4, 10),
            (300, Compression::Zstd, 100_000),
            (u64::MAX, Compression::None, 1),
        ] {
            let frame = ClientFrame {
                req_id,
                body: ClientRequest::PublishBatch {
                    channel: "orders".into(),
                    partition: (req_id % 2 == 0).then_some(4),
                    records: RecordsSection {
                        compression,
                        record_count: 77,
                        body: Bytes::from(vec![9u8; len]),
                    },
                },
            };
            let bytes = wire::encode_frame(&frame).unwrap();
            let body = bytes.slice(4..);
            let ours = ClientFrame::decode_frame(body.clone()).unwrap();
            assert_eq!(ours, frame);
            assert_eq!(ours, wire::decode_body::<ClientFrame>(&body).unwrap());
            let ClientRequest::PublishBatch { records, .. } = &ours.body else {
                panic!("publish batch expected")
            };
            if len > 0 {
                let range = body.as_ptr() as usize..body.as_ptr() as usize + body.len();
                assert!(range.contains(&(records.body.as_ptr() as usize)));
            }
            assert!(ClientFrame::decode_frame(body.slice(..body.len() - 1)).is_err() || len == 0);
        }
    }

    /// The zero-copy `Publish` decoder agrees with the reference decoder, and payloads point
    /// into the frame buffer.
    #[test]
    fn publish_decodes_without_copy_like_reference() {
        for req_id in [0u64, 1, 127, 128, 70_000, u64::MAX] {
            let frame = ClientFrame {
                req_id,
                body: ClientRequest::Publish {
                    channel: "orders".into(),
                    partition: if req_id.is_multiple_of(2) {
                        Some(3)
                    } else {
                        None
                    },
                    messages: (0..20)
                        .map(|i: usize| PublishMessage {
                            key: i.is_multiple_of(3).then(|| format!("k{i}")),
                            headers: if i.is_multiple_of(2) {
                                [("flux-producer".to_string(), "p".to_string())].into()
                            } else {
                                Default::default()
                            },
                            payload: Bytes::from(vec![i as u8; i * 10]),
                        })
                        .collect(),
                },
            };
            let bytes = wire::encode_frame(&frame).unwrap();
            let body = bytes.slice(4..);
            let ours = ClientFrame::decode_frame(body.clone()).unwrap();
            assert_eq!(ours, frame);
            let reference: ClientFrame = wire::decode_body(&body).unwrap();
            assert_eq!(ours, reference);
            let ClientRequest::Publish { messages, .. } = &ours.body else {
                panic!("publish expected")
            };
            let range = body.as_ptr() as usize..body.as_ptr() as usize + body.len();
            for m in messages.iter().filter(|m| !m.payload.is_empty()) {
                assert!(range.contains(&(m.payload.as_ptr() as usize)));
            }
        }
        // Other requests still decode (variant 2 at the position of req_id's last byte is
        // not mistaken for Publish).
        for req_id in [2u64, 0x82] {
            let f = ClientFrame {
                req_id,
                body: ClientRequest::Ping,
            };
            let b = wire::encode_frame(&f).unwrap();
            assert_eq!(ClientFrame::decode_frame(b.slice(4..)).unwrap(), f);
        }
        // Truncated publish is an error, not a panic.
        let f = ClientFrame {
            req_id: 5,
            body: ClientRequest::Publish {
                channel: "c".into(),
                partition: None,
                messages: vec![PublishMessage {
                    payload: Bytes::from_static(b"abcdef"),
                    ..Default::default()
                }],
            },
        };
        let b = wire::encode_frame(&f).unwrap();
        assert!(ClientFrame::decode_frame(b.slice(4..b.len() - 2)).is_err());
    }
}
