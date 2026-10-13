//! Client ⇄ broker binary protocol.
//!
//! Framing: every frame is `u32 big-endian length` followed by `length` bytes of a
//! postcard-encoded [`ClientFrame`] (client → broker) or [`ServerFrame`] (broker → client).
//! Frames larger than [`MAX_FRAME_LEN`] are a protocol error and close the connection.
//!
//! Flow:
//! 1. Client sends `Hello`; broker answers `Response { HelloOk }`. Nothing else is accepted first.
//! 2. Requests carry a client-chosen `req_id`; each gets exactly one `Response` with the same id.
//!    Responses may arrive out of order relative to `Deliver` frames.
//! 3. `Subscribe` returns a `subscription_id`; the broker then pushes `Deliver` frames, keeping at
//!    most `prefetch` unacked deliveries outstanding per subscription (flow control).
//! 4. `Ack`/`Nack` are fire-and-forget (`req_id` 0 = no response wanted) or request/response.
//!
//! Delivery is at-least-once. `delivery_id` is unique per (channel, group) for the broker lifetime.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::Message;
use crate::batch::RecordsSection;
use crate::model::{ChannelConfig, MessageId, PublishMessage, StartFrom};

/// 16 MiB.
pub const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;
/// v2: cluster metadata, partition-targeted publish, `NotLeader`/`Forbidden`, `MetadataChanged`.
/// v3: batched delivery (`DeliverBatch`). A broker accepts `Hello` with version 2 or 3 and
/// sends `DeliverBatch` only to v3 clients (v2 clients keep getting `Deliver`).
/// v4 (v0.7): record batches end to end, covering `PublishBatch`, `SubscribeWith`,
/// `DeliverRecords`, `AckRanges` and `Commit` (see [`crate::batch`]). A broker accepts
/// versions 2..=4 and uses each connection's version: it never sends a v4 frame to a v2/v3
/// client and converts between forms as needed. The SDK falls back to the broker's version
/// (v0.6 behaviour).
pub const PROTOCOL_VERSION: u16 = 4;
/// Oldest client protocol version a broker still accepts.
pub const MIN_PROTOCOL_VERSION: u16 = 2;
/// Upper bounds for one `DeliverBatch` (the broker coalesces whatever is ready for the
/// subscription, within prefetch credit, when it writes — it never waits to fill a batch,
/// so batching adds no latency).
pub const MAX_DELIVER_BATCH: usize = 256;
pub const MAX_DELIVER_BATCH_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientFrame {
    pub req_id: u64,
    pub body: ClientRequest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ClientRequest {
    Hello {
        protocol_version: u16,
        /// Human readable, shown in UI (e.g. "order-api-3").
        client_name: String,
        sdk_version: String,
        auth_token: Option<String>,
    },
    CreateChannel(ChannelConfig),
    /// Returns `PublishOk` with assigned ids, in order.
    ///
    /// `partition: None` — the broker picks the partition per message (key hash / round-robin);
    /// in a cluster the receiving node forwards to partition leaders (extra hop).
    /// `partition: Some(p)` — all messages go to `p`; a node that isn't `p`'s leader answers
    /// `NotLeader` (cluster-aware clients route directly using `Metadata`).
    Publish {
        channel: String,
        partition: Option<u32>,
        messages: Vec<PublishMessage>,
    },
    Subscribe {
        channel: String,
        group: String,
        prefetch: u32,
        start: StartFrom,
    },
    Unsubscribe {
        subscription_id: u64,
    },
    Ack {
        subscription_id: u64,
        delivery_ids: Vec<u64>,
    },
    Nack {
        subscription_id: u64,
        delivery_id: u64,
        /// Shown in the DLQ inbox grouping; keep it short and stable ("PostgreSQL timeout").
        reason: String,
        /// false = dead-letter immediately, skipping remaining attempts.
        retry: bool,
    },
    /// Periodic client-side stats (processing time, CPU) for the Consumers UI.
    ReportStats(ClientStats),
    Ping,
    /// Cluster topology + partition leaders for `channels` (empty = all channels).
    Metadata {
        channels: Vec<String>,
    },
    /// v4: one producer-encoded record batch, answered with `PublishBatchOk`. Ids are
    /// contiguous: `base_offset .. base_offset + record_count` on `partition`.
    ///
    /// `partition: Some(p)`: the fast path. The broker validates the records section (and
    /// whatever channel features need: size limits, schema), wraps it in an envelope and
    /// appends it with no per-message work. The same leader rules apply as for `Publish`.
    /// `partition: None`: the broker picks one partition for the whole batch (sticky) if
    /// no record has a key. With keys, every keyed record must map to the same partition by
    /// the channel's key hash (keyless records in it follow); the batch then goes there as
    /// is. A keyed batch spanning partitions is refused with `BadRequest` ("keyed batch spans
    /// partitions; send partition: Some(p)"); batches are never split. The SDK always sends
    /// `Some` once it knows the partition count.
    ///
    /// Durability is identical to `Publish`: the answer comes after the batch is durable
    /// (fsync policy) and, in a cluster, replicated to the ISR.
    PublishBatch {
        channel: String,
        partition: Option<u32>,
        records: RecordsSection,
    },
    /// v4: `Subscribe` with options. Answered with `Subscribed`.
    SubscribeWith {
        channel: String,
        group: String,
        prefetch: u32,
        start: StartFrom,
        options: SubscribeOptions,
    },
    /// v4: ack delivery id ranges, equivalent to `Ack` with every id in the ranges. Lets a
    /// consumer ack a whole `DeliverRecords` frame in a few bytes.
    AckRanges {
        subscription_id: u64,
        ranges: Vec<DeliveryRange>,
    },
    /// v4, only for `AckMode::Cumulative` subscriptions: everything in `partition` below
    /// `offset` is processed. The group's committed offset only moves forward; a lower value
    /// is ignored. Fire-and-forget like `Ack` (`req_id` 0) or request/response.
    Commit {
        subscription_id: u64,
        partition: u32,
        offset: u64,
    },
}

/// v4 subscription options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SubscribeOptions {
    pub ack_mode: AckMode,
    /// Receive stored batches verbatim (`DeliverRecords`) when possible. The broker still
    /// sends `DeliverBatch` for redeliveries, filtered channels, queue/keyed reordering and
    /// partial ranges it can't express.
    pub raw_records: bool,
    /// Flow control in bytes in addition to `prefetch` messages (`None` = messages only).
    pub prefetch_bytes: Option<u32>,
}

/// How a consumer settles messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum AckMode {
    /// Every delivery is acked individually (`Ack` / `AckRanges` / `Nack`). Per-message
    /// redelivery, ack timeouts, nack and DLQ all apply. The default.
    #[default]
    Individual,
    /// Stream channels only: the consumer commits a position per partition (`Commit`). Nothing
    /// is redelivered unless the subscription ends (crash, rebalance), and then delivery
    /// resumes from the committed offset (at-least-once). `Nack`, ack timeouts and DLQ don't
    /// apply. Cheapest mode. `SubscribeWith` on a queue/keyed channel answers `BadRequest`.
    Cumulative,
}

/// `first .. first + count` delivery ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryRange {
    pub first: u64,
    pub count: u32,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ClientStats {
    pub subscription_id: u64,
    pub cpu_percent: Option<f32>,
    pub memory_bytes: Option<u64>,
    pub processing_p50_ms: Option<f32>,
    pub processing_p99_ms: Option<f32>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ServerFrame {
    Response {
        req_id: u64,
        result: Result<ServerResponse, WireError>,
    },
    Deliver {
        subscription_id: u64,
        delivery: Delivery,
    },
    /// Broker is going away (shutdown) or kicked the client.
    Goodbye { reason: String },
    /// Topology or partition leadership changed; clients should re-fetch `Metadata`.
    /// Subscriptions on this connection stay valid for partitions this node still leads.
    MetadataChanged { epoch: u64 },
    /// v3: several deliveries for one subscription, in delivery order. Semantically identical
    /// to the same `Deliver` frames back to back (each counts against prefetch, each is acked
    /// individually or via one `Ack { delivery_ids }`). Never empty.
    DeliverBatch {
        subscription_id: u64,
        deliveries: Vec<Delivery>,
    },
    /// v4: stored batch envelopes for one partition, verbatim (see [`crate::batch`]), for
    /// subscriptions with `raw_records`.
    ///
    /// The frame delivers exactly the records with offsets `from_offset .. from_offset +
    /// count`, all first attempts. Records of the envelopes outside that range are skipped by
    /// the client. The delivery id of offset `o` is `first_delivery_id + (o - from_offset)`.
    /// Each record counts against prefetch and is settled individually, or with `AckRanges` /
    /// `Commit`, exactly like `DeliverBatch`. `envelopes` may be compressed per batch; the
    /// client decompresses. Never empty.
    DeliverRecords {
        subscription_id: u64,
        partition: u32,
        first_delivery_id: u64,
        from_offset: u64,
        count: u32,
        envelopes: Bytes,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ServerResponse {
    HelloOk {
        broker_version: String,
        node_id: String,
        connection_id: u64,
    },
    ChannelCreated,
    PublishOk {
        ids: Vec<MessageId>,
    },
    Subscribed {
        subscription_id: u64,
    },
    Ok,
    Pong,
    /// JSON-encoded [`crate::cluster::ClusterMetadata`] (postcard can't carry its maps of
    /// optional fields robustly across versions; metadata is small and infrequent).
    Metadata {
        json: String,
    },
    /// v4: answer to `PublishBatch`, with ids `base_offset .. base_offset + count` on `partition`.
    PublishBatchOk {
        partition: u32,
        base_offset: u64,
        count: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delivery {
    pub delivery_id: u64,
    /// 1-based.
    pub attempt: u32,
    pub message: Message,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{code:?}: {message}")]
pub struct WireError {
    pub code: ErrorCode,
    pub message: String,
}

impl WireError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    BadRequest,
    Unauthorized,
    NotFound,
    AlreadyExists,
    ProtocolViolation,
    Overloaded,
    Internal,
    /// Token lacks the permission for this operation.
    Forbidden,
    /// This node doesn't lead the partition. `message` = leader node id (may be empty while
    /// an election is in progress — retry after refreshing metadata).
    NotLeader,
}

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("frame of {0} bytes exceeds limit")]
    FrameTooLarge(usize),
    #[error("decode: {0}")]
    Decode(#[from] postcard::Error),
}

/// Encodes `value` as a complete frame (length prefix included).
pub fn encode_frame<T: Serialize>(value: &T) -> Result<Bytes, CodecError> {
    let mut buf = vec![0u8; 4];
    let body = postcard::to_extend(value, Vec::new())?;
    if body.len() > MAX_FRAME_LEN {
        return Err(CodecError::FrameTooLarge(body.len()));
    }
    buf[..4].copy_from_slice(&(body.len() as u32).to_be_bytes());
    buf.extend_from_slice(&body);
    Ok(Bytes::from(buf))
}

/// Decodes a frame body (without the length prefix).
pub fn decode_body<'a, T: Deserialize<'a>>(body: &'a [u8]) -> Result<T, CodecError> {
    Ok(postcard::from_bytes(body)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip() {
        let f = ClientFrame {
            req_id: 7,
            body: ClientRequest::Publish {
                channel: "orders".into(),
                partition: None,
                messages: vec![PublishMessage {
                    key: Some("8842".into()),
                    payload: Bytes::from_static(b"{}"),
                    ..Default::default()
                }],
            },
        };
        let bytes = encode_frame(&f).unwrap();
        let len = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
        assert_eq!(len, bytes.len() - 4);
        let back: ClientFrame = decode_body(&bytes[4..]).unwrap();
        assert_eq!(back, f);
    }
}
