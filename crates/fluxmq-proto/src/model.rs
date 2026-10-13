use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Prefix of system channels (`$system.events`): created by the broker itself, read-only for
/// users (no create/update/delete/publish through the API or the wire protocol; consuming,
/// browsing and routing *from* them is allowed).
pub const SYSTEM_CHANNEL_PREFIX: char = '$';

/// Whether `name` is a system channel (`$…`).
pub fn is_system_channel(name: &str) -> bool {
    name.starts_with(SYSTEM_CHANNEL_PREFIX)
}

/// Channel names: 1–128 chars of `[a-zA-Z0-9._-]`, optionally prefixed with `$` for system
/// channels (the rest follows the same rules). User-facing create paths reject `$` names
/// separately (see [`is_system_channel`]).
pub fn validate_channel_name(name: &str) -> Result<(), String> {
    let full_len = name.len();
    let name = name.strip_prefix(SYSTEM_CHANNEL_PREFIX).unwrap_or(name);
    if name.is_empty() || full_len > 128 {
        return Err("channel name must be 1-128 characters".into());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return Err("channel name may only contain [a-zA-Z0-9._-]".into());
    }
    // These suffixes are reserved by telemetry topic names (`channel.{name}.consumers`).
    if name.ends_with(".consumers") || name.ends_with(".tail") || name.starts_with('.') {
        return Err("channel name may not start with '.' or end with '.consumers'/'.tail'".into());
    }
    Ok(())
}

/// How a channel stores and distributes messages.
///
/// | mode   | storage                     | within a consumer group            | across groups |
/// |--------|-----------------------------|------------------------------------|---------------|
/// | Stream | retained log, replayable    | partitions spread over instances   | fan-out       |
/// | Queue  | removed once acked/dead     | competing consumers, any order     | fan-out       |
/// | Keyed  | retained log, replayable    | strict order per key               | fan-out       |
/// | Priority | queue, one partition per priority level | higher level first (v0.12) | competing   |
///
/// `Priority` (v0.12) is a queue whose partitions are priority levels (2–10; partition `n` =
/// level `n`, higher = more urgent). A message's level is its [`headers::PRIORITY`] header
/// (missing → 0, clamped to the top level, see [`priority_level`]); every publish path places it
/// in that level's partition whatever its key, and consumers always take from the highest
/// level with messages (strict priority, like RabbitMQ). A batch holds one level only.
///
/// Fan-out (every group sees every message) is a property of all modes rather than
/// a separate mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelMode {
    Stream,
    Queue,
    Keyed,
    /// v0.12: a queue with priority levels (appended last: the wire encodes the variant index).
    Priority,
}

impl ChannelMode {
    pub fn as_str(self) -> &'static str {
        match self {
            ChannelMode::Stream => "stream",
            ChannelMode::Queue => "queue",
            ChannelMode::Keyed => "keyed",
            ChannelMode::Priority => "priority",
        }
    }

    /// Competing consumers (each message to one consumer of a group): `Queue` and `Priority`.
    pub fn is_queue(self) -> bool {
        matches!(self, ChannelMode::Queue | ChannelMode::Priority)
    }
}

/// Priority levels a `Priority` channel may have (its partition count).
pub const PRIORITY_LEVELS: std::ops::RangeInclusive<u32> = 2..=10;

/// The level of a message on a `Priority` channel with `levels` levels: its
/// [`headers::PRIORITY`] header (a whole number) clamped to `levels - 1`; missing or not a
/// number → 0 (the lowest).
pub fn priority_level(headers: &BTreeMap<String, String>, levels: u32) -> u32 {
    priority_level_str(headers.get(headers::PRIORITY).map(String::as_str), levels)
}

/// [`priority_level`] from the header's value.
pub fn priority_level_str(value: Option<&str>, levels: u32) -> u32 {
    value
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map_or(0, |n| n.min(u64::from(levels.max(1) - 1)) as u32)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Drop segments older than this. `None` = keep forever.
    pub max_age_secs: Option<u64>,
    /// Drop oldest segments once the channel exceeds this size. `None` = unbounded.
    pub max_bytes: Option<u64>,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_age_secs: Some(7 * 24 * 3600),
            max_bytes: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Total delivery attempts before a message is dead-lettered (>= 1).
    pub max_attempts: u32,
    /// Delay before attempt N+1 is `backoff_ms[min(N-1, len-1)]`.
    pub backoff_ms: Vec<u64>,
    /// Unacked deliveries are treated as a failed attempt after this long.
    pub ack_timeout_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            backoff_ms: vec![1_000, 10_000, 60_000, 300_000],
            ack_timeout_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelConfig {
    pub name: String,
    pub mode: ChannelMode,
    /// Number of partitions (>= 1). Queue channels may use 1.
    pub partitions: u32,
    pub retention: RetentionPolicy,
    pub retry: RetryPolicy,
    /// Messages exhausting retries go to the channel's DLQ; when false they are dropped.
    pub dlq_enabled: bool,
    /// Informational: the payload/header field producers use as key (shown in UI).
    pub ordering_key: Option<String>,
    pub created_at_ms: u64,
}

impl ChannelConfig {
    pub fn new(name: impl Into<String>, mode: ChannelMode) -> Self {
        Self {
            name: name.into(),
            mode,
            partitions: match mode {
                ChannelMode::Queue => 1,
                ChannelMode::Priority => 3,
                _ => 8,
            },
            retention: RetentionPolicy::default(),
            retry: RetryPolicy::default(),
            dlq_enabled: true,
            ordering_key: None,
            created_at_ms: crate::now_ms(),
        }
    }
}

/// Well-known header names.
pub mod headers {
    pub const PRODUCER: &str = "flux-producer";
    pub const TRACE_ID: &str = "flux-trace-id";
    pub const CONTENT_TYPE: &str = "content-type";
    pub const EVENT_TYPE: &str = "flux-type";
    /// v0.12 idempotent publishing: `<producer id, 16 hex>.<batch sequence>` on the **first**
    /// record of a `PublishBatch` (see [`crate::batch::parse_pseq`]). The broker answers a
    /// batch it has already stored with the original offsets instead of storing it again.
    pub const PRODUCER_SEQ: &str = "flux-pseq";
    /// v0.12 priority queues: the message's priority level (`0` = lowest; see
    /// [`super::priority_level`]).
    pub const PRIORITY: &str = "flux-priority";
}

/// A stored message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub partition: u32,
    pub offset: u64,
    pub key: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub timestamp_ms: u64,
    pub payload: Bytes,
}

/// What a producer sends.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PublishMessage {
    pub key: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub payload: Bytes,
}

/// Where a new consumer group starts reading (ignored if the group already exists).
///
/// Externally tagged on purpose: it travels over postcard (non self-describing), so no
/// internally/adjacently tagged serde representations. JSON: `"earliest"`, `{"timestamp_ms": 1}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartFrom {
    Earliest,
    #[default]
    Latest,
    TimestampMs(u64),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum AttemptOutcome {
    InFlight,
    Acked,
    Nacked { reason: String },
    TimedOut,
}

/// One delivery of a message to a consumer instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryAttempt {
    pub attempt: u32,
    pub consumer: String,
    pub delivered_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub outcome: AttemptOutcome,
}

/// Identifies a message within a channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MessageId {
    pub partition: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetter {
    /// Unique within the channel.
    pub id: u64,
    pub channel: String,
    pub group: String,
    pub message: Message,
    pub attempts: Vec<DeliveryAttempt>,
    /// Last failure reason ("ack timeout" for timeouts, "unknown" if consumer gave none).
    pub reason: String,
    pub dead_at_ms: u64,
}

#[cfg(test)]
mod system_channel_tests {
    use super::*;

    #[test]
    fn system_channel_names() {
        assert!(validate_channel_name("$system.events").is_ok());
        assert!(is_system_channel("$system.events"));
        assert!(!is_system_channel("orders"));
        assert!(validate_channel_name("$").is_err());
        assert!(validate_channel_name("$$x").is_err());
        assert!(validate_channel_name("$.x").is_err());
        assert!(validate_channel_name("$x.consumers").is_err());
        assert!(validate_channel_name("$x.tail").is_err());
        assert!(validate_channel_name("a$b").is_err());
        assert!(validate_channel_name(&format!("${}", "a".repeat(127))).is_ok());
        assert!(validate_channel_name(&format!("${}", "a".repeat(128))).is_err());
        assert!(validate_channel_name(&"a".repeat(128)).is_ok());
    }
}
