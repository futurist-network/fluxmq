//! REST request/response bodies (JSON). Routes are documented in docs/CONTRACT.md.
//!
//! Payloads cross JSON as [`JsonPayload`]: UTF-8 text when valid, otherwise base64.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::model::{ChannelMode, DeliveryAttempt, RetentionPolicy, RetryPolicy, StartFrom};

/// Uniform error body for every non-2xx response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApiError {
    /// Machine-readable code: `bad_request`, `unauthorized`, `forbidden`, `not_found`,
    /// `conflict`, `unavailable`, `internal`, and (v0.3) `unsupported_in_cluster` — the
    /// operation is refused because this node is part of a cluster (see
    /// [`ERROR_UNSUPPORTED_IN_CLUSTER`]). Clients branch on this field, never on `message`.
    pub error: String,
    pub message: String,
    /// Byte offset of a filter DSL syntax error in the submitted text (`filter=` on message
    /// search, `POST /api/filter/parse`). Absent for every other error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<usize>,
}

/// [`ApiError::error`] of an operation refused because the node is clustered (HTTP 400).
pub const ERROR_UNSUPPORTED_IN_CLUSTER: &str = "unsupported_in_cluster";
/// `ApiError.error` for cluster-only operations on a single node (HTTP 400), e.g. rebalancing.
pub const ERROR_UNSUPPORTED_SINGLE_NODE: &str = "unsupported_single_node";

/// Short, human error classes used by outbound HTTP deliveries (notifications, webhooks,
/// backup target probes): `error` fields carry one of these (or `HTTP <status>`), the full
/// cause chain goes to the matching `*_detail` field.
pub mod short_error {
    pub const TIMEOUT: &str = "timeout";
    pub const CONNECTION_REFUSED: &str = "connection refused";
    /// Any other connect-phase failure (reset, unreachable, …).
    pub const CONNECTION_FAILED: &str = "connection failed";
    pub const TLS: &str = "TLS error";
    pub const DNS: &str = "DNS error";
    /// The target is blocked by the SSRF policy (webhooks; never retried).
    pub const BLOCKED: &str = "blocked by policy";
    pub const REQUEST_FAILED: &str = "request failed";

    /// Classifies a transport error from its flags and full cause chain (lower-cased
    /// matching). Pure function so every crate maps errors the same way.
    pub fn classify(chain: &str, is_timeout: bool, is_connect: bool) -> &'static str {
        let c = chain.to_ascii_lowercase();
        if c.contains("private targets are not allowed") || c.contains("blocked address") {
            BLOCKED
        } else if is_timeout || c.contains("timed out") || c.contains("deadline has elapsed") {
            TIMEOUT
        } else if c.contains("dns error")
            || c.contains("failed to lookup address")
            || c.contains("nodename nor servname")
            || c.contains("name or service not known")
            || c.contains("no such host")
            || c.contains("cannot resolve")
        {
            DNS
        } else if c.contains("certificate")
            || c.contains("tls")
            || c.contains("ssl")
            || c.contains("handshake")
        {
            TLS
        } else if c.contains("connection refused") {
            CONNECTION_REFUSED
        } else if is_connect
            || c.contains("connection reset")
            || c.contains("unreachable")
            || c.contains("broken pipe")
        {
            CONNECTION_FAILED
        } else {
            REQUEST_FAILED
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn classes() {
            assert_eq!(classify("x", true, true), TIMEOUT);
            assert_eq!(
                classify(
                    "error sending request: tcp connect error: Connection refused (os error 61)",
                    false,
                    true
                ),
                CONNECTION_REFUSED
            );
            assert_eq!(
                classify(
                    "dns error: failed to lookup address information",
                    false,
                    true
                ),
                DNS
            );
            assert_eq!(
                classify("invalid peer certificate: UnknownIssuer", false, true),
                TLS
            );
            assert_eq!(
                classify(
                    "'x' resolves only to blocked addresses (private targets are not allowed)",
                    false,
                    true
                ),
                BLOCKED
            );
            assert_eq!(
                classify("connection reset by peer", false, false),
                CONNECTION_FAILED
            );
            assert_eq!(classify("body error", false, false), REQUEST_FAILED);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "encoding", content = "data", rename_all = "snake_case")]
pub enum JsonPayload {
    Utf8(String),
    Base64(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateChannelRequest {
    pub name: String,
    pub mode: ChannelMode,
    /// Defaults: 8 for stream/keyed, 1 for queue.
    pub partitions: Option<u32>,
    pub retention: Option<RetentionPolicy>,
    pub retry: Option<RetryPolicy>,
    pub dlq_enabled: Option<bool>,
    pub ordering_key: Option<String>,
}

/// `PATCH /api/channels/{name}` — only retention, retry and dlq are mutable.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct UpdateChannelRequest {
    pub retention: Option<RetentionPolicy>,
    pub retry: Option<RetryPolicy>,
    pub dlq_enabled: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublishRequest {
    pub messages: Vec<PublishItem>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublishItem {
    pub key: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    pub payload: JsonPayload,
    /// v0.12: deliver at this unix time (ms) instead of now (see [`crate::scheduled`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deliver_at_ms: Option<u64>,
    /// v0.12: deliver this many milliseconds from now (broker clock). Exclusive with
    /// `deliver_at_ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delay_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublishResponse {
    pub ids: Vec<crate::model::MessageId>,
}

/// `GET /api/channels/{name}/messages` query string.
///
/// Precedence: if `offset` is set, read forward from (`partition`, `offset`); else if
/// `newest_first` is true, the newest `limit` matches within [`since_ms`, `until_ms`]; else if
/// `since_ms` is set, read forward from that time on all (or the given) partitions; else
/// read the newest `limit` messages (before `until_ms` if given). `key` filters by exact key.
/// Bounds are inclusive. Results are ordered by timestamp ascending.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct MessageQuery {
    pub partition: Option<u32>,
    pub offset: Option<u64>,
    pub key: Option<String>,
    pub since_ms: Option<u64>,
    pub until_ms: Option<u64>,
    /// Default 100, max 1000.
    pub limit: Option<u32>,
    /// Without `offset`: return the newest `limit` matches in [`since_ms`, `until_ms`] instead
    /// of the oldest (still sorted ascending; `has_more` = older matches exist). "Last 15 min".
    #[serde(default)]
    pub newest_first: Option<bool>,
    /// Filter DSL (see [`crate::routing`]), e.g. `order_id = 8842 AND country = "MD"`.
    /// Scan-based (no payload index): bounded by the same scan cap; `scanned` reports cost.
    #[serde(default)]
    pub filter: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageView {
    pub partition: u32,
    pub offset: u64,
    pub key: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub timestamp_ms: u64,
    pub size: u32,
    pub payload: JsonPayload,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessagePage {
    pub messages: Vec<MessageView>,
    /// Messages scanned to produce this page (search cost hint for the UI).
    pub scanned: u64,
    /// True if the scan stopped at a limit before exhausting the range.
    pub has_more: bool,
}

/// `GET /api/channels/{name}/messages/{partition}/{offset}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageDetail {
    pub message: MessageView,
    /// Delivery history per consumer group (retry timeline).
    pub deliveries: BTreeMap<String, Vec<DeliveryAttempt>>,
}

/// `POST /api/channels/{name}/replay` — re-publish messages (optionally into another channel).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayRequest {
    pub ids: Vec<crate::model::MessageId>,
    /// Defaults to the same channel.
    pub target_channel: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeadLetterView {
    pub id: u64,
    pub channel: String,
    pub group: String,
    pub reason: String,
    pub dead_at_ms: u64,
    pub message: MessageView,
    pub attempts: Vec<DeliveryAttempt>,
}

/// `GET /api/dlq/summary` — the error inbox grouping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DlqSummary {
    pub total: u64,
    pub groups: Vec<DlqReasonGroup>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DlqReasonGroup {
    pub reason: String,
    pub count: u64,
    pub channels: BTreeMap<String, u64>,
    pub first_ms: u64,
    pub last_ms: u64,
}

/// `GET /api/dlq` query.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct DlqQuery {
    pub channel: Option<String>,
    pub reason: Option<String>,
    pub limit: Option<u32>,
    /// Return entries with id < before (pagination, newest first).
    pub before: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DlqPage {
    pub entries: Vec<DeadLetterView>,
    pub total: u64,
}

/// `POST /api/dlq/retry` and `POST /api/dlq/discard`. Exactly one selector must be set.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct DlqActionRequest {
    /// `(channel, id)` pairs.
    pub ids: Option<Vec<(String, u64)>>,
    /// All entries with this reason (optionally restricted to `channel`).
    pub reason: Option<String>,
    pub channel: Option<String>,
    pub all: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DlqActionResponse {
    pub affected: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// `POST /api/channels/{name}/groups/{group}/reset` body (group comes from the path).
pub struct ResetOffsetsRequest {
    pub to: StartFrom,
}

/// `GET /api/search?q=` — command palette backing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchResult {
    pub kind: String,
    pub label: String,
    pub link: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub ts: u64,
    pub actor: String,
    pub action: String,
    pub target: String,
    pub detail: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub node_id: String,
    pub uptime_secs: u64,
    /// Optional build features of this node, e.g. `profiling` (`GET /api/debug/pprof/profile`).
    #[serde(default)]
    pub features: Vec<String>,
}
