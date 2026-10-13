//! Live subscription gateway status (v0.11): `GET /api/live/status` (read).
//!
//! The gateway itself (WebSocket `/api/channels/{name}/live`, SSE `/api/channels/{name}/events`)
//! is described in `docs/design/v0.10-live-gateway.md`. This is what the console shows about it:
//! each node's limits (`--live-*` flags; they may differ between nodes), open connections,
//! subscriptions per channel and the cumulative counters behind `fluxmq_live_*` metrics. Nodes
//! that can't be reached are left out (best-effort, like the other cluster-wide views).

use serde::{Deserialize, Serialize};

/// `--live-*` limits of one node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveLimits {
    pub max_connections: u64,
    pub max_connections_per_token: u64,
    pub max_connections_per_channel: u64,
    pub max_subscriptions: u64,
    /// 0 = unlimited.
    pub max_msgs_per_sec: u64,
    /// 0 = unlimited.
    pub max_bytes_per_sec: u64,
    pub max_payload_bytes: u64,
    pub max_batch: u64,
    pub send_buffer: u64,
    pub send_timeout_secs: u64,
    pub heartbeat_secs: u64,
    pub idle_timeout_secs: u64,
    pub fanout_buffer: u64,
    /// Browser origins allowed besides same-origin (`*` = any).
    pub allowed_origins: Vec<String>,
}

/// Live subscriptions on one channel (on one node, or summed).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveChannelCount {
    pub channel: String,
    pub subscriptions: u64,
}

/// Refused connections or subscriptions by reason (`node_limit`, `token_limit`,
/// `channel_limit`, `subscription_limit`, `origin`, `forbidden`, `unauthorized`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveRejection {
    pub reason: String,
    pub count: u64,
}

/// One node's gateway. Counters are cumulative since the node started.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveNodeStatus {
    pub node: String,
    pub limits: LiveLimits,
    pub connections_ws: u64,
    pub connections_sse: u64,
    /// Subscriptions still reading history (catch-up) / following new messages.
    pub subscriptions_catchup: u64,
    pub subscriptions_live: u64,
    /// Channels with a shared read running (one per channel, whatever the subscriber count).
    pub feeds: u64,
    pub messages_sent: u64,
    pub bytes_sent: u64,
    pub slow_disconnects: u64,
    /// Subscribers that fell behind retention and skipped messages.
    pub gaps: u64,
    /// Only reasons with a non-zero count.
    pub rejections: Vec<LiveRejection>,
    /// Channels with at least one subscription, most subscribed first.
    pub channels: Vec<LiveChannelCount>,
}

/// `GET /api/live/status`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveStatus {
    pub nodes: Vec<LiveNodeStatus>,
}
