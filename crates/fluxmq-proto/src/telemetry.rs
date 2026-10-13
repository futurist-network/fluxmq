//! Telemetry contract v1 — what the UI receives over `GET /ws` (JSON text frames).
//!
//! Principles (see docs/ROADMAP.md):
//! * The UI never sees the raw message stream, only aggregates computed by the broker.
//! * One WebSocket per browser tab, multiplexed by topic; the server only sends topics the
//!   client is subscribed to.
//! * On subscribe the server sends a full `snapshot`, then `delta`s. A delta is a JSON
//!   merge patch (RFC 7386): objects merge recursively, arrays and scalars replace, `null`
//!   deletes the key. `seq` increases by 1 per topic; on a gap the client re-subscribes.
//! * Slow clients get latest-wins: unsent deltas for a topic are coalesced, never queued.
//!
//! Rates are per second, latencies in milliseconds (f64), sizes in bytes, times in Unix ms.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::model::ChannelMode;

pub const CONTRACT_VERSION: u32 = 1;

// ---------------------------------------------------------------------------------------
// Envelope
// ---------------------------------------------------------------------------------------

/// Browser → server.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ClientMsg {
    Subscribe { topics: Vec<String> },
    Unsubscribe { topics: Vec<String> },
    Ping { nonce: Option<u64> },
}

/// Server → browser.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    /// First message on every connection.
    Hello {
        contract_version: u32,
        broker_version: String,
        node_id: String,
        server_time_ms: u64,
    },
    Snapshot {
        topic: String,
        seq: u64,
        ts: u64,
        data: serde_json::Value,
    },
    Delta {
        topic: String,
        seq: u64,
        ts: u64,
        data: serde_json::Value,
    },
    Pong {
        nonce: Option<u64>,
        server_time_ms: u64,
    },
    /// Subscription rejected (unknown topic, unknown channel, ...).
    Error {
        topic: Option<String>,
        message: String,
    },
}

/// Topic names. Channel-scoped topics embed the channel name verbatim.
pub mod topics {
    /// [`super::ClusterOverview`] — 250 ms.
    pub const CLUSTER: &str = "cluster.overview";
    /// [`super::ChannelsSummary`] (map name → summary) — 500 ms.
    pub const CHANNELS: &str = "channels.summary";
    /// [`super::AlertsState`] — on change, re-evaluated every 1 s.
    pub const ALERTS: &str = "alerts";
    /// [`super::EventsFeed`] — on change.
    pub const EVENTS: &str = "events";
    /// [`super::NodesState`] — 1 s.
    pub const NODES: &str = "nodes";

    /// [`super::ChannelDetail`] — 500 ms.
    pub fn channel(name: &str) -> String {
        format!("channel.{name}")
    }
    /// [`super::ConsumersState`] — 500 ms.
    pub fn consumers(name: &str) -> String {
        format!("channel.{name}.consumers")
    }
    /// [`super::TailState`] — sampled live messages, flushed every 250 ms.
    pub fn tail(name: &str) -> String {
        format!("channel.{name}.tail")
    }

    /// Parsed topic.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum Topic {
        Cluster,
        Channels,
        Alerts,
        Events,
        Nodes,
        Channel(String),
        Consumers(String),
        Tail(String),
    }

    pub fn parse(topic: &str) -> Option<Topic> {
        Some(match topic {
            CLUSTER => Topic::Cluster,
            CHANNELS => Topic::Channels,
            ALERTS => Topic::Alerts,
            EVENTS => Topic::Events,
            NODES => Topic::Nodes,
            _ => {
                let rest = topic.strip_prefix("channel.")?;
                if let Some(name) = rest.strip_suffix(".consumers") {
                    Topic::Consumers(name.to_string())
                } else if let Some(name) = rest.strip_suffix(".tail") {
                    Topic::Tail(name.to_string())
                } else {
                    Topic::Channel(rest.to_string())
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------------------
// Payloads
// ---------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Health {
    Healthy,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct LatencySummary {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
    pub max: f64,
}

/// `cluster.overview` — the dashboard header: HEALTH / TRAFFIC / LAG / ERRORS.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterOverview {
    pub cluster_name: String,
    pub health: Health,
    pub version: String,
    pub uptime_secs: u64,
    pub nodes_total: u32,
    /// Nodes whose cluster status is `Up`/`Leaving`/`Joining` (reachable members, whatever
    /// their health). "N/M nodes up" uses this; colour comes from `health`.
    #[serde(default)]
    pub nodes_up: u32,
    /// Nodes that are up and not `critical` health.
    pub nodes_healthy: u32,
    pub channels: u32,
    pub consumer_groups: u32,
    pub consumers: u32,
    /// Messages accepted by the broker per second.
    pub ingress_rate: f64,
    /// Messages delivered to consumers per second, summed over all groups (incl. redeliveries).
    pub egress_rate: f64,
    /// Acks per second, summed over all groups ("consumed").
    pub ack_rate: f64,
    pub ingress_bytes_rate: f64,
    pub egress_bytes_rate: f64,
    /// Publish request → durable append.
    pub publish_latency: LatencySummary,
    /// Append → first delivery (queueing delay).
    pub delivery_latency: LatencySummary,
    pub total_lag: u64,
    pub retries_rate: f64,
    pub retries_24h: u64,
    pub dlq_total: u64,
    pub storage_bytes: u64,
    pub storage_capacity_bytes: u64,
    pub messages_total: u64,
    /// Number of open alerts (any severity).
    pub open_alerts: u32,
    /// Clustered only: `true` when at least one peer's stats are missing or stale, so the
    /// cluster-wide figures above cover only the reachable nodes. Always `false` on one node.
    #[serde(default)]
    pub partial: bool,
}

/// `channels.summary`: map `channel name → ChannelSummary`.
pub type ChannelsSummary = BTreeMap<String, ChannelSummary>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChannelSummary {
    pub name: String,
    pub mode: ChannelMode,
    pub health: Health,
    pub partitions: u32,
    pub ingress_rate: f64,
    /// Deliveries per second, summed over all groups (incl. redeliveries).
    pub egress_rate: f64,
    /// Acks per second, summed over all groups ("consumed").
    pub ack_rate: f64,
    /// Max lag over consumer groups (messages).
    pub lag: u64,
    pub publish_p99_ms: f64,
    pub consumer_groups: u32,
    pub consumers: u32,
    pub storage_bytes: u64,
    pub messages: u64,
    pub dlq: u64,
}

/// `channel.{name}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChannelDetail {
    pub summary: ChannelSummary,
    pub config: crate::model::ChannelConfig,
    pub publish_latency: LatencySummary,
    pub delivery_latency: LatencySummary,
    pub retries_rate: f64,
    pub dlq_rate: f64,
    pub partitions: Vec<PartitionInfo>,
    pub oldest_timestamp_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub partition: u32,
    /// First retained offset.
    pub low_offset: u64,
    /// Next offset to be written.
    pub high_offset: u64,
    pub storage_bytes: u64,
    pub segments: u32,
    pub ingress_rate: f64,
}

/// `channel.{name}.consumers`: map `group name → GroupState`.
pub type ConsumersState = BTreeMap<String, GroupState>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupState {
    pub group: String,
    pub health: Health,
    pub lag: u64,
    pub egress_rate: f64,
    pub ack_rate: f64,
    pub nack_rate: f64,
    pub in_flight: u64,
    pub retry_pending: u64,
    pub dlq: u64,
    /// Delivery → ack.
    pub processing_latency: LatencySummary,
    /// Map `instance id → InstanceState`.
    pub instances: BTreeMap<String, InstanceState>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstanceState {
    /// `{client_name}#{connection_id}`.
    pub id: String,
    pub client_name: String,
    pub remote_addr: String,
    pub connected_at_ms: u64,
    pub health: Health,
    pub egress_rate: f64,
    pub ack_rate: f64,
    pub nack_rate: f64,
    pub in_flight: u64,
    pub prefetch: u32,
    /// Lag attributable to this instance (backlog on its assigned partitions; for Queue
    /// channels, group lag divided evenly).
    pub lag: u64,
    pub assigned_partitions: Vec<u32>,
    pub processing_p99_ms: f64,
    pub retries_total: u64,
    pub last_error: Option<String>,
    pub last_error_at_ms: Option<u64>,
    /// From `ClientStats`; `None` when the SDK doesn't report.
    pub cpu_percent: Option<f64>,
    pub memory_bytes: Option<u64>,
}

/// `channel.{name}.tail` — sampled live messages. Server keeps at most 200 per flush and
/// the client keeps the last N. Payload is UTF-8 lossy text truncated to 4 KiB.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TailState {
    /// Fraction of messages sampled in the last window (0..=1).
    pub sample_ratio: f64,
    pub shown_rate: f64,
    pub total_rate: f64,
    /// Newest last. Snapshot = recent buffer; deltas replace the array with the new batch.
    pub messages: Vec<TailMessage>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TailMessage {
    pub partition: u32,
    pub offset: u64,
    pub key: Option<String>,
    pub timestamp_ms: u64,
    pub headers: BTreeMap<String, String>,
    pub size: u32,
    pub payload_preview: String,
    pub truncated: bool,
}

/// `alerts`: map `alert id → Alert` (open alerts only; resolved ones are deleted).
pub type AlertsState = BTreeMap<String, Alert>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Alert {
    /// Stable for the same condition on the same subject, e.g. `lag_growth:email/workers`.
    pub id: String,
    pub severity: Health,
    pub kind: AlertKind,
    /// "Email queue is growing rapidly".
    pub title: String,
    /// "Lag increased from 1.2k → 18k in 6 min".
    pub detail: String,
    /// "Consumers processing slower than producers."
    pub possible_reason: Option<String>,
    /// UI route to inspect, e.g. `/channels/email/consumers`.
    pub link: Option<String>,
    pub channel: Option<String>,
    pub group: Option<String>,
    pub since_ms: u64,
    /// Structured values behind `title`/`detail` so clients can localise the text themselves
    /// (e.g. `slow_instance` → `{instance, p99_ms, group_p99_ms}`). Empty for kinds without
    /// parameters and for alerts relayed from peers (not carried over the cluster wire).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertKind {
    LagGrowth,
    ConsumerStalled,
    NoConsumers,
    DlqGrowth,
    HighRetryRate,
    SlowInstance,
    StorageNearlyFull,
    HighPublishLatency,
    /// A cluster node stopped answering (stats pulls and/or controller heartbeats).
    NodeDown,
    /// Partitions whose ISR is smaller than the replica set.
    UnderReplicatedPartitions,
    /// Partitions without a leader (unavailable for publish/consume).
    OfflinePartitions,
    /// v0.4: a served TLS certificate expires within `tls::CERT_EXPIRY_WARNING_DAYS`
    /// (critical within 3 days or expired). Id `tls_cert_expiring:{node}/{surface}`.
    TlsCertExpiring,
    /// v0.4: the last TLS certificate reload failed (the previous one is still served).
    /// Id `tls_reload_failed:{node}/{surface}`.
    TlsReloadFailed,
}

/// `events`: newest first, at most 100. Snapshot = full list; deltas replace the list.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventsFeed {
    pub events: Vec<Event>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub id: u64,
    pub ts: u64,
    pub kind: EventKind,
    /// "Channel created".
    pub title: String,
    /// "audit.logs".
    pub subject: String,
    pub severity: Health,
    /// Structured values behind `title`/`subject` for client-side localisation
    /// (e.g. `dead_lettered` → `{channel, count}`, `alert_opened` → `{alert_kind, alert_id, …}`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    ChannelCreated,
    ChannelDeleted,
    ChannelUpdated,
    ConsumerConnected,
    ConsumerDisconnected,
    AlertOpened,
    AlertResolved,
    DeadLettered,
    DlqRetried,
    DlqDiscarded,
    MessagesReplayed,
    /// The Raft leader (controller) changed; subject = new controller id (or `none`).
    ControllerChanged,
    /// A cluster node became unreachable; subject = node id.
    NodeDown,
    /// A cluster node joined or came back; subject = node id.
    NodeUp,
    /// v0.4 rebalancing; subject = trigger (`node n4 joined`, `manual`, …).
    RebalanceStarted,
    RebalanceCompleted,
    RebalanceFailed,
    /// subject = `channel/partition`; detail has from/to replicas.
    PartitionMoved,
    /// subject = down node id whose replicas are being replaced.
    ReplicaRepairStarted,
    /// subject = `client|http|cluster`.
    TlsReloadFailed,
    /// v0.5 decommission lifecycle; subject = node id (failed: title carries the error).
    NodeDecommissionStarted,
    NodeDecommissioned,
    NodeDecommissionFailed,
    /// A `Down` node with no replicas was forgotten; subject = node id.
    NodeForgotten,
}

/// `nodes`: map `node id → NodeState`. One entry on a single node; every cluster member when
/// clustered (unreachable peers are kept with `status = down`, `health = critical`).
pub type NodesState = BTreeMap<String, NodeState>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeState {
    pub id: String,
    pub address: String,
    pub health: Health,
    pub version: String,
    pub uptime_secs: u64,
    pub cpu_percent: f64,
    pub memory_bytes: u64,
    pub memory_total_bytes: u64,
    pub disk_used_bytes: u64,
    pub disk_total_bytes: u64,
    /// fsync latency p99.
    pub disk_latency_p99_ms: f64,
    pub connections: u32,
    /// Membership status (`up` on a single node).
    #[serde(default = "default_node_status")]
    pub status: crate::cluster::NodeStatus,
    /// Currently the Raft leader / controller (`true` on a single node).
    #[serde(default)]
    pub is_controller: bool,
}

fn default_node_status() -> crate::cluster::NodeStatus {
    crate::cluster::NodeStatus::Up
}

// ---------------------------------------------------------------------------------------
// History (REST `GET /api/history`)
// ---------------------------------------------------------------------------------------

/// Metric ids accepted by `/api/history?metric=`. Channel metrics: `channel.<name>.<m>`.
pub mod metrics {
    pub const INGRESS_RATE: &str = "ingress_rate";
    /// Deliveries/s summed over all groups.
    pub const EGRESS_RATE: &str = "egress_rate";
    /// Acks/s summed over all groups ("consumed").
    pub const ACK_RATE: &str = "ack_rate";
    pub const PUBLISH_P50: &str = "publish_p50";
    pub const PUBLISH_P95: &str = "publish_p95";
    pub const PUBLISH_P99: &str = "publish_p99";
    pub const LAG: &str = "lag";
    pub const RETRIES_RATE: &str = "retries_rate";
    pub const DLQ_RATE: &str = "dlq_rate";
    pub const CONSUMERS: &str = "consumers";
    pub const STORAGE_BYTES: &str = "storage_bytes";

    pub const ALL: &[&str] = &[
        INGRESS_RATE,
        EGRESS_RATE,
        ACK_RATE,
        PUBLISH_P50,
        PUBLISH_P95,
        PUBLISH_P99,
        LAG,
        RETRIES_RATE,
        DLQ_RATE,
        CONSUMERS,
        STORAGE_BYTES,
    ];
}

/// Range presets → bucket width (≈300–1500 points each).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HistoryRange {
    #[serde(rename = "5m")]
    M5,
    #[serde(rename = "15m")]
    M15,
    #[serde(rename = "1h")]
    H1,
    #[serde(rename = "24h")]
    H24,
    #[serde(rename = "7d")]
    D7,
}

impl HistoryRange {
    pub fn duration_ms(self) -> u64 {
        match self {
            HistoryRange::M5 => 5 * 60_000,
            HistoryRange::M15 => 15 * 60_000,
            HistoryRange::H1 => 3_600_000,
            HistoryRange::H24 => 86_400_000,
            HistoryRange::D7 => 7 * 86_400_000,
        }
    }
    pub fn bucket_ms(self) -> u64 {
        match self {
            HistoryRange::M5 => 1_000,
            HistoryRange::M15 => 1_000,
            HistoryRange::H1 => 5_000,
            HistoryRange::H24 => 60_000,
            HistoryRange::D7 => 30 * 60_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistorySeries {
    pub metric: String,
    pub range: HistoryRange,
    pub bucket_ms: u64,
    /// `[timestamp_ms, value]`, oldest first; missing buckets are omitted.
    pub points: Vec<(u64, f64)>,
}

#[cfg(test)]
mod tests {
    use super::topics::{Topic, parse};

    #[test]
    fn topic_parsing() {
        assert_eq!(parse("cluster.overview"), Some(Topic::Cluster));
        assert_eq!(
            parse("channel.orders.events"),
            Some(Topic::Channel("orders.events".into()))
        );
        assert_eq!(
            parse("channel.orders.events.consumers"),
            Some(Topic::Consumers("orders.events".into()))
        );
        assert_eq!(parse("channel.a.tail"), Some(Topic::Tail("a".into())));
        assert_eq!(parse("nope"), None);
    }
}
