//! Raw, cumulative statistics exposed to the observability plane.
//!
//! Everything here is *cumulative since broker start* (counters, histograms) or a point-in-time
//! gauge (lag, in-flight, sizes). The telemetry crate turns two snapshots into rates and
//! windowed percentiles (`Histogram::subtract`). Keeping the core free of windows/rates means
//! any number of observers can sample at any cadence without coordinating.
//!
//! Histogram units: **microseconds**, range 1µs..=60s, 3 significant figures.

use fluxmq_proto::model::ChannelConfig;
use fluxmq_proto::wire::ClientStats;
use hdrhistogram::Histogram;

pub type LatencyHistogram = Histogram<u64>;

/// Creates an empty histogram with the canonical bounds used across FluxMQ.
pub fn new_latency_histogram() -> LatencyHistogram {
    Histogram::new_with_bounds(1, 60_000_000, 3).expect("valid histogram bounds")
}

#[derive(Debug, Clone)]
pub struct BrokerStats {
    pub taken_at_ms: u64,
    pub node_id: String,
    pub cluster_name: String,
    pub started_at_ms: u64,
    pub connections: u32,
    pub channels: Vec<ChannelStats>,
    /// fsync duration.
    pub fsync_latency_us: LatencyHistogram,
    /// Group commit: batch submitted → a partition's share of it durable (v0.9; local node
    /// only, not carried in cluster telemetry snapshots).
    pub commit_latency_us: LatencyHistogram,
    pub storage_bytes: u64,
    pub storage_capacity_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct ChannelStats {
    pub config: ChannelConfig,
    pub partitions: Vec<PartitionStats>,
    pub published_total: u64,
    /// v0.12: retried producer batches answered with the stored copy (not stored again).
    /// Node-local (not carried in cluster snapshots).
    pub duplicates_total: u64,
    pub published_bytes_total: u64,
    /// Deliveries incl. redeliveries.
    pub delivered_total: u64,
    pub delivered_bytes_total: u64,
    pub acked_total: u64,
    pub nacked_total: u64,
    pub timed_out_total: u64,
    /// Redeliveries (attempt >= 2).
    pub retries_total: u64,
    pub dead_lettered_total: u64,
    /// Current DLQ size.
    pub dlq_size: u64,
    /// Publish request received → append durable.
    pub publish_latency_us: LatencyHistogram,
    /// Append → first delivery.
    pub delivery_latency_us: LatencyHistogram,
    pub groups: Vec<GroupStats>,
    pub oldest_timestamp_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct PartitionStats {
    pub partition: u32,
    pub low_offset: u64,
    pub high_offset: u64,
    pub storage_bytes: u64,
    pub segments: u32,
    pub published_total: u64,
}

#[derive(Debug, Clone)]
pub struct GroupStats {
    pub name: String,
    /// Messages not yet acked/dead-lettered by this group (incl. in-flight and retry-pending).
    pub lag: u64,
    pub in_flight: u64,
    pub retry_pending: u64,
    pub delivered_total: u64,
    pub acked_total: u64,
    pub nacked_total: u64,
    pub dead_lettered_total: u64,
    /// Messages skipped because retention removed them before the group consumed them
    /// (node-local; not carried in cluster snapshots).
    pub retention_skipped_total: u64,
    /// Bytes this group's catch-up read-ahead read from disk (node-local).
    pub catchup_read_bytes_total: u64,
    /// Delivery → ack.
    pub processing_latency_us: LatencyHistogram,
    pub instances: Vec<InstanceStats>,
}

#[derive(Debug, Clone)]
pub struct InstanceStats {
    /// `{client_name}#{connection_id}`.
    pub id: String,
    pub subscription_id: u64,
    pub client_name: String,
    pub remote_addr: String,
    pub connected_at_ms: u64,
    pub prefetch: u32,
    pub in_flight: u64,
    pub assigned_partitions: Vec<u32>,
    pub lag: u64,
    pub delivered_total: u64,
    pub acked_total: u64,
    pub nacked_total: u64,
    pub retries_total: u64,
    pub processing_latency_us: LatencyHistogram,
    pub last_error: Option<String>,
    pub last_error_at_ms: Option<u64>,
    pub client_stats: Option<ClientStats>,
}
