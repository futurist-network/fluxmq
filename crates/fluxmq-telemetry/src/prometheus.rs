//! Prometheus text exposition (format 0.0.4) for `GET /metrics` — **node-local**: every node
//! exposes its own counters (scrape all nodes and aggregate in PromQL, e.g.
//! `sum by (channel) (rate(fluxmq_channel_published_messages_total[1m]))`).
//!
//! Built straight from the collector's latest local [`BrokerStats`] snapshot (no JSON, no
//! extra broker sampling). Label set is bounded: `channel`, `group`, `node` (plus `severity`
//! on the alert gauge). Latency histograms are HDR → fixed-bucket Prometheus histograms in
//! seconds; `_sum` is approximated from the HDR mean (HDR does not keep an exact sum).

use std::fmt::Write as _;

use fluxmq_core::{BrokerStats, LatencyHistogram};
use fluxmq_proto::telemetry::Health;

use crate::documents;
use crate::hub::Shared;

/// Histogram bucket upper bounds in seconds (`+Inf` is implicit).
pub const LATENCY_BUCKETS: &[f64] = &[
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
    5.0, 10.0, 30.0, 60.0,
];

/// Prometheus content type for the text format.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Text-format writer that keeps each family's samples contiguous.
#[derive(Default)]
pub struct Writer {
    out: String,
}

fn escape(v: &str) -> String {
    let mut s = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => s.push_str("\\\\"),
            '"' => s.push_str("\\\""),
            '\n' => s.push_str("\\n"),
            c => s.push(c),
        }
    }
    s
}

fn fmt_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v.is_infinite() {
        if v > 0.0 {
            "+Inf".into()
        } else {
            "-Inf".into()
        }
    } else if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

fn labels(l: &[(&str, &str)], extra: Option<(&str, &str)>) -> String {
    if l.is_empty() && extra.is_none() {
        return String::new();
    }
    let mut s = String::from("{");
    for (i, (k, v)) in l.iter().chain(extra.iter()).enumerate() {
        if i > 0 {
            s.push(',');
        }
        let _ = write!(s, "{k}=\"{}\"", escape(v));
    }
    s.push('}');
    s
}

impl Writer {
    pub fn family(&mut self, name: &str, kind: &str, help: &str) {
        let _ = writeln!(self.out, "# HELP {name} {help}");
        let _ = writeln!(self.out, "# TYPE {name} {kind}");
    }

    pub fn sample(&mut self, name: &str, l: &[(&str, &str)], v: f64) {
        let _ = writeln!(self.out, "{name}{} {}", labels(l, None), fmt_value(v));
    }

    /// `_bucket` / `_sum` / `_count` lines of an HDR histogram (µs) in seconds.
    pub fn histogram(&mut self, name: &str, l: &[(&str, &str)], h: &LatencyHistogram) {
        let mut counts = vec![0u64; LATENCY_BUCKETS.len()];
        let mut overflow = 0u64;
        if !h.is_empty() {
            for v in h.iter_recorded() {
                let secs = v.value_iterated_to() as f64 / 1e6;
                let n = v.count_at_value();
                match LATENCY_BUCKETS.iter().position(|le| secs <= *le) {
                    Some(i) => counts[i] += n,
                    None => overflow += n,
                }
            }
        }
        let mut cum = 0u64;
        for (le, n) in LATENCY_BUCKETS.iter().zip(&counts) {
            cum += n;
            let le = fmt_value(*le);
            let _ = writeln!(
                self.out,
                "{name}_bucket{} {cum}",
                labels(l, Some(("le", le.as_str())))
            );
        }
        let total = cum + overflow;
        let _ = writeln!(
            self.out,
            "{name}_bucket{} {total}",
            labels(l, Some(("le", "+Inf")))
        );
        let sum = if h.is_empty() {
            0.0
        } else {
            (h.mean() * h.len() as f64).round() / 1e6
        };
        let _ = writeln!(self.out, "{name}_sum{} {}", labels(l, None), fmt_value(sum));
        let _ = writeln!(self.out, "{name}_count{} {total}", labels(l, None));
    }

    pub fn finish(self) -> String {
        self.out
    }
}

fn health_value(h: Health) -> f64 {
    match h {
        Health::Healthy => 0.0,
        Health::Warning => 1.0,
        Health::Critical => 2.0,
    }
}

/// Node-level inputs besides the stats snapshot.
#[derive(Debug, Clone, Default)]
pub struct NodeExtras {
    pub sample: crate::node::NodeSample,
    pub fsync_p99_ms: f64,
    pub node_health: Option<Health>,
    pub cluster_health: Option<Health>,
    /// (severity, count) of open alerts.
    pub alerts: Vec<(Health, u32)>,
    pub cluster: Option<ClusterGauges>,
}

#[derive(Debug, Clone, Default)]
pub struct ClusterGauges {
    pub is_controller: bool,
    pub nodes_total: u32,
    pub nodes_up: u32,
    pub under_replicated: u32,
    pub offline: u32,
    pub raft_term: u64,
    pub metadata_epoch: u64,
}

type ChannelCounter = (
    &'static str,
    &'static str,
    fn(&fluxmq_core::ChannelStats) -> u64,
);
type GroupCounter = (
    &'static str,
    &'static str,
    fn(&fluxmq_core::GroupStats) -> u64,
);

/// Renders the exposition for `stats` (pure; see [`render`] for the hub wrapper).
pub fn render_stats(stats: &BrokerStats, x: &NodeExtras) -> String {
    let mut w = Writer::default();
    let node = stats.node_id.as_str();
    let nl = [("node", node)];

    w.family(
        "fluxmq_node_info",
        "gauge",
        "Static node information (value is always 1).",
    );
    w.sample(
        "fluxmq_node_info",
        &[
            ("node", node),
            ("cluster", stats.cluster_name.as_str()),
            ("version", fluxmq_proto::VERSION),
        ],
        1.0,
    );
    let gauges: [(&str, &str, f64); 10] = [
        (
            "fluxmq_node_start_time_seconds",
            "Broker start time (Unix seconds).",
            stats.started_at_ms as f64 / 1000.0,
        ),
        (
            "fluxmq_node_uptime_seconds",
            "Seconds since broker start.",
            (stats.taken_at_ms.saturating_sub(stats.started_at_ms) / 1000) as f64,
        ),
        (
            "fluxmq_node_connections",
            "Open client protocol connections.",
            f64::from(stats.connections),
        ),
        (
            "fluxmq_node_cpu_percent",
            "Broker process CPU usage, 0-100 across all cores.",
            (f64::from(x.sample.cpu_percent) * 10.0).round() / 10.0,
        ),
        (
            "fluxmq_node_memory_bytes",
            "Broker process resident memory.",
            x.sample.memory_bytes as f64,
        ),
        (
            "fluxmq_node_memory_total_bytes",
            "Total machine memory.",
            x.sample.memory_total_bytes as f64,
        ),
        (
            "fluxmq_node_disk_used_bytes",
            "Used bytes on the data directory's filesystem.",
            x.sample.disk_used_bytes as f64,
        ),
        (
            "fluxmq_node_disk_total_bytes",
            "Size of the data directory's filesystem.",
            x.sample.disk_total_bytes as f64,
        ),
        (
            "fluxmq_node_storage_bytes",
            "Bytes stored by the broker (all channel logs).",
            stats.storage_bytes as f64,
        ),
        (
            "fluxmq_node_fsync_p99_seconds",
            "fsync latency p99 over the telemetry rate window.",
            x.fsync_p99_ms / 1000.0,
        ),
    ];
    for (name, help, v) in gauges {
        w.family(name, "gauge", help);
        w.sample(name, &nl, v);
    }
    w.family(
        "fluxmq_node_fsync_latency_seconds",
        "histogram",
        "fsync duration since broker start.",
    );
    w.histogram(
        "fluxmq_node_fsync_latency_seconds",
        &nl,
        &stats.fsync_latency_us,
    );
    if !stats.commit_latency_us.is_empty() {
        w.family(
            "fluxmq_node_commit_latency_seconds",
            "histogram",
            "Group commit: batch submitted until a partition's share of it is durable and \
             published (local node, since broker start).",
        );
        w.histogram(
            "fluxmq_node_commit_latency_seconds",
            &nl,
            &stats.commit_latency_us,
        );
    }
    if let Some(h) = x.node_health {
        w.family(
            "fluxmq_node_health",
            "gauge",
            "Node health: 0 healthy, 1 warning, 2 critical.",
        );
        w.sample("fluxmq_node_health", &nl, health_value(h));
    }
    if let Some(h) = x.cluster_health {
        w.family(
            "fluxmq_health",
            "gauge",
            "Overall health as shown in the console: 0 healthy, 1 warning, 2 critical.",
        );
        w.sample("fluxmq_health", &nl, health_value(h));
    }
    w.family(
        "fluxmq_alerts_open",
        "gauge",
        "Open alerts by severity (as seen by this node).",
    );
    for sev in [Health::Warning, Health::Critical] {
        let n = x
            .alerts
            .iter()
            .filter(|(s, _)| *s == sev)
            .map(|(_, n)| *n)
            .sum::<u32>();
        let s = if sev == Health::Warning {
            "warning"
        } else {
            "critical"
        };
        w.sample(
            "fluxmq_alerts_open",
            &[("node", node), ("severity", s)],
            f64::from(n),
        );
    }

    if let Some(c) = &x.cluster {
        let cg: [(&str, &str, f64); 7] = [
            (
                "fluxmq_cluster_is_controller",
                "1 when this node is the Raft leader (controller).",
                f64::from(u8::from(c.is_controller)),
            ),
            (
                "fluxmq_cluster_nodes",
                "Cluster members known to this node (incl. itself).",
                f64::from(c.nodes_total),
            ),
            (
                "fluxmq_cluster_nodes_up",
                "Cluster members reachable from this node (incl. itself).",
                f64::from(c.nodes_up),
            ),
            (
                "fluxmq_cluster_under_replicated_partitions",
                "Partitions whose ISR is smaller than their replica set.",
                f64::from(c.under_replicated),
            ),
            (
                "fluxmq_cluster_offline_partitions",
                "Partitions without a leader.",
                f64::from(c.offline),
            ),
            (
                "fluxmq_cluster_raft_term",
                "Current Raft term.",
                c.raft_term as f64,
            ),
            (
                "fluxmq_cluster_metadata_epoch",
                "Cluster metadata epoch (bumps on every topology/assignment change).",
                c.metadata_epoch as f64,
            ),
        ];
        for (name, help, v) in cg {
            w.family(name, "gauge", help);
            w.sample(name, &nl, v);
        }
    }

    // ---- channels -------------------------------------------------------------------------
    let counters: [ChannelCounter; 10] = [
        (
            "fluxmq_channel_published_messages_total",
            "Messages accepted by this node for the channel.",
            |c| c.published_total,
        ),
        (
            "fluxmq_channel_published_bytes_total",
            "Payload bytes accepted.",
            |c| c.published_bytes_total,
        ),
        (
            "fluxmq_channel_delivered_messages_total",
            "Deliveries to consumers (incl. redeliveries), all groups.",
            |c| c.delivered_total,
        ),
        (
            "fluxmq_channel_delivered_bytes_total",
            "Payload bytes delivered, all groups.",
            |c| c.delivered_bytes_total,
        ),
        ("fluxmq_channel_acked_total", "Acks, all groups.", |c| {
            c.acked_total
        }),
        ("fluxmq_channel_nacked_total", "Nacks, all groups.", |c| {
            c.nacked_total
        }),
        (
            "fluxmq_channel_timed_out_total",
            "Deliveries whose ack deadline expired.",
            |c| c.timed_out_total,
        ),
        (
            "fluxmq_channel_retries_total",
            "Redeliveries (attempt >= 2).",
            |c| c.retries_total,
        ),
        (
            "fluxmq_channel_publish_duplicates_total",
            "Retried producer batches recognized and not stored again (idempotent publishing).",
            |c| c.duplicates_total,
        ),
        (
            "fluxmq_channel_dead_lettered_total",
            "Messages moved to the DLQ (or dropped) after exhausting retries.",
            |c| c.dead_lettered_total,
        ),
    ];
    for (name, help, f) in counters {
        w.family(name, "counter", help);
        for c in &stats.channels {
            w.sample(name, &[("channel", c.config.name.as_str())], f(c) as f64);
        }
    }
    let cgauges: [ChannelCounter; 5] = [
        ("fluxmq_channel_dlq_messages", "Current DLQ size.", |c| {
            c.dlq_size
        }),
        (
            "fluxmq_channel_storage_bytes",
            "Bytes stored for the channel on this node.",
            |c| c.partitions.iter().map(|p| p.storage_bytes).sum(),
        ),
        (
            "fluxmq_channel_messages",
            "Retained messages (sum of high - low offsets).",
            documents::channel_messages,
        ),
        (
            "fluxmq_channel_partitions",
            "Partitions hosted on this node.",
            |c| c.partitions.len() as u64,
        ),
        (
            "fluxmq_channel_lag_messages",
            "Max consumer-group lag on the channel.",
            documents::channel_lag,
        ),
    ];
    for (name, help, f) in cgauges {
        w.family(name, "gauge", help);
        for c in &stats.channels {
            w.sample(name, &[("channel", c.config.name.as_str())], f(c) as f64);
        }
    }
    w.family(
        "fluxmq_channel_publish_latency_seconds",
        "histogram",
        "Publish request received -> durable append.",
    );
    for c in &stats.channels {
        w.histogram(
            "fluxmq_channel_publish_latency_seconds",
            &[("channel", c.config.name.as_str())],
            &c.publish_latency_us,
        );
    }
    w.family(
        "fluxmq_channel_delivery_latency_seconds",
        "histogram",
        "Append -> first delivery (queueing delay).",
    );
    for c in &stats.channels {
        w.histogram(
            "fluxmq_channel_delivery_latency_seconds",
            &[("channel", c.config.name.as_str())],
            &c.delivery_latency_us,
        );
    }

    // ---- consumer groups ------------------------------------------------------------------
    let ggauges: [GroupCounter; 4] = [
        (
            "fluxmq_group_lag_messages",
            "Messages not yet acked or dead-lettered by the group (incl. in-flight).",
            |g| g.lag,
        ),
        (
            "fluxmq_group_in_flight_messages",
            "Delivered, not yet acked.",
            |g| g.in_flight,
        ),
        (
            "fluxmq_group_retry_pending_messages",
            "Waiting for a redelivery.",
            |g| g.retry_pending,
        ),
        (
            "fluxmq_group_consumers",
            "Connected consumer instances.",
            |g| g.instances.len() as u64,
        ),
    ];
    let gcounters: [GroupCounter; 6] = [
        (
            "fluxmq_group_delivered_messages_total",
            "Deliveries to the group (incl. redeliveries).",
            |g| g.delivered_total,
        ),
        ("fluxmq_group_acked_total", "Acks by the group.", |g| {
            g.acked_total
        }),
        ("fluxmq_group_nacked_total", "Nacks by the group.", |g| {
            g.nacked_total
        }),
        (
            "fluxmq_group_dead_lettered_total",
            "Messages of the group moved to the DLQ.",
            |g| g.dead_lettered_total,
        ),
        (
            "fluxmq_group_retention_skipped_messages_total",
            "Messages retention removed before the group consumed them (skipped).",
            |g| g.retention_skipped_total,
        ),
        (
            "fluxmq_group_catchup_read_bytes_total",
            "Bytes the group's catch-up read-ahead read from disk (behind the recent cache).",
            |g| g.catchup_read_bytes_total,
        ),
    ];
    for (kind, list) in [("gauge", &ggauges[..]), ("counter", &gcounters[..])] {
        for (name, help, f) in list.iter() {
            w.family(name, kind, help);
            for c in &stats.channels {
                for g in &c.groups {
                    w.sample(
                        name,
                        &[
                            ("channel", c.config.name.as_str()),
                            ("group", g.name.as_str()),
                        ],
                        f(g) as f64,
                    );
                }
            }
        }
    }
    w.family(
        "fluxmq_group_processing_latency_seconds",
        "histogram",
        "Delivery -> ack.",
    );
    for c in &stats.channels {
        for g in &c.groups {
            w.histogram(
                "fluxmq_group_processing_latency_seconds",
                &[
                    ("channel", c.config.name.as_str()),
                    ("group", g.name.as_str()),
                ],
                &g.processing_latency_us,
            );
        }
    }
    w.finish()
}

/// The hub wrapper: latest local snapshot + node sample + alerts + cluster gauges.
pub(crate) fn render(shared: &Shared) -> String {
    let stats = shared
        .local_stats
        .read()
        .clone()
        .unwrap_or_else(|| std::sync::Arc::new(shared.source.stats()));
    let latest = shared.latest.read().clone();
    let mut alerts: Vec<(Health, u32)> = Vec::new();
    for a in shared.alerts.read().values() {
        match alerts.iter_mut().find(|(s, _)| *s == a.severity) {
            Some((_, n)) => *n += 1,
            None => alerts.push((a.severity, 1)),
        }
    }
    let cluster = shared.cluster.read().clone().map(|c| {
        let facts = c.facts();
        let (total, live) = c.peer_counts(fluxmq_proto::now_ms());
        ClusterGauges {
            is_controller: facts.local_is_controller,
            nodes_total: total + 1,
            nodes_up: live + 1,
            under_replicated: facts.under_replicated,
            offline: facts.offline,
            raft_term: facts.raft_term,
            metadata_epoch: facts.metadata_epoch,
        }
    });
    let extras = NodeExtras {
        sample: *shared.node_sample.read(),
        fsync_p99_ms: latest.as_ref().map_or(0.0, |l| l.derived.fsync_p99_ms),
        node_health: latest.as_ref().map(|l| l.health.node),
        cluster_health: latest.as_ref().map(|l| l.health.cluster),
        alerts,
        cluster,
    };
    render_stats(&stats, &extras)
}
