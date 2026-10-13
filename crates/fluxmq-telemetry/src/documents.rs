//! Pure builders for the topic documents (`fluxmq_proto::telemetry` payloads).
//!
//! Aggregation choices:
//! * `ChannelSummary.lag` = max lag over the channel's groups (as documented in proto);
//!   `ClusterOverview.total_lag` = sum of channel lags (so it matches the channels table).
//! * `GroupState.dlq` = messages this group has dead-lettered since broker start.
//! * `InstanceState.processing_p99_ms` = broker-measured windowed p99; falls back to the
//!   client-reported p99 when there were no acks in the window.

use std::collections::BTreeMap;

use fluxmq_core::{BrokerStats, ChannelStats};
use fluxmq_proto::model::ChannelConfig;
use fluxmq_proto::telemetry::{
    ChannelDetail, ChannelSummary, ChannelsSummary, ClusterOverview, ConsumersState, GroupState,
    Health, InstanceState, LatencySummary, NodeState, PartitionInfo,
};

use crate::health::HealthReport;
use crate::node::NodeSample;
use crate::rates::{ChannelDerived, Derived, round3};

pub fn channel_lag(ch: &ChannelStats) -> u64 {
    ch.groups.iter().map(|g| g.lag).max().unwrap_or(0)
}

pub fn channel_messages(ch: &ChannelStats) -> u64 {
    ch.partitions
        .iter()
        .map(|p| p.high_offset.saturating_sub(p.low_offset))
        .sum()
}

fn consumers_count(ch: &ChannelStats) -> u32 {
    ch.groups.iter().map(|g| g.instances.len() as u32).sum()
}

pub fn channel_summary(
    ch: &ChannelStats,
    cd: Option<&ChannelDerived>,
    health: Health,
) -> ChannelSummary {
    let d = cd.cloned().unwrap_or_default();
    ChannelSummary {
        name: ch.config.name.clone(),
        mode: ch.config.mode,
        health,
        partitions: ch.config.partitions,
        ingress_rate: d.ingress_rate,
        egress_rate: d.egress_rate,
        ack_rate: d.ack_rate,
        lag: channel_lag(ch),
        publish_p99_ms: d.publish_latency.p99,
        consumer_groups: ch.groups.len() as u32,
        consumers: consumers_count(ch),
        storage_bytes: ch.partitions.iter().map(|p| p.storage_bytes).sum(),
        messages: channel_messages(ch),
        dlq: ch.dlq_size,
    }
}

pub fn channels_summary(
    stats: &BrokerStats,
    derived: &Derived,
    health: &HealthReport,
) -> ChannelsSummary {
    stats
        .channels
        .iter()
        .map(|ch| {
            let name = &ch.config.name;
            (
                name.clone(),
                channel_summary(ch, derived.channels.get(name), health.channel(name)),
            )
        })
        .collect()
}

pub fn channel_detail(
    ch: &ChannelStats,
    cd: Option<&ChannelDerived>,
    health: Health,
) -> ChannelDetail {
    let d = cd.cloned().unwrap_or_default();
    ChannelDetail {
        summary: channel_summary(ch, cd, health),
        config: ch.config.clone(),
        publish_latency: d.publish_latency,
        delivery_latency: d.delivery_latency,
        retries_rate: d.retries_rate,
        dlq_rate: d.dlq_rate,
        partitions: ch
            .partitions
            .iter()
            .map(|p| PartitionInfo {
                partition: p.partition,
                low_offset: p.low_offset,
                high_offset: p.high_offset,
                storage_bytes: p.storage_bytes,
                segments: p.segments,
                ingress_rate: d
                    .partition_ingress
                    .get(&p.partition)
                    .copied()
                    .unwrap_or(0.0),
            })
            .collect(),
        oldest_timestamp_ms: ch.oldest_timestamp_ms,
    }
}

/// Detail for a channel that exists in config but not yet in the stats snapshot.
pub fn empty_channel_detail(config: &ChannelConfig) -> ChannelDetail {
    ChannelDetail {
        summary: ChannelSummary {
            name: config.name.clone(),
            mode: config.mode,
            health: Health::Healthy,
            partitions: config.partitions,
            ingress_rate: 0.0,
            egress_rate: 0.0,
            ack_rate: 0.0,
            lag: 0,
            publish_p99_ms: 0.0,
            consumer_groups: 0,
            consumers: 0,
            storage_bytes: 0,
            messages: 0,
            dlq: 0,
        },
        config: config.clone(),
        publish_latency: LatencySummary::default(),
        delivery_latency: LatencySummary::default(),
        retries_rate: 0.0,
        dlq_rate: 0.0,
        partitions: vec![],
        oldest_timestamp_ms: None,
    }
}

pub fn consumers_state(
    ch: &ChannelStats,
    cd: Option<&ChannelDerived>,
    health: &HealthReport,
) -> ConsumersState {
    let cname = &ch.config.name;
    ch.groups
        .iter()
        .map(|g| {
            let gd = cd
                .and_then(|c| c.groups.get(&g.name))
                .cloned()
                .unwrap_or_default();
            let instances: BTreeMap<String, InstanceState> =
                g.instances
                    .iter()
                    .map(|i| {
                        let id = gd.instances.get(&i.id).cloned().unwrap_or_default();
                        let client_p99 = i
                            .client_stats
                            .as_ref()
                            .and_then(|c| c.processing_p99_ms)
                            .map(f64::from);
                        let processing_p99_ms = if id.processing_samples > 0 {
                            id.processing_p99_ms
                        } else {
                            client_p99.map_or(0.0, round3)
                        };
                        let state = InstanceState {
                            id: i.id.clone(),
                            client_name: i.client_name.clone(),
                            remote_addr: i.remote_addr.clone(),
                            connected_at_ms: i.connected_at_ms,
                            health: health.instance(cname, &g.name, &i.id),
                            egress_rate: id.egress_rate,
                            ack_rate: id.ack_rate,
                            nack_rate: id.nack_rate,
                            in_flight: i.in_flight,
                            prefetch: i.prefetch,
                            lag: i.lag,
                            assigned_partitions: i.assigned_partitions.clone(),
                            processing_p99_ms,
                            retries_total: i.retries_total,
                            last_error: i.last_error.clone().or_else(|| {
                                i.client_stats.as_ref().and_then(|c| c.last_error.clone())
                            }),
                            last_error_at_ms: i.last_error_at_ms,
                            cpu_percent: i
                                .client_stats
                                .as_ref()
                                .and_then(|c| c.cpu_percent)
                                .map(|c| (f64::from(c) * 10.0).round() / 10.0),
                            memory_bytes: i.client_stats.as_ref().and_then(|c| c.memory_bytes),
                        };
                        (i.id.clone(), state)
                    })
                    .collect();
            let state = GroupState {
                group: g.name.clone(),
                health: health.group(cname, &g.name),
                lag: g.lag,
                egress_rate: gd.egress_rate,
                ack_rate: gd.ack_rate,
                nack_rate: gd.nack_rate,
                in_flight: g.in_flight,
                retry_pending: g.retry_pending,
                dlq: g.dead_lettered_total,
                processing_latency: gd.processing_latency,
                instances,
            };
            (g.name.clone(), state)
        })
        .collect()
}

/// Inputs for the cluster overview that do not come from a single snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClusterExtras {
    pub open_alerts: u32,
    pub retries_24h: u64,
    /// Disk size of the data directory's filesystem (used when the core reports capacity 0).
    pub disk_total_bytes: u64,
}

pub fn total_lag(stats: &BrokerStats) -> u64 {
    stats.channels.iter().map(channel_lag).sum()
}

pub fn total_consumers(stats: &BrokerStats) -> u32 {
    stats.channels.iter().map(consumers_count).sum()
}

pub fn cluster_overview(
    stats: &BrokerStats,
    derived: &Derived,
    health: &HealthReport,
    extras: ClusterExtras,
) -> ClusterOverview {
    let nodes_healthy = u32::from(health.node != Health::Critical);
    ClusterOverview {
        cluster_name: stats.cluster_name.clone(),
        health: health.cluster,
        version: fluxmq_proto::VERSION.to_string(),
        uptime_secs: stats.taken_at_ms.saturating_sub(stats.started_at_ms) / 1000,
        nodes_total: 1,
        // The node serving the document is up by definition.
        nodes_up: 1,
        nodes_healthy,
        channels: stats.channels.len() as u32,
        consumer_groups: stats.channels.iter().map(|c| c.groups.len() as u32).sum(),
        consumers: total_consumers(stats),
        ingress_rate: derived.ingress_rate,
        egress_rate: derived.egress_rate,
        ack_rate: derived.ack_rate,
        ingress_bytes_rate: derived.ingress_bytes_rate,
        egress_bytes_rate: derived.egress_bytes_rate,
        publish_latency: derived.publish_latency,
        delivery_latency: derived.delivery_latency,
        total_lag: total_lag(stats),
        retries_rate: derived.retries_rate,
        retries_24h: extras.retries_24h,
        dlq_total: stats.channels.iter().map(|c| c.dlq_size).sum(),
        storage_bytes: stats.storage_bytes,
        storage_capacity_bytes: if stats.storage_capacity_bytes > 0 {
            stats.storage_capacity_bytes
        } else {
            extras.disk_total_bytes
        },
        messages_total: stats.channels.iter().map(channel_messages).sum(),
        open_alerts: extras.open_alerts,
        partial: false,
    }
}

pub fn node_state(
    stats: &BrokerStats,
    address: &str,
    sample: &NodeSample,
    derived: &Derived,
    health: Health,
) -> NodeState {
    NodeState {
        id: stats.node_id.clone(),
        address: address.to_string(),
        health,
        version: fluxmq_proto::VERSION.to_string(),
        uptime_secs: stats.taken_at_ms.saturating_sub(stats.started_at_ms) / 1000,
        cpu_percent: (f64::from(sample.cpu_percent) * 10.0).round() / 10.0,
        memory_bytes: sample.memory_bytes,
        memory_total_bytes: sample.memory_total_bytes,
        disk_used_bytes: sample.disk_used_bytes,
        disk_total_bytes: sample.disk_total_bytes,
        disk_latency_p99_ms: derived.fsync_p99_ms,
        connections: stats.connections,
        status: fluxmq_proto::cluster::NodeStatus::Up,
        is_controller: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::{HealthTracker, NodeInputs};
    use crate::rates::derive_pair;
    use crate::rates::tests::{channel, empty_stats, group, instance};

    fn stats(ts: u64, published: u64, acked: u64) -> BrokerStats {
        let mut s = empty_stats(ts);
        s.started_at_ms = 0;
        let mut c = channel("orders");
        c.published_total = published;
        c.partitions[0].published_total = published;
        c.partitions[0].high_offset = published;
        c.dlq_size = 2;
        let mut g = group("billing");
        g.lag = published - acked;
        g.acked_total = acked;
        let mut i = instance("billing-svc#1");
        i.acked_total = acked;
        g.instances.push(i);
        c.groups.push(g);
        let mut g2 = group("audit");
        g2.lag = 7;
        c.groups.push(g2);
        s.channels.push(c);
        s
    }

    #[test]
    fn builds_all_documents() {
        let a = stats(0, 100, 50);
        let b = stats(2_000, 300, 250);
        let d = derive_pair(&b, Some(&a), true);
        let mut t = HealthTracker::new();
        let h = t.evaluate(&b, &d, &NodeInputs::default());

        let sum = channels_summary(&b, &d, &h);
        let o = &sum["orders"];
        assert_eq!(o.ingress_rate, 100.0);
        assert_eq!(o.lag, 50);
        assert_eq!(o.consumers, 1);
        assert_eq!(o.consumer_groups, 2);
        assert_eq!(o.messages, 300);
        assert_eq!(o.dlq, 2);

        let det = channel_detail(
            &b.channels[0],
            d.channels.get("orders"),
            h.channel("orders"),
        );
        assert_eq!(det.partitions[0].ingress_rate, 100.0);

        let cons = consumers_state(&b.channels[0], d.channels.get("orders"), &h);
        assert_eq!(cons["billing"].ack_rate, 100.0);
        assert_eq!(cons["billing"].instances["billing-svc#1"].ack_rate, 100.0);
        // audit has lag but no instances → critical.
        assert_eq!(cons["audit"].health, Health::Critical);

        let co = cluster_overview(
            &b,
            &d,
            &h,
            ClusterExtras {
                open_alerts: 1,
                retries_24h: 3,
                disk_total_bytes: 9,
            },
        );
        assert_eq!(co.total_lag, 50);
        assert_eq!(co.uptime_secs, 2);
        assert_eq!(co.health, Health::Critical);
        assert_eq!(co.storage_capacity_bytes, 9);
        assert_eq!(co.consumers, 1);
        assert_eq!(co.messages_total, 300);

        // Documents serialize to objects (the hub diffs JSON objects).
        assert!(serde_json::to_value(&co).expect("serializable").is_object());
    }
}
