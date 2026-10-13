//! Node stats wire format (`GET /internal/v1/node-stats`) and the cluster-wide merge.
//!
//! A [`NodeStatsSnapshot`] is a compact, self-contained copy of one node's cumulative
//! [`BrokerStats`] plus the node-level values only that node can compute (resource sample,
//! fsync p99, node health) and the partitions it currently leads. It is encoded with postcard;
//! HDR histograms travel as hdrhistogram V2 blobs (empty histograms as zero bytes).
//!
//! [`merge`] turns the local snapshot plus the live peers' snapshots into one cluster-wide
//! [`BrokerStats`] the collector consumes exactly like a single node's:
//!
//! * channel / group counters and latency histograms are summed over the nodes that **lead at
//!   least one partition** of the channel (followers would otherwise double count replicated
//!   appends);
//! * every partition is taken from its leader (fallback when nobody claims it: the replica with
//!   the highest `high_offset`);
//! * group gauges (lag, in-flight, retry-pending) and the channel DLQ size are summed over the
//!   same contributing nodes; `oldest_timestamp_ms` is the minimum;
//! * consumer instances are kept per node, with the id prefixed `"{node}/"`;
//! * node gauges: connections, storage bytes / capacity are summed; the fsync histogram stays the
//!   local node's (it is a node property — peers report their own p99 in their snapshot).
//!
//! **Node-local alerts** (v0.4, e.g. `tls_cert_expiring` — each node has its own
//! certificates) travel as an optional postcard trailer after the snapshot: a
//! `Vec<Alert>`. Older decoders ignore trailing bytes (postcard), and a snapshot without the
//! trailer decodes with no alerts, so format 1 stays compatible in both directions. Every node
//! adds its peers' local alerts to its alert candidates; only the controller notifies.
//!
//! Cumulative sums jump whenever the set of contributing nodes changes (peer down, restart,
//! leadership move). The caller detects that with [`Signature`] and resets the rate window, so a
//! membership change never shows up as a rate spike.

use std::collections::{BTreeMap, HashMap};

use fluxmq_core::{
    BrokerStats, ChannelStats, GroupStats, InstanceStats, LatencyHistogram, PartitionStats,
    new_latency_histogram,
};
use fluxmq_proto::model::ChannelConfig;
use fluxmq_proto::telemetry::{Alert, AlertKind, Health};
use fluxmq_proto::wire::ClientStats;
use hdrhistogram::serialization::{Deserializer, Serializer, V2Serializer};
use serde::{Deserialize, Serialize};

use crate::node::NodeSample;

/// Bumped on incompatible changes; peers with another format are treated as unreachable.
pub const SNAPSHOT_FORMAT: u32 = 1;
/// `Content-Type` of the encoded snapshot.
pub const CONTENT_TYPE: &str = "application/x-fluxmq-node-stats";

/// channel → partitions this node leads. `None` (the whole map) = unknown / not clustered:
/// the node is treated as the leader of everything it reports.
pub type Leads = BTreeMap<String, Vec<u32>>;

/// An encoded HDR histogram (V2, uncompressed; empty = no samples). postcard stores a
/// `Vec<u8>` as a varint length plus the raw bytes.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Hist(pub Vec<u8>);

impl Hist {
    pub fn encode(h: &LatencyHistogram) -> Hist {
        if h.is_empty() {
            return Hist::default();
        }
        let mut buf = Vec::new();
        match V2Serializer::new().serialize(h, &mut buf) {
            Ok(_) => Hist(buf),
            Err(e) => {
                tracing::warn!(error = ?e, "could not encode histogram");
                Hist::default()
            }
        }
    }

    pub fn decode(&self) -> Result<LatencyHistogram, String> {
        if self.0.is_empty() {
            return Ok(new_latency_histogram());
        }
        Deserializer::new()
            .deserialize(&mut self.0.as_slice())
            .map_err(|e| format!("histogram: {e}"))
    }
}

/// Node-level values that only the node itself can compute.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeMeta {
    pub node_id: String,
    pub cluster_name: String,
    /// Host name (shown in `nodes`).
    pub address: String,
    pub version: String,
    pub taken_at_ms: u64,
    pub started_at_ms: u64,
    pub health: Health,
    pub cpu_percent: f32,
    pub memory_bytes: u64,
    pub memory_total_bytes: u64,
    pub disk_used_bytes: u64,
    pub disk_total_bytes: u64,
    pub fsync_p99_ms: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeStatsSnapshot {
    pub format: u32,
    pub meta: NodeMeta,
    pub leads: Option<Leads>,
    pub connections: u32,
    pub storage_bytes: u64,
    pub storage_capacity_bytes: u64,
    pub fsync_latency: Hist,
    pub channels: Vec<ChannelSnap>,
    /// Node-local alert candidates (TLS); encoded as a trailer, see the module docs.
    #[serde(skip)]
    pub alerts: Vec<Alert>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChannelSnap {
    pub config: ChannelConfig,
    pub partitions: Vec<PartitionSnap>,
    /// published, published_bytes, delivered, delivered_bytes, acked, nacked, timed_out,
    /// retries, dead_lettered.
    pub counters: [u64; 9],
    pub dlq_size: u64,
    pub publish_latency: Hist,
    pub delivery_latency: Hist,
    pub groups: Vec<GroupSnap>,
    pub oldest_timestamp_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionSnap {
    pub partition: u32,
    pub low_offset: u64,
    pub high_offset: u64,
    pub storage_bytes: u64,
    pub segments: u32,
    pub published_total: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupSnap {
    pub name: String,
    pub lag: u64,
    pub in_flight: u64,
    pub retry_pending: u64,
    /// delivered, acked, nacked, dead_lettered.
    pub counters: [u64; 4],
    pub processing_latency: Hist,
    pub instances: Vec<InstanceSnap>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstanceSnap {
    pub id: String,
    pub subscription_id: u64,
    pub client_name: String,
    pub remote_addr: String,
    pub connected_at_ms: u64,
    pub prefetch: u32,
    pub in_flight: u64,
    pub assigned_partitions: Vec<u32>,
    pub lag: u64,
    /// delivered, acked, nacked, retries.
    pub counters: [u64; 4],
    pub processing_latency: Hist,
    pub last_error: Option<String>,
    pub last_error_at_ms: Option<u64>,
    pub client_stats: Option<ClientStats>,
}

/// A peer snapshot decoded once on receipt (histograms included).
#[derive(Debug, Clone)]
pub struct DecodedSnapshot {
    pub meta: NodeMeta,
    pub leads: Option<Leads>,
    pub stats: BrokerStats,
    /// The peer's node-local alert candidates.
    pub alerts: Vec<Alert>,
}

/// Wire shape of an alert in the snapshot trailer: the v0.4 [`Alert`] layout, frozen.
/// postcard is positional, so new `Alert` fields (e.g. `params`, a JSON map postcard can't
/// encode) must not leak into the trailer, or mixed-version peers stop decoding each other.
/// Params are dropped in transit; clients fall back to the English title/detail.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct AlertWire {
    pub id: String,
    pub severity: Health,
    pub kind: AlertKind,
    pub title: String,
    pub detail: String,
    pub possible_reason: Option<String>,
    pub link: Option<String>,
    pub channel: Option<String>,
    pub group: Option<String>,
    pub since_ms: u64,
}

impl From<&Alert> for AlertWire {
    fn from(a: &Alert) -> Self {
        AlertWire {
            id: a.id.clone(),
            severity: a.severity,
            kind: a.kind,
            title: a.title.clone(),
            detail: a.detail.clone(),
            possible_reason: a.possible_reason.clone(),
            link: a.link.clone(),
            channel: a.channel.clone(),
            group: a.group.clone(),
            since_ms: a.since_ms,
        }
    }
}

impl From<AlertWire> for Alert {
    fn from(a: AlertWire) -> Self {
        Alert {
            id: a.id,
            severity: a.severity,
            kind: a.kind,
            title: a.title,
            detail: a.detail,
            possible_reason: a.possible_reason,
            link: a.link,
            channel: a.channel,
            group: a.group,
            since_ms: a.since_ms,
            params: BTreeMap::new(),
        }
    }
}

impl NodeStatsSnapshot {
    /// Builds the snapshot of this node.
    pub fn build(stats: &BrokerStats, meta: NodeMeta, leads: Option<Leads>) -> Self {
        NodeStatsSnapshot {
            format: SNAPSHOT_FORMAT,
            meta,
            leads,
            connections: stats.connections,
            storage_bytes: stats.storage_bytes,
            storage_capacity_bytes: stats.storage_capacity_bytes,
            fsync_latency: Hist::encode(&stats.fsync_latency_us),
            channels: stats.channels.iter().map(channel_snap).collect(),
            alerts: Vec::new(),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, String> {
        let mut bytes = postcard::to_stdvec(self).map_err(|e| e.to_string())?;
        if !self.alerts.is_empty() {
            let wire: Vec<AlertWire> = self.alerts.iter().map(AlertWire::from).collect();
            let trailer = postcard::to_stdvec(&wire).map_err(|e| e.to_string())?;
            bytes.extend_from_slice(&trailer);
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let (mut s, rest): (NodeStatsSnapshot, &[u8]) =
            postcard::take_from_bytes(bytes).map_err(|e| e.to_string())?;
        if s.format != SNAPSHOT_FORMAT {
            return Err(format!(
                "unsupported node-stats format {} (expected {SNAPSHOT_FORMAT})",
                s.format
            ));
        }
        if !rest.is_empty() {
            let wire: Vec<AlertWire> =
                postcard::from_bytes(rest).map_err(|e| format!("alerts trailer: {e}"))?;
            s.alerts = wire.into_iter().map(Alert::from).collect();
        }
        Ok(s)
    }

    /// Back to [`BrokerStats`] (histograms decoded).
    pub fn into_decoded(self) -> Result<DecodedSnapshot, String> {
        let stats = BrokerStats {
            taken_at_ms: self.meta.taken_at_ms,
            node_id: self.meta.node_id.clone(),
            cluster_name: self.meta.cluster_name.clone(),
            started_at_ms: self.meta.started_at_ms,
            connections: self.connections,
            channels: self
                .channels
                .into_iter()
                .map(channel_stats)
                .collect::<Result<_, _>>()?,
            fsync_latency_us: self.fsync_latency.decode()?,
            // Not carried in snapshots (local node metric).
            commit_latency_us: new_latency_histogram(),
            storage_bytes: self.storage_bytes,
            storage_capacity_bytes: self.storage_capacity_bytes,
        };
        Ok(DecodedSnapshot {
            meta: self.meta,
            leads: self.leads,
            stats,
            alerts: self.alerts,
        })
    }
}

impl NodeMeta {
    /// Node-level values for the local node.
    pub fn local(
        stats: &BrokerStats,
        address: &str,
        sample: &NodeSample,
        fsync_p99_ms: f64,
        health: Health,
    ) -> Self {
        NodeMeta {
            node_id: stats.node_id.clone(),
            cluster_name: stats.cluster_name.clone(),
            address: address.to_string(),
            version: fluxmq_proto::VERSION.to_string(),
            taken_at_ms: stats.taken_at_ms,
            started_at_ms: stats.started_at_ms,
            health,
            cpu_percent: sample.cpu_percent,
            memory_bytes: sample.memory_bytes,
            memory_total_bytes: sample.memory_total_bytes,
            disk_used_bytes: sample.disk_used_bytes,
            disk_total_bytes: sample.disk_total_bytes,
            fsync_p99_ms,
        }
    }
}

fn channel_snap(c: &ChannelStats) -> ChannelSnap {
    ChannelSnap {
        config: c.config.clone(),
        partitions: c
            .partitions
            .iter()
            .map(|p| PartitionSnap {
                partition: p.partition,
                low_offset: p.low_offset,
                high_offset: p.high_offset,
                storage_bytes: p.storage_bytes,
                segments: p.segments,
                published_total: p.published_total,
            })
            .collect(),
        counters: [
            c.published_total,
            c.published_bytes_total,
            c.delivered_total,
            c.delivered_bytes_total,
            c.acked_total,
            c.nacked_total,
            c.timed_out_total,
            c.retries_total,
            c.dead_lettered_total,
        ],
        dlq_size: c.dlq_size,
        publish_latency: Hist::encode(&c.publish_latency_us),
        delivery_latency: Hist::encode(&c.delivery_latency_us),
        groups: c
            .groups
            .iter()
            .map(|g| GroupSnap {
                name: g.name.clone(),
                lag: g.lag,
                in_flight: g.in_flight,
                retry_pending: g.retry_pending,
                counters: [
                    g.delivered_total,
                    g.acked_total,
                    g.nacked_total,
                    g.dead_lettered_total,
                ],
                processing_latency: Hist::encode(&g.processing_latency_us),
                instances: g
                    .instances
                    .iter()
                    .map(|i| InstanceSnap {
                        id: i.id.clone(),
                        subscription_id: i.subscription_id,
                        client_name: i.client_name.clone(),
                        remote_addr: i.remote_addr.clone(),
                        connected_at_ms: i.connected_at_ms,
                        prefetch: i.prefetch,
                        in_flight: i.in_flight,
                        assigned_partitions: i.assigned_partitions.clone(),
                        lag: i.lag,
                        counters: [
                            i.delivered_total,
                            i.acked_total,
                            i.nacked_total,
                            i.retries_total,
                        ],
                        processing_latency: Hist::encode(&i.processing_latency_us),
                        last_error: i.last_error.clone(),
                        last_error_at_ms: i.last_error_at_ms,
                        client_stats: i.client_stats.clone(),
                    })
                    .collect(),
            })
            .collect(),
        oldest_timestamp_ms: c.oldest_timestamp_ms,
    }
}

fn channel_stats(c: ChannelSnap) -> Result<ChannelStats, String> {
    let [pb, pbb, d, db, a, n, t, r, dl] = c.counters;
    Ok(ChannelStats {
        // Node-local (not carried in cluster snapshots), like retention skips.
        duplicates_total: 0,
        config: c.config,
        partitions: c
            .partitions
            .into_iter()
            .map(|p| PartitionStats {
                partition: p.partition,
                low_offset: p.low_offset,
                high_offset: p.high_offset,
                storage_bytes: p.storage_bytes,
                segments: p.segments,
                published_total: p.published_total,
            })
            .collect(),
        published_total: pb,
        published_bytes_total: pbb,
        delivered_total: d,
        delivered_bytes_total: db,
        acked_total: a,
        nacked_total: n,
        timed_out_total: t,
        retries_total: r,
        dead_lettered_total: dl,
        dlq_size: c.dlq_size,
        publish_latency_us: c.publish_latency.decode()?,
        delivery_latency_us: c.delivery_latency.decode()?,
        groups: c
            .groups
            .into_iter()
            .map(|g| {
                let [d, a, n, dl] = g.counters;
                Ok(GroupStats {
                    name: g.name,
                    lag: g.lag,
                    in_flight: g.in_flight,
                    retry_pending: g.retry_pending,
                    delivered_total: d,
                    acked_total: a,
                    nacked_total: n,
                    dead_lettered_total: dl,
                    // Node-local metric, not part of the snapshot wire format.
                    retention_skipped_total: 0,
                    catchup_read_bytes_total: 0,
                    processing_latency_us: g.processing_latency.decode()?,
                    instances: g
                        .instances
                        .into_iter()
                        .map(|i| {
                            let [d, a, n, r] = i.counters;
                            Ok(InstanceStats {
                                id: i.id,
                                subscription_id: i.subscription_id,
                                client_name: i.client_name,
                                remote_addr: i.remote_addr,
                                connected_at_ms: i.connected_at_ms,
                                prefetch: i.prefetch,
                                in_flight: i.in_flight,
                                assigned_partitions: i.assigned_partitions,
                                lag: i.lag,
                                delivered_total: d,
                                acked_total: a,
                                nacked_total: n,
                                retries_total: r,
                                processing_latency_us: i.processing_latency.decode()?,
                                last_error: i.last_error,
                                last_error_at_ms: i.last_error_at_ms,
                                client_stats: i.client_stats,
                            })
                        })
                        .collect::<Result<_, String>>()?,
                })
            })
            .collect::<Result<_, String>>()?,
        oldest_timestamp_ms: c.oldest_timestamp_ms,
    })
}

// ---------------------------------------------------------------------------------------
// Merge
// ---------------------------------------------------------------------------------------

/// One node's input to [`merge`].
#[derive(Debug, Clone, Copy)]
pub struct NodeInput<'a> {
    pub stats: &'a BrokerStats,
    pub leads: Option<&'a Leads>,
}

impl NodeInput<'_> {
    fn leads_channel(&self, channel: &str) -> bool {
        self.leads
            .is_none_or(|l| l.get(channel).is_some_and(|p| !p.is_empty()))
    }

    fn leads_partition(&self, channel: &str, partition: u32) -> bool {
        self.leads
            .is_none_or(|l| l.get(channel).is_some_and(|p| p.contains(&partition)))
    }
}

fn add_hist(into: &mut LatencyHistogram, from: &LatencyHistogram) {
    if !from.is_empty()
        && let Err(e) = into.add(from)
    {
        tracing::debug!(error = ?e, "histogram merge failed");
    }
}

fn empty_channel(config: &ChannelConfig) -> ChannelStats {
    ChannelStats {
        config: config.clone(),
        partitions: Vec::new(),
        published_total: 0,
        duplicates_total: 0,
        published_bytes_total: 0,
        delivered_total: 0,
        delivered_bytes_total: 0,
        acked_total: 0,
        nacked_total: 0,
        timed_out_total: 0,
        retries_total: 0,
        dead_lettered_total: 0,
        dlq_size: 0,
        publish_latency_us: new_latency_histogram(),
        delivery_latency_us: new_latency_histogram(),
        groups: Vec::new(),
        oldest_timestamp_ms: None,
    }
}

/// Merges the local node (first) and the live peers into cluster-wide stats (see module docs).
/// Node identity (`node_id`, `started_at_ms`, `taken_at_ms`) is the local node's.
pub fn merge(local: NodeInput<'_>, peers: &[NodeInput<'_>]) -> BrokerStats {
    let mut out = BrokerStats {
        taken_at_ms: local.stats.taken_at_ms,
        node_id: local.stats.node_id.clone(),
        cluster_name: local.stats.cluster_name.clone(),
        started_at_ms: local.stats.started_at_ms,
        connections: 0,
        channels: Vec::new(),
        fsync_latency_us: local.stats.fsync_latency_us.clone(),
        commit_latency_us: local.stats.commit_latency_us.clone(),
        storage_bytes: 0,
        storage_capacity_bytes: 0,
    };
    let mut order: Vec<String> = Vec::new();
    let mut channels: HashMap<String, ChannelStats> = HashMap::new();
    // channel → partition → (claimed by its leader, stats)
    let mut parts: HashMap<String, BTreeMap<u32, (bool, PartitionStats)>> = HashMap::new();

    for node in std::iter::once(&local).chain(peers.iter()) {
        let s = node.stats;
        out.connections = out.connections.saturating_add(s.connections);
        out.storage_bytes = out.storage_bytes.saturating_add(s.storage_bytes);
        out.storage_capacity_bytes = out
            .storage_capacity_bytes
            .saturating_add(s.storage_capacity_bytes);
        let prefix = &s.node_id;
        for c in &s.channels {
            let name = &c.config.name;
            let entry = channels.entry(name.clone()).or_insert_with(|| {
                order.push(name.clone());
                empty_channel(&c.config)
            });
            let pmap = parts.entry(name.clone()).or_default();
            for p in &c.partitions {
                let leader = node.leads_partition(name, p.partition);
                let replace = match pmap.get(&p.partition) {
                    None => true,
                    Some((was_leader, prev)) => {
                        (leader && !was_leader)
                            || (leader == *was_leader && p.high_offset > prev.high_offset)
                    }
                };
                if replace {
                    pmap.insert(p.partition, (leader, p.clone()));
                }
            }
            if !node.leads_channel(name) {
                continue;
            }
            entry.published_total += c.published_total;
            entry.published_bytes_total += c.published_bytes_total;
            entry.delivered_total += c.delivered_total;
            entry.delivered_bytes_total += c.delivered_bytes_total;
            entry.acked_total += c.acked_total;
            entry.nacked_total += c.nacked_total;
            entry.timed_out_total += c.timed_out_total;
            entry.retries_total += c.retries_total;
            entry.dead_lettered_total += c.dead_lettered_total;
            entry.dlq_size += c.dlq_size;
            add_hist(&mut entry.publish_latency_us, &c.publish_latency_us);
            add_hist(&mut entry.delivery_latency_us, &c.delivery_latency_us);
            entry.oldest_timestamp_ms = match (entry.oldest_timestamp_ms, c.oldest_timestamp_ms) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            for g in &c.groups {
                let pos = match entry.groups.iter().position(|x| x.name == g.name) {
                    Some(p) => p,
                    None => {
                        entry.groups.push(GroupStats {
                            name: g.name.clone(),
                            lag: 0,
                            in_flight: 0,
                            retry_pending: 0,
                            delivered_total: 0,
                            acked_total: 0,
                            nacked_total: 0,
                            dead_lettered_total: 0,
                            retention_skipped_total: 0,
                            catchup_read_bytes_total: 0,
                            processing_latency_us: new_latency_histogram(),
                            instances: Vec::new(),
                        });
                        entry.groups.len() - 1
                    }
                };
                let mg = &mut entry.groups[pos];
                mg.lag += g.lag;
                mg.in_flight += g.in_flight;
                mg.retry_pending += g.retry_pending;
                mg.delivered_total += g.delivered_total;
                mg.acked_total += g.acked_total;
                mg.nacked_total += g.nacked_total;
                mg.dead_lettered_total += g.dead_lettered_total;
                mg.retention_skipped_total += g.retention_skipped_total;
                mg.catchup_read_bytes_total += g.catchup_read_bytes_total;
                add_hist(&mut mg.processing_latency_us, &g.processing_latency_us);
                for i in &g.instances {
                    let mut i = i.clone();
                    i.id = format!("{prefix}/{}", i.id);
                    mg.instances.push(i);
                }
            }
        }
    }

    for name in order {
        let Some(mut c) = channels.remove(&name) else {
            continue;
        };
        c.partitions = parts
            .remove(&name)
            .unwrap_or_default()
            .into_values()
            .map(|(_, p)| p)
            .collect();
        out.channels.push(c);
    }
    out
}

/// What determines the composition of a merged snapshot; a change means cumulative values are
/// not comparable with the previous ones (the rate window must restart).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Signature(Vec<(String, u64, Option<Leads>)>);

impl Signature {
    pub fn of(nodes: &[NodeInput<'_>]) -> Signature {
        let mut v: Vec<_> = nodes
            .iter()
            .map(|n| {
                (
                    n.stats.node_id.clone(),
                    n.stats.started_at_ms,
                    n.leads.cloned(),
                )
            })
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        Signature(v)
    }
}

/// Encodes this node's snapshot from the collector's latest local sample (cached per sample).
pub(crate) fn node_stats(shared: &crate::hub::Shared) -> Option<std::sync::Arc<Vec<u8>>> {
    let local = shared.local_stats.read().clone()?;
    if let Some((ts, bytes)) = shared.snapshot_cache.lock().as_ref()
        && *ts == local.taken_at_ms
    {
        return Some(bytes.clone());
    }
    let (health, fsync_p99) = shared
        .latest
        .read()
        .as_ref()
        .map_or((Health::Healthy, 0.0), |l| {
            (l.health.node, l.derived.fsync_p99_ms)
        });
    let sample = *shared.node_sample.read();
    let leads = shared
        .cluster
        .read()
        .as_ref()
        .and_then(|c| c.facts().local_leads);
    let meta = NodeMeta::local(&local, &shared.address, &sample, fsync_p99, health);
    let mut snap = NodeStatsSnapshot::build(&local, meta, leads);
    let extra = shared.extra_alerts.read().clone();
    if let Some(source) = extra {
        snap.alerts = source(fluxmq_proto::now_ms());
    }
    let bytes = match snap.encode() {
        Ok(b) => std::sync::Arc::new(b),
        Err(e) => {
            tracing::error!(error = %e, "could not encode node stats");
            return None;
        }
    };
    *shared.snapshot_cache.lock() = Some((local.taken_at_ms, bytes.clone()));
    Some(bytes)
}
