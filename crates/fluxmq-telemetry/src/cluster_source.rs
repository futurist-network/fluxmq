//! Cluster-wide aggregation: [`ClusterSource`] pulls every peer's [`NodeStatsSnapshot`]
//! (`GET /internal/v1/node-stats`) once per second and merges them with the local stats.
//!
//! * Pulls run on their own task with a per-request timeout, never on the collector tick: the
//!   collector only reads the latest decoded snapshots ([`ClusterSource::view`] is a pure merge).
//! * A peer whose last successful pull is older than [`STALE_AFTER`] (or that the controller
//!   reports `down`) is **down**: excluded from the merge, listed in `nodes` with
//!   `status = down` / `health = critical`, and the cluster figures are flagged `partial`.
//! * Whenever the set of merged nodes changes (peer down/up, restart, leadership move) the view's
//!   `generation` increases so the collector restarts its rate window (no spikes).
//! * Single node (`!cluster.is_clustered()`): nothing runs and the collector never calls in —
//!   exactly the v0.1 path.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use fluxmq_cluster::Cluster;
use fluxmq_core::{BrokerEvent, BrokerStats};
use fluxmq_proto::cluster::{NodeInfo, NodeStatus};
use fluxmq_proto::model::ChannelConfig;
use fluxmq_proto::telemetry::{Health, NodeState};
use parking_lot::{Mutex, RwLock};
use tokio::sync::broadcast;

use crate::snapshot::{DecodedSnapshot, Leads, NodeInput, NodeStatsSnapshot, Signature, merge};
use crate::source::{Source, TailFeed};

/// Pull cadence.
pub const POLL_EVERY: Duration = Duration::from_secs(1);
/// Per-pull timeout.
pub const PULL_TIMEOUT: Duration = Duration::from_millis(900);
/// A peer without a successful pull for this long is considered down.
pub const STALE_AFTER: Duration = Duration::from_millis(3_500);
/// Path of the node-stats endpoint.
pub const NODE_STATS_PATH: &str = "/internal/v1/node-stats";

/// Last known state of one peer.
#[derive(Debug, Clone)]
pub struct PeerEntry {
    pub info: NodeInfo,
    pub last: Option<Arc<DecodedSnapshot>>,
    /// Local wall-clock time of the last successful pull.
    pub last_ok_ms: Option<u64>,
    pub last_error: Option<String>,
}

impl PeerEntry {
    pub fn new(info: NodeInfo) -> Self {
        Self {
            info,
            last: None,
            last_ok_ms: None,
            last_error: None,
        }
    }

    /// Up = fresh snapshot and not reported down by the controller.
    pub fn is_live(&self, now_ms: u64) -> bool {
        self.info.status != NodeStatus::Down
            && self.last.is_some()
            && self
                .last_ok_ms
                .is_some_and(|t| now_ms.saturating_sub(t) <= STALE_AFTER.as_millis() as u64)
    }
}

/// Cluster-level facts refreshed by the poll task (from `Cluster::status()`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClusterFacts {
    pub controller: Option<String>,
    pub local_is_controller: bool,
    pub under_replicated: u32,
    pub offline: u32,
    pub replication_factor: u32,
    pub raft_term: u64,
    pub metadata_epoch: u64,
    /// Partitions led by this node.
    pub local_leads: Option<Leads>,
    /// The current (or last) rebalance plan, replicated (v0.4; events feed).
    pub rebalance: Option<fluxmq_cluster::rebalance::PlanDoc>,
    /// Current and recent decommission / forget records, replicated (v0.5; events feed).
    pub decommissions: Vec<fluxmq_cluster::DecommissionRecord>,
}

/// Peer table + facts. Pure over `now_ms` so it is unit-testable.
#[derive(Debug, Default)]
pub struct ClusterState {
    pub peers: BTreeMap<String, PeerEntry>,
    pub facts: ClusterFacts,
}

/// What the collector needs besides the merged stats.
#[derive(Debug, Clone, Default)]
pub struct ClusterInfo {
    pub generation: u64,
    /// Peers (excluding this node), down ones included.
    pub peers: Vec<NodeState>,
    /// Peers currently down (sorted).
    pub down: Vec<String>,
    /// Peers whose data is missing from the merge.
    pub partial: bool,
    pub facts: ClusterFacts,
    /// Per-peer detail for down nodes: (id, seconds since last success, last error).
    pub down_detail: Vec<(String, Option<u64>, Option<String>)>,
    /// Node-local alert candidates of the live peers (v0.4: TLS), ids carry the node id.
    pub peer_alerts: Vec<fluxmq_proto::telemetry::Alert>,
}

/// Merged stats + cluster info for one collector tick.
pub struct ClusterView {
    pub stats: BrokerStats,
    pub info: ClusterInfo,
}

impl ClusterState {
    /// Replaces the peer list (keeps known snapshots of peers still present).
    pub fn set_members(&mut self, members: Vec<NodeInfo>) {
        let mut next = BTreeMap::new();
        for info in members {
            let mut e = self
                .peers
                .remove(&info.id)
                .unwrap_or_else(|| PeerEntry::new(info.clone()));
            e.info = info;
            next.insert(e.info.id.clone(), e);
        }
        self.peers = next;
    }

    pub fn record_ok(&mut self, id: &str, snap: DecodedSnapshot, now_ms: u64) {
        if let Some(e) = self.peers.get_mut(id) {
            e.last = Some(Arc::new(snap));
            e.last_ok_ms = Some(now_ms);
            e.last_error = None;
        }
    }

    pub fn record_err(&mut self, id: &str, error: String) {
        if let Some(e) = self.peers.get_mut(id) {
            e.last_error = Some(error);
        }
    }

    /// Merges `local` with the live peers. `sig`/`generation` carry the change detector.
    pub fn view(
        &self,
        local: &BrokerStats,
        now_ms: u64,
        tracker: &mut (Signature, u64),
    ) -> ClusterView {
        let local_input = NodeInput {
            stats: local,
            leads: self.facts.local_leads.as_ref(),
        };
        let mut inputs = Vec::new();
        let mut peers = Vec::new();
        let mut down = Vec::new();
        let mut down_detail = Vec::new();
        let mut peer_alerts = Vec::new();
        for (id, e) in &self.peers {
            let live = e.is_live(now_ms);
            let is_controller = self.facts.controller.as_deref() == Some(id.as_str());
            match (&e.last, live) {
                (Some(snap), true) => {
                    inputs.push(NodeInput {
                        stats: &snap.stats,
                        leads: snap.leads.as_ref(),
                    });
                    peers.push(peer_state(e, snap, is_controller));
                    peer_alerts.extend(snap.alerts.iter().cloned());
                }
                _ => {
                    down.push(id.clone());
                    down_detail.push((
                        id.clone(),
                        e.last_ok_ms.map(|t| now_ms.saturating_sub(t) / 1000),
                        e.last_error.clone(),
                    ));
                    peers.push(down_state(e, is_controller));
                }
            }
        }
        let mut all = vec![local_input];
        all.extend(inputs.iter().copied());
        let sig = Signature::of(&all);
        if sig != tracker.0 {
            tracker.0 = sig;
            tracker.1 += 1;
        }
        let stats = merge(local_input, &inputs);
        ClusterView {
            stats,
            info: ClusterInfo {
                generation: tracker.1,
                partial: !down.is_empty(),
                peers,
                down,
                facts: self.facts.clone(),
                down_detail,
                peer_alerts,
            },
        }
    }
}

fn peer_state(e: &PeerEntry, snap: &DecodedSnapshot, is_controller: bool) -> NodeState {
    let m = &snap.meta;
    NodeState {
        id: e.info.id.clone(),
        address: if m.address.is_empty() {
            e.info.client_addr.clone()
        } else {
            m.address.clone()
        },
        health: m.health,
        version: m.version.clone(),
        uptime_secs: m.taken_at_ms.saturating_sub(m.started_at_ms) / 1000,
        cpu_percent: (f64::from(m.cpu_percent) * 10.0).round() / 10.0,
        memory_bytes: m.memory_bytes,
        memory_total_bytes: m.memory_total_bytes,
        disk_used_bytes: m.disk_used_bytes,
        disk_total_bytes: m.disk_total_bytes,
        disk_latency_p99_ms: m.fsync_p99_ms,
        connections: snap.stats.connections,
        status: e.info.status,
        is_controller,
    }
}

fn down_state(e: &PeerEntry, is_controller: bool) -> NodeState {
    let last = e.last.as_ref().map(|s| &s.meta);
    NodeState {
        id: e.info.id.clone(),
        address: last
            .map(|m| m.address.clone())
            .filter(|a| !a.is_empty())
            .unwrap_or_else(|| e.info.client_addr.clone()),
        health: Health::Critical,
        version: last.map_or_else(|| e.info.version.clone(), |m| m.version.clone()),
        uptime_secs: 0,
        cpu_percent: 0.0,
        memory_bytes: 0,
        memory_total_bytes: last.map_or(0, |m| m.memory_total_bytes),
        disk_used_bytes: last.map_or(0, |m| m.disk_used_bytes),
        disk_total_bytes: last.map_or(0, |m| m.disk_total_bytes),
        disk_latency_p99_ms: 0.0,
        connections: 0,
        status: NodeStatus::Down,
        is_controller,
    }
}

/// The cluster-aware [`Source`]: local broker + peers' snapshots.
pub struct ClusterSource {
    local: Arc<dyn Source>,
    cluster: Cluster,
    state: RwLock<ClusterState>,
    tracker: Mutex<(Signature, u64)>,
}

impl ClusterSource {
    pub fn new(local: Arc<dyn Source>, cluster: Cluster) -> Self {
        Self {
            local,
            cluster,
            state: RwLock::new(ClusterState::default()),
            tracker: Mutex::new((Signature::default(), 0)),
        }
    }

    pub fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    pub fn is_clustered(&self) -> bool {
        self.cluster.is_clustered()
    }

    /// Merged view over `local` (already sampled by the caller).
    pub fn view(&self, local: &BrokerStats, now_ms: u64) -> ClusterView {
        let state = self.state.read();
        let mut tracker = self.tracker.lock();
        state.view(local, now_ms, &mut tracker)
    }

    /// Latest facts (controller, partition health, local leads).
    pub fn facts(&self) -> ClusterFacts {
        self.state.read().facts.clone()
    }

    /// Number of peers and how many are live.
    pub fn peer_counts(&self, now_ms: u64) -> (u32, u32) {
        let s = self.state.read();
        let live = s.peers.values().filter(|e| e.is_live(now_ms)).count();
        (s.peers.len() as u32, live as u32)
    }

    /// Refreshes membership + facts from the cluster (cheap, synchronous).
    fn refresh_facts(&self) {
        let status = self.cluster.status();
        let me = self.cluster.node_id().to_string();
        let local_leads: Leads = status
            .metadata
            .channels
            .iter()
            .map(|(ch, parts)| {
                let led: Vec<u32> = parts
                    .iter()
                    .filter(|p| p.leader.as_deref() == Some(me.as_str()))
                    .map(|p| p.partition)
                    .collect();
                (ch.clone(), led)
            })
            .collect();
        let controller = status
            .metadata
            .controller
            .clone()
            .or_else(|| status.raft.leader.clone());
        let facts = ClusterFacts {
            local_is_controller: controller.as_deref() == Some(me.as_str()),
            controller,
            under_replicated: status.under_replicated_partitions,
            offline: status.offline_partitions,
            replication_factor: status.replication_factor,
            raft_term: status.raft.term,
            metadata_epoch: status.metadata.epoch,
            local_leads: Some(local_leads),
            rebalance: self.cluster.rebalance_plan(),
            decommissions: self.cluster.decommission_records(),
        };
        let peers = self.cluster.peers();
        let mut st = self.state.write();
        st.facts = facts;
        st.set_members(peers);
    }

    /// The pull loop. Exits when `shutdown` flips.
    pub(crate) async fn run(self: Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        // The cluster's internal client: `https` + node certificate with cluster TLS, and the
        // cluster secret header (a plain client here broke every pull in a TLS cluster — peers
        // showed as down).
        let client = self.cluster.internal_http().with_timeout(PULL_TIMEOUT);
        let mut interval = tokio::time::interval(POLL_EVERY);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = shutdown.changed() => {}
            }
            if *shutdown.borrow() {
                return;
            }
            if !self.cluster.is_clustered() {
                continue;
            }
            self.refresh_facts();
            let targets: Vec<(String, String)> = self
                .state
                .read()
                .peers
                .values()
                .map(|e| (e.info.id.clone(), e.info.http_addr.clone()))
                .collect();
            let mut set = tokio::task::JoinSet::new();
            for (id, addr) in targets {
                let client = client.clone();
                set.spawn(async move { (id, pull(&client, &addr).await) });
            }
            while let Some(res) = set.join_next().await {
                let Ok((id, result)) = res else { continue };
                let now = fluxmq_proto::now_ms();
                match result {
                    Ok(snap) => self.state.write().record_ok(&id, snap, now),
                    Err(e) => {
                        tracing::debug!(peer = %id, error = %e, "node-stats pull failed");
                        self.state.write().record_err(&id, e);
                    }
                }
            }
        }
    }
}

/// Pulls one peer's snapshot (`addr` = its advertised `host:port` HTTP address) through the
/// cluster's internal client (scheme, TLS identity and secret come from it).
pub(crate) async fn pull(
    client: &fluxmq_cluster::InternalHttp,
    addr: &str,
) -> Result<DecodedSnapshot, String> {
    if addr.is_empty() {
        return Err("peer has no HTTP address".into());
    }
    let body = client
        .get_bytes(addr, NODE_STATS_PATH)
        .await
        .map_err(|e| match e {
            fluxmq_cluster::TransportError::Status(code, _) => format!("HTTP {code}"),
            e => e.to_string(),
        })?;
    // Decoding histograms is CPU work proportional to the peer's entity count: keep it off the
    // async workers.
    tokio::task::spawn_blocking(move || NodeStatsSnapshot::decode(&body)?.into_decoded())
        .await
        .map_err(|e| e.to_string())?
}

impl Source for ClusterSource {
    fn stats(&self) -> BrokerStats {
        let local = self.local.stats();
        if !self.is_clustered() {
            return local;
        }
        self.view(&local, fluxmq_proto::now_ms()).stats
    }
    fn channel(&self, name: &str) -> Option<ChannelConfig> {
        self.local.channel(name)
    }
    fn events(&self) -> Option<broadcast::Receiver<BrokerEvent>> {
        self.local.events()
    }
    fn tail(&self, channel: &str) -> Result<Box<dyn TailFeed>, String> {
        self.local.tail(channel)
    }
}
