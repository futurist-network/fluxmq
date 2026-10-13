//! Cluster membership, Raft-replicated metadata, partition assignment and failover
//! (roadmap stage 9). Model: `fluxmq_proto::cluster` module docs; design: `docs/design/v0.2.md`.
//!
//! * Every node is a Raft member (openraft 0.9.x); the Raft leader is the **controller**.
//! * The Raft state machine implements [`fluxmq_meta::MetaStore`] — so auth tokens, routes,
//!   channel configs (`cluster/channels/<name>`), assignments (`cluster/assignments/<name>`)
//!   and the node registry (`cluster/nodes/<id>`) are replicated and identical everywhere.
//! * The controller assigns partition leaders/replicas (balanced, rack-unaware v1), reacts to
//!   node liveness (heartbeats) and ISR changes, and drives [`fluxmq_replication`] roles on
//!   every node by publishing assignments; each node applies its own roles locally.
//! * Single node = [`Cluster::single_node`]: local meta store, this node leads everything,
//!   no Raft, no replication — v0.1 behaviour, zero config.
//!
//! Module map: [`raft`] (openraft types, persistent log, KV state machine, transport),
//! `meta` (the Raft-backed `MetaStore`), [`assign`] (pure placement/failover logic),
//! `controller` (runs on the Raft leader), `decommission` (v0.5 node removal, on the
//! controller), `reconcile` (per node), `runtime` (startup,
//! heartbeats + leases, bootstrap), `server` (cluster RPC listener), [`rpc`] (node-to-node
//! HTTP client). Operations guide: `docs/CLUSTER.md`.

pub mod assign;
mod controller;
mod decommission;
mod meta;
mod publish;
pub mod raft;
pub mod rebalance;
mod rebalancer;
mod reconcile;
pub mod rpc;
mod runtime;
mod server;

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use fluxmq_core::{Broker, CoreError};
use fluxmq_meta::{
    LocalMetaStore, MetaError, MetaStore, MetaStoreExt, Precondition, SharedMetaStore,
};
use fluxmq_proto::api::UpdateChannelRequest;
use fluxmq_proto::cluster::{
    ClusterMetadata, ClusterStatus, DecommissionStatus, NodeInfo, NodeStatus, PartitionAssignment,
    RaftStatus, RebalanceConfig, RebalanceMode, RebalanceRequest, RebalanceStatus,
};
use tokio::sync::watch;
use tokio::time::Instant;

/// Max wait for a channel committed cluster-wide to appear on this node.
pub const LOCAL_CHANNEL_WAIT: Duration = Duration::from_secs(3);

pub use publish::cluster_err as wire_error;
pub use rpc::{InternalHttp, NodeRecord, RebalanceAction, TransportError};
pub use runtime::REMOVED_MARKER;

use crate::controller::{ASSIGNMENTS_PREFIX, NODES_PREFIX, assignment_key, channel_key};
use crate::rpc::{ControllerOk, ControllerOp, RpcError, RpcErrorKind};
use crate::runtime::{CONTROLLER_PATH, Runtime};

/// Default `node_timeout`: a node is declared `Down` this long after its last heartbeat.
pub const DEFAULT_NODE_TIMEOUT: Duration = Duration::from_secs(6);
/// How long API calls wait for the controller / local application of a change.
const OP_DEADLINE: Duration = Duration::from_secs(10);
/// `GET /api/cluster/rebalance` asks the controller (live lag, storage) for at most this long,
/// then answers from the local replica of the plan.
const REBALANCE_STATUS_DEADLINE: Duration = Duration::from_secs(3);

/// HTTP header carrying the cluster secret on `/internal/*` requests between nodes.
pub const CLUSTER_SECRET_HEADER: &str = "x-fluxmq-cluster-secret";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerSpec {
    pub id: String,
    /// Raft address of the peer.
    pub raft_addr: String,
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub node_id: String,
    pub cluster_name: String,
    pub data_dir: PathBuf,
    pub raft_addr: SocketAddr,
    pub replication_addr: SocketAddr,
    /// Addresses other nodes/clients use to reach this node.
    pub advertise_client_addr: String,
    pub advertise_http_addr: String,
    pub advertise_raft_addr: String,
    pub advertise_replication_addr: String,
    /// Initial voters (may include this node). Bootstrapping happens once, by the node with the
    /// lowest id among `peers` when no Raft state exists yet.
    pub peers: Vec<PeerSpec>,
    /// Default for new channels; capped at the number of live nodes.
    pub replication_factor: u32,
    pub min_isr: u32,
    /// Shared secret for Raft RPC, replication and `/internal/*` (required when clustered).
    pub cluster_secret: Option<String>,
    /// Liveness timeout (default [`DEFAULT_NODE_TIMEOUT`]); heartbeat interval and leases
    /// derive from it.
    pub node_timeout: Duration,
    /// Cluster mutual TLS (v0.4) for Raft RPC, replication and internal HTTP; `None` = plain.
    pub tls: Option<fluxmq_tls::ClusterTls>,
    /// v0.4: rebalancing mode written to `cluster/rebalance/config` when that key is absent
    /// (`--auto-rebalance`); afterwards the API is the source of truth.
    pub auto_rebalance: RebalanceMode,
    /// v0.4: automatic rebalance triggers are evaluated at most this often
    /// ([`DEFAULT_REBALANCE_INTERVAL`]; shorter in tests).
    pub rebalance_interval: Duration,
}

/// Default [`ClusterConfig::rebalance_interval`].
pub const DEFAULT_REBALANCE_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error(transparent)]
    Meta(#[from] MetaError),
    #[error(transparent)]
    Core(#[from] fluxmq_core::CoreError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("raft: {0}")]
    Raft(String),
    #[error("not the controller (controller: {0:?})")]
    NotController(Option<String>),
    #[error("invalid: {0}")]
    Invalid(String),
    /// The operation conflicts with the current state (e.g. a rebalance is already running).
    #[error("conflict: {0}")]
    Conflict(String),
    /// The named subject does not exist (e.g. an unknown channel in a rebalance request).
    #[error("not found: {0}")]
    NotFound(String),
}

/// Where a partition is served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionRoute {
    Local,
    Remote(NodeInfo),
    /// No leader right now (election/failover in progress) — retry shortly.
    Unavailable,
}

/// Cheap to clone.
#[derive(Clone)]
pub struct Cluster {
    inner: Arc<Inner>,
}

struct Inner {
    node: NodeInfo,
    cluster_name: String,
    broker: Broker,
    meta: SharedMetaStore,
    epoch: watch::Sender<u64>,
    /// Clustered mode only.
    rt: Option<Arc<Runtime>>,
}

impl Cluster {
    /// Zero-config single node: meta store at `data_dir/meta.json`.
    pub async fn single_node(
        broker: Broker,
        data_dir: &Path,
        cluster_name: &str,
        client_addr: String,
        http_addr: String,
    ) -> Result<Self, ClusterError> {
        let path = data_dir.join("meta.json");
        let meta = tokio::task::spawn_blocking(move || LocalMetaStore::open(path))
            .await
            .map_err(|e| ClusterError::Raft(e.to_string()))??;
        Ok(Self::with_meta(
            broker,
            Arc::new(meta),
            cluster_name,
            client_addr,
            http_addr,
        ))
    }

    /// Memory-only single node (tests, `AppState::new` defaults).
    pub fn standalone(broker: Broker) -> Self {
        Self::with_meta(
            broker,
            Arc::new(LocalMetaStore::in_memory()),
            "local",
            String::new(),
            String::new(),
        )
    }

    fn with_meta(
        broker: Broker,
        meta: SharedMetaStore,
        cluster_name: &str,
        client_addr: String,
        http_addr: String,
    ) -> Self {
        let node = NodeInfo {
            id: broker.node_id().to_string(),
            client_addr,
            http_addr,
            status: NodeStatus::Up,
            is_controller: true,
            is_voter: true,
            version: fluxmq_proto::VERSION.to_string(),
        };
        let (epoch, _) = watch::channel(0);
        Self {
            inner: Arc::new(Inner {
                node,
                cluster_name: cluster_name.to_string(),
                broker,
                meta,
                epoch,
                rt: None,
            }),
        }
    }

    /// Multi-node: starts Raft, the replication manager, membership and the controller loop.
    ///
    /// Returns as soon as the node runs; it serves partitions once it holds a controller
    /// lease and has applied its roles (until then partition requests get `NotLeader`).
    pub async fn start(config: ClusterConfig, broker: Broker) -> Result<Self, ClusterError> {
        let node = NodeInfo {
            id: config.node_id.clone(),
            client_addr: config.advertise_client_addr.clone(),
            http_addr: config.advertise_http_addr.clone(),
            status: NodeStatus::Up,
            is_controller: false,
            is_voter: false,
            version: fluxmq_proto::VERSION.to_string(),
        };
        let cluster_name = config.cluster_name.clone();
        let (epoch, _) = watch::channel(0);
        let rt = runtime::start(config, broker.clone(), epoch).await?;
        let meta: SharedMetaStore = rt.meta.clone();
        let (dummy, _) = watch::channel(0);
        Ok(Self {
            inner: Arc::new(Inner {
                node,
                cluster_name,
                broker,
                meta,
                epoch: dummy,
                rt: Some(rt),
            }),
        })
    }

    pub fn is_clustered(&self) -> bool {
        self.inner.rt.is_some()
    }

    /// Shared secret for node-to-node calls (`/internal/*`, Raft, replication), from
    /// `--cluster-secret`. Sent in the [`CLUSTER_SECRET_HEADER`] header (never `Authorization`,
    /// which belongs to RBAC). `None` for a single node.
    pub fn cluster_secret(&self) -> Option<&str> {
        self.inner.rt.as_ref()?.cfg.cluster_secret.as_deref()
    }

    /// Authenticated client for other nodes' `/internal/v1/*` routes.
    pub fn internal_http(&self) -> InternalHttp {
        let rpc = match &self.inner.rt {
            Some(rt) => rt.rpc.clone(),
            None => rpc::RpcClient::new(None),
        };
        InternalHttp {
            rpc,
            timeout: Duration::from_secs(5),
        }
    }

    pub fn node_id(&self) -> &str {
        &self.inner.node.id
    }

    pub(crate) fn broker(&self) -> &Broker {
        &self.inner.broker
    }

    /// Publishes like a client `Publish` request: `partition: None` picks partitions per
    /// message (key hash / round-robin) and, when clustered, forwards to partition leaders;
    /// `Some(p)` is served only by `p`'s leader (`NotLeader` otherwise). Single node without a
    /// partition = `Broker::publish` (v0.1 behaviour).
    pub async fn publish(
        &self,
        channel: &str,
        partition: Option<u32>,
        messages: Vec<fluxmq_proto::PublishMessage>,
    ) -> Result<Vec<fluxmq_proto::MessageId>, fluxmq_proto::wire::WireError> {
        match partition {
            None if !self.is_clustered() => self
                .inner
                .broker
                .publish(channel, messages)
                .await
                .map_err(|e| fluxmq_proto::wire::WireError::from(&e)),
            None => publish::publish_routed(self, channel, messages).await,
            Some(p) => publish::publish_partition(self, channel, p, messages).await,
        }
    }

    /// v0.7 `PublishBatch`: `partition: Some(p)` is served only by `p`'s leader (`NotLeader`
    /// otherwise); `None` routes the whole batch to one partition (key hash, or sticky when
    /// keyless; a keyed batch spanning partitions is refused) and forwards it raw to that
    /// partition's leader. Returns `(partition, base_offset, count)`. Single node delegates
    /// to `Broker::publish_batch`.
    pub async fn publish_batch(
        &self,
        channel: &str,
        partition: Option<u32>,
        records: fluxmq_proto::batch::RecordsSection,
    ) -> Result<(u32, u64, u32), fluxmq_proto::wire::WireError> {
        if !self.is_clustered() {
            return self
                .inner
                .broker
                .publish_batch(channel, partition, records)
                .await
                .map_err(|e| fluxmq_proto::wire::WireError::from(&e));
        }
        match partition {
            None => publish::publish_batch_routed(self, channel, records).await,
            Some(p) => publish::publish_batch_partition(self, channel, p, records)
                .await
                .map(|(base, count)| (p, base, count)),
        }
    }

    /// Bound cluster RPC (Raft) address when clustered.
    pub fn raft_addr(&self) -> Option<SocketAddr> {
        self.inner.rt.as_ref().map(|rt| rt.raft_local_addr)
    }

    /// Bound replication listener address when clustered.
    pub fn replication_addr(&self) -> Option<SocketAddr> {
        self.inner.rt.as_ref().map(|rt| rt.replication.local_addr())
    }

    /// The cluster-wide metadata store (Raft-backed when clustered).
    pub fn meta(&self) -> SharedMetaStore {
        self.inner.meta.clone()
    }

    /// Topology + assignments for `channels` (empty = all).
    pub fn metadata(&self, channels: &[String]) -> ClusterMetadata {
        if let Some(rt) = &self.inner.rt {
            return self.clustered_metadata(rt, channels);
        }
        let me = &self.inner.node;
        let configs: Vec<fluxmq_proto::ChannelConfig> = self
            .inner
            .broker
            .channels()
            .into_iter()
            .filter(|c| channels.is_empty() || channels.contains(&c.name))
            .collect();
        let priority_channels = configs
            .iter()
            .filter(|c| c.mode == fluxmq_proto::model::ChannelMode::Priority)
            .map(|c| c.name.clone())
            .collect();
        let channels = configs
            .into_iter()
            .map(|c| {
                let parts = (0..c.partitions)
                    .map(|p| PartitionAssignment {
                        partition: p,
                        leader: Some(me.id.clone()),
                        replicas: vec![me.id.clone()],
                        isr: vec![me.id.clone()],
                        epoch: 0,
                    })
                    .collect();
                (c.name, parts)
            })
            .collect();
        ClusterMetadata {
            cluster_name: self.inner.cluster_name.clone(),
            epoch: *self.inner.epoch.borrow(),
            controller: Some(me.id.clone()),
            nodes: vec![me.clone()],
            channels,
            priority_channels,
        }
    }

    fn clustered_metadata(&self, rt: &Runtime, filter: &[String]) -> ClusterMetadata {
        let (voters, controller) = {
            let m = rt.raft.metrics();
            let m = m.borrow();
            let membership = m.membership_config.membership();
            let voters: BTreeSet<u64> = membership.voter_ids().collect();
            let controller = m
                .current_leader
                .and_then(|id| membership.get_node(&id).map(|n| n.name.clone()));
            (voters, controller)
        };
        let nodes = rt
            .meta
            .list_as::<NodeRecord>(NODES_PREFIX)
            .unwrap_or_default()
            .into_iter()
            .map(|(_, r)| NodeInfo {
                is_controller: controller.as_deref() == Some(r.id.as_str()),
                is_voter: voters.contains(&raft::raft_id(&r.id)),
                id: r.id,
                client_addr: r.client_addr,
                http_addr: r.http_addr,
                status: r.status,
                version: r.version,
            })
            .collect();
        let channels = rt
            .meta
            .list(ASSIGNMENTS_PREFIX)
            .into_iter()
            .filter_map(|(k, v)| {
                let name = k.strip_prefix(ASSIGNMENTS_PREFIX)?.to_string();
                if !filter.is_empty() && !filter.contains(&name) {
                    return None;
                }
                let parts: Vec<PartitionAssignment> = serde_json::from_value(v.value).ok()?;
                Some((name, parts))
            })
            .collect();
        let priority_channels = rt
            .meta
            .list_as::<fluxmq_proto::ChannelConfig>(controller::CHANNELS_PREFIX)
            .unwrap_or_default()
            .into_iter()
            .filter(|(_, c)| {
                c.mode == fluxmq_proto::model::ChannelMode::Priority
                    && (filter.is_empty() || filter.contains(&c.name))
            })
            .map(|(_, c)| c.name)
            .collect();
        ClusterMetadata {
            cluster_name: self.inner.cluster_name.clone(),
            epoch: rt.sm.cluster_epoch(),
            controller,
            nodes,
            channels,
            priority_channels,
        }
    }

    /// Bumps on every topology/assignment change.
    pub fn watch_epoch(&self) -> watch::Receiver<u64> {
        match &self.inner.rt {
            Some(rt) => rt.epoch.subscribe(),
            None => self.inner.epoch.subscribe(),
        }
    }

    pub fn status(&self) -> ClusterStatus {
        let metadata = self.metadata(&[]);
        let Some(rt) = &self.inner.rt else {
            return ClusterStatus {
                replication_factor: 1,
                under_replicated_partitions: 0,
                offline_partitions: 0,
                raft: RaftStatus {
                    term: 0,
                    leader: Some(self.inner.node.id.clone()),
                    last_log_index: 0,
                    committed_index: 0,
                    voters: vec![self.inner.node.id.clone()],
                    learners: Vec::new(),
                },
                partitions: Vec::new(),
                metadata,
            };
        };
        let mut under = 0;
        let mut offline = 0;
        for p in metadata.channels.values().flatten() {
            if p.leader.is_none() {
                offline += 1;
            } else if p.isr.len() < p.replicas.len() {
                under += 1;
            }
        }
        let raft = {
            let m = rt.raft.metrics();
            let m = m.borrow();
            let membership = m.membership_config.membership();
            let name = |id: u64| {
                membership
                    .get_node(&id)
                    .map_or_else(|| id.to_string(), |n| n.name.clone())
            };
            RaftStatus {
                term: m.current_term,
                leader: m.current_leader.map(name),
                last_log_index: m.last_log_index.unwrap_or(0),
                committed_index: m.last_applied.map_or(0, |l| l.index),
                voters: membership.voter_ids().map(name).collect(),
                learners: membership.learner_ids().map(name).collect(),
            }
        };
        ClusterStatus {
            replication_factor: rt.cfg.replication_factor,
            under_replicated_partitions: under,
            offline_partitions: offline,
            raft,
            partitions: rt.replication.status(),
            metadata,
        }
    }

    /// Other nodes (excludes this one). Empty for a single node.
    pub fn peers(&self) -> Vec<NodeInfo> {
        if self.inner.rt.is_none() {
            return Vec::new();
        }
        self.metadata(&[])
            .nodes
            .into_iter()
            .filter(|n| n.id != self.inner.node.id)
            .collect()
    }

    /// Info about node `id` (registry), if known.
    pub fn node(&self, id: &str) -> Option<NodeInfo> {
        self.metadata(&[]).nodes.into_iter().find(|n| n.id == id)
    }

    pub fn route_partition(&self, channel: &str, partition: u32) -> PartitionRoute {
        let Some(rt) = &self.inner.rt else {
            return PartitionRoute::Local;
        };
        let parts: Option<Vec<PartitionAssignment>> =
            rt.meta.get_as(&assignment_key(channel)).ok().flatten();
        let Some(parts) = parts else {
            // Unknown channel: let the broker answer (ChannelNotFound); a channel whose
            // assignment isn't written yet is temporarily unavailable.
            return if rt.meta.get(&channel_key(channel)).is_some() {
                PartitionRoute::Unavailable
            } else {
                PartitionRoute::Local
            };
        };
        let Some(leader) = parts
            .iter()
            .find(|p| p.partition == partition)
            .and_then(|p| p.leader.clone())
        else {
            return if parts.iter().any(|p| p.partition == partition) {
                PartitionRoute::Unavailable
            } else {
                // Out of range: the broker reports the error.
                PartitionRoute::Local
            };
        };
        if leader == rt.cfg.node_id {
            if rt.may_lead() && self.inner.broker.channel(channel).is_some() {
                PartitionRoute::Local
            } else {
                PartitionRoute::Unavailable
            }
        } else {
            match self.node(&leader) {
                Some(n) => PartitionRoute::Remote(n),
                None => PartitionRoute::Unavailable,
            }
        }
    }

    /// Runs a controller command on the current Raft leader (retrying across elections).
    async fn controller_call(
        &self,
        rt: &Runtime,
        op: ControllerOp,
    ) -> Result<ControllerOk, RpcError> {
        self.controller_call_within(rt, op, OP_DEADLINE).await
    }

    async fn controller_call_within(
        &self,
        rt: &Runtime,
        op: ControllerOp,
        within: Duration,
    ) -> Result<ControllerOk, RpcError> {
        let deadline = Instant::now() + within;
        let mut backoff = Duration::from_millis(25);
        loop {
            let res = match rt.leader() {
                Some((id, _)) if id == rt.raft_id => controller::execute(rt, op.clone()).await,
                Some((_, node)) => match rt
                    .rpc
                    .post::<_, Result<ControllerOk, RpcError>>(
                        &node.addr,
                        CONTROLLER_PATH,
                        &op,
                        within,
                    )
                    .await
                {
                    Ok(r) => r,
                    Err(e) if e.is_connect() => {
                        Err(RpcError::new(RpcErrorKind::NotLeader, e.to_string()))
                    }
                    Err(e) => Err(RpcError::new(
                        RpcErrorKind::Unavailable,
                        format!("controller call failed (outcome unknown): {e}"),
                    )),
                },
                None => Err(RpcError::new(
                    RpcErrorKind::NotLeader,
                    "no controller elected",
                )),
            };
            match res {
                Err(e) if e.kind == RpcErrorKind::NotLeader => {
                    if Instant::now() + backoff > deadline {
                        return Err(e);
                    }
                }
                other => return other,
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_millis(400));
        }
    }

    /// Waits (bounded) until `cond` holds for the local broker.
    async fn wait_local(&self, mut cond: impl FnMut(&Broker) -> bool) -> bool {
        let deadline = Instant::now() + OP_DEADLINE;
        while Instant::now() < deadline {
            if cond(&self.inner.broker) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        cond(&self.inner.broker)
    }

    /// A channel that exists in the cluster metadata but not yet on this node (its reconciler
    /// creates local storage asynchronously after the controller committed it): wait up to
    /// [`LOCAL_CHANNEL_WAIT`] for it. Returns whether the channel exists locally. Immediate on
    /// a single node, for local channels, and for names unknown to the cluster. Lets a client
    /// create a channel through one node and subscribe/publish on another right away.
    pub async fn await_local_channel(&self, name: &str) -> bool {
        if self.inner.broker.channel(name).is_some() {
            return true;
        }
        let Some(rt) = &self.inner.rt else {
            return false;
        };
        if rt.meta.get(&channel_key(name)).is_none() {
            return false;
        }
        let deadline = Instant::now() + LOCAL_CHANNEL_WAIT;
        while Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
            if self.inner.broker.channel(name).is_some() {
                return true;
            }
        }
        false
    }

    /// Cluster-wide channel creation (through the controller when clustered; locally otherwise).
    pub async fn create_channel(
        &self,
        config: fluxmq_proto::ChannelConfig,
    ) -> Result<fluxmq_proto::ChannelConfig, ClusterError> {
        let Some(rt) = &self.inner.rt else {
            return Ok(self.inner.broker.create_channel(config)?);
        };
        let name = config.name.clone();
        let ok = self
            .controller_call(rt, ControllerOp::CreateChannel(config))
            .await
            .map_err(|e| rpc_to_cluster(e, &name))?;
        rt.meta.wait_for(ok.revision).await?;
        if !self.wait_local(|b| b.channel(&name).is_some()).await {
            return Err(ClusterError::Meta(MetaError::Unavailable(format!(
                "channel '{name}' was created but is not ready on this node yet"
            ))));
        }
        let created = rt
            .meta
            .get_as::<fluxmq_proto::ChannelConfig>(&channel_key(&name))?
            .or_else(|| self.inner.broker.channel(&name))
            .ok_or_else(|| ClusterError::Core(CoreError::ChannelNotFound(name.clone())))?;
        Ok(created)
    }

    pub async fn delete_channel(&self, name: &str) -> Result<(), ClusterError> {
        let Some(rt) = &self.inner.rt else {
            return Ok(self.inner.broker.delete_channel(name)?);
        };
        let ok = self
            .controller_call(rt, ControllerOp::DeleteChannel(name.to_string()))
            .await
            .map_err(|e| rpc_to_cluster(e, name))?;
        rt.meta.wait_for(ok.revision).await?;
        self.wait_local(|b| b.channel(name).is_none()).await;
        Ok(())
    }

    /// Cluster-wide channel config update (retention/retry/DLQ); locally when single node.
    pub async fn update_channel(
        &self,
        name: &str,
        update: UpdateChannelRequest,
    ) -> Result<fluxmq_proto::ChannelConfig, ClusterError> {
        let Some(rt) = &self.inner.rt else {
            return Ok(self.inner.broker.update_channel(name, update)?);
        };
        if let Some(r) = &update.retry
            && r.max_attempts == 0
        {
            return Err(ClusterError::Core(CoreError::InvalidArgument(
                "retry.max_attempts must be >= 1".into(),
            )));
        }
        let key = channel_key(name);
        for _ in 0..5 {
            let Some(v) = rt.meta.get(&key) else {
                return Err(ClusterError::Core(CoreError::ChannelNotFound(
                    name.to_string(),
                )));
            };
            let mut cfg: fluxmq_proto::ChannelConfig =
                serde_json::from_value(v.value).map_err(MetaError::from)?;
            if let Some(r) = update.retention.clone() {
                cfg.retention = r;
            }
            if let Some(r) = update.retry.clone() {
                cfg.retry = r;
            }
            if let Some(d) = update.dlq_enabled {
                cfg.dlq_enabled = d;
            }
            let pre = Precondition {
                key: key.clone(),
                expected_mod_revision: v.mod_revision,
            };
            match rt
                .meta
                .put(
                    &key,
                    serde_json::to_value(&cfg).map_err(MetaError::from)?,
                    Some(pre),
                )
                .await
            {
                Ok(_) => {
                    let want = cfg.clone();
                    self.wait_local(|b| {
                        b.channel(name).is_some_and(|c| {
                            c.retention == want.retention
                                && c.retry == want.retry
                                && c.dlq_enabled == want.dlq_enabled
                        })
                    })
                    .await;
                    return Ok(cfg);
                }
                Err(MetaError::Conflict { .. }) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(ClusterError::Meta(MetaError::Unavailable(
            "too many concurrent updates; retry".into(),
        )))
    }

    /// Admin operations (clustered only).
    pub async fn reassign(
        &self,
        channel: &str,
        partition: u32,
        replicas: Vec<String>,
    ) -> Result<(), ClusterError> {
        let Some(rt) = &self.inner.rt else {
            return Err(ClusterError::Invalid("not clustered".into()));
        };
        let op = ControllerOp::Reassign {
            channel: channel.to_string(),
            partition,
            replicas,
        };
        let ok = self
            .controller_call(rt, op)
            .await
            .map_err(|e| rpc_to_cluster(e, channel))?;
        rt.meta.wait_for(ok.revision).await?;
        Ok(())
    }

    pub async fn drain(&self, node_id: &str) -> Result<u32, ClusterError> {
        let Some(rt) = &self.inner.rt else {
            return Err(ClusterError::Invalid("not clustered".into()));
        };
        let ok = self
            .controller_call(rt, ControllerOp::Drain(node_id.to_string()))
            .await
            .map_err(|e| match e.kind {
                RpcErrorKind::NotFound => ClusterError::Invalid(e.message),
                _ => rpc_to_cluster(e, ""),
            })?;
        rt.meta.wait_for(ok.revision).await?;
        Ok(ok.count)
    }

    /// Graceful leave before shutdown (no-op on a single node): drains this node's
    /// leaderships, stops following (no more ISR membership, no lease renewal) and tells the
    /// controller it left — it is marked `Down` and removed from every ISR at once, so publish
    /// acks on the remaining nodes don't wait for `node_timeout`. Best effort within
    /// `timeout`; on failure the peers fall back to the normal Down detection.
    pub async fn leave(&self, timeout: Duration) {
        let Some(rt) = &self.inner.rt else {
            return;
        };
        let node = rt.node_id().to_string();
        let started = Instant::now();
        let left = |d: Duration| d.saturating_sub(started.elapsed());
        match tokio::time::timeout(left(timeout), self.drain(&node)).await {
            Ok(Ok(moved)) => tracing::info!(moved, "leaderships drained before shutdown"),
            Ok(Err(e)) => tracing::warn!(error = %e, "drain before shutdown failed"),
            Err(_) => tracing::warn!("drain before shutdown timed out"),
        }
        // Let the local reconciler apply the drained roles before it stops: a partition this
        // node still leads locally after the drain keeps accepting (forwarded) publishes that
        // can never be acknowledged once replication stops — the senders would hang for the
        // forward timeout. Bounded: at most half of the remaining budget.
        let settle = Instant::now() + left(timeout) / 2;
        loop {
            let still = self.stale_local_leaderships(&node);
            if still == 0 {
                break;
            }
            if Instant::now() >= settle {
                tracing::warn!(
                    partitions = still,
                    "drained leaderships not yet demoted locally"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // Stop following: a follower that keeps fetching would be re-added to ISRs.
        rt.leaving.cancel();
        rt.replication.shutdown().await;
        let op = ControllerOp::Leave {
            node,
            incarnation: rt.incarnation,
        };
        match tokio::time::timeout(left(timeout), self.controller_call(rt, op)).await {
            Ok(Ok(ok)) => {
                let _ = tokio::time::timeout(left(timeout), rt.meta.wait_for(ok.revision)).await;
                tracing::info!("left the cluster");
            }
            Ok(Err(e)) => {
                tracing::warn!(error = %e.message, "leave failed; peers will detect the shutdown")
            }
            Err(_) => tracing::warn!("leave timed out; peers will detect the shutdown"),
        }
    }

    /// Partitions this node still leads locally although the metadata names another leader
    /// (the reconciler hasn't demoted them yet).
    fn stale_local_leaderships(&self, node: &str) -> usize {
        let Some(rt) = &self.inner.rt else {
            return 0;
        };
        let md = self.metadata(&[]);
        rt.replication
            .status()
            .into_iter()
            .filter(|s| s.leader.as_deref() == Some(node))
            .filter(|s| {
                md.channels
                    .get(&s.channel)
                    .and_then(|ps| ps.iter().find(|p| p.partition == s.partition))
                    .is_some_and(|p| p.leader.as_deref() != Some(node))
            })
            .count()
    }

    pub async fn shutdown(&self) {
        if let Some(rt) = &self.inner.rt {
            rt.stop().await;
        }
    }

    // ---- v0.4 automatic rebalancing (contract: `fluxmq_proto::cluster`) ----

    /// `GET /api/cluster/rebalance`: from the controller (live catch-up lag, node storage),
    /// falling back to the local replica of the plan. Single node: mode off, idle.
    pub async fn rebalance_status(&self) -> RebalanceStatus {
        let Some(rt) = &self.inner.rt else {
            let assignments: rebalance::Assignments = self.metadata(&[]).channels;
            return rebalancer::single_node_status(
                self.node_id(),
                &assignments,
                self.inner.broker.stats().storage_bytes,
            );
        };
        match self
            .controller_call_within(rt, ControllerOp::RebalanceStatus, REBALANCE_STATUS_DEADLINE)
            .await
        {
            Ok(ControllerOk {
                rebalance: Some(s), ..
            }) => *s,
            Ok(_) => rebalancer::status(rt),
            Err(e) => {
                tracing::debug!(error = %e, "rebalance status from the controller failed");
                rebalancer::status(rt)
            }
        }
    }

    /// The current (or last) persisted plan, read locally (`None` on a single node).
    pub fn rebalance_plan(&self) -> Option<rebalance::PlanDoc> {
        rebalancer::load_plan(self.inner.rt.as_ref()?).map(|(_, p)| p)
    }

    /// `PUT /api/cluster/rebalance/config` (clustered only).
    pub async fn set_rebalance_config(
        &self,
        config: RebalanceConfig,
    ) -> Result<RebalanceStatus, ClusterError> {
        self.rebalance_op(ControllerOp::RebalanceConfig(config))
            .await
    }

    /// `POST /api/cluster/rebalance` (clustered only): a dry run returns the plan only.
    pub async fn rebalance(&self, req: RebalanceRequest) -> Result<RebalanceStatus, ClusterError> {
        self.rebalance_op(ControllerOp::Rebalance(req)).await
    }

    /// `POST /api/cluster/rebalance/{pause,resume,cancel}` (clustered only).
    pub async fn rebalance_control(
        &self,
        action: RebalanceAction,
    ) -> Result<RebalanceStatus, ClusterError> {
        self.rebalance_op(ControllerOp::RebalanceControl(action))
            .await
    }

    async fn rebalance_op(&self, op: ControllerOp) -> Result<RebalanceStatus, ClusterError> {
        let Some(rt) = &self.inner.rt else {
            return Err(ClusterError::Invalid(
                "rebalancing requires a cluster".into(),
            ));
        };
        let ok = self
            .controller_call(rt, op)
            .await
            .map_err(|e| match e.kind {
                RpcErrorKind::Conflict => ClusterError::Conflict(e.message),
                RpcErrorKind::NotFound => ClusterError::NotFound(e.message),
                _ => rpc_to_cluster(e, ""),
            })?;
        // Read-your-writes: the controller's applied revision after the op.
        rt.meta.wait_for(ok.revision).await?;
        ok.rebalance
            .map(|s| *s)
            .ok_or_else(|| ClusterError::Raft("controller returned no rebalance status".into()))
    }

    // ---- v0.5 decommission / forget (contract: `fluxmq_proto::cluster`) ----

    /// `POST /api/cluster/nodes/{id}/decommission` (clustered only): marks the node
    /// `Decommissioning`, drains its leaderships and starts moving its replicas off; the
    /// controller then removes it from the Raft membership and the registry. Idempotent while
    /// in progress. `Conflict` when a rebalance plan runs, the other up nodes can't hold its
    /// replicas, or the Raft quorum would be unsafe.
    pub async fn decommission(&self, node: &str) -> Result<DecommissionStatus, ClusterError> {
        self.decommission_op(ControllerOp::Decommission(node.to_string()))
            .await
    }

    /// `DELETE /api/cluster/nodes/{id}` (clustered only): forgets a `Down` node that holds no
    /// replicas (Raft membership + registry). `Conflict` otherwise.
    pub async fn forget_node(&self, node: &str) -> Result<DecommissionStatus, ClusterError> {
        self.decommission_op(ControllerOp::Forget(node.to_string()))
            .await
    }

    /// `GET /api/cluster/nodes/{id}/decommission`: from the replicated state (any node).
    /// `NotFound` when the node has no decommission record (never decommissioned, or it
    /// restarted and rejoined as a new node since).
    pub fn decommission_status(&self, node: &str) -> Result<DecommissionStatus, ClusterError> {
        let Some(rt) = &self.inner.rt else {
            return Err(ClusterError::Invalid(
                "decommissioning requires a cluster".into(),
            ));
        };
        decommission::status(rt, node).ok_or_else(|| {
            ClusterError::NotFound(format!(
                "node '{node}' has no decommission in progress or on record"
            ))
        })
    }

    /// `POST /api/cluster/nodes/{id}/decommission?dry_run=true`: the same checks (409 /
    /// 404 like a real start) and the repair plan it would run; nothing is written.
    pub async fn decommission_dry_run(&self, node: &str) -> Result<RebalanceStatus, ClusterError> {
        self.rebalance_op(ControllerOp::DecommissionDryRun(node.to_string()))
            .await
    }

    /// `GET /api/cluster/decommissions`: current and recent (24 h) decommissions, newest
    /// first, from the replicated state. Empty on a single node.
    pub fn decommissions(&self) -> Vec<DecommissionStatus> {
        self.inner
            .rt
            .as_ref()
            .map(|rt| decommission::list(rt))
            .unwrap_or_default()
    }

    /// Decommission starts and ends observed by this node while controller (for the audit
    /// log). `None` on a single node.
    pub fn decommission_events(
        &self,
    ) -> Option<tokio::sync::broadcast::Receiver<DecommissionStatus>> {
        Some(self.inner.rt.as_ref()?.decommission_events.subscribe())
    }

    /// Current and recent decommission / forget records with their origin, from the
    /// replicated state (any node; the telemetry events feed derives the
    /// `node_decommission_*` / `node_forgotten` events from them). Empty on a single node.
    pub fn decommission_records(&self) -> Vec<DecommissionRecord> {
        self.inner
            .rt
            .as_ref()
            .map(|rt| {
                decommission::records_with_origin(rt)
                    .into_iter()
                    .map(|(status, forget)| DecommissionRecord { status, forget })
                    .collect()
            })
            .unwrap_or_default()
    }

    async fn decommission_op(&self, op: ControllerOp) -> Result<DecommissionStatus, ClusterError> {
        let Some(rt) = &self.inner.rt else {
            return Err(ClusterError::Invalid(
                "decommissioning requires a cluster".into(),
            ));
        };
        let ok = self
            .controller_call(rt, op)
            .await
            .map_err(|e| match e.kind {
                RpcErrorKind::Conflict => ClusterError::Conflict(e.message),
                RpcErrorKind::NotFound => ClusterError::NotFound(e.message),
                _ => rpc_to_cluster(e, ""),
            })?;
        rt.meta.wait_for(ok.revision).await?;
        ok.decommission
            .ok_or_else(|| ClusterError::Raft("controller returned no decommission status".into()))
    }
}

/// One decommission record and whether it was started by a forget (`DELETE`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecommissionRecord {
    pub status: DecommissionStatus,
    pub forget: bool,
}

/// Maps a controller RPC error; `channel` names the subject for not-found/exists.
fn rpc_to_cluster(e: RpcError, channel: &str) -> ClusterError {
    match e.kind {
        RpcErrorKind::AlreadyExists => {
            ClusterError::Core(CoreError::ChannelExists(channel.to_string()))
        }
        RpcErrorKind::NotFound if !channel.is_empty() && e.message.starts_with("channel") => {
            ClusterError::Core(CoreError::ChannelNotFound(channel.to_string()))
        }
        RpcErrorKind::NotFound | RpcErrorKind::Invalid => ClusterError::Invalid(e.message),
        RpcErrorKind::NotLeader => ClusterError::NotController(e.leader),
        RpcErrorKind::Conflict | RpcErrorKind::Unavailable => {
            ClusterError::Meta(MetaError::Unavailable(e.message))
        }
        RpcErrorKind::Internal | RpcErrorKind::Removed => ClusterError::Raft(e.message),
    }
}

/// Parses `id@host:port,id@host:port` (the `--peers` flag).
pub fn parse_peers(s: &str) -> Result<Vec<PeerSpec>, String> {
    let mut out: Vec<PeerSpec> = Vec::new();
    for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (id, addr) = part
            .split_once('@')
            .ok_or_else(|| format!("peer '{part}' must look like id@host:port"))?;
        if id.is_empty() || !addr.contains(':') {
            return Err(format!("peer '{part}' must look like id@host:port"));
        }
        if out.iter().any(|p| p.id == id) {
            return Err(format!("duplicate peer id '{id}'"));
        }
        out.push(PeerSpec {
            id: id.to_string(),
            raft_addr: addr.to_string(),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peers_parse() {
        let p = parse_peers("n1@127.0.0.1:7422, n2@host-b:7422").unwrap();
        assert_eq!(p.len(), 2);
        assert_eq!(p[1].id, "n2");
        assert_eq!(p[1].raft_addr, "host-b:7422");
        assert!(parse_peers("n1").is_err());
        assert!(parse_peers("n1@x:1,n1@y:2").is_err());
        assert!(parse_peers("").unwrap().is_empty());
    }
}
