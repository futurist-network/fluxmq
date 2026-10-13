//! The clustered-mode runtime: Raft node, RPC listener, replication manager and the background
//! loops (heartbeat/lease, controller, reconciler).
//!
//! Timing model (defaults, `node_timeout` = 6 s):
//! * every node heartbeats the controller each `node_timeout / 6` (1 s);
//! * the controller confirms its leadership with a quorum (`ensure_linearizable`) before
//!   acking, so an ack proves "controller at a time after the heartbeat was sent";
//! * a node holds a **lease** for `node_timeout * 2/3` (4 s) from the *send* time of its last
//!   acked heartbeat; without a valid lease it stops leading partitions (self-fencing);
//! * the controller declares a node `Down` `node_timeout` after the last heartbeat it
//!   *received* (≥ send time), i.e. strictly after the node's lease expired, and only then
//!   fails its partitions over. A newly elected controller waits `lease + 1 s` before failing
//!   over nodes it has not heard from, which also covers leases granted by its predecessor.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fluxmq_core::Broker;
use fluxmq_proto::cluster::NodeStatus;
use fluxmq_replication::{ReplicationConfig, ReplicationManager};
use parking_lot::Mutex;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::controller::ControllerState;
use crate::meta::{RaftMetaStore, current_leader};
use crate::raft::network::Network;
use crate::raft::store::{LogStore, SmShared, StateMachine};
use crate::raft::{NodeId, Raft, RaftNode, raft_id};
use crate::rpc::{Heartbeat, HeartbeatAck, InfoResponse, NodeRecord, RpcClient, RpcError};
use crate::{ClusterConfig, ClusterError};
use fluxmq_meta::MetaStore;

pub const HEARTBEAT_PATH: &str = "/cluster/heartbeat";
/// v0.5: written to the data dir when this process learns it was removed from the cluster
/// (decommissioned / forgotten). The next start wipes the stale Raft state (a removed member's
/// old log and term would disrupt the group when it is re-added) and joins as a new node.
pub const REMOVED_MARKER: &str = "cluster-removed";
/// v0.5: inside a Raft dir wiped after a removal: this node never bootstraps a cluster (it
/// only joins through the controller).
const NO_BOOTSTRAP_MARKER: &str = "no-bootstrap";
pub const INFO_PATH: &str = "/cluster/info";
pub const CONTROLLER_PATH: &str = "/cluster/controller";

#[derive(Debug, Clone, Copy)]
pub struct Timings {
    pub heartbeat: Duration,
    pub node_timeout: Duration,
    pub lease: Duration,
    pub controller_grace: Duration,
    pub tick: Duration,
    /// A returning preferred leader must be up this long before leadership moves back.
    pub rebalance_stable: Duration,
}

impl Timings {
    pub fn from_node_timeout(node_timeout: Duration) -> Self {
        let node_timeout = node_timeout.max(Duration::from_millis(600));
        let lease = node_timeout * 2 / 3;
        Self {
            heartbeat: node_timeout / 6,
            node_timeout,
            lease,
            // Leases granted by a previous controller started before this one was
            // elected (its confirmation round needed the quorum that later voted it out),
            // so they all expire within `lease` of the election.
            controller_grace: lease + Duration::from_millis(500),
            tick: Duration::from_millis(250),
            rebalance_stable: node_timeout + Duration::from_secs(4),
        }
    }
}

/// Last acknowledged heartbeat.
#[derive(Debug, Clone, Copy, Default)]
pub struct Lease {
    pub valid_until: Option<Instant>,
    /// Controller's applied revision at ack time; a fenced node only unfences once it has
    /// applied at least this much (so it never leads on stale assignments).
    pub revision: u64,
}

pub(crate) struct Runtime {
    pub cfg: ClusterConfig,
    pub timings: Timings,
    pub raft: Raft,
    pub raft_id: NodeId,
    pub sm: Arc<SmShared>,
    pub meta: Arc<RaftMetaStore>,
    pub rpc: RpcClient,
    pub broker: Broker,
    pub replication: ReplicationManager,
    pub controller: Mutex<ControllerState>,
    pub lease: watch::Sender<Lease>,
    /// Maintained by the reconciler; true until the first lease + catch-up.
    pub fenced: AtomicBool,
    pub epoch: watch::Sender<u64>,
    pub incarnation: u64,
    pub shutdown: CancellationToken,
    /// Graceful leave in progress ([`crate::Cluster::leave`]): the reconciler, ISR persister
    /// and heartbeats stop, so this node neither follows (and rejoins ISRs) nor renews its
    /// lease. Raft and the controller keep running until `shutdown`.
    pub leaving: CancellationToken,
    pub tasks: Mutex<Vec<JoinHandle<()>>>,
    pub raft_local_addr: SocketAddr,
    /// Start time of the last successful leadership confirmation (coalesces heartbeats).
    pub confirmed: tokio::sync::Mutex<Option<Instant>>,
    /// Serializes controller ticks and operator commands so a tick never acts on a liveness
    /// view older than a command it races with (e.g. a drain).
    pub controller_lock: tokio::sync::Mutex<()>,
    /// Rebalance executor state (controller only; reset on leadership loss).
    pub rebalance: Mutex<crate::rebalancer::RebalanceLocal>,
    /// v0.5: decommission starts and ends observed by this node as controller (audit).
    pub decommission_events:
        tokio::sync::broadcast::Sender<fluxmq_proto::cluster::DecommissionStatus>,
}

impl Runtime {
    pub fn node_id(&self) -> &str {
        &self.cfg.node_id
    }

    /// This node's registry document (status filled in by the controller).
    pub fn record(&self) -> NodeRecord {
        NodeRecord {
            id: self.cfg.node_id.clone(),
            client_addr: self.cfg.advertise_client_addr.clone(),
            http_addr: self.cfg.advertise_http_addr.clone(),
            raft_addr: self.cfg.advertise_raft_addr.clone(),
            replication_addr: self.cfg.advertise_replication_addr.clone(),
            version: fluxmq_proto::VERSION.to_string(),
            status: NodeStatus::Up,
            incarnation: self.incarnation,
            features: crate::rpc::features_for(&self.cfg.node_id),
        }
    }

    pub fn leader(&self) -> Option<(NodeId, RaftNode)> {
        current_leader(&self.raft)
    }

    pub fn is_leader(&self) -> bool {
        self.leader().is_some_and(|(id, _)| id == self.raft_id)
    }

    pub fn leader_hint(&self) -> Option<(String, String)> {
        self.leader().map(|(_, n)| (n.name, n.addr))
    }

    /// Valid lease and not fenced: this node may lead partitions.
    pub fn may_lead(&self) -> bool {
        !self.fenced.load(Ordering::Acquire)
            && self
                .lease
                .borrow()
                .valid_until
                .is_some_and(|t| t > Instant::now())
    }

    fn spawn(&self, fut: impl std::future::Future<Output = ()> + Send + 'static) {
        self.tasks.lock().push(tokio::spawn(fut));
    }

    pub async fn stop(&self) {
        self.shutdown.cancel();
        let tasks: Vec<_> = self.tasks.lock().drain(..).collect();
        for t in &tasks {
            t.abort();
        }
        for t in tasks {
            let _ = t.await;
        }
        if let Err(e) = self.raft.shutdown().await {
            tracing::warn!(error = %e, "raft shutdown");
        }
        self.replication.shutdown().await;
    }
}

/// Replaces a `:0` port in an advertised address with the actually bound port.
fn fix_port(advertised: &mut String, bound: SocketAddr) {
    if let Some(host) = advertised.strip_suffix(":0") {
        *advertised = format!("{host}:{}", bound.port());
    }
}

pub(crate) async fn start(
    mut cfg: ClusterConfig,
    broker: Broker,
    epoch: watch::Sender<u64>,
) -> Result<Arc<Runtime>, ClusterError> {
    if cfg.node_id != broker.node_id() {
        return Err(ClusterError::Invalid(format!(
            "cluster node id '{}' differs from the broker node id '{}'",
            cfg.node_id,
            broker.node_id()
        )));
    }
    let secret = cfg
        .cluster_secret
        .clone()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ClusterError::Invalid("a cluster secret is required".into()))?;
    let mut ids = BTreeMap::new();
    for p in &cfg.peers {
        if let Some(other) = ids.insert(raft_id(&p.id), p.id.clone())
            && other != p.id
        {
            return Err(ClusterError::Invalid(format!(
                "node ids '{other}' and '{}' collide; rename one",
                p.id
            )));
        }
    }
    let timings = Timings::from_node_timeout(cfg.node_timeout);

    let raft_dir = cfg.data_dir.join("raft");
    let marker = cfg.data_dir.join(REMOVED_MARKER);
    let (log, sm) = tokio::task::spawn_blocking(move || {
        if marker.exists() {
            // Removed from the cluster last time: start over as a brand-new member.
            tracing::info!("this node was removed from its cluster; joining as a new node");
            if raft_dir.exists() {
                std::fs::remove_dir_all(&raft_dir)?;
            }
            std::fs::create_dir_all(&raft_dir)?;
            std::fs::write(raft_dir.join(NO_BOOTSTRAP_MARKER), b"")?;
            std::fs::remove_file(&marker)?;
        }
        Ok::<_, std::io::Error>((LogStore::open(&raft_dir)?, SmShared::open(&raft_dir)?))
    })
    .await
    .map_err(|e| ClusterError::Raft(e.to_string()))??;

    let raft_config = openraft::Config {
        cluster_name: cfg.cluster_name.clone(),
        heartbeat_interval: 150,
        election_timeout_min: 1000,
        election_timeout_max: 2000,
        install_snapshot_timeout: 5000,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(1000),
        max_in_snapshot_log_to_keep: 200,
        ..Default::default()
    }
    .validate()
    .map_err(|e| ClusterError::Raft(e.to_string()))?;

    let rpc = RpcClient::with_tls(Some(secret.clone()), cfg.tls.as_ref());
    let me = raft_id(&cfg.node_id);
    let raft = Raft::new(
        me,
        Arc::new(raft_config),
        Network { rpc: rpc.clone() },
        log,
        StateMachine { shared: sm.clone() },
    )
    .await
    .map_err(|e| ClusterError::Raft(e.to_string()))?;

    let listener = tokio::net::TcpListener::bind(cfg.raft_addr).await?;
    let raft_local_addr = listener.local_addr()?;
    fix_port(&mut cfg.advertise_raft_addr, raft_local_addr);

    let mut rcfg = ReplicationConfig::new(cfg.node_id.clone(), cfg.replication_addr);
    rcfg.secret = Some(secret);
    rcfg.tls = cfg.tls.clone();
    let replication = match ReplicationManager::start(broker.clone(), rcfg).await {
        Ok(r) => r,
        Err(e) => {
            let _ = raft.shutdown().await;
            return Err(ClusterError::Raft(format!("starting replication: {e}")));
        }
    };

    fix_port(
        &mut cfg.advertise_replication_addr,
        replication.local_addr(),
    );

    let meta = Arc::new(RaftMetaStore {
        sm: sm.clone(),
        raft: raft.clone(),
        raft_id: me,
        rpc: rpc.clone(),
    });
    let (lease, _) = watch::channel(Lease::default());
    let rt = Arc::new(Runtime {
        timings,
        raft,
        raft_id: me,
        sm,
        meta,
        rpc,
        broker,
        replication,
        controller: Mutex::new(ControllerState::default()),
        lease,
        fenced: AtomicBool::new(true),
        epoch,
        incarnation: fluxmq_proto::now_ms(),
        shutdown: CancellationToken::new(),
        leaving: CancellationToken::new(),
        tasks: Mutex::new(Vec::new()),
        raft_local_addr,
        confirmed: tokio::sync::Mutex::new(None),
        controller_lock: tokio::sync::Mutex::new(()),
        rebalance: Mutex::new(Default::default()),
        decommission_events: tokio::sync::broadcast::channel(64).0,
        cfg,
    });

    let app = crate::server::router(rt.clone());
    let token = rt.shutdown.clone();
    let mode = match &rt.cfg.tls {
        Some(t) => fluxmq_tls::http::HttpMode::Tls {
            config: t.server_config(),
            failures: t.failures(),
            cluster_client_auth: true,
        },
        None => fluxmq_tls::http::HttpMode::Plain,
    };
    rt.spawn(async move {
        if let Err(e) = fluxmq_tls::http::serve(listener, app, mode, token).await {
            tracing::error!(error = %e, "cluster RPC listener failed");
        }
    });
    rt.spawn(bootstrap(rt.clone()));
    rt.spawn(heartbeat_loop(rt.clone()));
    rt.spawn(crate::controller::run(rt.clone()));
    rt.spawn(crate::rebalancer::run(rt.clone()));
    rt.spawn(crate::reconcile::run(rt.clone()));
    rt.spawn(crate::reconcile::isr_loop(rt.clone()));
    tracing::info!(
        node = %rt.cfg.node_id,
        raft = %raft_local_addr,
        replication = %rt.replication.local_addr(),
        peers = rt.cfg.peers.len(),
        "cluster node started"
    );
    Ok(rt)
}

/// Initializes the Raft group exactly once: by the lowest-id initial peer, when it has no
/// Raft state and no other peer reports an initialized cluster.
async fn bootstrap(rt: Arc<Runtime>) {
    match rt.raft.is_initialized().await {
        Ok(true) | Err(_) => return,
        Ok(false) => {}
    }
    let me = rt.node_id().to_string();
    if rt
        .cfg
        .data_dir
        .join("raft")
        .join(NO_BOOTSTRAP_MARKER)
        .exists()
    {
        tracing::info!("rejoining after a removal: waiting to be added by the controller");
        return;
    }
    if !rt.cfg.peers.iter().any(|p| p.id == me) {
        tracing::info!("not an initial peer: waiting to be added by the controller");
        return;
    }
    let lowest = rt.cfg.peers.iter().map(|p| p.id.as_str()).min();
    if lowest != Some(me.as_str()) {
        return;
    }
    for p in rt.cfg.peers.iter().filter(|p| p.id != me) {
        let info: Result<InfoResponse, _> = rt
            .rpc
            .get(&p.raft_addr, INFO_PATH, &(), Duration::from_millis(800))
            .await;
        if let Ok(info) = info
            && info.initialized
        {
            tracing::info!(peer = %p.id, "cluster already initialized; not bootstrapping");
            return;
        }
    }
    let members: BTreeMap<NodeId, RaftNode> = rt
        .cfg
        .peers
        .iter()
        .map(|p| {
            (
                raft_id(&p.id),
                RaftNode {
                    name: p.id.clone(),
                    addr: p.raft_addr.clone(),
                },
            )
        })
        .collect();
    match rt.raft.initialize(members).await {
        Ok(()) => tracing::info!(
            voters = rt.cfg.peers.len(),
            "bootstrapped metadata Raft group"
        ),
        Err(e) => tracing::warn!(error = %e, "raft initialize"),
    }
}

/// Controller side of a heartbeat (runs on the Raft leader).
pub(crate) async fn handle_heartbeat(
    rt: &Runtime,
    hb: Heartbeat,
) -> Result<HeartbeatAck, RpcError> {
    let received = Instant::now();
    if !rt.is_leader() {
        return Err(RpcError::not_leader(rt.leader_hint()));
    }
    if !confirm_leadership_after(rt, received).await {
        return Err(RpcError::not_leader(rt.leader_hint()));
    }
    let id = hb.record.id.clone();
    let incarnation = hb.record.incarnation;
    let storage = hb.storage_bytes;
    // v0.5: a decommissioned / forgotten process (a restart has a newer incarnation).
    if crate::decommission::refused_incarnation(rt, &id).is_some_and(|inc| incarnation <= inc) {
        return Err(RpcError::new(
            crate::rpc::RpcErrorKind::Removed,
            format!("node '{id}' was removed from the cluster; restart it to join as a new node"),
        ));
    }
    if !rt.controller.lock().observe(hb.record, received) {
        return Err(RpcError::new(
            crate::rpc::RpcErrorKind::Unavailable,
            "node left the cluster (graceful shutdown)",
        ));
    }
    rt.controller.lock().observe_storage(&id, storage);
    // A restarted node gets no lease until the controller has recorded the restart (and
    // moved its leaderships away), so it can't lead on its pre-restart assignments.
    let recorded: Option<NodeRecord> = rt
        .meta
        .get(&crate::controller::node_key(&id))
        .and_then(|v| serde_json::from_value(v.value).ok());
    if recorded.is_some_and(|r| r.incarnation != incarnation) {
        return Err(RpcError::new(
            crate::rpc::RpcErrorKind::Unavailable,
            "restart being processed by the controller",
        ));
    }
    Ok(HeartbeatAck {
        revision: rt.sm.revision(),
        controller: rt.node_id().to_string(),
    })
}

/// True once a quorum confirmed this node's leadership in a round that *started* after
/// `t`. Concurrent heartbeats share one confirmation round.
async fn confirm_leadership_after(rt: &Runtime, t: Instant) -> bool {
    let mut last = rt.confirmed.lock().await;
    if last.is_some_and(|s| s >= t) {
        return true;
    }
    let started = Instant::now();
    match tokio::time::timeout(rt.timings.heartbeat, rt.raft.ensure_linearizable()).await {
        Ok(Ok(_)) => {
            *last = Some(started);
            true
        }
        _ => false,
    }
}

async fn heartbeat_loop(rt: Arc<Runtime>) {
    let mut tick = tokio::time::interval(rt.timings.heartbeat);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut hint: Option<String> = None;
    let mut seed = 0usize;
    // Storage bytes ride along every few beats (rebalancing tie-breaker); `stats()` walks
    // every channel, so it isn't sampled on each beat.
    let mut beats = 0u64;
    let mut storage: Option<u64> = None;
    loop {
        tokio::select! {
            () = rt.shutdown.cancelled() => return,
            () = rt.leaving.cancelled() => return,
            _ = tick.tick() => {}
        }
        let sent_at = Instant::now();
        if beats.is_multiple_of(10) {
            storage = Some(rt.broker.stats().storage_bytes);
        }
        beats += 1;
        let hb = Heartbeat {
            record: rt.record(),
            storage_bytes: storage,
        };
        let result = match rt.leader() {
            Some((id, _)) if id == rt.raft_id => handle_heartbeat(&rt, hb).await,
            leader => {
                // Leader per local Raft state, else a hint, else round-robin over the seeds
                // (a joining node isn't a Raft member yet and knows no leader).
                let target = leader
                    .map(|(_, n)| n.addr)
                    .or_else(|| hint.take())
                    .or_else(|| {
                        let seeds: Vec<&str> = rt
                            .cfg
                            .peers
                            .iter()
                            .filter(|p| p.id != rt.cfg.node_id)
                            .map(|p| p.raft_addr.as_str())
                            .collect();
                        seed = seed.wrapping_add(1);
                        seeds.get(seed % seeds.len().max(1)).map(|s| s.to_string())
                    });
                let Some(target) = target else { continue };
                match rt
                    .rpc
                    .post::<_, Result<HeartbeatAck, RpcError>>(
                        &target,
                        HEARTBEAT_PATH,
                        &hb,
                        rt.timings.heartbeat,
                    )
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::trace!(error = %e, %target, "heartbeat failed");
                        continue;
                    }
                }
            }
        };
        match result {
            Ok(ack) => {
                rt.lease.send_replace(Lease {
                    valid_until: Some(sent_at + rt.timings.lease),
                    revision: ack.revision,
                });
            }
            Err(e) if e.kind == crate::rpc::RpcErrorKind::Removed => {
                retire(&rt).await;
                return;
            }
            Err(e) => hint = e.leader_addr,
        }
    }
}

/// v0.5: this process was removed from the cluster (decommissioned / forgotten). It stops
/// following, leading and heartbeating, shuts its Raft member down (a removed member that
/// keeps campaigning only disturbs the group) and leaves [`REMOVED_MARKER`] so a restart joins
/// as a new node. The HTTP API keeps serving its last local view until the process stops.
async fn retire(rt: &Runtime) {
    tracing::warn!(
        node = %rt.node_id(),
        "this node was removed from the cluster; stopping its cluster roles (restart it to \
         join as a new node)"
    );
    if let Err(e) = std::fs::write(rt.cfg.data_dir.join(REMOVED_MARKER), b"") {
        tracing::warn!(error = %e, "could not write the removal marker");
    }
    rt.fenced.store(true, Ordering::Release);
    rt.lease.send_replace(Lease::default());
    rt.leaving.cancel();
    rt.replication.shutdown().await;
    if let Err(e) = rt.raft.shutdown().await {
        tracing::debug!(error = %e, "raft shutdown after removal");
    }
}
