//! Partition data replication (roadmap stage 9). See `fluxmq_proto::cluster` module docs for
//! the model (leader/follower, ISR, high watermark, epochs) and `docs/design/v0.2.md`.
//!
//! Division of labour:
//! * **fluxmq-cluster** (controller) decides *who* leads/follows each partition and calls
//!   [`ReplicationManager::set_role`]; it persists ISR changes reported via
//!   [`ReplicationManager::isr_changes`] and picks failover leaders from the ISR.
//! * **this crate + fluxmq-core** do the data movement: follower fetchers (pull, over a
//!   dedicated TCP port), leader-side follower progress / ISR / high-watermark tracking,
//!   publish acks gated on the HW (`acks=all`), consumer visibility below the HW, replication of
//!   group offsets + DLQ state, epoch fencing and truncation on leader change.
//!
//! A node without a role for a partition neither serves publishes nor consumers for it.
//! In single-node mode this crate is not used (core behaves exactly as in v0.1).
//!
//! ## Replica moves (v0.4)
//!
//! The rebalance executor moves replicas with ordinary assignment changes (add replicas →
//! catch up into the ISR → final replica list). Support here:
//! * **bootstrap** — a follower with no data, or behind the leader's log start, restarts its
//!   log at the leader's `log_start_offset` (fetcher, on `OffsetOutOfRange`);
//! * **state snapshot** — group committed offsets + DLQ are sent as a snapshot on the first
//!   fetch of every leadership; a follower joins the ISR only after applying it;
//! * **removed-replica cleanup** — [`ReplicationManager::remove_replica`] (explicit: a `None`
//!   role alone never deletes data);
//! * **catch-up throttle** — [`ReplicationManager::set_catch_up_throttle`] (leader-side, only
//!   for followers outside the ISR) and [`ReplicationManager::follower_progress`] (lag per
//!   follower for `PartitionMove::lag`).
//!
//! Public signatures are fixed (fluxmq-cluster compiles against them).
//!
//! ## Protocol and invariants
//!
//! See [`protocol`] for the wire format, `fluxmq_core::replica` for the log-side invariants
//! (epoch fencing, HW / ack HW, two-phase ISR shrink, leader-epoch truncation) and
//! [`fetcher`] for the follower state machine. Acked messages (`acks=all`, ISR >= min_isr)
//! survive any failover the controller performs from the committed ISR: an offset is acked
//! only once it is durable on every member of `ISR ∪ committed ISR`.
//!
//! Threading: all network I/O is async; record reads run on `spawn_blocking`; appends and
//! truncations run on the channel writer thread (core). No blocking file I/O on async threads.

pub mod chunk;
pub mod client;
mod fetcher;
pub mod protocol;
mod server;
mod throttle;
pub mod transport;

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use fluxmq_core::{Broker, CoreError, LeaderParams, ReplicaHandle, ReplicaRole};
use fluxmq_proto::cluster::PartitionReplicationStatus;
use parking_lot::{Mutex, RwLock};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::client::PeerClient;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionRole {
    Leader {
        epoch: u64,
        /// All replicas including this node.
        replicas: Vec<String>,
        /// Publishes fail with `NotEnoughReplicas` when the ISR is smaller.
        min_isr: u32,
    },
    Follower {
        epoch: u64,
        leader: String,
    },
    /// Not a replica (or partition offline): reject publishes and consumers.
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaEndpoint {
    pub node_id: String,
    /// Replication listener of that node.
    pub addr: SocketAddr,
}

/// Reported by a partition leader when it shrinks/expands the ISR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IsrChange {
    pub channel: String,
    pub partition: u32,
    pub epoch: u64,
    pub isr: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub node_id: String,
    pub listen_addr: SocketAddr,
    /// A follower not caught up within this long is removed from the ISR.
    pub replica_lag_max: Duration,
    pub fetch_max_bytes: u32,
    /// Long-poll wait when a follower is caught up.
    pub fetch_max_wait: Duration,
    /// Upper bound for a publish waiting on replication (`acks=all`); then it fails with
    /// `CoreError::AckTimeout` (wire `Overloaded`, retryable).
    pub publish_ack_timeout: Duration,
    /// Shared cluster secret: when set, peers must present it in the replication handshake
    /// (compared in constant time) and this node presents it to the leaders it fetches from.
    pub secret: Option<String>,
    /// Cluster mutual TLS (v0.4): when set, the listener requires it and fetches use it.
    pub tls: Option<fluxmq_tls::ClusterTls>,
}

impl ReplicationConfig {
    pub fn new(node_id: impl Into<String>, listen_addr: SocketAddr) -> Self {
        Self {
            node_id: node_id.into(),
            listen_addr,
            replica_lag_max: Duration::from_secs(10),
            fetch_max_bytes: 4 * 1024 * 1024,
            fetch_max_wait: Duration::from_millis(100),
            publish_ack_timeout: Duration::from_secs(30),
            secret: None,
            tls: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Core(#[from] fluxmq_core::CoreError),
    #[error("stale epoch {got} < {current}")]
    StaleEpoch { got: u64, current: u64 },
    /// [`ReplicationManager::remove_replica`] is deleting this partition's local data; the
    /// role change is refused (retry once the removal finished).
    #[error("removal of the local replica of {channel}/{partition} in progress")]
    RemovalInProgress { channel: String, partition: u32 },
}

/// Catch-up progress of one follower, as seen by the partition leader
/// ([`ReplicationManager::follower_progress`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowerProgress {
    pub node_id: String,
    /// The follower's log end offset as last reported (`None` = not heard from this epoch).
    pub log_end_offset: Option<u64>,
    /// Messages behind the leader's log end offset (`None` while the LEO is unknown).
    pub lag: Option<u64>,
    pub in_isr: bool,
    /// Its fetches are currently subject to the catch-up throttle (not in the ISR and a
    /// throttle is set for the partition).
    pub throttled: bool,
}

/// Nodes able to read v0.7 batch envelopes according to the cluster's node registry: gates
/// whether a leader starts **writing** envelopes (`ReplicationManager::node_supports_batches`).
/// Serving is gated by what each follower declares on its own connection
/// (`protocol::Capabilities`), never by the registry. Until the cluster sets the list
/// (`None`), every node counts as able (replication used without a registry, e.g. in tests).
#[derive(Default)]
pub(crate) struct NodeVersions {
    capable: RwLock<Option<HashSet<String>>>,
    /// Last "batches refused to an old follower" alert per (node, channel, partition).
    alerted: Mutex<HashMap<(String, String, u32), Instant>>,
    /// Fetches refused because the follower is older than v0.7 (metric).
    pub(crate) refused: std::sync::atomic::AtomicU64,
}

impl NodeVersions {
    pub(crate) fn supports_batches(&self, node: &str) -> bool {
        self.capable
            .read()
            .as_ref()
            .is_none_or(|c| c.contains(node))
    }

    /// Loud, but at most every 30 s per follower and partition.
    pub(crate) fn alert_refused(&self, node: &str, channel: &str, partition: u32) {
        self.refused.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let mut a = self.alerted.lock();
        let key = (node.to_string(), channel.to_string(), partition);
        if a.get(&key)
            .is_some_and(|t| now.duration_since(*t) < Duration::from_secs(30))
        {
            return;
        }
        a.insert(key, now);
        tracing::error!(
            follower = node,
            channel,
            partition,
            "partition holds batch records (v0.7) but its follower runs an older release that \
             can't read them: refusing to replicate; upgrade the node or remove it from the \
             partition's replicas (downgrade after batches were written is unsupported)"
        );
    }
}

/// Cheap to clone.
#[derive(Clone)]
pub struct ReplicationManager {
    inner: Arc<Inner>,
}

struct PartEntry {
    role: PartitionRole,
    handle: ReplicaHandle,
    fetcher: Option<JoinHandle<()>>,
}

pub(crate) struct Inner {
    broker: Broker,
    pub(crate) config: ReplicationConfig,
    local_addr: SocketAddr,
    rt: tokio::runtime::Handle,
    endpoints: RwLock<HashMap<String, SocketAddr>>,
    /// One connection per leader node, shared by its partitions' fetchers.
    clients: tokio::sync::Mutex<HashMap<String, Arc<PeerClient>>>,
    parts: Mutex<HashMap<(String, u32), PartEntry>>,
    isr_tx: broadcast::Sender<IsrChange>,
    /// Partitions whose local data [`ReplicationManager::remove_replica`] is deleting.
    removing: Mutex<HashSet<(String, u32)>>,
    throttles: Arc<throttle::Throttles>,
    versions: Arc<NodeVersions>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    shutdown_tx: watch::Sender<bool>,
    stopped: AtomicBool,
}

impl Inner {
    /// Connection to `node`'s replication listener (connects / reconnects as needed).
    pub(crate) async fn client_for(&self, node: &str) -> Result<Arc<PeerClient>, String> {
        let addr = self
            .endpoints
            .read()
            .get(node)
            .copied()
            .ok_or_else(|| format!("no replication endpoint known for node {node}"))?;
        let mut clients = self.clients.lock().await;
        if let Some(c) = clients.get(node)
            && !c.is_dead()
            && c.addr == addr
        {
            return Ok(c.clone());
        }
        clients.remove(node);
        let c = PeerClient::connect_with(
            addr,
            &self.config.node_id,
            self.config.secret.clone(),
            self.config.tls.as_ref(),
        )
        .await
        .map_err(|e| e.to_string())?;
        let c = Arc::new(c);
        clients.insert(node.to_string(), c.clone());
        Ok(c)
    }

    pub(crate) fn forget_client_if_dead(&self, node: &str) {
        if let Ok(mut clients) = self.clients.try_lock()
            && clients.get(node).is_some_and(|c| c.is_dead())
        {
            clients.remove(node);
        }
    }

    fn tick(&self) {
        let now = Instant::now();
        let handles: Vec<ReplicaHandle> = {
            let mut parts = self.parts.lock();
            parts.retain(|_, e| !e.handle.is_deleted());
            parts
                .values()
                .filter(|e| matches!(e.role, PartitionRole::Leader { .. }))
                .map(|e| e.handle.clone())
                .collect()
        };
        for h in handles {
            h.tick(now);
        }
    }
}

fn stale(e: CoreError) -> ReplicationError {
    match e {
        CoreError::StaleEpoch { got, current } => ReplicationError::StaleEpoch { got, current },
        e => ReplicationError::Core(e),
    }
}

impl ReplicationManager {
    /// Binds the replication listener and starts serving fetches. Switches the broker to
    /// replicated mode: every partition is offline (no publishes, no consumers) until
    /// [`set_role`](Self::set_role) gives it a role — call this before accepting clients.
    pub async fn start(
        broker: Broker,
        config: ReplicationConfig,
    ) -> Result<Self, ReplicationError> {
        broker.enable_replication();
        let listener = TcpListener::bind(config.listen_addr).await?;
        let local_addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let inner = Arc::new(Inner {
            broker: broker.clone(),
            config: config.clone(),
            local_addr,
            rt: tokio::runtime::Handle::current(),
            endpoints: RwLock::new(HashMap::new()),
            clients: tokio::sync::Mutex::new(HashMap::new()),
            parts: Mutex::new(HashMap::new()),
            isr_tx: broadcast::channel(4096).0,
            removing: Mutex::new(HashSet::new()),
            throttles: Arc::new(throttle::Throttles::default()),
            versions: Arc::new(NodeVersions::default()),
            tasks: Mutex::new(Vec::new()),
            shutdown_tx,
            stopped: AtomicBool::new(false),
        });
        let ctx = Arc::new(server::ServerCtx {
            broker: broker.clone(),
            secret: config.secret.clone(),
            node_id: config.node_id.clone(),
            fetch_max_bytes: config
                .fetch_max_bytes
                .min((protocol::MAX_FRAME_LEN / 2) as u32),
            throttles: inner.throttles.clone(),
            versions: inner.versions.clone(),
            tls: config.tls.clone(),
        });
        let mut tasks = vec![tokio::spawn(server::accept_loop(
            listener,
            ctx,
            shutdown_rx,
        ))];
        // ISR maintenance.
        let weak = Arc::downgrade(&inner);
        let every = (config.replica_lag_max / 4)
            .clamp(Duration::from_millis(10), Duration::from_millis(250));
        tasks.push(tokio::spawn(async move {
            let mut t = tokio::time::interval(every);
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                t.tick().await;
                let Some(inner) = weak.upgrade() else { break };
                inner.tick();
            }
        }));
        // Core ISR updates → IsrChange broadcast.
        let mut rx = broker.isr_updates();
        let tx = inner.isr_tx.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(u) => {
                        let _ = tx.send(IsrChange {
                            channel: u.channel,
                            partition: u.partition,
                            epoch: u.epoch,
                            isr: u.isr,
                        });
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(missed = n, "ISR change listener lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }));
        inner.tasks.lock().extend(tasks);
        info!(node = %config.node_id, addr = %local_addr, "replication listener started");
        Ok(Self { inner })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }

    /// Where to reach every node's replication listener.
    pub fn set_endpoints(&self, endpoints: Vec<ReplicaEndpoint>) {
        let mut map = self.inner.endpoints.write();
        map.clear();
        for e in endpoints {
            map.insert(e.node_id, e.addr);
        }
    }

    /// Nodes that advertise v0.7 batch records in the cluster's node registry (input of the
    /// write-format decision, see [`node_supports_batches`](Self::node_supports_batches)).
    ///
    /// Serving is independent of this list: a leader serves envelopes to every follower that
    /// declared [`protocol::FEATURE_BATCH_RECORDS`] in its replication handshake. A follower
    /// that didn't (a release before v0.7) gets only the legacy records before the first
    /// envelope, then an error, with a rate-limited `error!` alert
    /// ([`refused_old_follower_fetches`](Self::refused_old_follower_fetches)).
    pub fn set_batch_capable_nodes(&self, nodes: HashSet<String>) {
        *self.inner.versions.capable.write() = Some(nodes);
    }

    /// Whether `node` may receive batch envelopes (see
    /// [`set_batch_capable_nodes`](Self::set_batch_capable_nodes)).
    pub fn node_supports_batches(&self, node: &str) -> bool {
        self.inner.versions.supports_batches(node)
    }

    /// Fetches refused because the follower runs a release older than v0.7 and the
    /// partition holds batches (total since start).
    pub fn refused_old_follower_fetches(&self) -> u64 {
        self.inner.versions.refused.load(Ordering::Relaxed)
    }

    /// v0.7 rolling upgrade: whether this node, as leader, writes batch envelopes for
    /// (`channel`, `partition`) (`false`: `PublishBatch` is stored as legacy records). The
    /// cluster enables it only while every replica in the assignment runs v0.7 or newer.
    pub fn set_batch_writes(
        &self,
        channel: &str,
        partition: u32,
        enabled: bool,
    ) -> Result<(), ReplicationError> {
        self.inner
            .broker
            .replica(channel, partition)?
            .set_batch_writes(enabled);
        Ok(())
    }

    /// Current write format of (`channel`, `partition`) (see
    /// [`set_batch_writes`](Self::set_batch_writes)); `None` for an unknown partition.
    pub fn batch_writes(&self, channel: &str, partition: u32) -> Option<bool> {
        self.inner
            .broker
            .replica(channel, partition)
            .ok()
            .map(|h| h.batch_writes())
    }

    /// Idempotent; roles with an epoch lower than the partition's current one are rejected
    /// with [`ReplicationError::StaleEpoch`] (nothing changes). A leader ↔ follower change
    /// requires a higher epoch; an equal epoch may update a leader's replicas / `min_isr`, a
    /// follower's leader, or move the partition offline.
    /// Becoming follower: stop serving, truncate to the leader's end offset for our last
    /// epoch, then fetch. Becoming leader: resume dispatch from replicated group offsets,
    /// accept publishes.
    pub fn set_role(
        &self,
        channel: &str,
        partition: u32,
        role: PartitionRole,
    ) -> Result<(), ReplicationError> {
        if self.inner.stopped.load(Ordering::Acquire) {
            return Err(ReplicationError::Core(CoreError::ShuttingDown));
        }
        let key = (channel.to_string(), partition);
        let mut parts = self.inner.parts.lock();
        if self.inner.removing.lock().contains(&key) {
            return Err(ReplicationError::RemovalInProgress {
                channel: channel.to_string(),
                partition,
            });
        }
        let existing = parts.get(&key).filter(|e| !e.handle.is_deleted()).map(|e| {
            (
                e.role.clone(),
                e.handle.clone(),
                e.fetcher.as_ref().is_some_and(|f| !f.is_finished()),
            )
        });
        let handle = match &existing {
            Some((_, h, _)) => h.clone(),
            None => self.inner.broker.replica(channel, partition)?,
        };
        if let Some((cur, _, fetching)) = &existing
            && *cur == role
            && (!matches!(role, PartitionRole::Follower { .. }) || *fetching)
        {
            return Ok(());
        }
        let mut fetcher = None;
        match &role {
            PartitionRole::Leader {
                epoch,
                replicas,
                min_isr,
            } => {
                handle
                    .become_leader(LeaderParams {
                        epoch: *epoch,
                        replicas: replicas.clone(),
                        min_isr: *min_isr,
                        replica_lag_max: self.inner.config.replica_lag_max,
                        ack_timeout: self.inner.config.publish_ack_timeout,
                    })
                    .map_err(stale)?;
            }
            PartitionRole::Follower { epoch, leader } => {
                if *leader == self.inner.config.node_id {
                    return Err(ReplicationError::Core(CoreError::InvalidArgument(
                        "a node cannot follow itself".into(),
                    )));
                }
                handle.become_follower(*epoch, leader).map_err(stale)?;
                let inner = self.inner.clone();
                let (h, e, l) = (handle.clone(), *epoch, leader.clone());
                fetcher = Some(
                    self.inner
                        .rt
                        .spawn(async move { fetcher::run(inner, h, e, l).await }),
                );
            }
            PartitionRole::None => {
                let epoch = handle.epoch();
                handle.become_offline(epoch).map_err(stale)?;
            }
        }
        // The new role is in force (and fences the old one in core); stop the old fetcher.
        if let Some(old) = parts.remove(&key)
            && let Some(f) = old.fetcher
        {
            f.abort();
        }
        parts.insert(
            key,
            PartEntry {
                role,
                handle,
                fetcher,
            },
        );
        Ok(())
    }

    /// Removed replica cleanup (v0.4 moves): this node is **no longer in the replica list**
    /// of (`channel`, `partition`) as of assignment `epoch`. Stops following, takes the
    /// partition offline at `epoch` and deletes everything held locally for it (records,
    /// epoch history, group committed positions on the partition, its DLQ entries — see
    /// `ReplicaHandle::remove_local_data`). When no partition of the channel holds anything
    /// on this node any more, the channel directory is removed too (the channel stays
    /// registered; its storage is re-created automatically if this node later gets a role
    /// for one of its partitions). Returns whether the channel's storage is now released.
    ///
    /// **Only call this for a removal, never for a transient `None` role.** A
    /// [`PartitionRole::None`] alone (leader unknown / partition offline, fenced leadership)
    /// keeps the data: `set_role(None)` never deletes anything. The caller (the cluster
    /// reconciler) decides from the assignment itself: call it when the committed
    /// assignment's `replicas` does not contain this node, with that assignment's epoch.
    ///
    /// Safety checks (nothing is deleted when any fails):
    /// * `epoch` older than the partition's current epoch → [`ReplicationError::StaleEpoch`]
    ///   (the node may have been re-added by a newer assignment);
    /// * the node currently leads or follows the partition **at `epoch` itself** → refused
    ///   with `InvalidArgument` (an assignment that lists the node cannot also remove it;
    ///   every replica-set change bumps the epoch);
    /// * a role set while the deletion runs wins: the deletion steps are fenced on
    ///   "offline at `epoch`", and [`set_role`](Self::set_role) for the partition fails
    ///   with [`ReplicationError::RemovalInProgress`] until it finished (retry).
    ///
    /// Idempotent (calling it again, or for a partition this node never held, is cheap and
    /// returns the same answer); usable at startup for every unassigned partition, which
    /// also removes empty channel skeletons on nodes that hold none of their partitions.
    pub async fn remove_replica(
        &self,
        channel: &str,
        partition: u32,
        epoch: u64,
    ) -> Result<bool, ReplicationError> {
        if self.inner.stopped.load(Ordering::Acquire) {
            return Err(ReplicationError::Core(CoreError::ShuttingDown));
        }
        let key = (channel.to_string(), partition);
        let handle = {
            let mut parts = self.inner.parts.lock();
            let mut removing = self.inner.removing.lock();
            if removing.contains(&key) {
                return Err(ReplicationError::RemovalInProgress {
                    channel: channel.to_string(),
                    partition,
                });
            }
            let handle = match parts.get(&key).filter(|e| !e.handle.is_deleted()) {
                Some(e) => e.handle.clone(),
                None => self.inner.broker.replica(channel, partition)?,
            };
            let current = handle.epoch();
            if epoch < current {
                return Err(ReplicationError::StaleEpoch {
                    got: epoch,
                    current,
                });
            }
            if epoch == current
                && matches!(
                    handle.status().role,
                    ReplicaRole::Leader | ReplicaRole::Follower { .. }
                )
            {
                return Err(ReplicationError::Core(CoreError::InvalidArgument(format!(
                    "{channel}/{partition}: still a replica at epoch {epoch}; refusing to delete"
                ))));
            }
            handle.become_offline(epoch).map_err(stale)?;
            if let Some(old) = parts.remove(&key)
                && let Some(f) = old.fetcher
            {
                f.abort();
            }
            parts.insert(
                key.clone(),
                PartEntry {
                    role: PartitionRole::None,
                    handle: handle.clone(),
                    fetcher: None,
                },
            );
            removing.insert(key.clone());
            handle
        };
        struct Done<'a>(&'a Inner, (String, u32));
        impl Drop for Done<'_> {
            fn drop(&mut self) {
                self.0.removing.lock().remove(&self.1);
            }
        }
        let _done = Done(&self.inner, key);
        let released = handle.remove_local_data(epoch).await.map_err(stale)?;
        Ok(released)
    }

    /// Limits the fetch rate (record bytes per second) this node serves, as partition
    /// leader, to followers of (`channel`, `partition`) that are **not in the ISR** —
    /// replicas catching up during a move (`MoveState::CatchingUp`) or after an outage.
    /// In-sync followers are never throttled. `0` removes the limit. Per follower: each
    /// catching-up follower gets the full rate (`RebalanceConfig::throttle_bytes_per_sec`
    /// is "per moving replica").
    ///
    /// Enforced by whichever node leads the partition, so call it on every node (cheap,
    /// idempotent; it survives leader changes that way) — e.g. from each node's reconciler
    /// for every partition of the plan in `CatchingUp`, and with `0` once the move leaves
    /// that state. The setting is in-memory (re-apply after a restart).
    pub fn set_catch_up_throttle(&self, channel: &str, partition: u32, bytes_per_sec: u64) {
        self.inner.throttles.set(channel, partition, bytes_per_sec);
    }

    /// Current catch-up throttle of a partition (`0` = unlimited).
    pub fn catch_up_throttle(&self, channel: &str, partition: u32) -> u64 {
        self.inner.throttles.get(channel, partition)
    }

    /// Leader only: catch-up progress of every follower of (`channel`, `partition`)
    /// (sorted by node id); `None` when this node does not lead the partition. The executor
    /// can use `lag` of the new replicas for `PartitionMove::lag`; a move is complete once
    /// every new replica has `in_isr`.
    pub fn follower_progress(
        &self,
        channel: &str,
        partition: u32,
    ) -> Option<Vec<FollowerProgress>> {
        let h = self.inner.broker.replica(channel, partition).ok()?;
        let s = h.status();
        if s.role != ReplicaRole::Leader {
            return None;
        }
        let throttled = self.inner.throttles.get(channel, partition) > 0;
        Some(
            s.follower_leo
                .iter()
                .map(|(id, leo)| {
                    let in_isr = s.isr.contains(id);
                    FollowerProgress {
                        node_id: id.clone(),
                        log_end_offset: *leo,
                        lag: leo.map(|l| s.log_end_offset.saturating_sub(l)),
                        in_isr,
                        throttled: throttled && !in_isr,
                    }
                })
                .collect(),
        )
    }

    pub fn status(&self) -> Vec<PartitionReplicationStatus> {
        let entries: Vec<(String, u32, ReplicaHandle)> = {
            let mut parts = self.inner.parts.lock();
            parts.retain(|_, e| !e.handle.is_deleted());
            parts
                .iter()
                .map(|((c, p), e)| (c.clone(), *p, e.handle.clone()))
                .collect()
        };
        let mut out: Vec<PartitionReplicationStatus> = entries
            .into_iter()
            .map(|(channel, partition, h)| {
                let s = h.status();
                let leader = match &s.role {
                    ReplicaRole::Leader => Some(self.inner.config.node_id.clone()),
                    ReplicaRole::Follower { leader } => Some(leader.clone()),
                    _ => None,
                };
                PartitionReplicationStatus {
                    channel,
                    partition,
                    leader,
                    epoch: s.epoch,
                    high_watermark: s.high_watermark,
                    log_end_offset: s.log_end_offset,
                    follower_lag: s
                        .follower_leo
                        .iter()
                        .map(|(id, leo)| {
                            (
                                id.clone(),
                                s.log_end_offset.saturating_sub(leo.unwrap_or(0)),
                            )
                        })
                        .collect(),
                    isr: s.isr,
                }
            })
            .collect();
        out.sort_by(|a, b| (&a.channel, a.partition).cmp(&(&b.channel, b.partition)));
        out
    }

    /// Called by the controller after an [`IsrChange`] for (`channel`, `partition`, `epoch`)
    /// has been durably committed to cluster metadata.
    ///
    /// Invariant (prevents losing acked data on failover): on an ISR **shrink** the leader keeps
    /// computing the HW over the *previous* (larger) ISR until this confirmation arrives, because
    /// the controller may only elect leaders from the committed ISR. ISR **expansion** may take
    /// effect for HW immediately (a superset is always safe). Stale epochs are ignored.
    ///
    /// Also call it right after `set_role(Leader)` with the ISR committed at election: until
    /// the first confirmation every replica counts as committed.
    pub fn isr_committed(&self, channel: &str, partition: u32, epoch: u64, isr: Vec<String>) {
        let h = {
            let parts = self.inner.parts.lock();
            parts
                .get(&(channel.to_string(), partition))
                .map(|e| e.handle.clone())
        };
        if let Some(h) = h {
            h.isr_committed(epoch, &isr);
        }
    }

    pub fn isr_changes(&self) -> broadcast::Receiver<IsrChange> {
        self.inner.isr_tx.subscribe()
    }

    /// Stops the listener, fetchers and peer connections. Partition roles stay as they are
    /// (the broker keeps rejecting publishes for partitions it does not lead).
    pub async fn shutdown(&self) {
        if self.inner.stopped.swap(true, Ordering::AcqRel) {
            return;
        }
        let _ = self.inner.shutdown_tx.send(true);
        for t in self.inner.tasks.lock().drain(..) {
            t.abort();
        }
        let fetchers: Vec<JoinHandle<()>> = self
            .inner
            .parts
            .lock()
            .values_mut()
            .filter_map(|e| e.fetcher.take())
            .collect();
        for f in &fetchers {
            f.abort();
        }
        for f in fetchers {
            let _ = f.await;
        }
        self.inner.clients.lock().await.clear();
        info!(node = %self.inner.config.node_id, "replication stopped");
    }
}
