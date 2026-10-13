//! The controller: runs on every node, acts only while the node is the Raft leader.
//!
//! Each tick: node registry + liveness (Up/Down/Leaving/Decommissioning), Raft membership
//! (learners in, promotion to voter up to 5 voters; never a decommissioning or removed node),
//! assignments for new channels, cleanup of orphaned
//! assignments, failover from the ISR and preferred-leader rebalancing. All assignment writes
//! are compare-and-set on the key's `mod_revision`, so concurrent operator commands and ticks
//! never overwrite each other.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use fluxmq_meta::{MetaError, MetaStore, MetaStoreExt, Precondition};
use fluxmq_proto::ChannelConfig;
use fluxmq_proto::cluster::{NodeStatus, PartitionAssignment};
use fluxmq_proto::model::validate_channel_name;
use openraft::ChangeMembers;
use tokio::time::Instant;

use crate::assign::{self, View};
use crate::raft::{RaftNode, raft_id};
use crate::rpc::{ControllerOk, ControllerOp, NodeRecord, RpcError, RpcErrorKind};
use crate::runtime::Runtime;

pub const NODES_PREFIX: &str = "cluster/nodes/";
pub const CHANNELS_PREFIX: &str = "cluster/channels/";
pub const ASSIGNMENTS_PREFIX: &str = "cluster/assignments/";
const MAX_VOTERS: usize = 5;
const MEMBERSHIP_TIMEOUT: Duration = Duration::from_secs(5);
/// After an election, a new channel is placed on fewer live nodes than the cluster is known to
/// have (bounded by the RF) only once this long has passed: channels created right after the
/// election (the system channels) would otherwise get a reduced replication factor while the
/// other nodes' first heartbeats are still in flight.
const PLACEMENT_GRACE: Duration = Duration::from_secs(5);

pub fn node_key(id: &str) -> String {
    format!("{NODES_PREFIX}{id}")
}
pub fn channel_key(name: &str) -> String {
    format!("{CHANNELS_PREFIX}{name}")
}
pub fn assignment_key(name: &str) -> String {
    format!("{ASSIGNMENTS_PREFIX}{name}")
}

struct Seen {
    at: Instant,
    record: NodeRecord,
    /// First heartbeat of the current "alive" streak.
    since: Instant,
}

/// In-memory controller state (only meaningful on the leader; reset on leadership change).
#[derive(Default)]
pub(crate) struct ControllerState {
    leader_since: Option<Instant>,
    seen: HashMap<String, Seen>,
    /// Node → incarnation that left gracefully; its heartbeats are ignored (a restart has a
    /// new incarnation and is welcome).
    left: HashMap<String, u64>,
    /// Restarted node → (incarnation, channels whose demotion was committed since the restart
    /// was observed). The restart is recorded once this covers every channel.
    restart_progress: HashMap<String, (u64, BTreeSet<String>)>,
    /// Node → storage bytes from its latest heartbeat (rebalance tie-breaker / status).
    storage: HashMap<String, u64>,
    /// v0.5: decommissioning node → since when no move off it could be planned.
    pub(crate) decommission_stalled: HashMap<String, Instant>,
}

impl ControllerState {
    /// Records a heartbeat; `false` (ignored) for a process that left gracefully.
    pub fn observe(&mut self, record: NodeRecord, at: Instant) -> bool {
        if self.left.get(&record.id) == Some(&record.incarnation) {
            return false;
        }
        let since = match self.seen.get(&record.id) {
            Some(s) if s.record.incarnation == record.incarnation => s.since,
            _ => at,
        };
        self.seen
            .insert(record.id.clone(), Seen { at, record, since });
        true
    }

    pub fn observe_storage(&mut self, node: &str, bytes: Option<u64>) {
        if let Some(b) = bytes {
            self.storage.insert(node.to_string(), b);
        }
    }

    /// Drops the liveness state of removed processes (`node → newest refused incarnation`).
    pub fn forget_removed(&mut self, refused: &HashMap<String, u64>) {
        self.seen.retain(|id, s| {
            refused
                .get(id)
                .is_none_or(|inc| s.record.incarnation > *inc)
        });
    }

    /// Forgets a node entirely (removed from the cluster).
    pub fn remove_node(&mut self, node: &str) {
        self.seen.remove(node);
        self.restart_progress.remove(node);
        self.storage.remove(node);
    }
}

/// Snapshot of the liveness view.
struct Liveness {
    view: View,
    stable: BTreeSet<String>,
    /// Registry updates to write.
    updates: Vec<NodeRecord>,
}

fn liveness(
    rt: &Runtime,
    st: &ControllerState,
    records: &BTreeMap<String, NodeRecord>,
    now: Instant,
) -> Liveness {
    let t = &rt.timings;
    // v0.5: nodes being decommissioned keep that status (whatever their incarnation) and,
    // like leaving nodes, never lead or receive new channels.
    let decommissioning = crate::decommission::active(rt);
    let in_grace = st
        .leader_since
        .is_none_or(|s| now.duration_since(s) < t.controller_grace);
    let mut view = View::default();
    let mut stable = BTreeSet::new();
    let mut updates = Vec::new();
    for (id, s) in &st.seen {
        if now.duration_since(s.at) < t.node_timeout {
            view.alive.insert(id.clone());
            if now.duration_since(s.since) >= t.rebalance_stable {
                stable.insert(id.clone());
            }
            let mut rec = s.record.clone();
            rec.status = match records.get(id) {
                _ if decommissioning.contains(id) => NodeStatus::Decommissioning,
                Some(old)
                    if old.status == NodeStatus::Leaving && old.incarnation == rec.incarnation =>
                {
                    NodeStatus::Leaving
                }
                _ => NodeStatus::Up,
            };
            if records.get(id) != Some(&rec) {
                updates.push(rec);
            }
        }
    }
    for (id, rec) in records {
        if view.alive.contains(id) {
            continue;
        }
        if in_grace && rec.status != NodeStatus::Down {
            // A new controller has no heartbeat history yet: assume nodes that were up still
            // are until the grace period (≥ any lease its predecessor granted) has passed.
            view.alive.insert(id.clone());
        } else if rec.status != NodeStatus::Down {
            let mut down = rec.clone();
            down.status = NodeStatus::Down;
            updates.push(down);
        }
    }
    for (id, rec) in records {
        if matches!(
            rec.status,
            NodeStatus::Leaving | NodeStatus::Decommissioning
        ) && view.alive.contains(id)
        {
            view.leaving.insert(id.clone());
        }
    }
    for rec in &updates {
        if matches!(
            rec.status,
            NodeStatus::Leaving | NodeStatus::Decommissioning
        ) {
            view.leaving.insert(rec.id.clone());
        }
    }
    for id in &decommissioning {
        if view.alive.contains(id) {
            view.leaving.insert(id.clone());
        }
    }
    Liveness {
        view,
        stable,
        updates,
    }
}

pub(crate) fn records(rt: &Runtime) -> BTreeMap<String, NodeRecord> {
    rt.meta
        .list_as::<NodeRecord>(NODES_PREFIX)
        .unwrap_or_default()
        .into_iter()
        .map(|(_, r)| (r.id.clone(), r))
        .collect()
}

/// Current liveness view (leader only; empty `updates` are not written here).
pub(crate) fn current_view(rt: &Runtime) -> (View, BTreeMap<String, NodeRecord>) {
    let recs = records(rt);
    let st = rt.controller.lock();
    let l = liveness(rt, &st, &recs, Instant::now());
    (l.view, recs)
}

/// The controller's liveness picture for the rebalancer (leader only).
pub(crate) struct Snapshot {
    pub view: View,
    /// Alive nodes up for at least `rebalance_stable`.
    pub stable: BTreeSet<String>,
    pub records: BTreeMap<String, NodeRecord>,
    /// Node → last heartbeat received.
    pub last_seen: HashMap<String, Instant>,
    /// Node → storage bytes (latest heartbeat).
    pub storage: BTreeMap<String, u64>,
    /// v0.5: nodes being decommissioned (never targets; every replica moves off).
    pub decommission: BTreeSet<String>,
}

pub(crate) fn snapshot(rt: &Runtime) -> Snapshot {
    let recs = records(rt);
    let st = rt.controller.lock();
    let l = liveness(rt, &st, &recs, Instant::now());
    Snapshot {
        view: l.view,
        stable: l.stable,
        last_seen: st.seen.iter().map(|(id, s)| (id.clone(), s.at)).collect(),
        storage: st
            .storage
            .iter()
            .filter(|(id, _)| recs.contains_key(*id))
            .map(|(id, b)| (id.clone(), *b))
            .collect(),
        records: recs,
        decommission: crate::decommission::active(rt),
    }
}

/// Whether a new channel may be placed on `cands` now (see [`PLACEMENT_GRACE`]).
fn placement_ready(rt: &Runtime, cands: &[String], recs: &BTreeMap<String, NodeRecord>) -> bool {
    if cands.is_empty() {
        return false;
    }
    // Nodes that will be placement candidates: registered Up / Joining ones, plus initial peers
    // that haven't registered yet (bootstrap). Down, leaving and decommissioning nodes never
    // take new replicas, so they don't count.
    let registered = recs
        .values()
        .filter(|r| matches!(r.status, NodeStatus::Up | NodeStatus::Joining))
        .count();
    let unregistered = rt
        .cfg
        .peers
        .iter()
        .filter(|p| !recs.contains_key(&p.id))
        .count();
    let known = registered + unregistered;
    let want = (rt.cfg.replication_factor.max(1) as usize).min(known);
    cands.len() >= want
        || rt
            .controller
            .lock()
            .leader_since
            .is_some_and(|t| t.elapsed() >= PLACEMENT_GRACE)
}

fn candidates(view: &View) -> Vec<String> {
    view.alive
        .iter()
        .filter(|n| !view.leaving.contains(*n))
        .cloned()
        .collect()
}

pub(crate) async fn run(rt: Arc<Runtime>) {
    let mut tick = tokio::time::interval(rt.timings.tick * 2);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = rt.shutdown.cancelled() => return,
            _ = tick.tick() => {}
        }
        if !rt.is_leader() {
            let mut st = rt.controller.lock();
            st.leader_since = None;
            st.seen.clear();
            st.restart_progress.clear();
            st.decommission_stalled.clear();
            continue;
        }
        {
            let mut st = rt.controller.lock();
            if st.leader_since.is_none() {
                tracing::info!(node = %rt.node_id(), "became controller");
                st.leader_since = Some(Instant::now());
            }
        }
        if let Err(e) = tick_once(&rt).await {
            tracing::debug!(error = %e, "controller tick");
        }
    }
}

async fn tick_once(rt: &Runtime) -> Result<(), MetaError> {
    let _serial = rt.controller_lock.lock().await;
    // A heartbeat of a removed (decommissioned / forgotten) process that raced its removal
    // must not re-register it.
    rt.controller
        .lock()
        .forget_removed(&crate::decommission::refused(rt));
    let recs = records(rt);
    let live = {
        let st = rt.controller.lock();
        liveness(rt, &st, &recs, Instant::now())
    };
    // A node that restarted (new incarnation, not yet recorded) lost its in-memory replica
    // state: for this tick treat it as dead, i.e. move its leaderships to other ISR members
    // and drop it from ISRs. It leads again only after rejoining the ISR (preferred-leader
    // election once stable).
    let restarted: BTreeSet<String> = live
        .updates
        .iter()
        .filter(|r| {
            recs.get(&r.id)
                .is_some_and(|old| old.incarnation != r.incarnation)
        })
        .map(|r| r.id.clone())
        .collect();
    let mut view = live.view.clone();
    for id in &restarted {
        view.alive.remove(id);
        view.leaving.remove(id);
    }
    // Down nodes are written first (they only gate future decisions); restarts are recorded
    // only once their demotion is persisted, so a failed write is retried next tick.
    for rec in live.updates.iter().filter(|r| !restarted.contains(&r.id)) {
        if recs.get(&rec.id).map(|r| r.status) != Some(rec.status) {
            tracing::info!(node = %rec.id, status = ?rec.status, "node status");
        }
        rt.meta.put_as(&node_key(&rec.id), rec).await?;
    }
    membership(rt, &view, &recs).await;
    // CAS conflicts are common while a node rejoins (its leaders persist ISR expansions
    // concurrently with this tick dropping it from ISRs): re-read and retry a few times.
    let mut demoted: BTreeSet<String> = BTreeSet::new();
    let channels: Vec<String> = parse_assignments(rt).into_keys().collect();
    for _ in 0..3 {
        let conflicted = assignments(rt, &view, &live.stable).await?;
        // Written, or nothing to change: the restarted nodes are neither leader nor in the ISR
        // of these channels as of a write committed after the restart was observed.
        demoted.extend(
            channels
                .iter()
                .filter(|c| !conflicted.contains(*c))
                .cloned(),
        );
        if conflicted.is_empty() {
            break;
        }
    }
    // Record a restart (→ lease, rejoin) once every channel has had its demotion committed at
    // least once since the restart was observed — per channel, across ticks — and no
    // assignment names it leader. Requiring one conflict-free pass over *all* channels in a
    // single tick delayed rejoining by tens of seconds while its leaders kept re-adding the
    // catching-up node to ISRs (legitimate re-adds: they follow the committed removal).
    if !restarted.is_empty() {
        let current = parse_assignments(rt);
        let still_leading: BTreeSet<String> = current
            .values()
            .flat_map(|(_, parts)| parts.iter())
            .filter_map(|p| p.leader.clone())
            .filter(|l| restarted.contains(l))
            .collect();
        let mut ready = Vec::new();
        {
            let mut st = rt.controller.lock();
            st.restart_progress.retain(|id, _| restarted.contains(id));
            for rec in live.updates.iter().filter(|r| restarted.contains(&r.id)) {
                let progress = st
                    .restart_progress
                    .entry(rec.id.clone())
                    .or_insert_with(|| (rec.incarnation, BTreeSet::new()));
                if progress.0 != rec.incarnation {
                    *progress = (rec.incarnation, BTreeSet::new());
                }
                progress.1.extend(demoted.iter().cloned());
                let all_demoted = current.keys().all(|c| progress.1.contains(c));
                if all_demoted && !still_leading.contains(&rec.id) {
                    ready.push(rec.clone());
                }
            }
        }
        for rec in ready {
            tracing::info!(node = %rec.id, "node restarted; leaderships moved, rejoining ISRs");
            rt.meta.put_as(&node_key(&rec.id), &rec).await?;
            rt.controller.lock().restart_progress.remove(&rec.id);
        }
    }
    Ok(())
}

async fn membership(rt: &Runtime, view: &View, recs: &BTreeMap<String, NodeRecord>) {
    let metrics = rt.raft.metrics().borrow().clone();
    let membership = metrics.membership_config.membership().clone();
    if membership.get_joint_config().len() > 1 {
        return;
    }
    let nodes: BTreeMap<u64, RaftNode> =
        membership.nodes().map(|(id, n)| (*id, n.clone())).collect();
    let seen: Vec<NodeRecord> = {
        let st = rt.controller.lock();
        st.seen.values().map(|s| s.record.clone()).collect()
    };
    // v0.5: a node being decommissioned is never (re-)added; a removed process neither (its
    // heartbeats are refused, but one may have raced the removal).
    let decommissioning = crate::decommission::active(rt);
    let refused = crate::decommission::refused(rt);
    let joinable = |r: &NodeRecord| {
        view.is_alive(&r.id)
            && !decommissioning.contains(&r.id)
            && refused.get(&r.id).is_none_or(|inc| r.incarnation > *inc)
    };
    for rec in seen.iter().filter(|r| joinable(r)) {
        let id = raft_id(&rec.id);
        match nodes.get(&id) {
            Some(n) if n.name != rec.id => {
                tracing::error!(node = %rec.id, existing = %n.name, "raft id collision; node not added");
            }
            Some(_) => {}
            None => {
                tracing::info!(node = %rec.id, "adding node as raft learner");
                let node = RaftNode {
                    name: rec.id.clone(),
                    addr: rec.raft_addr.clone(),
                };
                match tokio::time::timeout(MEMBERSHIP_TIMEOUT, rt.raft.add_learner(id, node, false))
                    .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => tracing::warn!(error = %e, "add learner"),
                    Err(_) => tracing::warn!("add learner timed out"),
                }
                return;
            }
        }
    }
    let voters: BTreeSet<u64> = membership.voter_ids().collect();
    if voters.len() >= MAX_VOTERS {
        return;
    }
    let last = metrics.last_log_index.unwrap_or(0);
    let replication = metrics.replication.unwrap_or_default();
    for learner in membership.learner_ids() {
        let Some(n) = nodes.get(&learner) else {
            continue;
        };
        let up = recs
            .get(&n.name)
            .is_some_and(|r| r.status == NodeStatus::Up);
        let matched = replication
            .get(&learner)
            .and_then(|m| m.map(|l| l.index))
            .unwrap_or(0);
        if up && view.is_alive(&n.name) && !decommissioning.contains(&n.name) && matched + 5 >= last
        {
            tracing::info!(node = %n.name, "promoting learner to voter");
            let change = ChangeMembers::AddVoterIds(BTreeSet::from([learner]));
            match tokio::time::timeout(MEMBERSHIP_TIMEOUT, rt.raft.change_membership(change, true))
                .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => tracing::warn!(error = %e, "promote voter"),
                Err(_) => tracing::warn!("promote voter timed out"),
            }
            return;
        }
    }
}

pub(crate) fn parse_assignments(rt: &Runtime) -> BTreeMap<String, (u64, Vec<PartitionAssignment>)> {
    rt.meta
        .list(ASSIGNMENTS_PREFIX)
        .into_iter()
        .filter_map(|(k, v)| {
            let name = k.strip_prefix(ASSIGNMENTS_PREFIX)?.to_string();
            let parts = serde_json::from_value(v.value).ok()?;
            Some((name, (v.mod_revision, parts)))
        })
        .collect()
}

pub(crate) async fn cas_put(
    rt: &Runtime,
    key: &str,
    value: &impl serde::Serialize,
    expected: u64,
) -> Result<u64, MetaError> {
    let value = serde_json::to_value(value)?;
    rt.meta
        .put(
            key,
            value,
            Some(Precondition {
                key: key.to_string(),
                expected_mod_revision: expected,
            }),
        )
        .await
}

async fn assignments(
    rt: &Runtime,
    view: &View,
    stable: &BTreeSet<String>,
) -> Result<BTreeSet<String>, MetaError> {
    let channels: BTreeMap<String, ChannelConfig> = rt
        .meta
        .list_as::<ChannelConfig>(CHANNELS_PREFIX)?
        .into_iter()
        .map(|(_, c)| (c.name.clone(), c))
        .collect();
    let current = parse_assignments(rt);
    let mut load = assign::leader_load(current.values().map(|(_, p)| p));
    let cands = candidates(view);
    let ready = placement_ready(rt, &cands, &records(rt));

    for (name, cfg) in &channels {
        if !current.contains_key(name) && ready {
            let parts =
                assign::new_assignment(cfg.partitions, &cands, rt.cfg.replication_factor, &load);
            // Channels placed in the same pass (e.g. both system channels right after start)
            // spread their leaders instead of all landing on the least-loaded node.
            for l in parts.iter().filter_map(|p| p.leader.clone()) {
                *load.entry(l).or_default() += 1;
            }
            match cas_put(rt, &assignment_key(name), &parts, 0).await {
                Ok(_) | Err(MetaError::Conflict { .. }) => {}
                Err(e) => return Err(e),
            }
        }
    }
    for (name, (rev, _)) in &current {
        if !channels.contains_key(name) {
            let key = assignment_key(name);
            let pre = Precondition {
                key: key.clone(),
                expected_mod_revision: *rev,
            };
            match rt.meta.delete(&key, Some(pre)).await {
                Ok(_) | Err(MetaError::Conflict { .. }) => {}
                Err(e) => return Err(e),
            }
        }
    }
    // Channels whose assignment write lost a CAS race this pass.
    let mut conflicted = BTreeSet::new();
    for (name, (rev, parts)) in current {
        let Some(cfg) = channels.get(&name) else {
            continue;
        };
        let mut changed = false;
        // Partitions without any replica (placed while no node was alive, before v0.5): no
        // copy of their data exists anywhere, so placing them afresh is safe.
        let fresh = (ready && parts.iter().any(|p| p.replicas.is_empty())).then(|| {
            assign::new_assignment(cfg.partitions, &cands, rt.cfg.replication_factor, &load)
        });
        let next: Vec<PartitionAssignment> = parts
            .iter()
            .map(|p| {
                if let Some(f) = fresh
                    .as_ref()
                    .filter(|_| p.replicas.is_empty())
                    .and_then(|f| f.iter().find(|x| x.partition == p.partition))
                {
                    let mut n = f.clone();
                    n.epoch = p.epoch + 1;
                    tracing::info!(channel = %name, partition = p.partition, replicas = ?n.replicas, "placing a partition that had no replicas");
                    changed = true;
                    n
                } else if let Some(n) = assign::failover(p, view) {
                    tracing::info!(channel = %name, partition = p.partition, leader = ?n.leader, epoch = n.epoch, isr = ?n.isr, "failover");
                    changed = true;
                    n
                } else if let Some(n) = assign::preferred_election(p, view, stable) {
                    tracing::info!(channel = %name, partition = p.partition, leader = ?n.leader, epoch = n.epoch, "preferred leader election");
                    changed = true;
                    n
                } else {
                    p.clone()
                }
            })
            .collect();
        if changed {
            match cas_put(rt, &assignment_key(&name), &next, rev).await {
                Ok(_) => {}
                Err(MetaError::Conflict { .. }) => {
                    conflicted.insert(name.clone());
                }
                Err(e) => return Err(e),
            }
        }
    }
    Ok(conflicted)
}

pub(crate) fn meta_err(e: MetaError) -> RpcError {
    match e {
        MetaError::Conflict { .. } => RpcError::new(RpcErrorKind::Conflict, e.to_string()),
        MetaError::Unavailable(m) => RpcError::new(RpcErrorKind::Unavailable, m),
        MetaError::InvalidKey(_) | MetaError::TooLarge(_) => {
            RpcError::new(RpcErrorKind::Invalid, e.to_string())
        }
        e => RpcError::new(RpcErrorKind::Internal, e.to_string()),
    }
}

/// Executes an operator/API command (leader only).
pub(crate) async fn execute(rt: &Runtime, op: ControllerOp) -> Result<ControllerOk, RpcError> {
    let _serial = rt.controller_lock.lock().await;
    if !rt.is_leader() {
        return Err(RpcError::not_leader(rt.leader_hint()));
    }
    match op {
        ControllerOp::CreateChannel(cfg) => create_channel(rt, cfg).await,
        ControllerOp::DeleteChannel(name) => delete_channel(rt, &name).await,
        ControllerOp::Reassign {
            channel,
            partition,
            replicas,
        } => reassign(rt, &channel, partition, replicas).await,
        ControllerOp::Drain(node) => drain(rt, &node).await,
        ControllerOp::Leave { node, incarnation } => leave(rt, &node, incarnation).await,
        ControllerOp::Decommission(node) => crate::decommission::start(rt, &node)
            .await
            .map(|s| decommission_ok(rt, s)),
        ControllerOp::Forget(node) => crate::decommission::forget(rt, &node)
            .await
            .map(|s| decommission_ok(rt, s)),
        ControllerOp::DecommissionDryRun(node) => {
            crate::decommission::dry_run(rt, &node).map(|s| rebalance_ok(rt, s))
        }
        ControllerOp::RebalanceStatus => Ok(rebalance_ok(rt, crate::rebalancer::status(rt))),
        ControllerOp::RebalanceConfig(cfg) => crate::rebalancer::set_config(rt, cfg)
            .await
            .map(|s| rebalance_ok(rt, s)),
        ControllerOp::Rebalance(req) => crate::rebalancer::request(rt, req)
            .await
            .map(|s| rebalance_ok(rt, s)),
        ControllerOp::RebalanceControl(action) => crate::rebalancer::control(rt, action)
            .await
            .map(|s| rebalance_ok(rt, s)),
    }
}

fn rebalance_ok(rt: &Runtime, status: fluxmq_proto::cluster::RebalanceStatus) -> ControllerOk {
    ControllerOk {
        revision: rt.sm.revision(),
        count: status.moves.len() as u32,
        rebalance: Some(Box::new(status)),
        decommission: None,
    }
}

fn decommission_ok(
    rt: &Runtime,
    status: fluxmq_proto::cluster::DecommissionStatus,
) -> ControllerOk {
    ControllerOk {
        revision: rt.sm.revision(),
        count: status.replicas_remaining,
        rebalance: None,
        decommission: Some(status),
    }
}

fn invalid(msg: impl Into<String>) -> RpcError {
    RpcError::new(RpcErrorKind::Invalid, msg)
}

async fn create_channel(rt: &Runtime, cfg: ChannelConfig) -> Result<ControllerOk, RpcError> {
    validate_channel_name(&cfg.name).map_err(invalid)?;
    if cfg.partitions == 0 {
        return Err(invalid("partitions must be >= 1"));
    }
    if cfg.mode == fluxmq_proto::model::ChannelMode::Priority
        && !fluxmq_proto::model::PRIORITY_LEVELS.contains(&cfg.partitions)
    {
        return Err(invalid(format!(
            "a priority channel has {}-{} levels (partitions), got {}",
            fluxmq_proto::model::PRIORITY_LEVELS.start(),
            fluxmq_proto::model::PRIORITY_LEVELS.end(),
            cfg.partitions
        )));
    }
    if cfg.retry.max_attempts == 0 {
        return Err(invalid("retry.max_attempts must be >= 1"));
    }
    let mut cfg = cfg;
    if cfg.created_at_ms == 0 {
        cfg.created_at_ms = fluxmq_proto::now_ms();
    }
    // Names differing only in case share a directory on case-insensitive filesystems.
    if let Some(existing) = rt
        .meta
        .list(CHANNELS_PREFIX)
        .into_iter()
        .find_map(|(k, _)| {
            let name = k.strip_prefix(CHANNELS_PREFIX)?;
            name.eq_ignore_ascii_case(&cfg.name)
                .then(|| name.to_string())
        })
    {
        return Err(RpcError::new(
            RpcErrorKind::AlreadyExists,
            format!("channel '{existing}' already exists"),
        ));
    }
    let ckey = channel_key(&cfg.name);
    match cas_put(rt, &ckey, &cfg, 0).await {
        Ok(_) => {}
        Err(MetaError::Conflict { .. }) => {
            return Err(RpcError::new(
                RpcErrorKind::AlreadyExists,
                format!("channel '{}' already exists", cfg.name),
            ));
        }
        Err(e) => return Err(meta_err(e)),
    }
    let (view, recs) = current_view(rt);
    let cands = candidates(&view);
    if !placement_ready(rt, &cands, &recs) {
        // Right after an election: no live node yet (an assignment now would have no
        // replicas and could never get a leader) or fewer than the cluster has (reduced RF).
        // Leave the channel unplaced; the controller tick places it once nodes are alive.
        tracing::info!(channel = %cfg.name, "channel created; placement waits for live nodes");
        return Ok(ControllerOk {
            revision: rt.meta.revision(),
            count: 0,
            rebalance: None,
            decommission: None,
        });
    }
    let current = parse_assignments(rt);
    let load = assign::leader_load(current.values().map(|(_, p)| p));
    let mut parts =
        assign::new_assignment(cfg.partitions, &cands, rt.cfg.replication_factor, &load);
    let akey = assignment_key(&cfg.name);
    // A leftover assignment of an earlier channel with this name: supersede its epochs.
    let expected = match current.get(&cfg.name) {
        Some((rev, old)) => {
            let base = old.iter().map(|p| p.epoch).max().unwrap_or(0);
            for p in &mut parts {
                p.epoch = base + 1;
            }
            *rev
        }
        None => 0,
    };
    let revision = match cas_put(rt, &akey, &parts, expected).await {
        Ok(r) => r,
        // The controller tick placed it concurrently.
        Err(MetaError::Conflict { .. }) => rt.meta.revision(),
        Err(e) => return Err(meta_err(e)),
    };
    tracing::info!(channel = %cfg.name, partitions = cfg.partitions, "channel created");
    Ok(ControllerOk {
        revision,
        count: 0,
        rebalance: None,
        decommission: None,
    })
}

async fn delete_channel(rt: &Runtime, name: &str) -> Result<ControllerOk, RpcError> {
    let ckey = channel_key(name);
    let Some(v) = rt.meta.get(&ckey) else {
        return Err(RpcError::new(
            RpcErrorKind::NotFound,
            format!("channel '{name}' not found"),
        ));
    };
    rt.meta
        .delete(
            &ckey,
            Some(Precondition {
                key: ckey.clone(),
                expected_mod_revision: v.mod_revision,
            }),
        )
        .await
        .map_err(meta_err)?;
    let revision = rt
        .meta
        .delete(&assignment_key(name), None)
        .await
        .map_err(meta_err)?;
    tracing::info!(channel = %name, "channel deleted");
    Ok(ControllerOk {
        revision,
        count: 0,
        rebalance: None,
        decommission: None,
    })
}

async fn reassign(
    rt: &Runtime,
    channel: &str,
    partition: u32,
    replicas: Vec<String>,
) -> Result<ControllerOk, RpcError> {
    let (view, recs) = current_view(rt);
    if let Some(unknown) = replicas.iter().find(|r| !recs.contains_key(*r)) {
        return Err(invalid(format!("unknown node '{unknown}'")));
    }
    let key = assignment_key(channel);
    let Some(v) = rt.meta.get(&key) else {
        return Err(RpcError::new(
            RpcErrorKind::NotFound,
            format!("channel '{channel}' has no assignment"),
        ));
    };
    let mut parts: Vec<PartitionAssignment> = serde_json::from_value(v.value)
        .map_err(|e| RpcError::new(RpcErrorKind::Internal, e.to_string()))?;
    let Some(p) = parts.iter_mut().find(|p| p.partition == partition) else {
        return Err(RpcError::new(
            RpcErrorKind::NotFound,
            format!("partition {partition} not found in '{channel}'"),
        ));
    };
    *p = assign::reassign_partition(p, replicas, &view).map_err(invalid)?;
    let revision = cas_put(rt, &key, &parts, v.mod_revision)
        .await
        .map_err(meta_err)?;
    Ok(ControllerOk {
        revision,
        count: 1,
        rebalance: None,
        decommission: None,
    })
}

/// Graceful leave: `node` (already drained, no longer following) is marked `Down`; whatever
/// it still leads fails over and it is dropped from every ISR right away.
async fn leave(rt: &Runtime, node: &str, incarnation: u64) -> Result<ControllerOk, RpcError> {
    let recs = records(rt);
    let Some(rec) = recs.get(node) else {
        return Err(RpcError::new(
            RpcErrorKind::NotFound,
            format!("unknown node '{node}'"),
        ));
    };
    if rec.incarnation != incarnation {
        return Err(RpcError::new(
            RpcErrorKind::Invalid,
            format!("node '{node}' restarted since; not leaving"),
        ));
    }
    {
        let mut st = rt.controller.lock();
        st.left.insert(node.to_string(), incarnation);
        st.seen.remove(node);
    }
    let mut down = rec.clone();
    down.status = NodeStatus::Down;
    let mut revision = rt
        .meta
        .put_as(&node_key(node), &down)
        .await
        .map_err(meta_err)?;
    let (view, _) = current_view(rt);
    let mut moved = 0;
    for _attempt in 0..3 {
        let mut conflict = false;
        for (name, (rev, parts)) in parse_assignments(rt) {
            let mut changed = 0;
            let next: Vec<PartitionAssignment> = parts
                .iter()
                .map(|p| match assign::failover(p, &view) {
                    Some(n) => {
                        changed += 1;
                        n
                    }
                    None => p.clone(),
                })
                .collect();
            if changed > 0 {
                match cas_put(rt, &assignment_key(&name), &next, rev).await {
                    Ok(r) => {
                        revision = r;
                        moved += changed;
                    }
                    Err(MetaError::Conflict { .. }) => conflict = true,
                    Err(e) => return Err(meta_err(e)),
                }
            }
        }
        if !conflict {
            break;
        }
    }
    tracing::info!(%node, partitions = moved, "node left the cluster");
    Ok(ControllerOk {
        revision,
        count: moved,
        rebalance: None,
        decommission: None,
    })
}

/// CAS rounds of a drain before giving up on channels that keep changing.
const DRAIN_ATTEMPTS: usize = 5;

async fn drain(rt: &Runtime, node: &str) -> Result<ControllerOk, RpcError> {
    let (_, recs) = current_view(rt);
    let Some(rec) = recs.get(node) else {
        return Err(RpcError::new(
            RpcErrorKind::NotFound,
            format!("unknown node '{node}'"),
        ));
    };
    let mut leaving = rec.clone();
    // A decommissioning node keeps its status (it is drained the same way).
    if leaving.status != NodeStatus::Decommissioning {
        leaving.status = NodeStatus::Leaving;
    }
    let revision = rt
        .meta
        .put_as(&node_key(node), &leaving)
        .await
        .map_err(meta_err)?;
    let (rev, moved) = drain_leaderships(rt, node).await?;
    tracing::info!(%node, moved, "node drained");
    Ok(ControllerOk {
        revision: revision.max(rev),
        count: moved,
        rebalance: None,
        decommission: None,
    })
}

/// Moves every leadership of `node` to another live ISR member (CAS, retried). Returns the
/// last revision written and the number of leaderships moved. The caller has already marked
/// the node `Leaving` / `Decommissioning` (so elections don't hand leadership back).
pub(crate) async fn drain_leaderships(rt: &Runtime, node: &str) -> Result<(u64, u32), RpcError> {
    let (mut view, _) = current_view(rt);
    let mut revision = rt.sm.revision();
    view.leaving.insert(node.to_string());
    // Re-read and retry channels whose CAS lost a race (partition leaders persist ISR changes
    // concurrently, the controller tick may elect): a skipped channel would stay led by the
    // leaving node until its leave, stalling publishes to it for the forward timeout.
    let mut moved = 0;
    let mut stuck: Vec<String> = Vec::new();
    for attempt in 0..DRAIN_ATTEMPTS {
        let mut conflict = false;
        stuck.clear();
        for (name, (rev, parts)) in parse_assignments(rt) {
            let mut changed = 0;
            let next: Vec<PartitionAssignment> = parts
                .iter()
                .map(|p| match assign::drain_partition(p, node, &view) {
                    Some(n) => {
                        changed += 1;
                        n
                    }
                    None => {
                        if p.leader.as_deref() == Some(node) {
                            stuck.push(format!("{name}/{}", p.partition));
                        }
                        p.clone()
                    }
                })
                .collect();
            if changed > 0 {
                match cas_put(rt, &assignment_key(&name), &next, rev).await {
                    Ok(r) => {
                        revision = r;
                        moved += changed;
                    }
                    Err(MetaError::Conflict { .. }) => conflict = true,
                    Err(e) => return Err(meta_err(e)),
                }
            }
        }
        if !conflict {
            break;
        }
        tracing::debug!(%node, attempt, "drain lost a CAS race; retrying");
    }
    if !stuck.is_empty() {
        // No other in-sync replica to take over; the leave fails these over (or they wait
        // for a replica to catch up).
        tracing::warn!(%node, partitions = ?stuck, "partitions could not be drained (no other in-sync replica)");
    }
    Ok((revision, moved))
}
