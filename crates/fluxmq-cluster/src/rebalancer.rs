//! The rebalance **executor** and **triggers** (v0.4). Runs on every node, acts only on the
//! controller (Raft leader). Planning is pure ([`crate::rebalance`]); this module does the
//! I/O: it persists the plan at [`PLAN_KEY`], drives every move through the move protocol
//! with compare-and-set assignment writes, and evaluates the automatic triggers.
//!
//! Move protocol (per partition, see the proto docs):
//! 1. `Pending → CatchingUp`: `replicas = from ∪ to` (old preferred first) at the same leader
//!    and epoch, so the new followers start fetching without entering the ISR;
//! 2. once every new replica is in the committed ISR: commit `to` (`CatchingUp → Finalizing`);
//!    the leader is kept when it stays a replica, otherwise the first live ISR member of `to`
//!    takes over (epoch + 1);
//! 3. preferred-leader election to `to[0]` (`Finalizing → Done`).
//!
//! A move whose new replicas don't reach the ISR within `move_timeout_secs` is rolled back to
//! `from`. Every write is a CAS on the assignment key, retried on conflict like the drain;
//! the plan is written after the assignment, and [`rebalance::phase`] recognises an
//! assignment that is ahead of the persisted plan, so a new controller resumes exactly where
//! the old one stopped. At most `max_concurrent_moves` moves are in flight.
//!
//! Triggers (mode `repair` / `full`, never while a plan is active, evaluated at most every
//! [`crate::ClusterConfig::rebalance_interval`] — 30 s by default — and first one interval
//! after this node became controller): a replica on a node down longer than
//! `replace_after_secs`, a partition below the replication factor (capped RF growth), and —
//! `full` only — replica / preferred-leader imbalance above `imbalance_threshold` (e.g. after a
//! node joined and has been up for `rebalance_stable`). Mode `off` never plans by itself.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use fluxmq_meta::{MetaError, MetaStore, MetaStoreExt};
use fluxmq_proto::cluster::{
    MoveReason, MoveState, NodeStatus, PartitionAssignment, PartitionMove,
    PartitionReplicationStatus, RebalanceConfig, RebalanceMode, RebalanceRequest, RebalanceState,
    RebalanceStatus,
};
use fluxmq_proto::now_ms;
use tokio::time::Instant;

use crate::assign::{self, View};
use crate::controller::{self, Snapshot, assignment_key};
use crate::rebalance::{
    self, Assignments, CONFIG_KEY, MovePhase, PLAN_KEY, PlacementView, PlanDoc, PlanOptions,
    PlanOutcome,
};
use crate::rpc::{RebalanceAction, RpcError, RpcErrorKind};
use crate::runtime::Runtime;

/// The HTTP API's per-node replication status route (`fluxmq-api`): lag of new replicas is
/// read from the partition leader.
const REPLICATION_STATUS_PATH: &str = "/internal/v1/replication-status";
const LAG_REFRESH: Duration = Duration::from_secs(2);
const LAG_TIMEOUT: Duration = Duration::from_secs(2);
/// A committed move waits this long for its (live) preferred leader before it counts as done
/// anyway (the controller tick's preferred-leader election finishes the job later).
const FINALIZE_GRACE: Duration = Duration::from_secs(30);
/// CAS rounds per assignment write before the step gives up (retried next step).
const CAS_ATTEMPTS: usize = 5;
/// After a failed plan, automatic triggers wait this many intervals.
const FAILED_BACKOFF_INTERVALS: u32 = 10;

/// Controller-local executor state (reset when leadership is lost).
#[derive(Default)]
pub(crate) struct RebalanceLocal {
    /// Last trigger evaluation (`None` until this node acts as controller).
    last_eval: Option<Instant>,
    /// Registered node → since when it has been seen down.
    down_since: HashMap<String, Instant>,
    /// (channel, partition) → messages the slowest new replica is behind.
    lag: HashMap<(String, u32), u64>,
    /// (plan id, channel, partition) → first lag observed while catching up.
    initial_lag: HashMap<(u64, String, u32), u64>,
    last_lag: Option<Instant>,
    /// (plan id, channel, partition) → when the move reached `Finalizing`.
    finalizing_since: HashMap<(u64, String, u32), Instant>,
    /// Eligible nodes at the last evaluation ("node joined" trigger label).
    last_eligible: BTreeSet<String>,
}

impl RebalanceLocal {
    fn reset(&mut self) {
        *self = Self::default();
    }
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
            rt.rebalance.lock().reset();
            continue;
        }
        if let Err(e) = step(&rt).await {
            tracing::debug!(error = %e, "rebalance step");
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Meta documents
// ---------------------------------------------------------------------------------------------

/// The stored config, or the default with the server's `--auto-rebalance` mode.
pub(crate) fn effective_config(rt: &Runtime) -> RebalanceConfig {
    rt.meta
        .get_as::<RebalanceConfig>(CONFIG_KEY)
        .ok()
        .flatten()
        .unwrap_or_else(|| seed_config(rt))
}

fn seed_config(rt: &Runtime) -> RebalanceConfig {
    RebalanceConfig {
        mode: rt.cfg.auto_rebalance,
        ..RebalanceConfig::default()
    }
}

/// Writes the seeded config once (CAS on absence), so every later controller agrees on it
/// whatever its own `--auto-rebalance` flag says. The API is the source of truth afterwards.
async fn ensure_config(rt: &Runtime) -> Result<RebalanceConfig, MetaError> {
    if let Some(c) = rt.meta.get_as::<RebalanceConfig>(CONFIG_KEY)? {
        return Ok(c);
    }
    let seed = seed_config(rt);
    match controller::cas_put(rt, CONFIG_KEY, &seed, 0).await {
        Ok(_) => {
            tracing::info!(mode = ?seed.mode, "rebalance config seeded");
            Ok(seed)
        }
        Err(MetaError::Conflict { .. }) => Ok(effective_config(rt)),
        Err(e) => Err(e),
    }
}

pub(crate) fn load_plan(rt: &Runtime) -> Option<(u64, PlanDoc)> {
    let v = rt.meta.get(PLAN_KEY)?;
    let plan = serde_json::from_value(v.value).ok()?;
    Some((v.mod_revision, plan))
}

fn assignments(rt: &Runtime) -> Assignments {
    controller::parse_assignments(rt)
        .into_iter()
        .map(|(name, (_, parts))| (name, parts))
        .collect()
}

fn active(state: RebalanceState) -> bool {
    state != RebalanceState::Idle
}

fn in_flight(m: &PartitionMove) -> bool {
    matches!(m.state, MoveState::CatchingUp | MoveState::Finalizing)
}

// ---------------------------------------------------------------------------------------------
// Placement view (controller)
// ---------------------------------------------------------------------------------------------

/// Classifies nodes for planning and updates the down-since bookkeeping. Automatic plans
/// only target nodes up for `rebalance_stable` (`require_stable`); an operator request may
/// target any live `Up` node right away.
fn placement(
    snap: &Snapshot,
    local: &mut RebalanceLocal,
    cfg: &RebalanceConfig,
    assignments: &Assignments,
    now: Instant,
    require_stable: bool,
) -> PlacementView {
    let alive = snap.view.alive.clone();
    let eligible: BTreeSet<String> = alive
        .iter()
        .filter(|n| {
            (!require_stable || snap.stable.contains(*n))
                && !snap.view.leaving.contains(*n)
                && !snap.decommission.contains(*n)
                && snap
                    .records
                    .get(*n)
                    .is_some_and(|r| r.status == NodeStatus::Up)
        })
        .cloned()
        .collect();
    local.down_since.retain(|n, _| !alive.contains(n));
    for id in snap.records.keys().filter(|n| !alive.contains(*n)) {
        local
            .down_since
            .entry(id.clone())
            .or_insert_with(|| snap.last_seen.get(id).copied().unwrap_or(now));
    }
    let mut replace = BTreeSet::new();
    if cfg.replace_after_secs > 0 {
        let after = Duration::from_secs(cfg.replace_after_secs);
        replace.extend(
            local
                .down_since
                .iter()
                .filter(|(_, t)| now.duration_since(**t) >= after)
                .map(|(n, _)| n.clone()),
        );
        // Replicas on ids the registry doesn't know at all.
        replace.extend(
            assignments
                .values()
                .flatten()
                .flat_map(|p| p.replicas.iter())
                .filter(|r| !snap.records.contains_key(*r) && !alive.contains(*r))
                .cloned(),
        );
    }
    PlacementView {
        alive,
        eligible,
        replace,
        decommission: snap.decommission.clone(),
    }
}

// ---------------------------------------------------------------------------------------------
// The step
// ---------------------------------------------------------------------------------------------

async fn step(rt: &Runtime) -> Result<(), MetaError> {
    refresh_lag(rt).await;
    let _serial = rt.controller_lock.lock().await;
    if !rt.is_leader() {
        return Ok(());
    }
    let now = Instant::now();
    let cfg = ensure_config(rt).await?;
    let snap = controller::snapshot(rt);
    let current = assignments(rt);
    let pv = {
        let mut local = rt.rebalance.lock();
        placement(&snap, &mut local, &cfg, &current, now, true)
    };
    // v0.5: decommissions advance first (they may start their own repair plan).
    crate::decommission::step(rt, &snap).await?;
    match load_plan(rt) {
        Some((rev, plan)) if active(plan.state) => drive(rt, rev, plan, &cfg, &snap, &pv).await,
        prev => maybe_trigger(rt, prev, &cfg, &snap, &pv, &current, now).await,
    }
}

async fn maybe_trigger(
    rt: &Runtime,
    prev: Option<(u64, PlanDoc)>,
    cfg: &RebalanceConfig,
    snap: &Snapshot,
    pv: &PlacementView,
    current: &Assignments,
    now: Instant,
) -> Result<(), MetaError> {
    let interval = rt.cfg.rebalance_interval;
    // After a failed plan, balance moves wait out the backoff (no retry loop on the same
    // move) — but repairs of down nodes don't: a plan that failed because its target died
    // (a balance plan towards a node that then crashed) must not hold back replacing that
    // node's replicas for minutes.
    let mut backoff = false;
    let joined: BTreeSet<String> = {
        let mut local = rt.rebalance.lock();
        let Some(last) = local.last_eval else {
            // Just took over: give liveness one interval to settle.
            local.last_eval = Some(now);
            local.last_eligible = pv.eligible.clone();
            return Ok(());
        };
        if now.duration_since(last) < interval || cfg.mode == RebalanceMode::Off {
            return Ok(());
        }
        if let Some((_, p)) = &prev
            && p.outcome == Some(PlanOutcome::Failed)
            && p.finished_at_ms.is_some_and(|f| {
                now_ms().saturating_sub(f)
                    < (interval * FAILED_BACKOFF_INTERVALS).as_millis() as u64
            })
        {
            backoff = true;
        }
        local.last_eval = Some(now);
        let joined = pv
            .eligible
            .difference(&local.last_eligible)
            .cloned()
            .collect();
        local.last_eligible = pv.eligible.clone();
        joined
    };
    let mut moves = rebalance::plan(
        current,
        pv,
        rt.cfg.replication_factor,
        cfg,
        &snap.storage,
        &PlanOptions::auto(cfg.mode),
    );
    if backoff {
        moves.retain(|m| m.reason == MoveReason::Repair);
    }
    if moves.is_empty() {
        return Ok(());
    }
    let repair_nodes = repair_nodes(&moves, pv);
    let trigger = if moves.iter().any(|m| m.reason == MoveReason::Repair) {
        if repair_nodes.is_empty() {
            "replication factor below target".to_string()
        } else {
            let local = rt.rebalance.lock();
            let parts: Vec<String> = repair_nodes
                .iter()
                .map(|n| {
                    let secs = local
                        .down_since
                        .get(n)
                        .map_or(0, |t| now.duration_since(*t).as_secs());
                    format!("node {n} down {secs}s")
                })
                .collect();
            parts.join(", ")
        }
    } else if !joined.is_empty() {
        let names: Vec<&str> = joined.iter().map(String::as_str).collect();
        format!("node {} joined", names.join(", "))
    } else {
        format!(
            "imbalance {} > threshold {}",
            rebalance::imbalance(current, &pv.eligible),
            cfg.imbalance_threshold
        )
    };
    start_plan(rt, prev, moves, trigger, repair_nodes)
        .await
        .map(drop)
}

/// Down nodes some move removes (the repair targets).
fn repair_nodes(moves: &[PartitionMove], pv: &PlacementView) -> Vec<String> {
    let set: BTreeSet<String> = moves
        .iter()
        .flat_map(|m| {
            m.from_replicas
                .iter()
                .filter(|r| !m.to_replicas.contains(r) && pv.replace.contains(*r))
        })
        .cloned()
        .collect();
    set.into_iter().collect()
}

/// Persists a new running plan; returns its id.
async fn start_plan(
    rt: &Runtime,
    prev: Option<(u64, PlanDoc)>,
    moves: Vec<PartitionMove>,
    trigger: String,
    repair_nodes: Vec<String>,
) -> Result<u64, MetaError> {
    let (rev, prev_id) = prev.map_or((0, 0), |(r, p)| (r, p.id));
    let started = now_ms();
    let doc = PlanDoc {
        id: started.max(prev_id + 1),
        state: RebalanceState::Running,
        trigger: Some(trigger.clone()),
        moves,
        started_at_ms: Some(started),
        finished_at_ms: None,
        outcome: None,
        repair_nodes,
    };
    controller::cas_put(rt, PLAN_KEY, &doc, rev).await?;
    tracing::info!(%trigger, moves = doc.moves.len(), "rebalance started");
    Ok(doc.id)
}

/// v0.5 decommission (controller, controller lock held): the repair-only moves that take
/// every replica off the decommissioning nodes (any live `Up` node is a target, stable or
/// not, whatever the rebalance mode). Returns the moves; nothing is persisted.
/// `extra`: also treat this node as decommissioning (dry run before it starts).
pub(crate) fn decommission_moves(
    rt: &Runtime,
    snap: &Snapshot,
    extra: Option<&str>,
) -> (Vec<PartitionMove>, Vec<String>) {
    let cfg = effective_config(rt);
    let current = assignments(rt);
    let mut pv = {
        let mut local = rt.rebalance.lock();
        placement(snap, &mut local, &cfg, &current, Instant::now(), false)
    };
    if let Some(n) = extra {
        pv.eligible.remove(n);
        pv.decommission.insert(n.to_string());
    }
    let opts = PlanOptions {
        repair: true,
        balance: false,
        manual: false,
        channels: Vec::new(),
    };
    let moves = rebalance::plan(
        &current,
        &pv,
        rt.cfg.replication_factor,
        &cfg,
        &snap.storage,
        &opts,
    );
    let repair = repair_nodes(&moves, &pv);
    (moves, repair)
}

/// Starts a decommission plan (`trigger` names the node); `None` when a plan is active.
pub(crate) async fn start_decommission_plan(
    rt: &Runtime,
    moves: Vec<PartitionMove>,
    repair: Vec<String>,
    trigger: String,
) -> Result<Option<u64>, MetaError> {
    let prev = load_plan(rt);
    if prev.as_ref().is_some_and(|(_, p)| active(p.state)) {
        return Ok(None);
    }
    start_plan(rt, prev, moves, trigger, repair).await.map(Some)
}

/// Status with `moves` replaced by a proposed (unpersisted) plan and its projected load.
pub(crate) fn dry_run_status(
    rt: &Runtime,
    moves: Vec<PartitionMove>,
    trigger: String,
) -> RebalanceStatus {
    let current = assignments(rt);
    let storage = if rt.is_leader() {
        controller::snapshot(rt).storage
    } else {
        BTreeMap::new()
    };
    let mut st = status(rt);
    st.projected_load = rebalance::node_load(
        &rebalance::apply(&current, &moves),
        &node_statuses(rt),
        &storage,
    );
    st.moves = moves;
    st.trigger = Some(trigger);
    st
}

/// A plan (other than idle) is in progress.
pub(crate) fn plan_active(rt: &Runtime) -> bool {
    load_plan(rt).is_some_and(|(_, p)| active(p.state))
}

/// Advances an active plan by one step.
async fn drive(
    rt: &Runtime,
    rev: u64,
    mut plan: PlanDoc,
    cfg: &RebalanceConfig,
    snap: &Snapshot,
    pv: &PlacementView,
) -> Result<(), MetaError> {
    let before = plan.clone();
    let now = Instant::now();
    let timeout = Duration::from_secs(cfg.move_timeout_secs);
    for i in 0..plan.moves.len() {
        if in_flight(&plan.moves[i]) {
            let m = &mut plan.moves[i];
            progress(rt, plan.id, m, &snap.view, timeout, now).await?;
            attach_lag(rt, plan.id, m);
        }
    }
    if plan.state == RebalanceState::Running {
        let max = cfg.max_concurrent_moves.max(1) as usize;
        let mut busy = plan.moves.iter().filter(|m| in_flight(m)).count();
        for i in 0..plan.moves.len() {
            if busy >= max {
                break;
            }
            if plan.moves[i].state != MoveState::Pending {
                continue;
            }
            start(rt, &mut plan.moves[i], &snap.view, pv).await?;
            if in_flight(&plan.moves[i]) {
                busy += 1;
            }
        }
    }
    let busy = plan.moves.iter().any(in_flight);
    let pending = plan.moves.iter().any(|m| m.state == MoveState::Pending);
    if plan.state == RebalanceState::Cancelling {
        // Moves that never started are dropped (in-flight ones finish or roll back).
        for m in plan
            .moves
            .iter_mut()
            .filter(|m| m.state == MoveState::Pending)
        {
            m.state = MoveState::Cancelled;
            m.finished_at_ms = Some(now_ms());
        }
    }
    let outcome = match plan.state {
        RebalanceState::Cancelling if !busy => Some(PlanOutcome::Cancelled),
        RebalanceState::Running | RebalanceState::Paused if !busy && !pending => Some(
            if plan.moves.iter().any(|m| m.state == MoveState::RolledBack) {
                PlanOutcome::Failed
            } else {
                PlanOutcome::Completed
            },
        ),
        _ => None,
    };
    if let Some(outcome) = outcome {
        plan.state = RebalanceState::Idle;
        plan.outcome = Some(outcome);
        plan.finished_at_ms = Some(now_ms());
        tracing::info!(?outcome, trigger = ?plan.trigger, "rebalance finished");
    }
    // Lag alone isn't worth a Raft write; it rides along with the next state change.
    let mut cmp = plan.clone();
    for (m, b) in cmp.moves.iter_mut().zip(&before.moves) {
        m.lag = b.lag;
        m.initial_lag = b.initial_lag;
    }
    if cmp != before {
        match controller::cas_put(rt, PLAN_KEY, &plan, rev).await {
            Ok(_) | Err(MetaError::Conflict { .. }) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Live catch-up lag (and the lag at the start of catch-up) of a `CatchingUp` move.
fn attach_lag(rt: &Runtime, plan_id: u64, m: &mut PartitionMove) {
    if m.state != MoveState::CatchingUp {
        return;
    }
    let mut local = rt.rebalance.lock();
    let Some(lag) = local.lag.get(&(m.channel.clone(), m.partition)).copied() else {
        return;
    };
    m.lag = Some(lag);
    let initial = *local
        .initial_lag
        .entry((plan_id, m.channel.clone(), m.partition))
        .or_insert_with(|| m.initial_lag.unwrap_or(lag).max(lag));
    m.initial_lag = Some(initial);
}

fn fail(m: &mut PartitionMove, state: MoveState, error: impl Into<String>) {
    let error = error.into();
    tracing::warn!(channel = %m.channel, partition = m.partition, %error, "partition move failed");
    m.state = state;
    m.error = Some(error);
    m.finished_at_ms = Some(now_ms());
    m.lag = None;
}

fn current_partition(rt: &Runtime, channel: &str, partition: u32) -> Option<PartitionAssignment> {
    rt.meta
        .get_as::<Vec<PartitionAssignment>>(&assignment_key(channel))
        .ok()
        .flatten()?
        .into_iter()
        .find(|p| p.partition == partition)
}

enum WriteErr {
    /// The partition (or channel) no longer exists.
    Gone,
    /// The transition is not possible now (message).
    Refused(String),
    /// Lost every CAS round; retry next step.
    Conflict,
    Meta(MetaError),
}

/// Read-modify-write of one partition's assignment with CAS, retried on conflict. `f` returns
/// the new assignment, or `Err` to refuse.
async fn write_partition(
    rt: &Runtime,
    channel: &str,
    partition: u32,
    f: impl Fn(&PartitionAssignment) -> Result<PartitionAssignment, String>,
) -> Result<PartitionAssignment, WriteErr> {
    let key = assignment_key(channel);
    for _ in 0..CAS_ATTEMPTS {
        let Some(v) = rt.meta.get(&key) else {
            return Err(WriteErr::Gone);
        };
        let mut parts: Vec<PartitionAssignment> =
            serde_json::from_value(v.value).map_err(|e| WriteErr::Meta(e.into()))?;
        let Some(p) = parts.iter_mut().find(|p| p.partition == partition) else {
            return Err(WriteErr::Gone);
        };
        let next = f(p).map_err(WriteErr::Refused)?;
        if next == *p {
            return Ok(next);
        }
        *p = next.clone();
        match controller::cas_put(rt, &key, &parts, v.mod_revision).await {
            Ok(_) => return Ok(next),
            Err(MetaError::Conflict { .. }) => continue,
            Err(e) => return Err(WriteErr::Meta(e)),
        }
    }
    Err(WriteErr::Conflict)
}

/// Starts a pending move (or adopts the progress an earlier controller made).
async fn start(
    rt: &Runtime,
    m: &mut PartitionMove,
    view: &View,
    pv: &PlacementView,
) -> Result<(), MetaError> {
    let Some(p) = current_partition(rt, &m.channel, m.partition) else {
        fail(m, MoveState::RolledBack, "partition no longer exists");
        return Ok(());
    };
    match rebalance::phase(&p, m) {
        MovePhase::Added => {
            m.state = MoveState::CatchingUp;
            m.started_at_ms.get_or_insert_with(now_ms);
            return Ok(());
        }
        MovePhase::Committed => {
            m.state = MoveState::Finalizing;
            m.started_at_ms.get_or_insert_with(now_ms);
            return Ok(());
        }
        MovePhase::Diverged => {
            fail(
                m,
                MoveState::RolledBack,
                "assignment changed since the plan was made; skipped",
            );
            return Ok(());
        }
        MovePhase::Before => {}
    }
    let added: Vec<&String> = m
        .to_replicas
        .iter()
        .filter(|r| !m.from_replicas.contains(r))
        .collect();
    if let Some(bad) = added
        .iter()
        .find(|r| !pv.alive.contains(**r) || view.leaving.contains(**r))
    {
        fail(
            m,
            MoveState::RolledBack,
            format!("target node {bad} is unavailable"),
        );
        return Ok(());
    }
    if !p
        .leader
        .as_ref()
        .is_some_and(|l| view.is_alive(l) && p.replicas.contains(l))
    {
        // No live leader to copy from right now; try again next step.
        return Ok(());
    }
    let to = m.to_replicas.clone();
    let res = if added.is_empty() {
        // Reorder / shrink only: commit directly.
        write_partition(rt, &m.channel, m.partition, |p| {
            rebalance::commit_replicas(p, &to, view)
        })
        .await
        .map(|_| MoveState::Finalizing)
    } else {
        write_partition(rt, &m.channel, m.partition, |p| {
            if p.replicas != m.from_replicas {
                return Err("assignment changed".into());
            }
            Ok(rebalance::add_replicas(p, &to))
        })
        .await
        .map(|_| MoveState::CatchingUp)
    };
    match res {
        Ok(state) => {
            tracing::info!(channel = %m.channel, partition = m.partition, from = ?m.from_replicas, to = ?m.to_replicas, ?state, "partition move started");
            m.state = state;
            m.started_at_ms = Some(now_ms());
            m.error = None;
        }
        Err(WriteErr::Gone) => fail(m, MoveState::RolledBack, "partition no longer exists"),
        Err(WriteErr::Refused(e)) => fail(m, MoveState::RolledBack, e),
        Err(WriteErr::Conflict) => {}
        Err(WriteErr::Meta(e)) => return Err(e),
    }
    Ok(())
}

/// Advances an in-flight move.
async fn progress(
    rt: &Runtime,
    plan_id: u64,
    m: &mut PartitionMove,
    view: &View,
    timeout: Duration,
    now: Instant,
) -> Result<(), MetaError> {
    let Some(p) = current_partition(rt, &m.channel, m.partition) else {
        fail(m, MoveState::RolledBack, "partition no longer exists");
        return Ok(());
    };
    let phase = rebalance::phase(&p, m);
    if m.state == MoveState::CatchingUp {
        match phase {
            MovePhase::Before => {
                // Restored (by a rollback whose plan update was lost, or by an operator).
                let e = m
                    .error
                    .clone()
                    .unwrap_or_else(|| "replica list restored".into());
                fail(m, MoveState::RolledBack, e);
                return Ok(());
            }
            MovePhase::Committed => m.state = MoveState::Finalizing,
            MovePhase::Diverged => {
                fail(
                    m,
                    MoveState::RolledBack,
                    "assignment changed by someone else; move abandoned",
                );
                return Ok(());
            }
            MovePhase::Added => {
                let elapsed = m.started_at_ms.map_or(0, |s| now_ms().saturating_sub(s));
                let to = m.to_replicas.clone();
                if rebalance::caught_up(&p, &m.from_replicas, &to) {
                    match write_partition(rt, &m.channel, m.partition, |p| {
                        rebalance::commit_replicas(p, &to, view)
                    })
                    .await
                    {
                        Ok(next) => {
                            tracing::info!(channel = %m.channel, partition = m.partition, replicas = ?next.replicas, leader = ?next.leader, epoch = next.epoch, "partition move committed");
                            m.state = MoveState::Finalizing;
                            m.lag = Some(0);
                        }
                        Err(WriteErr::Gone) => {
                            fail(m, MoveState::RolledBack, "partition no longer exists");
                            return Ok(());
                        }
                        Err(WriteErr::Refused(_)) | Err(WriteErr::Conflict) => {}
                        Err(WriteErr::Meta(e)) => return Err(e),
                    }
                }
                if m.state == MoveState::CatchingUp && elapsed >= timeout.as_millis() as u64 {
                    let from = m.from_replicas.clone();
                    match write_partition(rt, &m.channel, m.partition, |p| {
                        rebalance::commit_replicas(p, &from, view)
                    })
                    .await
                    {
                        Ok(_) => fail(
                            m,
                            MoveState::RolledBack,
                            format!(
                                "new replicas did not join the ISR within {}s; rolled back",
                                timeout.as_secs()
                            ),
                        ),
                        Err(WriteErr::Gone) => {
                            fail(m, MoveState::RolledBack, "partition no longer exists")
                        }
                        Err(WriteErr::Refused(e)) => {
                            // Nobody of the old set is a live ISR member: keep the union
                            // (never below the old replica set) and retry.
                            m.error = Some(format!("timed out; rollback pending: {e}"));
                        }
                        Err(WriteErr::Conflict) => {}
                        Err(WriteErr::Meta(e)) => return Err(e),
                    }
                }
                if m.state != MoveState::Finalizing {
                    return Ok(());
                }
            }
        }
    }
    // Finalizing: the final list is committed; move leadership to the preferred replica.
    let key = (plan_id, m.channel.clone(), m.partition);
    let since = *rt
        .rebalance
        .lock()
        .finalizing_since
        .entry(key.clone())
        .or_insert(now);
    let Some(p) = current_partition(rt, &m.channel, m.partition) else {
        fail(m, MoveState::RolledBack, "partition no longer exists");
        return Ok(());
    };
    if rebalance::phase(&p, m) != MovePhase::Committed {
        fail(
            m,
            MoveState::RolledBack,
            "assignment changed by someone else after commit",
        );
        rt.rebalance.lock().finalizing_since.remove(&key);
        return Ok(());
    }
    let preferred = m.to_replicas.first().cloned();
    // A preferred replica that is down can't take leadership: waiting the grace out would
    // only hold up the plan (the tick's preferred election hands it back when it returns).
    let preferred_down = preferred.as_ref().is_some_and(|n| !view.is_alive(n));
    let mut done =
        p.leader == preferred || preferred_down || now.duration_since(since) >= FINALIZE_GRACE;
    if !done && let Some(pref) = &preferred {
        let stable = BTreeSet::from([pref.clone()]);
        if assign::preferred_election(&p, view, &stable).is_some() {
            match write_partition(rt, &m.channel, m.partition, |p| {
                Ok(assign::preferred_election(p, view, &stable).unwrap_or_else(|| p.clone()))
            })
            .await
            {
                Ok(next) => done = next.leader == preferred,
                Err(WriteErr::Gone) => {
                    fail(m, MoveState::RolledBack, "partition no longer exists");
                    return Ok(());
                }
                Err(WriteErr::Refused(_)) | Err(WriteErr::Conflict) => {}
                Err(WriteErr::Meta(e)) => return Err(e),
            }
        }
    }
    if done {
        tracing::info!(channel = %m.channel, partition = m.partition, replicas = ?m.to_replicas, "partition moved");
        m.state = MoveState::Done;
        m.finished_at_ms = Some(now_ms());
        m.lag = None;
        rt.rebalance.lock().finalizing_since.remove(&key);
    }
    Ok(())
}

/// Refreshes the catch-up lag of in-flight moves from their partition leaders (no locks held
/// across the network calls).
async fn refresh_lag(rt: &Runtime) {
    {
        let local = rt.rebalance.lock();
        if local.last_lag.is_some_and(|t| t.elapsed() < LAG_REFRESH) {
            return;
        }
    }
    rt.rebalance.lock().last_lag = Some(Instant::now());
    let Some((_, plan)) = load_plan(rt) else {
        return;
    };
    let moving: Vec<&PartitionMove> = plan
        .moves
        .iter()
        .filter(|m| m.state == MoveState::CatchingUp)
        .collect();
    if moving.is_empty() {
        rt.rebalance.lock().lag.clear();
        return;
    }
    let current = assignments(rt);
    let leader_of = |m: &PartitionMove| {
        current
            .get(&m.channel)
            .and_then(|ps| ps.iter().find(|p| p.partition == m.partition))
            .and_then(|p| p.leader.clone())
    };
    let leaders: BTreeSet<String> = moving.iter().filter_map(|m| leader_of(m)).collect();
    let records = controller::records(rt);
    let records = &records;
    let mut statuses: BTreeMap<(String, u32), PartitionReplicationStatus> = BTreeMap::new();
    let calls = leaders.iter().map(|l| async move {
        if l == rt.node_id() {
            return rt.replication.status();
        }
        let Some(addr) = records.get(l).map(|r| r.http_addr.clone()) else {
            return Vec::new();
        };
        rt.rpc
            .get::<_, Vec<PartitionReplicationStatus>>(
                &addr,
                REPLICATION_STATUS_PATH,
                &(),
                LAG_TIMEOUT,
            )
            .await
            .unwrap_or_default()
    });
    for (leader, list) in leaders.iter().zip(futures::future::join_all(calls).await) {
        // A node lists the partitions it follows too (`leader` = the node it follows, no
        // follower lag): keep only the rows it reports as leader, or a follower row could
        // overwrite the leader's and freeze the reported lag.
        for s in list
            .into_iter()
            .filter(|s| s.leader.as_deref() == Some(leader.as_str()))
        {
            statuses.insert((s.channel.clone(), s.partition), s);
        }
    }
    let mut lag = HashMap::new();
    for m in moving {
        let key = (m.channel.clone(), m.partition);
        let Some(s) = statuses.get(&key) else {
            continue;
        };
        let worst = m
            .to_replicas
            .iter()
            .filter(|r| !m.from_replicas.contains(r))
            .map(|r| s.follower_lag.get(r).copied().unwrap_or(s.log_end_offset))
            .max();
        if let Some(w) = worst {
            lag.insert(key, w);
        }
    }
    rt.rebalance.lock().lag = lag;
}

// ---------------------------------------------------------------------------------------------
// Status and operator commands (controller; `execute` holds the controller lock)
// ---------------------------------------------------------------------------------------------

/// Status from replicated state; on the controller also with live lag and node storage.
/// Load lists every registered node (with its status); imbalance counts `Up` nodes only.
pub(crate) fn status(rt: &Runtime) -> RebalanceStatus {
    let cfg = effective_config(rt);
    let plan = load_plan(rt).map(|(_, p)| p);
    let current = assignments(rt);
    let nodes = node_statuses(rt);
    let up: BTreeSet<String> = nodes
        .iter()
        .filter(|(_, s)| **s == NodeStatus::Up)
        .map(|(n, _)| n.clone())
        .collect();
    let alive: BTreeSet<String> = nodes
        .iter()
        .filter(|(_, s)| **s != NodeStatus::Down)
        .map(|(n, _)| n.clone())
        .collect();
    let storage = if rt.is_leader() {
        controller::snapshot(rt).storage
    } else {
        BTreeMap::new()
    };
    let mut moves = plan.as_ref().map(|p| p.moves.clone()).unwrap_or_default();
    if rt.is_leader()
        && let Some(p) = &plan
    {
        for m in moves.iter_mut() {
            attach_lag(rt, p.id, m);
        }
    }
    let remaining: Vec<PartitionMove> = moves
        .iter()
        .filter(|m| m.state == MoveState::Pending || in_flight(m))
        .cloned()
        .collect();
    let projected = rebalance::apply(&current, &remaining);
    RebalanceStatus {
        config: cfg,
        state: plan.as_ref().map_or(RebalanceState::Idle, |p| p.state),
        load: rebalance::node_load(&current, &nodes, &storage),
        imbalance: rebalance::imbalance(&current, &up),
        under_replicated: rebalance::under_replicated(&current, &alive, rt.cfg.replication_factor),
        moves,
        trigger: plan.as_ref().and_then(|p| p.trigger.clone()),
        started_at_ms: plan.as_ref().and_then(|p| p.started_at_ms),
        finished_at_ms: plan.as_ref().and_then(|p| p.finished_at_ms),
        projected_load: rebalance::node_load(&projected, &nodes, &storage),
    }
}

/// Registered nodes with their status (load listings).
fn node_statuses(rt: &Runtime) -> BTreeMap<String, NodeStatus> {
    controller::records(rt)
        .into_values()
        .map(|r| (r.id, r.status))
        .collect()
}

fn invalid(msg: impl Into<String>) -> RpcError {
    RpcError::new(RpcErrorKind::Invalid, msg)
}

fn meta_err(e: MetaError) -> RpcError {
    match e {
        MetaError::Conflict { .. } => RpcError::new(
            RpcErrorKind::Conflict,
            "the rebalance plan changed concurrently; retry",
        ),
        MetaError::Unavailable(m) => RpcError::new(RpcErrorKind::Unavailable, m),
        e => RpcError::new(RpcErrorKind::Internal, e.to_string()),
    }
}

pub(crate) async fn set_config(
    rt: &Runtime,
    cfg: RebalanceConfig,
) -> Result<RebalanceStatus, RpcError> {
    rebalance::validate_config(&cfg).map_err(invalid)?;
    rt.meta.put_as(CONFIG_KEY, &cfg).await.map_err(meta_err)?;
    tracing::info!(?cfg, "rebalance config updated");
    Ok(status(rt))
}

pub(crate) async fn request(
    rt: &Runtime,
    req: RebalanceRequest,
) -> Result<RebalanceStatus, RpcError> {
    let current = assignments(rt);
    if let Some(unknown) = req.channels.iter().find(|c| !current.contains_key(*c)) {
        return Err(RpcError::new(
            RpcErrorKind::NotFound,
            format!("channel '{unknown}' not found"),
        ));
    }
    let prev = load_plan(rt);
    if !req.dry_run
        && let Some((_, p)) = &prev
        && active(p.state)
    {
        return Err(RpcError::new(
            RpcErrorKind::Conflict,
            "a rebalance is already in progress; cancel it or wait for it to finish",
        ));
    }
    let cfg = effective_config(rt);
    let snap = controller::snapshot(rt);
    let pv = {
        let mut local = rt.rebalance.lock();
        placement(&snap, &mut local, &cfg, &current, Instant::now(), false)
    };
    let moves = rebalance::plan(
        &current,
        &pv,
        rt.cfg.replication_factor,
        &cfg,
        &snap.storage,
        &PlanOptions::manual(req.channels.clone()),
    );
    if req.dry_run {
        let mut st = status(rt);
        st.projected_load = rebalance::node_load(
            &rebalance::apply(&current, &moves),
            &node_statuses(rt),
            &snap.storage,
        );
        st.moves = moves;
        st.trigger = Some("manual (dry run)".into());
        return Ok(st);
    }
    if moves.is_empty() {
        let mut st = status(rt);
        st.moves = Vec::new();
        st.trigger = Some("manual (nothing to move)".into());
        return Ok(st);
    }
    let repair = repair_nodes(&moves, &pv);
    start_plan(rt, prev, moves, "manual".into(), repair)
        .await
        .map_err(meta_err)?;
    Ok(status(rt))
}

pub(crate) async fn control(
    rt: &Runtime,
    action: RebalanceAction,
) -> Result<RebalanceStatus, RpcError> {
    let Some((rev, mut plan)) = load_plan(rt).filter(|(_, p)| active(p.state)) else {
        return Err(invalid("no rebalance is in progress"));
    };
    let next = match (action, plan.state) {
        (RebalanceAction::Pause, RebalanceState::Running | RebalanceState::Paused) => {
            RebalanceState::Paused
        }
        (RebalanceAction::Resume, RebalanceState::Running | RebalanceState::Paused) => {
            RebalanceState::Running
        }
        (RebalanceAction::Cancel, _) => RebalanceState::Cancelling,
        (_, RebalanceState::Cancelling) => {
            return Err(invalid("the rebalance is being cancelled"));
        }
        (_, s) => return Err(invalid(format!("cannot {action:?} a plan in state {s:?}"))),
    };
    if next != plan.state {
        plan.state = next;
        controller::cas_put(rt, PLAN_KEY, &plan, rev)
            .await
            .map_err(meta_err)?;
        tracing::info!(?action, "rebalance {action:?}");
    }
    Ok(status(rt))
}

/// Single node: rebalancing doesn't apply (mode off, idle, this node's load).
pub(crate) fn single_node_status(
    node: &str,
    assignments: &Assignments,
    storage: u64,
) -> RebalanceStatus {
    let nodes = BTreeMap::from([(node.to_string(), NodeStatus::Up)]);
    let storage = BTreeMap::from([(node.to_string(), storage)]);
    let load = rebalance::node_load(assignments, &nodes, &storage);
    RebalanceStatus {
        config: RebalanceConfig {
            mode: RebalanceMode::Off,
            ..RebalanceConfig::default()
        },
        state: RebalanceState::Idle,
        projected_load: load.clone(),
        load,
        imbalance: 0,
        under_replicated: 0,
        moves: Vec::new(),
        trigger: None,
        started_at_ms: None,
        finished_at_ms: None,
    }
}

/// Partitions catching up new replicas in the active plan → the configured throttle. Applied
/// by the reconciler on every node (enforced by whichever node leads the partition).
pub(crate) fn throttle_targets(rt: &Runtime) -> HashMap<(String, u32), u64> {
    let mut out = HashMap::new();
    let Some((_, plan)) = load_plan(rt).filter(|(_, p)| active(p.state)) else {
        return out;
    };
    let rate = effective_config(rt).throttle_bytes_per_sec;
    if rate == 0 {
        return out;
    }
    for m in plan
        .moves
        .iter()
        .filter(|m| m.state == MoveState::CatchingUp)
    {
        out.insert((m.channel.clone(), m.partition), rate);
    }
    out
}
