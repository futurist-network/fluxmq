//! Node decommission and forget (v0.5). Contract: the decommission block of
//! `fluxmq_proto::cluster` (read it first).
//!
//! State lives in meta at `cluster/decommission/<id>` ([`DecommissionDoc`]), so any controller
//! resumes where its predecessor stopped. Everything runs on the controller under the
//! controller lock: [`start`] / [`forget`] (operator commands) and [`step`] (every rebalance
//! step).
//!
//! Decommission (`MovingReplicas → LeavingMembership → Removed`, or `Failed`):
//! 1. **Start** (refused with `Conflict` while a rebalance plan runs, while another node is
//!    being decommissioned, when removing the node would break the Raft quorum, or when the
//!    other `Up` nodes can't hold a partition's replicas): the doc is written, the registry
//!    status becomes `Decommissioning` (kept by the liveness tick, whatever the incarnation),
//!    its leaderships are drained, and a repair-only plan is started through the normal
//!    rebalance executor (trigger `decommission <id>`).
//! 2. **Moving replicas**: the planner never targets a decommissioning node and plans a
//!    `Repair` move for every replica it holds ([`crate::rebalance::PlacementView`]), in every
//!    plan — automatic, manual or the decommission's own. Whenever no plan runs and replicas
//!    remain, the next repair plan starts (at most [`MAX_PLANS`]; no possible move for
//!    [`STALL_TIMEOUT`] → `Failed`; a cancelled decommission plan → `Failed`). A failed
//!    decommission leaves the node in the cluster (status back to `Up`); `POST` again retries.
//! 3. **Leaving membership** (0 replicas): quorum re-checked (waits, with the reason in
//!    `error`, while unsafe), then openraft `change_membership` removes the node — a voter
//!    with `RemoveVoters` (not retained as learner), a learner with `RemoveNodes`.
//! 4. **Removed**: `cluster/nodes/<id>` is deleted. Heartbeats from the removed process (any
//!    incarnation up to the recorded one) are refused with [`RpcErrorKind::Removed`]; the
//!    process then stops its cluster roles ([`crate::runtime`]). A restart (newer incarnation)
//!    is accepted as a brand-new node: registered, added as a learner, promoted like any
//!    joining node; the doc is deleted once the new incarnation is registered.
//!
//! **Removing the controller.** openraft 0.9 has no leadership transfer, and a triggered
//! election on another voter can't win while the leader lease is valid. So the controller
//! decommissions itself like any node — its replicas and partition leaderships move away
//! first while it keeps acting as controller — and in step 3 it removes *itself* with
//! `change_membership`. openraft keeps such a leader in charge until the membership without it
//! is committed and applied (so the change completes and the followers learn the commit), then
//! steps it down to a non-voter. The remaining voters elect a new controller after the
//! election timeout (1–2 s); it finds the doc in `LeavingMembership` with the node already out
//! of the membership and finishes step 4. During the hand-over no partition is affected
//! (the node leads none) and node leases (`node_timeout * 2/3`) outlast an election, so data
//! traffic continues; controller commands retry across the election as usual.
//!
//! Records stay readable for [`RETENTION`] after they end (`GET /api/cluster/decommissions`),
//! then [`step`] deletes them. The refusal of a removed process lives in its own key
//! (`cluster/removed/<id>` → incarnation), kept until a newer incarnation registers, so it
//! outlives the record. Cancel = `POST /api/cluster/rebalance/cancel` while moving replicas:
//! in-flight moves finish, the decommission ends `failed` with error `cancelled`, and the node
//! is `Up` again (moved replicas stay moved) — like after any failure. Start and end are
//! published on [`Runtime::decommission_events`] (the server writes `node.decommission`
//! audit entries on the node that observed them).
//!
//! Forget (`DELETE /api/cluster/nodes/{id}`): only a `Down` node holding no replicas (the dead
//! machine after repair). The doc is written in `LeavingMembership` and steps 3–4 run at once
//! (retried by [`step`] if the membership change can't be made right now).
//!
//! Quorum safety (both commands, re-checked before the membership change): removing a learner
//! is always safe; removing a voter needs ≥ 1 remaining voter, and a majority of live voters
//! both in the remaining set and in the current set (joint consensus needs both).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use fluxmq_meta::{MetaError, MetaStore, MetaStoreExt, Precondition};
use fluxmq_proto::cluster::{DecommissionState, DecommissionStatus, NodeStatus};
use fluxmq_proto::now_ms;
use openraft::ChangeMembers;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::controller::{self, Snapshot, meta_err, node_key, parse_assignments, records};
use crate::raft::raft_id;
use crate::rebalance::PlanOutcome;
use crate::rpc::{RpcError, RpcErrorKind};
use crate::runtime::Runtime;

/// Meta prefix of the decommission documents (`cluster/decommission/<node id>`).
pub const DECOMMISSION_PREFIX: &str = "cluster/decommission/";
/// Meta prefix of refused (removed) processes: `cluster/removed/<node id>` → incarnation.
pub const REMOVED_PREFIX: &str = "cluster/removed/";
/// Finished records stay readable this long.
pub const RETENTION: Duration = Duration::from_secs(24 * 3600);
/// Repair plans a decommission may start before it gives up.
const MAX_PLANS: u32 = 5;
/// A decommission that can't plan a single move off the node for this long fails.
const STALL_TIMEOUT: Duration = Duration::from_secs(120);
const MEMBERSHIP_TIMEOUT: Duration = Duration::from_secs(10);

pub fn doc_key(node: &str) -> String {
    format!("{DECOMMISSION_PREFIX}{node}")
}

fn removed_key(node: &str) -> String {
    format!("{REMOVED_PREFIX}{node}")
}

/// The persisted decommission state of one node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DecommissionDoc {
    pub status: DecommissionStatus,
    /// Newest incarnation of the node's process seen while decommissioning; once `Removed`,
    /// heartbeats of this or an older incarnation are refused (a restart is newer).
    pub incarnation: u64,
    /// Repair plans started for this decommission.
    #[serde(default)]
    pub plans: u32,
    /// The plan currently (or last) moving its replicas.
    #[serde(default)]
    pub plan_id: Option<u64>,
    /// Started by `DELETE` (forget) rather than a decommission.
    #[serde(default)]
    pub forget: bool,
}

fn docs(rt: &Runtime) -> BTreeMap<String, (u64, DecommissionDoc)> {
    rt.meta
        .list(DECOMMISSION_PREFIX)
        .into_iter()
        .filter_map(|(k, v)| {
            let node = k.strip_prefix(DECOMMISSION_PREFIX)?.to_string();
            let doc = serde_json::from_value(v.value).ok()?;
            Some((node, (v.mod_revision, doc)))
        })
        .collect()
}

fn in_progress(state: DecommissionState) -> bool {
    matches!(
        state,
        DecommissionState::MovingReplicas | DecommissionState::LeavingMembership
    )
}

/// Nodes being decommissioned (moving replicas or leaving the membership).
pub(crate) fn active(rt: &Runtime) -> BTreeSet<String> {
    docs(rt)
        .into_iter()
        .filter(|(_, (_, d))| in_progress(d.status.state))
        .map(|(n, _)| n)
        .collect()
}

/// Removed nodes → newest incarnation whose heartbeats are refused.
pub(crate) fn refused(rt: &Runtime) -> HashMap<String, u64> {
    rt.meta
        .list(REMOVED_PREFIX)
        .into_iter()
        .filter_map(|(k, v)| {
            let node = k.strip_prefix(REMOVED_PREFIX)?.to_string();
            Some((node, v.value.as_u64()?))
        })
        .collect()
}

/// The newest refused incarnation of `node`, if it was removed.
pub(crate) fn refused_incarnation(rt: &Runtime, node: &str) -> Option<u64> {
    rt.meta.get(&removed_key(node))?.value.as_u64()
}

/// `GET /api/cluster/decommissions`: current and recent (≤ [`RETENTION`]) decommissions,
/// newest first.
pub(crate) fn list(rt: &Runtime) -> Vec<DecommissionStatus> {
    let mut out: Vec<DecommissionStatus> = docs(rt)
        .into_keys()
        .filter_map(|n| status(rt, &n))
        .collect();
    out.sort_by_key(|d| std::cmp::Reverse(d.started_at_ms));
    out
}

/// Every record with its origin (`true` = forget), for the events feed: any node derives the
/// decommission lifecycle events from the replicated docs, like the rebalance events.
pub(crate) fn records_with_origin(rt: &Runtime) -> Vec<(DecommissionStatus, bool)> {
    docs(rt)
        .into_iter()
        .map(|(n, (_, d))| {
            let forget = d.forget;
            (status(rt, &n).unwrap_or(d.status), forget)
        })
        .collect()
}

fn emit(rt: &Runtime, st: &DecommissionStatus) {
    let _ = rt.decommission_events.send(st.clone());
}

fn replicas_on(rt: &Runtime, node: &str) -> u32 {
    parse_assignments(rt)
        .values()
        .flat_map(|(_, parts)| parts.iter())
        .filter(|p| p.replicas.iter().any(|r| r == node))
        .count() as u32
}

/// `GET /api/cluster/nodes/{id}/decommission` from the replicated state (any node); the
/// replica count is live while the decommission runs.
pub(crate) fn status(rt: &Runtime, node: &str) -> Option<DecommissionStatus> {
    let doc: DecommissionDoc = rt.meta.get_as(&doc_key(node)).ok().flatten()?;
    let mut st = doc.status;
    if in_progress(st.state) {
        st.replicas_remaining = replicas_on(rt, node);
    }
    Some(st)
}

fn conflict(msg: impl Into<String>) -> RpcError {
    RpcError::new(RpcErrorKind::Conflict, msg)
}

fn trigger(node: &str) -> String {
    format!("decommission {node}")
}

async fn save(rt: &Runtime, node: &str, rev: u64, doc: &DecommissionDoc) -> Result<u64, MetaError> {
    controller::cas_put(rt, &doc_key(node), doc, rev).await
}

/// Refuses removals that would leave the metadata Raft group without a safe quorum (see the
/// module docs). `alive` = the controller's liveness view.
fn quorum_check(rt: &Runtime, node: &str, alive: &BTreeSet<String>) -> Result<(), String> {
    let metrics = rt.raft.metrics().borrow().clone();
    let membership = metrics.membership_config.membership();
    let id = raft_id(node);
    let voters: BTreeSet<u64> = membership.voter_ids().collect();
    if !voters.contains(&id) {
        return Ok(());
    }
    let live = |v: &u64| {
        membership
            .get_node(v)
            .is_some_and(|n| alive.contains(&n.name))
    };
    let remaining: Vec<u64> = voters.iter().filter(|v| **v != id).copied().collect();
    if remaining.is_empty() {
        return Err(format!(
            "'{node}' is the only Raft voter; removing it would leave the cluster without voters"
        ));
    }
    let need = remaining.len() / 2 + 1;
    let live_remaining = remaining.iter().filter(|v| live(v)).count();
    let live_all = voters.iter().filter(|v| live(v)).count();
    if live_remaining < need || live_all < voters.len() / 2 + 1 {
        return Err(format!(
            "removing '{node}' would leave {live_remaining} of {} remaining Raft voters live; a \
             quorum needs {need}",
            remaining.len()
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Operator commands (controller, controller lock held)
// ---------------------------------------------------------------------------------------------

/// `POST /api/cluster/nodes/{id}/decommission`. Idempotent while in progress (returns the
/// current status).
pub(crate) async fn start(rt: &Runtime, node: &str) -> Result<DecommissionStatus, RpcError> {
    let all = docs(rt);
    if let Some((_, d)) = all.get(node)
        && in_progress(d.status.state)
    {
        return Ok(status(rt, node).unwrap_or_else(|| d.status.clone()));
    }
    let (rec, held) = precheck(rt, node, &all)?;
    let mut doc = DecommissionDoc {
        status: DecommissionStatus {
            node: node.to_string(),
            state: DecommissionState::MovingReplicas,
            replicas_remaining: held,
            initial_replicas: held,
            started_at_ms: now_ms(),
            finished_at_ms: None,
            error: None,
        },
        incarnation: rec.incarnation,
        plans: 0,
        plan_id: None,
        forget: false,
    };
    let prev = all.get(node).map_or(0, |(r, _)| *r);
    let rev = save(rt, node, prev, &doc).await.map_err(meta_err)?;
    let mut marked = rec.clone();
    marked.status = NodeStatus::Decommissioning;
    rt.meta
        .put_as(&node_key(node), &marked)
        .await
        .map_err(meta_err)?;
    let (_, drained) = controller::drain_leaderships(rt, node).await?;
    tracing::info!(%node, replicas = held, drained, "decommission started");
    rt.controller.lock().decommission_stalled.remove(node);
    emit(rt, &doc.status);
    if held > 0 {
        let snap = controller::snapshot(rt);
        let (moves, repair) = crate::rebalancer::decommission_moves(rt, &snap, None);
        if !moves.is_empty()
            && let Some(id) =
                crate::rebalancer::start_decommission_plan(rt, moves, repair, trigger(node))
                    .await
                    .map_err(meta_err)?
        {
            doc.plans = 1;
            doc.plan_id = Some(id);
            save(rt, node, rev, &doc).await.map_err(meta_err)?;
        }
    }
    Ok(status(rt, node).unwrap_or(doc.status))
}

/// `POST /api/cluster/nodes/{id}/decommission?dry_run=true`: the same checks, and the plan a
/// decommission would start (nothing is written).
pub(crate) fn dry_run(
    rt: &Runtime,
    node: &str,
) -> Result<fluxmq_proto::cluster::RebalanceStatus, RpcError> {
    let all = docs(rt);
    if !all
        .get(node)
        .is_some_and(|(_, d)| in_progress(d.status.state))
    {
        precheck(rt, node, &all)?;
    }
    let snap = controller::snapshot(rt);
    let (moves, _) = crate::rebalancer::decommission_moves(rt, &snap, Some(node));
    Ok(crate::rebalancer::dry_run_status(
        rt,
        moves,
        format!("{} (dry run)", trigger(node)),
    ))
}

/// The start checks (see the module docs): the node's record and its replica count.
fn precheck(
    rt: &Runtime,
    node: &str,
    all: &BTreeMap<String, (u64, DecommissionDoc)>,
) -> Result<(crate::rpc::NodeRecord, u32), RpcError> {
    let recs = records(rt);
    let Some(rec) = recs.get(node) else {
        return Err(RpcError::new(
            RpcErrorKind::NotFound,
            format!("unknown node '{node}'"),
        ));
    };
    if let Some((other, _)) = all
        .iter()
        .find(|(n, (_, d))| *n != node && in_progress(d.status.state))
    {
        return Err(conflict(format!(
            "node '{other}' is being decommissioned; wait for it to finish"
        )));
    }
    if crate::rebalancer::plan_active(rt) {
        return Err(conflict(
            "a rebalance plan is in progress; cancel it or wait for it to finish",
        ));
    }
    let (view, _) = controller::current_view(rt);
    quorum_check(rt, node, &view.alive).map_err(conflict)?;
    // Every partition with a replica here must keep its replica count on the other Up nodes,
    // and must have a live leader to copy from.
    let others = recs
        .values()
        .filter(|r| {
            r.id != node
                && r.status == NodeStatus::Up
                && view.is_alive(&r.id)
                && !view.leaving.contains(&r.id)
        })
        .count();
    let mut held = 0u32;
    for (channel, (_, parts)) in parse_assignments(rt) {
        for p in parts
            .iter()
            .filter(|p| p.replicas.iter().any(|r| r == node))
        {
            held += 1;
            let need = p.replicas.len();
            if others < need {
                return Err(conflict(format!(
                    "channel '{channel}' partition {} keeps {need} replicas but only {others} \
                     other node(s) are up; add a node first",
                    p.partition
                )));
            }
            let live_leader = p
                .leader
                .as_ref()
                .is_some_and(|l| view.is_alive(l) && p.replicas.contains(l));
            if !live_leader {
                return Err(conflict(format!(
                    "channel '{channel}' partition {} has no live leader, so its data can't be \
                     copied off '{node}' right now; retry later",
                    p.partition
                )));
            }
        }
    }
    Ok((rec.clone(), held))
}

/// `DELETE /api/cluster/nodes/{id}`: forgets a `Down` node that holds no replicas.
pub(crate) async fn forget(rt: &Runtime, node: &str) -> Result<DecommissionStatus, RpcError> {
    if node == rt.node_id() {
        return Err(conflict(format!(
            "'{node}' is the controller and is up; decommission it instead"
        )));
    }
    let all = docs(rt);
    match all.get(node).map(|(_, d)| d.status.state) {
        Some(DecommissionState::MovingReplicas) => {
            return Err(conflict(format!(
                "'{node}' is being decommissioned; wait for it to finish"
            )));
        }
        Some(DecommissionState::LeavingMembership) => {
            return Ok(status(rt, node).expect("doc exists"));
        }
        _ => {}
    }
    let recs = records(rt);
    let rec = recs.get(node);
    let in_membership = {
        let m = rt.raft.metrics().borrow().clone();
        m.membership_config
            .membership()
            .get_node(&raft_id(node))
            .is_some_and(|n| n.name == node)
    };
    if rec.is_none() && !in_membership {
        return Err(RpcError::new(
            RpcErrorKind::NotFound,
            format!("unknown node '{node}'"),
        ));
    }
    let (view, _) = controller::current_view(rt);
    if let Some(rec) = rec
        && (rec.status != NodeStatus::Down || view.is_alive(node))
    {
        return Err(conflict(format!(
            "'{node}' is {}; only a down node can be forgotten (decommission a live node)",
            serde_json::to_value(rec.status)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_default()
        )));
    }
    let held = replicas_on(rt, node);
    if held > 0 {
        return Err(conflict(format!(
            "'{node}' still holds {held} replica(s); wait for the repair (replace_after_secs) \
             or a manual rebalance to move them first"
        )));
    }
    quorum_check(rt, node, &view.alive).map_err(conflict)?;
    let doc = DecommissionDoc {
        status: DecommissionStatus {
            node: node.to_string(),
            state: DecommissionState::LeavingMembership,
            replicas_remaining: 0,
            initial_replicas: 0,
            started_at_ms: now_ms(),
            finished_at_ms: None,
            error: None,
        },
        incarnation: rec.map_or(0, |r| r.incarnation),
        plans: 0,
        plan_id: None,
        forget: true,
    };
    let prev = all.get(node).map_or(0, |(r, _)| *r);
    let rev = save(rt, node, prev, &doc).await.map_err(meta_err)?;
    tracing::info!(%node, "forgetting node");
    emit(rt, &doc.status);
    let snap = controller::snapshot(rt);
    leave_membership(rt, &snap, node, rev, doc)
        .await
        .map_err(meta_err)?;
    Ok(status(rt, node).expect("doc exists"))
}

// ---------------------------------------------------------------------------------------------
// The step (controller, controller lock held; called from the rebalance step)
// ---------------------------------------------------------------------------------------------

pub(crate) async fn step(rt: &Runtime, snap: &Snapshot) -> Result<(), MetaError> {
    for (node, (rev, doc)) in docs(rt) {
        match doc.status.state {
            DecommissionState::MovingReplicas => moving(rt, snap, &node, rev, doc).await?,
            DecommissionState::LeavingMembership => {
                leave_membership(rt, snap, &node, rev, doc).await?
            }
            DecommissionState::Removed => {
                finish_removal(rt, &node, &doc).await?;
                expire(rt, &node, rev, &doc).await?
            }
            DecommissionState::Failed => expire(rt, &node, rev, &doc).await?,
        }
    }
    for (node, inc) in refused(rt) {
        rejoined(rt, &node, inc).await?;
    }
    Ok(())
}

/// Step 4 is several writes; a controller that stepped down halfway (it removed itself)
/// leaves the rest to the next one: the refusal and the registry entry of the removed
/// process are (re)applied idempotently.
async fn finish_removal(rt: &Runtime, node: &str, doc: &DecommissionDoc) -> Result<(), MetaError> {
    if refused_incarnation(rt, node).is_none_or(|inc| inc < doc.incarnation)
        && records(rt)
            .get(node)
            .is_none_or(|r| r.incarnation <= doc.incarnation)
    {
        rt.meta
            .put(&removed_key(node), serde_json::json!(doc.incarnation), None)
            .await?;
    }
    if records(rt)
        .get(node)
        .is_some_and(|r| r.incarnation <= doc.incarnation)
    {
        rt.meta.delete(&node_key(node), None).await?;
        rt.controller.lock().remove_node(node);
        tracing::info!(%node, "removed node's registry entry deleted");
    }
    Ok(())
}

/// Deletes a finished record older than [`RETENTION`].
async fn expire(
    rt: &Runtime,
    node: &str,
    rev: u64,
    doc: &DecommissionDoc,
) -> Result<(), MetaError> {
    let Some(done) = doc.status.finished_at_ms else {
        return Ok(());
    };
    if now_ms().saturating_sub(done) < RETENTION.as_millis() as u64 {
        return Ok(());
    }
    delete_if(rt, &doc_key(node), rev).await
}

async fn delete_if(rt: &Runtime, key: &str, rev: u64) -> Result<(), MetaError> {
    let pre = Precondition {
        key: key.to_string(),
        expected_mod_revision: rev,
    };
    match rt.meta.delete(key, Some(pre)).await {
        Ok(_) | Err(MetaError::Conflict { .. }) => Ok(()),
        Err(e) => Err(e),
    }
}

async fn fail(
    rt: &Runtime,
    node: &str,
    rev: u64,
    mut doc: DecommissionDoc,
    error: String,
) -> Result<(), MetaError> {
    tracing::warn!(%node, %error, "decommission failed");
    doc.status.state = DecommissionState::Failed;
    doc.status.replicas_remaining = replicas_on(rt, node);
    doc.status.error = Some(error);
    doc.status.finished_at_ms = Some(now_ms());
    rt.controller.lock().decommission_stalled.remove(node);
    save(rt, node, rev, &doc).await?;
    emit(rt, &doc.status);
    Ok(())
}

async fn moving(
    rt: &Runtime,
    snap: &Snapshot,
    node: &str,
    rev: u64,
    mut doc: DecommissionDoc,
) -> Result<(), MetaError> {
    let plan = crate::rebalancer::load_plan(rt).map(|(_, p)| p);
    if crate::rebalancer::plan_active(rt) {
        return Ok(());
    }
    let remaining = replicas_on(rt, node);
    if let Some(r) = snap.records.get(node) {
        doc.incarnation = doc.incarnation.max(r.incarnation);
    }
    if remaining == 0 {
        doc.status.state = DecommissionState::LeavingMembership;
        doc.status.replicas_remaining = 0;
        doc.status.error = None;
        rt.controller.lock().decommission_stalled.remove(node);
        save(rt, node, rev, &doc).await?;
        tracing::info!(%node, "decommission: replicas moved; leaving the raft membership");
        return Ok(());
    }
    if let (Some(id), Some(p)) = (doc.plan_id, &plan)
        && p.id == id
        && p.outcome == Some(PlanOutcome::Cancelled)
    {
        return fail(rt, node, rev, doc, "cancelled".to_string()).await;
    }
    if doc.plans >= MAX_PLANS {
        let e = format!(
            "{remaining} replica(s) could not be moved off '{node}' after {MAX_PLANS} plans (see \
             the rebalance moves for the errors)"
        );
        return fail(rt, node, rev, doc, e).await;
    }
    let (moves, repair) = crate::rebalancer::decommission_moves(rt, snap, None);
    let useful = moves.iter().any(|m| {
        m.from_replicas.iter().any(|r| r == node) && !m.to_replicas.iter().any(|r| r == node)
    });
    if !useful {
        let since = *rt
            .controller
            .lock()
            .decommission_stalled
            .entry(node.to_string())
            .or_insert_with(Instant::now);
        if since.elapsed() >= STALL_TIMEOUT {
            let e = format!(
                "{remaining} replica(s) can't be moved off '{node}': no other up node can take \
                 them, or their partitions have no live leader"
            );
            return fail(rt, node, rev, doc, e).await;
        }
        let msg = format!(
            "waiting: {remaining} replica(s) can't be moved right now (no eligible node or no \
             live leader)"
        );
        if doc.status.error.as_deref() != Some(msg.as_str()) {
            doc.status.error = Some(msg);
            save(rt, node, rev, &doc).await?;
        }
        return Ok(());
    }
    rt.controller.lock().decommission_stalled.remove(node);
    if let Some(id) =
        crate::rebalancer::start_decommission_plan(rt, moves, repair, trigger(node)).await?
    {
        doc.plans += 1;
        doc.plan_id = Some(id);
        doc.status.error = None;
        save(rt, node, rev, &doc).await?;
    }
    Ok(())
}

async fn leave_membership(
    rt: &Runtime,
    snap: &Snapshot,
    node: &str,
    rev: u64,
    mut doc: DecommissionDoc,
) -> Result<(), MetaError> {
    let membership = {
        let m = rt.raft.metrics().borrow().clone();
        m.membership_config.membership().clone()
    };
    if membership.get_joint_config().len() > 1 {
        return Ok(()); // a membership change is in flight
    }
    let id = raft_id(node);
    let member = membership.get_node(&id).is_some_and(|n| n.name == node);
    if member {
        if let Err(e) = quorum_check(rt, node, &snap.view.alive) {
            let msg = format!("waiting: {e}");
            if doc.status.error.as_deref() != Some(msg.as_str()) {
                doc.status.error = Some(msg);
                save(rt, node, rev, &doc).await?;
            }
            return Ok(());
        }
        let voter = membership.voter_ids().any(|v| v == id);
        let change = if voter {
            ChangeMembers::RemoveVoters(BTreeSet::from([id]))
        } else {
            ChangeMembers::RemoveNodes(BTreeSet::from([id]))
        };
        let me = id == rt.raft_id;
        tracing::info!(%node, voter, controller = me, "removing node from the raft membership");
        match tokio::time::timeout(MEMBERSHIP_TIMEOUT, rt.raft.change_membership(change, false))
            .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                tracing::warn!(%node, error = %e, "raft membership removal failed; retrying");
                return Ok(());
            }
            Err(_) => {
                tracing::warn!(%node, "raft membership removal timed out; retrying");
                return Ok(());
            }
        }
        if me {
            // openraft steps this leader down once the membership without it is committed and
            // applied; the next controller deletes the registry entry (module docs).
            tracing::info!(%node, "controller removed itself from the raft membership; stepping down");
            return Ok(());
        }
    }
    // Out of the membership. Refuse the process first (its heartbeats must not re-register
    // it), then delete the registry entry.
    if let Some(r) = records(rt).get(node) {
        doc.incarnation = doc.incarnation.max(r.incarnation);
    }
    doc.status.state = DecommissionState::Removed;
    doc.status.replicas_remaining = 0;
    doc.status.error = None;
    doc.status.finished_at_ms = Some(now_ms());
    rt.meta
        .put(&removed_key(node), serde_json::json!(doc.incarnation), None)
        .await?;
    save(rt, node, rev, &doc).await?;
    rt.meta.delete(&node_key(node), None).await?;
    rt.controller.lock().remove_node(node);
    tracing::info!(%node, forget = doc.forget, "node removed from the cluster");
    emit(rt, &doc.status);
    Ok(())
}

/// A removed node that restarted and registered with a newer incarnation joined as a new
/// node: its refusal has served its purpose (the record stays for its retention).
async fn rejoined(rt: &Runtime, node: &str, refused: u64) -> Result<(), MetaError> {
    let Some(rec) = records(rt).remove(node) else {
        return Ok(());
    };
    if rec.incarnation <= refused {
        return Ok(());
    }
    let key = removed_key(node);
    let Some(v) = rt.meta.get(&key) else {
        return Ok(());
    };
    delete_if(rt, &key, v.mod_revision).await?;
    tracing::info!(%node, "removed node restarted and joined as a new node");
    Ok(())
}
