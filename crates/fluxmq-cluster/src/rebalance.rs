//! Automatic rebalancing (v0.4): the pure **planner** plus the pure assignment transitions the
//! executor (`rebalancer`) applies. Contract: the v0.4 block of `fluxmq_proto::cluster`.
//!
//! No I/O; every function maps (assignments, placement view, config, storage) → moves or a
//! new assignment, deterministically (stable ordering everywhere: `BTreeMap`s, ties broken by
//! node id), so a dry run on any node and the executor on the controller agree.
//!
//! Planning happens on a working copy of every partition's replica list:
//!
//! 1. **Repair** — replicas on nodes in [`PlacementView::replace`] (down longer than
//!    `replace_after_secs`) and on nodes in [`PlacementView::decommission`] (v0.5: alive or
//!    not, every replica goes) are replaced by the least-loaded eligible node; partitions with
//!    fewer replicas than the replication factor (RF capped at creation) grow to it. A
//!    replica is only removed when a replacement exists, so repair never lowers durability.
//! 2. **Balance replicas** — while the most and least loaded eligible nodes differ by ≥ 2
//!    replicas, one replica moves from the former to the latter (sticky: partitions that move
//!    anyway are preferred, then partitions where the source is not the preferred leader).
//!    Runs only when `max - min` exceeds `imbalance_threshold` (or for a manual request).
//! 3. **Balance preferred leaders** — the same greedy over replica 0, by reordering a
//!    partition's replicas (no data movement).
//!
//! Every greedy step strictly lowers the sum of squared loads, so planning terminates, and
//! because the triggers are evaluated on the post-repair loads, planning again on the result
//! of a plan yields no moves (tested below with randomized cases).
//!
//! Partitions without a live leader are never moved (nothing to copy from), and a partition
//! never gets two replicas on one node.

use std::collections::{BTreeMap, BTreeSet};

use fluxmq_proto::cluster::{
    MoveReason, MoveState, NodeLoad, NodeStatus, PartitionAssignment, PartitionMove,
    RebalanceConfig,
};
use serde::{Deserialize, Serialize};

use crate::assign::View;

/// Assignments of every channel (`channel → partitions`).
pub type Assignments = BTreeMap<String, Vec<PartitionAssignment>>;

/// Node classification for planning.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlacementView {
    /// Alive nodes (heartbeating): their replicas are healthy, whatever their status.
    pub alive: BTreeSet<String>,
    /// Nodes that may receive replicas: alive, `Up`, not leaving/draining, stable.
    pub eligible: BTreeSet<String>,
    /// Nodes whose replicas are to be replaced (down longer than `replace_after_secs`).
    pub replace: BTreeSet<String>,
    /// v0.5: nodes being decommissioned. Never targets (callers keep them out of `eligible`);
    /// every replica they hold gets a `Repair` move whenever an eligible node can take it,
    /// whether they are alive or not.
    pub decommission: BTreeSet<String>,
}

impl PlacementView {
    /// The replica on `node` is to be replaced (long-down or decommissioning).
    fn evict(&self, node: &str) -> bool {
        (self.replace.contains(node) && !self.alive.contains(node))
            || self.decommission.contains(node)
    }

    /// The replica on `node` doesn't count towards the replication factor.
    fn leaving(&self, node: &str) -> bool {
        self.replace.contains(node) || self.decommission.contains(node)
    }
}

/// What a planning run may do.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanOptions {
    pub repair: bool,
    pub balance: bool,
    /// Operator request: balance regardless of `imbalance_threshold`; non-repair moves get
    /// [`MoveReason::Manual`].
    pub manual: bool,
    /// Only these channels may move (empty = all). Loads always count every channel.
    pub channels: Vec<String>,
}

impl PlanOptions {
    pub fn auto(mode: fluxmq_proto::cluster::RebalanceMode) -> Self {
        use fluxmq_proto::cluster::RebalanceMode;
        Self {
            repair: mode != RebalanceMode::Off,
            balance: mode == RebalanceMode::Full,
            manual: false,
            channels: Vec::new(),
        }
    }

    pub fn manual(channels: Vec<String>) -> Self {
        Self {
            repair: true,
            balance: true,
            manual: true,
            channels,
        }
    }
}

/// How a finished plan ended (persisted with the plan; drives the completion events).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanOutcome {
    Completed,
    /// At least one move was rolled back or failed.
    Failed,
    Cancelled,
}

/// The persisted plan at [`PLAN_KEY`]. Written only by the controller (CAS on the key), read
/// by every node (status, events feed).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDoc {
    /// Unique per plan (controller clock, ms; strictly increasing per key).
    pub id: u64,
    pub state: fluxmq_proto::cluster::RebalanceState,
    pub trigger: Option<String>,
    pub moves: Vec<PartitionMove>,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    #[serde(default)]
    pub outcome: Option<PlanOutcome>,
    /// Down nodes whose replicas this plan replaces (`ReplicaRepairStarted` events).
    #[serde(default)]
    pub repair_nodes: Vec<String>,
}

/// Meta key of the current (or last) plan.
pub const PLAN_KEY: &str = "cluster/rebalance/plan";
/// Meta key of the [`RebalanceConfig`].
pub const CONFIG_KEY: &str = "cluster/rebalance/config";

/// Rejects nonsensical settings (API `PUT`).
pub fn validate_config(c: &RebalanceConfig) -> Result<(), String> {
    if c.max_concurrent_moves == 0 {
        return Err("max_concurrent_moves must be >= 1".into());
    }
    if c.max_concurrent_moves > 1000 {
        return Err("max_concurrent_moves must be <= 1000".into());
    }
    if c.move_timeout_secs == 0 {
        return Err("move_timeout_secs must be >= 1".into());
    }
    Ok(())
}

/// Live replicas a partition should have: the configured RF (never less than it has now),
/// capped at the nodes that can hold one.
fn desired_rf(p: &PartitionAssignment, rf: u32, capacity: usize) -> usize {
    (rf.max(1) as usize).max(p.replicas.len()).min(capacity)
}

/// The partition has a live leader to copy from.
fn has_live_leader(p: &PartitionAssignment, alive: &BTreeSet<String>) -> bool {
    p.leader
        .as_ref()
        .is_some_and(|l| alive.contains(l) && p.replicas.contains(l))
}

/// Replica counts per eligible node over `target` (every eligible node present, possibly 0).
fn replica_load(
    target: &BTreeMap<(String, u32), Vec<String>>,
    eligible: &BTreeSet<String>,
) -> BTreeMap<String, usize> {
    let mut load: BTreeMap<String, usize> = eligible.iter().map(|n| (n.clone(), 0)).collect();
    for reps in target.values() {
        for r in reps {
            if let Some(c) = load.get_mut(r) {
                *c += 1;
            }
        }
    }
    load
}

fn preferred_load(
    target: &BTreeMap<(String, u32), Vec<String>>,
    eligible: &BTreeSet<String>,
) -> BTreeMap<String, usize> {
    let mut load: BTreeMap<String, usize> = eligible.iter().map(|n| (n.clone(), 0)).collect();
    for reps in target.values() {
        if let Some(c) = reps.first().and_then(|r| load.get_mut(r)) {
            *c += 1;
        }
    }
    load
}

fn spread(load: &BTreeMap<String, usize>) -> usize {
    match (load.values().max(), load.values().min()) {
        (Some(max), Some(min)) => max - min,
        _ => 0,
    }
}

/// `max - min` replicas per eligible node.
pub fn imbalance(assignments: &Assignments, eligible: &BTreeSet<String>) -> u32 {
    spread(&replica_load(&working_copy(assignments), eligible)) as u32
}

/// Partitions with fewer live replicas than they should have (RF capped at alive nodes).
pub fn under_replicated(assignments: &Assignments, alive: &BTreeSet<String>, rf: u32) -> u32 {
    assignments
        .values()
        .flatten()
        .filter(|p| {
            let live = p.replicas.iter().filter(|r| alive.contains(*r)).count();
            live < desired_rf(p, rf, alive.len())
        })
        .count() as u32
}

fn working_copy(assignments: &Assignments) -> BTreeMap<(String, u32), Vec<String>> {
    assignments
        .iter()
        .flat_map(|(c, parts)| {
            parts
                .iter()
                .map(move |p| ((c.clone(), p.partition), p.replicas.clone()))
        })
        .collect()
}

/// Least-loaded eligible node not in `exclude` (ties: less storage, then id).
fn pick_target(
    load: &BTreeMap<String, usize>,
    storage: &BTreeMap<String, u64>,
    exclude: &[String],
) -> Option<String> {
    load.iter()
        .filter(|(n, _)| !exclude.contains(n))
        .min_by_key(|(n, c)| (**c, storage.get(*n).copied().unwrap_or(0), (*n).clone()))
        .map(|(n, _)| n.clone())
}

/// Nodes sorted for the greedy: sources most loaded first, destinations least loaded first
/// (ties: storage, then id — deterministic).
fn ordered(
    load: &BTreeMap<String, usize>,
    storage: &BTreeMap<String, u64>,
) -> (Vec<String>, Vec<String>) {
    let key = |n: &String| storage.get(n).copied().unwrap_or(0);
    let mut src: Vec<String> = load.keys().cloned().collect();
    src.sort_by(|a, b| {
        load[b]
            .cmp(&load[a])
            .then(key(b).cmp(&key(a)))
            .then(a.cmp(b))
    });
    let mut dst: Vec<String> = load.keys().cloned().collect();
    dst.sort_by(|a, b| {
        load[a]
            .cmp(&load[b])
            .then(key(a).cmp(&key(b)))
            .then(a.cmp(b))
    });
    (src, dst)
}

/// Computes the moves (repair + balance) for the current state. Deterministic.
pub fn plan(
    assignments: &Assignments,
    view: &PlacementView,
    rf: u32,
    config: &RebalanceConfig,
    storage: &BTreeMap<String, u64>,
    opts: &PlanOptions,
) -> Vec<PartitionMove> {
    let current = working_copy(assignments);
    let mut target = current.clone();
    let parts: BTreeMap<(String, u32), &PartitionAssignment> = assignments
        .iter()
        .flat_map(|(c, ps)| ps.iter().map(move |p| ((c.clone(), p.partition), p)))
        .collect();
    let movable: BTreeSet<(String, u32)> = parts
        .iter()
        .filter(|((c, _), p)| {
            (opts.channels.is_empty() || opts.channels.contains(c))
                && has_live_leader(p, &view.alive)
                && {
                    let uniq: BTreeSet<&String> = p.replicas.iter().collect();
                    uniq.len() == p.replicas.len()
                }
        })
        .map(|(k, _)| k.clone())
        .collect();
    let eligible = &view.eligible;
    let mut repaired: BTreeSet<(String, u32)> = BTreeSet::new();

    // 1. Repair.
    if opts.repair {
        let mut load = replica_load(&target, eligible);
        for key in &movable {
            let p = parts[key];
            let reps = target.get_mut(key).expect("working copy");
            // Replace replicas on long-down / decommissioning nodes (only when a replacement
            // exists).
            let dead: Vec<String> = reps.iter().filter(|r| view.evict(r)).cloned().collect();
            for d in dead {
                let Some(t) = pick_target(&load, storage, reps) else {
                    break;
                };
                reps.retain(|r| *r != d);
                reps.push(t.clone());
                *load.get_mut(&t).expect("eligible") += 1;
                repaired.insert(key.clone());
            }
            // Grow a capped RF.
            let pinned = reps
                .iter()
                .filter(|r| !eligible.contains(*r) && !view.leaving(r))
                .count();
            let want = desired_rf(p, rf, eligible.len() + pinned);
            let live = reps.iter().filter(|r| !view.leaving(r)).count();
            for _ in live..want {
                let Some(t) = pick_target(&load, storage, reps) else {
                    break;
                };
                reps.push(t.clone());
                *load.get_mut(&t).expect("eligible") += 1;
                repaired.insert(key.clone());
            }
        }
    }

    let threshold = config.imbalance_threshold as usize;
    let changed = |target: &BTreeMap<(String, u32), Vec<String>>, k: &(String, u32)| {
        target.get(k) != current.get(k)
    };

    // 2. Balance replicas per eligible node.
    if opts.balance && eligible.len() >= 2 {
        let mut load = replica_load(&target, eligible);
        if opts.manual || spread(&load) > threshold {
            loop {
                let (srcs, dsts) = ordered(&load, storage);
                let mut step: Option<((String, u32), String, String)> = None;
                'search: for src in &srcs {
                    for dst in &dsts {
                        if load[src] < load[dst] + 2 {
                            continue;
                        }
                        let best = movable
                            .iter()
                            .filter(|k| {
                                let reps = &target[*k];
                                reps.contains(src) && !reps.contains(dst)
                            })
                            .min_by_key(|k| {
                                let reps = &target[*k];
                                (
                                    !changed(&target, k),
                                    reps.first() == Some(src),
                                    (*k).clone(),
                                )
                            });
                        if let Some(k) = best {
                            step = Some((k.clone(), src.clone(), dst.clone()));
                            break 'search;
                        }
                    }
                }
                let Some((k, src, dst)) = step else { break };
                let reps = target.get_mut(&k).expect("working copy");
                for r in reps.iter_mut() {
                    if *r == src {
                        *r = dst.clone();
                    }
                }
                *load.get_mut(&src).expect("eligible") -= 1;
                *load.get_mut(&dst).expect("eligible") += 1;
            }
        }
    }

    // 3. Balance preferred leaders (replica 0) by reordering.
    if opts.balance && eligible.len() >= 2 {
        let mut pref = preferred_load(&target, eligible);
        if opts.manual || spread(&pref) > threshold {
            loop {
                let (srcs, dsts) = ordered(&pref, storage);
                let mut step: Option<((String, u32), String, String)> = None;
                'search: for src in &srcs {
                    for dst in &dsts {
                        if pref[src] < pref[dst] + 2 {
                            continue;
                        }
                        let best = movable
                            .iter()
                            .filter(|k| {
                                let reps = &target[*k];
                                reps.first() == Some(src) && reps.contains(dst)
                            })
                            .min_by_key(|k| (!changed(&target, k), (*k).clone()));
                        if let Some(k) = best {
                            step = Some((k.clone(), src.clone(), dst.clone()));
                            break 'search;
                        }
                    }
                }
                let Some((k, src, dst)) = step else { break };
                let reps = target.get_mut(&k).expect("working copy");
                reps.retain(|r| *r != dst);
                reps.insert(0, dst.clone());
                *pref.get_mut(&src).expect("eligible") -= 1;
                *pref.get_mut(&dst).expect("eligible") += 1;
            }
        }
    }

    let mut moves: Vec<PartitionMove> = target
        .iter()
        .filter(|(k, reps)| current.get(*k) != Some(*reps))
        .map(|((channel, partition), to)| {
            let key = (channel.clone(), *partition);
            let reason = if repaired.contains(&key) {
                MoveReason::Repair
            } else if opts.manual {
                MoveReason::Manual
            } else {
                MoveReason::Balance
            };
            PartitionMove {
                channel: channel.clone(),
                partition: *partition,
                reason,
                from_replicas: current[&key].clone(),
                to_replicas: to.clone(),
                state: MoveState::Pending,
                lag: None,
                initial_lag: None,
                started_at_ms: None,
                finished_at_ms: None,
                error: None,
            }
        })
        .collect();
    // Repairs first (durability), then by partition.
    moves.sort_by(|a, b| {
        (a.reason != MoveReason::Repair, &a.channel, a.partition).cmp(&(
            b.reason != MoveReason::Repair,
            &b.channel,
            b.partition,
        ))
    });
    moves
}

/// Applies `moves` (their `to_replicas`) to `assignments`, as if every move completed.
pub fn apply(assignments: &Assignments, moves: &[PartitionMove]) -> Assignments {
    let mut out = assignments.clone();
    for m in moves {
        if let Some(p) = out
            .get_mut(&m.channel)
            .and_then(|ps| ps.iter_mut().find(|p| p.partition == m.partition))
        {
            p.replicas = m.to_replicas.clone();
            p.isr = m.to_replicas.clone();
            if !p.leader.as_ref().is_some_and(|l| p.replicas.contains(l)) {
                p.leader = m.to_replicas.first().cloned();
            }
        }
    }
    out
}

/// Per-node load over `nodes` (node → registry status; sorted by id).
pub fn node_load(
    assignments: &Assignments,
    nodes: &BTreeMap<String, NodeStatus>,
    storage: &BTreeMap<String, u64>,
) -> Vec<NodeLoad> {
    let mut load: BTreeMap<String, NodeLoad> = nodes
        .iter()
        .map(|(n, status)| {
            (
                n.clone(),
                NodeLoad {
                    node: n.clone(),
                    status: *status,
                    replicas: 0,
                    leaders: 0,
                    preferred_leaders: 0,
                    storage_bytes: storage.get(n).copied().unwrap_or(0),
                },
            )
        })
        .collect();
    for p in assignments.values().flatten() {
        for (i, r) in p.replicas.iter().enumerate() {
            if let Some(l) = load.get_mut(r) {
                l.replicas += 1;
                if i == 0 {
                    l.preferred_leaders += 1;
                }
            }
        }
        if let Some(l) = p.leader.as_ref().and_then(|l| load.get_mut(l)) {
            l.leaders += 1;
        }
    }
    load.into_values().collect()
}

// ---------------------------------------------------------------------------------------------
// Assignment transitions of the move protocol (applied by the executor with CAS)
// ---------------------------------------------------------------------------------------------

/// `from` followed by the replicas of `to` it lacks (old preferred first).
pub fn union_replicas(from: &[String], to: &[String]) -> Vec<String> {
    let mut out = from.to_vec();
    out.extend(to.iter().filter(|r| !from.contains(r)).cloned());
    out
}

/// Step 1: add the new replicas. Leader, ISR and epoch are unchanged: the leader picks up the
/// new followers at the same epoch (not in the ISR until caught up), so publishes never wait
/// for an empty replica.
pub fn add_replicas(p: &PartitionAssignment, to: &[String]) -> PartitionAssignment {
    let mut next = p.clone();
    next.replicas = union_replicas(&p.replicas, to);
    next
}

/// Commits `replicas` as the final list (step 2, or a rollback to the old list): the ISR is
/// restricted to it; the leader is kept when it stays a live in-sync replica, otherwise the
/// first live ISR member in `replicas` order leads. The epoch is bumped when the leader
/// changes or a replica is removed — a removed replica deletes its local copy only for an
/// assignment epoch newer than the one it followed at (`ReplicationManager::remove_replica`);
/// a pure reorder keeps it. `Err` when no live ISR member exists (retry later).
pub fn commit_replicas(
    p: &PartitionAssignment,
    replicas: &[String],
    view: &View,
) -> Result<PartitionAssignment, String> {
    let mut next = p.clone();
    next.replicas = replicas.to_vec();
    next.isr.retain(|n| replicas.contains(n));
    let removed = p.replicas.iter().any(|r| !replicas.contains(r));
    let keep = p
        .leader
        .as_ref()
        .is_some_and(|l| replicas.contains(l) && view.is_alive(l) && p.isr.contains(l));
    if keep {
        if removed {
            next.epoch += 1;
        }
        return Ok(next);
    }
    let leader = replicas
        .iter()
        .find(|n| next.isr.contains(n) && view.is_alive(n) && !view.leaving.contains(*n))
        .or_else(|| {
            replicas
                .iter()
                .find(|n| next.isr.contains(n) && view.is_alive(n))
        })
        .cloned()
        .ok_or_else(|| "no live in-sync replica among the target replicas".to_string())?;
    next.leader = Some(leader);
    next.epoch += 1;
    Ok(next)
}

/// Every replica of `to` that `from` lacks is in the ISR.
pub fn caught_up(p: &PartitionAssignment, from: &[String], to: &[String]) -> bool {
    to.iter()
        .filter(|r| !from.contains(r))
        .all(|r| p.isr.contains(r))
}

fn same_set(a: &[String], b: &[String]) -> bool {
    a.len() == b.len() && a.iter().all(|x| b.contains(x))
}

/// Where a partition stands relative to a move (used to resume after a controller change:
/// the assignment write may have committed while the plan update did not).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MovePhase {
    /// `replicas == from`.
    Before,
    /// `replicas == from ∪ to` (as a set).
    Added,
    /// `replicas == to` (as a set).
    Committed,
    /// Anything else: changed by someone else.
    Diverged,
}

pub fn phase(p: &PartitionAssignment, m: &PartitionMove) -> MovePhase {
    if p.replicas == m.from_replicas {
        MovePhase::Before
    } else if same_set(&p.replicas, &m.to_replicas) {
        MovePhase::Committed
    } else if same_set(
        &p.replicas,
        &union_replicas(&m.from_replicas, &m.to_replicas),
    ) {
        MovePhase::Added
    } else {
        MovePhase::Diverged
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluxmq_proto::cluster::RebalanceMode;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }
    fn set(v: &[&str]) -> BTreeSet<String> {
        v.iter().map(|x| x.to_string()).collect()
    }
    fn up(v: &[&str]) -> BTreeMap<String, NodeStatus> {
        v.iter().map(|x| (x.to_string(), NodeStatus::Up)).collect()
    }

    fn part(p: u32, replicas: &[&str]) -> PartitionAssignment {
        PartitionAssignment {
            partition: p,
            leader: replicas.first().map(|r| r.to_string()),
            replicas: s(replicas),
            isr: s(replicas),
            epoch: 1,
        }
    }

    fn pv(alive: &[&str], replace: &[&str]) -> PlacementView {
        PlacementView {
            alive: set(alive),
            eligible: set(alive),
            replace: set(replace),
            decommission: BTreeSet::new(),
        }
    }

    /// `alive` nodes, `decom` among them (or down) being decommissioned (never eligible).
    fn pv_decom(alive: &[&str], decom: &[&str]) -> PlacementView {
        let mut v = pv(alive, &[]);
        for d in decom {
            v.eligible.remove(*d);
            v.decommission.insert(d.to_string());
        }
        v
    }

    fn cfg(threshold: u32) -> RebalanceConfig {
        RebalanceConfig {
            imbalance_threshold: threshold,
            ..RebalanceConfig::default()
        }
    }

    fn full() -> PlanOptions {
        PlanOptions::auto(RebalanceMode::Full)
    }

    fn load_of(a: &Assignments, nodes: &[&str]) -> Vec<u32> {
        node_load(a, &up(nodes), &BTreeMap::new())
            .iter()
            .map(|l| l.replicas)
            .collect()
    }

    /// 3 nodes, 6 partitions, RF 2 (as `new_assignment` places them).
    fn three_node_channel() -> Assignments {
        let nodes = s(&["n1", "n2", "n3"]);
        let a = crate::assign::new_assignment(6, &nodes, 2, &BTreeMap::new());
        BTreeMap::from([("orders".to_string(), a)])
    }

    #[test]
    fn balanced_cluster_needs_nothing() {
        let a = three_node_channel();
        let v = pv(&["n1", "n2", "n3"], &[]);
        assert!(plan(&a, &v, 2, &cfg(1), &BTreeMap::new(), &full()).is_empty());
    }

    #[test]
    fn new_node_gets_its_share_with_minimal_moves() {
        let a = three_node_channel();
        let v = pv(&["n1", "n2", "n3", "n4"], &[]);
        let moves = plan(&a, &v, 2, &cfg(1), &BTreeMap::new(), &full());
        let after = apply(&a, &moves);
        // 12 replicas over 4 nodes: exactly 3 each, reached by moving 3 replicas.
        assert_eq!(load_of(&after, &["n1", "n2", "n3", "n4"]), vec![3, 3, 3, 3]);
        let replica_moves: usize = moves
            .iter()
            .map(|m| {
                m.to_replicas
                    .iter()
                    .filter(|r| !m.from_replicas.contains(r))
                    .count()
            })
            .sum();
        assert_eq!(replica_moves, 3, "{moves:#?}");
        assert!(moves.iter().all(|m| m.reason == MoveReason::Balance));
        assert!(moves.iter().all(|m| m.state == MoveState::Pending));
        // Preferred leaders are spread too: max - min <= 1.
        let pref: Vec<u32> = node_load(&after, &up(&["n1", "n2", "n3", "n4"]), &BTreeMap::new())
            .iter()
            .map(|l| l.preferred_leaders)
            .collect();
        assert!(
            pref.iter().max().unwrap() - pref.iter().min().unwrap() <= 1,
            "{pref:?}"
        );
        assert!(plan(&after, &v, 2, &cfg(1), &BTreeMap::new(), &full()).is_empty());
    }

    #[test]
    fn threshold_and_modes_gate_balancing() {
        let a = three_node_channel();
        let v = pv(&["n1", "n2", "n3", "n4"], &[]);
        // Spread 4 (4,4,4,0) is within a threshold of 4.
        assert!(plan(&a, &v, 2, &cfg(4), &BTreeMap::new(), &full()).is_empty());
        // Repair-only mode never balances; off does nothing.
        let repair = PlanOptions::auto(RebalanceMode::Repair);
        assert!(plan(&a, &v, 2, &cfg(1), &BTreeMap::new(), &repair).is_empty());
        let off = PlanOptions::auto(RebalanceMode::Off);
        assert!(plan(&a, &v, 2, &cfg(1), &BTreeMap::new(), &off).is_empty());
        // A manual request ignores the threshold.
        let manual = PlanOptions::manual(Vec::new());
        let moves = plan(&a, &v, 2, &cfg(4), &BTreeMap::new(), &manual);
        assert!(!moves.is_empty());
        assert!(moves.iter().all(|m| m.reason == MoveReason::Manual));
        // Channel filter: nothing outside it moves.
        let other = PlanOptions::manual(vec!["other".into()]);
        assert!(plan(&a, &v, 2, &cfg(1), &BTreeMap::new(), &other).is_empty());
    }

    #[test]
    fn repair_replaces_long_down_node_and_restores_rf() {
        let a = three_node_channel();
        // n2 is down but not long enough: nothing to repair.
        let v = pv(&["n1", "n3"], &[]);
        let repair = PlanOptions::auto(RebalanceMode::Repair);
        assert!(plan(&a, &v, 2, &cfg(1), &BTreeMap::new(), &repair).is_empty());
        // Down long enough (after failover, so live leaders): every replica on n2 is
        // replaced by the only other node.
        let mut a = a;
        for p in a.get_mut("orders").unwrap() {
            if p.leader.as_deref() == Some("n2") {
                p.leader = p.replicas.iter().find(|r| *r != "n2").cloned();
            }
        }
        let v = pv(&["n1", "n3"], &["n2"]);
        let moves = plan(&a, &v, 2, &cfg(1), &BTreeMap::new(), &repair);
        assert!(!moves.is_empty());
        let after = apply(&a, &moves);
        for p in &after["orders"] {
            assert_eq!(p.replicas.len(), 2);
            assert!(!p.replicas.contains(&"n2".to_string()), "{p:?}");
        }
        assert!(moves.iter().all(|m| m.reason == MoveReason::Repair));
        assert!(plan(&after, &v, 2, &cfg(1), &BTreeMap::new(), &repair).is_empty());
        assert_eq!(under_replicated(&after, &set(&["n1", "n3"]), 2), 0);
        assert!(under_replicated(&a, &set(&["n1", "n3"]), 2) > 0);
    }

    #[test]
    fn repair_keeps_dead_replica_without_replacement() {
        // RF 2 on two nodes, n2 gone: no third node to replace it with — keep it.
        let a = BTreeMap::from([("c".to_string(), vec![part(0, &["n1", "n2"])])]);
        let v = pv(&["n1"], &["n2"]);
        assert!(plan(&a, &v, 2, &cfg(1), &BTreeMap::new(), &full()).is_empty());
    }

    #[test]
    fn capped_rf_grows_and_offline_partitions_stay() {
        let mut a = BTreeMap::from([(
            "c".to_string(),
            vec![part(0, &["n1"]), part(1, &["n2"]), part(2, &["n1"])],
        )]);
        // Partition 2 is offline (no leader): never touched.
        a.get_mut("c").unwrap()[2].leader = None;
        let v = pv(&["n1", "n2", "n3"], &[]);
        let repair = PlanOptions::auto(RebalanceMode::Repair);
        let moves = plan(&a, &v, 3, &cfg(1), &BTreeMap::new(), &repair);
        assert_eq!(moves.len(), 2);
        for m in &moves {
            assert_eq!(m.to_replicas.len(), 3);
            assert_eq!(
                m.to_replicas[0], m.from_replicas[0],
                "leader stays preferred"
            );
        }
        assert!(moves.iter().all(|m| m.partition != 2));
    }

    #[test]
    fn leaving_and_down_nodes_are_never_targets() {
        let a = three_node_channel();
        let mut v = pv(&["n1", "n2", "n3", "n4", "n5"], &[]);
        v.eligible.remove("n4"); // leaving / draining / not yet stable
        let moves = plan(&a, &v, 2, &cfg(1), &BTreeMap::new(), &full());
        for m in &moves {
            assert!(!m.to_replicas.contains(&"n4".to_string()));
            assert!(
                m.from_replicas.contains(&"n4".to_string())
                    || !m.to_replicas.contains(&"n4".to_string())
            );
        }
        assert!(
            moves
                .iter()
                .any(|m| m.to_replicas.contains(&"n5".to_string()))
        );
    }

    #[test]
    fn decommissioned_node_loses_every_replica_and_gets_none() {
        // 4 nodes, RF 2, n2 decommissioned (alive, still leading some partitions).
        let nodes = s(&["n1", "n2", "n3", "n4"]);
        let a = BTreeMap::from([(
            "orders".to_string(),
            crate::assign::new_assignment(8, &nodes, 2, &BTreeMap::new()),
        )]);
        let v = pv_decom(&["n1", "n2", "n3", "n4"], &["n2"]);
        // Repair-only (what the decommission executor plans) and a full auto plan alike.
        for opts in [PlanOptions::auto(RebalanceMode::Repair), full()] {
            let moves = plan(&a, &v, 2, &cfg(1), &BTreeMap::new(), &opts);
            let after = apply(&a, &moves);
            for p in &after["orders"] {
                assert!(!p.replicas.contains(&"n2".to_string()), "{p:?}");
                assert_eq!(p.replicas.len(), 2, "{p:?}");
            }
            assert!(
                moves
                    .iter()
                    .filter(|m| m.from_replicas.contains(&"n2".to_string()))
                    .all(|m| m.reason == MoveReason::Repair)
            );
            assert!(
                moves
                    .iter()
                    .all(|m| !m.to_replicas.contains(&"n2".to_string()))
            );
            assert!(plan(&after, &v, 2, &cfg(1), &BTreeMap::new(), &opts).is_empty());
        }
        // Mode off plans nothing, even for a decommissioning node (the executor plans
        // repair itself).
        let off = PlanOptions::auto(RebalanceMode::Off);
        assert!(plan(&a, &v, 2, &cfg(1), &BTreeMap::new(), &off).is_empty());
        // A down decommissioning node is evicted too (its partitions led elsewhere).
        let mut down = a.clone();
        for p in down.get_mut("orders").unwrap() {
            if p.leader.as_deref() == Some("n2") {
                p.leader = p.replicas.iter().find(|r| *r != "n2").cloned();
            }
        }
        let v = pv_decom(&["n1", "n3", "n4"], &["n2"]);
        let moves = plan(&down, &v, 2, &cfg(1), &BTreeMap::new(), &full());
        let after = apply(&down, &moves);
        assert!(
            after["orders"]
                .iter()
                .all(|p| !p.replicas.contains(&"n2".to_string()))
        );
        // Not enough other nodes for RF 2: the replica stays (never below the old set).
        let two = BTreeMap::from([("c".to_string(), vec![part(0, &["n1", "n2"])])]);
        let v = pv_decom(&["n1", "n2"], &["n2"]);
        assert!(plan(&two, &v, 2, &cfg(1), &BTreeMap::new(), &full()).is_empty());
    }

    #[test]
    fn storage_breaks_ties() {
        let a = three_node_channel();
        let v = pv(&["n1", "n2", "n3", "n4", "n5"], &[]);
        let storage = BTreeMap::from([("n4".to_string(), 1_000_000u64), ("n5".to_string(), 0)]);
        let moves = plan(&a, &v, 2, &cfg(1), &storage, &full());
        let after = apply(&a, &moves);
        let l = node_load(&after, &up(&["n4", "n5"]), &storage);
        assert!(l[1].replicas >= l[0].replicas, "{l:?}");
    }

    #[test]
    fn transitions() {
        let view = View {
            alive: set(&["n1", "n2", "n3", "n4"]),
            leaving: BTreeSet::new(),
        };
        let p = part(0, &["n1", "n2"]);
        let to = s(&["n4", "n2"]);
        let added = add_replicas(&p, &to);
        assert_eq!(added.replicas, s(&["n1", "n2", "n4"]));
        assert_eq!((added.leader.as_deref(), added.epoch), (Some("n1"), 1));
        assert_eq!(added.isr, s(&["n1", "n2"]));
        let m = PartitionMove {
            channel: "c".into(),
            partition: 0,
            reason: MoveReason::Balance,
            from_replicas: p.replicas.clone(),
            to_replicas: to.clone(),
            state: MoveState::Pending,
            lag: None,
            initial_lag: None,
            started_at_ms: None,
            finished_at_ms: None,
            error: None,
        };
        assert_eq!(phase(&p, &m), MovePhase::Before);
        assert_eq!(phase(&added, &m), MovePhase::Added);
        assert!(!caught_up(&added, &p.replicas, &to));
        let mut synced = added.clone();
        synced.isr.push("n4".into());
        assert!(caught_up(&synced, &p.replicas, &to));
        // Leader n1 is removed: n4 (first live ISR member in target order) leads, epoch + 1.
        let done = commit_replicas(&synced, &to, &view).unwrap();
        assert_eq!(done.replicas, to);
        assert_eq!(done.isr, s(&["n2", "n4"]));
        assert_eq!((done.leader.as_deref(), done.epoch), (Some("n4"), 2));
        assert_eq!(phase(&done, &m), MovePhase::Committed);
        // Committing before n4 caught up keeps n2 (live ISR) as leader.
        let early = commit_replicas(&added, &to, &view).unwrap();
        assert_eq!(early.leader.as_deref(), Some("n2"));
        // Leader stays, n2 removed: epoch + 1 (the removed replica may delete its copy).
        let keep = commit_replicas(&synced, &s(&["n1", "n4"]), &view).unwrap();
        assert_eq!((keep.leader.as_deref(), keep.epoch), (Some("n1"), 2));
        // Pure reorder with the same leader: same epoch.
        let reorder = commit_replicas(&synced, &s(&["n4", "n1", "n2"]), &view).unwrap();
        assert_eq!((reorder.leader.as_deref(), reorder.epoch), (Some("n1"), 1));
        // Rollback to `from` with nobody alive in it fails.
        let dead = View {
            alive: set(&["n4"]),
            leaving: BTreeSet::new(),
        };
        assert!(commit_replicas(&added, &p.replicas, &dead).is_err());
        let mut other = p.clone();
        other.replicas = s(&["n3", "n2"]);
        assert_eq!(phase(&other, &m), MovePhase::Diverged);
    }

    // ---- randomized property tests (deterministic seeds; no extra dependencies) ----

    /// xorshift64*: tiny deterministic PRNG for the property tests.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n.max(1)
        }
        fn chance(&mut self, pct: u64) -> bool {
            self.below(100) < pct
        }
    }

    struct Case {
        a: Assignments,
        view: PlacementView,
        rf: u32,
        cfg: RebalanceConfig,
        storage: BTreeMap<String, u64>,
    }

    fn random_case(rng: &mut Rng) -> Case {
        let n_nodes = 1 + rng.below(7) as usize;
        let nodes: Vec<String> = (1..=n_nodes).map(|i| format!("n{i}")).collect();
        let rf = 1 + rng.below(3) as u32;
        let mut a = BTreeMap::new();
        for c in 0..(1 + rng.below(4)) {
            let parts = 1 + rng.below(8) as u32;
            let mut ps = Vec::new();
            for p in 0..parts {
                // Random distinct replicas (1..=min(rf+1, n)).
                let k = 1 + rng.below((rf as u64 + 1).min(n_nodes as u64)) as usize;
                let mut pool = nodes.clone();
                let mut reps = Vec::new();
                for _ in 0..k {
                    let i = rng.below(pool.len() as u64) as usize;
                    reps.push(pool.remove(i));
                }
                let mut pa = PartitionAssignment {
                    partition: p,
                    leader: Some(reps[0].clone()),
                    isr: reps.clone(),
                    replicas: reps,
                    epoch: 1,
                };
                if rng.chance(10) {
                    pa.leader = None;
                }
                ps.push(pa);
            }
            a.insert(format!("ch{c}"), ps);
        }
        let mut view = PlacementView::default();
        for n in &nodes {
            match rng.below(10) {
                0 => {
                    view.replace.insert(n.clone());
                }
                1 => {} // down, not replaceable yet
                2 => {
                    view.alive.insert(n.clone()); // alive but leaving / unstable
                }
                3 if rng.chance(30) => {
                    // Decommissioning (alive or down).
                    if rng.chance(70) {
                        view.alive.insert(n.clone());
                    }
                    view.decommission.insert(n.clone());
                }
                _ => {
                    view.alive.insert(n.clone());
                    view.eligible.insert(n.clone());
                }
            }
        }
        // Extra fresh nodes with no replicas.
        for i in 0..rng.below(3) {
            let id = format!("x{i}");
            view.alive.insert(id.clone());
            view.eligible.insert(id);
        }
        let storage = view
            .alive
            .iter()
            .map(|n| (n.clone(), rng.below(1000)))
            .collect();
        let cfg = RebalanceConfig {
            imbalance_threshold: rng.below(3) as u32,
            ..RebalanceConfig::default()
        };
        Case {
            a,
            view,
            rf,
            cfg,
            storage,
        }
    }

    fn opts_for(rng: &mut Rng) -> PlanOptions {
        match rng.below(4) {
            0 => PlanOptions::auto(RebalanceMode::Repair),
            1 => PlanOptions::manual(Vec::new()),
            _ => PlanOptions::auto(RebalanceMode::Full),
        }
    }

    #[test]
    fn property_invariants_and_convergence() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for case_no in 0..3000 {
            let c = random_case(&mut rng);
            let opts = opts_for(&mut rng);
            let moves = plan(&c.a, &c.view, c.rf, &c.cfg, &c.storage, &opts);
            let ctx = || format!("case {case_no}: {:?} {:?} {opts:?}", c.a, c.view);

            // Deterministic.
            assert_eq!(
                moves,
                plan(&c.a, &c.view, c.rf, &c.cfg, &c.storage, &opts),
                "{}",
                ctx()
            );
            let parts: BTreeMap<(String, u32), &PartitionAssignment> =
                c.a.iter()
                    .flat_map(|(ch, ps)| ps.iter().map(move |p| ((ch.clone(), p.partition), p)))
                    .collect();
            for m in &moves {
                let p = parts[&(m.channel.clone(), m.partition)];
                assert_eq!(m.from_replicas, p.replicas, "{}", ctx());
                // Distinct replicas, never empty.
                let uniq: BTreeSet<&String> = m.to_replicas.iter().collect();
                assert_eq!(uniq.len(), m.to_replicas.len(), "{}", ctx());
                assert!(!m.to_replicas.is_empty(), "{}", ctx());
                // Only eligible nodes are added.
                for r in m
                    .to_replicas
                    .iter()
                    .filter(|r| !m.from_replicas.contains(r))
                {
                    assert!(c.view.eligible.contains(r), "{} {m:?}", ctx());
                }
                // Only replaceable nodes or (when balancing) eligible ones are removed;
                // pinned replicas (alive-ineligible, briefly down) always stay.
                for r in m
                    .from_replicas
                    .iter()
                    .filter(|r| !m.to_replicas.contains(r))
                {
                    assert!(
                        c.view.replace.contains(r)
                            || c.view.eligible.contains(r)
                            || c.view.decommission.contains(r),
                        "{} {m:?}",
                        ctx()
                    );
                }
                // Never fewer replicas than before (durability).
                assert!(m.to_replicas.len() >= m.from_replicas.len(), "{}", ctx());
                // Moved partitions have a live leader.
                assert!(has_live_leader(p, &c.view.alive), "{}", ctx());
                if !opts.channels.is_empty() {
                    assert!(opts.channels.contains(&m.channel));
                }
            }
            // Convergence: planning again on the result is a no-op.
            let after = apply(&c.a, &moves);
            let again = plan(&after, &c.view, c.rf, &c.cfg, &c.storage, &opts);
            assert!(
                again.is_empty(),
                "{}\nfirst {moves:#?}\nagain {again:#?}",
                ctx()
            );

            // Repair restores RF wherever enough eligible nodes exist.
            if opts.repair {
                for (ch, ps) in &after {
                    for p in ps {
                        if !has_live_leader(p, &c.view.alive) {
                            continue;
                        }
                        let gone = |r: &String| {
                            c.view.replace.contains(r) || c.view.decommission.contains(r)
                        };
                        let live = p.replicas.iter().filter(|r| !gone(r)).count();
                        let avail = c.view.eligible.len()
                            + p.replicas
                                .iter()
                                .filter(|r| !c.view.eligible.contains(*r) && !gone(r))
                                .count();
                        let want = (c.rf as usize).min(avail);
                        assert!(live >= want, "{ch}/{} {p:?} {}", p.partition, ctx());
                        // No replica left on a replaceable / decommissioning node when another
                        // eligible node could take it.
                        if p.replicas.iter().any(|r| {
                            (c.view.replace.contains(r) && !c.view.alive.contains(r))
                                || c.view.decommission.contains(r)
                        }) {
                            let free = c
                                .view
                                .eligible
                                .iter()
                                .filter(|n| !p.replicas.contains(n))
                                .count();
                            assert_eq!(free, 0, "{ch}/{} {}", p.partition, ctx());
                        }
                    }
                }
            }
            // Balance: within one replica per eligible node unless constrained.
            if opts.balance && opts.manual && c.view.eligible.len() >= 2 {
                let movable_all = after.values().flatten().all(|p| {
                    has_live_leader(p, &c.view.alive)
                        && p.replicas.iter().all(|r| c.view.eligible.contains(r))
                });
                let load = replica_load(&working_copy(&after), &c.view.eligible);
                let rf_fits = after
                    .values()
                    .flatten()
                    .all(|p| p.replicas.len() * 2 <= c.view.eligible.len());
                if movable_all && rf_fits {
                    assert!(spread(&load) <= 1, "{load:?} {}", ctx());
                }
            }
        }
    }

    #[test]
    fn property_balance_moves_are_minimal_for_fresh_nodes() {
        // Adding k empty nodes to a balanced cluster moves exactly the replicas they need.
        let mut rng = Rng(42);
        for _ in 0..300 {
            let n = 2 + rng.below(4) as usize;
            let k = 1 + rng.below(3) as usize;
            let rf = 1 + rng.below(n.min(3) as u64) as u32;
            let parts = 1 + rng.below(24) as u32;
            let nodes: Vec<String> = (1..=n).map(|i| format!("n{i}")).collect();
            let a = BTreeMap::from([(
                "c".to_string(),
                crate::assign::new_assignment(parts, &nodes, rf, &BTreeMap::new()),
            )]);
            let mut all = nodes.clone();
            all.extend((1..=k).map(|i| format!("z{i}")));
            let refs: Vec<&str> = all.iter().map(String::as_str).collect();
            let v = pv(&refs, &[]);
            let moves = plan(&a, &v, rf, &cfg(1), &BTreeMap::new(), &full());
            let after = apply(&a, &moves);
            let total = (parts * rf) as usize;
            let floor = total / all.len();
            let moved: usize = moves
                .iter()
                .map(|m| {
                    m.to_replicas
                        .iter()
                        .filter(|r| !m.from_replicas.contains(r))
                        .count()
                })
                .sum();
            let load = replica_load(&working_copy(&after), &v.eligible);
            assert!(spread(&load) <= 1, "{load:?}");
            // Each fresh node ends with floor or floor+1; only replicas *to* them move.
            let fresh: usize = all[n..].iter().map(|z| load[z]).sum();
            let old: BTreeSet<String> = nodes.iter().cloned().collect();
            let old_balanced = spread(&replica_load(&working_copy(&a), &old)) <= 1;
            if old_balanced && spread(&replica_load(&working_copy(&a), &v.eligible)) > 1 {
                assert_eq!(moved, fresh, "{moves:#?}");
                assert!(fresh >= floor * k);
            } else if old_balanced {
                assert!(
                    moves
                        .iter()
                        .all(|m| same_set(&m.from_replicas, &m.to_replicas))
                );
            }
        }
    }
}
