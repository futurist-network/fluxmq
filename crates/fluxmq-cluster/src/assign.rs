//! Pure placement / leader-election logic used by the controller and the per-node reconciler.
//! No I/O; every function maps (current assignment, liveness view) → new assignment.
//!
//! Invariants:
//! * Leaders are only ever chosen from the (committed) ISR — no unclean election. A partition
//!   with no live ISR member goes offline (`leader: None`) and keeps its ISR so that the first
//!   ISR member to come back is elected.
//! * Every leadership change bumps `epoch`, which fences the previous leader.

use std::collections::{BTreeMap, BTreeSet};

use fluxmq_proto::cluster::PartitionAssignment;
use fluxmq_replication::PartitionRole;

/// Liveness as seen by the controller.
#[derive(Debug, Clone, Default)]
pub struct View {
    /// Nodes considered alive (heartbeating).
    pub alive: BTreeSet<String>,
    /// Alive nodes being drained: not chosen as leader unless nothing else is possible.
    pub leaving: BTreeSet<String>,
}

impl View {
    pub fn is_alive(&self, n: &str) -> bool {
        self.alive.contains(n)
    }

    fn can_lead(&self, n: &str) -> bool {
        self.alive.contains(n) && !self.leaving.contains(n)
    }
}

/// Number of partitions each node leads.
pub fn leader_load<'a>(
    assignments: impl IntoIterator<Item = &'a Vec<PartitionAssignment>>,
) -> BTreeMap<String, usize> {
    let mut load = BTreeMap::new();
    for parts in assignments {
        for p in parts {
            if let Some(l) = &p.leader {
                *load.entry(l.clone()).or_insert(0) += 1;
            }
        }
    }
    load
}

/// Placement for a new channel. `nodes` = candidate nodes (alive, not leaving), any order.
/// RF = min(`rf`, nodes). Each partition's leader is the least-loaded node (ties broken by a
/// rotating index), followers are the next nodes in ring order; preferred leader = replica 0.
pub fn new_assignment(
    partitions: u32,
    nodes: &[String],
    rf: u32,
    load: &BTreeMap<String, usize>,
) -> Vec<PartitionAssignment> {
    let mut nodes: Vec<String> = nodes.to_vec();
    nodes.sort();
    nodes.dedup();
    let n = nodes.len();
    if n == 0 {
        return (0..partitions)
            .map(|p| PartitionAssignment {
                partition: p,
                leader: None,
                replicas: Vec::new(),
                isr: Vec::new(),
                epoch: 1,
            })
            .collect();
    }
    let rf = (rf.max(1) as usize).min(n);
    let mut load: Vec<usize> = nodes
        .iter()
        .map(|id| load.get(id).copied().unwrap_or(0))
        .collect();
    (0..partitions)
        .map(|p| {
            let start = p as usize % n;
            let idx = (0..n)
                .map(|i| (start + i) % n)
                .min_by_key(|&i| load[i])
                .expect("n > 0");
            load[idx] += 1;
            let replicas: Vec<String> = (0..rf).map(|i| nodes[(idx + i) % n].clone()).collect();
            PartitionAssignment {
                partition: p,
                leader: Some(replicas[0].clone()),
                isr: replicas.clone(),
                replicas,
                epoch: 1,
            }
        })
        .collect()
}

/// Best leader candidate among the ISR, in replica (preference) order: alive non-leaving
/// first, then alive leaving. `exclude` is never chosen.
fn pick_leader(a: &PartitionAssignment, view: &View, exclude: Option<&str>) -> Option<String> {
    let ordered = a
        .replicas
        .iter()
        .chain(a.isr.iter().filter(|n| !a.replicas.contains(n)))
        .filter(|n| a.isr.contains(n) && Some(n.as_str()) != exclude);
    let candidates: Vec<&String> = ordered.collect();
    candidates
        .iter()
        .find(|n| view.can_lead(n))
        .or_else(|| candidates.iter().find(|n| view.is_alive(n)))
        .map(|n| (*n).clone())
}

/// Failover for one partition: `None` if no change is needed. With a live leader, dead
/// followers are removed from the ISR (epoch unchanged). Otherwise a new leader from the live
/// ISR (dead members removed from the ISR, epoch + 1), or — if no ISR member is alive —
/// offline (`leader: None`, epoch + 1, ISR kept).
pub fn failover(a: &PartitionAssignment, view: &View) -> Option<PartitionAssignment> {
    let mut next = a.clone();
    if let Some(l) = &a.leader
        && view.is_alive(l)
        && a.replicas.contains(l)
    {
        // Leader fine: drop dead followers from the ISR right away (same epoch) so acks
        // (HW over the committed ISR) don't wait for the leader's lag eviction.
        if a.isr.iter().all(|n| view.is_alive(n)) {
            return None;
        }
        next.isr.retain(|n| view.is_alive(n));
        return Some(next);
    }
    match pick_leader(a, view, None) {
        Some(leader) => {
            next.isr.retain(|n| view.is_alive(n));
            next.leader = Some(leader);
            next.epoch += 1;
            Some(next)
        }
        None if a.leader.is_some() => {
            next.leader = None;
            next.epoch += 1;
            Some(next)
        }
        None => None,
    }
}

/// Moves leadership back to the preferred replica (replica 0) when it is alive, in the ISR,
/// not leaving and `stable` (up long enough to not flap).
pub fn preferred_election(
    a: &PartitionAssignment,
    view: &View,
    stable: &BTreeSet<String>,
) -> Option<PartitionAssignment> {
    let preferred = a.replicas.first()?;
    if a.leader.as_ref() == Some(preferred)
        || !a.isr.contains(preferred)
        || !view.can_lead(preferred)
        || !stable.contains(preferred)
    {
        return None;
    }
    let mut next = a.clone();
    next.leader = Some(preferred.clone());
    next.epoch += 1;
    Some(next)
}

/// Moves leadership off `node` to another live ISR member, if there is one.
pub fn drain_partition(
    a: &PartitionAssignment,
    node: &str,
    view: &View,
) -> Option<PartitionAssignment> {
    if a.leader.as_deref() != Some(node) {
        return None;
    }
    let mut v = view.clone();
    v.leaving.insert(node.to_string());
    let leader = pick_leader(a, &v, Some(node))?;
    let mut next = a.clone();
    next.leader = Some(leader);
    next.epoch += 1;
    Some(next)
}

/// Operator reassignment to `replicas` (preferred leader first). The current leader is kept
/// if it stays a replica; otherwise the new leader must be a live ISR member among the new
/// replicas (add replicas first, wait for them to join the ISR, then remove old ones).
pub fn reassign_partition(
    a: &PartitionAssignment,
    replicas: Vec<String>,
    view: &View,
) -> Result<PartitionAssignment, String> {
    if replicas.is_empty() {
        return Err("replicas must not be empty".into());
    }
    let unique: BTreeSet<&String> = replicas.iter().collect();
    if unique.len() != replicas.len() {
        return Err("replicas must be distinct".into());
    }
    let mut next = a.clone();
    next.replicas = replicas;
    next.isr.retain(|n| next.replicas.contains(n));
    let keep = a
        .leader
        .as_ref()
        .is_some_and(|l| next.replicas.contains(l) && view.is_alive(l));
    if !keep {
        let Some(leader) = pick_leader(&next, view, None) else {
            return Err(
                "none of the new replicas is a live in-sync replica; keep the current leader \
                 in the list until the new replicas have caught up"
                    .into(),
            );
        };
        next.leader = Some(leader);
    }
    next.epoch += 1;
    Ok(next)
}

/// ISR change reported by the partition leader; applied only if it still leads at `epoch`.
pub fn apply_isr(
    a: &PartitionAssignment,
    leader: &str,
    epoch: u64,
    isr: &[String],
) -> Option<PartitionAssignment> {
    if a.leader.as_deref() != Some(leader) || a.epoch != epoch || a.isr == isr {
        return None;
    }
    let mut next = a.clone();
    next.isr = isr.to_vec();
    Some(next)
}

/// The replication role of node `me` for one partition.
pub fn role_for(me: &str, a: &PartitionAssignment, min_isr: u32) -> PartitionRole {
    match &a.leader {
        Some(l) if l == me => PartitionRole::Leader {
            epoch: a.epoch,
            replicas: a.replicas.clone(),
            min_isr: min_isr.clamp(1, a.replicas.len().max(1) as u32),
        },
        Some(l) if a.replicas.iter().any(|r| r == me) => PartitionRole::Follower {
            epoch: a.epoch,
            leader: l.clone(),
        },
        _ => PartitionRole::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn view(alive: &[&str]) -> View {
        View {
            alive: alive.iter().map(|x| x.to_string()).collect(),
            leaving: BTreeSet::new(),
        }
    }

    fn pa(
        leader: Option<&str>,
        replicas: &[&str],
        isr: &[&str],
        epoch: u64,
    ) -> PartitionAssignment {
        PartitionAssignment {
            partition: 0,
            leader: leader.map(str::to_string),
            replicas: s(replicas),
            isr: s(isr),
            epoch,
        }
    }

    #[test]
    fn new_assignment_spreads_leaders_and_caps_rf() {
        let a = new_assignment(6, &s(&["n3", "n1", "n2"]), 3, &BTreeMap::new());
        let load = leader_load([&a]);
        assert_eq!(load.values().copied().collect::<Vec<_>>(), vec![2, 2, 2]);
        for p in &a {
            assert_eq!(p.replicas.len(), 3);
            assert_eq!(p.leader.as_ref(), p.replicas.first());
            assert_eq!(p.isr, p.replicas);
            assert_eq!(p.epoch, 1);
        }
        // RF capped at live nodes.
        let b = new_assignment(2, &s(&["n1"]), 3, &BTreeMap::new());
        assert!(b.iter().all(|p| p.replicas == s(&["n1"])));
        // Existing load is balanced out: n1 already leads 4.
        let load = BTreeMap::from([("n1".to_string(), 4)]);
        let c = new_assignment(4, &s(&["n1", "n2", "n3"]), 2, &load);
        assert!(c.iter().all(|p| p.leader.as_deref() != Some("n1")));
        // No nodes: offline placeholders.
        assert!(
            new_assignment(1, &[], 3, &BTreeMap::new())[0]
                .leader
                .is_none()
        );
    }

    #[test]
    fn failover_elects_from_live_isr_preferring_replica_order() {
        let a = pa(Some("n1"), &["n1", "n2", "n3"], &["n1", "n2", "n3"], 4);
        assert_eq!(failover(&a, &view(&["n1", "n2", "n3"])), None);
        // Dead follower: shrunk out of the ISR, leader and epoch unchanged.
        let f = failover(&a, &view(&["n1", "n2"])).unwrap();
        assert_eq!((f.leader.as_deref(), f.epoch), (Some("n1"), 4));
        assert_eq!(f.isr, s(&["n1", "n2"]));
        let f = failover(&a, &view(&["n2", "n3"])).unwrap();
        assert_eq!(f.leader.as_deref(), Some("n2"));
        assert_eq!(f.epoch, 5);
        assert_eq!(f.isr, s(&["n2", "n3"]));
        // n2 not in ISR → n3.
        let a = pa(Some("n1"), &["n1", "n2", "n3"], &["n1", "n3"], 4);
        assert_eq!(
            failover(&a, &view(&["n2", "n3"]))
                .unwrap()
                .leader
                .as_deref(),
            Some("n3")
        );
    }

    #[test]
    fn no_unclean_election_and_recovery_from_offline() {
        let a = pa(Some("n1"), &["n1", "n2"], &["n1"], 2);
        let off = failover(&a, &view(&["n2"])).unwrap();
        assert_eq!(off.leader, None);
        assert_eq!(off.epoch, 3);
        assert_eq!(off.isr, s(&["n1"]), "ISR kept while offline");
        // Still offline: no further change / epoch churn.
        assert_eq!(failover(&off, &view(&["n2"])), None);
        // The ISR member returns → elected.
        let back = failover(&off, &view(&["n1", "n2"])).unwrap();
        assert_eq!(back.leader.as_deref(), Some("n1"));
        assert_eq!(back.epoch, 4);
    }

    #[test]
    fn leaving_nodes_are_last_resort() {
        let a = pa(Some("n1"), &["n1", "n2", "n3"], &["n1", "n2", "n3"], 1);
        let mut v = view(&["n2", "n3"]);
        v.leaving.insert("n2".into());
        assert_eq!(failover(&a, &v).unwrap().leader.as_deref(), Some("n3"));
        let mut v = view(&["n2"]);
        v.leaving.insert("n2".into());
        assert_eq!(failover(&a, &v).unwrap().leader.as_deref(), Some("n2"));
    }

    #[test]
    fn preferred_election_requires_isr_and_stability() {
        let a = pa(Some("n2"), &["n1", "n2"], &["n1", "n2"], 3);
        let v = view(&["n1", "n2"]);
        let stable: BTreeSet<String> = ["n1".to_string()].into();
        assert_eq!(preferred_election(&a, &v, &BTreeSet::new()), None);
        let p = preferred_election(&a, &v, &stable).unwrap();
        assert_eq!((p.leader.as_deref(), p.epoch), (Some("n1"), 4));
        let not_isr = pa(Some("n2"), &["n1", "n2"], &["n2"], 3);
        assert_eq!(preferred_election(&not_isr, &v, &stable), None);
    }

    #[test]
    fn drain_and_reassign() {
        let a = pa(Some("n1"), &["n1", "n2", "n3"], &["n1", "n3"], 7);
        let v = view(&["n1", "n2", "n3"]);
        let d = drain_partition(&a, "n1", &v).unwrap();
        assert_eq!((d.leader.as_deref(), d.epoch), (Some("n3"), 8));
        assert_eq!(drain_partition(&a, "n2", &v), None);
        let solo = pa(Some("n1"), &["n1"], &["n1"], 1);
        assert_eq!(drain_partition(&solo, "n1", &v), None);

        // Keep leader when it stays a replica; ISR restricted to new replicas.
        let r = reassign_partition(&a, s(&["n3", "n1", "n4"]), &v).unwrap();
        assert_eq!(r.leader.as_deref(), Some("n1"));
        assert_eq!(r.isr, s(&["n1", "n3"]));
        assert_eq!(r.epoch, 8);
        // Leader removed: new leader must come from the ISR.
        let r = reassign_partition(&a, s(&["n3", "n2"]), &v).unwrap();
        assert_eq!(r.leader.as_deref(), Some("n3"));
        assert!(reassign_partition(&a, s(&["n2", "n4"]), &v).is_err());
        assert!(reassign_partition(&a, s(&["n2", "n2"]), &v).is_err());
    }

    #[test]
    fn isr_updates_are_fenced_by_epoch_and_leader() {
        let a = pa(Some("n1"), &["n1", "n2"], &["n1", "n2"], 3);
        assert_eq!(apply_isr(&a, "n2", 3, &s(&["n2"])), None);
        assert_eq!(apply_isr(&a, "n1", 2, &s(&["n1"])), None);
        assert_eq!(apply_isr(&a, "n1", 3, &s(&["n1", "n2"])), None);
        assert_eq!(apply_isr(&a, "n1", 3, &s(&["n1"])).unwrap().isr, s(&["n1"]));
    }

    #[test]
    fn roles() {
        let a = pa(Some("n1"), &["n1", "n2"], &["n1", "n2"], 3);
        assert_eq!(
            role_for("n1", &a, 5),
            PartitionRole::Leader {
                epoch: 3,
                replicas: s(&["n1", "n2"]),
                min_isr: 2
            }
        );
        assert_eq!(
            role_for("n2", &a, 1),
            PartitionRole::Follower {
                epoch: 3,
                leader: "n1".into()
            }
        );
        assert_eq!(role_for("n3", &a, 1), PartitionRole::None);
        let off = pa(None, &["n1", "n2"], &["n1"], 4);
        assert_eq!(role_for("n1", &off, 1), PartitionRole::None);
    }
}
