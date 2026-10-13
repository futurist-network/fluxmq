//! Per-node reconciler: makes local state follow the replicated metadata.
//!
//! * local channels ⇄ `cluster/channels/*` (create / update; delete only once this node has
//!   caught up with the controller, so a restarting node never deletes on a stale view);
//! * replication endpoints from `cluster/nodes/*`;
//! * replication roles from `cluster/assignments/*` (leader roles only with a valid lease —
//!   see `runtime` docs), plus `isr_committed` for partitions this node leads;
//! * the metadata epoch → `Cluster::watch_epoch` (the server pushes `MetadataChanged`).
//!
//! The ISR loop persists ISR changes reported by local partition leaders (CAS on the
//! assignment, only while this node still leads at that epoch).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use fluxmq_meta::{MetaError, MetaStore, MetaStoreExt, Precondition};
use fluxmq_proto::ChannelConfig;
use fluxmq_proto::api::UpdateChannelRequest;
use fluxmq_proto::cluster::PartitionAssignment;
use fluxmq_replication::{PartitionRole, ReplicaEndpoint};
use tokio::sync::broadcast::error::RecvError;
use tokio::time::Instant;

use crate::assign;
use crate::controller::{ASSIGNMENTS_PREFIX, CHANNELS_PREFIX, NODES_PREFIX, assignment_key};
use crate::rpc::NodeRecord;
use crate::runtime::Runtime;

#[derive(Default)]
struct State {
    /// (channel, partition) → (role applied, ISR confirmed via `isr_committed`).
    roles: HashMap<(String, u32), (PartitionRole, Vec<String>)>,
    endpoints: Vec<(String, String)>,
    caught_up: bool,
    epoch: u64,
    /// (channel, partition) → assignment epoch whose replica removal was processed.
    removed: HashMap<(String, u32), u64>,
    /// Catch-up throttles applied (v0.4 rebalancing).
    throttles: HashMap<(String, u32), u64>,
    /// Registry nodes advertising batch records (v0.7 rolling upgrade).
    batch_capable: Option<BTreeSet<String>>,
    /// Batch-write switch applied per led partition (v0.7 rolling upgrade).
    batch_writes: HashMap<(String, u32), bool>,
}

pub(crate) async fn run(rt: Arc<Runtime>) {
    let mut events = rt.sm.events.subscribe();
    let mut lease = rt.lease.subscribe();
    let mut tick = tokio::time::interval(rt.timings.tick);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut st = State::default();
    loop {
        tokio::select! {
            () = rt.shutdown.cancelled() => return,
            () = rt.leaving.cancelled() => return,
            ev = events.recv() => match ev {
                Ok(_) | Err(RecvError::Lagged(_)) => {
                    // Coalesce bursts.
                    while events.try_recv().is_ok() {}
                }
                Err(RecvError::Closed) => return,
            },
            _ = lease.changed() => {}
            _ = tick.tick() => {}
        }
        reconcile(&rt, &mut st).await;
    }
}

fn update_fenced(rt: &Runtime) -> bool {
    let lease = *rt.lease.borrow();
    let valid = lease.valid_until.is_some_and(|t| t > Instant::now());
    let was = rt.fenced.load(Ordering::Acquire);
    let fenced = if was {
        !(valid && rt.sm.revision() >= lease.revision)
    } else {
        !valid
    };
    // Fencing takes effect immediately; unfencing only at the end of a reconcile pass, once
    // local channels exist and roles are applied (so clients never see "channel not found"
    // from a node that metadata names as leader).
    if fenced && !was {
        tracing::warn!(node = %rt.node_id(), "lost contact with the controller: fencing partition leadership");
        rt.fenced.store(true, Ordering::Release);
    }
    fenced
}

async fn reconcile(rt: &Arc<Runtime>, st: &mut State) {
    let fenced = update_fenced(rt);
    if !fenced {
        st.caught_up = true;
    }
    let desired: BTreeMap<String, ChannelConfig> = rt
        .meta
        .list_as::<ChannelConfig>(CHANNELS_PREFIX)
        .unwrap_or_default()
        .into_iter()
        .map(|(_, c)| (c.name.clone(), c))
        .collect();
    sync_channels(rt, &desired, st.caught_up).await;
    sync_endpoints(rt, st).await;
    sync_roles(rt, st, &desired, fenced).await;
    sync_throttles(rt, st);
    if !fenced && rt.fenced.load(Ordering::Acquire) {
        tracing::info!(node = %rt.node_id(), "controller lease acquired; serving partitions");
        rt.fenced.store(false, Ordering::Release);
    }
    let epoch = rt.sm.cluster_epoch();
    if epoch != st.epoch {
        st.epoch = epoch;
        rt.epoch.send_replace(epoch);
    }
}

async fn sync_channels(
    rt: &Arc<Runtime>,
    desired: &BTreeMap<String, ChannelConfig>,
    may_delete: bool,
) {
    let local: BTreeMap<String, ChannelConfig> = rt
        .broker
        .channels()
        .into_iter()
        .map(|c| (c.name.clone(), c))
        .collect();
    let mut ops: Vec<ChannelOp> = Vec::new();
    for (name, want) in desired {
        match local.get(name) {
            None => ops.push(ChannelOp::Create(want.clone())),
            Some(have)
                if have.partitions != want.partitions
                    || have.created_at_ms != want.created_at_ms =>
            {
                // A different incarnation of the channel (deleted and re-created while this
                // node was away).
                if may_delete {
                    ops.push(ChannelOp::Recreate(want.clone()));
                }
            }
            Some(have)
                if have.retention != want.retention
                    || have.retry != want.retry
                    || have.dlq_enabled != want.dlq_enabled =>
            {
                ops.push(ChannelOp::Update(want.clone()));
            }
            Some(_) => {}
        }
    }
    if may_delete {
        for name in local.keys().filter(|n| !desired.contains_key(*n)) {
            ops.push(ChannelOp::Delete(name.clone()));
        }
    }
    if ops.is_empty() {
        return;
    }
    let broker = rt.broker.clone();
    let _ = tokio::task::spawn_blocking(move || {
        for op in ops {
            let (what, name, res) = match op {
                ChannelOp::Create(c) => {
                    let name = c.name.clone();
                    ("create", name, broker.create_channel(c).map(drop))
                }
                ChannelOp::Recreate(c) => {
                    let name = c.name.clone();
                    let _ = broker.delete_channel(&name);
                    ("recreate", name, broker.create_channel(c).map(drop))
                }
                ChannelOp::Update(c) => {
                    let req = UpdateChannelRequest {
                        retention: Some(c.retention),
                        retry: Some(c.retry),
                        dlq_enabled: Some(c.dlq_enabled),
                    };
                    (
                        "update",
                        c.name.clone(),
                        broker.update_channel(&c.name, req).map(drop),
                    )
                }
                ChannelOp::Delete(name) => {
                    let res = broker.delete_channel(&name);
                    ("delete", name, res)
                }
            };
            match res {
                Ok(()) => tracing::info!(channel = %name, "local channel {what}"),
                Err(e) => {
                    tracing::warn!(channel = %name, error = %e, "local channel {what} failed")
                }
            }
        }
    })
    .await;
}

enum ChannelOp {
    Create(ChannelConfig),
    Recreate(ChannelConfig),
    Update(ChannelConfig),
    Delete(String),
}

async fn sync_endpoints(rt: &Runtime, st: &mut State) {
    let records = rt
        .meta
        .list_as::<NodeRecord>(NODES_PREFIX)
        .unwrap_or_default();
    let capable: BTreeSet<String> = records
        .iter()
        .filter(|(_, r)| r.supports_batches())
        .map(|(_, r)| r.id.clone())
        .collect();
    if st.batch_capable.as_ref() != Some(&capable) {
        rt.replication
            .set_batch_capable_nodes(capable.iter().cloned().collect());
        st.batch_capable = Some(capable);
    }
    let mut want: Vec<(String, String)> = records
        .into_iter()
        .map(|(_, r)| (r.id, r.replication_addr))
        .collect();
    want.sort();
    if want == st.endpoints {
        return;
    }
    let mut endpoints = Vec::new();
    for (id, addr) in &want {
        match tokio::net::lookup_host(addr.as_str())
            .await
            .ok()
            .and_then(|mut a| a.next())
        {
            Some(sa) => endpoints.push(ReplicaEndpoint {
                node_id: id.clone(),
                addr: sa,
            }),
            None => {
                tracing::warn!(node = %id, %addr, "cannot resolve replication address");
                return;
            }
        }
    }
    rt.replication.set_endpoints(endpoints);
    st.endpoints = want;
}

async fn sync_roles(
    rt: &Arc<Runtime>,
    st: &mut State,
    desired: &BTreeMap<String, ChannelConfig>,
    fenced: bool,
) {
    let me = rt.node_id().to_string();
    let mut want: HashMap<(String, u32), (PartitionRole, Vec<String>)> = HashMap::new();
    // Partitions whose committed replica list doesn't contain this node, with that epoch.
    let mut not_replica: Vec<((String, u32), u64)> = Vec::new();
    for (key, v) in rt.meta.list(ASSIGNMENTS_PREFIX) {
        let Some(name) = key.strip_prefix(ASSIGNMENTS_PREFIX) else {
            continue;
        };
        if !desired.contains_key(name) || rt.broker.channel(name).is_none() {
            continue;
        }
        let Ok(parts) = serde_json::from_value::<Vec<PartitionAssignment>>(v.value) else {
            continue;
        };
        for p in parts {
            if !p.replicas.contains(&me) {
                not_replica.push(((name.to_string(), p.partition), p.epoch));
            }
            let mut role = assign::role_for(&me, &p, rt.cfg.min_isr);
            if fenced && matches!(role, PartitionRole::Leader { .. }) {
                role = PartitionRole::None;
            }
            want.insert((name.to_string(), p.partition), (role, p.isr));
        }
    }
    st.roles.retain(|k, _| want.contains_key(k));
    st.removed.retain(|k, _| want.contains_key(k));
    let mut changes = Vec::new();
    for (k, (role, isr)) in want.clone() {
        let prev = st.roles.get(&k);
        let role_changed = prev.is_none_or(|(r, _)| *r != role);
        let isr_changed =
            matches!(role, PartitionRole::Leader { .. }) && prev.is_none_or(|(_, i)| *i != isr);
        if role_changed || isr_changed {
            changes.push((k, role, isr, role_changed));
        }
    }
    // Write format first, so a partition never leads with a stale one.
    sync_batch_writes(rt, st, &want);
    if !changes.is_empty() {
        apply_roles(rt, st, changes).await;
    }
    if st.caught_up {
        remove_replicas(rt, st, not_replica).await;
    }
}

/// v0.7 rolling upgrade rule: a partition leader writes batch envelopes only while every
/// replica in the partition's assignment runs v0.7 or newer (its registry record advertises
/// `FEATURE_BATCH_RECORDS`; a node not in the registry counts as old). Otherwise `PublishBatch` is stored as legacy
/// records, which v0.6 followers can replicate. Re-evaluated on every pass, so batches turn
/// on as soon as the last replica's upgraded incarnation is registered.
///
/// A node older than v0.7 (re)added to a partition that already wrote batches is refused
/// by the leader's fetch path (`ReplicationManager::set_batch_capable_nodes`); this logs the
/// cause here as well.
fn sync_batch_writes(
    rt: &Runtime,
    st: &mut State,
    roles: &HashMap<(String, u32), (PartitionRole, Vec<String>)>,
) {
    let mut want: HashMap<(String, u32), bool> = HashMap::new();
    for (key, (role, _)) in roles {
        if let PartitionRole::Leader { replicas, .. } = role {
            let old: Vec<&String> = replicas
                .iter()
                .filter(|r| !rt.replication.node_supports_batches(r))
                .collect();
            if !old.is_empty() && st.batch_writes.get(key) == Some(&true) {
                tracing::error!(
                    channel = %key.0,
                    partition = key.1,
                    old_replicas = ?old,
                    "a replica older than v0.7 was added to a partition that writes batch \
                     records: batch writes are off again, but it can't replicate the batches \
                     already written (downgrade is unsupported; upgrade the node or reassign)"
                );
            }
            want.insert(key.clone(), old.is_empty());
        }
    }
    st.batch_writes.retain(|k, _| want.contains_key(k));
    for (key, on) in want {
        if st.batch_writes.get(&key) == Some(&on) {
            continue;
        }
        match rt.replication.set_batch_writes(&key.0, key.1, on) {
            Ok(()) => {
                tracing::info!(channel = %key.0, partition = key.1, batch_writes = on, "partition write format set");
                st.batch_writes.insert(key, on);
            }
            Err(e) => {
                tracing::debug!(channel = %key.0, partition = key.1, error = %e, "setting batch writes deferred");
            }
        }
    }
}

/// A replica removed from a partition's replica list (rebalance move, reassignment, repair)
/// deletes its local copy (`ReplicationManager::remove_replica`): once per assignment epoch,
/// only after this node caught up with the controller (never on a stale view). Partitions
/// this node never held are a cheap no-op there.
async fn remove_replicas(rt: &Arc<Runtime>, st: &mut State, candidates: Vec<((String, u32), u64)>) {
    for (key, epoch) in candidates {
        if st.removed.get(&key).is_some_and(|e| *e >= epoch) {
            continue;
        }
        if !matches!(st.roles.get(&key), Some((PartitionRole::None, _))) {
            // The `None` role must be in force first (applied by `apply_roles`).
            continue;
        }
        match rt.replication.remove_replica(&key.0, key.1, epoch).await {
            Ok(released) => {
                tracing::debug!(channel = %key.0, partition = key.1, epoch, released, "replica removal processed");
                st.removed.insert(key, epoch);
            }
            Err(fluxmq_replication::ReplicationError::StaleEpoch { .. }) => {
                st.removed.insert(key, epoch);
            }
            Err(e) => {
                tracing::debug!(channel = %key.0, partition = key.1, epoch, error = %e, "replica removal deferred");
            }
        }
    }
}

/// Catch-up throttles for partitions of the active rebalance plan (every node sets them;
/// the partition leader enforces them).
fn sync_throttles(rt: &Runtime, st: &mut State) {
    let want = crate::rebalancer::throttle_targets(rt);
    if want == st.throttles {
        return;
    }
    for (channel, partition) in st.throttles.keys() {
        if !want.contains_key(&(channel.clone(), *partition)) {
            rt.replication.set_catch_up_throttle(channel, *partition, 0);
        }
    }
    for ((channel, partition), rate) in &want {
        if st.throttles.get(&(channel.clone(), *partition)) != Some(rate) {
            rt.replication
                .set_catch_up_throttle(channel, *partition, *rate);
        }
    }
    st.throttles = want;
}

/// (partition, role, committed ISR, role changed).
type RoleChange = ((String, u32), PartitionRole, Vec<String>, bool);

async fn apply_roles(rt: &Arc<Runtime>, st: &mut State, changes: Vec<RoleChange>) {
    let repl = rt.replication.clone();
    let applied = tokio::task::spawn_blocking(move || {
        let mut ok = Vec::new();
        for ((channel, partition), role, isr, role_changed) in changes {
            if role_changed && let Err(e) = repl.set_role(&channel, partition, role.clone()) {
                tracing::warn!(%channel, partition, ?role, error = %e, "set_role failed");
                continue;
            }
            if let PartitionRole::Leader { epoch, .. } = &role {
                repl.isr_committed(&channel, partition, *epoch, isr.clone());
            }
            if role_changed {
                tracing::debug!(%channel, partition, ?role, "role applied");
            }
            ok.push(((channel, partition), (role, isr)));
        }
        ok
    })
    .await
    .unwrap_or_default();
    st.roles.extend(applied);
}

/// Persists ISR changes reported by local partition leaders.
pub(crate) async fn isr_loop(rt: Arc<Runtime>) {
    let mut rx = rt.replication.isr_changes();
    // Reports not written yet, at most one per partition: only the latest ISR of a
    // partition is ever written. Writing historical reports one by one (each a Raft write)
    // replays stale ISRs under churn; the reconciler then applies them as committed shrinks
    // and the leader reports again — an ISR flap.
    let mut pending: std::collections::VecDeque<fluxmq_replication::IsrChange> =
        std::collections::VecDeque::new();
    fn coalesce(
        pending: &mut std::collections::VecDeque<fluxmq_replication::IsrChange>,
        c: fluxmq_replication::IsrChange,
    ) {
        match pending
            .iter_mut()
            .find(|p| p.channel == c.channel && p.partition == c.partition)
        {
            Some(p) => *p = c,
            None => pending.push_back(c),
        }
    }
    loop {
        if pending.is_empty() {
            tokio::select! {
                () = rt.shutdown.cancelled() => return,
                () = rt.leaving.cancelled() => return,
                c = rx.recv() => match c {
                    Ok(c) => coalesce(&mut pending, c),
                    Err(RecvError::Lagged(n)) => {
                        tracing::warn!(missed = n, "ISR change notifications lagged");
                        continue;
                    }
                    Err(RecvError::Closed) => return,
                },
            }
        }
        loop {
            match rx.try_recv() {
                Ok(c) => coalesce(&mut pending, c),
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                    tracing::warn!(missed = n, "ISR change notifications lagged");
                }
                Err(_) => break,
            }
        }
        let Some(mut change) = pending.pop_front() else {
            continue;
        };
        // Every leader of the channel writes the same assignment key, so conflicts are
        // normal under churn (restarts, elections). Retry them with a small jittered
        // backoff; a change that still can't be written is re-sent by the leader's ISR
        // tick (`ReplicaHandle::tick`) until the controller commits it.
        let mut persisted = false;
        for attempt in 0..20u32 {
            if attempt > 0 {
                // A newer report for this partition supersedes the one being retried.
                while let Ok(c) = rx.try_recv() {
                    coalesce(&mut pending, c);
                }
                if let Some(i) = pending
                    .iter()
                    .position(|p| p.channel == change.channel && p.partition == change.partition)
                {
                    change = pending.remove(i).expect("index in range");
                }
            }
            let key = assignment_key(&change.channel);
            let Some(v) = rt.meta.get(&key) else { break };
            let Ok(mut parts) = serde_json::from_value::<Vec<PartitionAssignment>>(v.value) else {
                break;
            };
            let Some(p) = parts.iter_mut().find(|p| p.partition == change.partition) else {
                break;
            };
            let Some(next) = assign::apply_isr(p, rt.node_id(), change.epoch, &change.isr) else {
                // Already committed, or stale (another epoch / leader): nothing to write.
                persisted = true;
                break;
            };
            *p = next;
            let value = match serde_json::to_value(&parts) {
                Ok(v) => v,
                Err(_) => break,
            };
            let pre = Precondition {
                key: key.clone(),
                expected_mod_revision: v.mod_revision,
            };
            match rt.meta.put(&key, value, Some(pre)).await {
                Ok(_) => {
                    tracing::info!(channel = %change.channel, partition = change.partition, isr = ?change.isr, "ISR persisted");
                    // `isr_committed` follows from the reconciler once applied locally.
                    persisted = true;
                    break;
                }
                Err(MetaError::Conflict { .. }) => {
                    let jitter = u64::from(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_or(0, |d| d.subsec_nanos())
                            % 20,
                    );
                    tokio::time::sleep(Duration::from_millis(
                        5 * u64::from(attempt.min(10)) + jitter,
                    ))
                    .await;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "persisting ISR change failed; retrying");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
        if !persisted {
            tracing::warn!(channel = %change.channel, partition = change.partition, epoch = change.epoch, isr = ?change.isr, "ISR change not persisted; the partition leader will report it again");
        }
    }
}
