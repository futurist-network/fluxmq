//! Content-based routing (roadmap stage 8) and the filter DSL shared with message search.
//! Contract and semantics: `fluxmq_proto::routing` (read its module docs first).
//!
//! * Routes persist in the meta store under `routes/<id>` (cluster-replicated when Raft-backed)
//!   and are hot-reloaded via `meta.watch()`.
//! * Per source channel with ≥ 1 enabled route, one router worker subscribes as consumer group
//!   `$router` (`StartFrom::Latest` when the group is new) and forwards matches with
//!   `Broker::publish`, acking after all targets accepted; on failure → nack(retry) so the
//!   channel's retry policy and DLQ apply. In a cluster, a node runs workers only for the
//!   partitions it leads (the subscription already scopes this).
//!
//! Implementation notes:
//! * Deliveries are processed in batches (up to the prefetch, 256). Copies are grouped per
//!   target channel and published with one `Broker::publish` per target (targets in parallel);
//!   the batch is then acked in one call, failed deliveries are nacked individually.
//! * One copy per (message, target): when several routes of the same source match a message
//!   and share a target, the first matching route (by creation time) forwards it.
//! * A target that already appears in `flux-route-path` is skipped (loop protection), and so
//!   is any forward that would exceed [`MAX_HOPS`].
//! * A partial failure (one target accepted, another failed) nacks the delivery; the retry
//!   re-publishes to all targets (at-least-once — duplicates in the accepting target).
//! * Workers stop when their source has no enabled route left, and end by themselves when the
//!   source channel is deleted. When the last route of a source is deleted, the API also
//!   deletes the source's `$router` group (on the partition leaders in a cluster) — see
//!   [`RouterService::uses_source`]; a source whose routes are all disabled keeps its group; routes of a deleted channel are kept (inactive) and resume if
//!   a channel with that name is created again. A periodic reconcile (5 s) restarts workers
//!   that ended for any other reason.

pub mod filter;
pub mod scan;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use base64::Engine as _;
use fluxmq_core::{Broker, BrokerEvent, SubscriberInfo, Subscription};
use fluxmq_meta::{MetaError, MetaEvent, Precondition, SharedMetaStore};
use fluxmq_proto::api::{JsonPayload, MessageView};
use fluxmq_proto::routing::{
    Condition, CreateRouteRequest, FieldRef, Op, Route, RouteStats, RouteTestRequest,
    RouteTestResponse, UpdateRouteRequest,
};
use fluxmq_proto::wire::Delivery;
use fluxmq_proto::{Message, PublishMessage, ROUTER_GROUP, StartFrom, now_ms};
use parking_lot::{Mutex, RwLock};
use serde_json::Value;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// `{channel}/{partition}/{offset}` of the message a routed copy was made from.
pub const ROUTED_FROM_HEADER: &str = "flux-routed-from";
/// Id of the route that produced the copy.
pub const ROUTE_ID_HEADER: &str = "flux-route-id";
/// Comma list of the channels the message visited (source first, the copy's channel last).
pub const ROUTE_PATH_HEADER: &str = "flux-route-path";
/// Max forwards of one original message (loop protection).
pub const MAX_HOPS: usize = 8;
/// Meta store key prefix.
pub const ROUTES_PREFIX: &str = "routes/";
/// Outstanding deliveries per router worker (also the max batch).
pub const PREFETCH: u32 = 256;
pub const MAX_TARGETS: usize = 16;
pub const MAX_CONDITIONS: usize = 32;
pub const MAX_LIST_LEN: usize = 1000;
pub const DEFAULT_TEST_WINDOW_MS: u64 = 24 * 3600 * 1000;
pub const DEFAULT_SAMPLE_LIMIT: u32 = 20;
pub const MAX_SAMPLE_LIMIT: u32 = 100;
const RECONCILE_EVERY: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("route not found")]
    NotFound,
    #[error("invalid route: {0}")]
    Invalid(String),
    #[error("channel '{0}' not found")]
    ChannelNotFound(String),
    #[error(transparent)]
    Meta(#[from] MetaError),
    #[error(transparent)]
    Core(#[from] fluxmq_core::CoreError),
}

/// Cheap to clone.
#[derive(Clone)]
pub struct RouterService {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Counters {
    evaluated: AtomicU64,
    matched: AtomicU64,
    forwarded: AtomicU64,
    failed: AtomicU64,
    /// 0 = never.
    last_matched_ms: AtomicU64,
}

#[derive(Clone)]
struct ActiveRoute {
    route: Route,
    counters: Arc<Counters>,
}

#[derive(Default)]
struct State {
    /// id → (route, mod_revision).
    routes: BTreeMap<String, (Route, u64)>,
    /// id → revision of the delete (ignores stale watch events for deleted routes).
    tombstones: HashMap<String, u64>,
    /// source → enabled routes ordered by (created_at_ms, id).
    by_source: HashMap<String, Arc<Vec<ActiveRoute>>>,
}

struct Worker {
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

struct Inner {
    broker: Broker,
    meta: Option<SharedMetaStore>,
    state: RwLock<State>,
    stats: Mutex<HashMap<String, Arc<Counters>>>,
    workers: Mutex<HashMap<String, Worker>>,
    shutting_down: AtomicBool,
    stop: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl RouterService {
    /// Loads routes and starts workers. Must be called inside a Tokio runtime.
    pub async fn start(broker: Broker, meta: SharedMetaStore) -> Result<Self, RouterError> {
        let svc = Self::build(broker, Some(meta.clone()));
        // Subscribe before the initial load so no change is missed (stale ones are ignored by
        // revision).
        let meta_rx = meta.watch();
        let events_rx = svc.inner.broker.events();
        svc.inner.reload();
        svc.inner.reconcile();
        let task = tokio::spawn(watch_loop(
            svc.inner.clone(),
            meta_rx,
            events_rx,
            svc.inner.stop.subscribe(),
        ));
        svc.inner.tasks.lock().push(task);
        info!(
            routes = svc.inner.state.read().routes.len(),
            "router started"
        );
        Ok(svc)
    }

    /// No routes, no persistence (defaults for tests / `AppState::new`).
    pub fn disabled(broker: Broker) -> Self {
        Self::build(broker, None)
    }

    fn build(broker: Broker, meta: Option<SharedMetaStore>) -> Self {
        Self {
            inner: Arc::new(Inner {
                broker,
                meta,
                state: RwLock::new(State::default()),
                stats: Mutex::new(HashMap::new()),
                workers: Mutex::new(HashMap::new()),
                shutting_down: AtomicBool::new(false),
                stop: watch::Sender::new(false),
                tasks: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Ordered by creation time.
    pub fn routes(&self) -> Vec<Route> {
        let st = self.inner.state.read();
        let mut v: Vec<Route> = st.routes.values().map(|(r, _)| r.clone()).collect();
        v.sort_by(|a, b| (a.created_at_ms, &a.id).cmp(&(b.created_at_ms, &b.id)));
        v
    }

    pub fn route(&self, id: &str) -> Option<Route> {
        self.inner
            .state
            .read()
            .routes
            .get(id)
            .map(|(r, _)| r.clone())
    }

    /// Validates: source/targets exist, target ≠ source, ≤ 16 targets, ≤ 32 conditions, value
    /// present for value ops, numeric ops have numeric values.
    pub async fn create(&self, req: CreateRouteRequest) -> Result<Route, RouterError> {
        let meta = self.inner.meta()?;
        let name = validate_name(&req.name)?;
        if self.inner.broker.channel(&req.source).is_none() {
            return Err(RouterError::ChannelNotFound(req.source));
        }
        let targets = validate_targets(&self.inner.broker, &req.source, req.targets)?;
        let conditions = validate_conditions(req.conditions)?;
        let now = now_ms();
        let mut route = Route {
            id: String::new(),
            name,
            source: req.source,
            enabled: req.enabled,
            conditions,
            targets,
            created_at_ms: now,
            updated_at_ms: now,
        };
        for _ in 0..8 {
            route.id = new_id();
            let key = route_key(&route.id);
            let value = serde_json::to_value(&route).map_err(MetaError::from)?;
            let pre = Precondition {
                key: key.clone(),
                expected_mod_revision: 0,
            };
            match meta.put(&key, value.clone(), Some(pre)).await {
                Ok(rev) => {
                    self.inner.committed(&meta, &key, Some(value), rev).await?;
                    return Ok(route);
                }
                // Id collision (astronomically unlikely): pick another.
                Err(MetaError::Conflict { .. }) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(RouterError::Invalid("could not allocate a route id".into()))
    }

    pub async fn update(&self, id: &str, req: UpdateRouteRequest) -> Result<Route, RouterError> {
        let meta = self.inner.meta()?;
        let key = route_key(id);
        let current = meta.get(&key).ok_or(RouterError::NotFound)?;
        let mut route: Route = serde_json::from_value(current.value).map_err(MetaError::from)?;
        if let Some(name) = req.name {
            route.name = validate_name(&name)?;
        }
        if let Some(enabled) = req.enabled {
            route.enabled = enabled;
        }
        if let Some(conditions) = req.conditions {
            route.conditions = validate_conditions(conditions)?;
        }
        if let Some(targets) = req.targets {
            route.targets = validate_targets(&self.inner.broker, &route.source, targets)?;
        }
        route.updated_at_ms = now_ms().max(route.created_at_ms);
        let value = serde_json::to_value(&route).map_err(MetaError::from)?;
        let pre = Precondition {
            key: key.clone(),
            expected_mod_revision: current.mod_revision,
        };
        let rev = meta.put(&key, value.clone(), Some(pre)).await?;
        self.inner.committed(&meta, &key, Some(value), rev).await?;
        Ok(route)
    }

    pub async fn delete(&self, id: &str) -> Result<(), RouterError> {
        let meta = self.inner.meta()?;
        let key = route_key(id);
        let current = meta.get(&key).ok_or(RouterError::NotFound)?;
        let pre = Precondition {
            key: key.clone(),
            expected_mod_revision: current.mod_revision,
        };
        let rev = meta.delete(&key, Some(pre)).await?;
        self.inner.committed(&meta, &key, None, rev).await?;
        Ok(())
    }

    /// Backtest over stored messages. Blocking (scans the log): call via `spawn_blocking`.
    pub fn test(&self, req: &RouteTestRequest) -> Result<RouteTestResponse, RouterError> {
        let conditions = validate_conditions(req.conditions.clone())?;
        if self.inner.broker.channel(&req.source).is_none() {
            return Err(RouterError::ChannelNotFound(req.source.clone()));
        }
        let window = req.window_ms.unwrap_or(DEFAULT_TEST_WINDOW_MS);
        let since = now_ms().saturating_sub(window);
        let samples = req
            .sample_limit
            .unwrap_or(DEFAULT_SAMPLE_LIMIT)
            .min(MAX_SAMPLE_LIMIT) as usize;
        let r = scan::backtest(&self.inner.broker, &req.source, &conditions, since, samples)?;
        Ok(RouteTestResponse {
            scanned: r.scanned,
            matched: r.matched,
            truncated: r.truncated,
            samples: r.samples.into_iter().map(message_view).collect(),
        })
    }

    /// One entry per route (zeros for routes that never ran on this node).
    pub fn stats(&self) -> Vec<RouteStats> {
        let routes = self.routes();
        let stats = self.inner.stats.lock();
        routes
            .into_iter()
            .map(|r| {
                let c = stats.get(&r.id);
                let get =
                    |f: fn(&Counters) -> &AtomicU64| c.map_or(0, |c| f(c).load(Ordering::Relaxed));
                let last = get(|c| &c.last_matched_ms);
                RouteStats {
                    id: r.id,
                    evaluated: get(|c| &c.evaluated),
                    matched: get(|c| &c.matched),
                    forwarded: get(|c| &c.forwarded),
                    failed: get(|c| &c.failed),
                    last_matched_ms: (last > 0).then_some(last),
                }
            })
            .collect()
    }

    /// Stops the meta watcher and all workers. Workers finish the batch in progress (its
    /// deliveries are acked/nacked); prefetched, unprocessed deliveries return to the group.
    pub fn shutdown(&self) {
        if self.inner.shutting_down.swap(true, Ordering::AcqRel) {
            return;
        }
        let _ = self.inner.stop.send(true);
        for (_, w) in self.inner.workers.lock().drain() {
            let _ = w.stop.send(true);
        }
        info!("router stopped");
    }

    /// Whether any route (enabled or not) has `source` as its source channel — i.e. whether
    /// the source's `$router` group is still in use. A disabled route pauses routing and keeps
    /// the group (its committed offset); deleting the last route releases it.
    pub fn uses_source(&self, source: &str) -> bool {
        self.inner
            .state
            .read()
            .routes
            .values()
            .any(|(r, _)| r.source == source)
    }

    /// Number of running workers (source channels being routed). For tests and diagnostics.
    pub fn active_sources(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .inner
            .workers
            .lock()
            .iter()
            .filter(|(_, w)| !w.task.is_finished())
            .map(|(s, _)| s.clone())
            .collect();
        v.sort();
        v
    }
}

impl Inner {
    fn meta(&self) -> Result<SharedMetaStore, RouterError> {
        self.meta.clone().ok_or_else(|| {
            RouterError::Invalid("routing is not available (no metadata store)".into())
        })
    }

    /// After a successful write: wait until the local replica applied it, then apply it to
    /// the cache right away (read-your-writes for the API; the watch event is then a no-op).
    async fn committed(
        self: &Arc<Self>,
        meta: &SharedMetaStore,
        key: &str,
        value: Option<Value>,
        rev: u64,
    ) -> Result<(), RouterError> {
        meta.wait_for(rev).await?;
        self.apply(key, value, rev);
        self.reconcile();
        Ok(())
    }

    fn counters(&self, id: &str) -> Arc<Counters> {
        self.stats.lock().entry(id.to_string()).or_default().clone()
    }

    /// Applies one change (idempotent; ignores events older than what the cache holds).
    fn apply(&self, key: &str, value: Option<Value>, rev: u64) {
        let Some(id) = key.strip_prefix(ROUTES_PREFIX) else {
            return;
        };
        let mut st = self.state.write();
        let seen = st
            .routes
            .get(id)
            .map(|(_, r)| *r)
            .max(st.tombstones.get(id).copied())
            .unwrap_or(0);
        if rev <= seen {
            return;
        }
        match value {
            Some(v) => match serde_json::from_value::<Route>(v) {
                Ok(route) => {
                    st.tombstones.remove(id);
                    st.routes.insert(id.to_string(), (route, rev));
                }
                Err(e) => warn!(key, error = %e, "ignoring malformed route"),
            },
            None => {
                st.routes.remove(id);
                st.tombstones.insert(id.to_string(), rev);
                self.stats.lock().remove(id);
            }
        }
        self.rebuild(&mut st);
    }

    /// Full reload from the meta store (start, or after the watch lagged).
    fn reload(&self) {
        let Some(meta) = &self.meta else {
            return;
        };
        let mut routes = BTreeMap::new();
        for (key, v) in meta.list(ROUTES_PREFIX) {
            let id = &key[ROUTES_PREFIX.len()..];
            match serde_json::from_value::<Route>(v.value) {
                Ok(r) => {
                    routes.insert(id.to_string(), (r, v.mod_revision));
                }
                Err(e) => warn!(key, error = %e, "ignoring malformed route"),
            }
        }
        let mut st = self.state.write();
        st.routes = routes;
        st.tombstones.clear();
        self.stats.lock().retain(|id, _| st.routes.contains_key(id));
        self.rebuild(&mut st);
    }

    fn rebuild(&self, st: &mut State) {
        let mut by_source: HashMap<String, Vec<ActiveRoute>> = HashMap::new();
        for (route, _) in st.routes.values() {
            let counters = self.counters(&route.id);
            if route.enabled {
                by_source
                    .entry(route.source.clone())
                    .or_default()
                    .push(ActiveRoute {
                        route: route.clone(),
                        counters,
                    });
            }
        }
        st.by_source = by_source
            .into_iter()
            .map(|(s, mut v)| {
                v.sort_by(|a, b| {
                    (a.route.created_at_ms, &a.route.id).cmp(&(b.route.created_at_ms, &b.route.id))
                });
                (s, Arc::new(v))
            })
            .collect();
    }

    fn routes_for(&self, source: &str) -> Option<Arc<Vec<ActiveRoute>>> {
        self.state.read().by_source.get(source).cloned()
    }

    /// Starts a worker for every source with enabled routes (and an existing channel), stops
    /// the others. The subscription is made synchronously, so messages published after a
    /// route is created are routed.
    fn reconcile(self: &Arc<Self>) {
        if self.shutting_down.load(Ordering::Acquire) {
            return;
        }
        let wanted: Vec<String> = {
            let st = self.state.read();
            st.by_source
                .keys()
                .filter(|s| self.broker.channel(s).is_some())
                .cloned()
                .collect()
        };
        let mut workers = self.workers.lock();
        workers.retain(|src, w| {
            let keep = wanted.contains(src) && !w.task.is_finished();
            if !keep {
                let _ = w.stop.send(true);
                debug!(source = %src, "router worker stopping");
            }
            keep
        });
        for src in wanted {
            if workers.contains_key(&src) {
                continue;
            }
            let info = SubscriberInfo {
                client_name: "fluxmq-router".into(),
                remote_addr: "internal".into(),
                connection_id: 0,
            };
            let sub =
                match self
                    .broker
                    .subscribe(&src, ROUTER_GROUP, info, PREFETCH, StartFrom::Latest)
                {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(source = %src, error = %e, "router worker could not subscribe");
                        continue;
                    }
                };
            let (stop, stop_rx) = watch::channel(false);
            let task = tokio::spawn(run_worker(self.clone(), src.clone(), sub, stop_rx));
            debug!(source = %src, "router worker started");
            workers.insert(src, Worker { stop, task });
        }
    }
}

async fn watch_loop(
    inner: Arc<Inner>,
    mut meta_rx: broadcast::Receiver<MetaEvent>,
    mut events_rx: broadcast::Receiver<BrokerEvent>,
    mut stop: watch::Receiver<bool>,
) {
    let mut meta_open = true;
    let mut events_open = true;
    let mut tick = tokio::time::interval(RECONCILE_EVERY);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            r = meta_rx.recv(), if meta_open => match r {
                Ok(ev) => {
                    if ev.key.starts_with(ROUTES_PREFIX) {
                        inner.apply(&ev.key, ev.value, ev.revision);
                        inner.reconcile();
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(skipped = n, "route watch lagged; reloading");
                    inner.reload();
                    inner.reconcile();
                }
                Err(broadcast::error::RecvError::Closed) => meta_open = false,
            },
            r = events_rx.recv(), if events_open => match r {
                Ok(BrokerEvent::ChannelCreated { .. } | BrokerEvent::ChannelDeleted { .. })
                | Err(broadcast::error::RecvError::Lagged(_)) => inner.reconcile(),
                Ok(_) => {}
                Err(broadcast::error::RecvError::Closed) => events_open = false,
            },
            _ = tick.tick() => inner.reconcile(),
        }
    }
}

async fn run_worker(
    inner: Arc<Inner>,
    source: String,
    mut sub: Subscription,
    mut stop: watch::Receiver<bool>,
) {
    let mut batch: Vec<Delivery> = Vec::with_capacity(PREFETCH as usize);
    loop {
        if *stop.borrow() {
            break;
        }
        let first = tokio::select! {
            _ = stop.changed() => break,
            d = sub.next() => d,
        };
        let Some(first) = first else {
            // Channel deleted or broker shutting down.
            break;
        };
        batch.push(first);
        // Drain whatever is ready right now, without waiting (`next` is cancel-safe).
        while batch.len() < PREFETCH as usize {
            let more = tokio::select! {
                biased;
                d = sub.next() => d,
                () = std::future::ready(()) => None,
            };
            match more {
                Some(d) => batch.push(d),
                None => break,
            }
        }
        process_batch(&inner, &source, &sub, std::mem::take(&mut batch)).await;
    }
    debug!(source = %source, "router worker ended");
}

/// Visited channels: `flux-route-path` (if any) with `source` appended when missing.
fn visited_path<'m>(m: &'m Message, source: &'m str) -> Vec<&'m str> {
    let mut path: Vec<&str> = m
        .headers
        .get(ROUTE_PATH_HEADER)
        .map(|p| {
            p.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    if !path.contains(&source) {
        path.push(source);
    }
    path
}

struct TargetBatch {
    messages: Vec<PublishMessage>,
    /// (delivery index, route index) per message.
    owners: Vec<(usize, usize)>,
}

async fn process_batch(inner: &Inner, source: &str, sub: &Subscription, batch: Vec<Delivery>) {
    let routes = inner.routes_for(source).unwrap_or_default();
    let now = now_ms();
    let mut per_target: BTreeMap<String, TargetBatch> = BTreeMap::new();
    for (di, d) in batch.iter().enumerate() {
        let m = &d.message;
        let visited = visited_path(m, source);
        let ctx = filter::MatchContext::new(m);
        let mut used: Vec<&str> = Vec::new();
        for (ri, r) in routes.iter().enumerate() {
            r.counters.evaluated.fetch_add(1, Ordering::Relaxed);
            if !filter::matches_in(&r.route.conditions, &ctx) {
                continue;
            }
            r.counters.matched.fetch_add(1, Ordering::Relaxed);
            r.counters.last_matched_ms.fetch_max(now, Ordering::Relaxed);
            for t in &r.route.targets {
                if visited.contains(&t.as_str()) || visited.len() > MAX_HOPS {
                    debug!(source, target = %t, route = %r.route.id, "loop protection: skipped");
                    continue;
                }
                if used.contains(&t.as_str()) {
                    continue;
                }
                used.push(t);
                let mut headers = m.headers.clone();
                headers.insert(
                    ROUTED_FROM_HEADER.to_string(),
                    format!("{source}/{}/{}", m.partition, m.offset),
                );
                headers.insert(ROUTE_ID_HEADER.to_string(), r.route.id.clone());
                let mut path = visited.join(",");
                path.push(',');
                path.push_str(t);
                headers.insert(ROUTE_PATH_HEADER.to_string(), path);
                let tb = per_target.entry(t.clone()).or_insert_with(|| TargetBatch {
                    messages: Vec::new(),
                    owners: Vec::new(),
                });
                tb.messages.push(PublishMessage {
                    key: m.key.clone(),
                    headers,
                    payload: m.payload.clone(),
                });
                tb.owners.push((di, ri));
            }
        }
    }

    let mut failed: HashMap<usize, String> = HashMap::new();
    let mut set = tokio::task::JoinSet::new();
    for (target, tb) in per_target {
        let broker = inner.broker.clone();
        set.spawn(async move {
            let r = broker.publish(&target, tb.messages).await;
            (target, tb.owners, r)
        });
    }
    while let Some(joined) = set.join_next().await {
        let Ok((target, owners, result)) = joined else {
            continue; // a panicked publish task: its deliveries time out and are retried
        };
        match result {
            Ok(_) => {
                for (_, ri) in owners {
                    routes[ri]
                        .counters
                        .forwarded
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(e) => {
                debug!(source, target = %target, error = %e, "route forward failed");
                for (di, ri) in owners {
                    routes[ri].counters.failed.fetch_add(1, Ordering::Relaxed);
                    failed
                        .entry(di)
                        .or_insert_with(|| format!("route target unavailable: {target}"));
                }
            }
        }
    }

    let acks: Vec<u64> = batch
        .iter()
        .enumerate()
        .filter(|(di, _)| !failed.contains_key(di))
        .map(|(_, d)| d.delivery_id)
        .collect();
    if !acks.is_empty()
        && let Err(e) = sub.ack(&acks)
    {
        warn!(source, error = %e, "router ack failed");
    }
    for (di, reason) in failed {
        if let Err(e) = sub.nack(batch[di].delivery_id, &reason, true) {
            warn!(source, error = %e, "router nack failed");
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Validation

fn route_key(id: &str) -> String {
    format!("{ROUTES_PREFIX}{id}")
}

/// 10 lowercase base36 characters.
fn new_id() -> String {
    use std::hash::BuildHasher;
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut h = std::collections::hash_map::RandomState::new()
        .hash_one((nanos, COUNTER.fetch_add(1, Ordering::Relaxed)));
    const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    (0..10)
        .map(|_| {
            let c = ALPHABET[(h % 36) as usize] as char;
            h /= 36;
            c
        })
        .collect()
}

fn validate_name(name: &str) -> Result<String, RouterError> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > 128 {
        return Err(RouterError::Invalid("name must be 1-128 characters".into()));
    }
    Ok(name.to_string())
}

fn validate_targets(
    broker: &Broker,
    source: &str,
    targets: Vec<String>,
) -> Result<Vec<String>, RouterError> {
    if targets.is_empty() {
        return Err(RouterError::Invalid(
            "at least one target is required".into(),
        ));
    }
    if targets.len() > MAX_TARGETS {
        return Err(RouterError::Invalid(format!(
            "at most {MAX_TARGETS} targets are allowed"
        )));
    }
    let mut out: Vec<String> = Vec::with_capacity(targets.len());
    for t in targets {
        if t == source {
            return Err(RouterError::Invalid(format!(
                "target '{t}' is the source channel"
            )));
        }
        if fluxmq_proto::model::is_system_channel(&t) {
            return Err(RouterError::Invalid(format!(
                "target '{t}' is a system channel (read-only)"
            )));
        }
        if out.contains(&t) {
            return Err(RouterError::Invalid(format!("duplicate target '{t}'")));
        }
        if broker.channel(&t).is_none() {
            return Err(RouterError::ChannelNotFound(t));
        }
        out.push(t);
    }
    Ok(out)
}

fn is_scalar(v: &Value) -> bool {
    !matches!(v, Value::Array(_) | Value::Object(_))
}

/// Checks and normalizes conditions (drops values of `exists`/`not_exists`). After this,
/// `filter::parse(filter::render(c)) == c`.
pub fn validate_conditions(conditions: Vec<Condition>) -> Result<Vec<Condition>, RouterError> {
    if conditions.len() > MAX_CONDITIONS {
        return Err(RouterError::Invalid(format!(
            "at most {MAX_CONDITIONS} conditions are allowed"
        )));
    }
    let invalid =
        |i: usize, msg: String| RouterError::Invalid(format!("condition {}: {msg}", i + 1));
    conditions
        .into_iter()
        .enumerate()
        .map(|(i, mut c)| {
            match &c.field {
                FieldRef::Header(n) if n.is_empty() => {
                    return Err(invalid(i, "header name is empty".into()));
                }
                FieldRef::Payload(p) if p.is_empty() => {
                    return Err(invalid(i, "payload path is empty".into()));
                }
                _ => {}
            }
            match c.op {
                Op::Exists | Op::NotExists => c.value = None,
                Op::In | Op::NotIn => match &c.value {
                    Some(Value::Array(items))
                        if !items.is_empty()
                            && items.len() <= MAX_LIST_LEN
                            && items.iter().all(is_scalar) => {}
                    _ => {
                        return Err(invalid(
                            i,
                            format!("'in'/'not in' needs a list of 1-{MAX_LIST_LEN} scalar values"),
                        ));
                    }
                },
                Op::Gt | Op::Gte | Op::Lt | Op::Lte => match &c.value {
                    // Strings compare lexically (ISO dates, versions); numbers numerically.
                    Some(Value::Number(_) | Value::String(_)) => {}
                    _ => {
                        return Err(invalid(
                            i,
                            "comparison needs a number (or a string, compared lexically)".into(),
                        ));
                    }
                },
                Op::Eq | Op::Ne | Op::Prefix | Op::Contains => match &c.value {
                    Some(v) if is_scalar(v) => {}
                    Some(_) => return Err(invalid(i, "value must be a scalar".into())),
                    None => return Err(invalid(i, "value is required".into())),
                },
            }
            Ok(c)
        })
        .collect()
}

/// Same conversion as the API's message views (UTF-8 text, otherwise base64).
pub fn message_view(m: Message) -> MessageView {
    let payload = match std::str::from_utf8(&m.payload) {
        Ok(s) => JsonPayload::Utf8(s.to_string()),
        Err(_) => JsonPayload::Base64(base64::engine::general_purpose::STANDARD.encode(&m.payload)),
    };
    MessageView {
        partition: m.partition,
        offset: m.offset,
        key: m.key,
        headers: m.headers,
        timestamp_ms: m.timestamp_ms,
        size: u32::try_from(m.payload.len()).unwrap_or(u32::MAX),
        payload,
    }
}
