//! HTTP push subscriptions (v0.3). Contract and semantics: `fluxmq_proto::webhooks` (read its
//! module docs first). Architecture follows the router (`fluxmq-router`):
//!
//! * Subscriptions persist in the meta store under `webhooks/<id>` (cluster-replicated when
//!   Raft-backed) and are hot-reloaded via `meta.watch()`.
//! * Each enabled subscription whose channel exists runs one worker consuming as group
//!   `$webhook:<name>` (the subscription's `start` applies when the group is new). In a
//!   cluster a node serves only the partitions it leads (the subscription already scopes this).
//! * Any change of a subscription restarts its worker (the old one stops gracefully first:
//!   in-flight requests finish, their verdicts are applied). Disable stops the worker and keeps
//!   the group (paused, committed offsets kept); delete stops it and the API then deletes the
//!   group (on every partition leader in a cluster — see [`WebhookService::uses_group`]).
//! * A periodic reconcile (5 s) restarts workers that ended (e.g. channel re-created).
//! * Delivery details (outcome mapping, Retry-After hold, strict ordering) live in
//!   [`worker`] and [`deliver`]; the SSRF guard in [`ssrf`].

pub mod deliver;
pub mod signature;
pub mod ssrf;
pub mod stats;
pub mod validate;
mod worker;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use fluxmq_core::{Broker, BrokerEvent, SubscriberInfo};
use fluxmq_meta::{MetaError, MetaEvent, Precondition, SharedMetaStore};
use fluxmq_proto::now_ms;
use fluxmq_proto::webhooks::{
    CreateWebhookRequest, UpdateWebhookRequest, WebhookStats, WebhookSubscription,
    WebhookTestResult,
};
use parking_lot::{Mutex, RwLock};
use serde_json::Value;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

pub use signature::{sign, verify};
pub use ssrf::PrivatePolicy;
pub use stats::{NodeHistory, NodeWebhookStats};

/// Meta store key prefix.
pub const WEBHOOKS_PREFIX: &str = "webhooks/";
/// Consumer group prefix (`$webhook:<name>`).
pub const GROUP_PREFIX: &str = "$webhook:";
/// Node-local stats of every subscription (cluster fan-out, cluster secret).
pub const INTERNAL_STATS_PATH: &str = "/internal/v1/webhooks/stats";
/// Node-local recent attempts / request history of one subscription (`{id}` path segment).
pub const INTERNAL_ATTEMPTS_PATH: &str = "/internal/v1/webhooks/{id}/attempts";
pub const INTERNAL_HISTORY_PATH: &str = "/internal/v1/webhooks/{id}/history";
const RECONCILE_EVERY: Duration = Duration::from_secs(5);

/// The consumer group of subscription `name`.
pub fn group_name(name: &str) -> String {
    format!("{GROUP_PREFIX}{name}")
}

#[derive(Debug, thiserror::Error)]
pub enum WebhookError {
    #[error("webhook not found")]
    NotFound,
    #[error("invalid webhook: {0}")]
    Invalid(String),
    #[error("channel '{0}' not found")]
    ChannelNotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error(transparent)]
    Meta(#[from] MetaError),
}

#[derive(Clone, Debug)]
pub struct WebhookConfig {
    pub private: PrivatePolicy,
}

impl Default for WebhookConfig {
    /// Private targets allowed (dev/tests). The server passes the flag / auth-mode policy.
    fn default() -> Self {
        Self {
            private: PrivatePolicy::Fixed(true),
        }
    }
}

/// Cheap to clone.
#[derive(Clone)]
pub struct WebhookService {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct State {
    /// id → (subscription, mod_revision).
    subs: BTreeMap<String, (WebhookSubscription, u64)>,
    /// id → revision of the delete (ignores stale watch events for deleted subscriptions).
    tombstones: HashMap<String, u64>,
}

struct Worker {
    /// Revision of the subscription the worker runs with.
    rev: u64,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

struct Inner {
    broker: Broker,
    meta: Option<SharedMetaStore>,
    config: WebhookConfig,
    client: reqwest::Client,
    state: RwLock<State>,
    stats: Mutex<HashMap<String, Arc<stats::Counters>>>,
    workers: Mutex<HashMap<String, Worker>>,
    shutting_down: AtomicBool,
    stop: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl WebhookService {
    /// Loads subscriptions and starts workers. Must be called inside a Tokio runtime.
    pub async fn start(
        broker: Broker,
        meta: SharedMetaStore,
        config: WebhookConfig,
    ) -> Result<Self, WebhookError> {
        let svc = Self::build(broker, Some(meta.clone()), config);
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
            webhooks = svc.inner.state.read().subs.len(),
            "webhooks started"
        );
        Ok(svc)
    }

    /// No subscriptions, no persistence (defaults for tests / `AppState::new`).
    pub fn disabled(broker: Broker) -> Self {
        Self::build(broker, None, WebhookConfig::default())
    }

    fn build(broker: Broker, meta: Option<SharedMetaStore>, config: WebhookConfig) -> Self {
        let client = deliver::client(config.private.clone());
        Self {
            inner: Arc::new(Inner {
                broker,
                meta,
                client,
                config,
                state: RwLock::new(State::default()),
                stats: Mutex::new(HashMap::new()),
                workers: Mutex::new(HashMap::new()),
                shutting_down: AtomicBool::new(false),
                stop: watch::Sender::new(false),
                tasks: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Redacted, ordered by creation time.
    pub fn list(&self) -> Vec<WebhookSubscription> {
        let st = self.inner.state.read();
        let mut v: Vec<WebhookSubscription> = st
            .subs
            .values()
            .map(|(s, _)| validate::redact(s.clone()))
            .collect();
        v.sort_by(|a, b| (a.created_at_ms, &a.id).cmp(&(b.created_at_ms, &b.id)));
        v
    }

    /// Redacted.
    pub fn get(&self, id: &str) -> Option<WebhookSubscription> {
        self.full(id).map(validate::redact)
    }

    fn full(&self, id: &str) -> Option<WebhookSubscription> {
        self.inner.state.read().subs.get(id).map(|(s, _)| s.clone())
    }

    /// Validates (incl. the SSRF check) and persists; returns the redacted subscription.
    pub async fn create(
        &self,
        req: CreateWebhookRequest,
    ) -> Result<WebhookSubscription, WebhookError> {
        let meta = self.inner.meta()?;
        let mut sub = validate::from_create(req)?;
        validate::validate(&self.inner.broker, &self.inner.config.private, &sub).await?;
        if self.name_taken(&sub.name, None) {
            return Err(WebhookError::Conflict(format!(
                "a webhook named '{}' already exists",
                sub.name
            )));
        }
        for _ in 0..8 {
            sub.id = new_id();
            let key = key(&sub.id);
            let value = serde_json::to_value(&sub).map_err(MetaError::from)?;
            let pre = Precondition {
                key: key.clone(),
                expected_mod_revision: 0,
            };
            match meta.put(&key, value.clone(), Some(pre)).await {
                Ok(rev) => {
                    self.inner.committed(&meta, &key, Some(value), rev).await?;
                    return Ok(validate::redact(sub));
                }
                Err(MetaError::Conflict { .. }) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(WebhookError::Invalid(
            "could not allocate a webhook id".into(),
        ))
    }

    pub async fn update(
        &self,
        id: &str,
        req: UpdateWebhookRequest,
    ) -> Result<WebhookSubscription, WebhookError> {
        let meta = self.inner.meta()?;
        let key = key(id);
        let current = meta.get(&key).ok_or(WebhookError::NotFound)?;
        let sub: WebhookSubscription =
            serde_json::from_value(current.value).map_err(MetaError::from)?;
        let sub = validate::apply_update(sub, req)?;
        validate::validate(&self.inner.broker, &self.inner.config.private, &sub).await?;
        let value = serde_json::to_value(&sub).map_err(MetaError::from)?;
        let pre = Precondition {
            key: key.clone(),
            expected_mod_revision: current.mod_revision,
        };
        let rev = meta.put(&key, value.clone(), Some(pre)).await?;
        self.inner.committed(&meta, &key, Some(value), rev).await?;
        Ok(validate::redact(sub))
    }

    /// Deletes the definition and stops its worker; returns the deleted (redacted)
    /// subscription so the caller can release its group (`$webhook:<name>` on `channel`).
    pub async fn delete(&self, id: &str) -> Result<WebhookSubscription, WebhookError> {
        let meta = self.inner.meta()?;
        let key = key(id);
        let current = meta.get(&key).ok_or(WebhookError::NotFound)?;
        let sub: WebhookSubscription =
            serde_json::from_value(current.value).map_err(MetaError::from)?;
        let pre = Precondition {
            key: key.clone(),
            expected_mod_revision: current.mod_revision,
        };
        let rev = meta.delete(&key, Some(pre)).await?;
        self.inner.committed(&meta, &key, None, rev).await?;
        Ok(validate::redact(sub))
    }

    /// Sends one synthetic envelope to a stored subscription, or — with a `draft` — to the
    /// stored subscription with the draft merged over it (secret rule of updates; validated
    /// like an update, nothing persisted).
    pub async fn test(
        &self,
        id: &str,
        draft: Option<UpdateWebhookRequest>,
    ) -> Result<WebhookTestResult, WebhookError> {
        let sub = self.full(id).ok_or(WebhookError::NotFound)?;
        let sub = match draft {
            None => sub,
            Some(req) => {
                let sub = validate::apply_update(sub, req)?;
                validate::validate(&self.inner.broker, &self.inner.config.private, &sub).await?;
                sub
            }
        };
        Ok(self.send_test(&sub).await)
    }

    /// Test before saving: validates the request like `create` (except name uniqueness) and
    /// sends one synthetic envelope. Nothing is persisted.
    pub async fn test_request(
        &self,
        req: CreateWebhookRequest,
    ) -> Result<WebhookTestResult, WebhookError> {
        let sub = validate::from_create(req)?;
        validate::validate(&self.inner.broker, &self.inner.config.private, &sub).await?;
        Ok(self.send_test(&sub).await)
    }

    async fn send_test(&self, sub: &WebhookSubscription) -> WebhookTestResult {
        let (body, headers) = deliver::synthetic(
            sub,
            &format!("test/{}", now_ms()),
            "FluxMQ webhook test delivery",
        );
        let r = deliver::send(
            &self.inner.client,
            sub,
            &self.inner.config.private,
            body,
            headers,
            true,
        )
        .await;
        let ok = matches!(deliver::classify(&r, false), deliver::Verdict::Ack);
        WebhookTestResult {
            ok,
            status: r.status,
            latency_ms: r.latency.as_millis() as u64,
            error: match (&r.error, r.status) {
                (Some(e), _) => Some(e.reason().to_string()),
                (None, Some(s)) if !ok => Some(format!("HTTP {s}")),
                _ => None,
            },
            error_detail: match (&r.error, r.status, &r.snippet) {
                (Some(e), _, _) => Some(e.detail()),
                (None, Some(s), Some(body)) if !ok && !body.trim().is_empty() => {
                    Some(format!("HTTP {s}: {}", body.trim()))
                }
                _ => None,
            },
            response_snippet: r.snippet.filter(|s| !s.is_empty()),
        }
    }

    /// This node's raw stats (mergeable; one entry per known subscription).
    pub fn local_stats(&self) -> Vec<NodeWebhookStats> {
        let ids = self.ids();
        let stats = self.inner.stats.lock();
        ids.iter()
            .map(|id| match stats.get(id) {
                Some(c) => c.raw(id),
                None => stats::Counters::default().raw(id),
            })
            .collect()
    }

    /// Node-local stats as the API shape (single node = everything).
    pub fn stats(&self) -> Vec<WebhookStats> {
        stats::merge(&self.ids(), self.local_stats())
    }

    /// Sums per-node stats (cluster fan-out) over the subscriptions known here.
    pub fn merge_stats(&self, nodes: Vec<NodeWebhookStats>) -> Vec<WebhookStats> {
        stats::merge(&self.ids(), nodes)
    }

    /// This node's recent requests of `id` (newest first); `None` for an unknown id.
    pub fn local_attempts(&self, id: &str) -> Option<Vec<fluxmq_proto::webhooks::WebhookAttempt>> {
        self.full(id)?;
        let node = self.inner.broker.node_id().to_string();
        let c = self.inner.stats.lock().get(id).cloned();
        Some(
            c.map(|c| c.recent_attempts())
                .unwrap_or_default()
                .into_iter()
                .map(|mut a| {
                    a.node.clone_from(&node);
                    a
                })
                .collect(),
        )
    }

    /// This node's request history of `id` over `range` (`15m`, `1h`, `24h`); `None` for an
    /// unknown id or range.
    pub fn local_history(&self, id: &str, range: &str) -> Option<NodeHistory> {
        self.full(id)?;
        let c = self.inner.stats.lock().get(id).cloned();
        let h = c.as_ref().map(|c| c.history.lock().slots(range, now_ms()));
        let (bucket_ms, slots) = match h {
            Some(r) => r?,
            None => stats::History::default().slots(range, now_ms())?,
        };
        Some(NodeHistory { bucket_ms, slots })
    }

    /// Subscription ids, ordered by creation time.
    pub fn ids(&self) -> Vec<String> {
        self.list().into_iter().map(|s| s.id).collect()
    }

    /// Whether `group` on `channel` belongs to a stored (enabled or disabled) subscription —
    /// such a group must not be deleted through the groups API.
    pub fn uses_group(&self, channel: &str, group: &str) -> bool {
        let Some(name) = group.strip_prefix(GROUP_PREFIX) else {
            return false;
        };
        self.inner
            .state
            .read()
            .subs
            .values()
            .any(|(s, _)| s.name == name && s.channel == channel)
    }

    fn name_taken(&self, name: &str, except: Option<&str>) -> bool {
        self.inner
            .state
            .read()
            .subs
            .values()
            .any(|(s, _)| s.name == name && Some(s.id.as_str()) != except)
    }

    /// Ids of subscriptions with a running worker (tests, diagnostics).
    pub fn active(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .inner
            .workers
            .lock()
            .iter()
            .filter(|(_, w)| !w.task.is_finished())
            .map(|(id, _)| id.clone())
            .collect();
        v.sort();
        v
    }

    /// Stops the watcher and all workers; waits (up to `budget`) for in-flight requests to
    /// finish and their verdicts to be applied. Unresolved deliveries return to their groups.
    pub async fn shutdown(&self, budget: Duration) {
        if self.inner.shutting_down.swap(true, Ordering::AcqRel) {
            return;
        }
        let _ = self.inner.stop.send(true);
        let workers: Vec<Worker> = self.inner.workers.lock().drain().map(|(_, w)| w).collect();
        for w in &workers {
            let _ = w.stop.send(true);
        }
        let deadline = Instant::now() + budget;
        for w in workers {
            let left = deadline.saturating_duration_since(Instant::now());
            let abort = w.task.abort_handle();
            if tokio::time::timeout(left, w.task).await.is_err() {
                abort.abort();
            }
        }
        for t in self.inner.tasks.lock().drain(..) {
            t.abort();
        }
        info!("webhooks stopped");
    }
}

impl Inner {
    fn meta(&self) -> Result<SharedMetaStore, WebhookError> {
        self.meta.clone().ok_or_else(|| {
            WebhookError::Invalid("webhooks are not available (no metadata store)".into())
        })
    }

    async fn committed(
        self: &Arc<Self>,
        meta: &SharedMetaStore,
        key: &str,
        value: Option<Value>,
        rev: u64,
    ) -> Result<(), WebhookError> {
        meta.wait_for(rev).await?;
        self.apply(key, value, rev);
        self.reconcile();
        Ok(())
    }

    fn counters(&self, id: &str) -> Arc<stats::Counters> {
        self.stats.lock().entry(id.to_string()).or_default().clone()
    }

    fn apply(&self, key: &str, value: Option<Value>, rev: u64) {
        let Some(id) = key.strip_prefix(WEBHOOKS_PREFIX) else {
            return;
        };
        let mut st = self.state.write();
        let seen = st
            .subs
            .get(id)
            .map(|(_, r)| *r)
            .max(st.tombstones.get(id).copied())
            .unwrap_or(0);
        if rev <= seen {
            return;
        }
        match value {
            Some(v) => match serde_json::from_value::<WebhookSubscription>(v) {
                Ok(s) => {
                    st.tombstones.remove(id);
                    st.subs.insert(id.to_string(), (s, rev));
                }
                Err(e) => warn!(key, error = %e, "ignoring malformed webhook"),
            },
            None => {
                st.subs.remove(id);
                st.tombstones.insert(id.to_string(), rev);
                self.stats.lock().remove(id);
            }
        }
    }

    fn reload(&self) {
        let Some(meta) = &self.meta else {
            return;
        };
        let mut subs = BTreeMap::new();
        for (key, v) in meta.list(WEBHOOKS_PREFIX) {
            let id = &key[WEBHOOKS_PREFIX.len()..];
            match serde_json::from_value::<WebhookSubscription>(v.value) {
                Ok(s) => {
                    subs.insert(id.to_string(), (s, v.mod_revision));
                }
                Err(e) => warn!(key, error = %e, "ignoring malformed webhook"),
            }
        }
        let mut st = self.state.write();
        st.subs = subs;
        st.tombstones.clear();
        self.stats.lock().retain(|id, _| st.subs.contains_key(id));
    }

    /// Runs a worker for every enabled subscription whose channel exists (restarting workers
    /// whose definition changed), stops the others. A first subscription is made
    /// synchronously, so messages published right after a create are delivered.
    fn reconcile(self: &Arc<Self>) {
        if self.shutting_down.load(Ordering::Acquire) {
            return;
        }
        let wanted: Vec<(WebhookSubscription, u64)> = {
            let st = self.state.read();
            st.subs
                .values()
                .filter(|(s, _)| s.enabled && self.broker.channel(&s.channel).is_some())
                .cloned()
                .collect()
        };
        let mut workers = self.workers.lock();
        let mut previous: HashMap<String, JoinHandle<()>> = HashMap::new();
        let ids: Vec<String> = workers.keys().cloned().collect();
        for id in ids {
            let w = &workers[&id];
            let keep =
                wanted.iter().any(|(s, rev)| s.id == id && *rev == w.rev) && !w.task.is_finished();
            if !keep {
                let w = workers.remove(&id).expect("listed above");
                let _ = w.stop.send(true);
                debug!(webhook = %id, "webhook worker stopping");
                previous.insert(id, w.task);
            }
        }
        for (sub, rev) in wanted {
            if workers.contains_key(&sub.id) {
                continue;
            }
            let id = sub.id.clone();
            let (stop, stop_rx) = watch::channel(false);
            let ctx = worker::Ctx {
                sub: Arc::new(sub),
                client: self.client.clone(),
                policy: self.config.private.clone(),
                counters: self.counters(&id),
                broker: self.broker.clone(),
            };
            let task = match previous.remove(&id) {
                // Restart: subscribe once the old worker has let go (the group exists).
                Some(old) => {
                    let inner = self.clone();
                    tokio::spawn(async move {
                        let _ = old.await;
                        if inner.shutting_down.load(Ordering::Acquire) || *stop_rx.borrow() {
                            return;
                        }
                        if let Some(s) = inner.subscribe(&ctx.sub) {
                            worker::run(ctx, s, stop_rx).await;
                        }
                    })
                }
                None => match self.subscribe(&ctx.sub) {
                    Some(s) => tokio::spawn(worker::run(ctx, s, stop_rx)),
                    None => continue,
                },
            };
            debug!(webhook = %id, "webhook worker started");
            workers.insert(id, Worker { rev, stop, task });
        }
    }

    fn subscribe(&self, sub: &WebhookSubscription) -> Option<fluxmq_core::Subscription> {
        let info = SubscriberInfo {
            client_name: "fluxmq-webhooks".into(),
            // Scheme + authority only (paths/queries may carry tokens).
            remote_addr: reqwest::Url::parse(&sub.url)
                .map(|u| u.origin().ascii_serialization())
                .unwrap_or_default(),
            connection_id: 0,
        };
        let prefetch = sub.max_in_flight.max(1) * sub.batch_max.max(1);
        match self.broker.subscribe(
            &sub.channel,
            &group_name(&sub.name),
            info,
            prefetch,
            sub.start,
        ) {
            Ok(s) => Some(s),
            Err(e) => {
                warn!(webhook = %sub.name, error = %e, "webhook worker could not subscribe");
                None
            }
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
                    if ev.key.starts_with(WEBHOOKS_PREFIX) {
                        inner.apply(&ev.key, ev.value, ev.revision);
                        inner.reconcile();
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(skipped = n, "webhook watch lagged; reloading");
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

fn key(id: &str) -> String {
    format!("{WEBHOOKS_PREFIX}{id}")
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
