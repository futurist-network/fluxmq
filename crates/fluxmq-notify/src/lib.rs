//! Notifications (v0.3): alerts and broker events → Telegram, Slack, Discord, email,
//! PagerDuty and generic webhooks, plus the `$system.events` pull feed.
//! Contract and semantics: `fluxmq_proto::notify` (read its module docs first).
//!
//! * Notifiers and rules persist in the meta store (`notify/notifiers/<id>`,
//!   `notify/rules/<id>`) and are hot-reloaded via `meta.watch()` — identical on every node.
//! * Input: [`fluxmq_telemetry::TelemetryHub::notifications`] (alert transitions + events).
//! * Only the controller dispatches and appends to the feed (single node: always) — see
//!   [`dispatch`] for grouping / repeat / resolved semantics and leadership hand-over.
//! * Senders ([`senders`]) retry with backoff (3 attempts); every delivery outcome is a
//!   [`NotificationRecord`] persisted in the replicated system channel
//!   `$system.notifications` (v0.5, see [`history`]: controller appends, 7 days / newest
//!   10 000 records, survives failover and restart), plus an in-memory ring of the last
//!   [`HISTORY_CAPACITY`] records on the node that produced them.
//! * Secrets are redacted on every read path ([`NotifyService::notifiers`] etc. return
//!   redacted copies); see [`validate`] for the update rule. Secrets are never logged.

pub mod config;
mod dispatch;
pub mod feed;
pub mod history;
pub mod matcher;
pub mod render;
pub mod senders;
pub mod validate;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use fluxmq_cluster::Cluster;
use fluxmq_core::Broker;
use fluxmq_meta::{MetaError, MetaEvent, Precondition, SharedMetaStore};
use fluxmq_proto::notify::{
    CreateNotifierRequest, CreateRuleRequest, DeliveryStatus, FeedPage, NotificationPayload,
    NotificationRecord, NotificationRule, Notifier, TestResult, UpdateNotifierRequest,
    UpdateRuleRequest,
};
use fluxmq_proto::now_ms;
use fluxmq_proto::telemetry::AlertsState;
use fluxmq_telemetry::{TelemetryHub, TelemetrySignal};
use parking_lot::{Mutex, RwLock};
use serde_json::Value;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub use config::{Captured, Endpoints, NotifyConfig};
pub use feed::FeedAfter;

pub const NOTIFIERS_PREFIX: &str = "notify/notifiers/";
pub const RULES_PREFIX: &str = "notify/rules/";
pub const HISTORY_CAPACITY: usize = 1000;
pub const MAX_NOTIFIERS: usize = 100;
pub const MAX_RULES: usize = 200;
/// Start waits at most this long for each system channel to be created.
const START_ENSURE: Duration = Duration::from_secs(3);

#[derive(Debug, thiserror::Error)]
pub enum NotifyError {
    #[error("{0} not found")]
    NotFound(String),
    #[error("invalid: {0}")]
    Invalid(String),
    /// Notifier still referenced by rules (names listed).
    #[error("notifier is used by rule(s): {0}")]
    InUse(String),
    #[error("notifications are not available (no metadata store)")]
    Unavailable,
    #[error(transparent)]
    Meta(#[from] MetaError),
    #[error(transparent)]
    Core(#[from] fluxmq_core::CoreError),
    #[error("internal: {0}")]
    Internal(String),
}

/// "Should this node dispatch?" plus identity used in payloads.
pub trait Leadership: Send + Sync + 'static {
    fn is_controller(&self) -> bool;
    fn cluster_name(&self) -> String;
    fn node_id(&self) -> String;
}

/// [`Leadership`] of a [`Cluster`] handle: a single node always dispatches; a cluster node
/// when it is the Raft leader (controller).
pub struct ClusterLeadership(pub Cluster);

/// Channel filter that matches nothing → `metadata` returns only the topology.
const NO_CHANNELS: &str = "\0";

impl Leadership for ClusterLeadership {
    fn is_controller(&self) -> bool {
        if !self.0.is_clustered() {
            return true;
        }
        self.0
            .metadata(&[NO_CHANNELS.to_string()])
            .controller
            .as_deref()
            == Some(self.0.node_id())
    }

    fn cluster_name(&self) -> String {
        self.0.metadata(&[NO_CHANNELS.to_string()]).cluster_name
    }

    fn node_id(&self) -> String {
        self.0.node_id().to_string()
    }
}

/// Explicit wiring (tests and embeddings); [`NotifyService::start`] builds it from the
/// broker, telemetry hub and cluster.
pub struct Wiring {
    pub meta: SharedMetaStore,
    pub signals: broadcast::Receiver<TelemetrySignal>,
    /// Currently open alerts (latest text for repeats; leadership seeding).
    pub open_alerts: Arc<dyn Fn() -> AlertsState + Send + Sync>,
    pub leadership: Arc<dyn Leadership>,
    /// Re-check leadership when it changes.
    pub epoch: Option<watch::Receiver<u64>>,
    /// Feed storage (`None` = no feed).
    pub broker: Option<Broker>,
    /// Feed writes go through the cluster (routed to the partition leader) when set.
    pub cluster: Option<Cluster>,
}

/// Cheap to clone.
#[derive(Clone)]
pub struct NotifyService {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct State {
    notifiers: BTreeMap<String, (Notifier, u64)>,
    rules: BTreeMap<String, (NotificationRule, u64)>,
    tombstones: HashMap<String, u64>,
}

pub(crate) struct Inner {
    meta: Option<SharedMetaStore>,
    state: RwLock<State>,
    rules_cache: RwLock<Arc<Vec<NotificationRule>>>,
    history: Mutex<VecDeque<NotificationRecord>>,
    /// Persistent history writer (`None` without a broker).
    history_tx: Mutex<Option<mpsc::UnboundedSender<NotificationRecord>>>,
    pub(crate) senders: senders::Senders,
    pub(crate) leadership: Arc<dyn Leadership>,
    pub(crate) open_alerts: Arc<dyn Fn() -> AlertsState + Send + Sync>,
    pub(crate) broker: Option<Broker>,
    pub(crate) cluster: Option<Cluster>,
    feed_appended: watch::Sender<u64>,
    stop: watch::Sender<bool>,
    shutting_down: AtomicBool,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl NotifyService {
    /// Loads notifiers/rules, creates `$system.events` when missing (controller), starts the
    /// dispatcher and the feed writer. Must be called inside a Tokio runtime.
    pub async fn start(
        broker: Broker,
        meta: SharedMetaStore,
        telemetry: &TelemetryHub,
        cluster: Cluster,
        config: NotifyConfig,
    ) -> Result<Self, NotifyError> {
        let hub = telemetry.clone();
        let wiring = Wiring {
            meta,
            signals: telemetry.notifications(),
            open_alerts: Arc::new(move || hub.alerts()),
            leadership: Arc::new(ClusterLeadership(cluster.clone())),
            epoch: cluster.is_clustered().then(|| cluster.watch_epoch()),
            broker: Some(broker),
            cluster: Some(cluster),
        };
        Self::start_with(wiring, config).await
    }

    /// [`NotifyService::start`] with explicit wiring.
    pub async fn start_with(w: Wiring, config: NotifyConfig) -> Result<Self, NotifyError> {
        let feed_enabled = config.feed && w.broker.is_some();
        let svc = Self::build(
            Some(w.meta.clone()),
            config,
            w.leadership,
            w.open_alerts,
            w.broker,
            w.cluster,
        );
        let inner = svc.inner.clone();
        let meta_rx = w.meta.watch();
        inner.reload();
        // System channels: created here when possible (instantly on a single node), but never
        // blocking start for long — a node restarting as a stale Raft leader (openraft restores
        // its own committed vote) can't commit the creation; the writers' `ensure` ticks retry.
        if feed_enabled {
            match tokio::time::timeout(START_ENSURE, feed::ensure_channel(&inner)).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    warn!(error = %e, "could not create the system events channel yet; retrying")
                }
                Err(_) => warn!(
                    "creating the system events channel is taking long; retrying in the background"
                ),
            }
        }
        if inner.broker.is_some() {
            match tokio::time::timeout(START_ENSURE, history::ensure_channel(&inner)).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    warn!(error = %e, "could not create the notification history channel yet; retrying")
                }
                Err(_) => warn!(
                    "creating the notification history channel is taking long; retrying in the background"
                ),
            }
        }
        let feed_tx = if feed_enabled {
            let (tx, rx) = mpsc::unbounded_channel();
            let h = tokio::spawn(feed::run_writer(
                inner.clone(),
                rx,
                inner.feed_appended.clone(),
                inner.stop.subscribe(),
            ));
            inner.tasks.lock().push(h);
            Some(tx)
        } else {
            None
        };
        if inner.broker.is_some() {
            let (tx, rx) = mpsc::unbounded_channel();
            *inner.history_tx.lock() = Some(tx);
            let h = tokio::spawn(history::run_writer(
                inner.clone(),
                rx,
                inner.stop.subscribe(),
            ));
            inner.tasks.lock().push(h);
        }
        let d = dispatch::Dispatcher::new(inner.clone(), feed_tx);
        let h = tokio::spawn(d.run(w.signals, w.epoch, inner.stop.subscribe()));
        inner.tasks.lock().push(h);
        let h = tokio::spawn(watch_loop(inner.clone(), meta_rx, inner.stop.subscribe()));
        inner.tasks.lock().push(h);
        {
            let st = inner.state.read();
            info!(
                notifiers = st.notifiers.len(),
                rules = st.rules.len(),
                "notifications started"
            );
        }
        Ok(svc)
    }

    /// No persistence, no dispatch, no feed (defaults for tests / `AppState::new`).
    pub fn disabled() -> Self {
        struct Never;
        impl Leadership for Never {
            fn is_controller(&self) -> bool {
                false
            }
            fn cluster_name(&self) -> String {
                "local".into()
            }
            fn node_id(&self) -> String {
                "local".into()
            }
        }
        Self::build(
            None,
            NotifyConfig::default(),
            Arc::new(Never),
            Arc::new(AlertsState::new),
            None,
            None,
        )
    }

    fn build(
        meta: Option<SharedMetaStore>,
        config: NotifyConfig,
        leadership: Arc<dyn Leadership>,
        open_alerts: Arc<dyn Fn() -> AlertsState + Send + Sync>,
        broker: Option<Broker>,
        cluster: Option<Cluster>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                meta,
                state: RwLock::new(State::default()),
                rules_cache: RwLock::new(Arc::new(Vec::new())),
                history: Mutex::new(VecDeque::new()),
                history_tx: Mutex::new(None),
                senders: senders::Senders::new(config),
                leadership,
                open_alerts,
                broker,
                cluster,
                feed_appended: watch::Sender::new(0),
                stop: watch::Sender::new(false),
                shutting_down: AtomicBool::new(false),
                tasks: Mutex::new(Vec::new()),
            }),
        }
    }

    // -----------------------------------------------------------------------------------
    // Notifiers
    // -----------------------------------------------------------------------------------

    /// Redacted, ordered by creation time.
    pub fn notifiers(&self) -> Vec<Notifier> {
        let st = self.inner.state.read();
        let mut v: Vec<Notifier> = st
            .notifiers
            .values()
            .map(|(n, _)| validate::redact(n))
            .collect();
        v.sort_by(|a, b| (a.created_at_ms, &a.id).cmp(&(b.created_at_ms, &b.id)));
        v
    }

    /// Redacted.
    pub fn notifier(&self, id: &str) -> Option<Notifier> {
        self.inner.notifier(id).map(|n| validate::redact(&n))
    }

    /// Returns the redacted notifier.
    pub async fn create_notifier(
        &self,
        req: CreateNotifierRequest,
    ) -> Result<Notifier, NotifyError> {
        let meta = self.inner.meta()?;
        if self.inner.state.read().notifiers.len() >= MAX_NOTIFIERS {
            return Err(NotifyError::Invalid(format!(
                "at most {MAX_NOTIFIERS} notifiers"
            )));
        }
        let name = validate::name(&req.name)?;
        let kind = validate::kind(validate::merge_secrets(req.kind, None)?)?;
        let now = now_ms();
        let mut n = Notifier {
            id: String::new(),
            name,
            enabled: req.enabled,
            kind,
            created_at_ms: now,
            updated_at_ms: now,
        };
        n.id = self
            .inner
            .insert_new(&meta, NOTIFIERS_PREFIX, |id| {
                n.id = id.to_string();
                serde_json::to_value(&n)
            })
            .await?;
        Ok(validate::redact(&n))
    }

    pub async fn update_notifier(
        &self,
        id: &str,
        req: UpdateNotifierRequest,
    ) -> Result<Notifier, NotifyError> {
        let meta = self.inner.meta()?;
        let key = format!("{NOTIFIERS_PREFIX}{id}");
        let current = meta
            .get(&key)
            .ok_or_else(|| NotifyError::NotFound(format!("notifier '{id}'")))?;
        let mut n: Notifier = serde_json::from_value(current.value).map_err(MetaError::from)?;
        if let Some(name) = req.name {
            n.name = validate::name(&name)?;
        }
        if let Some(enabled) = req.enabled {
            n.enabled = enabled;
        }
        if let Some(kind) = req.kind {
            n.kind = validate::kind(validate::merge_secrets(kind, Some(&n.kind))?)?;
        }
        n.updated_at_ms = now_ms().max(n.created_at_ms);
        let value = serde_json::to_value(&n).map_err(MetaError::from)?;
        self.inner
            .put_checked(&meta, &key, value, current.mod_revision)
            .await?;
        Ok(validate::redact(&n))
    }

    /// 409 ([`NotifyError::InUse`]) while a rule references it.
    pub async fn delete_notifier(&self, id: &str) -> Result<(), NotifyError> {
        let meta = self.inner.meta()?;
        let users: Vec<String> = self
            .inner
            .rules_snapshot()
            .iter()
            .filter(|r| r.notifier_ids.iter().any(|n| n == id))
            .map(|r| r.name.clone())
            .collect();
        if !users.is_empty() {
            return Err(NotifyError::InUse(users.join(", ")));
        }
        self.inner
            .delete_checked(&meta, &format!("{NOTIFIERS_PREFIX}{id}"), "notifier", id)
            .await
    }

    /// Sends a test message to a stored notifier (even when disabled), synchronously,
    /// one attempt. Recorded in the history. With a `draft` the edit is merged over the
    /// stored notifier first (secret rule of updates: `REDACTED` = stored secret) and
    /// validated; nothing is persisted.
    pub async fn test(
        &self,
        id: &str,
        draft: Option<UpdateNotifierRequest>,
    ) -> Result<TestResult, NotifyError> {
        let n = self
            .inner
            .notifier(id)
            .ok_or_else(|| NotifyError::NotFound(format!("notifier '{id}'")))?;
        let kind = match draft {
            None => n.kind,
            Some(req) => {
                if let Some(name) = &req.name {
                    validate::name(name)?;
                }
                match req.kind {
                    Some(kind) => validate::kind(validate::merge_secrets(kind, Some(&n.kind))?)?,
                    None => n.kind,
                }
            }
        };
        Ok(self.inner.test(&n.id, &kind).await)
    }

    /// Test-before-save: validates `req` and sends a test message without persisting.
    /// [`fluxmq_proto::notify::REDACTED`] is not accepted here (nothing stored to keep).
    pub async fn test_unsaved(
        &self,
        req: CreateNotifierRequest,
    ) -> Result<TestResult, NotifyError> {
        validate::name(&req.name)?;
        let kind = validate::kind(validate::merge_secrets(req.kind, None)?)?;
        Ok(self.inner.test("(unsaved)", &kind).await)
    }

    // -----------------------------------------------------------------------------------
    // Rules
    // -----------------------------------------------------------------------------------

    pub fn rules(&self) -> Vec<NotificationRule> {
        self.inner.rules_snapshot().as_ref().clone()
    }

    pub fn rule(&self, id: &str) -> Option<NotificationRule> {
        self.inner
            .state
            .read()
            .rules
            .get(id)
            .map(|(r, _)| r.clone())
    }

    pub async fn create_rule(
        &self,
        req: CreateRuleRequest,
    ) -> Result<NotificationRule, NotifyError> {
        let meta = self.inner.meta()?;
        if self.inner.state.read().rules.len() >= MAX_RULES {
            return Err(NotifyError::Invalid(format!("at most {MAX_RULES} rules")));
        }
        let now = now_ms();
        let mut rule = NotificationRule {
            id: String::new(),
            name: req.name,
            enabled: req.enabled,
            notifier_ids: req.notifier_ids,
            matcher: req.matcher,
            group_wait_secs: req
                .group_wait_secs
                .unwrap_or(validate::DEFAULT_GROUP_WAIT_SECS),
            repeat_interval_secs: req
                .repeat_interval_secs
                .unwrap_or(validate::DEFAULT_REPEAT_SECS),
            send_resolved: req.send_resolved.unwrap_or(true),
            created_at_ms: now,
            updated_at_ms: now,
        };
        validate::rule(&mut rule)?;
        self.inner.check_notifiers(&rule.notifier_ids)?;
        rule.id = self
            .inner
            .insert_new(&meta, RULES_PREFIX, |id| {
                rule.id = id.to_string();
                serde_json::to_value(&rule)
            })
            .await?;
        Ok(rule)
    }

    pub async fn update_rule(
        &self,
        id: &str,
        req: UpdateRuleRequest,
    ) -> Result<NotificationRule, NotifyError> {
        let meta = self.inner.meta()?;
        let key = format!("{RULES_PREFIX}{id}");
        let current = meta
            .get(&key)
            .ok_or_else(|| NotifyError::NotFound(format!("rule '{id}'")))?;
        let mut rule: NotificationRule =
            serde_json::from_value(current.value).map_err(MetaError::from)?;
        if let Some(v) = req.name {
            rule.name = v;
        }
        if let Some(v) = req.enabled {
            rule.enabled = v;
        }
        if let Some(v) = req.notifier_ids {
            rule.notifier_ids = v;
        }
        if let Some(v) = req.matcher {
            rule.matcher = v;
        }
        if let Some(v) = req.group_wait_secs {
            rule.group_wait_secs = v;
        }
        if let Some(v) = req.repeat_interval_secs {
            rule.repeat_interval_secs = v;
        }
        if let Some(v) = req.send_resolved {
            rule.send_resolved = v;
        }
        validate::rule(&mut rule)?;
        self.inner.check_notifiers(&rule.notifier_ids)?;
        rule.updated_at_ms = now_ms().max(rule.created_at_ms);
        let value = serde_json::to_value(&rule).map_err(MetaError::from)?;
        self.inner
            .put_checked(&meta, &key, value, current.mod_revision)
            .await?;
        Ok(rule)
    }

    pub async fn delete_rule(&self, id: &str) -> Result<(), NotifyError> {
        let meta = self.inner.meta()?;
        self.inner
            .delete_checked(&meta, &format!("{RULES_PREFIX}{id}"), "rule", id)
            .await
    }

    // -----------------------------------------------------------------------------------
    // History, feed, lifecycle
    // -----------------------------------------------------------------------------------

    /// This node's in-memory ring of recent records (newest first, at most `limit` ≤
    /// [`HISTORY_CAPACITY`]); lost on restart. The API reads [`Self::stored_history`].
    pub fn history(&self, limit: usize) -> Vec<NotificationRecord> {
        self.inner
            .history
            .lock()
            .iter()
            .take(limit.min(HISTORY_CAPACITY))
            .cloned()
            .collect()
    }

    /// The persistent history from this node's copy of `$system.notifications` merged with
    /// its in-memory ring (deduplicated, newest first, at most `limit` ≤
    /// [`history::HISTORY_MAX_RECORDS`]). In a cluster ask the channel's partition leader (the
    /// HTTP layer proxies there); a node without a copy only has its ring.
    pub async fn stored_history(&self, limit: usize) -> Vec<NotificationRecord> {
        let limit = limit.clamp(1, history::HISTORY_MAX_RECORDS);
        let stored = match &self.inner.broker {
            Some(b) => history::read_local(b.clone(), limit)
                .await
                .unwrap_or_else(|e| {
                    warn!(error = %e, "reading the notification history channel");
                    Vec::new()
                }),
            None => Vec::new(),
        };
        history::merge(stored, self.history(HISTORY_CAPACITY), limit)
    }

    /// Controller side of [`history::INTERNAL_HISTORY_APPEND_PATH`]: appends records produced
    /// on another node. `Unavailable` when this node is not the controller (the sender retries
    /// with the current one).
    pub async fn append_history(
        &self,
        records: Vec<NotificationRecord>,
    ) -> Result<(), NotifyError> {
        if !self.inner.leadership.is_controller() {
            return Err(NotifyError::Unavailable);
        }
        history::append(&self.inner, &records)
            .await
            .map_err(NotifyError::Internal)
    }

    /// Long-poll read of the `$system.events` feed (`wait` ≤ 30 s, `limit` 1–1000).
    pub async fn feed(
        &self,
        after: FeedAfter,
        limit: u32,
        wait: Duration,
    ) -> Result<FeedPage, NotifyError> {
        let broker = self.inner.broker.clone().ok_or(NotifyError::Unavailable)?;
        feed::read(
            broker,
            self.inner.feed_appended.subscribe(),
            after,
            limit,
            wait,
        )
        .await
    }

    /// Stops the dispatcher, feed writer and meta watcher (in-flight sends finish on their
    /// own tasks).
    pub fn shutdown(&self) {
        if self.inner.shutting_down.swap(true, Ordering::AcqRel) {
            return;
        }
        let _ = self.inner.stop.send(true);
        for h in self.inner.tasks.lock().drain(..) {
            h.abort();
        }
    }
}

impl Inner {
    fn meta(&self) -> Result<SharedMetaStore, NotifyError> {
        self.meta.clone().ok_or(NotifyError::Unavailable)
    }

    pub(crate) fn notifier(&self, id: &str) -> Option<Notifier> {
        self.state.read().notifiers.get(id).map(|(n, _)| n.clone())
    }

    pub(crate) fn rules_snapshot(&self) -> Arc<Vec<NotificationRule>> {
        self.rules_cache.read().clone()
    }

    fn check_notifiers(&self, ids: &[String]) -> Result<(), NotifyError> {
        let st = self.state.read();
        for id in ids {
            if !st.notifiers.contains_key(id) {
                return Err(NotifyError::Invalid(format!("unknown notifier '{id}'")));
            }
        }
        Ok(())
    }

    /// Keeps `r` in the ring and queues it for the persistent history.
    pub(crate) fn record(&self, r: NotificationRecord) {
        self.remember(r.clone());
        if let Some(tx) = &*self.history_tx.lock() {
            let _ = tx.send(r);
        }
    }

    fn remember(&self, r: NotificationRecord) {
        let mut h = self.history.lock();
        h.push_front(r);
        h.truncate(HISTORY_CAPACITY);
    }

    async fn test(
        &self,
        notifier_id: &str,
        kind: &fluxmq_proto::notify::NotifierKind,
    ) -> TestResult {
        let payload = NotificationPayload {
            id: new_id(),
            cluster: self.leadership.cluster_name(),
            sent_at_ms: now_ms(),
            rule: "test".into(),
            status: "test".into(),
            alerts: vec![],
            events: vec![],
            console_url: self.senders.config().public_url.clone(),
        };
        let started = Instant::now();
        let result = self.senders.send_once(kind, &payload).await;
        let latency_ms = started.elapsed().as_millis() as u64;
        let (error, error_detail) = match result {
            Ok(()) => (None, None),
            Err(e) => (Some(e.message), e.detail),
        };
        let record = NotificationRecord {
            id: payload.id,
            ts: now_ms(),
            rule_id: None,
            notifier_id: notifier_id.to_string(),
            status: if error.is_none() {
                DeliveryStatus::Sent
            } else {
                DeliveryStatus::Failed
            },
            summary: "test message".into(),
            attempts: 1,
            error: error.clone(),
            error_detail: error_detail.clone(),
        };
        // Persisted before the API answers, so the history shows it on any node right away.
        self.remember(record.clone());
        if self.broker.is_some()
            && let Err(e) = history::persist(self, vec![record]).await
        {
            warn!(error = %e, "test send not persisted in the notification history");
        }
        TestResult {
            ok: error.is_none(),
            latency_ms,
            error,
            error_detail,
        }
    }

    /// Creates `prefix<new id>` (precondition: absent); returns the id.
    async fn insert_new(
        &self,
        meta: &SharedMetaStore,
        prefix: &str,
        mut value_for: impl FnMut(&str) -> Result<Value, serde_json::Error>,
    ) -> Result<String, NotifyError> {
        for _ in 0..8 {
            let id = new_id();
            let key = format!("{prefix}{id}");
            let value = value_for(&id).map_err(MetaError::from)?;
            let pre = Precondition {
                key: key.clone(),
                expected_mod_revision: 0,
            };
            match meta.put(&key, value.clone(), Some(pre)).await {
                Ok(rev) => {
                    meta.wait_for(rev).await?;
                    self.apply(&key, Some(value), rev);
                    return Ok(id);
                }
                Err(MetaError::Conflict { .. }) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(NotifyError::Internal("could not allocate an id".into()))
    }

    async fn put_checked(
        &self,
        meta: &SharedMetaStore,
        key: &str,
        value: Value,
        expected: u64,
    ) -> Result<(), NotifyError> {
        let pre = Precondition {
            key: key.to_string(),
            expected_mod_revision: expected,
        };
        let rev = meta.put(key, value.clone(), Some(pre)).await?;
        meta.wait_for(rev).await?;
        self.apply(key, Some(value), rev);
        Ok(())
    }

    async fn delete_checked(
        &self,
        meta: &SharedMetaStore,
        key: &str,
        what: &str,
        id: &str,
    ) -> Result<(), NotifyError> {
        let current = meta
            .get(key)
            .ok_or_else(|| NotifyError::NotFound(format!("{what} '{id}'")))?;
        let pre = Precondition {
            key: key.to_string(),
            expected_mod_revision: current.mod_revision,
        };
        let rev = meta.delete(key, Some(pre)).await?;
        meta.wait_for(rev).await?;
        self.apply(key, None, rev);
        Ok(())
    }

    /// Applies one change (idempotent; ignores events older than the cache).
    fn apply(&self, key: &str, value: Option<Value>, rev: u64) {
        let mut st = self.state.write();
        let seen = |st: &State, id: &str, cur: Option<u64>| {
            cur.max(st.tombstones.get(id).copied()).unwrap_or(0)
        };
        if let Some(id) = key.strip_prefix(NOTIFIERS_PREFIX) {
            let tomb = format!("n/{id}");
            let cur = st.notifiers.get(id).map(|(_, r)| *r);
            if rev <= seen(&st, &tomb, cur) {
                return;
            }
            match value.map(serde_json::from_value::<Notifier>) {
                Some(Ok(n)) => {
                    st.tombstones.remove(&tomb);
                    st.notifiers.insert(id.to_string(), (n, rev));
                }
                Some(Err(e)) => warn!(key, error = %e, "ignoring malformed notifier"),
                None => {
                    st.notifiers.remove(id);
                    st.tombstones.insert(tomb, rev);
                }
            }
        } else if let Some(id) = key.strip_prefix(RULES_PREFIX) {
            let tomb = format!("r/{id}");
            let cur = st.rules.get(id).map(|(_, r)| *r);
            if rev <= seen(&st, &tomb, cur) {
                return;
            }
            match value.map(serde_json::from_value::<NotificationRule>) {
                Some(Ok(r)) => {
                    st.tombstones.remove(&tomb);
                    st.rules.insert(id.to_string(), (r, rev));
                }
                Some(Err(e)) => warn!(key, error = %e, "ignoring malformed notification rule"),
                None => {
                    st.rules.remove(id);
                    st.tombstones.insert(tomb, rev);
                }
            }
            self.rebuild(&st);
        }
    }

    fn reload(&self) {
        let Some(meta) = &self.meta else {
            return;
        };
        let mut st = State::default();
        for (key, v) in meta.list(NOTIFIERS_PREFIX) {
            match serde_json::from_value::<Notifier>(v.value) {
                Ok(n) => {
                    st.notifiers.insert(
                        key[NOTIFIERS_PREFIX.len()..].to_string(),
                        (n, v.mod_revision),
                    );
                }
                Err(e) => warn!(key, error = %e, "ignoring malformed notifier"),
            }
        }
        for (key, v) in meta.list(RULES_PREFIX) {
            match serde_json::from_value::<NotificationRule>(v.value) {
                Ok(r) => {
                    st.rules
                        .insert(key[RULES_PREFIX.len()..].to_string(), (r, v.mod_revision));
                }
                Err(e) => warn!(key, error = %e, "ignoring malformed notification rule"),
            }
        }
        let mut cur = self.state.write();
        *cur = st;
        self.rebuild(&cur);
    }

    fn rebuild(&self, st: &State) {
        let mut v: Vec<NotificationRule> = st.rules.values().map(|(r, _)| r.clone()).collect();
        v.sort_by(|a, b| (a.created_at_ms, &a.id).cmp(&(b.created_at_ms, &b.id)));
        *self.rules_cache.write() = Arc::new(v);
    }
}

async fn watch_loop(
    inner: Arc<Inner>,
    mut rx: broadcast::Receiver<MetaEvent>,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            r = rx.recv() => match r {
                Ok(ev) => {
                    if ev.key.starts_with(NOTIFIERS_PREFIX) || ev.key.starts_with(RULES_PREFIX) {
                        inner.apply(&ev.key, ev.value, ev.revision);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(skipped = n, "notification config watch lagged; reloading");
                    inner.reload();
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
        }
    }
}

/// 10 lowercase base36 characters.
pub(crate) fn new_id() -> String {
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
