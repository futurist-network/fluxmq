//! Shared hub state: latest document per topic, subscriber refcounts, change notification.
//!
//! Every topic keeps its latest JSON document plus a hub-wide monotonically increasing
//! `version`. Publishing an identical document is a no-op (no version bump). Each publish also
//! caches the merge patch from the previous version, so a session that is exactly one version
//! behind (the common case) reuses it instead of diffing again — documents and patches are
//! computed once regardless of the number of sessions.
//!
//! Lock order: `refcounts` → `topics`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use fluxmq_core::BrokerStats;
use fluxmq_proto::telemetry::topics::{self, Topic};
use fluxmq_proto::telemetry::{
    AlertsState, ConsumersState, EventKind, Health, HistoryRange, HistorySeries, TailState,
};
use parking_lot::{Mutex, RwLock};
use serde_json::Value;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

use crate::cluster_source::{ClusterInfo, ClusterSource};
use crate::collector::Collector;
use crate::health::HealthReport;
use crate::history::History;
use crate::node::NodeSample;
use crate::patch::{diff, strip_nulls};
use crate::rates::Derived;
use crate::session::Session;
use crate::source::Source;
use crate::{TelemetryConfig, TelemetrySignal, documents, tail};

/// Buffered signals per listener before it lags (`RecvError::Lagged`).
const SIGNAL_CAPACITY: usize = 1024;

#[derive(Debug, Clone)]
pub(crate) struct TopicState {
    pub version: u64,
    pub doc: Arc<Value>,
    /// What a new subscriber receives instead of `doc` (tail: the recent buffer).
    pub snapshot: Option<Arc<Value>>,
    /// Merge patch from `.0` (previous version) to `version`.
    pub last_patch: Option<(u64, Arc<Value>)>,
}

/// The latest computed state, used to build channel documents on demand when a topic gets its
/// first subscriber between collector ticks.
pub(crate) struct Latest {
    pub stats: Arc<BrokerStats>,
    pub derived: Arc<Derived>,
    pub health: Arc<HealthReport>,
}

pub(crate) struct Shared {
    pub source: Arc<dyn Source>,
    pub node_id: String,
    pub config: TelemetryConfig,
    pub data_dir: Option<PathBuf>,
    version: AtomicU64,
    topics: RwLock<HashMap<String, TopicState>>,
    refcounts: Mutex<HashMap<String, usize>>,
    notify: watch::Sender<u64>,
    shutdown: AtomicBool,
    shutdown_tx: watch::Sender<bool>,
    pub alerts: RwLock<AlertsState>,
    pub history: Mutex<History>,
    pub latest: RwLock<Option<Arc<Latest>>>,
    /// Host name reported in `nodes` and node-stats snapshots.
    pub address: String,
    /// This node's own (un-merged) stats from the last tick — what peers and `/metrics` see.
    pub local_stats: RwLock<Option<Arc<BrokerStats>>>,
    /// Latest resource sample.
    pub node_sample: RwLock<NodeSample>,
    /// Set by [`TelemetryHub::attach_cluster`] when clustered.
    pub cluster: RwLock<Option<Arc<ClusterSource>>>,
    /// Latest cluster info computed by the collector (clustered only).
    pub cluster_info: RwLock<Option<Arc<ClusterInfo>>>,
    /// Encoded node-stats snapshot, keyed by the local stats' `taken_at_ms`.
    pub snapshot_cache: Mutex<Option<(u64, Arc<Vec<u8>>)>>,
    /// Alert transitions + new events for notification consumers.
    signals: broadcast::Sender<TelemetrySignal>,
    tails: Mutex<HashMap<String, JoinHandle<()>>>,
    runtime: Option<tokio::runtime::Handle>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// Extra alert candidates evaluated with the built-in ones (v0.4: TLS certificates).
    pub extra_alerts: RwLock<Option<AlertSource>>,
    /// Events pushed from outside the collector, drained into the feed on the next tick.
    pub pending_events: Mutex<Vec<(EventKind, String, String, Health)>>,
}

/// Produces alert candidates for a tick (`now_ms`); ids must be stable per condition.
pub type AlertSource = Arc<dyn Fn(u64) -> Vec<fluxmq_proto::telemetry::Alert> + Send + Sync>;

impl Shared {
    pub fn new(
        source: Arc<dyn Source>,
        node_id: String,
        data_dir: Option<PathBuf>,
        config: TelemetryConfig,
    ) -> Arc<Shared> {
        Arc::new(Shared {
            source,
            node_id,
            config,
            data_dir,
            version: AtomicU64::new(0),
            topics: RwLock::new(HashMap::new()),
            refcounts: Mutex::new(HashMap::new()),
            notify: watch::channel(0).0,
            shutdown: AtomicBool::new(false),
            shutdown_tx: watch::channel(false).0,
            alerts: RwLock::new(AlertsState::new()),
            history: Mutex::new(History::new()),
            latest: RwLock::new(None),
            address: sysinfo::System::host_name().unwrap_or_else(|| "localhost".into()),
            local_stats: RwLock::new(None),
            node_sample: RwLock::new(NodeSample::default()),
            cluster: RwLock::new(None),
            cluster_info: RwLock::new(None),
            snapshot_cache: Mutex::new(None),
            signals: broadcast::channel(SIGNAL_CAPACITY).0,
            tails: Mutex::new(HashMap::new()),
            runtime: tokio::runtime::Handle::try_current().ok(),
            tasks: Mutex::new(Vec::new()),
            extra_alerts: RwLock::new(None),
            pending_events: Mutex::new(Vec::new()),
        })
    }

    pub fn subscribe_notify(&self) -> watch::Receiver<u64> {
        self.notify.subscribe()
    }

    pub fn subscribe_shutdown(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    /// Fans a signal out to [`TelemetryHub::notifications`] listeners (no-op without any).
    pub fn signal(&self, signal: TelemetrySignal) {
        let _ = self.signals.send(signal);
    }

    pub fn subscribe_signals(&self) -> broadcast::Receiver<TelemetrySignal> {
        self.signals.subscribe()
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Wakes all sessions.
    pub fn notify(&self) {
        self.notify.send_modify(|v| *v = v.wrapping_add(1));
    }

    pub fn add_task(&self, h: JoinHandle<()>) {
        self.tasks.lock().push(h);
    }

    /// Replaces the document of `topic`. Returns whether it changed. Call [`Shared::notify`]
    /// after a batch of publishes.
    pub fn publish(&self, topic: &str, mut doc: Value, snapshot: Option<Value>) -> bool {
        strip_nulls(&mut doc);
        let snapshot = snapshot.map(|mut s| {
            strip_nulls(&mut s);
            Arc::new(s)
        });
        let mut topics = self.topics.write();
        match topics.get_mut(topic) {
            Some(t) if *t.doc == doc => {
                if snapshot.is_some() {
                    t.snapshot = snapshot;
                }
                false
            }
            Some(t) => {
                let patch = diff(&t.doc, &doc);
                let from = t.version;
                t.version = self.version.fetch_add(1, Ordering::Relaxed) + 1;
                t.last_patch = patch.map(|p| (from, Arc::new(p)));
                t.doc = Arc::new(doc);
                if snapshot.is_some() {
                    t.snapshot = snapshot;
                }
                true
            }
            None => {
                let version = self.version.fetch_add(1, Ordering::Relaxed) + 1;
                topics.insert(
                    topic.to_string(),
                    TopicState {
                        version,
                        doc: Arc::new(doc),
                        snapshot,
                        last_patch: None,
                    },
                );
                true
            }
        }
    }

    pub fn publish_typed<T: serde::Serialize>(&self, topic: &str, doc: &T) -> bool {
        match serde_json::to_value(doc) {
            Ok(v) => self.publish(topic, v, None),
            Err(e) => {
                tracing::error!(topic, error = %e, "failed to serialize telemetry document");
                false
            }
        }
    }

    pub fn topic(&self, topic: &str) -> Option<TopicState> {
        self.topics.read().get(topic).cloned()
    }

    pub fn with_topics<R>(&self, f: impl FnOnce(&HashMap<String, TopicState>) -> R) -> R {
        f(&self.topics.read())
    }

    /// Topics with at least one subscriber.
    pub fn subscribed(&self) -> Vec<String> {
        self.refcounts.lock().keys().cloned().collect()
    }

    #[cfg(test)]
    pub fn refcount(&self, topic: &str) -> usize {
        self.refcounts.lock().get(topic).copied().unwrap_or(0)
    }

    /// Removes channel-scoped documents nobody subscribes to anymore.
    pub fn prune_unsubscribed(&self) {
        let refs = self.refcounts.lock();
        let mut topics = self.topics.write();
        topics.retain(|name, _| is_global(name) || refs.contains_key(name));
    }

    /// Adds a subscriber to `topic`. Starts the tail pump on the first `channel.X.tail`
    /// subscriber. On error nothing is retained.
    pub fn acquire(self: &Arc<Self>, topic: &str, parsed: &Topic) -> Result<(), String> {
        let mut refs = self.refcounts.lock();
        let count = refs.entry(topic.to_string()).or_insert(0);
        *count += 1;
        if *count == 1
            && let Topic::Tail(channel) = parsed
            && let Err(e) = self.start_tail(topic, channel)
        {
            refs.remove(topic);
            return Err(e);
        }
        Ok(())
    }

    /// Removes a subscriber; the last one drops channel-scoped documents and stops tail pumps.
    pub fn release(&self, topic: &str) {
        let mut refs = self.refcounts.lock();
        let Some(count) = refs.get_mut(topic) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count > 0 {
            return;
        }
        refs.remove(topic);
        if !is_global(topic) {
            self.topics.write().remove(topic);
        }
        if let Some(h) = self.tails.lock().remove(topic) {
            h.abort();
        }
    }

    fn start_tail(self: &Arc<Self>, topic: &str, channel: &str) -> Result<(), String> {
        let Some(rt) = self.runtime.clone() else {
            return Err("live tail unavailable: no async runtime".into());
        };
        let feed = self.source.tail(channel)?;
        let empty = TailState {
            sample_ratio: 1.0,
            shown_rate: 0.0,
            total_rate: 0.0,
            messages: vec![],
        };
        if let Ok(v) = serde_json::to_value(&empty) {
            self.publish(topic, v.clone(), Some(v));
        }
        let handle = rt.spawn(tail::run_pump(
            Arc::downgrade(self),
            topic.to_string(),
            feed,
            tail::FLUSH_INTERVAL,
        ));
        if let Some(old) = self.tails.lock().insert(topic.to_string(), handle) {
            old.abort();
        }
        Ok(())
    }

    /// Makes sure a document exists for a newly subscribed channel-scoped topic.
    pub fn ensure_doc(&self, topic: &str, parsed: &Topic) {
        if self.topics.read().contains_key(topic) {
            return;
        }
        let latest = self.latest.read().clone();
        let stats_ch = |name: &str| {
            latest.as_ref().and_then(|l| {
                l.stats
                    .channels
                    .iter()
                    .find(|c| c.config.name == name)
                    .cloned()
            })
        };
        match parsed {
            Topic::Channel(name) => {
                let doc = match (stats_ch(name), latest.as_ref()) {
                    (Some(ch), Some(l)) => documents::channel_detail(
                        &ch,
                        l.derived.channels.get(name),
                        l.health.channel(name),
                    ),
                    _ => match self.source.channel(name) {
                        Some(cfg) => documents::empty_channel_detail(&cfg),
                        None => return,
                    },
                };
                self.publish_typed(topic, &doc);
            }
            Topic::Consumers(name) => {
                let doc = match (stats_ch(name), latest.as_ref()) {
                    (Some(ch), Some(l)) => {
                        documents::consumers_state(&ch, l.derived.channels.get(name), &l.health)
                    }
                    _ => ConsumersState::new(),
                };
                self.publish_typed(topic, &doc);
            }
            _ => {}
        }
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.shutdown_tx.send_replace(true);
        for (_, h) in self.tails.lock().drain() {
            h.abort();
        }
        // The collector exits on the shutdown signal (persisting history first); other tasks
        // are aborted.
        let tasks: Vec<_> = self.tasks.lock().drain(..).collect();
        drop(tasks);
        self.notify();
    }
}

/// Global topics are always maintained; channel-scoped ones only while subscribed.
pub(crate) fn is_global(topic: &str) -> bool {
    matches!(
        topic,
        topics::CLUSTER | topics::CHANNELS | topics::ALERTS | topics::EVENTS | topics::NODES
    )
}

/// Handle to the running observability plane. Cheap to clone.
#[derive(Clone)]
pub struct TelemetryHub {
    pub(crate) shared: Arc<Shared>,
}

impl TelemetryHub {
    /// Spawns the collector (and tail/event pumps). Must be called inside a Tokio runtime.
    pub fn start(broker: fluxmq_core::Broker, config: TelemetryConfig) -> TelemetryHub {
        let node_id = broker.node_id().to_string();
        let data_dir = broker.config().data_dir.clone();
        Self::start_with_source(Arc::new(broker), node_id, Some(data_dir), config)
    }

    /// [`TelemetryHub::start`] over any [`Source`] (used by tests and alternative embeddings).
    pub(crate) fn start_with_source(
        source: Arc<dyn Source>,
        node_id: String,
        data_dir: Option<PathBuf>,
        config: TelemetryConfig,
    ) -> TelemetryHub {
        let (hub, collector) = Self::build(source, node_id, data_dir, config);
        let events = hub.shared.source.events();
        let h = tokio::spawn(crate::collector::run(collector, events));
        hub.shared.add_task(h);
        hub
    }

    /// Creates the hub and its collector without spawning anything.
    pub(crate) fn build(
        source: Arc<dyn Source>,
        node_id: String,
        data_dir: Option<PathBuf>,
        config: TelemetryConfig,
    ) -> (TelemetryHub, Collector) {
        let shared = Shared::new(source, node_id, data_dir, config);
        let collector = Collector::new(&shared);
        (TelemetryHub { shared }, collector)
    }

    /// A new per-connection session (one per WebSocket).
    pub fn session(&self) -> Session {
        Session::new(self.shared.clone())
    }

    /// Downsampled history for `metric` (see `fluxmq_proto::telemetry::metrics`;
    /// channel metrics are `channel.<name>.<metric>`).
    pub fn history(&self, metric: &str, range: HistoryRange) -> Result<HistorySeries, String> {
        self.shared
            .history
            .lock()
            .query(metric, range, fluxmq_proto::now_ms())
    }

    /// Currently open alerts.
    pub fn alerts(&self) -> AlertsState {
        self.shared.alerts.read().clone()
    }

    /// Alert transitions and new broker events as they happen — computed once by the collector
    /// (the same transitions that feed the `alerts`/`events` topics), any number of listeners.
    /// A listener that falls more than 1024 signals behind gets `RecvError::Lagged`.
    /// In a cluster every node computes the same cluster-wide alerts; consumers that must act
    /// once (notifications) gate on being the controller.
    pub fn notifications(&self) -> broadcast::Receiver<TelemetrySignal> {
        self.shared.subscribe_signals()
    }

    /// Enables cluster-wide aggregation (no-op for a single node: the collector keeps the
    /// v0.1 path and nothing is polled). Call once, after the cluster is up.
    /// Adds alert candidates from outside the broker stats (v0.4: `tls_cert_expiring`,
    /// `tls_reload_failed`). They go through the same hysteresis, events and notifications.
    pub fn set_alert_source(
        &self,
        source: impl Fn(u64) -> Vec<fluxmq_proto::telemetry::Alert> + Send + Sync + 'static,
    ) {
        *self.shared.extra_alerts.write() = Some(Arc::new(source));
    }

    /// Appends an event to the feed (and the notification signals) on the next tick.
    pub fn push_event(
        &self,
        kind: EventKind,
        title: impl Into<String>,
        subject: impl Into<String>,
        severity: Health,
    ) {
        self.shared
            .pending_events
            .lock()
            .push((kind, title.into(), subject.into(), severity));
    }

    pub fn attach_cluster(&self, cluster: fluxmq_cluster::Cluster) {
        if !cluster.is_clustered() {
            return;
        }
        let source = Arc::new(ClusterSource::new(self.shared.source.clone(), cluster));
        *self.shared.cluster.write() = Some(source.clone());
        if let Some(rt) = self.shared.runtime.clone() {
            let h = rt.spawn(source.run(self.shared.subscribe_shutdown()));
            self.shared.add_task(h);
        }
    }

    /// Whether cluster aggregation is active.
    pub fn is_clustered(&self) -> bool {
        self.shared.cluster.read().is_some()
    }

    /// The encoded [`crate::snapshot::NodeStatsSnapshot`] of this node (`None` before the
    /// first collector tick). Cached per collector sample, so N peers pulling cost one encode.
    pub fn node_stats(&self) -> Option<Arc<Vec<u8>>> {
        crate::snapshot::node_stats(&self.shared)
    }

    /// Prometheus text exposition (format 0.0.4) of this node's metrics.
    pub fn prometheus(&self) -> String {
        crate::prometheus::render(&self.shared)
    }

    /// Stops background tasks.
    pub fn shutdown(&self) {
        self.shared.shutdown();
    }
}
