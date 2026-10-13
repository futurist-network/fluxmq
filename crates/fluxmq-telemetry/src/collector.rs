//! The collector: one task that samples the broker on a fixed tick and computes every document
//! once per tick, independent of the number of sessions.
//!
//! Cadences (in ticks of `config.tick`, at least 1): `cluster.overview` 250 ms; per-channel
//! derived values, `channels.summary`, `channel.{name}`, `channel.{name}.consumers` 500 ms;
//! `nodes`, health/alerts and history samples 1 s; `events` and `alerts` whenever they change.
//! Channel-scoped documents are only computed for topics with ≥ 1 subscriber.
//!
//! [`Collector::tick`] is synchronous and pure over its inputs (stats snapshot, drained broker
//! events, optional node sample) so it can be driven by tests without a running core;
//! [`run`] is the thin async shell that gathers those inputs.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use fluxmq_core::{BrokerEvent, BrokerStats};
use fluxmq_proto::telemetry::topics::{self, Topic};
use fluxmq_proto::telemetry::{EventKind, Health, metrics};
use tokio::sync::broadcast;

use crate::TelemetrySignal;
use crate::alerts::{self, AlertEngine};
use crate::cluster_source::{ClusterInfo, ClusterView};
use crate::documents::{self, ClusterExtras};
use crate::events::EventLog;
use crate::health::{HealthReport, HealthTracker, NodeInputs};
use crate::history::{self, channel_metric};
use crate::hub::{Latest, Shared};
use crate::node::{NodeSample, NodeSampler};
use crate::rates::{Derived, Window, counter_delta, derive};

const CLUSTER_EVERY: Duration = Duration::from_millis(250);
const CHANNELS_EVERY: Duration = Duration::from_millis(500);
const NODES_EVERY: Duration = Duration::from_secs(1);
const HEALTH_EVERY: Duration = Duration::from_secs(1);
const PERSIST_EVERY: Duration = Duration::from_secs(60);
const DAY_MS: u64 = 86_400_000;

fn ticks(every: Duration, tick: Duration) -> u64 {
    let t = tick.as_millis().max(1);
    ((every.as_millis() + t / 2) / t).max(1) as u64
}

pub(crate) struct Collector {
    shared: Weak<Shared>,
    window: Window,
    tracker: HealthTracker,
    alerts: AlertEngine,
    events: EventLog,
    /// `(ts, cumulative retries)` sampled once per minute, for `retries_24h`.
    retries: VecDeque<(u64, u64)>,
    tick_no: u64,
    every_cluster: u64,
    every_channels: u64,
    every_nodes: u64,
    every_health: u64,
    every_prune: u64,
    node: NodeSample,
    full: Arc<Derived>,
    health: Arc<HealthReport>,
    address: String,
    rate_window_ms: u64,
    /// Cluster view generation of the window's snapshots (0 = single node).
    generation: u64,
    /// Controller seen on the previous tick (`None` = not observed yet).
    last_controller: Option<Option<String>>,
    /// Peers that were down / known on the previous tick.
    last_down: BTreeSet<String>,
    last_peers: BTreeSet<String>,
    /// Rebalance plan seen on the previous tick (`None` = not observed yet).
    last_plan: Option<Option<fluxmq_cluster::rebalance::PlanDoc>>,
    /// Decommission records seen on the previous tick, by node (`None` = not observed yet).
    last_decommissions: Option<BTreeMap<String, fluxmq_cluster::DecommissionRecord>>,
}

impl Collector {
    pub fn new(shared: &Arc<Shared>) -> Self {
        let tick = shared.config.tick;
        Self {
            shared: Arc::downgrade(shared),
            window: Window::new(shared.config.rate_window.as_millis() as u64),
            tracker: HealthTracker::new(),
            alerts: AlertEngine::default(),
            events: EventLog::new(),
            retries: VecDeque::new(),
            tick_no: 0,
            every_cluster: ticks(CLUSTER_EVERY, tick),
            every_channels: ticks(CHANNELS_EVERY, tick),
            every_nodes: ticks(NODES_EVERY, tick),
            every_health: ticks(HEALTH_EVERY, tick),
            every_prune: ticks(Duration::from_secs(3600), tick),
            node: NodeSample::default(),
            full: Arc::new(Derived::default()),
            health: Arc::new(HealthReport::default()),
            address: shared.address.clone(),
            rate_window_ms: shared.config.rate_window.as_millis() as u64,
            generation: 0,
            last_controller: None,
            last_down: BTreeSet::new(),
            last_peers: BTreeSet::new(),
            last_plan: None,
            last_decommissions: None,
        }
    }

    /// Whether the next tick publishes `nodes` (the caller samples sysinfo only then).
    pub fn nodes_due(&self) -> bool {
        self.tick_no.is_multiple_of(self.every_nodes)
    }

    #[cfg(test)]
    pub fn events_feed(&self) -> fluxmq_proto::telemetry::EventsFeed {
        self.events.feed()
    }

    #[cfg(test)]
    pub fn set_alert_hysteresis(&mut self, open_after: u32, resolve_after: u32) {
        self.alerts = AlertEngine::new(open_after, resolve_after);
    }

    /// One single-node collector step. Returns `false` when the hub is gone.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn tick(
        &mut self,
        stats: BrokerStats,
        broker_events: Vec<BrokerEvent>,
        node: Option<NodeSample>,
    ) -> bool {
        self.tick_with(stats, broker_events, node, None)
    }

    /// One collector step over local stats, merged with the peers when `cluster` is given.
    pub fn tick_with(
        &mut self,
        local: BrokerStats,
        broker_events: Vec<BrokerEvent>,
        node: Option<NodeSample>,
        cluster: Option<ClusterView>,
    ) -> bool {
        let Some(shared) = self.shared.upgrade() else {
            return false;
        };
        let local = Arc::new(local);
        let (stats, cluster) = match cluster {
            Some(v) => (Arc::new(v.stats), Some(v.info)),
            None => (local.clone(), None),
        };
        let now = stats.taken_at_ms;
        *shared.local_stats.write() = Some(local);
        let generation = cluster.as_ref().map_or(0, |c| c.generation);
        if generation != self.generation {
            // The merged snapshot's composition changed: cumulative values are not comparable
            // with the window's older snapshots.
            self.window = Window::new(self.rate_window_ms);
            self.generation = generation;
        }
        self.window.push(stats.clone());

        let cluster_due = self.tick_no.is_multiple_of(self.every_cluster);
        let channels_due = self.tick_no.is_multiple_of(self.every_channels);
        let health_due = self.tick_no.is_multiple_of(self.every_health);
        let nodes_due = self.nodes_due();
        let full_due = channels_due || health_due;

        let derived = Arc::new(derive(&self.window, full_due));
        if full_due {
            self.full = derived.clone();
        }
        for e in &broker_events {
            self.events.push_broker(now, e);
        }
        if let Some(n) = node {
            self.node = n;
            *shared.node_sample.write() = n;
        }
        if let Some(info) = &cluster {
            self.cluster_events(now, info);
        }

        if health_due {
            let inputs = NodeInputs {
                disk_used_bytes: self.node.disk_used_bytes,
                disk_total_bytes: self.node.disk_total_bytes,
                fsync_p99_ms: derived.fsync_p99_ms,
            };
            let mut report = self.tracker.evaluate(&stats, &self.full, &inputs);
            let mut candidates = alerts::candidates(&report, &stats, &self.full, &stats.node_id);
            if let Some(info) = &cluster {
                candidates.extend(alerts::cluster_candidates(info));
            }
            let extra = shared.extra_alerts.read().clone();
            if let Some(source) = extra {
                candidates.extend(source(now));
            }
            for t in self.alerts.evaluate(now, candidates) {
                self.events.push_transition(now, &t);
                shared.signal(match t {
                    alerts::Transition::Opened(a) => TelemetrySignal::AlertOpened(a),
                    alerts::Transition::Resolved(a) => TelemetrySignal::AlertResolved(a),
                });
            }
            // One roll-up rule: an open alert raises its object (and its parents) to at least
            // its severity, so status dots never contradict open alerts.
            report.apply_alerts(self.alerts.state());
            self.health = Arc::new(report);
            *shared.alerts.write() = self.alerts.state().clone();
            shared.publish_typed(topics::ALERTS, self.alerts.state());
            self.record_history(&shared, &stats, &derived);
            self.sample_retries(&stats);
        }

        *shared.latest.write() = Some(Arc::new(Latest {
            stats: stats.clone(),
            derived: self.full.clone(),
            health: self.health.clone(),
        }));

        if cluster_due {
            let extras = ClusterExtras {
                open_alerts: self.alerts.state().len() as u32,
                retries_24h: self.retries_24h(&stats),
                disk_total_bytes: self.node.disk_total_bytes,
            };
            let mut doc = documents::cluster_overview(&stats, &derived, &self.health, extras);
            if let Some(info) = &cluster {
                apply_cluster(&mut doc, info);
            }
            shared.publish_typed(topics::CLUSTER, &doc);
        }

        if channels_due {
            let summary = documents::channels_summary(&stats, &self.full, &self.health);
            shared.publish_typed(topics::CHANNELS, &summary);
            for topic in shared.subscribed() {
                match topics::parse(&topic) {
                    Some(Topic::Channel(name)) => {
                        if let Some(ch) = stats.channels.iter().find(|c| c.config.name == name) {
                            let doc = documents::channel_detail(
                                ch,
                                self.full.channels.get(&name),
                                self.health.channel(&name),
                            );
                            shared.publish_typed(&topic, &doc);
                        }
                    }
                    Some(Topic::Consumers(name)) => {
                        if let Some(ch) = stats.channels.iter().find(|c| c.config.name == name) {
                            let doc = documents::consumers_state(
                                ch,
                                self.full.channels.get(&name),
                                &self.health,
                            );
                            shared.publish_typed(&topic, &doc);
                        }
                    }
                    _ => {}
                }
            }
            shared.prune_unsubscribed();
        }

        if nodes_due {
            let node_health = self.health.node;
            let local = shared
                .local_stats
                .read()
                .clone()
                .unwrap_or_else(|| stats.clone());
            let mut state =
                documents::node_state(&local, &self.address, &self.node, &derived, node_health);
            let mut nodes = fluxmq_proto::telemetry::NodesState::new();
            if let Some(info) = &cluster {
                state.is_controller = info.facts.local_is_controller;
                for p in &info.peers {
                    nodes.insert(p.id.clone(), p.clone());
                }
            }
            nodes.insert(stats.node_id.clone(), state);
            shared.publish_typed(topics::NODES, &nodes);
        }

        *shared.cluster_info.write() = cluster.map(Arc::new);

        let pushed = std::mem::take(&mut *shared.pending_events.lock());
        for (kind, title, subject, severity) in pushed {
            self.events.push(now, kind, title, subject, severity);
        }
        for e in self.events.take_fresh() {
            shared.signal(TelemetrySignal::Event(e));
        }
        if self.events.take_changed() || self.tick_no == 0 {
            shared.publish_typed(topics::EVENTS, &self.events.feed());
        }

        if self.tick_no > 0 && self.tick_no.is_multiple_of(self.every_prune) {
            shared.history.lock().prune(now);
        }

        self.tick_no += 1;
        shared.notify();
        true
    }

    /// Controller changes and peer up/down transitions → events feed.
    fn cluster_events(&mut self, now: u64, info: &ClusterInfo) {
        let controller = info.facts.controller.clone();
        if let Some(prev) = &self.last_controller
            && *prev != controller
        {
            let subject = controller.clone().unwrap_or_else(|| "none".into());
            let (title, severity) = match &controller {
                Some(c) => (format!("Controller changed to {c}"), Health::Warning),
                None => ("Controller lost".to_string(), Health::Critical),
            };
            self.events
                .push(now, EventKind::ControllerChanged, title, subject, severity);
        }
        self.last_controller = Some(controller);

        let down: BTreeSet<String> = info.down.iter().cloned().collect();
        let peers: BTreeSet<String> = info.peers.iter().map(|p| p.id.clone()).collect();
        for id in down.difference(&self.last_down) {
            self.events.push(
                now,
                EventKind::NodeDown,
                format!("Node {id} is unreachable"),
                id.clone(),
                Health::Critical,
            );
        }
        for id in peers.iter().filter(|id| !down.contains(*id)) {
            let came_back = self.last_down.contains(id);
            let joined = !self.last_peers.contains(id) && !self.last_peers.is_empty();
            if came_back || joined {
                let title = if came_back {
                    format!("Node {id} is back")
                } else {
                    format!("Node {id} joined")
                };
                self.events
                    .push(now, EventKind::NodeUp, title, id.clone(), Health::Healthy);
            }
        }
        self.last_down = down;
        self.last_peers = peers;
        self.rebalance_events(now, info.facts.rebalance.as_ref());
        self.decommission_events(now, &info.facts.decommissions);
    }

    /// Decommission / forget transitions (v0.5) → events feed. Like the rebalance events,
    /// every node derives them from the replicated records, so the feed agrees cluster-wide;
    /// the first observation is a baseline. A record is identified by node + start time (a
    /// retried decommission is a new record).
    pub(crate) fn decommission_events(
        &mut self,
        now: u64,
        records: &[fluxmq_cluster::DecommissionRecord],
    ) {
        use fluxmq_proto::cluster::DecommissionState as S;
        let next: BTreeMap<String, fluxmq_cluster::DecommissionRecord> = records
            .iter()
            .map(|r| (r.status.node.clone(), r.clone()))
            .collect();
        let Some(prev) = self.last_decommissions.replace(next) else {
            return;
        };
        for r in records {
            let st = &r.status;
            let node = st.node.clone();
            let known = prev
                .get(&node)
                .filter(|p| p.status.started_at_ms == st.started_at_ms)
                .map(|p| p.status.state);
            let ended = |s: Option<S>| matches!(s, Some(S::Removed | S::Failed));
            if known.is_none() && !r.forget {
                let n = st.initial_replicas;
                self.events.push(
                    now,
                    EventKind::NodeDecommissionStarted,
                    format!(
                        "Decommissioning node {node}: {n} replica{} to move",
                        if n == 1 { "" } else { "s" }
                    ),
                    node.clone(),
                    Health::Warning,
                );
            }
            if ended(known) {
                continue;
            }
            match st.state {
                S::Removed if r.forget => self.events.push(
                    now,
                    EventKind::NodeForgotten,
                    format!("Node {node} forgotten"),
                    node.clone(),
                    Health::Healthy,
                ),
                S::Removed => self.events.push(
                    now,
                    EventKind::NodeDecommissioned,
                    format!("Node {node} decommissioned and removed from the cluster"),
                    node.clone(),
                    Health::Healthy,
                ),
                S::Failed => {
                    let what = if r.forget { "Forget" } else { "Decommission" };
                    let err = st.error.as_deref().unwrap_or("unknown error");
                    self.events.push(
                        now,
                        EventKind::NodeDecommissionFailed,
                        format!("{what} of node {node} failed: {err}"),
                        node.clone(),
                        Health::Critical,
                    )
                }
                S::MovingReplicas | S::LeavingMembership => {}
            }
        }
    }

    /// Rebalance plan transitions (v0.4) → events feed. Every node derives them from the
    /// replicated plan, so the feed agrees cluster-wide; the first observation is a baseline.
    pub(crate) fn rebalance_events(
        &mut self,
        now: u64,
        plan: Option<&fluxmq_cluster::rebalance::PlanDoc>,
    ) {
        use fluxmq_cluster::rebalance::PlanOutcome;
        use fluxmq_proto::cluster::MoveState;
        let Some(prev) = self.last_plan.replace(plan.cloned()) else {
            return;
        };
        let Some(plan) = plan else {
            return;
        };
        let same = prev.as_ref().filter(|p| p.id == plan.id);
        if same.is_none() {
            let trigger = plan.trigger.clone().unwrap_or_else(|| "manual".into());
            let n = plan.moves.len();
            self.events.push(
                now,
                EventKind::RebalanceStarted,
                format!(
                    "Rebalance started: {n} partition move{}",
                    if n == 1 { "" } else { "s" }
                ),
                trigger,
                Health::Healthy,
            );
            for node in &plan.repair_nodes {
                self.events.push(
                    now,
                    EventKind::ReplicaRepairStarted,
                    format!("Replacing the replicas of node {node}"),
                    node.clone(),
                    Health::Warning,
                );
            }
        }
        for (i, m) in plan.moves.iter().enumerate() {
            let was_done = same
                .and_then(|p| p.moves.get(i))
                .is_some_and(|pm| pm.state == MoveState::Done);
            if m.state == MoveState::Done && !was_done {
                self.events.push(
                    now,
                    EventKind::PartitionMoved,
                    format!(
                        "{} → {}",
                        m.from_replicas.join(","),
                        m.to_replicas.join(",")
                    ),
                    format!("{}/{}", m.channel, m.partition),
                    Health::Healthy,
                );
            }
        }
        let was_finished = same.is_some_and(|p| p.outcome.is_some());
        if let Some(outcome) = plan.outcome
            && !was_finished
        {
            let done = plan
                .moves
                .iter()
                .filter(|m| m.state == MoveState::Done)
                .count();
            let rolled_back = plan
                .moves
                .iter()
                .filter(|m| m.state == MoveState::RolledBack)
                .count();
            let counts = format!("{done} done, {rolled_back} rolled back");
            let subject = plan.trigger.clone().unwrap_or_else(|| "manual".into());
            let (kind, title, severity) = match outcome {
                PlanOutcome::Completed => (
                    EventKind::RebalanceCompleted,
                    format!("Rebalance completed: {counts}"),
                    Health::Healthy,
                ),
                PlanOutcome::Cancelled => (
                    EventKind::RebalanceCompleted,
                    format!("Rebalance cancelled: {counts}"),
                    Health::Warning,
                ),
                PlanOutcome::Failed => (
                    EventKind::RebalanceFailed,
                    format!("Rebalance failed: {counts}"),
                    Health::Critical,
                ),
            };
            self.events.push(now, kind, title, subject, severity);
        }
    }

    fn record_history(&self, shared: &Shared, stats: &BrokerStats, derived: &Derived) {
        let ts = stats.taken_at_ms;
        let mut h = shared.history.lock();
        h.record(metrics::INGRESS_RATE, ts, derived.ingress_rate);
        h.record(metrics::EGRESS_RATE, ts, derived.egress_rate);
        h.record(metrics::ACK_RATE, ts, derived.ack_rate);
        h.record(metrics::PUBLISH_P50, ts, derived.publish_latency.p50);
        h.record(metrics::PUBLISH_P95, ts, derived.publish_latency.p95);
        h.record(metrics::PUBLISH_P99, ts, derived.publish_latency.p99);
        h.record(metrics::LAG, ts, documents::total_lag(stats) as f64);
        h.record(metrics::RETRIES_RATE, ts, derived.retries_rate);
        h.record(metrics::DLQ_RATE, ts, derived.dlq_rate);
        h.record(
            metrics::CONSUMERS,
            ts,
            documents::total_consumers(stats) as f64,
        );
        h.record(metrics::STORAGE_BYTES, ts, stats.storage_bytes as f64);
        for ch in &stats.channels {
            let name = &ch.config.name;
            let cd = derived.channels.get(name).cloned().unwrap_or_default();
            h.record(
                &channel_metric(name, metrics::INGRESS_RATE),
                ts,
                cd.ingress_rate,
            );
            h.record(
                &channel_metric(name, metrics::EGRESS_RATE),
                ts,
                cd.egress_rate,
            );
            h.record(&channel_metric(name, metrics::ACK_RATE), ts, cd.ack_rate);
            h.record(
                &channel_metric(name, metrics::PUBLISH_P99),
                ts,
                cd.publish_latency.p99,
            );
            h.record(
                &channel_metric(name, metrics::LAG),
                ts,
                documents::channel_lag(ch) as f64,
            );
            h.record(&channel_metric(name, metrics::DLQ_RATE), ts, cd.dlq_rate);
        }
    }

    fn total_retries(stats: &BrokerStats) -> u64 {
        stats.channels.iter().map(|c| c.retries_total).sum()
    }

    fn sample_retries(&mut self, stats: &BrokerStats) {
        let now = stats.taken_at_ms;
        if self
            .retries
            .back()
            .is_none_or(|(t, _)| now.saturating_sub(*t) >= 60_000)
        {
            self.retries.push_back((now, Self::total_retries(stats)));
        }
        while self.retries.len() > 2 && self.retries[1].0 <= now.saturating_sub(DAY_MS) {
            self.retries.pop_front();
        }
    }

    fn retries_24h(&self, stats: &BrokerStats) -> u64 {
        let now = stats.taken_at_ms;
        let total = Self::total_retries(stats);
        if stats.started_at_ms + DAY_MS >= now {
            // Broker younger than a day: cumulative counters cover exactly its lifetime.
            return total;
        }
        match self
            .retries
            .iter()
            .rev()
            .find(|(t, _)| *t <= now - DAY_MS)
            .or(self.retries.front())
        {
            Some((_, base)) => counter_delta(total, *base),
            None => 0,
        }
    }
}

/// Overlays cluster facts on the (merged) overview document.
fn apply_cluster(doc: &mut fluxmq_proto::telemetry::ClusterOverview, info: &ClusterInfo) {
    let peers_healthy = info
        .peers
        .iter()
        .filter(|p| p.health != Health::Critical && !info.down.contains(&p.id))
        .count() as u32;
    let peers_up = info
        .peers
        .iter()
        .filter(|p| !info.down.contains(&p.id))
        .count() as u32;
    doc.nodes_total = 1 + info.peers.len() as u32;
    doc.nodes_up = 1 + peers_up;
    doc.nodes_healthy += peers_healthy;
    doc.partial = info.partial;
    let floor = if info.facts.offline > 0 {
        Health::Critical
    } else if info.partial || info.facts.under_replicated > 0 {
        Health::Warning
    } else {
        Health::Healthy
    };
    doc.health = doc.health.max(floor);
}

/// The async shell around [`Collector::tick`].
pub(crate) async fn run(
    mut collector: Collector,
    mut events: Option<broadcast::Receiver<BrokerEvent>>,
) {
    let Some(shared) = collector.shared.upgrade() else {
        return;
    };
    let tick = shared.config.tick.max(Duration::from_millis(1));
    let history_dir = shared.config.history_dir.clone();
    let data_dir = shared.data_dir.clone();
    let source = shared.source.clone();
    let mut shutdown = shared.subscribe_shutdown();
    let weak = collector.shared.clone();
    drop(shared);

    if let Some(dir) = history_dir.clone() {
        match tokio::task::spawn_blocking(move || history::load_blocking(&dir)).await {
            Ok(Ok(Some(file))) => {
                if let Some(s) = weak.upgrade() {
                    s.history.lock().load(file, fluxmq_proto::now_ms());
                }
            }
            Ok(Ok(None)) => {}
            Ok(Err(e)) => tracing::warn!(error = %e, "could not load telemetry history"),
            Err(e) => tracing::warn!(error = %e, "history load task failed"),
        }
    }

    let data_dir =
        data_dir.unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| ".".into()));
    // sysinfo touches the filesystem: keep it off the async workers.
    let mut sampler = tokio::task::spawn_blocking(move || NodeSampler::new(data_dir))
        .await
        .ok();
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_persist = Instant::now();

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = shutdown.changed() => break,
        }
        if *shutdown.borrow() {
            break;
        }
        let stats = source.stats();
        let broker_events = drain_events(&mut events);
        let node = if collector.nodes_due() {
            match sampler.take() {
                Some(mut s) => match tokio::task::spawn_blocking(move || {
                    let sample = s.sample();
                    (s, sample)
                })
                .await
                {
                    Ok((s, sample)) => {
                        sampler = Some(s);
                        Some(sample)
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "node sampler failed; node metrics disabled");
                        None
                    }
                },
                None => None,
            }
        } else {
            None
        };
        let cluster = weak
            .upgrade()
            .and_then(|s| s.cluster.read().clone())
            .map(|c| c.view(&stats, fluxmq_proto::now_ms()));
        if !collector.tick_with(stats, broker_events, node, cluster) {
            return;
        }
        if last_persist.elapsed() >= PERSIST_EVERY {
            last_persist = Instant::now();
            persist(&weak, history_dir.as_deref()).await;
        }
    }
    persist(&weak, history_dir.as_deref()).await;
}

async fn persist(shared: &Weak<Shared>, dir: Option<&std::path::Path>) {
    let (Some(dir), Some(shared)) = (dir, shared.upgrade()) else {
        return;
    };
    let file = shared.history.lock().to_persist(fluxmq_proto::now_ms());
    drop(shared);
    let dir = dir.to_path_buf();
    match tokio::task::spawn_blocking(move || history::save_blocking(&dir, &file)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(error = %e, "could not persist telemetry history"),
        Err(e) => tracing::warn!(error = %e, "history persist task failed"),
    }
}

fn drain_events(rx: &mut Option<broadcast::Receiver<BrokerEvent>>) -> Vec<BrokerEvent> {
    let mut out = Vec::new();
    let Some(r) = rx else { return out };
    loop {
        match r.try_recv() {
            Ok(e) => out.push(e),
            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                tracing::warn!(skipped = n, "telemetry lagged behind broker events");
            }
            Err(broadcast::error::TryRecvError::Empty) => break,
            Err(broadcast::error::TryRecvError::Closed) => {
                *rx = None;
                break;
            }
        }
    }
    out
}
