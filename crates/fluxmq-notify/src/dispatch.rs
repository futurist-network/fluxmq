//! The dispatcher task: telemetry signals → rule matching → per-(rule, notifier) groups →
//! deliveries; plus the `$system.events` feed appends. Runs everything only while this node
//! is the controller (a single node always is).
//!
//! Per group (rule, notifier):
//! * the first matching alert/event starts a `group_wait_secs` window; everything matching
//!   within it goes out as one message per status (firing / resolved / event);
//! * an alert opened and resolved inside the same window is dropped (nobody was told);
//! * notified alerts that stay open are re-sent every `repeat_interval_secs` (0 = never),
//!   with their latest text from the telemetry plane;
//! * a resolve of a notified alert produces a recovery message when `send_resolved`.
//!
//! Leadership: checked on every signal, on cluster epoch changes and every 5 s. Losing it
//! drops pending groups; gaining it (follower → controller while running) seeds the "already notified" set from the alerts that
//! are open right now (the previous controller notified them), so they come back only at
//! the repeat interval instead of as duplicates.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use fluxmq_proto::notify::{
    DeliveryStatus, FeedItem, NotificationPayload, NotificationRecord, NotificationRule,
};
use fluxmq_proto::now_ms;
use fluxmq_proto::telemetry::{Alert, Event, EventKind};
use fluxmq_telemetry::TelemetrySignal;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::matcher::{matches_alert, matches_event};
use crate::{Inner, new_id};

/// Max events buffered per group window.
const MAX_GROUP_EVENTS: usize = 100;
const LEADER_RECHECK: Duration = Duration::from_secs(5);

#[derive(Default)]
struct Group {
    firing: BTreeMap<String, Alert>,
    resolved: BTreeMap<String, Alert>,
    events: Vec<Event>,
    flush_at: Option<Instant>,
    /// Open alerts this group already sent → when last sent.
    notified: HashMap<String, Instant>,
}

impl Group {
    fn schedule(&mut self, at: Instant) {
        if self.flush_at.is_none() {
            self.flush_at = Some(at);
        }
    }

    fn is_idle(&self) -> bool {
        self.firing.is_empty()
            && self.resolved.is_empty()
            && self.events.is_empty()
            && self.notified.is_empty()
    }
}

type Key = (String, String);

pub(crate) struct Dispatcher {
    inner: Arc<Inner>,
    groups: HashMap<Key, Group>,
    /// `None` until the first check (startup never seeds: nothing was notified before).
    controller: Option<bool>,
    feed_tx: Option<mpsc::UnboundedSender<FeedItem>>,
}

impl Dispatcher {
    pub fn new(inner: Arc<Inner>, feed_tx: Option<mpsc::UnboundedSender<FeedItem>>) -> Self {
        Self {
            inner,
            groups: HashMap::new(),
            controller: None,
            feed_tx,
        }
    }

    pub async fn run(
        mut self,
        mut signals: broadcast::Receiver<TelemetrySignal>,
        mut epoch: Option<watch::Receiver<u64>>,
        mut stop: watch::Receiver<bool>,
    ) {
        self.check_leadership();
        let mut recheck = tokio::time::interval(LEADER_RECHECK);
        recheck.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut signals_open = true;
        loop {
            let deadline = self.next_deadline();
            let sleep = tokio::time::sleep_until(
                deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(3600)),
            );
            tokio::select! {
                _ = stop.changed() => break,
                r = signals.recv(), if signals_open => match r {
                    Ok(sig) => self.on_signal(sig),
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "notifications lagged behind telemetry signals");
                    }
                    Err(broadcast::error::RecvError::Closed) => signals_open = false,
                },
                r = async { epoch.as_mut().expect("guarded").changed().await }, if epoch.is_some() => {
                    if r.is_err() {
                        epoch = None;
                    }
                    self.check_leadership();
                }
                _ = recheck.tick() => {
                    self.check_leadership();
                    self.prune();
                }
                () = sleep, if deadline.is_some() => self.on_timer(Instant::now()),
            }
        }
    }

    fn check_leadership(&mut self) -> bool {
        let now = self.inner.leadership.is_controller();
        match (self.controller, now) {
            (Some(false), true) => {
                info!("notifications: this node became the controller; dispatching");
                self.seed(Instant::now());
            }
            (Some(true), false) => {
                info!("notifications: no longer the controller; dispatch paused");
                self.groups.clear();
            }
            _ => {}
        }
        self.controller = Some(now);
        now
    }

    /// Alerts open right now count as already notified (by the previous controller).
    fn seed(&mut self, now: Instant) {
        let open = (self.inner.open_alerts)();
        let rules = self.inner.rules_snapshot();
        for alert in open.values() {
            for rule in rules.iter().filter(|r| r.enabled) {
                if !matches_alert(&rule.matcher, alert) {
                    continue;
                }
                for nid in &rule.notifier_ids {
                    self.groups
                        .entry((rule.id.clone(), nid.clone()))
                        .or_default()
                        .notified
                        .entry(alert.id.clone())
                        .or_insert(now);
                }
            }
        }
    }

    fn on_signal(&mut self, sig: TelemetrySignal) {
        if !self.check_leadership() {
            return;
        }
        let now = Instant::now();
        self.feed(&sig);
        let rules = self.inner.rules_snapshot();
        match sig {
            TelemetrySignal::AlertOpened(alert) => {
                for rule in rules.iter().filter(|r| r.enabled) {
                    if !matches_alert(&rule.matcher, &alert) {
                        continue;
                    }
                    let at = now + Duration::from_secs(rule.group_wait_secs.into());
                    for nid in &rule.notifier_ids {
                        let g = self
                            .groups
                            .entry((rule.id.clone(), nid.clone()))
                            .or_default();
                        g.resolved.remove(&alert.id);
                        g.firing.insert(alert.id.clone(), alert.clone());
                        g.schedule(at);
                    }
                }
            }
            TelemetrySignal::AlertResolved(alert) => {
                let by_id: HashMap<&str, &NotificationRule> =
                    rules.iter().map(|r| (r.id.as_str(), r)).collect();
                for ((rule_id, _), g) in self.groups.iter_mut() {
                    let pending = g.firing.remove(&alert.id).is_some();
                    let notified = g.notified.remove(&alert.id).is_some();
                    if !notified {
                        // Opened and resolved within the window (or never ours): silence.
                        let _ = pending;
                        continue;
                    }
                    let Some(rule) = by_id.get(rule_id.as_str()) else {
                        continue;
                    };
                    if rule.enabled && rule.send_resolved {
                        g.resolved.insert(alert.id.clone(), alert.clone());
                        g.schedule(now + Duration::from_secs(rule.group_wait_secs.into()));
                    }
                }
            }
            TelemetrySignal::Event(event) => {
                for rule in rules.iter().filter(|r| r.enabled) {
                    if !matches_event(&rule.matcher, &event) {
                        continue;
                    }
                    let at = now + Duration::from_secs(rule.group_wait_secs.into());
                    for nid in &rule.notifier_ids {
                        let g = self
                            .groups
                            .entry((rule.id.clone(), nid.clone()))
                            .or_default();
                        if g.events.len() < MAX_GROUP_EVENTS {
                            g.events.push(event.clone());
                        }
                        g.schedule(at);
                    }
                }
            }
        }
    }

    /// Appends the signal to the `$system.events` feed (one copy: controller only). Events
    /// for alert transitions are skipped (the transition itself is in the feed), and so are
    /// events about system channels (e.g. the feed channel's own creation).
    fn feed(&self, sig: &TelemetrySignal) {
        let Some(tx) = &self.feed_tx else {
            return;
        };
        let node = self.inner.leadership.node_id();
        let item = match sig {
            TelemetrySignal::AlertOpened(a) => FeedItem {
                cursor: String::new(),
                ts: now_ms(),
                kind: "alert_opened".into(),
                alert: Some(a.clone()),
                event: None,
                node,
            },
            TelemetrySignal::AlertResolved(a) => FeedItem {
                cursor: String::new(),
                ts: now_ms(),
                kind: "alert_resolved".into(),
                alert: Some(a.clone()),
                event: None,
                node,
            },
            TelemetrySignal::Event(e) => {
                if matches!(e.kind, EventKind::AlertOpened | EventKind::AlertResolved)
                    || crate::matcher::event_channel(e)
                        .is_some_and(fluxmq_proto::model::is_system_channel)
                {
                    return;
                }
                FeedItem {
                    cursor: String::new(),
                    ts: e.ts,
                    kind: "event".into(),
                    alert: None,
                    event: Some(e.clone()),
                    node,
                }
            }
        };
        let _ = tx.send(item);
    }

    fn next_deadline(&self) -> Option<Instant> {
        let rules = self.inner.rules_snapshot();
        let repeat: HashMap<&str, u32> = rules
            .iter()
            .map(|r| (r.id.as_str(), r.repeat_interval_secs))
            .collect();
        let mut best: Option<Instant> = None;
        let mut consider = |t: Instant| {
            best = Some(best.map_or(t, |b| b.min(t)));
        };
        for ((rule_id, _), g) in &self.groups {
            if let Some(t) = g.flush_at {
                consider(t);
            }
            if let Some(&secs) = repeat.get(rule_id.as_str())
                && secs > 0
                && let Some(oldest) = g.notified.values().min()
            {
                consider(*oldest + Duration::from_secs(secs.into()));
            }
        }
        best
    }

    fn on_timer(&mut self, now: Instant) {
        if self.controller != Some(true) {
            return;
        }
        let rules = self.inner.rules_snapshot();
        let by_id: HashMap<&str, &NotificationRule> =
            rules.iter().map(|r| (r.id.as_str(), r)).collect();
        // Repeats → pending firing with the latest alert text.
        let open = (self.inner.open_alerts)();
        for ((rule_id, _), g) in self.groups.iter_mut() {
            let Some(rule) = by_id.get(rule_id.as_str()) else {
                continue;
            };
            if rule.repeat_interval_secs == 0 {
                continue;
            }
            let every = Duration::from_secs(rule.repeat_interval_secs.into());
            let due: Vec<String> = g
                .notified
                .iter()
                .filter(|(_, last)| now >= **last + every)
                .map(|(id, _)| id.clone())
                .collect();
            for id in due {
                match open.get(&id) {
                    Some(a) => {
                        g.firing.insert(id.clone(), a.clone());
                        // Pushed forward now so a failing flush can't spin.
                        g.notified.insert(id, now);
                        g.schedule(now);
                    }
                    // Resolved while we weren't looking (lagged signals).
                    None => {
                        g.notified.remove(&id);
                    }
                }
            }
        }
        let due: Vec<Key> = self
            .groups
            .iter()
            .filter(|(_, g)| g.flush_at.is_some_and(|t| t <= now))
            .map(|(k, _)| k.clone())
            .collect();
        for key in due {
            self.flush(&key, &by_id, now);
        }
        self.groups
            .retain(|_, g| !g.is_idle() || g.flush_at.is_some());
    }

    fn flush(&mut self, key: &Key, rules: &HashMap<&str, &NotificationRule>, now: Instant) {
        let Some(g) = self.groups.get_mut(key) else {
            return;
        };
        g.flush_at = None;
        let firing: Vec<Alert> = std::mem::take(&mut g.firing).into_values().collect();
        let resolved: Vec<Alert> = std::mem::take(&mut g.resolved).into_values().collect();
        let events = std::mem::take(&mut g.events);
        let (rule_id, notifier_id) = key;
        let Some(rule) = rules.get(rule_id.as_str()).filter(|r| r.enabled) else {
            g.notified.clear();
            return;
        };
        for a in &firing {
            g.notified.insert(a.id.clone(), now);
        }
        let Some(notifier) = self.inner.notifier(notifier_id) else {
            debug!(rule = %rule_id, notifier = %notifier_id, "notifier gone; dropping");
            return;
        };
        let cluster = self.inner.leadership.cluster_name();
        let console_url = self.inner.senders.config().public_url.clone();
        let mut payloads = Vec::new();
        let base = |status: &str, alerts: Vec<Alert>, events: Vec<Event>| NotificationPayload {
            id: new_id(),
            cluster: cluster.clone(),
            sent_at_ms: now_ms(),
            rule: rule.name.clone(),
            status: status.into(),
            alerts,
            events,
            console_url: console_url.clone(),
        };
        if !firing.is_empty() {
            payloads.push(base("firing", firing, vec![]));
        }
        if !resolved.is_empty() {
            payloads.push(base("resolved", resolved, vec![]));
        }
        if !events.is_empty() {
            payloads.push(base("event", vec![], events));
        }
        for payload in payloads {
            let summary = summary(&payload);
            if !notifier.enabled {
                self.inner.record(NotificationRecord {
                    id: payload.id.clone(),
                    ts: now_ms(),
                    rule_id: Some(rule.id.clone()),
                    notifier_id: notifier.id.clone(),
                    status: DeliveryStatus::Suppressed,
                    summary: format!("{summary} (notifier disabled)"),
                    attempts: 0,
                    error: None,
                    error_detail: None,
                });
                continue;
            }
            let inner = self.inner.clone();
            let kind = notifier.kind.clone();
            let notifier_id = notifier.id.clone();
            let rule_id = rule.id.clone();
            tokio::spawn(async move {
                let out = inner.senders.deliver(&kind, &payload).await;
                if let Some(e) = &out.error {
                    let detail = out.error_detail.as_deref().unwrap_or("");
                    warn!(notifier = %notifier_id, rule = %rule_id, attempts = out.attempts, error = %e, detail, "notification failed");
                } else {
                    debug!(notifier = %notifier_id, rule = %rule_id, "notification sent");
                }
                inner.record(NotificationRecord {
                    id: payload.id.clone(),
                    ts: now_ms(),
                    rule_id: Some(rule_id),
                    notifier_id,
                    status: if out.error.is_none() {
                        DeliveryStatus::Sent
                    } else {
                        DeliveryStatus::Failed
                    },
                    summary,
                    attempts: out.attempts,
                    error: out.error,
                    error_detail: out.error_detail,
                });
            });
        }
    }

    /// Drops groups whose rule is gone/disabled or no longer lists the notifier.
    fn prune(&mut self) {
        let rules = self.inner.rules_snapshot();
        self.groups.retain(|(rule_id, nid), _| {
            rules
                .iter()
                .any(|r| r.enabled && &r.id == rule_id && r.notifier_ids.contains(nid))
        });
    }
}

/// One-line history summary: `firing: <title>` / `firing: 3 alerts` / `event: 2 events`.
pub(crate) fn summary(p: &NotificationPayload) -> String {
    let what = match (p.alerts.as_slice(), p.events.as_slice()) {
        ([a], []) => a.title.clone(),
        ([], [e]) => format!("{} {}", e.title, e.subject),
        ([], []) => "test message".into(),
        (a, []) => format!("{} alerts", a.len()),
        (_, e) => format!("{} events", e.len()),
    };
    crate::render::truncate(&format!("{}: {what}", p.status), 200)
}
