//! Dispatcher timelines on a paused clock: grouping window, repeat interval, resolved
//! messages, controller-only dispatch, disabled notifiers. Deliveries are intercepted via
//! `NotifyConfig::capture` (no network).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fluxmq_meta::{LocalMetaStore, SharedMetaStore};
use fluxmq_notify::{Captured, Leadership, NotifyConfig, NotifyService, Wiring};
use fluxmq_proto::notify::{
    CreateNotifierRequest, CreateRuleRequest, DeliveryStatus, NotifierKind, RuleMatch,
    UpdateNotifierRequest,
};
use fluxmq_proto::telemetry::{Alert, AlertKind, AlertsState, Event, EventKind, Health};
use fluxmq_telemetry::TelemetrySignal;
use parking_lot::Mutex;
use tokio::sync::{broadcast, mpsc};

struct Lead(AtomicBool);

impl Leadership for Lead {
    fn is_controller(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
    fn cluster_name(&self) -> String {
        "test".into()
    }
    fn node_id(&self) -> String {
        "n1".into()
    }
}

struct H {
    svc: NotifyService,
    signals: broadcast::Sender<TelemetrySignal>,
    out: mpsc::UnboundedReceiver<Captured>,
    lead: Arc<Lead>,
    open: Arc<Mutex<AlertsState>>,
    notifier: String,
}

async fn setup(controller: bool) -> H {
    let meta: SharedMetaStore = Arc::new(LocalMetaStore::in_memory());
    let (signals, rx) = broadcast::channel(64);
    let (tx, out) = mpsc::unbounded_channel();
    let lead = Arc::new(Lead(AtomicBool::new(controller)));
    let open: Arc<Mutex<AlertsState>> = Arc::default();
    let o = open.clone();
    let config = NotifyConfig {
        capture: Some(tx),
        ..NotifyConfig::default()
    };
    let svc = NotifyService::start_with(
        Wiring {
            meta,
            signals: rx,
            open_alerts: Arc::new(move || o.lock().clone()),
            leadership: lead.clone(),
            epoch: None,
            broker: None,
            cluster: None,
        },
        config,
    )
    .await
    .unwrap();
    let n = svc
        .create_notifier(CreateNotifierRequest {
            name: "hook".into(),
            enabled: true,
            kind: NotifierKind::Webhook {
                url: "https://example.invalid/hook".into(),
                secret: None,
                headers: Default::default(),
            },
        })
        .await
        .unwrap();
    H {
        svc,
        signals,
        out,
        lead,
        open,
        notifier: n.id,
    }
}

impl H {
    async fn rule(&self, group_wait: u32, repeat: u32, send_resolved: bool, events: &[EventKind]) {
        self.svc
            .create_rule(CreateRuleRequest {
                name: "ops".into(),
                enabled: true,
                notifier_ids: vec![self.notifier.clone()],
                matcher: RuleMatch {
                    min_severity: Health::Warning,
                    alert_kinds: vec![],
                    event_kinds: events.to_vec(),
                    channels: vec![],
                },
                group_wait_secs: Some(group_wait),
                repeat_interval_secs: Some(repeat),
                send_resolved: Some(send_resolved),
            })
            .await
            .unwrap();
    }

    fn open(&self, a: &Alert) {
        self.open.lock().insert(a.id.clone(), a.clone());
        self.signals
            .send(TelemetrySignal::AlertOpened(a.clone()))
            .unwrap();
    }

    fn resolve(&self, a: &Alert) {
        self.open.lock().remove(&a.id);
        self.signals
            .send(TelemetrySignal::AlertResolved(a.clone()))
            .unwrap();
    }

    fn drain(&mut self) -> Vec<Captured> {
        let mut v = Vec::new();
        while let Ok(c) = self.out.try_recv() {
            v.push(c);
        }
        v
    }
}

fn alert(id: &str, sev: Health) -> Alert {
    Alert {
        id: id.into(),
        severity: sev,
        kind: AlertKind::NoConsumers,
        title: format!("Alert {id}"),
        detail: "detail".into(),
        possible_reason: None,
        link: None,
        channel: Some("orders".into()),
        group: Some("billing".into()),
        since_ms: 1,
        params: Default::default(),
    }
}

async fn wait(secs: u64) {
    tokio::time::sleep(Duration::from_secs(secs)).await;
}

#[tokio::test(start_paused = true)]
async fn grouping_window_batches_alerts() {
    let mut h = setup(true).await;
    h.rule(30, 0, true, &[]).await;
    h.open(&alert("a", Health::Critical));
    wait(10).await;
    h.open(&alert("b", Health::Warning));
    wait(15).await;
    assert!(h.drain().is_empty(), "still inside the 30 s window");
    wait(6).await;
    let sent = h.drain();
    assert_eq!(sent.len(), 1);
    let p = &sent[0].payload;
    assert_eq!(p.status, "firing");
    assert_eq!(p.rule, "ops");
    assert_eq!(p.cluster, "test");
    let ids: Vec<&str> = p.alerts.iter().map(|a| a.id.as_str()).collect();
    assert_eq!(ids, ["a", "b"]);
    // A new alert after the flush starts a new window.
    h.open(&alert("c", Health::Warning));
    wait(31).await;
    let sent = h.drain();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].payload.alerts[0].id, "c");
    let hist = h.svc.history(10);
    assert_eq!(hist.len(), 2);
    assert!(hist.iter().all(|r| r.status == DeliveryStatus::Sent));
    assert_eq!(hist[0].summary, "firing: Alert c");
}

#[tokio::test(start_paused = true)]
async fn opened_and_resolved_within_window_is_silent() {
    let mut h = setup(true).await;
    h.rule(30, 0, true, &[]).await;
    let a = alert("a", Health::Critical);
    h.open(&a);
    wait(5).await;
    h.resolve(&a);
    wait(120).await;
    let sent = h.drain();
    assert!(sent.is_empty(), "{sent:?}");
}

#[tokio::test(start_paused = true)]
async fn repeat_interval_and_resolved() {
    let mut h = setup(true).await;
    h.rule(0, 60, true, &[]).await;
    let a = alert("a", Health::Critical);
    h.open(&a);
    wait(1).await;
    let sent = h.drain();
    assert_eq!(sent.len(), 1, "group_wait 0 sends right away");
    // Text changes while open; the repeat carries the latest version.
    let mut updated = a.clone();
    updated.detail = "worse".into();
    h.open.lock().insert(a.id.clone(), updated);
    wait(50).await;
    assert!(h.drain().is_empty());
    wait(11).await;
    let sent = h.drain();
    assert_eq!(sent.len(), 1, "repeated after 60 s");
    assert_eq!(sent[0].payload.status, "firing");
    assert_eq!(sent[0].payload.alerts[0].detail, "worse");
    wait(61).await;
    assert_eq!(h.drain().len(), 1, "and again");
    h.resolve(&a);
    wait(1).await;
    let sent = h.drain();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].payload.status, "resolved");
    wait(300).await;
    assert!(h.drain().is_empty(), "no repeats after resolve");
}

#[tokio::test(start_paused = true)]
async fn send_resolved_false_and_repeat_zero() {
    let mut h = setup(true).await;
    h.rule(0, 0, false, &[]).await;
    let a = alert("a", Health::Warning);
    h.open(&a);
    wait(1).await;
    assert_eq!(h.drain().len(), 1);
    wait(5 * 3600).await;
    assert!(h.drain().is_empty(), "repeat 0 = never");
    h.resolve(&a);
    wait(10).await;
    assert!(h.drain().is_empty(), "send_resolved = false");
}

#[tokio::test(start_paused = true)]
async fn events_are_grouped_and_opt_in() {
    let mut h = setup(true).await;
    h.rule(10, 0, true, &[EventKind::ChannelDeleted]).await;
    let ev = |id: u64, kind: EventKind| Event {
        id,
        ts: 0,
        kind,
        title: "Channel deleted".into(),
        subject: format!("ch{id}"),
        severity: Health::Warning,
        params: Default::default(),
    };
    for (i, k) in [
        EventKind::ChannelDeleted,
        EventKind::ChannelCreated,
        EventKind::ChannelDeleted,
    ]
    .into_iter()
    .enumerate()
    {
        h.signals
            .send(TelemetrySignal::Event(ev(i as u64, k)))
            .unwrap();
    }
    wait(11).await;
    let sent = h.drain();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].payload.status, "event");
    assert_eq!(sent[0].payload.events.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn only_the_controller_dispatches() {
    let mut h = setup(false).await;
    h.rule(0, 60, true, &[]).await;
    let a = alert("a", Health::Critical);
    h.open(&a);
    wait(120).await;
    assert!(h.drain().is_empty(), "follower never sends");
    // Becomes controller: `a` (open, notified by the old controller) is seeded — not
    // re-sent immediately, but repeated after the interval; new alerts go out normally.
    h.lead.0.store(true, Ordering::SeqCst);
    let b = alert("b", Health::Critical);
    h.open(&b);
    wait(1).await;
    let sent = h.drain();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].payload.alerts.len(), 1);
    assert_eq!(sent[0].payload.alerts[0].id, "b");
    wait(60).await;
    let ids: Vec<String> = h
        .drain()
        .iter()
        .flat_map(|c| c.payload.alerts.iter().map(|a| a.id.clone()))
        .collect();
    assert!(ids.contains(&"a".to_string()), "{ids:?}");
    // Loses leadership: pending work is dropped, nothing more is sent.
    h.lead.0.store(false, Ordering::SeqCst);
    let c = alert("c", Health::Critical);
    h.open(&c);
    wait(300).await;
    assert!(h.drain().is_empty());
}

#[tokio::test(start_paused = true)]
async fn disabled_notifier_is_suppressed() {
    let mut h = setup(true).await;
    h.rule(0, 0, true, &[]).await;
    h.svc
        .update_notifier(
            &h.notifier,
            UpdateNotifierRequest {
                enabled: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    h.open(&alert("a", Health::Critical));
    wait(1).await;
    assert!(h.drain().is_empty());
    let hist = h.svc.history(10);
    assert_eq!(hist.len(), 1);
    assert_eq!(hist[0].status, DeliveryStatus::Suppressed);
    assert_eq!(hist[0].attempts, 0);
}

#[tokio::test(start_paused = true)]
async fn severity_filter_applies() {
    let mut h = setup(true).await;
    h.svc
        .create_rule(CreateRuleRequest {
            name: "crit".into(),
            enabled: true,
            notifier_ids: vec![h.notifier.clone()],
            matcher: RuleMatch {
                min_severity: Health::Critical,
                alert_kinds: vec![],
                event_kinds: vec![],
                channels: vec!["orders*".into()],
            },
            group_wait_secs: Some(0),
            repeat_interval_secs: None,
            send_resolved: None,
        })
        .await
        .unwrap();
    h.open(&alert("w", Health::Warning));
    h.open(&alert("c", Health::Critical));
    wait(1).await;
    let sent = h.drain();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].payload.alerts.len(), 1);
    assert_eq!(sent[0].payload.alerts[0].id, "c");
}
