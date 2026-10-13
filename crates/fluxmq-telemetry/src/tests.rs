//! Hub / session / collector tests over a fake [`Source`] (no running core needed).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fluxmq_core::{BrokerEvent, BrokerStats};
use fluxmq_proto::model::{ChannelConfig, Message};
use fluxmq_proto::telemetry::{
    AlertKind, ClientMsg, ClusterOverview, ConsumersState, EventKind, EventsFeed, HistoryRange,
    ServerMsg, TailState,
};
use parking_lot::Mutex;
use serde_json::Value;
use tokio::sync::{broadcast, mpsc};

use crate::collector::Collector;
use crate::patch::apply;
use crate::rates::tests::{channel, empty_stats, group, instance};
use crate::source::{BoxFuture, Source, TailFeed};
use crate::{Session, TelemetryConfig, TelemetryHub};

struct FakeTail {
    rx: mpsc::UnboundedReceiver<Message>,
    published: Arc<AtomicU64>,
}

impl TailFeed for FakeTail {
    fn recv(&mut self) -> BoxFuture<'_, Option<Message>> {
        Box::pin(self.rx.recv())
    }
    fn take_published_count(&mut self) -> u64 {
        self.published.swap(0, Ordering::Relaxed)
    }
}

/// Test-side handle of an open fake tail: message sender + published counter.
type TailHandle = (mpsc::UnboundedSender<Message>, Arc<AtomicU64>);

#[derive(Default)]
struct FakeSource {
    stats: Mutex<Option<BrokerStats>>,
    tails: Mutex<HashMap<String, TailHandle>>,
    tail_opens: AtomicU64,
    events: Mutex<Option<broadcast::Sender<BrokerEvent>>>,
}

impl FakeSource {
    fn set(&self, s: BrokerStats) {
        *self.stats.lock() = Some(s);
    }
    fn current(&self) -> BrokerStats {
        self.stats.lock().clone().unwrap_or_else(|| empty_stats(0))
    }
}

impl Source for FakeSource {
    fn stats(&self) -> BrokerStats {
        self.current()
    }
    fn channel(&self, name: &str) -> Option<ChannelConfig> {
        self.current()
            .channels
            .into_iter()
            .find(|c| c.config.name == name)
            .map(|c| c.config)
    }
    fn events(&self) -> Option<broadcast::Receiver<BrokerEvent>> {
        self.events.lock().as_ref().map(|s| s.subscribe())
    }
    fn tail(&self, channel: &str) -> Result<Box<dyn TailFeed>, String> {
        if self.channel(channel).is_none() {
            return Err(format!("channel '{channel}' not found"));
        }
        self.tail_opens.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel();
        let published = Arc::new(AtomicU64::new(0));
        self.tails
            .lock()
            .insert(channel.to_string(), (tx, published.clone()));
        Ok(Box::new(FakeTail { rx, published }))
    }
}

fn config() -> TelemetryConfig {
    // 1 s tick → every cadence fires on every tick.
    TelemetryConfig {
        tick: Duration::from_secs(1),
        rate_window: Duration::from_secs(2),
        history_dir: None,
    }
}

fn setup() -> (Arc<FakeSource>, TelemetryHub, Collector) {
    let src = Arc::new(FakeSource::default());
    src.set(stats(0, 0, 0));
    let (hub, collector) = TelemetryHub::build(src.clone(), "n1".into(), None, config());
    (src, hub, collector)
}

/// One channel `orders` with group `billing` (1 instance).
fn stats(ts: u64, published: u64, lag: u64) -> BrokerStats {
    let mut s = empty_stats(ts);
    let mut c = channel("orders");
    c.published_total = published;
    c.partitions[0].published_total = published;
    c.partitions[0].high_offset = published;
    let mut g = group("billing");
    g.lag = lag;
    g.acked_total = published.saturating_sub(lag);
    g.instances.push(instance("billing-svc#1"));
    c.groups.push(g);
    s.channels.push(c);
    s
}

fn tick(src: &FakeSource, c: &mut Collector, s: BrokerStats) {
    src.set(s.clone());
    assert!(c.tick(s, vec![], None));
}

fn sub(topics: &[&str]) -> ClientMsg {
    ClientMsg::Subscribe {
        topics: topics.iter().map(|s| s.to_string()).collect(),
    }
}

async fn next_now(s: &mut Session) -> Option<ServerMsg> {
    tokio::time::timeout(Duration::from_millis(200), s.next())
        .await
        .ok()
        .flatten()
}

fn snapshot_data(m: &ServerMsg) -> (String, u64, Value) {
    match m {
        ServerMsg::Snapshot {
            topic, seq, data, ..
        } => (topic.clone(), *seq, data.clone()),
        other => panic!("expected snapshot, got {other:?}"),
    }
}

fn delta_data(m: &ServerMsg) -> (String, u64, Value) {
    match m {
        ServerMsg::Delta {
            topic, seq, data, ..
        } => (topic.clone(), *seq, data.clone()),
        other => panic!("expected delta, got {other:?}"),
    }
}

#[tokio::test]
async fn hello_and_ping() {
    let (_src, hub, _c) = setup();
    let mut s = hub.session();
    match s.hello() {
        ServerMsg::Hello {
            contract_version,
            broker_version,
            node_id,
            server_time_ms,
        } => {
            assert_eq!(contract_version, 1);
            assert_eq!(broker_version, fluxmq_proto::VERSION);
            assert_eq!(node_id, "n1");
            assert!(server_time_ms > 0);
        }
        other => panic!("{other:?}"),
    }
    let out = s.handle(ClientMsg::Ping { nonce: Some(7) });
    assert!(matches!(out[..], [ServerMsg::Pong { nonce: Some(7), .. }]));
}

#[tokio::test]
async fn unknown_topic_and_channel_are_errors() {
    let (src, hub, mut c) = setup();
    tick(&src, &mut c, stats(0, 0, 0));
    let mut s = hub.session();
    let out = s.handle(sub(&[
        "nope",
        "channel.missing",
        "channel.missing.tail",
        "channel.orders",
    ]));
    assert_eq!(out.len(), 4);
    assert_eq!(
        out[0],
        ServerMsg::Error {
            topic: Some("nope".into()),
            message: "unknown topic".into()
        }
    );
    assert_eq!(
        out[1],
        ServerMsg::Error {
            topic: Some("channel.missing".into()),
            message: "unknown channel".into()
        }
    );
    assert!(
        matches!(&out[2], ServerMsg::Error { topic: Some(t), .. } if t == "channel.missing.tail")
    );
    assert!(
        matches!(&out[3], ServerMsg::Snapshot { topic, seq: 1, .. } if topic == "channel.orders")
    );
    assert_eq!(s.topics(), vec!["channel.orders".to_string()]);
}

#[tokio::test]
async fn snapshot_then_deltas() {
    let (src, hub, mut c) = setup();
    tick(&src, &mut c, stats(0, 0, 0));
    let mut s = hub.session();
    let out = s.handle(sub(&["cluster.overview"]));
    let (topic, seq, mut state) = snapshot_data(&out[0]);
    assert_eq!((topic.as_str(), seq), ("cluster.overview", 1));
    let ov: ClusterOverview = serde_json::from_value(state.clone()).expect("cluster overview");
    assert_eq!(ov.cluster_name, "local");
    assert_eq!(ov.channels, 1);
    // Nothing pending.
    assert!(next_now(&mut s).await.is_none());

    tick(&src, &mut c, stats(1_000, 500, 0));
    let m = next_now(&mut s).await.expect("delta");
    let (_, seq, patch) = delta_data(&m);
    assert_eq!(seq, 2);
    assert_eq!(patch["ingress_rate"], 500.0);
    assert!(
        patch.get("cluster_name").is_none(),
        "unchanged fields are omitted: {patch}"
    );
    apply(&mut state, &patch);
    let ov: ClusterOverview = serde_json::from_value(state).expect("patched overview");
    assert_eq!(ov.ingress_rate, 500.0);
    assert_eq!(ov.messages_total, 500);
}

#[tokio::test]
async fn slow_session_coalesces_latest_wins() {
    let (src, hub, mut c) = setup();
    tick(&src, &mut c, stats(0, 0, 0));
    let mut s = hub.session();
    let (_, _, mut state) = snapshot_data(&s.handle(sub(&["channel.orders"]))[0]);
    for i in 1..=5u64 {
        tick(&src, &mut c, stats(i * 1_000, i * 100, i));
    }
    let m = next_now(&mut s).await.expect("one coalesced delta");
    let (_, seq, patch) = delta_data(&m);
    assert_eq!(seq, 2);
    apply(&mut state, &patch);
    let current = hub.shared.topic("channel.orders").expect("doc");
    assert_eq!(state, *current.doc);
    assert!(
        next_now(&mut s).await.is_none(),
        "no queued intermediate deltas"
    );
}

#[tokio::test]
async fn unsubscribe_stops_deltas_and_refcounts() {
    let (src, hub, mut c) = setup();
    tick(&src, &mut c, stats(0, 0, 0));
    // Channel docs are not computed without subscribers.
    assert!(hub.shared.topic("channel.orders").is_none());
    assert!(hub.shared.topic("channel.orders.consumers").is_none());

    let mut a = hub.session();
    let mut b = hub.session();
    a.handle(sub(&["channel.orders.consumers", "events"]));
    let out = b.handle(sub(&["channel.orders.consumers"]));
    let (_, _, data) = snapshot_data(&out[0]);
    let cons: ConsumersState = serde_json::from_value(data).expect("consumers");
    assert_eq!(cons["billing"].instances.len(), 1);
    // Subscribing twice in one session does not double-count.
    b.handle(sub(&["channel.orders.consumers"]));
    assert_eq!(hub.shared.refcount("channel.orders.consumers"), 2);

    drop(b);
    assert_eq!(hub.shared.refcount("channel.orders.consumers"), 1);
    a.handle(ClientMsg::Unsubscribe {
        topics: vec!["channel.orders.consumers".into()],
    });
    assert_eq!(hub.shared.refcount("channel.orders.consumers"), 0);
    assert!(hub.shared.topic("channel.orders.consumers").is_none());

    tick(&src, &mut c, stats(1_000, 100, 5));
    assert!(
        hub.shared.topic("channel.orders.consumers").is_none(),
        "not recomputed after unsubscribe"
    );
    // `events` unchanged → nothing for the remaining subscription either.
    assert!(next_now(&mut a).await.is_none());
}

#[tokio::test]
async fn subscribe_before_first_tick_gets_snapshot_via_next() {
    let (src, hub, mut c) = setup();
    let mut s = hub.session();
    assert!(s.handle(sub(&["cluster.overview"])).is_empty());
    tick(&src, &mut c, stats(0, 0, 0));
    let m = next_now(&mut s).await.expect("snapshot");
    let (topic, seq, _) = snapshot_data(&m);
    assert_eq!((topic.as_str(), seq), ("cluster.overview", 1));
}

#[tokio::test]
async fn next_is_cancel_safe() {
    let (src, hub, mut c) = setup();
    tick(&src, &mut c, stats(0, 0, 0));
    let mut s = hub.session();
    s.handle(sub(&["cluster.overview"]));
    // Cancel a pending next() a few times; no state may be lost.
    for _ in 0..3 {
        assert!(
            tokio::time::timeout(Duration::from_millis(10), s.next())
                .await
                .is_err()
        );
    }
    tick(&src, &mut c, stats(1_000, 10, 0));
    let (_, seq, _) = delta_data(&next_now(&mut s).await.expect("delta"));
    assert_eq!(seq, 2);
}

#[tokio::test]
async fn shutdown_ends_sessions() {
    let (src, hub, mut c) = setup();
    tick(&src, &mut c, stats(0, 0, 0));
    let mut s = hub.session();
    s.handle(sub(&["cluster.overview"]));
    let waiter = tokio::spawn(async move { s.next().await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    hub.shutdown();
    let r = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("woken")
        .expect("joined");
    assert!(r.is_none());
}

#[tokio::test]
async fn alerts_open_and_emit_events() {
    let (src, hub, mut c) = setup();
    c.set_alert_hysteresis(2, 2);
    let mut s = hub.session();
    // Lag with no instances → NoConsumers (critical).
    let mut st = stats(0, 100, 100);
    st.channels[0].groups[0].instances.clear();
    for i in 0..3u64 {
        st.taken_at_ms = i * 1_000;
        tick(&src, &mut c, st.clone());
    }
    let alerts = hub.alerts();
    let a = alerts
        .get("no_consumers:orders/billing")
        .expect("alert open");
    assert_eq!(a.kind, AlertKind::NoConsumers);
    assert_eq!(a.link.as_deref(), Some("/channels/orders/consumers"));

    let out = s.handle(sub(&["events", "alerts", "cluster.overview"]));
    let (_, _, ev) = snapshot_data(&out[0]);
    let feed: EventsFeed = serde_json::from_value(ev).expect("events");
    assert_eq!(feed.events[0].kind, EventKind::AlertOpened);
    assert_eq!(feed.events[0].title, "No consumers for Orders");
    let (_, _, ov) = snapshot_data(&out[2]);
    assert_eq!(ov["open_alerts"], 1);
    assert_eq!(ov["health"], "critical");

    // Consumer comes back → resolves after 2 clean evaluations.
    let mut st = stats(3_000, 100, 0);
    for i in 3..6u64 {
        st.taken_at_ms = i * 1_000;
        tick(&src, &mut c, st.clone());
    }
    assert!(hub.alerts().is_empty());
    let mut saw_resolved = false;
    while let Some(m) = next_now(&mut s).await {
        if let ServerMsg::Delta { topic, data, .. } = &m
            && topic == "events"
        {
            let feed: EventsFeed =
                serde_json::from_value(data.clone()).expect("events replace whole list");
            saw_resolved |= feed.events[0].kind == EventKind::AlertResolved;
        }
    }
    assert!(saw_resolved);
}

#[tokio::test]
async fn broker_events_feed() {
    let (src, hub, mut c) = setup();
    let mut s = hub.session();
    tick(&src, &mut c, stats(0, 0, 0));
    s.handle(sub(&["events"]));
    let st = stats(1_000, 0, 0);
    src.set(st.clone());
    c.tick(
        st,
        vec![BrokerEvent::ChannelCreated {
            channel: "audit.logs".into(),
        }],
        None,
    );
    let (_, seq, data) = delta_data(&next_now(&mut s).await.expect("delta"));
    assert_eq!(seq, 2);
    assert_eq!(data["events"][0]["title"], "Channel created");
    assert_eq!(data["events"][0]["subject"], "audit.logs");
}

#[tokio::test]
async fn history_is_recorded() {
    let (src, hub, mut c) = setup();
    let base = fluxmq_proto::now_ms() - 5_000;
    for i in 0..5u64 {
        tick(&src, &mut c, stats(base + i * 1_000, i * 100, 0));
    }
    let h = hub
        .history("ingress_rate", HistoryRange::M5)
        .expect("valid metric");
    assert!(h.points.len() >= 4, "{:?}", h.points);
    assert!(h.points.iter().any(|(_, v)| *v == 100.0), "{:?}", h.points);
    let h = hub
        .history("channel.orders.lag", HistoryRange::M15)
        .expect("valid metric");
    assert!(!h.points.is_empty());
    assert!(hub.history("bogus", HistoryRange::M5).is_err());
}

#[tokio::test]
async fn tail_pump_lifecycle() {
    let (src, hub, mut c) = setup();
    tick(&src, &mut c, stats(0, 0, 0));
    let mut a = hub.session();
    let mut b = hub.session();
    let out = a.handle(sub(&["channel.orders.tail"]));
    let (_, _, data) = snapshot_data(&out[0]);
    let t: TailState = serde_json::from_value(data).expect("tail state");
    assert!(t.messages.is_empty());
    b.handle(sub(&["channel.orders.tail"]));
    assert_eq!(
        src.tail_opens.load(Ordering::Relaxed),
        1,
        "one pump per channel"
    );

    let (tx, published) = src
        .tails
        .lock()
        .get("orders")
        .cloned()
        .expect("tail opened");
    // Let the pump start and consume its first interval tick.
    tokio::time::sleep(Duration::from_millis(50)).await;
    for i in 0..3u64 {
        published.fetch_add(2, Ordering::Relaxed);
        tx.send(Message {
            partition: 0,
            offset: i,
            key: None,
            headers: Default::default(),
            timestamp_ms: 1,
            payload: format!("msg-{i}").into_bytes().into(),
        })
        .expect("pump alive");
    }
    let m = tokio::time::timeout(Duration::from_secs(2), a.next())
        .await
        .expect("flush")
        .expect("delta");
    let (topic, _, data) = delta_data(&m);
    assert_eq!(topic, "channel.orders.tail");
    let msgs = data["messages"].as_array().expect("batch replaces array");
    assert_eq!(msgs.len(), 3);
    assert_eq!(msgs[0]["payload_preview"], "msg-0");
    assert_eq!(data["sample_ratio"], 0.5);

    // A new subscriber gets the recent buffer as snapshot.
    let mut late = hub.session();
    let (_, _, snap) = snapshot_data(&late.handle(sub(&["channel.orders.tail"]))[0]);
    assert_eq!(snap["messages"].as_array().map(Vec::len), Some(3));

    drop(late);
    drop(b);
    a.handle(ClientMsg::Unsubscribe {
        topics: vec!["channel.orders.tail".into()],
    });
    assert_eq!(hub.shared.refcount("channel.orders.tail"), 0);
    // Pump aborted → its receiver is dropped.
    tokio::time::timeout(Duration::from_secs(1), tx.closed())
        .await
        .expect("pump stopped");
}

#[tokio::test]
async fn running_hub_with_spawned_collector() {
    let src = Arc::new(FakeSource::default());
    src.set(stats(fluxmq_proto::now_ms(), 0, 0));
    let (etx, _) = broadcast::channel(16);
    *src.events.lock() = Some(etx.clone());
    let cfg = TelemetryConfig {
        tick: Duration::from_millis(20),
        ..config()
    };
    let hub =
        TelemetryHub::start_with_source(src.clone(), "n1".into(), Some(std::env::temp_dir()), cfg);
    let mut s = hub.session();
    s.handle(sub(&["events", "nodes"]));
    etx.send(BrokerEvent::ConsumerConnected {
        channel: "orders".into(),
        group: "billing".into(),
        instance: "email-service#3".into(),
    })
    .expect("receiver exists");
    let mut got_event = false;
    let mut got_nodes = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while !(got_event && got_nodes) && tokio::time::Instant::now() < deadline {
        let Ok(Some(m)) = tokio::time::timeout(Duration::from_millis(500), s.next()).await else {
            continue;
        };
        let (topic, data) = match &m {
            ServerMsg::Snapshot { topic, data, .. } | ServerMsg::Delta { topic, data, .. } => {
                (topic.clone(), data.clone())
            }
            _ => continue,
        };
        if topic == "events" {
            got_event |= data["events"]
                .as_array()
                .is_some_and(|e| e.iter().any(|e| e["subject"] == "email-service"));
        }
        if topic == "nodes" {
            got_nodes |= data["n1"]["memory_total_bytes"].as_u64().unwrap_or(0) > 0;
        }
    }
    assert!(got_event, "consumer connected event delivered");
    assert!(got_nodes, "node sample delivered");
    hub.shutdown();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), s.next())
            .await
            .expect("ends")
            .is_none()
    );
}

#[test]
fn public_handles_are_send_and_sync() {
    fn send<T: Send>(_: &T) {}
    fn send_sync<T: Send + Sync>() {}
    send_sync::<TelemetryHub>();
    send_sync::<Session>();
    let (_src, hub, _c) = setup();
    let mut s = hub.session();
    // The `next()` future is polled from spawned (multi-threaded) transport tasks.
    send(&s.next());
}

#[tokio::test]
async fn notifications_signal_transitions_and_events_once() {
    use crate::TelemetrySignal;
    let (src, hub, mut c) = setup();
    c.set_alert_hysteresis(2, 2);
    let mut rx1 = hub.notifications();
    let mut rx2 = hub.notifications();
    let mut st = stats(0, 100, 100);
    st.channels[0].groups[0].instances.clear();
    for i in 0..3u64 {
        st.taken_at_ms = i * 1_000;
        tick(&src, &mut c, st.clone());
    }
    let mut st = stats(3_000, 100, 0);
    for i in 3..6u64 {
        st.taken_at_ms = i * 1_000;
        tick(&src, &mut c, st.clone());
    }
    let drain = |rx: &mut tokio::sync::broadcast::Receiver<TelemetrySignal>| {
        let mut v = Vec::new();
        while let Ok(s) = rx.try_recv() {
            v.push(s);
        }
        v
    };
    let a = drain(&mut rx1);
    assert_eq!(a, drain(&mut rx2), "every listener sees the same signals");
    let opened: Vec<_> = a
        .iter()
        .filter_map(|s| match s {
            TelemetrySignal::AlertOpened(a) => Some(a.id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(opened, ["no_consumers:orders/billing"]);
    let resolved = a
        .iter()
        .filter(|s| matches!(s, TelemetrySignal::AlertResolved(_)))
        .count();
    assert_eq!(resolved, 1);
    let kinds: Vec<EventKind> = a
        .iter()
        .filter_map(|s| match s {
            TelemetrySignal::Event(e) => Some(e.kind),
            _ => None,
        })
        .collect();
    assert_eq!(kinds, [EventKind::AlertOpened, EventKind::AlertResolved]);
    // Opened precedes its event, resolved precedes its event.
    assert!(matches!(a[0], TelemetrySignal::AlertOpened(_)));
}
