//! End-to-end: a real `Broker` in a temp dir feeding a running `TelemetryHub`.

use std::collections::BTreeMap;
use std::time::Duration;

use fluxmq_core::{Broker, BrokerConfig, SubscriberInfo};
use fluxmq_proto::model::{ChannelConfig, ChannelMode, PublishMessage, StartFrom};
use fluxmq_proto::telemetry::{
    ChannelDetail, ChannelsSummary, ClientMsg, ClusterOverview, ConsumersState, EventsFeed,
    HistoryRange, ServerMsg, TailState,
};
use fluxmq_telemetry::patch::apply;
use fluxmq_telemetry::{Session, TelemetryConfig, TelemetryHub};
use serde_json::Value;

/// Client-side view: applies snapshots and merge-patch deltas, checking seq continuity.
#[derive(Default)]
struct Mirror {
    docs: BTreeMap<String, Value>,
    seqs: BTreeMap<String, u64>,
    tail_seen: usize,
}

impl Mirror {
    fn on(&mut self, m: ServerMsg) {
        match m {
            ServerMsg::Snapshot {
                topic, seq, data, ..
            } => {
                if topic.ends_with(".tail") {
                    self.tail_seen += data["messages"].as_array().map_or(0, Vec::len);
                }
                self.docs.insert(topic.clone(), data);
                self.seqs.insert(topic, seq);
            }
            ServerMsg::Delta {
                topic, seq, data, ..
            } => {
                let last = self
                    .seqs
                    .get(&topic)
                    .copied()
                    .expect("delta before snapshot");
                assert_eq!(seq, last + 1, "seq gap on {topic}");
                self.seqs.insert(topic.clone(), seq);
                if topic.ends_with(".tail") {
                    self.tail_seen += data
                        .get("messages")
                        .and_then(Value::as_array)
                        .map_or(0, Vec::len);
                }
                apply(self.docs.get_mut(&topic).expect("doc"), &data);
            }
            ServerMsg::Error { topic, message } => panic!("error on {topic:?}: {message}"),
            _ => {}
        }
    }

    fn get<T: serde::de::DeserializeOwned>(&self, topic: &str) -> Option<T> {
        self.docs
            .get(topic)
            .map(|v| serde_json::from_value(v.clone()).expect("doc matches proto type"))
    }
}

async fn pump_until(
    session: &mut Session,
    mirror: &mut Mirror,
    timeout: Duration,
    mut done: impl FnMut(&Mirror) -> bool,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !done(mirror) {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !left.is_zero(),
            "timed out; docs: {:#?}",
            mirror.docs.keys().collect::<Vec<_>>()
        );
        if let Ok(Some(m)) =
            tokio::time::timeout(left.min(Duration::from_millis(200)), session.next()).await
        {
            mirror.on(m);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_over_real_broker() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = Broker::open(BrokerConfig::new(dir.path().join("data")))
        .await
        .expect("open broker");
    broker
        .create_channel(ChannelConfig::new("orders", ChannelMode::Stream))
        .expect("create channel");

    let config = TelemetryConfig {
        tick: Duration::from_millis(50),
        rate_window: Duration::from_millis(500),
        history_dir: Some(dir.path().join("telemetry")),
    };
    let hub = TelemetryHub::start(broker.clone(), config);
    let mut session = hub.session();
    let mut mirror = Mirror::default();
    assert!(matches!(
        session.hello(),
        ServerMsg::Hello {
            contract_version: 1,
            ..
        }
    ));

    let topics = [
        "cluster.overview",
        "channels.summary",
        "channel.orders",
        "channel.orders.consumers",
        "channel.orders.tail",
        "events",
        "alerts",
        "nodes",
    ];
    for m in session.handle(ClientMsg::Subscribe {
        topics: topics.iter().map(|t| t.to_string()).collect(),
    }) {
        mirror.on(m);
    }
    let errs = session.handle(ClientMsg::Subscribe {
        topics: vec!["channel.nope".into()],
    });
    assert!(matches!(&errs[..], [ServerMsg::Error { .. }]));

    // Lifecycle event after the hub started.
    broker
        .create_channel(ChannelConfig::new("audit.logs", ChannelMode::Queue))
        .expect("create channel");

    let mut sub = broker
        .subscribe(
            "orders",
            "billing",
            SubscriberInfo {
                client_name: "billing-svc".into(),
                remote_addr: "127.0.0.1:9".into(),
                connection_id: 1,
            },
            50,
            StartFrom::Earliest,
        )
        .expect("subscribe");

    let consumer = tokio::spawn(async move {
        let mut acked = 0;
        while acked < 200 {
            let Some(d) = sub.next().await else { break };
            sub.ack(&[d.delivery_id]).expect("ack");
            acked += 1;
        }
        sub
    });

    // Publish over ~2 s while the session is being pumped, so rates are observed live.
    let producer_broker = broker.clone();
    let producer = tokio::spawn(async move {
        for batch in 0..20 {
            let msgs = (0..10)
                .map(|i| PublishMessage {
                    key: Some(format!("k{}", i % 7)),
                    headers: Default::default(),
                    payload: format!("{{\"order\":{}}}", batch * 10 + i)
                        .into_bytes()
                        .into(),
                })
                .collect();
            producer_broker
                .publish("orders", msgs)
                .await
                .expect("publish");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    let mut saw_ingress = false;
    pump_until(&mut session, &mut mirror, Duration::from_secs(10), |m| {
        if let Some(ov) = m.get::<ClusterOverview>("cluster.overview") {
            saw_ingress |= ov.ingress_rate > 0.0;
        }
        let summary_ok = m
            .get::<ChannelsSummary>("channels.summary")
            .is_some_and(|s| {
                s.get("orders")
                    .is_some_and(|o| o.messages == 200 && o.lag == 0)
                    && s.contains_key("audit.logs")
            });
        let consumers_ok = m
            .get::<ConsumersState>("channel.orders.consumers")
            .is_some_and(|c| c.get("billing").is_some_and(|g| g.instances.len() == 1));
        let events_ok = m.get::<EventsFeed>("events").is_some_and(|e| {
            e.events
                .iter()
                .any(|e| e.title == "Channel created" && e.subject == "audit.logs")
        });
        let detail_ok = m
            .get::<ChannelDetail>("channel.orders")
            .is_some_and(|d| d.summary.messages == 200);
        let overview_ok = m
            .get::<ClusterOverview>("cluster.overview")
            .is_some_and(|o| o.messages_total == 200 && o.channels == 2);
        detail_ok
            && overview_ok
            && saw_ingress
            && summary_ok
            && consumers_ok
            && events_ok
            && m.tail_seen > 0
    })
    .await;
    tokio::time::timeout(Duration::from_secs(10), producer)
        .await
        .expect("producer finished")
        .expect("producer");
    let sub = tokio::time::timeout(Duration::from_secs(10), consumer)
        .await
        .expect("consumer done")
        .expect("join");

    let detail: ChannelDetail = mirror.get("channel.orders").expect("channel detail");
    assert_eq!(detail.summary.name, "orders");
    assert_eq!(
        detail
            .partitions
            .iter()
            .map(|p| p.high_offset - p.low_offset)
            .sum::<u64>(),
        200
    );
    let ov: ClusterOverview = mirror.get("cluster.overview").expect("overview");
    assert_eq!(ov.channels, 2);
    assert_eq!(ov.messages_total, 200);
    let tail: TailState = mirror.get("channel.orders.tail").expect("tail");
    assert!(tail.sample_ratio <= 1.0);
    assert!(mirror.docs.contains_key("nodes"));

    // History got 1 Hz samples.
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let h = hub
        .history("ingress_rate", HistoryRange::M5)
        .expect("history");
    assert!(!h.points.is_empty());
    assert!(h.points.iter().any(|p| p.1 > 0.0), "{:?}", h.points);
    assert!(
        !hub.history("channel.orders.lag", HistoryRange::M5)
            .expect("history")
            .points
            .is_empty()
    );

    drop(sub);
    hub.shutdown();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), session.next())
            .await
            .expect("ends")
            .is_none()
    );
    tokio::time::timeout(Duration::from_secs(10), broker.shutdown())
        .await
        .expect("broker shutdown finished");
}
