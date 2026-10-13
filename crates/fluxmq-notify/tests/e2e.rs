//! End to end with a real Broker + TelemetryHub: an alert opens (lag with no consumers) →
//! the mock webhook receives a signed payload; the `$system.events` feed (long-poll,
//! cursors, `latest`, gaps).

mod common;

use std::time::{Duration, Instant};

use common::mock_http;
use fluxmq_cluster::Cluster;
use fluxmq_core::{Broker, BrokerConfig, FsyncPolicy, SubscriberInfo};
use fluxmq_notify::senders::{SIGNATURE_HEADER, verify_signature};
use fluxmq_notify::{FeedAfter, NotifyConfig, NotifyService};
use fluxmq_proto::model::{ChannelConfig, ChannelMode, RetentionPolicy};
use fluxmq_proto::notify::{
    CreateNotifierRequest, CreateRuleRequest, FeedItem, NotificationPayload, NotifierKind,
    RuleMatch, SYSTEM_EVENTS_CHANNEL,
};
use fluxmq_proto::telemetry::{AlertKind, Health};
use fluxmq_proto::{PublishMessage, StartFrom};
use fluxmq_telemetry::{TelemetryConfig, TelemetryHub};

struct H {
    broker: Broker,
    svc: NotifyService,
    telemetry: TelemetryHub,
    _dir: tempfile::TempDir,
}

impl Drop for H {
    fn drop(&mut self) {
        self.svc.shutdown();
        self.telemetry.shutdown();
    }
}

async fn setup(segment_bytes: Option<u64>) -> H {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::new(dir.path());
    cfg.fsync = FsyncPolicy::OsBuffered;
    if let Some(s) = segment_bytes {
        cfg.segment_bytes = s;
    }
    let broker = Broker::open(cfg).await.unwrap();
    let telemetry = TelemetryHub::start(broker.clone(), TelemetryConfig::default());
    let cluster = Cluster::standalone(broker.clone());
    let svc = NotifyService::start(
        broker.clone(),
        cluster.meta(),
        &telemetry,
        cluster,
        NotifyConfig::default().with_public_url(Some("https://mq.example.com/".into())),
    )
    .await
    .unwrap();
    H {
        broker,
        svc,
        telemetry,
        _dir: dir,
    }
}

fn info() -> SubscriberInfo {
    SubscriberInfo {
        client_name: "test".into(),
        remote_addr: "local".into(),
        connection_id: 0,
    }
}

#[tokio::test]
async fn alert_reaches_signed_webhook_and_feed() {
    let h = setup(None).await;
    let (base, mock) = mock_http().await;
    let n = h
        .svc
        .create_notifier(CreateNotifierRequest {
            name: "receiver".into(),
            enabled: true,
            kind: NotifierKind::Webhook {
                url: format!("{base}/fluxmq"),
                secret: Some("e2e-secret".into()),
                headers: Default::default(),
            },
        })
        .await
        .unwrap();
    h.svc
        .create_rule(CreateRuleRequest {
            name: "all warnings".into(),
            enabled: true,
            notifier_ids: vec![n.id],
            matcher: RuleMatch {
                min_severity: Health::Warning,
                alert_kinds: vec![],
                event_kinds: vec![],
                channels: vec!["orders".into()],
            },
            group_wait_secs: Some(0),
            repeat_interval_secs: None,
            send_resolved: None,
        })
        .await
        .unwrap();

    // Lag with no consumers: a group exists, its only consumer left, messages pile up.
    let mut ch = ChannelConfig::new("orders", ChannelMode::Stream);
    ch.partitions = 1;
    h.broker.create_channel(ch).unwrap();
    let sub = h
        .broker
        .subscribe("orders", "billing", info(), 10, StartFrom::Earliest)
        .unwrap();
    drop(sub);
    let msgs = (0..20)
        .map(|i| PublishMessage {
            key: None,
            headers: Default::default(),
            payload: format!("{{\"n\":{i}}}").into(),
        })
        .collect();
    h.broker.publish("orders", msgs).await.unwrap();

    let reqs = mock.wait_for(1, 30).await;
    let r = &reqs[0];
    assert_eq!(r.uri, "/fluxmq");
    let sig = r.headers.get(SIGNATURE_HEADER).unwrap().to_str().unwrap();
    assert!(verify_signature(
        "e2e-secret",
        sig,
        &r.body,
        fluxmq_proto::now_ms() / 1000,
        300
    ));
    let p: NotificationPayload = serde_json::from_slice(&r.body).unwrap();
    assert_eq!(p.status, "firing");
    assert_eq!(p.rule, "all warnings");
    assert_eq!(p.console_url.as_deref(), Some("https://mq.example.com"));
    assert_eq!(p.alerts[0].kind, AlertKind::NoConsumers);
    assert_eq!(p.alerts[0].channel.as_deref(), Some("orders"));
    // The history entry is written right after the response.
    let mut hist = h.svc.history(10);
    for _ in 0..50 {
        if !hist.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        hist = h.svc.history(10);
    }
    assert_eq!(hist[0].status, fluxmq_proto::notify::DeliveryStatus::Sent);
    assert_eq!(hist[0].attempts, 1);

    // The same transition is in the pull feed (exactly once), next to the channel event.
    let page = h
        .svc
        .feed(FeedAfter::Start, 100, Duration::from_secs(5))
        .await
        .unwrap();
    let opened: Vec<&FeedItem> = page
        .items
        .iter()
        .filter(|i| {
            i.kind == "alert_opened"
                && i.alert
                    .as_ref()
                    .is_some_and(|a| a.id == "no_consumers:orders/billing")
        })
        .collect();
    assert_eq!(opened.len(), 1, "{:?}", page.items);
    assert_eq!(
        opened[0].alert.as_ref().unwrap().kind,
        AlertKind::NoConsumers
    );
    assert!(
        page.items
            .iter()
            .any(|i| i.kind == "event" && i.event.as_ref().is_some_and(|e| e.subject == "orders"))
    );
    assert!(
        !page.items.iter().any(|i| i
            .event
            .as_ref()
            .is_some_and(|e| matches!(e.kind, fluxmq_proto::telemetry::EventKind::AlertOpened))),
        "alert events are not duplicated in the feed"
    );
    // Cursors are offsets, ascending.
    let cursors: Vec<u64> = page
        .items
        .iter()
        .map(|i| i.cursor.parse().unwrap())
        .collect();
    assert!(cursors.windows(2).all(|w| w[0] < w[1]));
}

#[tokio::test]
async fn feed_long_poll_cursor_latest() {
    let h = setup(None).await;
    let cfg = h
        .broker
        .channel(SYSTEM_EVENTS_CHANNEL)
        .expect("created on start");
    assert_eq!(cfg.partitions, 1);
    assert_eq!(cfg.retention.max_age_secs, Some(7 * 24 * 3600));

    // Long-poll on `latest` wakes up as soon as something is appended.
    let svc = h.svc.clone();
    let started = Instant::now();
    let poll = tokio::spawn(async move {
        svc.feed(FeedAfter::Latest, 100, Duration::from_secs(20))
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    h.broker
        .create_channel(ChannelConfig::new("audit.logs", ChannelMode::Stream))
        .unwrap();
    let page = poll.await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(10), "woke up early");
    assert_eq!(page.items.len(), 1, "{:?}", page.items);
    let first = &page.items[0];
    assert_eq!(first.kind, "event");
    assert_eq!(first.event.as_ref().unwrap().subject, "audit.logs");
    assert_eq!(page.next_cursor.as_deref(), Some(first.cursor.as_str()));
    assert!(!page.gap);

    // Continue from the cursor: only newer items.
    h.broker
        .create_channel(ChannelConfig::new("audit.more", ChannelMode::Stream))
        .unwrap();
    let after: u64 = first.cursor.parse().unwrap();
    let page2 = h
        .svc
        .feed(FeedAfter::Offset(after), 100, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(page2.items.len(), 1);
    assert_eq!(page2.items[0].event.as_ref().unwrap().subject, "audit.more");
    assert!(page2.items[0].cursor.parse::<u64>().unwrap() > after);

    // Nothing new: returns after wait_ms with the same cursor.
    let t = Instant::now();
    let empty = h
        .svc
        .feed(
            FeedAfter::Offset(page2.items[0].cursor.parse().unwrap()),
            100,
            Duration::from_millis(400),
        )
        .await
        .unwrap();
    assert!(empty.items.is_empty());
    assert!(t.elapsed() >= Duration::from_millis(350));
    assert_eq!(empty.next_cursor, page2.next_cursor);

    // `latest` with nothing new → empty page whose cursor is the current end.
    let latest = h
        .svc
        .feed(FeedAfter::Latest, 100, Duration::ZERO)
        .await
        .unwrap();
    assert!(latest.items.is_empty());
    assert_eq!(latest.next_cursor, page2.next_cursor);

    // Limit.
    let all = h
        .svc
        .feed(FeedAfter::Start, 1, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(all.items.len(), 1);

    // `tail`: the newest N items, oldest first, real cursors; continue with next_cursor.
    let tail = h
        .svc
        .feed(FeedAfter::Tail(1), 100, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(tail.items.len(), 1);
    assert_eq!(tail.items[0].cursor, page2.items[0].cursor, "newest item");
    assert_eq!(tail.next_cursor, page2.next_cursor);
    let tail2 = h
        .svc
        .feed(FeedAfter::Tail(2), 100, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(tail2.items.len(), 2);
    assert_eq!(tail2.items[0].cursor, first.cursor);
    assert!(tail2.items.iter().all(|i| !i.cursor.is_empty()));
    let big = h
        .svc
        .feed(FeedAfter::Tail(1000), 100, Duration::ZERO)
        .await
        .unwrap();
    assert!(big.items.len() >= 2 && !big.gap);
    assert!(FeedAfter::from_query(Some("3"), Some(5)).is_err());
    assert!(FeedAfter::from_query(None, Some(0)).is_err());
    assert!(FeedAfter::from_query(None, Some(1001)).is_err());
    assert_eq!(
        FeedAfter::from_query(None, Some(7)).unwrap(),
        FeedAfter::Tail(7)
    );
}

#[tokio::test]
async fn feed_reports_gaps_after_retention() {
    let h = setup(Some(4096)).await;
    // The system appends through the Broker API (users can't): fill several segments.
    let item = FeedItem {
        cursor: String::new(),
        ts: 1,
        kind: "event".into(),
        alert: None,
        event: None,
        node: "x".repeat(400),
    };
    for _ in 0..60 {
        h.broker
            .publish(
                SYSTEM_EVENTS_CHANNEL,
                vec![PublishMessage {
                    key: None,
                    headers: Default::default(),
                    payload: serde_json::to_vec(&item).unwrap().into(),
                }],
            )
            .await
            .unwrap();
    }
    h.broker
        .update_channel(
            SYSTEM_EVENTS_CHANNEL,
            fluxmq_proto::api::UpdateChannelRequest {
                retention: Some(RetentionPolicy {
                    max_age_secs: Some(7 * 24 * 3600),
                    max_bytes: Some(8192),
                }),
                ..Default::default()
            },
        )
        .unwrap();
    h.broker.enforce_retention().await;
    let page = h
        .svc
        .feed(FeedAfter::Offset(0), 10, Duration::ZERO)
        .await
        .unwrap();
    assert!(page.gap, "items after offset 0 were removed");
    assert!(page.items[0].cursor.parse::<u64>().unwrap() > 1);
    // Reading from the start never reports a gap.
    let page = h
        .svc
        .feed(FeedAfter::Start, 10, Duration::ZERO)
        .await
        .unwrap();
    assert!(!page.gap);
}
