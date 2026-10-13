//! Router end-to-end tests against a real broker in a temp dir.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use fluxmq_core::{Broker, BrokerConfig, FsyncPolicy};
use fluxmq_meta::{LocalMetaStore, MetaStoreExt, SharedMetaStore};
use fluxmq_proto::api::MessageQuery;
use fluxmq_proto::model::{ChannelConfig, ChannelMode, Message, RetentionPolicy, RetryPolicy};
use fluxmq_proto::routing::{CreateRouteRequest, Route, RouteTestRequest, UpdateRouteRequest};
use fluxmq_proto::{PublishMessage, ROUTER_GROUP};
use fluxmq_router::{RouterError, RouterService, filter, scan};
use serde_json::json;

async fn open_broker(dir: &Path) -> Broker {
    let mut c = BrokerConfig::new(dir);
    c.fsync = FsyncPolicy::OsBuffered;
    c.offset_checkpoint_interval = Duration::from_millis(20);
    Broker::open(c).await.unwrap()
}

fn channel(b: &Broker, name: &str, partitions: u32) {
    let mut c = ChannelConfig::new(name, ChannelMode::Stream);
    c.partitions = partitions;
    c.retention = RetentionPolicy {
        max_age_secs: None,
        max_bytes: None,
    };
    c.retry = RetryPolicy {
        max_attempts: 2,
        backoff_ms: vec![30],
        ack_timeout_ms: 5_000,
    };
    b.create_channel(c).unwrap();
}

fn pm(key: Option<&str>, payload: serde_json::Value) -> PublishMessage {
    PublishMessage {
        key: key.map(str::to_string),
        headers: BTreeMap::new(),
        payload: Bytes::from(payload.to_string()),
    }
}

fn route_req(name: &str, source: &str, filter_text: &str, targets: &[&str]) -> CreateRouteRequest {
    CreateRouteRequest {
        name: name.into(),
        source: source.into(),
        enabled: true,
        conditions: filter::parse(filter_text).unwrap(),
        targets: targets.iter().map(|s| s.to_string()).collect(),
    }
}

/// Every stored message of a channel, oldest first.
fn all(b: &Broker, ch: &str) -> Vec<Message> {
    let q = MessageQuery {
        limit: Some(1000),
        ..Default::default()
    };
    scan::search(b, ch, &q, &[]).unwrap().messages
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

struct Env {
    broker: Broker,
    meta: SharedMetaStore,
    router: RouterService,
    dir: tempfile::TempDir,
}

async fn env() -> Env {
    let dir = tempfile::tempdir().unwrap();
    let broker = open_broker(&dir.path().join("data")).await;
    let meta: SharedMetaStore =
        Arc::new(LocalMetaStore::open(dir.path().join("meta.json")).unwrap());
    let router = RouterService::start(broker.clone(), meta.clone())
        .await
        .unwrap();
    Env {
        broker,
        meta,
        router,
        dir,
    }
}

fn stat(r: &RouterService, id: &str) -> fluxmq_proto::routing::RouteStats {
    r.stats().into_iter().find(|s| s.id == id).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routes_by_payload_field_to_two_targets() {
    let e = env().await;
    for ch in ["orders", "orders.md", "orders.ro", "audit"] {
        channel(&e.broker, ch, 2);
    }
    let md = e
        .router
        .create(route_req(
            "md",
            "orders",
            "country = MD AND amount >= 100",
            &["orders.md", "audit"],
        ))
        .await
        .unwrap();
    let ro = e
        .router
        .create(route_req(
            "ro",
            "orders",
            "country = RO",
            &["orders.ro", "audit"],
        ))
        .await
        .unwrap();
    assert_eq!(md.id.len(), 10);
    assert_eq!(e.router.routes().len(), 2);
    assert_eq!(e.router.active_sources(), vec!["orders".to_string()]);

    let msgs = vec![
        pm(
            Some("o1"),
            json!({"country":"MD","amount":150,"order_id":"8842"}),
        ),
        pm(Some("o2"), json!({"country":"MD","amount":10})),
        pm(Some("o3"), json!({"country":"RO","amount":1})),
        pm(Some("o4"), json!({"country":"UA","amount":1000})),
        pm(None, json!("not an object")),
    ];
    e.broker.publish("orders", msgs).await.unwrap();

    eventually("forwards", || all(&e.broker, "audit").len() == 2).await;
    eventually("md", || all(&e.broker, "orders.md").len() == 1).await;
    let m = &all(&e.broker, "orders.md")[0];
    assert_eq!(m.key.as_deref(), Some("o1"));
    assert_eq!(m.headers["flux-route-id"], md.id);
    assert_eq!(m.headers["flux-route-path"], "orders,orders.md");
    assert!(m.headers["flux-routed-from"].starts_with("orders/"));
    let doc: serde_json::Value = serde_json::from_slice(&m.payload).unwrap();
    assert_eq!(doc["order_id"], "8842");
    eventually("ro", || all(&e.broker, "orders.ro").len() == 1).await;
    // Key preserved → same partitioning as a direct publish.
    let ro_msg = &all(&e.broker, "orders.ro")[0];
    assert_eq!(ro_msg.key.as_deref(), Some("o3"));

    eventually("stats", || {
        stat(&e.router, &md.id).forwarded == 2 && stat(&e.router, &ro.id).forwarded == 2
    })
    .await;
    let s = stat(&e.router, &md.id);
    assert_eq!((s.matched, s.forwarded, s.failed), (1, 2, 0));
    assert!(s.last_matched_ms.is_some());
    let s = stat(&e.router, &ro.id);
    assert_eq!((s.evaluated, s.matched, s.forwarded), (5, 1, 2));
    assert!(e.broker.has_group("orders", ROUTER_GROUP).unwrap());
    e.router.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn header_match_disabled_route_and_enable() {
    let e = env().await;
    channel(&e.broker, "events", 1);
    channel(&e.broker, "payments", 1);
    let mut req = route_req(
        "pay",
        "events",
        "header.flux-type prefix payment.",
        &["payments"],
    );
    req.enabled = false;
    let r = e.router.create(req).await.unwrap();
    assert!(
        e.router.active_sources().is_empty(),
        "disabled route runs no worker"
    );

    let mut m = pm(None, json!({"x":1}));
    m.headers
        .insert("flux-type".into(), "payment.settled".into());
    e.broker.publish("events", vec![m.clone()]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(all(&e.broker, "payments").is_empty());

    let updated = e
        .router
        .update(
            &r.id,
            UpdateRouteRequest {
                enabled: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(updated.enabled);
    assert!(updated.updated_at_ms >= updated.created_at_ms);
    assert_eq!(e.router.active_sources(), vec!["events".to_string()]);
    // New group starts at Latest: the earlier message is not routed retroactively.
    let mut other = m.clone();
    other
        .headers
        .insert("flux-type".into(), "order.created".into());
    e.broker.publish("events", vec![m, other]).await.unwrap();
    eventually("payment routed", || all(&e.broker, "payments").len() == 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(all(&e.broker, "payments").len(), 1);

    // Disabling stops the worker again.
    e.router
        .update(
            &r.id,
            UpdateRouteRequest {
                enabled: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    eventually("worker stopped", || e.router.active_sources().is_empty()).await;
    e.router.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loop_protection_a_b_a() {
    let e = env().await;
    channel(&e.broker, "a", 1);
    channel(&e.broker, "b", 1);
    let ab = e
        .router
        .create(route_req("ab", "a", "", &["b"]))
        .await
        .unwrap();
    let ba = e
        .router
        .create(route_req("ba", "b", "", &["a"]))
        .await
        .unwrap();
    e.broker
        .publish("a", vec![pm(None, json!(1))])
        .await
        .unwrap();
    eventually("b got the copy", || all(&e.broker, "b").len() == 1).await;
    eventually("b's router evaluated it", || {
        stat(&e.router, &ba.id).evaluated == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(all(&e.broker, "a").len(), 1, "no copy back into a");
    assert_eq!(all(&e.broker, "b").len(), 1);
    let s = stat(&e.router, &ba.id);
    assert_eq!((s.matched, s.forwarded, s.failed), (1, 0, 0));
    eventually("ab counted", || stat(&e.router, &ab.id).forwarded == 1).await;
    e.router.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hop_limit_stops_long_chains() {
    let e = env().await;
    let names: Vec<String> = (0..11).map(|i| format!("c{i}")).collect();
    for n in &names {
        channel(&e.broker, n, 1);
    }
    for w in names.windows(2) {
        e.router
            .create(route_req(&w[0], &w[0], "", &[&w[1]]))
            .await
            .unwrap();
    }
    e.broker
        .publish("c0", vec![pm(None, json!("hop"))])
        .await
        .unwrap();
    eventually("8 hops", || all(&e.broker, "c8").len() == 1).await;
    let m = &all(&e.broker, "c8")[0];
    assert_eq!(m.headers["flux-route-path"], "c0,c1,c2,c3,c4,c5,c6,c7,c8");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(all(&e.broker, "c9").is_empty(), "9th hop must be refused");
    e.router.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_target_retries_then_dead_letters() {
    let e = env().await;
    channel(&e.broker, "src", 1);
    channel(&e.broker, "gone", 1);
    let r = e
        .router
        .create(route_req("r", "src", "", &["gone"]))
        .await
        .unwrap();
    e.broker.delete_channel("gone").unwrap();
    e.broker
        .publish("src", vec![pm(None, json!({"id":1}))])
        .await
        .unwrap();
    eventually("dead letter", || {
        e.broker.dlq_list(Some("src"), None, None, 10).1 == 1
    })
    .await;
    let (dl, _) = e.broker.dlq_list(Some("src"), None, None, 10);
    assert_eq!(dl[0].group, ROUTER_GROUP);
    assert_eq!(dl[0].reason, "route target unavailable: gone");
    assert_eq!(dl[0].attempts.len(), 2, "retried per the channel's policy");
    let s = stat(&e.router, &r.id);
    assert_eq!((s.matched, s.failed, s.forwarded), (2, 2, 0));
    e.router.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hot_reload_from_meta_and_channel_lifecycle() {
    let e = env().await;
    channel(&e.broker, "in", 1);
    channel(&e.broker, "out", 1);
    // Another node (or an operator) writes the route directly into the meta store.
    let route = Route {
        id: "ext1".into(),
        name: "external".into(),
        source: "in".into(),
        enabled: true,
        conditions: filter::parse("n > 1").unwrap(),
        targets: vec!["out".into()],
        created_at_ms: 1,
        updated_at_ms: 1,
    };
    e.meta.put_as("routes/ext1", &route).await.unwrap();
    eventually("route loaded", || e.router.route("ext1").is_some()).await;
    eventually("worker", || {
        e.router.active_sources() == vec!["in".to_string()]
    })
    .await;
    e.broker
        .publish(
            "in",
            vec![pm(None, json!({"n":1})), pm(None, json!({"n":2}))],
        )
        .await
        .unwrap();
    eventually("routed", || all(&e.broker, "out").len() == 1).await;

    // Deleting the source channel ends the worker; the route stays.
    e.broker.delete_channel("in").unwrap();
    eventually("worker ended", || e.router.active_sources().is_empty()).await;
    assert!(e.router.route("ext1").is_some());
    // Re-creating the channel resumes routing.
    channel(&e.broker, "in", 1);
    eventually("worker restarted", || {
        e.router.active_sources() == vec!["in".to_string()]
    })
    .await;
    e.broker
        .publish("in", vec![pm(None, json!({"n":5}))])
        .await
        .unwrap();
    eventually("routed again", || all(&e.broker, "out").len() == 2).await;

    // A delete in the meta store unloads it.
    e.meta.delete("routes/ext1", None).await.unwrap();
    eventually("route unloaded", || e.router.route("ext1").is_none()).await;
    eventually("worker stopped", || e.router.active_sources().is_empty()).await;
    e.router.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routes_persist_across_restart_and_group_resumes() {
    let dir = tempfile::tempdir().unwrap();
    let meta_path = dir.path().join("meta.json");
    let id;
    {
        let broker = open_broker(&dir.path().join("data")).await;
        channel(&broker, "s", 1);
        channel(&broker, "t", 1);
        let meta: SharedMetaStore = Arc::new(LocalMetaStore::open(&meta_path).unwrap());
        let router = RouterService::start(broker.clone(), meta).await.unwrap();
        id = router
            .create(route_req("keep", "s", "", &["t"]))
            .await
            .unwrap()
            .id;
        broker.publish("s", vec![pm(None, json!(1))]).await.unwrap();
        eventually("first", || all(&broker, "t").len() == 1).await;
        router.shutdown();
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Published while the router is down: routed after the restart (the group exists).
        broker.publish("s", vec![pm(None, json!(2))]).await.unwrap();
        broker.shutdown().await;
    }
    let broker = open_broker(&dir.path().join("data")).await;
    let meta: SharedMetaStore = Arc::new(LocalMetaStore::open(&meta_path).unwrap());
    let router = RouterService::start(broker.clone(), meta).await.unwrap();
    assert_eq!(router.routes().len(), 1);
    assert_eq!(router.route(&id).unwrap().name, "keep");
    eventually("backlog routed", || all(&broker, "t").len() == 2).await;
    router.shutdown();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validation_errors() {
    let e = env().await;
    channel(&e.broker, "x", 1);
    channel(&e.broker, "y", 1);
    let bad = |r: Result<Route, RouterError>| match r {
        Err(RouterError::Invalid(m)) => m,
        other => panic!("expected Invalid, got {other:?}"),
    };
    let m = bad(e.router.create(route_req("", "x", "", &["y"])).await);
    assert!(m.contains("name"));
    let m = bad(e.router.create(route_req("n", "x", "", &["x"])).await);
    assert!(m.contains("source"));
    let m = bad(e.router.create(route_req("n", "x", "", &[])).await);
    assert!(m.contains("target"));
    let many: Vec<String> = (0..17).map(|i| format!("t{i}")).collect();
    let many: Vec<&str> = many.iter().map(String::as_str).collect();
    bad(e.router.create(route_req("n", "x", "", &many)).await);
    let mut req = route_req("n", "x", "", &["y"]);
    req.conditions = filter::parse(&vec!["a = 1"; 33].join(" AND ")).unwrap();
    bad(e.router.create(req).await);
    let mut req = route_req("n", "x", "", &["y"]);
    req.conditions = vec![fluxmq_proto::routing::Condition {
        field: fluxmq_proto::routing::FieldRef::Payload("a".into()),
        op: fluxmq_proto::routing::Op::Gt,
        value: Some(json!(true)),
    }];
    assert!(bad(e.router.create(req).await).contains("comparison"));
    let mut req = route_req("n", "x", "", &["y"]);
    req.conditions = vec![fluxmq_proto::routing::Condition {
        field: fluxmq_proto::routing::FieldRef::Payload("a".into()),
        op: fluxmq_proto::routing::Op::Eq,
        value: None,
    }];
    assert!(bad(e.router.create(req).await).contains("required"));
    assert!(matches!(
        e.router.create(route_req("n", "nope", "", &["y"])).await,
        Err(RouterError::ChannelNotFound(c)) if c == "nope"
    ));
    assert!(matches!(
        e.router.create(route_req("n", "x", "", &["nope"])).await,
        Err(RouterError::ChannelNotFound(c)) if c == "nope"
    ));
    assert!(matches!(
        e.router
            .update("missing", UpdateRouteRequest::default())
            .await,
        Err(RouterError::NotFound)
    ));
    assert!(matches!(
        e.router.delete("missing").await,
        Err(RouterError::NotFound)
    ));
    // exists drops a stray value (normalization keeps render/parse round-trips exact).
    let mut req = route_req("n", "x", "a exists", &["y"]);
    req.conditions[0].value = Some(json!(1));
    let r = e.router.create(req).await.unwrap();
    assert_eq!(r.conditions[0].value, None);
    e.router.delete(&r.id).await.unwrap();
    assert!(e.router.routes().is_empty());
    let _ = &e.dir;
    e.router.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_service_rejects_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let broker = open_broker(dir.path()).await;
    let r = RouterService::disabled(broker.clone());
    assert!(r.routes().is_empty());
    assert!(matches!(
        r.create(route_req("n", "x", "", &["y"])).await,
        Err(RouterError::Invalid(_))
    ));
}

// ---- backtest and search ------------------------------------------------------------------

async fn seeded(partitions: u32, n: usize) -> (Broker, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let broker = open_broker(dir.path()).await;
    channel(&broker, "orders", partitions);
    let countries = ["MD", "RO", "UA", "DE"];
    for chunk in (0..n).collect::<Vec<_>>().chunks(500) {
        let msgs = chunk
            .iter()
            .map(|&i| {
                let mut m = pm(
                    Some(&format!("user-{}", i % 13)),
                    json!({"i": i, "country": countries[i % 4], "amount": i % 250,
                           "order_id": format!("{}", 8000 + i)}),
                );
                if i % 10 == 0 {
                    m.headers.insert("flux-type".into(), "vip".into());
                }
                m
            })
            .collect();
        broker.publish("orders", msgs).await.unwrap();
    }
    (broker, dir)
}

fn brute(b: &Broker, conds: &[fluxmq_proto::routing::Condition]) -> Vec<Message> {
    let mut v = Vec::new();
    for p in 0..b.channel("orders").unwrap().partitions {
        let mut offset = 0;
        loop {
            let q = MessageQuery {
                partition: Some(p),
                offset: Some(offset),
                limit: Some(1000),
                ..Default::default()
            };
            let (page, _, _) = b.read_messages("orders", &q).unwrap();
            let Some(last) = page.last() else { break };
            offset = last.offset + 1;
            v.extend(page.into_iter().filter(|m| filter::matches(conds, m)));
        }
    }
    v.sort_by_key(|m| (m.timestamp_ms, m.partition, m.offset));
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backtest_counts_and_samples() {
    let (broker, _dir) = seeded(4, 5000).await;
    let router = RouterService::disabled(broker.clone());
    let conds = filter::parse("country = MD AND amount >= 100").unwrap();
    let expected = brute(&broker, &conds);
    let started = Instant::now();
    let r = router
        .test(&RouteTestRequest {
            source: "orders".into(),
            conditions: conds.clone(),
            window_ms: None,
            sample_limit: Some(5),
        })
        .unwrap();
    eprintln!("backtest 5000 msgs: {:?}", started.elapsed());
    assert_eq!(r.scanned, 5000);
    assert_eq!(r.matched, expected.len() as u64);
    assert!(!r.truncated);
    assert_eq!(r.samples.len(), 5);
    // Samples are the newest matches, newest first.
    let newest: Vec<(u32, u64)> = expected
        .iter()
        .rev()
        .take(5)
        .map(|m| (m.partition, m.offset))
        .collect();
    let got: Vec<(u32, u64)> = r.samples.iter().map(|m| (m.partition, m.offset)).collect();
    assert_eq!(got, newest);

    // Window excludes everything.
    let r = router
        .test(&RouteTestRequest {
            source: "orders".into(),
            conditions: conds.clone(),
            window_ms: Some(0),
            sample_limit: None,
        })
        .unwrap();
    assert!(r.matched <= expected.len() as u64);
    // Cap → truncated.
    let r = scan::backtest_capped(&broker, "orders", &conds, 0, 3, 1500).unwrap();
    assert!(r.truncated);
    assert!(r.scanned >= 1500 && r.scanned < 5000);
    // Unknown source / bad conditions.
    assert!(matches!(
        router.test(&RouteTestRequest {
            source: "nope".into(),
            conditions: vec![],
            window_ms: None,
            sample_limit: None
        }),
        Err(RouterError::ChannelNotFound(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filtered_search_matches_brute_force() {
    let (broker, _dir) = seeded(4, 4000).await;
    let conds = filter::parse("header.flux-type = vip AND amount < 200").unwrap();
    let expected = brute(&broker, &conds);
    assert!(expected.len() > 100);
    let ids = |v: &[Message]| {
        v.iter()
            .map(|m| (m.partition, m.offset))
            .collect::<Vec<_>>()
    };

    // Default: newest `limit` matches, ascending.
    let q = MessageQuery {
        limit: Some(50),
        ..Default::default()
    };
    let started = Instant::now();
    let r = scan::search(&broker, "orders", &q, &conds).unwrap();
    eprintln!(
        "search newest 50 of 4000: {:?}, scanned {}",
        started.elapsed(),
        r.scanned
    );
    assert_eq!(ids(&r.messages), ids(&expected[expected.len() - 50..]));
    assert!(r.has_more);
    assert!(r.scanned < 4000, "stops early: {}", r.scanned);

    // since_ms forward → oldest matches from that time.
    let since = expected[10].timestamp_ms;
    let q = MessageQuery {
        since_ms: Some(since),
        limit: Some(30),
        ..Default::default()
    };
    let r = scan::search(&broker, "orders", &q, &conds).unwrap();
    let want: Vec<_> = expected
        .iter()
        .filter(|m| m.timestamp_ms >= since)
        .take(30)
        .cloned()
        .collect();
    assert_eq!(ids(&r.messages), ids(&want));

    // Key restricts to the key's partition and filters by key.
    let q = MessageQuery {
        key: Some("user-3".into()),
        limit: Some(1000),
        ..Default::default()
    };
    let r = scan::search(&broker, "orders", &q, &conds).unwrap();
    let want: Vec<_> = expected
        .iter()
        .filter(|m| m.key.as_deref() == Some("user-3"))
        .cloned()
        .collect();
    assert_eq!(ids(&r.messages), ids(&want));
    assert!(!r.has_more);

    // Explicit partition + offset → forward within that partition.
    let q = MessageQuery {
        partition: Some(2),
        offset: Some(100),
        limit: Some(10),
        ..Default::default()
    };
    let r = scan::search(&broker, "orders", &q, &conds).unwrap();
    let want: Vec<_> = expected
        .iter()
        .filter(|m| m.partition == 2 && m.offset >= 100)
        .take(10)
        .cloned()
        .collect();
    assert_eq!(ids(&r.messages), ids(&want));

    // No match: whole channel scanned, nothing more.
    let none = filter::parse("country = XX").unwrap();
    let r = scan::search(&broker, "orders", &MessageQuery::default(), &none).unwrap();
    assert!(r.messages.is_empty());
    assert_eq!(r.scanned, 4000);
    assert!(!r.has_more);

    // Scan cap → has_more.
    let r = scan::search_capped(&broker, "orders", &MessageQuery::default(), &none, 1000).unwrap();
    assert!(r.has_more);

    // numeric ↔ string equality through search.
    let r = scan::search(
        &broker,
        "orders",
        &MessageQuery::default(),
        &filter::parse("order_id = 8842").unwrap(),
    )
    .unwrap();
    assert_eq!(r.messages.len(), 1);
    assert!(scan::search(&broker, "nope", &MessageQuery::default(), &none).is_err());
}

/// Rough performance numbers: `cargo test --release -p fluxmq-router --test router perf -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn perf() {
    let message = Message {
        partition: 0,
        offset: 0,
        key: Some("user-1".into()),
        headers: BTreeMap::from([("flux-type".to_string(), "order.created".to_string())]),
        timestamp_ms: 0,
        payload: Bytes::from(
            json!({"type":"order.created","order_id":"8842","country":"MD","amount":120.5,
                   "customer":{"id":77,"tier":"gold"},"items":[{"sku":"A","qty":1},{"sku":"B","qty":2}]})
            .to_string(),
        ),
    };
    for f in [
        "header.flux-type = order.created",
        "country = MD AND amount >= 100",
        "type = order.created AND country in [MD, RO] AND items.1.sku = B AND customer.tier != silver",
    ] {
        let conds = filter::parse(f).unwrap();
        let n = 200_000;
        let t = Instant::now();
        let mut hits = 0;
        for _ in 0..n {
            hits += usize::from(filter::matches(&conds, &message));
        }
        assert_eq!(hits, n);
        eprintln!("eval {:>4} ns/msg  {f}", t.elapsed().as_nanos() / n as u128);
    }
    let (broker, _dir) = seeded(8, 200_000).await;
    let conds = filter::parse("country = MD AND amount >= 100").unwrap();
    let t = Instant::now();
    let r = scan::backtest(&broker, "orders", &conds, 0, 20).unwrap();
    eprintln!(
        "backtest: scanned {} matched {} truncated {} in {:?}",
        r.scanned,
        r.matched,
        r.truncated,
        t.elapsed()
    );
    let t = Instant::now();
    let none = filter::parse("country = XX").unwrap();
    let r = scan::search(&broker, "orders", &MessageQuery::default(), &none).unwrap();
    eprintln!(
        "search (no match, full cap): scanned {} in {:?}",
        r.scanned,
        t.elapsed()
    );
    let t = Instant::now();
    let r = scan::search(&broker, "orders", &MessageQuery::default(), &conds).unwrap();
    eprintln!(
        "search newest 100: scanned {} in {:?}",
        r.scanned,
        t.elapsed()
    );
}
