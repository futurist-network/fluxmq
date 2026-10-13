//! Delayed messages (v0.12) end to end: REST and SDK, timing, listing, cancel, release now,
//! durability across a restart, unknown channels and bounds.

mod common;

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use common::{TestServer, WAIT, eventually};
use fluxmq_client::{BatchingOptions, Client, ClientOptions, Consumer, SubscribeOptions, message};
use fluxmq_proto::model::{ChannelConfig, ChannelMode, StartFrom};
use fluxmq_proto::scheduled::{SCHEDULED_FOR_HEADER, SCHEDULED_ID_HEADER};
use fluxmq_server::{Server, ServerConfig};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

async fn client(addr: std::net::SocketAddr, name: &str) -> Client {
    Client::connect(ClientOptions::new(addr.to_string(), name))
        .await
        .expect("connect")
}

async fn queue(c: &Client, name: &str) {
    c.ensure_channel(ChannelConfig::new(name, ChannelMode::Queue))
        .await
        .unwrap();
}

async fn consumer(c: &Client, ch: &str) -> Consumer {
    c.subscribe(
        ch,
        "g",
        SubscribeOptions::default()
            .prefetch(100)
            .start(StartFrom::Earliest),
    )
    .await
    .unwrap()
}

/// Next delivery's (payload, headers), acked; `None` when nothing arrives within `wait`.
async fn next(
    c: &mut Consumer,
    wait: Duration,
) -> Option<(String, std::collections::BTreeMap<String, String>)> {
    let d = tokio::time::timeout(wait, c.next()).await.ok()??.unwrap();
    let out = (
        String::from_utf8(d.payload().to_vec()).unwrap(),
        d.headers().clone(),
    );
    d.ack().await.unwrap();
    Some(out)
}

#[tokio::test(flavor = "multi_thread")]
async fn rest_delay_lists_then_delivers_on_time() {
    let srv = TestServer::start().await;
    let c = client(srv.client_addr, "t").await;
    queue(&c, "rem").await;
    let mut cons = consumer(&c, "rem").await;
    let t0 = Instant::now();
    let r = srv
        .post_json(
            "/api/channels/rem/publish",
            json!({ "messages": [
                { "key": "u1", "payload": { "encoding": "utf8", "data": "later-1" }, "delay_ms": 1500 },
                { "payload": { "encoding": "utf8", "data": "now" } },
                { "payload": { "encoding": "utf8", "data": "later-2" }, "delay_ms": 1500,
                  "headers": { "flux-type": "Reminder" } },
            ]}),
        )
        .await;
    assert!(r.status().is_success(), "{}", r.status());
    let ids: Value = r.json().await.unwrap();
    assert_eq!(ids["ids"].as_array().unwrap().len(), 3);

    // The immediate one arrives right away, the delayed ones are listed.
    assert_eq!(next(&mut cons, WAIT).await.unwrap().0, "now");
    let list = eventually(|| async {
        let l = srv.get_json("/api/scheduled?channel=rem").await;
        (l["total"] == 2).then_some(l)
    })
    .await;
    assert_eq!(list["channels"][0]["channel"], "rem");
    // Same due time: the order between them is by schedule id (partition), so look them up.
    let items = list["items"].as_array().unwrap();
    assert!(
        items.iter().any(|i| i["event_type"] == "Reminder"),
        "{items:?}"
    );
    assert!(items.iter().any(|i| i["key"] == "u1"), "{items:?}");
    assert!(
        next(&mut cons, Duration::from_millis(900)).await.is_none(),
        "too early"
    );

    let mut got = BTreeSet::new();
    for _ in 0..2 {
        let (p, h) = next(&mut cons, WAIT).await.expect("released");
        assert!(h.contains_key(SCHEDULED_ID_HEADER), "{h:?}");
        assert!(h.contains_key(SCHEDULED_FOR_HEADER));
        assert!(!h.contains_key("flux-delay-ms"));
        got.insert(p);
    }
    let took = t0.elapsed();
    assert!(
        took >= Duration::from_millis(1400),
        "released after {took:?}"
    );
    assert!(took < Duration::from_secs(4), "released after {took:?}");
    assert_eq!(got, BTreeSet::from(["later-1".into(), "later-2".into()]));
    let list = eventually(|| async {
        let l = srv.get_json("/api/scheduled").await;
        (l["total"] == 0).then_some(l)
    })
    .await;
    assert_eq!(list["released"], 2);
    // Nothing more (released exactly once).
    assert!(next(&mut cons, Duration::from_millis(500)).await.is_none());
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_and_release_now() {
    let srv = TestServer::start().await;
    let c = client(srv.client_addr, "t").await;
    queue(&c, "q").await;
    let mut cons = consumer(&c, "q").await;
    let p = c.producer("q");
    let keep = p
        .send(message("keep").delay(Duration::from_secs(60)))
        .await
        .unwrap();
    let drop_ = p
        .send(message("drop").delay(Duration::from_secs(60)))
        .await
        .unwrap();
    eventually(|| async { (srv.get_json("/api/scheduled").await["total"] == 2).then_some(()) })
        .await;

    let del = srv
        .req(
            reqwest::Method::DELETE,
            &format!("/api/scheduled/{}/{}", drop_.partition, drop_.offset),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), 204);
    let again = srv
        .req(
            reqwest::Method::DELETE,
            &format!("/api/scheduled/{}/{}", drop_.partition, drop_.offset),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(again.status(), 404);
    let rel = srv
        .post_json(
            &format!("/api/scheduled/{}/{}/release", keep.partition, keep.offset),
            json!({}),
        )
        .await;
    assert_eq!(rel.status(), 204);
    let (payload, h) = next(&mut cons, WAIT).await.expect("released now");
    assert_eq!(payload, "keep");
    assert_eq!(
        h[SCHEDULED_ID_HEADER],
        format!("{}/{}", keep.partition, keep.offset)
    );
    assert!(
        next(&mut cons, Duration::from_millis(800)).await.is_none(),
        "cancelled one came"
    );
    let l = srv.get_json("/api/scheduled").await;
    assert_eq!(l["total"], 0);
    assert_eq!(l["cancelled"], 1);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bounds_unknown_channel_and_batching_producer() {
    let srv = TestServer::start().await;
    let c = client(srv.client_addr, "t").await;
    queue(&c, "b").await;
    // Over 30 days and both headers: refused; unknown channel: not found.
    let far = srv
        .post_json(
            "/api/channels/b/publish",
            json!({ "messages": [{ "payload": { "encoding": "utf8", "data": "x" }, "delay_ms": 31u64 * 86_400_000 }] }),
        )
        .await;
    assert_eq!(far.status(), 400);
    let both = srv
        .post_json(
            "/api/channels/b/publish",
            json!({ "messages": [{ "payload": { "encoding": "utf8", "data": "x" }, "delay_ms": 10, "deliver_at_ms": 1 }] }),
        )
        .await;
    assert_eq!(both.status(), 400);
    let missing = c
        .producer("nope")
        .send(message("x").delay(Duration::from_secs(5)))
        .await;
    assert!(missing.is_err());
    // A past due time publishes at once; the batching producer handles delayed messages too.
    let mut cons = consumer(&c, "b").await;
    let bp = c.batching_producer("b", BatchingOptions::default());
    let past = std::time::SystemTime::now() - Duration::from_secs(10);
    let f1 = bp.send(message("past").deliver_at(past)).await.unwrap();
    let f2 = bp
        .send(message("soon").delay(Duration::from_millis(400)))
        .await
        .unwrap();
    let f3 = bp.send(message("plain")).await.unwrap();
    f1.await.unwrap();
    f2.await.unwrap();
    f3.await.unwrap();
    let mut got = Vec::new();
    for _ in 0..3 {
        got.push(next(&mut cons, WAIT).await.expect("delivered").0);
    }
    assert_eq!(got.last().unwrap(), "soon", "{got:?}");
    srv.stop().await;
}

/// A scheduled message survives a broker restart and is delivered once after it.
#[tokio::test(flavor = "multi_thread")]
async fn survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let start = |dir: std::path::PathBuf| async move {
        let mut config = ServerConfig::ephemeral(dir);
        config.fsync = fluxmq_server::FsyncMode::Os;
        let server = Server::bind(config).await.unwrap();
        let addr = server.client_addr();
        let token = CancellationToken::new();
        let h = tokio::spawn(server.run(token.clone().cancelled_owned()));
        (addr, token, h)
    };
    let (addr, token, h) = start(dir.path().join("data")).await;
    let c = client(addr, "t").await;
    queue(&c, "r").await;
    c.producer("r")
        .send(message("after-restart").delay(Duration::from_secs(3)))
        .await
        .unwrap();
    c.producer("r")
        .send(message("second").delay(Duration::from_secs(3)))
        .await
        .unwrap();
    drop(c);
    token.cancel();
    h.await.unwrap().unwrap();

    let (addr, token, h) = start(dir.path().join("data")).await;
    let c = client(addr, "t2").await;
    let mut cons = consumer(&c, "r").await;
    let mut got = Vec::new();
    while let Some((p, _)) = next(&mut cons, Duration::from_secs(6)).await {
        got.push(p);
        if got.len() == 2 {
            break;
        }
    }
    got.sort();
    assert_eq!(got, vec!["after-restart".to_string(), "second".to_string()]);
    assert!(
        next(&mut cons, Duration::from_millis(1500)).await.is_none(),
        "duplicate"
    );
    drop(c);
    token.cancel();
    h.await.unwrap().unwrap();
}
