//! Live subscription gateway (v0.10) end to end: WebSocket and SSE, resume by cursor,
//! filters, the `subscriber` role, origin checks, limits, revocation, shared fan-out.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use common::{TestServer, WAIT};
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn create_channel(srv: &TestServer, name: &str, partitions: u32) {
    let r = srv
        .post_json(
            "/api/channels",
            json!({ "name": name, "mode": "stream", "partitions": partitions }),
        )
        .await;
    assert!(r.status().is_success(), "create {name}: {}", r.status());
}

/// Publishes `n` messages with keys `k{i % keys}` and payload `{"i": start + i}`.
async fn publish(srv: &TestServer, ch: &str, start: usize, n: usize, keys: usize) {
    let msgs: Vec<Value> = (start..start + n)
        .map(|i| {
            json!({
                "key": format!("k{}", i % keys),
                "headers": { "type": if i % 2 == 0 { "even" } else { "odd" } },
                "payload": { "encoding": "utf8", "data": format!("{{\"i\":{i}}}") },
            })
        })
        .collect();
    let r = srv
        .post_json(
            &format!("/api/channels/{ch}/publish"),
            json!({ "messages": msgs }),
        )
        .await;
    assert!(r.status().is_success(), "publish: {}", r.status());
}

fn ws_request(
    srv: &TestServer,
    path: &str,
    token: Option<&str>,
    origin: Option<&str>,
) -> tokio_tungstenite::tungstenite::handshake::client::Request {
    let mut req = format!("ws://{}{}", srv.http_addr, path)
        .into_client_request()
        .unwrap();
    let mut protos = "fluxmq.live.v1".to_string();
    if let Some(t) = token {
        protos.push_str(", fluxmq.token.");
        protos.push_str(t);
    }
    req.headers_mut().insert(
        "sec-websocket-protocol",
        HeaderValue::from_str(&protos).unwrap(),
    );
    if let Some(o) = origin {
        req.headers_mut()
            .insert("origin", HeaderValue::from_str(o).unwrap());
    }
    req
}

async fn ws_connect(srv: &TestServer, path: &str, token: Option<&str>) -> Ws {
    let (ws, resp) = tokio_tungstenite::connect_async(ws_request(srv, path, token, None))
        .await
        .expect("live ws connect");
    // Only our protocol is echoed, never the token.
    assert_eq!(
        resp.headers()
            .get("sec-websocket-protocol")
            .map(|v| v.to_str().unwrap()),
        Some("fluxmq.live.v1")
    );
    ws
}

/// HTTP status of a refused WebSocket handshake.
async fn ws_refused(
    srv: &TestServer,
    path: &str,
    token: Option<&str>,
    origin: Option<&str>,
) -> u16 {
    match tokio_tungstenite::connect_async(ws_request(srv, path, token, origin)).await {
        Err(WsError::Http(resp)) => resp.status().as_u16(),
        Ok(_) => panic!("{path}: handshake unexpectedly accepted"),
        Err(e) => panic!("{path}: {e}"),
    }
}

async fn next_event(ws: &mut Ws) -> Value {
    loop {
        let msg = tokio::time::timeout(WAIT, ws.next())
            .await
            .expect("live event in time")
            .expect("open")
            .expect("ok");
        match msg {
            Message::Text(t) => return serde_json::from_str(&t).expect("json"),
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("unexpected ws message {other:?}"),
        }
    }
}

/// The close frame's code (after draining events).
async fn ws_close_code(ws: &mut Ws) -> u16 {
    loop {
        match tokio::time::timeout(WAIT, ws.next())
            .await
            .expect("close in time")
        {
            Some(Ok(Message::Close(Some(f)))) => return u16::from(f.code),
            Some(Ok(Message::Close(None))) | None => return 0,
            Some(Ok(_)) => continue,
            Some(Err(e)) => panic!("ws error before close: {e}"),
        }
    }
}

/// Collected messages of one subscription.
#[derive(Default)]
struct Got {
    /// (partition, offset) → payload `i`.
    msgs: BTreeMap<(u64, u64), u64>,
    keys: BTreeSet<String>,
    cursor: Option<String>,
    events: Vec<String>,
}

impl Got {
    fn add(&mut self, ev: &Value) {
        let ty = ev["type"].as_str().unwrap().to_string();
        if let Some(c) = ev["cursor"].as_str() {
            self.cursor = Some(c.to_string());
        }
        if ty == "messages" {
            for m in ev["messages"].as_array().unwrap() {
                let k = (
                    m["partition"].as_u64().unwrap(),
                    m["offset"].as_u64().unwrap(),
                );
                let payload: Value = serde_json::from_str(m["payload"].as_str().unwrap()).unwrap();
                let prev = self.msgs.insert(k, payload["i"].as_u64().unwrap());
                assert!(prev.is_none(), "duplicate delivery of {k:?}");
                self.keys.insert(m["key"].as_str().unwrap().to_string());
            }
        }
        self.events.push(ty);
    }

    fn payloads(&self) -> BTreeSet<u64> {
        self.msgs.values().copied().collect()
    }
}

async fn ws_collect(ws: &mut Ws, got: &mut Got, want: usize) {
    while got.msgs.len() < want {
        let ev = next_event(ws).await;
        got.add(&ev);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn ws_earliest_live_and_resume_by_cursor() {
    let srv = TestServer::start().await;
    create_channel(&srv, "orders", 3).await;
    publish(&srv, "orders", 0, 500, 7).await;

    let mut ws = ws_connect(&srv, "/api/channels/orders/live?from=earliest", None).await;
    let hello = next_event(&mut ws).await;
    assert_eq!(hello["type"], "hello");
    assert_eq!(hello["sub"], "0");
    assert_eq!(hello["partitions"], json!([0, 1, 2]));
    assert!(hello["cursor"].as_str().unwrap().starts_with("c1."));
    let mut got = Got::default();
    ws_collect(&mut ws, &mut got, 500).await;
    // Then live: new messages arrive through the shared feed.
    publish(&srv, "orders", 500, 40, 7).await;
    ws_collect(&mut ws, &mut got, 540).await;
    assert_eq!(got.payloads(), (0..540).collect());
    let cursor = got.cursor.clone().unwrap();
    ws.close(None).await.unwrap();

    // Published while disconnected: resuming with the cursor yields exactly these.
    publish(&srv, "orders", 540, 25, 7).await;
    let mut ws = ws_connect(
        &srv,
        &format!("/api/channels/orders/live?from=earliest&cursor={cursor}"),
        None,
    )
    .await;
    assert_eq!(next_event(&mut ws).await["type"], "hello");
    let mut again = Got::default();
    ws_collect(&mut ws, &mut again, 25).await;
    assert_eq!(again.payloads(), (540..565).collect());
    // Nothing more arrives.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), ws.next())
            .await
            .is_err()
    );

    // `latest` (default) sees only what comes after.
    let mut tail = ws_connect(&srv, "/api/channels/orders/live", None).await;
    assert_eq!(next_event(&mut tail).await["type"], "hello");
    publish(&srv, "orders", 565, 3, 7).await;
    let mut t = Got::default();
    ws_collect(&mut tail, &mut t, 3).await;
    assert_eq!(t.payloads(), (565..568).collect());

    // A cursor of another channel is a 400; an unknown channel 404.
    create_channel(&srv, "other", 1).await;
    assert_eq!(
        ws_refused(
            &srv,
            &format!("/api/channels/other/live?cursor={cursor}"),
            None,
            None
        )
        .await,
        400
    );
    assert_eq!(
        ws_refused(&srv, "/api/channels/nope/live", None, None).await,
        404
    );
    assert_eq!(
        ws_refused(&srv, "/api/channels/orders/live?partitions=9", None, None).await,
        400
    );
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ws_filters_multiple_subscriptions_and_reset() {
    let srv = TestServer::start().await;
    create_channel(&srv, "a", 2).await;
    create_channel(&srv, "b", 1).await;
    publish(&srv, "a", 0, 100, 5).await;
    publish(&srv, "b", 0, 10, 1).await;

    let mut ws = ws_connect(
        &srv,
        "/api/channels/a/live?from=earliest&key=k1,k2&header.type=even",
        None,
    )
    .await;
    assert_eq!(next_event(&mut ws).await["type"], "hello");
    let mut got = Got::default();
    // i % 5 in {1, 2} and i even → i % 10 in {2, 6}: 20 of 100.
    ws_collect(&mut ws, &mut got, 20).await;
    assert!(got.payloads().iter().all(|i| i % 10 == 2 || i % 10 == 6));

    // A second subscription on the same connection, another channel, filter language.
    ws.send(Message::Text(
        json!({ "op": "subscribe", "id": "bb", "channel": "b", "from": "earliest",
                "filter": "i >= 5" })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let mut b = Got::default();
    while b.msgs.len() < 5 {
        let ev = next_event(&mut ws).await;
        if ev["sub"] == "bb" {
            b.add(&ev);
        }
    }
    assert_eq!(b.payloads(), (5..10).collect());
    // Duplicate id and bad commands are errors, the connection stays.
    ws.send(Message::Text(
        json!({ "op": "subscribe", "id": "bb" }).to_string().into(),
    ))
    .await
    .unwrap();
    let e = next_event(&mut ws).await;
    assert_eq!(e["type"], "error");
    ws.send(Message::Text("{nope".into())).await.unwrap();
    assert_eq!(next_event(&mut ws).await["type"], "error");
    ws.send(Message::Text(json!({ "op": "ping" }).to_string().into()))
        .await
        .unwrap();
    assert_eq!(next_event(&mut ws).await["type"], "pong");

    // A resume offset beyond the end is reported and clamped.
    let mut r = ws_connect(&srv, "/api/channels/b/live?from=offsets:0=1000", None).await;
    assert_eq!(next_event(&mut r).await["type"], "hello");
    let reset = next_event(&mut r).await;
    assert_eq!(reset["type"], "reset");
    assert_eq!(reset["requested"], 1000);
    assert_eq!(reset["actual"], 10);
    publish(&srv, "b", 10, 1, 1).await;
    let mut one = Got::default();
    ws_collect(&mut r, &mut one, 1).await;
    assert_eq!(one.payloads(), BTreeSet::from([10]));

    // Deleting the channel closes a connection whose only subscription it was (4404).
    let del = srv
        .req(reqwest::Method::DELETE, "/api/channels/b")
        .send()
        .await
        .unwrap();
    assert!(del.status().is_success());
    assert_eq!(ws_close_code(&mut r).await, 4404);
    srv.stop().await;
}

/// SSE event stream reader over a reqwest response.
struct Sse {
    resp: reqwest::Response,
    buf: String,
}

impl Sse {
    async fn next(&mut self) -> (Option<String>, String, Value) {
        loop {
            if let Some(end) = self.buf.find("\n\n") {
                let block: String = self.buf.drain(..end + 2).collect();
                if block.starts_with(':') {
                    continue; // heartbeat
                }
                let (mut id, mut ty, mut data) = (None, String::new(), String::new());
                for line in block.lines() {
                    if let Some(v) = line.strip_prefix("id: ") {
                        id = Some(v.to_string());
                    } else if let Some(v) = line.strip_prefix("event: ") {
                        ty = v.to_string();
                    } else if let Some(v) = line.strip_prefix("data: ") {
                        data.push_str(v);
                    }
                }
                return (id, ty, serde_json::from_str(&data).expect("event json"));
            }
            let chunk = tokio::time::timeout(WAIT, self.resp.chunk())
                .await
                .expect("sse chunk in time")
                .expect("sse body")
                .expect("stream open");
            self.buf.push_str(std::str::from_utf8(&chunk).unwrap());
        }
    }
}

async fn sse_open(srv: &TestServer, path_query: &str, last_event_id: Option<&str>) -> Sse {
    let mut req = srv.http.get(srv.url(path_query));
    if let Some(id) = last_event_id {
        req = req.header("last-event-id", id);
    }
    let resp = req.send().await.unwrap();
    assert_eq!(resp.status(), 200, "{path_query}");
    assert_eq!(
        resp.headers()["content-type"].to_str().unwrap(),
        "text/event-stream"
    );
    Sse {
        resp,
        buf: String::new(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sse_events_ids_and_last_event_id_resume() {
    let srv = TestServer::start().await;
    create_channel(&srv, "feed", 2).await;
    publish(&srv, "feed", 0, 60, 3).await;
    let mut s = sse_open(&srv, "/api/channels/feed/events?from=earliest&key=k0", None).await;
    let (_, ty, hello) = s.next().await;
    assert_eq!(
        (ty.as_str(), hello["type"].as_str()),
        ("hello", Some("hello"))
    );
    let mut got = Got::default();
    let mut last_id = None;
    while got.msgs.len() < 20 {
        let (id, ty, ev) = s.next().await;
        assert_eq!(ty, ev["type"].as_str().unwrap());
        if ty == "messages" {
            assert_eq!(id.as_deref(), ev["cursor"].as_str(), "id = cursor");
            last_id = id;
        }
        got.add(&ev);
    }
    assert_eq!(got.keys, BTreeSet::from(["k0".to_string()]));
    drop(s);
    // The browser's reconnect sends Last-Event-ID: only newer messages follow.
    publish(&srv, "feed", 60, 30, 3).await;
    let mut s = sse_open(
        &srv,
        "/api/channels/feed/events?from=earliest&key=k0",
        last_id.as_deref(),
    )
    .await;
    s.next().await; // hello
    let mut again = Got::default();
    while again.msgs.len() < 10 {
        let (_, _, ev) = s.next().await;
        again.add(&ev);
    }
    assert_eq!(again.payloads(), (60..90).filter(|i| i % 3 == 0).collect());
    // WS-only query tokens are not a thing; bad params are 400 before streaming.
    let r = srv
        .http
        .get(srv.url("/api/channels/feed/events?from=yesterday"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn subscriber_role_origin_limits_and_revocation() {
    let srv = TestServer::start_with(|c| {
        c.admin_token = Some("s3cret".into());
        c.live.max_connections_per_channel = 2;
        c.live.allowed_origins = vec!["https://app.example.com".into()];
    })
    .await;
    create_channel(&srv, "notifications.user42", 1).await;
    create_channel(&srv, "secret", 1).await;
    let resp = srv
        .post_json(
            "/api/auth/tokens",
            json!({ "name": "browser", "role": "subscriber", "ttl_secs": 3600,
                    "scopes": { "subscribe": ["notifications.*"] } }),
        )
        .await;
    assert_eq!(resp.status(), 201);
    let info: Value = resp.json().await.unwrap();
    let tok = info["token"].as_str().unwrap().to_string();
    // A subscriber needs subscribe patterns and nothing else.
    for bad in [
        json!({ "name": "x", "role": "subscriber" }),
        json!({ "name": "x", "role": "subscriber",
                "scopes": { "subscribe": ["a"], "publish": ["a"] } }),
    ] {
        assert_eq!(srv.post_json("/api/auth/tokens", bad).await.status(), 400);
    }

    let path = "/api/channels/notifications.user42/live";
    let mut ws = ws_connect(&srv, path, Some(&tok)).await;
    assert_eq!(next_event(&mut ws).await["type"], "hello");
    // No token, a channel outside the scope, a foreign origin.
    assert_eq!(ws_refused(&srv, path, None, None).await, 401);
    assert_eq!(
        ws_refused(&srv, "/api/channels/secret/live", Some(&tok), None).await,
        403
    );
    assert_eq!(
        ws_refused(&srv, path, Some(&tok), Some("https://evil.example")).await,
        403
    );
    // An allowed cross-origin page, and same-origin, pass.
    let (mut ok, _) = tokio_tungstenite::connect_async(ws_request(
        &srv,
        path,
        Some(&tok),
        Some("https://app.example.com"),
    ))
    .await
    .expect("allowed origin");
    // Per-channel limit: 2 already open.
    assert_eq!(ws_refused(&srv, path, Some(&tok), None).await, 429);
    // The subscriber token has no REST rights at all.
    let r = srv
        .http
        .get(srv.url("/api/channels"))
        .bearer_auth(&tok)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    // SSE takes the token from the query string. (The closed WS frees its channel slot
    // as soon as the server sees the close.)
    drop(ok.close(None).await);
    let sse_url = srv.url(&format!(
        "/api/channels/notifications.user42/events?access_token={tok}"
    ));
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let r = srv.http.get(&sse_url).send().await.unwrap();
        if r.status() == 200 {
            break;
        }
        assert_eq!(r.status(), 429);
        assert!(tokio::time::Instant::now() < deadline, "slot never freed");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Revocation closes the live connection with 4401.
    let id = info["info"]["id"].as_str().unwrap();
    let del = srv
        .req(reqwest::Method::DELETE, &format!("/api/auth/tokens/{id}"))
        .send()
        .await
        .unwrap();
    assert!(del.status().is_success());
    assert_eq!(ws_close_code(&mut ws).await, 4401);
    srv.stop().await;
}

/// Many tail subscribers share one feed: one read per batch, everyone gets everything.
#[tokio::test(flavor = "multi_thread")]
async fn shared_fanout_many_subscribers() {
    let srv = TestServer::start().await;
    create_channel(&srv, "hot", 2).await;
    let mut conns = Vec::new();
    for _ in 0..200 {
        let mut ws = ws_connect(&srv, "/api/channels/hot/live", None).await;
        assert_eq!(next_event(&mut ws).await["type"], "hello");
        conns.push(ws);
    }
    let metrics = srv
        .req(reqwest::Method::GET, "/metrics")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("fluxmq_live_feeds{node="), "{metrics}");
    let line = metrics
        .lines()
        .find(|l| l.starts_with("fluxmq_live_feeds{"))
        .unwrap();
    assert!(line.ends_with(" 1"), "{line}");
    assert!(metrics.contains("endpoint=\"ws\"} 200"));
    for round in 0..5 {
        publish(&srv, "hot", round * 20, 20, 4).await;
    }
    let all = futures::future::join_all(conns.iter_mut().map(|ws| async move {
        let mut g = Got::default();
        ws_collect(ws, &mut g, 100).await;
        g.payloads()
    }))
    .await;
    for p in all {
        assert_eq!(p, (0..100).collect::<BTreeSet<u64>>());
    }
    // The console's view of the same gateway.
    let st = srv.get_json("/api/live/status").await;
    let n = &st["nodes"][0];
    assert_eq!(n["connections_ws"], 200, "{st}");
    assert_eq!(n["connections_sse"], 0);
    assert_eq!(n["feeds"], 1);
    assert_eq!(
        n["channels"],
        json!([{ "channel": "hot", "subscriptions": 200 }])
    );
    assert_eq!(n["messages_sent"], 200 * 100);
    assert_eq!(n["limits"]["max_subscriptions"], 16);
    assert_eq!(n["rejections"], json!([]));
    drop(conns);
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let st = srv.get_json("/api/live/status").await;
        if st["nodes"][0]["connections_ws"] == 0 && st["nodes"][0]["channels"] == json!([]) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "slots not released: {st}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    srv.stop().await;
}
