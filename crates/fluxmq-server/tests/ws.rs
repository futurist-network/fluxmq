//! WebSocket telemetry gateway end-to-end tests.

mod common;

use std::time::Duration;

use common::{TestServer, WAIT};
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn connect(srv: &TestServer, query: &str) -> Ws {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws{query}", srv.http_addr))
        .await
        .expect("ws connect");
    ws
}

async fn next_json(ws: &mut Ws) -> Value {
    loop {
        let msg = tokio::time::timeout(WAIT, ws.next())
            .await
            .expect("ws message in time")
            .expect("open")
            .expect("ok");
        match msg {
            Message::Text(t) => return serde_json::from_str(&t).expect("json"),
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("unexpected ws message {other:?}"),
        }
    }
}

async fn send(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string().into()))
        .await
        .expect("ws send");
}

#[tokio::test(flavor = "multi_thread")]
async fn hello_snapshot_then_delta() {
    let srv = TestServer::start().await;
    let mut ws = connect(&srv, "").await;
    let hello = next_json(&mut ws).await;
    assert_eq!(hello["type"], "hello");
    assert_eq!(hello["contract_version"], 1);

    send(
        &mut ws,
        json!({ "op": "subscribe", "topics": ["cluster.overview"] }),
    )
    .await;
    let snap = next_json(&mut ws).await;
    assert_eq!(snap["type"], "snapshot");
    assert_eq!(snap["topic"], "cluster.overview");
    let seq = snap["seq"].as_u64().unwrap();
    assert!(snap["data"]["cluster_name"].is_string());

    // Generate some change so a delta is due.
    srv.post_json(
        "/api/channels",
        json!({ "name": "ws-test", "mode": "stream" }),
    )
    .await;
    let delta = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let m = next_json(&mut ws).await;
            if m["type"] == "delta" && m["topic"] == "cluster.overview" {
                return m;
            }
        }
    })
    .await
    .expect("delta within 10 s");
    assert_eq!(delta["seq"].as_u64().unwrap(), seq + 1);

    send(&mut ws, json!({ "op": "ping", "nonce": 7 })).await;
    let pong = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let m = next_json(&mut ws).await;
            if m["type"] == "pong" {
                return m;
            }
        }
    })
    .await
    .expect("pong");
    assert_eq!(pong["nonce"], 7);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_and_unknown_topics_get_errors_without_closing() {
    let srv = TestServer::start().await;
    let mut ws = connect(&srv, "").await;
    assert_eq!(next_json(&mut ws).await["type"], "hello");

    ws.send(Message::Text("not json".into())).await.unwrap();
    let err = next_json(&mut ws).await;
    assert_eq!(err["type"], "error");

    send(
        &mut ws,
        json!({ "op": "subscribe", "topics": ["bogus.topic"] }),
    )
    .await;
    let err = next_json(&mut ws).await;
    assert_eq!(err["type"], "error");
    assert_eq!(err["topic"], "bogus.topic");

    // Still alive.
    send(&mut ws, json!({ "op": "subscribe", "topics": ["alerts"] })).await;
    let snap = next_json(&mut ws).await;
    assert_eq!(snap["type"], "snapshot");
    assert_eq!(snap["topic"], "alerts");
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ws_requires_token_when_configured() {
    let srv = TestServer::start_with(|c| c.admin_token = Some("s3cret".into())).await;
    let denied = tokio_tungstenite::connect_async(format!("ws://{}/ws", srv.http_addr)).await;
    assert!(denied.is_err(), "upgrade must be refused without a token");
    let mut ws = connect(&srv, "?token=s3cret").await;
    assert_eq!(next_json(&mut ws).await["type"], "hello");
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ws_closes_on_shutdown() {
    let srv = TestServer::start().await;
    let mut ws = connect(&srv, "").await;
    assert_eq!(next_json(&mut ws).await["type"], "hello");
    srv.trigger_shutdown();
    let closed = tokio::time::timeout(WAIT, async {
        loop {
            match ws.next().await {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
                Some(Ok(_)) => continue,
            }
        }
    })
    .await;
    assert!(closed.is_ok(), "websocket closed on shutdown");
    srv.stop().await;
}
