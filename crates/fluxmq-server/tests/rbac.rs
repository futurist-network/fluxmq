//! RBAC end-to-end: access tokens created over REST, enforced on the client protocol and WS.

mod common;

use bytes::Bytes;
use common::{RawConn, TestServer};
use fluxmq_proto::model::{ChannelConfig, ChannelMode, PublishMessage, StartFrom};
use fluxmq_proto::wire::{ClientRequest, ErrorCode, PROTOCOL_VERSION, ServerResponse};
use futures::StreamExt;
use serde_json::json;

fn channel(name: &str) -> ChannelConfig {
    let mut c = ChannelConfig::new(name, ChannelMode::Queue);
    c.partitions = 1;
    c
}

fn publish(channel: &str) -> ClientRequest {
    ClientRequest::Publish {
        channel: channel.into(),
        partition: None,
        messages: vec![PublishMessage {
            key: None,
            headers: Default::default(),
            payload: Bytes::from_static(b"x"),
        }],
    }
}

fn subscribe(channel: &str) -> ClientRequest {
    ClientRequest::Subscribe {
        channel: channel.into(),
        group: "g".into(),
        prefetch: 10,
        start: StartFrom::Earliest,
    }
}

async fn create_token(srv: &TestServer, body: serde_json::Value) -> String {
    let resp = srv.post_json("/api/auth/tokens", body).await;
    assert_eq!(resp.status(), 201);
    let v: serde_json::Value = resp.json().await.unwrap();
    v["token"].as_str().unwrap().to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn wire_protocol_enforces_roles_and_scopes() {
    let srv = TestServer::start_with(|c| c.admin_token = Some("s3cret".into())).await;
    for name in ["orders.eu", "orders.us", "payments"] {
        srv.broker.create_channel(channel(name)).unwrap();
    }
    let svc = create_token(
        &srv,
        json!({
            "name": "svc",
            "role": "viewer",
            "scopes": {"publish": ["orders.*"], "consume": ["orders.eu"]}
        }),
    )
    .await;
    let viewer = create_token(&srv, json!({"name": "dash", "role": "viewer"})).await;
    let operator = create_token(&srv, json!({"name": "ops", "role": "operator"})).await;

    // Scoped viewer: only matching channels.
    let mut c = RawConn::connect_hello(srv.client_addr, Some(&svc)).await;
    assert!(matches!(
        c.request(publish("orders.eu")).await,
        Ok(ServerResponse::PublishOk { .. })
    ));
    assert!(matches!(
        c.request(publish("orders.us")).await,
        Ok(ServerResponse::PublishOk { .. })
    ));
    let err = c.request(publish("payments")).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::Forbidden);
    let sub = c.request(subscribe("orders.eu")).await;
    let Ok(ServerResponse::Subscribed { subscription_id }) = sub else {
        panic!("{sub:?}")
    };
    // Ack rides on the authorized subscription.
    let (_, d) = c.next_delivery().await;
    assert_eq!(
        c.request(ClientRequest::Ack {
            subscription_id,
            delivery_ids: vec![d.delivery_id],
        })
        .await,
        Ok(ServerResponse::Ok)
    );
    assert_eq!(
        c.request(subscribe("orders.us")).await.unwrap_err().code,
        ErrorCode::Forbidden
    );
    assert_eq!(
        c.request(ClientRequest::CreateChannel(channel("orders.new")))
            .await
            .unwrap_err()
            .code,
        ErrorCode::Forbidden
    );
    // Connection survives authorization failures.
    assert_eq!(
        c.request(ClientRequest::Ping).await,
        Ok(ServerResponse::Pong)
    );

    // Unscoped viewer: no data plane.
    let mut v = RawConn::connect_hello(srv.client_addr, Some(&viewer)).await;
    assert_eq!(
        v.request(publish("orders.eu")).await.unwrap_err().code,
        ErrorCode::Forbidden
    );

    // Operator: publish/consume anything, but no channel creation.
    let mut o = RawConn::connect_hello(srv.client_addr, Some(&operator)).await;
    assert!(o.request(publish("payments")).await.is_ok());
    assert!(o.request(subscribe("payments")).await.is_ok());
    assert_eq!(
        o.request(ClientRequest::CreateChannel(channel("new")))
            .await
            .unwrap_err()
            .code,
        ErrorCode::Forbidden
    );

    // Bootstrap admin: everything.
    let mut a = RawConn::connect_hello(srv.client_addr, Some("s3cret")).await;
    assert_eq!(
        a.request(ClientRequest::CreateChannel(channel("new")))
            .await,
        Ok(ServerResponse::ChannelCreated)
    );

    // Bad tokens: Unauthorized + Goodbye.
    let mut bad = RawConn::connect(srv.client_addr).await;
    // Flip the last secret char to a *different* hex digit (replacing it with a fixed digit
    // was a no-op 1 time in 16).
    let last = if svc.ends_with('0') { '1' } else { '0' };
    let forged = format!("{}{last}", &svc[..svc.len() - 1]);
    assert_ne!(forged, svc);
    assert_eq!(
        bad.hello(PROTOCOL_VERSION, Some(&forged))
            .await
            .unwrap_err()
            .code,
        ErrorCode::Unauthorized
    );
    assert!(bad.expect_goodbye().await.is_some());
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ws_and_rest_with_access_token() {
    let srv = TestServer::start_with(|c| c.admin_token = Some("s3cret".into())).await;
    let viewer = create_token(&srv, json!({"name": "dash", "role": "viewer"})).await;

    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://{}/ws?token={viewer}", srv.http_addr))
            .await
            .expect("viewer may open the telemetry socket");
    let first = tokio::time::timeout(common::WAIT, ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let hello: serde_json::Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
    assert_eq!(hello["type"], "hello");

    let http = reqwest::Client::new();
    let me: serde_json::Value = http
        .get(srv.url("/api/auth/me"))
        .bearer_auth(&viewer)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        (me["name"].as_str(), me["role"].as_str()),
        (Some("dash"), Some("viewer"))
    );
    let denied = http
        .post(srv.url("/api/dlq/retry"))
        .bearer_auth(&viewer)
        .json(&json!({"all": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 403);

    // Tokens persist in the meta store: visible in the listing with their creator.
    let list = srv.get_json("/api/auth/tokens").await;
    assert_eq!(list[0]["created_by"], "admin");
    // The secret is in no file of the data dir (meta store, audit log, ...).
    let secret = viewer.rsplit('_').next().unwrap().to_string();
    // (Scanned before stop: stopping drops the temp dir.)
    let dir = srv.dir.path().to_path_buf();
    let mut stack = vec![dir];
    let mut files = 0;
    while let Some(d) = stack.pop() {
        // A directory may vanish concurrently too (temp/trash dirs).
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else {
                files += 1;
                // Files may be rotated concurrently (tmp files); skip vanished ones.
                let Ok(bytes) = std::fs::read(&p) else {
                    continue;
                };
                assert!(
                    !bytes.windows(secret.len()).any(|w| w == secret.as_bytes()),
                    "secret found in {}",
                    p.display()
                );
            }
        }
    }
    assert!(files > 0);
    srv.stop().await;
}
