//! Raw client-protocol tests over real TCP.

mod common;

use bytes::Bytes;
use common::{RawConn, TestServer, eventually};
use fluxmq_proto::model::{ChannelConfig, ChannelMode, PublishMessage, RetryPolicy, StartFrom};
use fluxmq_proto::wire::{ClientRequest, ErrorCode, PROTOCOL_VERSION, ServerFrame, ServerResponse};

fn channel(name: &str, mode: ChannelMode) -> ChannelConfig {
    let mut c = ChannelConfig::new(name, mode);
    c.partitions = 1;
    c
}

fn msg(payload: &'static str) -> PublishMessage {
    PublishMessage {
        key: None,
        headers: Default::default(),
        payload: Bytes::from_static(payload.as_bytes()),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn handshake_ok_then_ping() {
    let srv = TestServer::start().await;
    let mut c = RawConn::connect(srv.client_addr).await;
    match c.hello(PROTOCOL_VERSION, None).await {
        Ok(ServerResponse::HelloOk {
            broker_version,
            connection_id,
            ..
        }) => {
            assert_eq!(broker_version, fluxmq_proto::VERSION);
            assert!(connection_id > 0);
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(
        c.request(ClientRequest::Ping).await,
        Ok(ServerResponse::Pong)
    );
    // A second Hello is rejected but the connection stays usable.
    let again = c.hello(PROTOCOL_VERSION, None).await.unwrap_err();
    assert_eq!(again.code, ErrorCode::ProtocolViolation);
    assert_eq!(
        c.request(ClientRequest::Ping).await,
        Ok(ServerResponse::Pong)
    );
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn handshake_rejects_wrong_version_and_non_hello() {
    let srv = TestServer::start().await;

    let mut c = RawConn::connect(srv.client_addr).await;
    let err = c.hello(PROTOCOL_VERSION + 1, None).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::ProtocolViolation);
    assert!(c.expect_goodbye().await.is_some());
    assert!(c.recv().await.is_none(), "connection closed after Goodbye");

    let mut c = RawConn::connect(srv.client_addr).await;
    let err = c.request(ClientRequest::Ping).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::ProtocolViolation);
    assert!(c.expect_goodbye().await.is_some());

    // Garbage frame → Goodbye with a protocol error.
    let mut c = RawConn::connect(srv.client_addr).await;
    c.send_raw(&[0, 0, 0, 2, 0xff, 0xff]).await;
    let reason = c.expect_goodbye().await.expect("goodbye");
    assert!(reason.contains("protocol"), "{reason}");
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn handshake_requires_token_when_configured() {
    let srv = TestServer::start_with(|c| c.admin_token = Some("s3cret".into())).await;

    let mut c = RawConn::connect(srv.client_addr).await;
    let err = c.hello(PROTOCOL_VERSION, None).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::Unauthorized);
    assert!(c.expect_goodbye().await.is_some());

    let mut c = RawConn::connect(srv.client_addr).await;
    assert_eq!(
        c.hello(PROTOCOL_VERSION, Some("nope"))
            .await
            .unwrap_err()
            .code,
        ErrorCode::Unauthorized
    );

    let mut c = RawConn::connect_hello(srv.client_addr, Some("s3cret")).await;
    assert_eq!(
        c.request(ClientRequest::Ping).await,
        Ok(ServerResponse::Pong)
    );
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_subscribe_ack() {
    let srv = TestServer::start().await;
    let mut c = RawConn::connect_hello(srv.client_addr, None).await;

    assert_eq!(
        c.request(ClientRequest::CreateChannel(channel(
            "orders",
            ChannelMode::Stream
        )))
        .await,
        Ok(ServerResponse::ChannelCreated)
    );
    let dup = c
        .request(ClientRequest::CreateChannel(channel(
            "orders",
            ChannelMode::Stream,
        )))
        .await
        .unwrap_err();
    assert_eq!(dup.code, ErrorCode::AlreadyExists);

    let published = c
        .request(ClientRequest::Publish {
            partition: None,
            channel: "orders".into(),
            messages: vec![msg("a"), msg("b")],
        })
        .await;
    let Ok(ServerResponse::PublishOk { ids }) = published else {
        panic!("{published:?}")
    };
    assert_eq!(ids.len(), 2);

    let missing = c
        .request(ClientRequest::Publish {
            partition: None,
            channel: "nope".into(),
            messages: vec![msg("x")],
        })
        .await;
    assert_eq!(missing.unwrap_err().code, ErrorCode::NotFound);

    let sub = c
        .request(ClientRequest::Subscribe {
            channel: "orders".into(),
            group: "billing".into(),
            prefetch: 10,
            start: StartFrom::Earliest,
        })
        .await;
    let Ok(ServerResponse::Subscribed { subscription_id }) = sub else {
        panic!("{sub:?}")
    };

    let (sid, d1) = c.next_delivery().await;
    assert_eq!(sid, subscription_id);
    assert_eq!(d1.attempt, 1);
    assert_eq!(&d1.message.payload[..], b"a");
    let (_, d2) = c.next_delivery().await;
    assert_eq!(&d2.message.payload[..], b"b");

    // Ack with a response, then fire-and-forget.
    assert_eq!(
        c.request(ClientRequest::Ack {
            subscription_id,
            delivery_ids: vec![d1.delivery_id]
        })
        .await,
        Ok(ServerResponse::Ok)
    );
    c.send(
        0,
        ClientRequest::Ack {
            subscription_id,
            delivery_ids: vec![d2.delivery_id],
        },
    )
    .await;

    // Unknown subscription.
    let err = c
        .request(ClientRequest::Ack {
            subscription_id: 999_999,
            delivery_ids: vec![1],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);

    // Live delivery after subscribe.
    c.request(ClientRequest::Publish {
        partition: None,
        channel: "orders".into(),
        messages: vec![msg("c")],
    })
    .await
    .unwrap();
    let (_, d3) = c.next_delivery().await;
    assert_eq!(&d3.message.payload[..], b"c");
    c.send(
        0,
        ClientRequest::Ack {
            subscription_id,
            delivery_ids: vec![d3.delivery_id],
        },
    )
    .await;

    // Group lag drains to zero once everything is acked.
    eventually(|| async { (srv.group_lag("orders", "billing") == Some(0)).then_some(()) }).await;

    assert_eq!(
        c.request(ClientRequest::Unsubscribe { subscription_id })
            .await,
        Ok(ServerResponse::Ok)
    );
    assert_eq!(
        c.request(ClientRequest::Unsubscribe { subscription_id })
            .await
            .unwrap_err()
            .code,
        ErrorCode::NotFound
    );
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nack_retry_then_dead_letter_visible_via_rest() {
    let srv = TestServer::start().await;
    let mut c = RawConn::connect_hello(srv.client_addr, None).await;
    let mut cfg = channel("payments", ChannelMode::Queue);
    cfg.retry = RetryPolicy {
        max_attempts: 2,
        backoff_ms: vec![10],
        ack_timeout_ms: 30_000,
    };
    c.request(ClientRequest::CreateChannel(cfg)).await.unwrap();
    c.request(ClientRequest::Publish {
        partition: None,
        channel: "payments".into(),
        messages: vec![msg("{\"amount\":5}")],
    })
    .await
    .unwrap();
    let Ok(ServerResponse::Subscribed { subscription_id }) = c
        .request(ClientRequest::Subscribe {
            channel: "payments".into(),
            group: "workers".into(),
            prefetch: 5,
            start: StartFrom::Earliest,
        })
        .await
    else {
        panic!("subscribe failed")
    };

    let (_, d1) = c.next_delivery().await;
    assert_eq!(d1.attempt, 1);
    c.request(ClientRequest::Nack {
        subscription_id,
        delivery_id: d1.delivery_id,
        reason: "PostgreSQL timeout".into(),
        retry: true,
    })
    .await
    .unwrap();
    let (_, d2) = c.next_delivery().await;
    assert_eq!(d2.attempt, 2);
    assert_eq!(d2.message.offset, d1.message.offset);
    c.send(
        0,
        ClientRequest::Nack {
            subscription_id,
            delivery_id: d2.delivery_id,
            reason: "PostgreSQL timeout".into(),
            retry: true,
        },
    )
    .await;

    // Dead-lettered after max_attempts → visible in REST.
    let page = eventually(|| async {
        let page = srv.get_json("/api/dlq?channel=payments").await;
        (page["total"] == 1).then_some(page)
    })
    .await;
    let entry = &page["entries"][0];
    assert_eq!(entry["reason"], "PostgreSQL timeout");
    assert_eq!(entry["group"], "workers");
    assert_eq!(
        entry["message"]["payload"],
        serde_json::json!({"encoding": "utf8", "data": "{\"amount\":5}"})
    );
    let id = entry["id"].as_u64().unwrap();

    let summary = srv.get_json("/api/dlq/summary").await;
    assert_eq!(summary["total"], 1);
    assert_eq!(summary["groups"][0]["reason"], "PostgreSQL timeout");

    // Retry from the DLQ → redelivered to the group with a fresh attempt budget.
    let resp = srv
        .post_json(
            "/api/dlq/retry",
            serde_json::json!({ "ids": [["payments", id]] }),
        )
        .await;
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.json::<serde_json::Value>().await.unwrap()["affected"],
        1
    );
    let (_, d3) = c.next_delivery().await;
    assert_eq!(d3.message.offset, d1.message.offset);
    assert_eq!(d3.attempt, 1);
    c.request(ClientRequest::Ack {
        subscription_id,
        delivery_ids: vec![d3.delivery_id],
    })
    .await
    .unwrap();
    assert_eq!(srv.get_json("/api/dlq?channel=payments").await["total"], 0);

    // The retry is audited.
    let audit = srv.get_json("/api/audit?limit=5").await;
    assert_eq!(audit[0]["action"], "dlq.retry");
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nack_without_retry_dead_letters_immediately() {
    let srv = TestServer::start().await;
    let mut c = RawConn::connect_hello(srv.client_addr, None).await;
    c.request(ClientRequest::CreateChannel(channel(
        "emails",
        ChannelMode::Stream,
    )))
    .await
    .unwrap();
    c.request(ClientRequest::Publish {
        partition: None,
        channel: "emails".into(),
        messages: vec![msg("x")],
    })
    .await
    .unwrap();
    let Ok(ServerResponse::Subscribed { subscription_id }) = c
        .request(ClientRequest::Subscribe {
            channel: "emails".into(),
            group: "senders".into(),
            prefetch: 1,
            start: StartFrom::Earliest,
        })
        .await
    else {
        panic!()
    };
    let (_, d) = c.next_delivery().await;
    c.request(ClientRequest::Nack {
        subscription_id,
        delivery_id: d.delivery_id,
        reason: "bad address".into(),
        retry: false,
    })
    .await
    .unwrap();
    eventually(|| async {
        (srv.get_json("/api/dlq?channel=emails").await["total"] == 1).then_some(())
    })
    .await;
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn prefetch_bounds_outstanding_deliveries() {
    let srv = TestServer::start().await;
    let mut c = RawConn::connect_hello(srv.client_addr, None).await;
    c.request(ClientRequest::CreateChannel(channel(
        "bulk",
        ChannelMode::Queue,
    )))
    .await
    .unwrap();
    let messages = (0..10).map(|_| msg("m")).collect();
    c.request(ClientRequest::Publish {
        partition: None,
        channel: "bulk".into(),
        messages,
    })
    .await
    .unwrap();
    let Ok(ServerResponse::Subscribed { subscription_id }) = c
        .request(ClientRequest::Subscribe {
            channel: "bulk".into(),
            group: "g".into(),
            prefetch: 3,
            start: StartFrom::Earliest,
        })
        .await
    else {
        panic!()
    };
    let mut got = Vec::new();
    for _ in 0..3 {
        got.push(c.next_delivery().await.1);
    }
    // No 4th delivery while 3 are outstanding: a Ping round trip shows nothing else arrived.
    assert_eq!(
        c.request(ClientRequest::Ping).await,
        Ok(ServerResponse::Pong)
    );
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(
        c.request(ClientRequest::Ping).await,
        Ok(ServerResponse::Pong)
    );
    assert!(c.deliveries.is_empty(), "prefetch exceeded");
    c.request(ClientRequest::Ack {
        subscription_id,
        delivery_ids: got.iter().map(|d| d.delivery_id).collect(),
    })
    .await
    .unwrap();
    for _ in 0..3 {
        c.next_delivery().await;
    }
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_sends_goodbye() {
    let srv = TestServer::start().await;
    let mut c = RawConn::connect_hello(srv.client_addr, None).await;
    srv.trigger_shutdown();
    let reason = c.expect_goodbye().await.expect("goodbye on shutdown");
    assert!(reason.contains("shutting down"), "{reason}");
    assert!(matches!(
        c.recv().await,
        None | Some(ServerFrame::Goodbye { .. })
    ));
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn system_channels_are_read_only_over_the_wire() {
    let srv = TestServer::start().await;
    let mut c = RawConn::connect_hello(srv.client_addr, None).await;
    let sys = fluxmq_proto::notify::SYSTEM_EVENTS_CHANNEL;
    // Created by the server on start.
    eventually(|| async { srv.broker.channel(sys).is_some().then_some(()) }).await;

    let err = c
        .request(ClientRequest::CreateChannel(channel(
            "$mine",
            ChannelMode::Stream,
        )))
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    let err = c
        .request(ClientRequest::Publish {
            partition: None,
            channel: sys.into(),
            messages: vec![msg("x")],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::Forbidden);
    assert!(err.message.contains("read-only"), "{}", err.message);
    // Consuming it is allowed.
    let sub = c
        .request(ClientRequest::Subscribe {
            channel: sys.into(),
            group: "audit-sync".into(),
            prefetch: 10,
            start: StartFrom::Earliest,
        })
        .await;
    assert!(
        matches!(sub, Ok(ServerResponse::Subscribed { .. })),
        "{sub:?}"
    );
    srv.stop().await;
}
