mod common;

use std::time::Duration;

use common::{Behavior, FakeBroker, payload};
use fluxmq_client::{
    BatchingOptions, Client, ClientError, ClientOptions, ErrorCode, ReconnectPolicy, StartFrom,
    SubscribeOptions, message,
};
use fluxmq_proto::{ChannelConfig, ChannelMode, PublishMessage, headers};
use futures::StreamExt;

async fn setup(behavior: Behavior) -> (FakeBroker, Client) {
    let broker = FakeBroker::start(behavior).await;
    let client = Client::connect(ClientOptions::new(broker.addr(), "test-client"))
        .await
        .unwrap();
    client
        .create_channel(ChannelConfig::new("ch", ChannelMode::Stream))
        .await
        .unwrap();
    (broker, client)
}

fn opts(earliest: bool, prefetch: u32) -> SubscribeOptions {
    SubscribeOptions::default()
        .prefetch(prefetch)
        .start(if earliest {
            StartFrom::Earliest
        } else {
            StartFrom::Latest
        })
}

#[tokio::test]
async fn handshake_and_server_info() {
    let broker = FakeBroker::start(Behavior::default()).await;
    let client = Client::connect(ClientOptions::new(broker.addr(), "order-api-1"))
        .await
        .unwrap();
    assert_eq!(client.server_info().broker_version, "fake");
    assert_eq!(client.client_name(), "order-api-1");
    assert_eq!(
        broker.rec(|r| r.hellos.clone()),
        vec!["order-api-1".to_string()]
    );
    client.ping().await.unwrap();
}

#[tokio::test]
async fn handshake_rejects_bad_token() {
    let broker = FakeBroker::start(Behavior {
        token: Some("secret".into()),
        ..Default::default()
    })
    .await;
    let err = Client::connect(ClientOptions::new(broker.addr(), "x").auth_token("nope"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::Unauthorized), "{err}");
    Client::connect(ClientOptions::new(broker.addr(), "x").auth_token("secret"))
        .await
        .unwrap();
}

#[tokio::test]
async fn connect_refused_is_io_error() {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    drop(l);
    let err = Client::connect(ClientOptions::new(addr, "x"))
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::Io(_)), "{err}");
}

#[tokio::test]
async fn ensure_channel_treats_already_exists_as_ok() {
    let (_b, client) = setup(Behavior::default()).await;
    let cfg = ChannelConfig::new("ch", ChannelMode::Stream);
    let err = client.create_channel(cfg.clone()).await.unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::AlreadyExists));
    assert!(!client.ensure_channel(cfg).await.unwrap());
    assert!(
        client
            .ensure_channel(ChannelConfig::new("new", ChannelMode::Queue))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn server_error_is_typed() {
    let (_b, client) = setup(Behavior::default()).await;
    let err = client
        .publish("missing", PublishMessage::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::NotFound));
}

#[tokio::test]
async fn concurrent_requests_correlate_out_of_order() {
    let (broker, client) = setup(Behavior {
        random_delay_ms: 20,
        ..Default::default()
    })
    .await;
    let mut handles = Vec::new();
    for i in 0..200u32 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let n = (i % 5 + 1) as usize;
            let msgs: Vec<_> = (0..n)
                .map(|j| PublishMessage {
                    payload: payload(&format!("{i}-{j}")),
                    ..Default::default()
                })
                .collect();
            (i, n, c.publish_batch("ch", msgs).await.unwrap())
        }));
    }
    let log = {
        let mut out = Vec::new();
        for h in handles {
            out.push(h.await.unwrap());
        }
        out
    };
    let stored = broker.messages("ch");
    for (i, n, ids) in log {
        assert_eq!(ids.len(), n);
        for (j, id) in ids.iter().enumerate() {
            // Each response must be routed to the request that caused it.
            assert_eq!(
                stored[id.offset as usize].payload,
                payload(&format!("{i}-{j}"))
            );
        }
    }
}

#[tokio::test]
async fn publish_batch_preserves_order_and_producer_header() {
    let (broker, client) = setup(Behavior::default()).await;
    let producer = client.producer("ch");
    let ids = producer
        .send_batch((0..10).map(|i| message(format!("m{i}")).key(format!("k{i}"))))
        .await
        .unwrap();
    assert_eq!(
        ids.iter().map(|i| i.offset).collect::<Vec<_>>(),
        (0..10).collect::<Vec<_>>()
    );
    assert_eq!(broker.rec(|r| r.publish_frames), 1);
    let id = producer
        .send(
            message("x")
                .header(headers::PRODUCER, "override")
                .event_type("E")
                .new_trace_id(),
        )
        .await
        .unwrap();
    let stored = broker.messages("ch");
    assert_eq!(
        stored[0].headers.get(headers::PRODUCER).map(String::as_str),
        Some("test-client")
    );
    assert_eq!(stored[3].key.as_deref(), Some("k3"));
    let last = &stored[id.offset as usize];
    assert_eq!(
        last.headers.get(headers::PRODUCER).map(String::as_str),
        Some("override")
    );
    assert_eq!(
        last.headers.get(headers::TRACE_ID).map(|s| s.len()),
        Some(32)
    );
    assert_eq!(client.publish_batch("ch", vec![]).await.unwrap(), vec![]);
}

#[tokio::test]
async fn subscribe_deliver_and_batched_acks() {
    let (broker, client) = setup(Behavior::default()).await;
    let producer = client.producer("ch");
    producer
        .send_batch((0..100).map(|i| message(format!("{i}")).key("k")))
        .await
        .unwrap();
    let mut consumer = client.subscribe("ch", "g", opts(true, 100)).await.unwrap();
    let mut seen = Vec::new();
    for _ in 0..100 {
        let d = consumer.next().await.unwrap().unwrap();
        assert_eq!(d.attempt(), 1);
        assert_eq!(d.key(), Some("k"));
        assert_eq!(d.header(headers::PRODUCER), Some("test-client"));
        seen.push(
            std::str::from_utf8(d.payload())
                .unwrap()
                .parse::<u32>()
                .unwrap(),
        );
        d.ack().await.unwrap();
    }
    assert_eq!(seen, (0..100).collect::<Vec<_>>());
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (frames, ids, req_ids) = broker.rec(|r| {
        (
            r.ack_frames.len(),
            r.acked_ids(),
            r.ack_frame_req_ids.clone(),
        )
    });
    assert_eq!(ids.len(), 100);
    assert!(
        frames < 100,
        "acks should be coalesced, got {frames} frames"
    );
    assert!(req_ids.iter().all(|&r| r == 0), "acks are fire-and-forget");
}

#[tokio::test]
async fn deliveries_published_after_subscribe_arrive_and_prefetch_is_respected() {
    let (broker, client) = setup(Behavior::default()).await;
    let mut consumer = client.subscribe("ch", "g", opts(false, 4)).await.unwrap();
    let producer = client.producer("ch");
    producer
        .send_batch((0..20).map(|i| message(format!("{i}"))))
        .await
        .unwrap();
    // Without acks only `prefetch` deliveries arrive.
    let mut held = Vec::new();
    for _ in 0..4 {
        held.push(consumer.next().await.unwrap().unwrap());
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(150), consumer.next())
            .await
            .is_err()
    );
    for d in held {
        d.ack().await.unwrap();
    }
    for _ in 4..20 {
        consumer.next().await.unwrap().unwrap().ack().await.unwrap();
    }
    consumer.unsubscribe().await.unwrap();
    assert_eq!(broker.rec(|r| r.acked_ids().len()), 20);
    assert_eq!(broker.rec(|r| r.unsubscribes.len()), 1);
}

#[tokio::test]
async fn nack_retry_redelivers_and_dlq_nack_does_not() {
    let (broker, client) = setup(Behavior::default()).await;
    client.publish("ch", message("a").build()).await.unwrap();
    client.publish("ch", message("b").build()).await.unwrap();
    let mut consumer = client.subscribe("ch", "g", opts(true, 10)).await.unwrap();
    let a = consumer.next().await.unwrap().unwrap();
    let b = consumer.next().await.unwrap().unwrap();
    a.nack("PostgreSQL timeout", true).await.unwrap();
    b.nack("Invalid schema", false).await.unwrap();
    let again = consumer.next().await.unwrap().unwrap();
    assert_eq!(again.payload().as_ref(), b"a");
    assert_eq!(again.attempt(), 2);
    again.ack().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(150), consumer.next())
            .await
            .is_err()
    );
    let nacks = broker.rec(|r| r.nacks.clone());
    assert_eq!(nacks.len(), 2);
    assert_eq!(nacks[0].1, "PostgreSQL timeout");
    assert!(nacks[0].2);
    assert!(!nacks[1].2);
}

#[tokio::test]
async fn reports_stats_with_processing_time_and_last_error() {
    let broker = FakeBroker::start(Behavior::default()).await;
    let mut o = ClientOptions::new(broker.addr(), "worker-03");
    o.stats_interval = Duration::from_millis(200);
    let client = Client::connect(o).await.unwrap();
    client
        .create_channel(ChannelConfig::new("ch", ChannelMode::Queue))
        .await
        .unwrap();
    client
        .publish_batch(
            "ch",
            (0..3).map(|i| message(format!("{i}")).build()).collect(),
        )
        .await
        .unwrap();
    let mut consumer = client.subscribe("ch", "g", opts(true, 10)).await.unwrap();
    for i in 0..3 {
        let d = consumer.next().await.unwrap().unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        if i == 2 {
            d.nack("HTTP 503", true).await.unwrap()
        } else {
            d.ack().await.unwrap()
        }
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let stats = broker.rec(|r| r.stats.clone());
    let s = stats
        .iter()
        .find(|s| s.processing_p50_ms.is_some())
        .expect("stats with samples");
    assert_eq!(s.subscription_id, consumer.subscription_id());
    let p50 = s.processing_p50_ms.unwrap();
    assert!((15.0..500.0).contains(&p50), "p50 {p50}");
    assert_eq!(s.last_error.as_deref(), Some("HTTP 503"));
    assert!(stats.iter().any(|s| s.memory_bytes.is_some()));
}

#[tokio::test]
async fn request_timeout() {
    let broker = FakeBroker::start(Behavior {
        swallow_publish: true,
        ..Default::default()
    })
    .await;
    let o = ClientOptions::new(broker.addr(), "t")
        .request_timeout(Duration::from_millis(100))
        .publish_timeout(Duration::from_millis(350));
    let client = Client::connect(o).await.unwrap();
    client
        .create_channel(ChannelConfig::new("ch", ChannelMode::Stream))
        .await
        .unwrap();
    let err = client
        .publish("ch", message("x").build())
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::Timeout), "{err}");
    // Timed-out publishes are retried until publish_timeout.
    let frames = broker.rec(|r| r.publish_frames);
    assert!((2..=4).contains(&frames), "{frames} attempts");
    // Connection still usable afterwards.
    client.ping().await.unwrap();
}

#[tokio::test]
async fn connection_loss_fails_pending_requests_and_ends_consumers() {
    let broker = FakeBroker::start(Behavior {
        swallow_publish: true,
        ..Default::default()
    })
    .await;
    let client = Client::connect(
        ClientOptions::new(broker.addr(), "t").reconnect(ReconnectPolicy::disabled()),
    )
    .await
    .unwrap();
    client
        .create_channel(ChannelConfig::new("ch", ChannelMode::Stream))
        .await
        .unwrap();
    let mut consumer = client.subscribe("ch", "g", opts(true, 10)).await.unwrap();
    let c2 = client.clone();
    let pending = tokio::spawn(async move { c2.publish("ch", message("x").build()).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    broker.kill_connections();
    let err = pending.await.unwrap().unwrap_err();
    assert!(matches!(err, ClientError::Closed(_)), "{err}");
    let item = consumer.next().await.unwrap();
    assert!(matches!(item, Err(ClientError::Closed(_))));
    assert!(consumer.next().await.is_none());
    assert!(client.is_closed());
    let err = client.ping().await.unwrap_err();
    assert!(matches!(err, ClientError::Closed(_)), "{err}");
}

#[tokio::test]
async fn goodbye_reason_is_reported() {
    let broker = FakeBroker::start(Behavior::default()).await;
    let client = Client::connect(
        ClientOptions::new(broker.addr(), "t").reconnect(ReconnectPolicy::disabled()),
    )
    .await
    .unwrap();
    client
        .create_channel(ChannelConfig::new("ch", ChannelMode::Stream))
        .await
        .unwrap();
    let mut consumer = client.subscribe("ch", "g", opts(true, 10)).await.unwrap();
    broker.goodbye("shutting down");
    match consumer.next().await.unwrap() {
        Err(ClientError::Closed(r)) => assert!(r.contains("shutting down"), "{r}"),
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn close_fails_requests_and_ends_consumers() {
    let (_broker, client) = setup(Behavior::default()).await;
    let mut consumer = client.subscribe("ch", "g", opts(true, 10)).await.unwrap();
    client.close();
    assert!(matches!(
        consumer.next().await,
        Some(Err(ClientError::Closed(_)))
    ));
    assert!(matches!(client.ping().await, Err(ClientError::Closed(_))));
}

#[tokio::test]
async fn batching_producer_batches_and_resolves_each_future() {
    let (broker, client) = setup(Behavior::default()).await;
    let bp = client.batching_producer(
        "ch",
        BatchingOptions {
            linger: Duration::from_millis(5),
            max_batch: 100,
            ..Default::default()
        },
    );
    let mut futs = Vec::new();
    for i in 0..1000 {
        futs.push(bp.send(message(format!("{i}"))).await.unwrap());
    }
    let stored_ids: Vec<_> = futures::future::join_all(futs)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    let stored = broker.messages("ch");
    assert_eq!(stored.len(), 1000);
    for (i, id) in stored_ids.iter().enumerate() {
        assert_eq!(stored[id.offset as usize].payload, payload(&format!("{i}")));
        assert_eq!(
            stored[id.offset as usize]
                .headers
                .get(headers::PRODUCER)
                .map(String::as_str),
            Some("test-client")
        );
    }
    let frames = broker.rec(|r| r.publish_frames);
    assert!(
        (10..=30).contains(&frames),
        "expected ~10 batches of <=100, got {frames}"
    );
    bp.publish(message("last")).await.unwrap();
    bp.close().await;
}

#[tokio::test]
async fn batching_producer_propagates_errors() {
    let (_broker, client) = setup(Behavior::default()).await;
    let bp = client.batching_producer("missing", BatchingOptions::default());
    let a = bp.send(message("a")).await.unwrap();
    let b = bp.send(message("b")).await.unwrap();
    assert_eq!(a.await.unwrap_err().code(), Some(ErrorCode::NotFound));
    assert_eq!(b.await.unwrap_err().code(), Some(ErrorCode::NotFound));
}

#[tokio::test]
async fn consumer_works_as_stream_with_combinators() {
    let (_broker, client) = setup(Behavior::default()).await;
    client
        .publish_batch(
            "ch",
            (0..5).map(|i| message(format!("{i}")).build()).collect(),
        )
        .await
        .unwrap();
    let consumer = client.subscribe("ch", "g", opts(true, 10)).await.unwrap();
    let got: Vec<_> = consumer
        .take(5)
        .then(|d| async move {
            let d = d.unwrap();
            let p = d.payload().clone();
            d.ack().await.unwrap();
            p
        })
        .collect()
        .await;
    assert_eq!(got.len(), 5);
}

#[tokio::test]
async fn stamp_producer_can_be_turned_off() {
    let broker = FakeBroker::start(Behavior::default()).await;
    let client = Client::connect(ClientOptions::new(broker.addr(), "quiet").stamp_producer(false))
        .await
        .unwrap();
    client
        .create_channel(ChannelConfig::new("ch", ChannelMode::Stream))
        .await
        .unwrap();
    client.producer("ch").send(message("a")).await.unwrap();
    client
        .producer("ch")
        .send(message("b").header(headers::PRODUCER, "explicit"))
        .await
        .unwrap();
    let bp = client.batching_producer("ch", BatchingOptions::default());
    bp.publish(message("c")).await.unwrap();
    bp.close().await;
    let stored = broker.messages("ch");
    assert!(!stored[0].headers.contains_key(headers::PRODUCER));
    assert_eq!(
        stored[1].headers.get(headers::PRODUCER).map(String::as_str),
        Some("explicit")
    );
    assert!(!stored[2].headers.contains_key(headers::PRODUCER));
    // With idempotent publishing off as well, nothing is added at all.
    let bare = Client::connect(
        ClientOptions::new(broker.addr(), "bare")
            .stamp_producer(false)
            .idempotent(false),
    )
    .await
    .unwrap();
    bare.producer("ch").send(message("d")).await.unwrap();
    let stored = broker.messages("ch");
    assert!(stored[3].headers.is_empty(), "{:?}", stored[3].headers);
}

/// Adaptive linger: with nothing in flight a message goes out at once, however long the
/// linger; messages arriving while a batch is in flight are coalesced.
#[tokio::test]
async fn batching_producer_lingers_only_while_in_flight() {
    let (broker, client) = setup(Behavior {
        random_delay_ms: 40,
        ..Default::default()
    })
    .await;
    let bp = client.batching_producer(
        "ch",
        BatchingOptions {
            linger: Duration::from_secs(30),
            ..Default::default()
        },
    );
    // Idle producer: no linger.
    let t = std::time::Instant::now();
    bp.publish(message("solo")).await.unwrap();
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
    assert_eq!(broker.rec(|r| r.publish_frames), 1);
    // One in flight, then a burst: the burst lingers until the first batch completes and
    // goes out as one batch (not after the 30 s linger).
    let t = std::time::Instant::now();
    let first = bp.send(message("first")).await.unwrap();
    let mut rest = Vec::new();
    for i in 0..200 {
        rest.push(bp.send(message(format!("{i}"))).await.unwrap());
    }
    first.await.unwrap();
    for f in rest {
        f.await.unwrap();
    }
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
    let frames = broker.rec(|r| r.publish_frames);
    assert!(
        frames <= 1 + 3,
        "burst should coalesce, got {frames} frames"
    );
    assert_eq!(broker.messages("ch").len(), 202);
    bp.close().await;
}

/// A broker older than the SDK (v0.4 speaks protocol 2 only) refuses `Hello` v4 naming its
/// version: the SDK reconnects with v2 directly and remembers it for later reconnects.
#[tokio::test]
async fn falls_back_to_protocol_v2_for_old_brokers() {
    let broker = FakeBroker::start(Behavior {
        max_protocol: Some(2),
        ..Default::default()
    })
    .await;
    let client = Client::connect(ClientOptions::new(broker.addr(), "new-sdk"))
        .await
        .unwrap();
    assert_eq!(broker.rec(|r| r.hello_versions.clone()), vec![4, 2]);
    client
        .create_channel(ChannelConfig::new("ch", ChannelMode::Stream))
        .await
        .unwrap();
    client.producer("ch").send(message("x")).await.unwrap();
    let mut consumer = client.subscribe("ch", "g", opts(true, 10)).await.unwrap();
    let d = consumer.next().await.unwrap().unwrap();
    assert_eq!(&d.payload()[..], b"x");
    d.ack().await.unwrap();
    // Reconnect: straight to v2, no refused Hello.
    broker.kill_connections();
    common::eventually(Duration::from_secs(10), || {
        (broker.rec(|r| r.hello_versions.len()) >= 3).then_some(())
    })
    .await;
    client.ping().await.unwrap();
    assert_eq!(broker.rec(|r| r.hello_versions.clone()), vec![4, 2, 2]);
    assert_eq!(broker.rec(|r| r.v4_on_old_protocol), 0);
}

#[tokio::test]
async fn current_brokers_negotiate_protocol_v4() {
    let broker = FakeBroker::start(Behavior::default()).await;
    let _client = Client::connect(ClientOptions::new(broker.addr(), "c"))
        .await
        .unwrap();
    assert_eq!(broker.rec(|r| r.hello_versions.clone()), vec![4]);
}

/// Regression (bench v0.6): per-message completions used tokio's oneshot, which spends the
/// awaiting task's coop budget (128 polls per scheduling). A caller that keeps thousands of sends
/// outstanding in one `FuturesUnordered` (the bench harness, window 20000) then got `Pending` for
/// completed sends once the budget ran out, and in Docker 1 KiB throughput fell from ~215k msg/s
/// (window 2000) to ~50k (window 20000). Completed sends must be ready regardless of the budget:
/// poll 1000 completed futures within one task poll, without yielding.
#[tokio::test]
async fn completed_sends_are_ready_without_coop_budget() {
    let (broker, client) = setup(Behavior::default()).await;
    let bp = client.batching_producer("ch", BatchingOptions::default());
    let mut futs = Vec::new();
    for i in 0..1000 {
        futs.push(bp.send(message(format!("{i}"))).await.unwrap());
    }
    bp.publish(message("last")).await.unwrap();
    while broker.messages("ch").len() < 1001 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    // One synchronous pass: no await in between, so no fresh budget.
    let ready = futs
        .into_iter()
        .filter_map(futures::FutureExt::now_or_never)
        .filter(|r| r.is_ok())
        .count();
    assert_eq!(
        ready, 1000,
        "completed sends reported pending (coop budget)"
    );
    bp.close().await;
}
