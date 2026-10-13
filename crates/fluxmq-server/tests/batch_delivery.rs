//! v0.5 batched delivery over real TCP: `DeliverBatch` for v3 connections, `Deliver` for v2,
//! prefetch / order / redelivery semantics unchanged, and the SDK's ack coalescing.

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use bytes::Bytes;
use common::{RawConn, TestServer, WAIT, eventually};
use fluxmq_client::{Client, ClientOptions, SubscribeOptions};
use fluxmq_proto::model::{ChannelConfig, ChannelMode, PublishMessage, RetryPolicy, StartFrom};
use fluxmq_proto::wire::{
    ClientRequest, Delivery, MAX_DELIVER_BATCH, MAX_DELIVER_BATCH_BYTES, ServerFrame,
    ServerResponse,
};

fn channel(name: &str, mode: ChannelMode, partitions: u32) -> ChannelConfig {
    let mut c = ChannelConfig::new(name, mode);
    c.partitions = partitions;
    c
}

fn msgs(n: usize, size: usize) -> Vec<PublishMessage> {
    (0..n)
        .map(|i| {
            let mut p = format!("m{i:06}").into_bytes();
            p.resize(size.max(p.len()), b'.');
            PublishMessage {
                key: None,
                headers: Default::default(),
                payload: Bytes::from(p),
            }
        })
        .collect()
}

async fn raw(srv: &TestServer, version: u16) -> RawConn {
    let mut c = RawConn::connect(srv.client_addr).await;
    let r = c.hello(version, None).await;
    assert!(matches!(r, Ok(ServerResponse::HelloOk { .. })), "{r:?}");
    c
}

async fn publish(c: &mut RawConn, ch: &str, messages: Vec<PublishMessage>) {
    for chunk in messages.chunks(500) {
        c.request(ClientRequest::Publish {
            channel: ch.into(),
            partition: None,
            messages: chunk.to_vec(),
        })
        .await
        .expect("publish");
    }
}

async fn subscribe(c: &mut RawConn, ch: &str, group: &str, prefetch: u32) -> u64 {
    match c
        .request(ClientRequest::Subscribe {
            channel: ch.into(),
            group: group.into(),
            prefetch,
            start: StartFrom::Earliest,
        })
        .await
    {
        Ok(ServerResponse::Subscribed { subscription_id }) => subscription_id,
        other => panic!("subscribe: {other:?}"),
    }
}

/// Next delivery frame as (is_batch, deliveries). Deliveries buffered by `RawConn::request`
/// while it waited for a response come first (their framing is not recorded: `is_batch` is
/// false for them, so frame-kind assertions must not interleave with `request`).
async fn delivery_frame(c: &mut RawConn) -> (bool, Vec<Delivery>) {
    if !c.deliveries.is_empty() {
        return (false, c.deliveries.drain(..).map(|(_, d)| d).collect());
    }
    loop {
        match c.recv().await.expect("connection open") {
            ServerFrame::Deliver { delivery, .. } => return (false, vec![delivery]),
            ServerFrame::DeliverBatch { deliveries, .. } => {
                assert!(!deliveries.is_empty(), "DeliverBatch is never empty");
                return (true, deliveries);
            }
            ServerFrame::MetadataChanged { .. } => {}
            other => panic!("expected a delivery frame, got {other:?}"),
        }
    }
}

/// Reads frames until nothing arrives for `quiet`.
async fn drain_quiet(c: &mut RawConn, quiet: Duration) -> Vec<(bool, Vec<Delivery>)> {
    let mut frames = Vec::new();
    while let Ok(f) = tokio::time::timeout(quiet, delivery_frame(c)).await {
        frames.push(f);
    }
    frames
}

fn ack(sub: u64, ds: &[Delivery]) -> ClientRequest {
    ClientRequest::Ack {
        subscription_id: sub,
        delivery_ids: ds.iter().map(|d| d.delivery_id).collect(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn v3_backlog_arrives_in_order_as_bounded_batches() {
    let srv = TestServer::start().await;
    let mut c = raw(&srv, 3).await;
    c.request(ClientRequest::CreateChannel(channel(
        "b",
        ChannelMode::Stream,
        1,
    )))
    .await
    .unwrap();
    publish(&mut c, "b", msgs(1000, 16)).await;
    let sub = subscribe(&mut c, "b", "g", 5000).await;
    let mut next_offset = 0u64;
    let mut frames = 0;
    let mut batches = 0;
    while next_offset < 1000 {
        let (is_batch, ds) = delivery_frame(&mut c).await;
        frames += 1;
        batches += usize::from(is_batch);
        assert!(ds.len() <= MAX_DELIVER_BATCH);
        for d in &ds {
            assert_eq!(
                d.message.offset, next_offset,
                "in order within the partition"
            );
            assert_eq!(d.attempt, 1);
            next_offset += 1;
        }
        c.send(0, ack(sub, &ds)).await;
    }
    assert!(batches > 0, "a ready backlog is coalesced");
    assert!(frames < 1000 / 2, "{frames} frames for 1000 deliveries");
    assert!(
        drain_quiet(&mut c, Duration::from_millis(200))
            .await
            .is_empty()
    );
    eventually(|| async { (srv.group_lag("b", "g") == Some(0)).then_some(()) }).await;
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn v3_batches_respect_prefetch() {
    let srv = TestServer::start().await;
    let mut c = raw(&srv, 3).await;
    c.request(ClientRequest::CreateChannel(channel(
        "p",
        ChannelMode::Queue,
        2,
    )))
    .await
    .unwrap();
    publish(&mut c, "p", msgs(50, 8)).await;
    let sub = subscribe(&mut c, "p", "g", 7).await;
    let got: Vec<Delivery> = drain_quiet(&mut c, Duration::from_millis(300))
        .await
        .into_iter()
        .flat_map(|(_, ds)| ds)
        .collect();
    assert_eq!(got.len(), 7, "never more than prefetch outstanding");
    // Free 4 slots → exactly 4 more.
    c.request(ack(sub, &got[..4])).await.unwrap();
    let more: Vec<Delivery> = drain_quiet(&mut c, Duration::from_millis(300))
        .await
        .into_iter()
        .flat_map(|(_, ds)| ds)
        .collect();
    assert_eq!(more.len(), 4);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn v3_batches_respect_the_byte_bound() {
    let srv = TestServer::start().await;
    let mut c = raw(&srv, 3).await;
    c.request(ClientRequest::CreateChannel(channel(
        "big",
        ChannelMode::Stream,
        1,
    )))
    .await
    .unwrap();
    // 12 × 300 KB: at most 3 fit in one 1 MiB batch.
    publish(&mut c, "big", msgs(12, 300 * 1024)).await;
    let sub = subscribe(&mut c, "big", "g", 100).await;
    let mut n = 0;
    while n < 12 {
        let (_, ds) = delivery_frame(&mut c).await;
        let bytes: usize = ds.iter().map(|d| d.message.payload.len()).sum();
        assert!(
            ds.len() == 1 || bytes <= MAX_DELIVER_BATCH_BYTES,
            "{bytes} bytes"
        );
        assert!(ds.len() <= 3);
        n += ds.len();
        c.send(0, ack(sub, &ds)).await;
    }
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn v2_client_still_gets_single_deliver_frames() {
    let srv = TestServer::start().await;
    let mut c = raw(&srv, 2).await;
    c.request(ClientRequest::CreateChannel(channel(
        "v2",
        ChannelMode::Stream,
        1,
    )))
    .await
    .unwrap();
    publish(&mut c, "v2", msgs(300, 16)).await;
    let sub = subscribe(&mut c, "v2", "g", 1000).await;
    for i in 0..300u64 {
        let (is_batch, ds) = delivery_frame(&mut c).await;
        assert!(!is_batch, "v2 never sees DeliverBatch");
        assert_eq!(ds[0].message.offset, i);
        c.send(0, ack(sub, &ds)).await;
    }
    eventually(|| async { (srv.group_lag("v2", "g") == Some(0)).then_some(()) }).await;
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn v2_and_v3_consumers_share_one_group() {
    let srv = TestServer::start().await;
    let mut admin = raw(&srv, 3).await;
    admin
        .request(ClientRequest::CreateChannel(channel(
            "mix",
            ChannelMode::Queue,
            4,
        )))
        .await
        .unwrap();
    let mut v2 = raw(&srv, 2).await;
    let mut v3 = raw(&srv, 3).await;
    let s2 = subscribe(&mut v2, "mix", "g", 16).await;
    let s3 = subscribe(&mut v3, "mix", "g", 16).await;
    publish(&mut admin, "mix", msgs(2000, 16)).await;
    let consume = |mut c: RawConn, sub: u64, v2: bool| {
        tokio::spawn(async move {
            let mut seen = Vec::new();
            while let Ok((is_batch, ds)) =
                tokio::time::timeout(Duration::from_millis(500), delivery_frame(&mut c)).await
            {
                assert!(!(v2 && is_batch), "v2 connection got a DeliverBatch");
                seen.extend(ds.iter().map(|d| (d.message.partition, d.message.offset)));
                c.send(0, ack(sub, &ds)).await;
            }
            seen
        })
    };
    let (a, b) = (consume(v2, s2, true), consume(v3, s3, false));
    let (a, b) = (a.await.unwrap(), b.await.unwrap());
    assert!(!a.is_empty() && !b.is_empty(), "both consumers took part");
    let all: BTreeSet<_> = a.iter().chain(&b).copied().collect();
    assert_eq!(
        all.len(),
        2000,
        "every message exactly once across the group"
    );
    assert_eq!(a.len() + b.len(), 2000);
    assert_eq!(srv.group_lag("mix", "g"), Some(0));
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn v3_nack_redelivery_and_dead_letter_unchanged() {
    let srv = TestServer::start().await;
    let mut c = raw(&srv, 3).await;
    let mut cfg = channel("n", ChannelMode::Queue, 1);
    cfg.retry = RetryPolicy {
        max_attempts: 2,
        backoff_ms: vec![20],
        ack_timeout_ms: 30_000,
    };
    c.request(ClientRequest::CreateChannel(cfg)).await.unwrap();
    publish(&mut c, "n", msgs(5, 8)).await;
    let sub = subscribe(&mut c, "n", "g", 100).await;
    let mut first = Vec::new();
    while first.len() < 5 {
        first.extend(delivery_frame(&mut c).await.1);
    }
    c.request(ack(sub, &first[1..])).await.unwrap();
    let nack = |id: u64| ClientRequest::Nack {
        subscription_id: sub,
        delivery_id: id,
        reason: "boom".into(),
        retry: true,
    };
    c.request(nack(first[0].delivery_id)).await.unwrap();
    let (_, again) = delivery_frame(&mut c).await;
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].attempt, 2);
    assert_eq!(again[0].message.offset, first[0].message.offset);
    c.request(nack(again[0].delivery_id)).await.unwrap();
    assert!(
        drain_quiet(&mut c, Duration::from_millis(200))
            .await
            .is_empty()
    );
    let dlq = srv.broker.dlq_summary();
    assert_eq!(dlq.total, 1);
    srv.stop().await;
}

// ---- SDK: ack coalescing --------------------------------------------------------------

async fn sdk_client(srv: &TestServer, name: &str) -> Client {
    Client::connect(ClientOptions::new(srv.client_addr.to_string(), name))
        .await
        .expect("connect")
}

fn acked(srv: &TestServer, ch: &str, group: &str) -> u64 {
    srv.broker
        .stats()
        .channels
        .into_iter()
        .find(|c| c.config.name == ch)
        .and_then(|c| c.groups.into_iter().find(|g| g.name == group))
        .map_or(0, |g| g.acked_total)
}

/// Lingering acks (10 s) never starve the flow: the size bound (capped at half the prefetch)
/// returns credit to the broker.
#[tokio::test(flavor = "multi_thread")]
async fn sdk_buffered_acks_do_not_starve_prefetch() {
    let srv = TestServer::start().await;
    let client = sdk_client(&srv, "starve").await;
    client
        .ensure_channel(channel("s", ChannelMode::Queue, 2))
        .await
        .unwrap();
    client.publish_batch("s", msgs(500, 16)).await.unwrap();
    let mut consumer = client
        .subscribe(
            "s",
            "g",
            SubscribeOptions::default()
                .prefetch(8)
                .start(StartFrom::Earliest)
                .ack_batch(1000)
                .ack_linger(Duration::from_secs(10)),
        )
        .await
        .unwrap();
    let t = std::time::Instant::now();
    for _ in 0..500 {
        let d = tokio::time::timeout(WAIT, consumer.next())
            .await
            .expect("no starvation")
            .unwrap()
            .unwrap();
        d.ack().await.unwrap();
    }
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sdk_acks_flushed_on_linger_unsubscribe_drop_and_close() {
    let srv = TestServer::start().await;
    let client = sdk_client(&srv, "flush").await;
    client
        .ensure_channel(channel("f", ChannelMode::Queue, 1))
        .await
        .unwrap();
    client.publish_batch("f", msgs(10, 8)).await.unwrap();
    let opts = |linger_ms: u64| {
        SubscribeOptions::default()
            .prefetch(100)
            .start(StartFrom::Earliest)
            .ack_batch(1000)
            .ack_linger(Duration::from_millis(linger_ms))
    };
    async fn take(c: &mut fluxmq_client::Consumer, n: usize) {
        for _ in 0..n {
            let d = tokio::time::timeout(WAIT, c.next())
                .await
                .expect("delivery")
                .unwrap()
                .unwrap();
            d.ack().await.unwrap();
        }
    }

    // Linger timer: 50 ms.
    let mut c1 = client.subscribe("f", "linger", opts(50)).await.unwrap();
    take(&mut c1, 10).await;
    assert!(acked(&srv, "f", "linger") < 10, "acks are buffered");
    eventually(|| async { (acked(&srv, "f", "linger") == 10).then_some(()) }).await;

    // Unsubscribe flushes before leaving (otherwise the deliveries would be redelivered).
    let mut c2 = client.subscribe("f", "unsub", opts(60_000)).await.unwrap();
    take(&mut c2, 10).await;
    c2.unsubscribe().await.unwrap();
    eventually(|| async { (acked(&srv, "f", "unsub") == 10).then_some(()) }).await;

    // Drop flushes before the best-effort Unsubscribe.
    let mut c3 = client.subscribe("f", "drop", opts(60_000)).await.unwrap();
    take(&mut c3, 10).await;
    drop(c3);
    eventually(|| async { (acked(&srv, "f", "drop") == 10).then_some(()) }).await;

    // Closing the client flushes buffered acks with the connection's final write.
    let other = sdk_client(&srv, "flush-close").await;
    let mut c4 = other.subscribe("f", "close", opts(60_000)).await.unwrap();
    take(&mut c4, 10).await;
    other.close();
    eventually(|| async { (acked(&srv, "f", "close") == 10).then_some(()) }).await;
    for g in ["linger", "unsub", "drop", "close"] {
        assert_eq!(srv.group_lag("f", g), Some(0), "group {g}");
    }
    srv.stop().await;
}
