//! Protocol v4 (v0.7) SDK paths against the fake broker: version fallback matrix, `PublishBatch`
//! with compression, raw `DeliverRecords` decoding, `AckRanges` coalescing, cumulative commits
//! and batches near the frame limit.

mod common;

use std::collections::{BTreeSet, HashSet};
use std::time::Duration;

use bytes::Bytes;
use common::{Behavior, FakeBroker, FakeCluster, eventually};
use fluxmq_client::{
    AckMode, BatchingOptions, Client, ClientError, ClientOptions, Compression, Delivery,
    ReconnectPolicy, StartFrom, SubscribeOptions, message,
};
use fluxmq_proto::batch::MAX_BATCH_LEN;
use fluxmq_proto::{ChannelConfig, ChannelMode, headers};

fn hdr<'a>(m: &'a fluxmq_proto::Message, name: &str) -> Option<&'a str> {
    m.headers.get(name).map(String::as_str)
}

const WAIT: Duration = Duration::from_secs(10);

fn opts(addr: impl Into<String>) -> ClientOptions {
    ClientOptions::new(addr, "v4-client").reconnect(
        ReconnectPolicy::default().backoff(Duration::from_millis(10), Duration::from_millis(50)),
    )
}

fn earliest(prefetch: u32) -> SubscribeOptions {
    SubscribeOptions::default()
        .prefetch(prefetch)
        .start(StartFrom::Earliest)
}

/// A one-node "cluster" (serves metadata, like every real broker since v0.2) with `partitions`
/// partitions of channel `ch`.
async fn node(behavior: Behavior, partitions: usize) -> (FakeCluster, Client) {
    let c = FakeCluster::start_with(1, behavior).await;
    c.create_channel("ch", &vec!["n1"; partitions]);
    let client = Client::connect(opts(c.nodes[0].addr())).await.unwrap();
    (c, client)
}

async fn take(consumer: &mut fluxmq_client::Consumer, n: usize) -> Vec<Delivery> {
    let mut out = Vec::new();
    while out.len() < n {
        let d = tokio::time::timeout(WAIT, consumer.next())
            .await
            .expect("delivery in time")
            .expect("stream open")
            .expect("no error");
        out.push(d);
    }
    out
}

fn compressible(i: usize, size: usize) -> Bytes {
    let s = format!("{{\"order\":{i},\"status\":\"created\",\"pad\":\"");
    let mut v = s.into_bytes();
    v.resize(size.max(v.len()), b'x');
    Bytes::from(v)
}

/// Every combination of broker protocol (2, 3, 4) and refusal style: the SDK lands on the
/// broker's version, all operations work, no v4 frame reaches an older broker, and reconnects
/// reuse the negotiated version.
#[tokio::test]
async fn fallback_matrix() {
    for (max, terse, expect) in [
        (4u16, false, vec![4u16]),
        (3, false, vec![4, 3]),
        (2, false, vec![4, 2]),
        (3, true, vec![4, 3]),
        (2, true, vec![4, 3, 2]),
    ] {
        let behavior = Behavior {
            max_protocol: Some(max),
            terse_refusal: terse,
            ..Default::default()
        };
        let (c, client) = node(behavior, 2).await;
        let b = &c.nodes[0];
        assert_eq!(
            b.rec(|r| r.hello_versions.clone()),
            expect,
            "max {max} terse {terse}"
        );
        // Publish (plain, batching, keyed) and consume with acks.
        client
            .publish_batch(
                "ch",
                (0..10).map(|i| message(format!("a{i}")).build()).collect(),
            )
            .await
            .unwrap();
        let bp = client.batching_producer("ch", BatchingOptions::default());
        let mut futs = Vec::new();
        for i in 0..20 {
            futs.push(
                bp.send(message(format!("b{i}")).key(format!("k{i}")))
                    .await
                    .unwrap(),
            );
        }
        for f in futs {
            f.await.unwrap();
        }
        bp.close().await;
        let mut consumer = client.subscribe("ch", "g", earliest(64)).await.unwrap();
        for d in take(&mut consumer, 30).await {
            d.ack().await.unwrap();
        }
        consumer.unsubscribe().await.unwrap();
        let (v4_on_old, batches, acked) = b.rec(|r| {
            (
                r.v4_on_old_protocol,
                r.publish_batches.len(),
                r.acked_ids().len(),
            )
        });
        assert_eq!(v4_on_old, 0);
        assert_eq!(acked, 30);
        assert_eq!(batches > 0, max >= 4, "PublishBatch only on v4 (max {max})");
        let subs = b.rec(|r| r.subscribe_options.clone());
        assert_eq!(subs.iter().all(Option::is_some), max >= 4);
        // Reconnect: straight to the negotiated version.
        let before = b.rec(|r| r.hello_versions.len());
        b.kill_connections();
        eventually(WAIT, || {
            (b.rec(|r| r.hello_versions.len()) > before).then_some(())
        })
        .await;
        client.ping().await.unwrap();
        let hellos = b.rec(|r| r.hello_versions.clone());
        assert_eq!(hellos[before..], [max], "cached version on reconnect");
    }
}

/// `PublishBatch` with every compression: the broker decodes exactly what was sent, ids map
/// from `base_offset + index`, and the producer stamp lands in the records.
#[tokio::test]
async fn publish_batch_compression_roundtrip() {
    for c in [Compression::None, Compression::Lz4, Compression::Zstd] {
        let cl = FakeCluster::start(1).await;
        cl.create_channel("ch", &["n1"]);
        let client = Client::connect(opts(cl.nodes[0].addr()).compression(c))
            .await
            .unwrap();
        let msgs: Vec<_> = (0..200)
            .map(|i| {
                message(compressible(i, 300))
                    .event_type("OrderCreated")
                    .key(format!("k{}", i % 7))
                    .build()
            })
            .collect();
        let ids = client
            .producer("ch")
            .send_batch(msgs.clone())
            .await
            .unwrap();
        let bp = client.batching_producer("ch", BatchingOptions::default());
        let mut futs = Vec::new();
        for i in 200..400 {
            futs.push(bp.send(message(compressible(i, 300))).await.unwrap());
        }
        let mut ids2 = Vec::new();
        for f in futs {
            ids2.push(f.await.unwrap());
        }
        bp.close().await;
        let stored = cl.nodes[0].messages("ch");
        assert_eq!(stored.len(), 400);
        for (id, m) in ids.iter().chain(&ids2).zip(0..) {
            let s = &stored[id.offset as usize];
            assert_eq!(s.payload, compressible(m, 300), "id {id:?} ({c:?})");
            assert_eq!(hdr(s, headers::PRODUCER), Some("v4-client"));
        }
        for (m, s) in msgs.iter().zip(&stored) {
            assert_eq!(s.key, m.key);
            assert_eq!(hdr(s, headers::EVENT_TYPE), Some("OrderCreated"));
        }
        let batches = cl.nodes[0].rec(|r| r.publish_batches.clone());
        assert!(!batches.is_empty());
        assert!(
            batches.iter().all(|(_, p, _, _)| p.is_some()),
            "partition always set"
        );
        assert!(
            batches.iter().all(|(_, _, _, bc)| *bc == c),
            "{c:?}: {batches:?}"
        );
    }
}

/// Keyless messages of the batching producer stick to one partition per batch and rotate;
/// keyed ones follow the key hash.
#[tokio::test]
async fn batching_producer_sticky_and_keyed_partitions() {
    let (c, client) = node(Behavior::default(), 4).await;
    let bp = client.batching_producer("ch", BatchingOptions::default());
    let mut ids = Vec::new();
    for round in 0..8 {
        let mut futs = Vec::new();
        for i in 0..50 {
            futs.push(bp.send(message(format!("{round}-{i}"))).await.unwrap());
        }
        for f in futs {
            ids.push(f.await.unwrap());
        }
    }
    let parts: HashSet<u32> = ids.iter().map(|i| i.partition).collect();
    assert!(
        parts.len() > 1,
        "keyless messages rotate partitions: {parts:?}"
    );
    let batches = c.nodes[0].rec(|r| r.publish_batches.clone());
    // Sticky: far fewer batches than messages.
    assert!(batches.len() < 400 / 4, "{} batches", batches.len());
    let mut futs = Vec::new();
    for i in 0..40 {
        let k = format!("key-{i}");
        futs.push((k.clone(), bp.send(message("x").key(k)).await.unwrap()));
    }
    for (k, f) in futs {
        let id = f.await.unwrap();
        assert_eq!(id.partition, fluxmq_proto::batch::key_partition(&k, 4));
    }
    bp.close().await;
}

/// Raw delivery: envelopes cover more records than delivered (partial ranges), compressed or
/// not; the consumer yields exactly the delivered range with the right ids, and acking in
/// order sends `AckRanges`.
#[tokio::test]
async fn raw_delivery_partial_ranges_and_ack_ranges() {
    for c in [Compression::None, Compression::Lz4, Compression::Zstd] {
        let (cl, client) = node(
            Behavior {
                envelope_align: 16,
                deliver_compression: c,
                ..Default::default()
            },
            1,
        )
        .await;
        let b = &cl.nodes[0];
        let n = 300;
        client
            .publish_batch(
                "ch",
                (0..n)
                    .map(|i| {
                        message(compressible(i, 100))
                            .key(format!("k{i}"))
                            .header("i", i.to_string())
                            .build()
                    })
                    .collect(),
            )
            .await
            .unwrap();
        // Prefetch 37: frames end mid-envelope, so envelopes start before / end after the range.
        let mut consumer = client
            .subscribe("ch", "g", earliest(37).ack_batch(18))
            .await
            .unwrap();
        let mut seen = BTreeSet::new();
        for j in 0..n {
            let d = take(&mut consumer, 1).await.pop().unwrap();
            assert_eq!(d.id().offset, j as u64, "{c:?}");
            assert_eq!(d.attempt(), 1);
            assert_eq!(d.payload(), &compressible(j, 100));
            assert_eq!(d.key(), Some(format!("k{j}").as_str()));
            assert_eq!(d.header("i"), Some(j.to_string().as_str()));
            assert!(seen.insert(d.delivery_id()));
            d.ack().await.unwrap();
        }
        eventually(WAIT, || (b.rec(|r| r.acked_ids().len()) == n).then_some(())).await;
        let (raw, ranges, acked) =
            b.rec(|r| (r.raw_frames, r.ack_range_frames.clone(), r.acked_ids()));
        assert!(raw >= n / 37, "raw frames used ({raw})");
        assert!(!ranges.is_empty(), "in-order acks coalesce into AckRanges");
        assert!(
            ranges.iter().all(|f| f.len() <= 2),
            "contiguous: {ranges:?}"
        );
        assert_eq!(acked, seen.into_iter().collect::<Vec<_>>());
        // Opting out of raw records gets per-message deliveries with the same API.
        let mut plain = client
            .subscribe("ch", "g2", earliest(64).raw_records(false))
            .await
            .unwrap();
        let before = b.rec(|r| r.raw_frames);
        let ds = take(&mut plain, 10).await;
        assert_eq!(ds[3].payload(), &compressible(3, 100));
        assert_eq!(b.rec(|r| r.raw_frames), before);
    }
}

/// A corrupt batch (CRC mismatch) drops the connection; after the reconnect the broker
/// redelivers and the consumer gets every message. Without verification it is not noticed.
#[tokio::test]
async fn corrupt_batch_is_detected_and_redelivered() {
    let (cl, client) = node(
        Behavior {
            corrupt_first_crc: true,
            ..Default::default()
        },
        1,
    )
    .await;
    let b = &cl.nodes[0];
    client
        .publish_batch(
            "ch",
            (0..20).map(|i| message(format!("m{i}")).build()).collect(),
        )
        .await
        .unwrap();
    let mut consumer = client.subscribe("ch", "g", earliest(64)).await.unwrap();
    let ds = take(&mut consumer, 20).await;
    let got: BTreeSet<String> = ds
        .iter()
        .map(|d| String::from_utf8(d.payload().to_vec()).unwrap())
        .collect();
    assert_eq!(got.len(), 20);
    assert!(
        ds.iter().all(|d| d.attempt() == 2),
        "redelivered after the reconnect"
    );
    assert!(b.rec(|r| r.hellos.len()) >= 2, "reconnected");

    let (cl, client) = node(
        Behavior {
            corrupt_first_crc: true,
            ..Default::default()
        },
        1,
    )
    .await;
    client
        .publish_batch(
            "ch",
            (0..20).map(|i| message(format!("m{i}")).build()).collect(),
        )
        .await
        .unwrap();
    let mut consumer = client
        .subscribe("ch", "g", earliest(64).verify_crc(false))
        .await
        .unwrap();
    let ds = take(&mut consumer, 20).await;
    assert!(ds.iter().all(|d| d.attempt() == 1));
    assert_eq!(cl.nodes[0].rec(|r| r.hellos.len()), 1);
}

/// Cumulative mode: `ack` commits position + 1 per partition (coalesced), `commit()` commits
/// everything handed out and waits for the broker; nack is refused.
#[tokio::test]
async fn cumulative_commits() {
    let (cl, client) = node(Behavior::default(), 2).await;
    let b = &cl.nodes[0];
    client
        .publish_batch(
            "ch",
            (0..40)
                .map(|i| message(format!("m{i}")).key(format!("k{i}")).build())
                .collect(),
        )
        .await
        .unwrap();
    let mut consumer = client
        .subscribe(
            "ch",
            "g",
            earliest(100)
                .cumulative()
                .ack_batch(10)
                .ack_linger(Duration::from_secs(30)),
        )
        .await
        .unwrap();
    assert_eq!(
        b.rec(|r| r.subscribe_options[0].map(|o| o.ack_mode)),
        Some(AckMode::Cumulative)
    );
    let ds = take(&mut consumer, 40).await;
    let mut last: std::collections::HashMap<u32, u64> = Default::default();
    let mut iter = ds.into_iter();
    // Ack the first 25 in order (2 full ack batches of 10 → Commit frames).
    for d in iter.by_ref().take(25) {
        last.insert(d.id().partition, d.id().offset);
        d.ack().await.unwrap();
    }
    eventually(WAIT, || (!b.rec(|r| r.commits.is_empty())).then_some(())).await;
    let commits = b.rec(|r| r.commits.clone());
    assert!(
        commits.iter().all(|c| c.3 == 0),
        "auto-commits are fire-and-forget"
    );
    // The rest: one nack (refused) and no acks, then commit() covers everything handed out.
    let rest: Vec<Delivery> = iter.collect();
    let max_handed: std::collections::HashMap<u32, u64> =
        rest.iter().fold(last.clone(), |mut m, d| {
            let e = m.entry(d.id().partition).or_insert(0);
            *e = (*e).max(d.id().offset);
            m
        });
    let mut rest = rest.into_iter();
    let first = rest.next().unwrap();
    let err = first.nack("nope", true).await.unwrap_err();
    assert!(matches!(err, ClientError::Server(_)), "{err:?}");
    consumer.commit().await.unwrap();
    let commits = b.rec(|r| r.commits.clone());
    let confirmed: Vec<_> = commits.iter().filter(|c| c.3 != 0).collect();
    assert!(!confirmed.is_empty(), "commit() waits for the broker");
    for (p, o) in max_handed {
        let best = commits
            .iter()
            .filter(|c| c.1 == p)
            .map(|c| c.2)
            .max()
            .unwrap();
        assert_eq!(best, o + 1, "partition {p}");
    }
    assert_eq!(b.rec(|r| r.ack_frames.len()), 0, "no individual acks");
}

/// Cumulative mode against a v3 broker is emulated with individual acks: acking offset `o`
/// acks every earlier delivery of the partition.
#[tokio::test]
async fn cumulative_emulated_on_v3() {
    let (cl, client) = node(
        Behavior {
            max_protocol: Some(3),
            ..Default::default()
        },
        1,
    )
    .await;
    let b = &cl.nodes[0];
    client
        .publish_batch(
            "ch",
            (0..10).map(|i| message(format!("m{i}")).build()).collect(),
        )
        .await
        .unwrap();
    let mut consumer = client
        .subscribe("ch", "g", earliest(64).cumulative())
        .await
        .unwrap();
    let mut ds = take(&mut consumer, 10).await;
    let sixth = ds.remove(5);
    let ids: Vec<u64> = ds[..5].iter().map(Delivery::delivery_id).collect();
    sixth.ack().await.unwrap();
    eventually(WAIT, || (b.rec(|r| r.acked_ids().len()) == 6).then_some(())).await;
    let acked = b.rec(|r| r.acked_ids());
    for id in ids {
        assert!(acked.contains(&id));
    }
    consumer.commit().await.unwrap();
    eventually(WAIT, || {
        (b.rec(|r| r.acked_ids().len()) == 10).then_some(())
    })
    .await;
    assert_eq!(b.rec(|r| r.v4_on_old_protocol), 0);
    assert!(b.rec(|r| r.commits.is_empty()));
}

/// Batches close to the frame limit go through on both paths; one byte too many is refused
/// locally with a clear error.
#[tokio::test]
async fn large_batches_near_the_frame_limit() {
    let (cl, _) = node(Behavior::default(), 1).await;
    let client = Client::connect(opts(cl.nodes[0].addr()).stamp_producer(false))
        .await
        .unwrap();
    // 4 messages filling the batch limit minus record overhead.
    let each = (MAX_BATCH_LEN - 32) / 4 - 16;
    let msgs: Vec<_> = (0..4u8)
        .map(|i| message(Bytes::from(vec![i; each])).build())
        .collect();
    let ids = client.publish_batch("ch", msgs).await.unwrap();
    assert_eq!(ids.len(), 4);
    let too_big: Vec<_> = (0..4u8)
        .map(|i| message(Bytes::from(vec![i; each + 64])).build())
        .collect();
    let err = client.publish_batch("ch", too_big).await.unwrap_err();
    assert!(matches!(err, ClientError::Protocol(_)), "{err:?}");
    // Raw delivery of the big batch (one ~4 MiB record per frame within prefetch 2).
    let mut consumer = client.subscribe("ch", "g", earliest(2)).await.unwrap();
    for i in 0..4 {
        let d = take(&mut consumer, 1).await.pop().unwrap();
        assert_eq!(d.payload().len(), each);
        assert!(d.payload().iter().all(|b| *b == i as u8));
        d.ack().await.unwrap();
    }
    // v3 broker: the same batch as `Publish`.
    let (cl3, _) = node(
        Behavior {
            max_protocol: Some(3),
            ..Default::default()
        },
        1,
    )
    .await;
    let c3 = Client::connect(opts(cl3.nodes[0].addr()).stamp_producer(false))
        .await
        .unwrap();
    let msgs: Vec<_> = (0..4u8)
        .map(|i| message(Bytes::from(vec![i; each - 64])).build())
        .collect();
    assert_eq!(c3.publish_batch("ch", msgs).await.unwrap().len(), 4);
}

/// A plain single-node broker without metadata: publishes still work on v4 (keyless batches
/// as `PublishBatch` without partition, keyed ones as `Publish` so the broker can split them).
#[tokio::test]
async fn no_metadata_keeps_working() {
    let broker = FakeBroker::start(Behavior::default()).await;
    let client = Client::connect(opts(broker.addr())).await.unwrap();
    client
        .create_channel(ChannelConfig::new("ch", ChannelMode::Stream))
        .await
        .unwrap();
    client
        .publish_batch(
            "ch",
            (0..5).map(|i| message(format!("{i}")).build()).collect(),
        )
        .await
        .unwrap();
    client
        .publish_batch(
            "ch",
            (0..5)
                .map(|i| message(format!("{i}")).key(format!("k{i}")).build())
                .collect(),
        )
        .await
        .unwrap();
    let bp = client.batching_producer("ch", BatchingOptions::default());
    bp.publish(message("x").key("k")).await.unwrap();
    bp.close().await;
    assert_eq!(broker.messages("ch").len(), 11);
    let batches = broker.rec(|r| r.publish_batches.clone());
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].1, None);
}
