//! Delivery semantics per channel mode, flow control, retries, timeouts and DLQ.

mod common;

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use common::*;
use fluxmq_core::{BrokerEvent, CoreError, DlqSelector};
use fluxmq_proto::model::{AttemptOutcome, ChannelMode, RetryPolicy, StartFrom};

#[tokio::test]
async fn stream_publish_consume_ack() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("orders", ChannelMode::Stream, 4))
        .unwrap();
    let ids = publish_keyed(&b, "orders", 100, 7).await;
    assert_eq!(ids.len(), 100);
    // Same key → same partition (crc32 % P).
    let mut by_key: HashMap<String, u32> = HashMap::new();
    for (i, id) in ids.iter().enumerate() {
        let k = format!("k{}", i % 7);
        assert_eq!(*by_key.entry(k).or_insert(id.partition), id.partition);
    }
    let mut sub = b
        .subscribe("orders", "g", info("w", 1), 1000, StartFrom::Earliest)
        .unwrap();
    let mut seen = BTreeSet::new();
    let mut last_per_partition: HashMap<u32, u64> = HashMap::new();
    for _ in 0..100 {
        let d = next(&mut sub).await;
        assert_eq!(d.attempt, 1);
        if let Some(prev) = last_per_partition.insert(d.message.partition, d.message.offset) {
            assert!(d.message.offset > prev, "in order within a partition");
        }
        seen.insert((d.message.partition, d.message.offset));
        sub.ack(&[d.delivery_id]).unwrap();
    }
    assert_eq!(seen.len(), 100);
    assert!(next_within(&mut sub, 100).await.is_none());
    let s = b.stats();
    let cs = &s.channels[0];
    assert_eq!(cs.published_total, 100);
    assert_eq!(cs.acked_total, 100);
    assert_eq!(cs.delivered_total, 100);
    assert_eq!(cs.groups[0].lag, 0);
    assert_eq!(cs.groups[0].in_flight, 0);
    assert_eq!(cs.groups[0].processing_latency_us.len(), 100);
    assert_eq!(cs.delivery_latency_us.len(), 100);
    assert!(matches!(
        sub.ack(&[999_999]),
        Err(CoreError::UnknownDelivery(999_999))
    ));
    b.shutdown().await;
}

#[tokio::test]
async fn latest_start_skips_existing_and_groups_fan_out() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("c", ChannelMode::Stream, 2))
        .unwrap();
    publish_keyed(&b, "c", 10, 3).await;
    let mut latest = b
        .subscribe("c", "latest", info("a", 1), 100, StartFrom::Latest)
        .unwrap();
    let mut earliest = b
        .subscribe("c", "earliest", info("b", 2), 100, StartFrom::Earliest)
        .unwrap();
    assert!(next_within(&mut latest, 100).await.is_none());
    publish_keyed(&b, "c", 5, 3).await;
    for _ in 0..5 {
        let d = next(&mut latest).await;
        latest.ack(&[d.delivery_id]).unwrap();
    }
    for _ in 0..15 {
        let d = next(&mut earliest).await;
        earliest.ack(&[d.delivery_id]).unwrap();
    }
    b.shutdown().await;
}

#[tokio::test]
async fn stream_assignment_and_rebalance() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("s", ChannelMode::Stream, 4))
        .unwrap();
    let mut a = b
        .subscribe("s", "g", info("a", 1), 100, StartFrom::Earliest)
        .unwrap();
    let assigned = |b: &fluxmq_core::Broker| -> HashMap<String, Vec<u32>> {
        b.stats().channels[0].groups[0]
            .instances
            .iter()
            .map(|i| (i.id.clone(), i.assigned_partitions.clone()))
            .collect()
    };
    assert_eq!(assigned(&b)["a#1"], vec![0, 1, 2, 3]);
    let mut bb = b
        .subscribe("s", "g", info("b", 2), 100, StartFrom::Earliest)
        .unwrap();
    let m = assigned(&b);
    assert_eq!(m["a#1"], vec![0, 2]);
    assert_eq!(m["b#2"], vec![1, 3]);
    publish_keyed(&b, "s", 200, 50).await;
    let mut got_a = 0;
    let mut got_b = 0;
    while let Some(d) = next_within(&mut a, 200).await {
        assert!(d.message.partition % 2 == 0, "a only gets its partitions");
        a.ack(&[d.delivery_id]).unwrap();
        got_a += 1;
    }
    while let Some(d) = next_within(&mut bb, 200).await {
        assert!(d.message.partition % 2 == 1, "b only gets its partitions");
        bb.ack(&[d.delivery_id]).unwrap();
        got_b += 1;
    }
    assert_eq!(got_a + got_b, 200);
    assert!(got_a > 0 && got_b > 0);
    drop(bb);
    assert_eq!(assigned(&b)["a#1"], vec![0, 1, 2, 3]);
    b.shutdown().await;
}

#[tokio::test]
async fn queue_competing_consumers() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("q", ChannelMode::Queue, 1))
        .unwrap();
    let mut a = b
        .subscribe("q", "workers", info("a", 1), 5, StartFrom::Earliest)
        .unwrap();
    let mut c = b
        .subscribe("q", "workers", info("c", 2), 5, StartFrom::Earliest)
        .unwrap();
    publish_keyed(&b, "q", 100, 10).await;
    let mut seen = BTreeSet::new();
    let (mut na, mut nc) = (0, 0);
    // Alternate: each takes up to its prefetch, then acks.
    while seen.len() < 100 {
        for (sub, n) in [(&mut a, &mut na), (&mut c, &mut nc)] {
            let mut batch = Vec::new();
            while let Some(d) = next_within(sub, 30).await {
                assert!(seen.insert(d.message.offset), "no duplicate deliveries");
                batch.push(d.delivery_id);
                *n += 1;
                if batch.len() == 5 {
                    break;
                }
            }
            sub.ack(&batch).unwrap();
        }
    }
    assert!(na > 0 && nc > 0, "both consumers got work: {na} / {nc}");
    b.shutdown().await;
}

#[tokio::test]
async fn keyed_strict_per_key_order_under_nack() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("k", ChannelMode::Keyed, 1))
        .unwrap();
    b.publish(
        "k",
        vec![
            msg(Some("A"), "A1"),
            msg(Some("A"), "A2"),
            msg(Some("B"), "B1"),
            msg(Some("A"), "A3"),
        ],
    )
    .await
    .unwrap();
    let mut sub = b
        .subscribe("k", "g", info("w", 1), 10, StartFrom::Earliest)
        .unwrap();
    let a1 = next(&mut sub).await;
    assert_eq!(payload(&a1), "A1");
    let b1 = next(&mut sub).await;
    assert_eq!(payload(&b1), "B1", "other keys proceed while A is blocked");
    assert!(
        next_within(&mut sub, 100).await.is_none(),
        "A2 waits for A1"
    );
    sub.nack(a1.delivery_id, "boom", true).unwrap();
    let a1b = next(&mut sub).await;
    assert_eq!(
        (payload(&a1b).as_str(), a1b.attempt),
        ("A1", 2),
        "A1 retried before A2"
    );
    sub.ack(&[a1b.delivery_id, b1.delivery_id]).unwrap();
    let a2 = next(&mut sub).await;
    assert_eq!(payload(&a2), "A2");
    // Dead-lettering also unblocks the key.
    sub.nack(a2.delivery_id, "fatal", false).unwrap();
    let a3 = next(&mut sub).await;
    assert_eq!(payload(&a3), "A3");
    sub.ack(&[a3.delivery_id]).unwrap();
    let s = b.stats();
    assert_eq!(s.channels[0].groups[0].lag, 0);
    assert_eq!(s.channels[0].dlq_size, 1);
    b.shutdown().await;
}

#[tokio::test]
async fn keyed_across_partitions_with_two_instances() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("k", ChannelMode::Keyed, 4))
        .unwrap();
    let mut s1 = b
        .subscribe("k", "g", info("a", 1), 50, StartFrom::Earliest)
        .unwrap();
    let mut s2 = b
        .subscribe("k", "g", info("b", 2), 50, StartFrom::Earliest)
        .unwrap();
    publish_keyed(&b, "k", 400, 20).await;
    let mut last: HashMap<String, u64> = HashMap::new();
    let mut total = 0;
    loop {
        let mut progressed = false;
        for sub in [&mut s1, &mut s2] {
            let mut batch = Vec::new();
            while let Some(d) = next_within(sub, 20).await {
                let key = d.message.key.clone().unwrap();
                let n: u64 = payload(&d)[1..].parse().unwrap();
                if let Some(prev) = last.insert(key, n) {
                    assert!(n > prev, "per-key order");
                }
                batch.push(d.delivery_id);
            }
            total += batch.len();
            progressed |= !batch.is_empty();
            sub.ack(&batch).unwrap();
        }
        if !progressed {
            break;
        }
    }
    assert_eq!(total, 400);
    b.shutdown().await;
}

#[tokio::test]
async fn prefetch_limits_outstanding() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("p", ChannelMode::Queue, 1))
        .unwrap();
    publish_keyed(&b, "p", 5, 5).await;
    let mut sub = b
        .subscribe("p", "g", info("w", 1), 2, StartFrom::Earliest)
        .unwrap();
    let d1 = next(&mut sub).await;
    let _d2 = next(&mut sub).await;
    assert!(
        next_within(&mut sub, 100).await.is_none(),
        "prefetch=2 reached"
    );
    // Wakes up efficiently once a slot frees.
    let waiter = tokio::spawn(async move {
        let d = next(&mut sub).await;
        (sub, d)
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    let s = b.stats();
    assert_eq!(s.channels[0].groups[0].instances[0].in_flight, 2);
    // Ack via a fresh lookup is not possible (sub moved); the group accepts acks of any
    // member, so subscribe a second member to ack d1.
    let other = b
        .subscribe("p", "g", info("x", 2), 1, StartFrom::Earliest)
        .unwrap();
    other.ack(&[d1.delivery_id]).unwrap();
    let (_sub, d3) = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d3.message.offset, 2);
    b.shutdown().await;
}

#[tokio::test]
async fn next_is_cancel_safe() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("c", ChannelMode::Queue, 1))
        .unwrap();
    let mut sub = b
        .subscribe("c", "g", info("w", 1), 10, StartFrom::Earliest)
        .unwrap();
    for _ in 0..20 {
        assert!(next_within(&mut sub, 1).await.is_none());
    }
    publish_keyed(&b, "c", 3, 1).await;
    for i in 0..3 {
        assert_eq!(next(&mut sub).await.message.offset, i);
    }
    b.shutdown().await;
}

#[tokio::test]
async fn ack_timeout_then_retry_then_dlq() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    let mut cfg = channel("t", ChannelMode::Stream, 1);
    cfg.retry = RetryPolicy {
        max_attempts: 2,
        backoff_ms: vec![10],
        ack_timeout_ms: 100,
    };
    b.create_channel(cfg).unwrap();
    let mut events = b.events();
    let ids = publish_keyed(&b, "t", 1, 1).await;
    let mut sub = b
        .subscribe("t", "g", info("w", 1), 10, StartFrom::Earliest)
        .unwrap();
    let d1 = next(&mut sub).await;
    assert_eq!(d1.attempt, 1);
    let d2 = next(&mut sub).await; // after 100ms timeout + 10ms backoff
    assert_eq!(d2.attempt, 2);
    assert_eq!(d2.message.offset, d1.message.offset);
    // Ack of the timed-out delivery is rejected.
    assert!(sub.ack(&[d1.delivery_id]).is_err());
    // Second timeout exhausts attempts.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let (dl, total) = b.dlq_list(Some("t"), None, None, 10);
    assert_eq!(total, 1);
    assert_eq!(dl[0].reason, "ack timeout");
    assert_eq!(dl[0].group, "g");
    assert_eq!(dl[0].attempts.len(), 2);
    assert!(
        dl[0]
            .attempts
            .iter()
            .all(|a| a.outcome == AttemptOutcome::TimedOut)
    );
    let s = b.stats();
    let cs = &s.channels[0];
    assert_eq!(cs.timed_out_total, 2);
    assert_eq!(cs.retries_total, 1);
    assert_eq!(cs.dead_lettered_total, 1);
    assert_eq!(cs.groups[0].lag, 0);
    // Retry timeline is in the attempt history.
    let (_, hist) = b.get_message("t", ids[0]).unwrap().unwrap();
    assert_eq!(hist["g"].len(), 2);
    // Batched DeadLettered event within ~1s.
    let ev = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(BrokerEvent::DeadLettered { count, reason, .. }) = events.recv().await {
                return (count, reason);
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(ev, (1, "ack timeout".to_string()));
    b.shutdown().await;
}

#[tokio::test]
async fn subscription_drop_returns_in_flight() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("d", ChannelMode::Queue, 1))
        .unwrap();
    publish_keyed(&b, "d", 2, 2).await;
    let mut a = b
        .subscribe("d", "g", info("a", 1), 10, StartFrom::Earliest)
        .unwrap();
    let d0 = next(&mut a).await;
    let d1 = next(&mut a).await;
    a.ack(&[d1.delivery_id]).unwrap();
    drop(a);
    let mut c = b
        .subscribe("d", "g", info("c", 2), 10, StartFrom::Earliest)
        .unwrap();
    let r = next(&mut c).await;
    assert_eq!(r.message.offset, d0.message.offset);
    assert_eq!(r.attempt, 2);
    c.ack(&[r.delivery_id]).unwrap();
    assert!(next_within(&mut c, 100).await.is_none());
    b.shutdown().await;
}

#[tokio::test]
async fn dlq_summary_retry_discard() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("a", ChannelMode::Queue, 1))
        .unwrap();
    b.create_channel(channel("b", ChannelMode::Queue, 1))
        .unwrap();
    for ch in ["a", "b"] {
        publish_keyed(&b, ch, 3, 3).await;
        let mut sub = b
            .subscribe(ch, "g", info("w", 1), 10, StartFrom::Earliest)
            .unwrap();
        for i in 0..3 {
            let d = next(&mut sub).await;
            let reason = if i == 2 { "bad payload" } else { "db down" };
            sub.nack(d.delivery_id, reason, false).unwrap();
        }
    }
    let sum = b.dlq_summary();
    assert_eq!(sum.total, 6);
    assert_eq!(sum.groups[0].reason, "db down");
    assert_eq!(sum.groups[0].count, 4);
    assert_eq!(sum.groups[0].channels["a"], 2);
    assert_eq!(sum.groups[1].count, 2);
    assert!(sum.groups[0].first_ms <= sum.groups[0].last_ms);
    // Newest first + pagination.
    let (page1, total) = b.dlq_list(None, None, None, 4);
    assert_eq!(total, 6);
    assert_eq!(page1.len(), 4);
    assert!(page1.windows(2).all(|w| w[0].id > w[1].id));
    let (page2, total2) = b.dlq_list(None, None, Some(page1[3].id), 4);
    assert_eq!(page2.len(), 2);
    assert_eq!(total2, 2);
    // Retry "db down" on channel a → back to group g with a fresh budget.
    let mut sub = b
        .subscribe("a", "g", info("w", 2), 10, StartFrom::Earliest)
        .unwrap();
    let n = b
        .dlq_retry(DlqSelector::Reason {
            reason: "db down".into(),
            channel: Some("a".into()),
        })
        .unwrap();
    assert_eq!(n, 2);
    assert_eq!(b.dlq_summary().total, 4, "retrying entries are hidden");
    let r1 = next(&mut sub).await;
    let r2 = next(&mut sub).await;
    assert_eq!((r1.attempt, r2.attempt), (1, 1));
    sub.ack(&[r1.delivery_id]).unwrap();
    // r2 fails again → a new DLQ entry replaces the old one.
    sub.nack(r2.delivery_id, "db down again", false).unwrap();
    let (a_entries, a_total) = b.dlq_list(Some("a"), None, None, 10);
    assert_eq!(a_total, 2);
    assert!(a_entries.iter().any(|e| e.reason == "db down again"));
    // Discard by id, then everything.
    let id = a_entries[0].id;
    assert_eq!(
        b.dlq_discard(DlqSelector::Ids(vec![("a".into(), id)]))
            .unwrap(),
        1
    );
    assert_eq!(
        b.dlq_discard(DlqSelector::Ids(vec![("a".into(), id)]))
            .unwrap(),
        0
    );
    assert_eq!(b.dlq_discard(DlqSelector::All).unwrap(), 4);
    assert_eq!(b.dlq_summary().total, 0);
    assert_eq!(
        b.stats().channels.iter().map(|c| c.dlq_size).sum::<u64>(),
        0
    );
    b.shutdown().await;
}

#[tokio::test]
async fn backoff_and_attempt_exhaustion_on_nack() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    let mut cfg = channel("n", ChannelMode::Stream, 1);
    cfg.retry = RetryPolicy {
        max_attempts: 3,
        backoff_ms: vec![50, 100],
        ack_timeout_ms: 5000,
    };
    b.create_channel(cfg).unwrap();
    publish_keyed(&b, "n", 1, 1).await;
    let mut sub = b
        .subscribe("n", "g", info("w", 1), 10, StartFrom::Earliest)
        .unwrap();
    let d = next(&mut sub).await;
    sub.nack(d.delivery_id, "e1", true).unwrap();
    let t = std::time::Instant::now();
    let d = next(&mut sub).await;
    assert!(t.elapsed() >= Duration::from_millis(45));
    assert_eq!(d.attempt, 2);
    sub.nack(d.delivery_id, "e2", true).unwrap();
    let t = std::time::Instant::now();
    let d = next(&mut sub).await;
    assert!(t.elapsed() >= Duration::from_millis(95));
    assert_eq!(d.attempt, 3);
    sub.nack(d.delivery_id, "e3", true).unwrap();
    let (dl, _) = b.dlq_list(None, None, None, 10);
    assert_eq!(dl.len(), 1);
    assert_eq!(dl[0].reason, "e3");
    let s = b.stats();
    let g = &s.channels[0].groups[0];
    assert_eq!((g.nacked_total, g.dead_lettered_total), (3, 1));
    assert_eq!(g.instances[0].last_error.as_deref(), Some("e3"));
    b.shutdown().await;
}

#[tokio::test]
async fn dlq_disabled_drops() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    let mut cfg = channel("x", ChannelMode::Queue, 1);
    cfg.dlq_enabled = false;
    b.create_channel(cfg).unwrap();
    publish_keyed(&b, "x", 1, 1).await;
    let mut sub = b
        .subscribe("x", "g", info("w", 1), 10, StartFrom::Earliest)
        .unwrap();
    let d = next(&mut sub).await;
    sub.nack(d.delivery_id, "nope", false).unwrap();
    assert_eq!(b.dlq_summary().total, 0);
    assert_eq!(b.stats().channels[0].dead_lettered_total, 1);
    b.shutdown().await;
}

#[tokio::test]
async fn reset_offsets_and_timestamp_start() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("r", ChannelMode::Stream, 1))
        .unwrap();
    publish_keyed(&b, "r", 5, 1).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let t = fluxmq_proto::now_ms();
    tokio::time::sleep(Duration::from_millis(5)).await;
    publish_keyed(&b, "r", 3, 1).await;
    let mut ts = b
        .subscribe("r", "ts", info("w", 1), 100, StartFrom::TimestampMs(t))
        .unwrap();
    for i in 5..8 {
        let d = next(&mut ts).await;
        assert_eq!(d.message.offset, i);
        ts.ack(&[d.delivery_id]).unwrap();
    }
    assert!(next_within(&mut ts, 50).await.is_none());
    b.reset_offsets("r", "ts", StartFrom::Earliest).unwrap();
    let d = next(&mut ts).await;
    assert_eq!(d.message.offset, 0);
    b.reset_offsets("r", "ts", StartFrom::Latest).unwrap();
    assert!(next_within(&mut ts, 50).await.is_none());
    // The delivery from before the reset can still be acked.
    ts.ack(&[d.delivery_id]).unwrap();
    assert_eq!(
        b.stats().channels[0]
            .groups
            .iter()
            .find(|g| g.name == "ts")
            .unwrap()
            .lag,
        0
    );
    // Reset of an unknown group creates it at that position.
    b.reset_offsets("r", "fresh", StartFrom::Earliest).unwrap();
    assert_eq!(
        b.stats().channels[0]
            .groups
            .iter()
            .find(|g| g.name == "fresh")
            .unwrap()
            .lag,
        8
    );
    b.shutdown().await;
}

#[tokio::test]
async fn delete_channel_ends_subscriptions_and_tail() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("gone", ChannelMode::Stream, 2))
        .unwrap();
    publish_keyed(&b, "gone", 10, 2).await;
    let mut sub = b
        .subscribe("gone", "g", info("w", 1), 1, StartFrom::Latest)
        .unwrap();
    let mut tail = b.tail("gone").unwrap();
    let waiter = tokio::spawn(async move { sub.next().await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    b.delete_channel("gone").unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(2), tail.recv())
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        b.publish("gone", vec![msg(None, "x")]).await,
        Err(CoreError::ChannelNotFound(_))
    ));
    assert!(b.channel("gone").is_none());
    // Data directory is removed (asynchronously by the writer thread).
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!dir.path().join("channels/gone").exists());
    assert_eq!(
        std::fs::read_dir(dir.path().join("trash")).unwrap().count(),
        0
    );
    // Re-create with the same name: empty.
    b.create_channel(channel("gone", ChannelMode::Stream, 2))
        .unwrap();
    let (m, _, _) = b.read_messages("gone", &Default::default()).unwrap();
    assert!(m.is_empty());
    b.shutdown().await;
}

/// Regression: every message is delivered exactly once (no stalls) when consuming a large
/// backlog written by many concurrent single-message publishers (exercises read-ahead
/// fetches, the recent-message cache and their hand-over).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backlog_dispatch_is_complete() {
    let dir = tempfile::tempdir().unwrap();
    let b = std::sync::Arc::new(open(dir.path()).await);
    b.create_channel(channel("big", ChannelMode::Stream, 4))
        .unwrap();
    let mut tasks = Vec::new();
    for t in 0..64 {
        let b = b.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..500 {
                b.publish(
                    "big",
                    vec![msg(Some(&format!("k{t}")), &format!("{t}-{i}"))],
                )
                .await
                .unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let total = 64 * 500;
    let mut sub = b
        .subscribe("big", "g", info("c", 1), 5000, StartFrom::Earliest)
        .unwrap();
    let mut seen = std::collections::HashSet::new();
    let mut acks = Vec::new();
    while seen.len() < total {
        let Some(d) = next_within(&mut sub, 2000).await else {
            let s = b.stats();
            panic!(
                "stalled at {} of {total}: {:?}",
                seen.len(),
                s.channels[0].groups[0].lag
            );
        };
        assert!(
            seen.insert((d.message.partition, d.message.offset)),
            "duplicate {:?}",
            d.message.offset
        );
        acks.push(d.delivery_id);
        if acks.len() >= 500 {
            sub.ack(&acks).unwrap();
            acks.clear();
        }
    }
    sub.ack(&acks).unwrap();
    drop(sub);
    b.shutdown().await;
}
