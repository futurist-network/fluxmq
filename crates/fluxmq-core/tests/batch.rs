//! Batch pull (`Subscription::next_batch` / `try_next_batch`): bounds, order, fairness and
//! unchanged retry / DLQ semantics.

mod common;

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use common::*;
use fluxmq_core::SharedDelivery as Delivery;
use fluxmq_core::{Subscription, delivery_wire_size};
use fluxmq_proto::model::{ChannelMode, RetryPolicy, StartFrom};

async fn batch(sub: &mut Subscription, max: usize, max_bytes: usize) -> Vec<Delivery> {
    let mut out = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        sub.next_batch(&mut out, max, max_bytes),
    )
    .await
    .expect("batch within 5s")
    .expect("subscription open");
    out
}

fn ids(ds: &[Delivery]) -> Vec<u64> {
    ds.iter().map(|d| d.delivery_id).collect()
}

#[tokio::test]
async fn batch_is_bounded_by_prefetch_credit() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("c", ChannelMode::Queue, 1))
        .unwrap();
    publish_keyed(&b, "c", 20, 20).await;
    let mut sub = b
        .subscribe("c", "g", info("w", 1), 5, StartFrom::Earliest)
        .unwrap();
    let first = batch(&mut sub, 256, usize::MAX).await;
    assert_eq!(first.len(), 5, "whole credit in one batch, no more");
    let mut more = Vec::new();
    assert_eq!(sub.try_next_batch(&mut more, 256, usize::MAX), Some(0));
    assert!(more.is_empty(), "no credit left");
    sub.ack(&ids(&first[..2])).unwrap();
    let second = batch(&mut sub, 256, usize::MAX).await;
    assert_eq!(second.len(), 2, "exactly the credit the acks freed");
    let s = b.stats();
    assert_eq!(s.channels[0].groups[0].in_flight, 5);
    b.shutdown().await;
}

#[tokio::test]
async fn batch_is_bounded_by_count_and_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("c", ChannelMode::Stream, 1))
        .unwrap();
    publish_keyed(&b, "c", 50, 1).await;
    let mut sub = b
        .subscribe("c", "g", info("w", 1), 1000, StartFrom::Earliest)
        .unwrap();
    assert_eq!(batch(&mut sub, 7, usize::MAX).await.len(), 7);
    // A byte budget below one message still yields one (a batch is never empty for size).
    let one = batch(&mut sub, 100, 1).await;
    assert_eq!(one.len(), 1);
    let size = delivery_wire_size(&one[0].message);
    let three = batch(&mut sub, 100, size * 3 + size / 2).await;
    assert_eq!(three.len(), 3, "byte bound");
    // Topping up a non-empty batch keeps the byte bound strict.
    let mut out = three;
    assert_eq!(sub.try_next_batch(&mut out, 100, size - 1), Some(0));
    assert_eq!(out.len(), 3);
    assert!(sub.try_next_batch(&mut out, 100, size + 10).unwrap() > 0);
    assert_eq!(out.len(), 4);
    b.shutdown().await;
}

#[tokio::test]
async fn messages_put_back_by_the_byte_bound_are_not_lost_or_reordered() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("c", ChannelMode::Stream, 3))
        .unwrap();
    publish_keyed(&b, "c", 300, 300).await;
    let mut sub = b
        .subscribe("c", "g", info("w", 1), 10_000, StartFrom::Earliest)
        .unwrap();
    // Fits ~2.5 messages: every batch ends with a put-back.
    let size = 70usize;
    let mut seen = BTreeSet::new();
    let mut last: HashMap<u32, u64> = HashMap::new();
    while seen.len() < 300 {
        let ds = batch(&mut sub, 256, size * 5 / 2).await;
        assert!(!ds.is_empty() && ds.len() <= 3);
        for d in &ds {
            assert_eq!(d.attempt, 1);
            if let Some(prev) = last.insert(d.message.partition, d.message.offset) {
                assert_eq!(
                    d.message.offset,
                    prev + 1,
                    "in order, no gaps per partition"
                );
            }
            assert!(
                seen.insert((d.message.partition, d.message.offset)),
                "no dup"
            );
        }
        sub.ack(&ids(&ds)).unwrap();
    }
    let mut rest = Vec::new();
    assert_eq!(sub.try_next_batch(&mut rest, 256, usize::MAX), Some(0));
    assert_eq!(b.stats().channels[0].groups[0].lag, 0);
    b.shutdown().await;
}

/// A batch of N is exactly N consecutive `next()` calls: same messages, same order, across
/// partitions (round-robin fairness preserved).
#[tokio::test]
async fn batch_dispatch_order_equals_sequential_polls() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("c", ChannelMode::Stream, 4))
        .unwrap();
    publish_keyed(&b, "c", 400, 13).await;
    let mut seq = b
        .subscribe("c", "seq", info("a", 1), 10_000, StartFrom::Earliest)
        .unwrap();
    let mut bat = b
        .subscribe("c", "bat", info("b", 2), 10_000, StartFrom::Earliest)
        .unwrap();
    let mut a = Vec::new();
    for _ in 0..400 {
        let d = next(&mut seq).await;
        a.push((d.message.partition, d.message.offset));
    }
    let mut c = Vec::new();
    while c.len() < 400 {
        for d in batch(&mut bat, 37, usize::MAX).await {
            c.push((d.message.partition, d.message.offset));
        }
    }
    assert_eq!(a, c);
    b.shutdown().await;
}

#[tokio::test]
async fn keyed_batches_never_hold_two_messages_of_one_key() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("k", ChannelMode::Keyed, 1))
        .unwrap();
    publish_keyed(&b, "k", 30, 3).await;
    let mut sub = b
        .subscribe("k", "g", info("w", 1), 100, StartFrom::Earliest)
        .unwrap();
    let mut done = 0;
    while done < 30 {
        let ds = batch(&mut sub, 256, usize::MAX).await;
        let keys: BTreeSet<_> = ds.iter().map(|d| d.message.key.clone()).collect();
        assert_eq!(keys.len(), ds.len(), "one in-flight message per key");
        assert!(ds.len() <= 3);
        done += ds.len();
        sub.ack(&ids(&ds)).unwrap();
    }
    b.shutdown().await;
}

#[tokio::test]
async fn nack_retry_and_dead_letter_work_with_batches() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    let mut cfg = channel("n", ChannelMode::Queue, 1);
    cfg.retry = RetryPolicy {
        max_attempts: 2,
        backoff_ms: vec![30],
        ack_timeout_ms: 5_000,
    };
    b.create_channel(cfg).unwrap();
    publish_keyed(&b, "n", 4, 4).await;
    let mut sub = b
        .subscribe("n", "g", info("w", 1), 10, StartFrom::Earliest)
        .unwrap();
    let ds = batch(&mut sub, 256, usize::MAX).await;
    assert_eq!(ds.len(), 4);
    // Ack two, nack one with retry, dead-letter one right away.
    sub.ack(&[ds[0].delivery_id, ds[2].delivery_id]).unwrap();
    sub.nack(ds[1].delivery_id, "boom", true).unwrap();
    sub.nack(ds[3].delivery_id, "fatal", false).unwrap();
    let t = std::time::Instant::now();
    let again = batch(&mut sub, 256, usize::MAX).await;
    assert!(
        t.elapsed() >= Duration::from_millis(25),
        "retry honours backoff"
    );
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].attempt, 2);
    assert_eq!(again[0].message.offset, ds[1].message.offset);
    sub.nack(again[0].delivery_id, "boom again", true).unwrap();
    // Attempts exhausted → DLQ; nothing more to deliver.
    let mut rest = Vec::new();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(sub.try_next_batch(&mut rest, 256, usize::MAX), Some(0));
    let (dl, _) = b.dlq_list(None, None, None, 10);
    let mut reasons: Vec<_> = dl.iter().map(|d| d.reason.as_str()).collect();
    reasons.sort();
    assert_eq!(reasons, ["boom again", "fatal"]);
    b.shutdown().await;
}

#[tokio::test]
async fn closing_a_subscription_redelivers_its_whole_batch() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("q", ChannelMode::Queue, 1))
        .unwrap();
    publish_keyed(&b, "q", 6, 6).await;
    let mut first = b
        .subscribe("q", "g", info("a", 1), 100, StartFrom::Earliest)
        .unwrap();
    let ds = batch(&mut first, 256, usize::MAX).await;
    assert_eq!(ds.len(), 6);
    first.ack(&[ds[0].delivery_id]).unwrap();
    drop(first);
    let mut second = b
        .subscribe("q", "g", info("b", 2), 100, StartFrom::Earliest)
        .unwrap();
    let again = batch(&mut second, 256, usize::MAX).await;
    // (Redelivery order after a disconnect is unspecified, as before batching.)
    let offsets: BTreeSet<u64> = again.iter().map(|d| d.message.offset).collect();
    assert_eq!(offsets, BTreeSet::from([1, 2, 3, 4, 5]));
    assert!(again.iter().all(|d| d.attempt == 2));
    b.shutdown().await;
}

#[tokio::test]
async fn next_batch_waits_for_messages_and_ends_on_delete() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("w", ChannelMode::Stream, 1))
        .unwrap();
    let mut sub = b
        .subscribe("w", "g", info("w", 1), 100, StartFrom::Latest)
        .unwrap();
    let b2 = b.clone();
    let publisher = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        publish_keyed(&b2, "w", 3, 3).await;
    });
    let ds = batch(&mut sub, 256, usize::MAX).await;
    assert!(!ds.is_empty());
    publisher.await.unwrap();
    b.delete_channel("w").unwrap();
    let mut out = Vec::new();
    // Whatever is still deliverable comes out, then the subscription ends.
    while tokio::time::timeout(
        Duration::from_secs(5),
        sub.next_batch(&mut out, 256, usize::MAX),
    )
    .await
    .expect("ends in time")
    .is_some()
    {}
    b.shutdown().await;
}

/// Acks wake only the subscription whose credit they free; a batch waiter blocked on
/// prefetch must still be woken by its own acks (even when another member sends them), and a
/// keyed ack that unblocks a deferred message wakes the subscription that can take it.
#[tokio::test]
async fn acks_wake_blocked_batch_waiters() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("q", ChannelMode::Queue, 1))
        .unwrap();
    publish_keyed(&b, "q", 6, 6).await;
    let mut sub = b
        .subscribe("q", "g", info("w", 1), 2, StartFrom::Earliest)
        .unwrap();
    let first = batch(&mut sub, 256, usize::MAX).await;
    assert_eq!(first.len(), 2);
    let waiter = tokio::spawn(async move {
        let d = batch(&mut sub, 256, usize::MAX).await;
        (sub, d)
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    // The group accepts acks from any member: a second member acks the first one's
    // deliveries, which must wake the first one (its credit was freed).
    let other = b
        .subscribe("q", "g", info("x", 2), 1, StartFrom::Earliest)
        .unwrap();
    other.ack(&ids(&first)).unwrap();
    let (_sub, more) = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("woken by the ack")
        .unwrap();
    assert!(!more.is_empty());

    // Keyed: the second message of a key is deferred until the first is acked.
    b.create_channel(channel("k", ChannelMode::Keyed, 1))
        .unwrap();
    publish_keyed(&b, "k", 2, 1).await;
    let mut ks = b
        .subscribe("k", "g", info("w", 3), 10, StartFrom::Earliest)
        .unwrap();
    let d1 = batch(&mut ks, 256, usize::MAX).await;
    assert_eq!(d1.len(), 1, "second message of the key is deferred");
    let waiter = tokio::spawn(async move {
        let d = batch(&mut ks, 256, usize::MAX).await;
        (ks, d)
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    // Ack through another member (the waiter has credit left, so only the unblocked key can
    // wake it).
    let member = b
        .subscribe("k", "g", info("z", 5), 1, StartFrom::Earliest)
        .unwrap();
    member.ack(&ids(&d1)).unwrap();
    let (_ks, d2) = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("woken by the keyed ack")
        .unwrap();
    assert_eq!(d2[0].message.offset, 1);
    assert!(
        std::sync::Arc::strong_count(&d2[0].message) >= 2,
        "shared with in-flight"
    );
    b.shutdown().await;
}
