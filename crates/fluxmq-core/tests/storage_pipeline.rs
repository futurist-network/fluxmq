//! v0.6 publish→durable path: sticky partitioning, pipelined group commit on the shared
//! fsync pool, paced retention.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::*;
use fluxmq_core::{Broker, FsyncPolicy};
use fluxmq_proto::model::{ChannelMode, MessageId, PublishMessage, RetentionPolicy};

fn keyless(n: usize, tag: &str) -> Vec<PublishMessage> {
    (0..n).map(|i| msg(None, &format!("{tag}-{i}"))).collect()
}

fn segments(b: &Broker, ch: &str) -> Vec<u32> {
    b.stats()
        .channels
        .iter()
        .find(|c| c.config.name == ch)
        .unwrap()
        .partitions
        .iter()
        .map(|p| p.segments)
        .collect()
}

#[tokio::test]
async fn sticky_partitioner_keeps_requests_together_and_balances() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("s", ChannelMode::Stream, 4))
        .unwrap();
    let mut per_partition = BTreeMap::<u32, usize>::new();
    for r in 0..40 {
        let ids = b.publish("s", keyless(10, &format!("r{r}"))).await.unwrap();
        let p = ids[0].partition;
        assert!(
            ids.iter().all(|id| id.partition == p),
            "keyless messages of one request share a partition"
        );
        *per_partition.entry(p).or_default() += ids.len();
    }
    // Rotating per request: balanced over requests.
    assert_eq!(per_partition.len(), 4);
    assert!(
        per_partition.values().all(|&n| n == 100),
        "{per_partition:?}"
    );
    // Keyed messages keep the stable key hash, even mixed into a keyless request.
    let mut mixed = keyless(3, "m");
    mixed.push(msg(Some("customer-42"), "k"));
    let ids = b.publish("s", mixed).await.unwrap();
    let expected = crc32fast::hash(b"customer-42") % 4;
    assert_eq!(ids[3].partition, expected);
    let again = b
        .publish("s", vec![msg(Some("customer-42"), "k2")])
        .await
        .unwrap();
    assert_eq!(again[0].partition, expected);
    // A request larger than the sticky byte bound (~1 MiB) moves on to other partitions.
    let big: Vec<PublishMessage> = (0..48)
        .map(|_| PublishMessage {
            key: None,
            headers: Default::default(),
            payload: Bytes::from(vec![1u8; 64 * 1024]),
        })
        .collect();
    let ids = b.publish("s", big).await.unwrap();
    let mut parts: Vec<u32> = ids.iter().map(|i| i.partition).collect();
    parts.dedup();
    assert!(parts.len() >= 2, "{parts:?}");
    // Each run within a request is contiguous in its partition (order preserved).
    for w in ids.windows(2) {
        if w[0].partition == w[1].partition {
            assert_eq!(w[0].offset + 1, w[1].offset);
        }
    }
    b.shutdown().await;
}

/// Many concurrent publishers: acked offsets per partition are exactly `0..n` (no gap, no
/// duplicate, acks only for durable data), and everything acked survives a restart.
async fn concurrent_publish_roundtrip(pipeline: bool, fsync: FsyncPolicy) {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.commit_pipeline = pipeline;
    cfg.fsync = fsync;
    cfg.segment_bytes = 16 * 1024; // many rolls inside pipelined batches
    let b = Arc::new(Broker::open(cfg.clone()).await.unwrap());
    b.create_channel(channel("p", ChannelMode::Stream, 4))
        .unwrap();
    let mut tasks = Vec::new();
    for t in 0..24 {
        let b = b.clone();
        tasks.push(tokio::spawn(async move {
            let mut ids: Vec<(MessageId, String)> = Vec::new();
            for r in 0..25 {
                let msgs: Vec<PublishMessage> = (0..8)
                    .map(|i| {
                        let key = (i % 3 == 0).then(|| format!("k{}", (t + i) % 7));
                        msg(key.as_deref(), &format!("t{t}-r{r}-i{i}"))
                    })
                    .collect();
                let payloads: Vec<String> = msgs
                    .iter()
                    .map(|m| String::from_utf8(m.payload.to_vec()).unwrap())
                    .collect();
                let got = b.publish("p", msgs).await.unwrap();
                ids.extend(got.into_iter().zip(payloads));
            }
            ids
        }));
    }
    let mut all: Vec<(MessageId, String)> = Vec::new();
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    assert_eq!(all.len(), 24 * 25 * 8);
    let mut per: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
    for (id, _) in &all {
        per.entry(id.partition).or_default().push(id.offset);
    }
    for (p, offs) in per.iter_mut() {
        offs.sort_unstable();
        assert!(
            offs.iter().enumerate().all(|(i, o)| *o == i as u64),
            "partition {p}: acked offsets are not 0..n"
        );
    }
    b.shutdown().await;
    drop(b);
    let b = Broker::open(cfg).await.unwrap();
    for (id, payload) in all.iter().step_by(7) {
        let m = b
            .get_message("p", *id)
            .unwrap()
            .expect("acked message present");
        assert_eq!(&m.0.payload[..], payload.as_bytes(), "{id:?}");
    }
    let s = b.stats();
    let ch = s.channels.iter().find(|c| c.config.name == "p").unwrap();
    for (p, offs) in &per {
        assert_eq!(ch.partitions[*p as usize].high_offset, offs.len() as u64);
    }
    b.shutdown().await;
}

#[tokio::test]
async fn pipelined_group_commit_is_ordered_and_durable() {
    let gc = FsyncPolicy::GroupCommit {
        interval: Duration::from_millis(2),
    };
    concurrent_publish_roundtrip(true, gc).await;
}

#[tokio::test]
async fn unpipelined_group_commit_still_works() {
    let gc = FsyncPolicy::GroupCommit {
        interval: Duration::from_millis(2),
    };
    concurrent_publish_roundtrip(false, gc).await;
}

#[tokio::test]
async fn os_buffered_ignores_the_pipeline_flag() {
    concurrent_publish_roundtrip(true, FsyncPolicy::OsBuffered).await;
}

/// Abrupt stop with pipelined commit: publishers are cancelled mid-request and the broker is
/// dropped without shutdown; on reopen every acked message is present and every partition is
/// a contiguous prefix. (Torn files after a real crash are covered by the storage unit test
/// `crash_recovery_with_pipelined_commit`.)
#[tokio::test]
async fn abrupt_stop_during_pipelined_commit_keeps_acked_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.segment_bytes = 8 * 1024;
    cfg.commit_pipeline = true;
    let acked = Arc::new(parking_lot::Mutex::new(Vec::<MessageId>::new()));
    {
        let b = Arc::new(Broker::open(cfg.clone()).await.unwrap());
        b.create_channel(channel("c", ChannelMode::Stream, 3))
            .unwrap();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut tasks = Vec::new();
        for t in 0..8 {
            let (b, acked, stop) = (b.clone(), acked.clone(), stop.clone());
            tasks.push(tokio::spawn(async move {
                let mut r = 0;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    if let Ok(ids) = b.publish("c", keyless(5, &format!("t{t}-{r}"))).await {
                        acked.lock().extend(ids);
                    }
                    r += 1;
                }
            }));
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
        // "Crash": stop recording acks first, then drop everything without shutdown (in-flight
        // batches may or may not have reached the disk; unacked data may survive).
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for t in &tasks {
            t.abort();
        }
        for t in tasks {
            let _ = t.await;
        }
        drop(b);
        // Writers stop asynchronously after the drop; let them exit before reopening.
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let acked = acked.lock().clone();
    assert!(!acked.is_empty());
    let b = Broker::open(cfg).await.unwrap();
    let s = b.stats();
    let ch = s.channels.iter().find(|c| c.config.name == "c").unwrap();
    for id in &acked {
        assert!(
            id.offset < ch.partitions[id.partition as usize].high_offset,
            "acked {id:?} lost"
        );
    }
    for p in 0..3u32 {
        let high = ch.partitions[p as usize].high_offset;
        for o in (0..high).step_by(11) {
            let m = b.get_message(
                "c",
                MessageId {
                    partition: p,
                    offset: o,
                },
            );
            assert!(
                matches!(m, Ok(Some(_))),
                "partition {p} offset {o} unreadable (gap)"
            );
        }
    }
    b.shutdown().await;
}

#[tokio::test]
async fn paced_retention_spreads_deletions() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    // Batch envelopes store these tiny messages in a few bytes each (~1.5 KiB per partition).
    cfg.segment_bytes = 128;
    cfg.segment_jitter = false;
    let b = Broker::open(cfg).await.unwrap();
    let mut c = channel("r", ChannelMode::Stream, 8);
    c.retention = RetentionPolicy {
        max_age_secs: None,
        max_bytes: Some(8 * 256),
    };
    b.create_channel(c).unwrap();
    for _ in 0..40 {
        b.publish("r", keyless(40, "x")).await.unwrap();
    }
    let before: u32 = segments(&b, "r").iter().sum();
    // Counted by the pass itself: the broker's background retention tick may run at the
    // same time, so the segment count difference can include its removals too.
    let started = std::time::Instant::now();
    let removed = b.debug_enforce_retention_paced().await;
    let took = started.elapsed();
    assert!(removed >= 1, "a paced pass removes something (of {before})");
    // v0.8: a pass removes everything due but never back to back (≥ 5 ms apart).
    assert!(
        took >= Duration::from_millis(5) * (removed as u32 - 1),
        "removals are spread in time: {removed} in {took:?}"
    );
    // Repeated passes converge to the unpaced result.
    for _ in 0..200 {
        b.debug_enforce_retention_paced().await;
    }
    let paced: Vec<u32> = segments(&b, "r");
    b.enforce_retention().await;
    assert_eq!(
        segments(&b, "r"),
        paced,
        "paced passes reach the same end state"
    );
    let s = b.stats();
    let ch = s.channels.iter().find(|c| c.config.name == "r").unwrap();
    for p in &ch.partitions {
        assert!(p.storage_bytes <= 256 + 128, "{}", p.storage_bytes);
    }
    b.shutdown().await;
}

/// v0.8: paced retention follows the ingest rate. Sustained ingest that rolls twice as many
/// segments per second as v0.7's fixed pace (16 per channel and second) removed keeps
/// every partition within its byte budget plus two segments the whole time, and the
/// partitions converge to the budget (Kafka's rule) once ingest stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paced_retention_keeps_up_with_sustained_ingest() {
    const PARTITIONS: usize = 16;
    const SEGMENT: u64 = 32 << 10;
    const REQUEST: usize = 8 << 10; // bytes per publish request (one partition each)
    const BUDGET: u64 = 4 * SEGMENT; // per partition
    // A segment rolls past `SEGMENT` by at most one request (plus envelope overhead).
    const SEG_MAX: u64 = SEGMENT + REQUEST as u64 + 2048;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.segment_bytes = SEGMENT;
    cfg.segment_jitter = false;
    // Retention does not depend on the fsync policy; without fsyncs the paced ingest rate
    // holds on a loaded test machine.
    cfg.fsync = FsyncPolicy::OsBuffered;
    let b = Arc::new(Broker::open(cfg).await.unwrap());
    let mut c = channel("hot", ChannelMode::Stream, PARTITIONS as u32);
    c.retention = RetentionPolicy {
        max_age_secs: None,
        max_bytes: Some(BUDGET * PARTITIONS as u64),
    };
    b.create_channel(c).unwrap();

    // 4 producers × 32 requests/s × 8 KiB = 1 MiB/s = 32 segment rolls/s on one channel
    // (2 per partition and second); v0.7 fell behind by ~1 segment/s per partition.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let published = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut producers = Vec::new();
    for t in 0..4 {
        let (b, stop, published) = (b.clone(), stop.clone(), published.clone());
        producers.push(tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_micros(1_000_000 / 32));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
            let payload = Bytes::from(vec![t as u8; 512]);
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                tick.tick().await;
                let msgs = (0..REQUEST / 512)
                    .map(|_| PublishMessage {
                        key: None,
                        headers: Default::default(),
                        payload: payload.clone(),
                    })
                    .collect();
                b.publish("hot", msgs).await.unwrap();
                published.fetch_add(REQUEST as u64, std::sync::atomic::Ordering::Relaxed);
            }
        }));
    }
    let sizes = |b: &Broker| -> Vec<u64> {
        let s = b.stats();
        let ch = s.channels.iter().find(|c| c.config.name == "hot").unwrap();
        ch.partitions.iter().map(|p| p.storage_bytes).collect()
    };
    let started = std::time::Instant::now();
    let mut max = vec![0u64; PARTITIONS];
    while started.elapsed() < Duration::from_secs(6) {
        for (m, s) in max.iter_mut().zip(sizes(&b)) {
            *m = (*m).max(s);
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for p in producers {
        p.await.unwrap();
    }
    let secs = started.elapsed().as_secs_f64();
    let rolls_per_s =
        published.load(std::sync::atomic::Ordering::Relaxed) as f64 / SEGMENT as f64 / secs;
    assert!(
        rolls_per_s > 24.0,
        "ingest too slow to exercise pacing: {rolls_per_s:.1} segments/s"
    );
    let bound = BUDGET + 2 * SEG_MAX;
    for (p, m) in max.iter().enumerate() {
        assert!(
            *m <= bound,
            "partition {p} reached {m} bytes, above budget {BUDGET} + 2 segments ({bound}); \
             ingest {rolls_per_s:.1} segments/s, all maxima {max:?}"
        );
    }
    // Converges to Kafka's rule once ingest stops: what remains covers the budget and the
    // oldest segment could not go without dropping below it.
    tokio::time::sleep(Duration::from_millis(500)).await;
    for (p, s) in sizes(&b).into_iter().enumerate() {
        assert!(
            s < BUDGET + SEG_MAX,
            "partition {p}: {s} bytes after ingest stopped"
        );
    }
    b.shutdown().await;
}

#[tokio::test]
async fn fsync_pool_is_shared_and_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.fsync_threads = 3;
    // Pipelined multi-chain batches go through the pool (single-chain commits — every commit
    // on Apple — are fsynced inline on the writer thread when nothing else is in flight).
    cfg.commit_pipeline = true;
    let b = Arc::new(Broker::open(cfg).await.unwrap());
    for i in 0..4 {
        b.create_channel(channel(&format!("c{i}"), ChannelMode::Stream, 8))
            .unwrap();
    }
    let mut tasks = Vec::new();
    for t in 0..16 {
        let b = b.clone();
        tasks.push(tokio::spawn(async move {
            for r in 0..20 {
                // Keyed across all partitions: many files per commit.
                let msgs = (0..16)
                    .map(|i| msg(Some(&format!("k{i}")), &format!("{t}-{r}")))
                    .collect();
                b.publish(&format!("c{}", t % 4), msgs).await.unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let threads = b.debug_fsync_threads();
    assert!(threads <= 3, "{threads} fsync threads");
    // v0.9: on Apple every commit is one barrier chain, run inline on the writer thread
    // (the device serializes barriers anyway), so the pool may never start a thread there.
    if !cfg!(target_vendor = "apple") {
        assert!(threads >= 1, "{threads} fsync threads");
    }
    b.shutdown().await;
}

/// A group overtaken by retention skips the removed messages — counted and logged, never
/// silent.
#[tokio::test]
async fn retention_overtaking_a_group_is_counted() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.segment_bytes = 1024;
    cfg.segment_jitter = false;
    let b = Broker::open(cfg).await.unwrap();
    let mut c = channel("o", ChannelMode::Stream, 1);
    c.retention = RetentionPolicy {
        max_age_secs: None,
        max_bytes: Some(2048),
    };
    b.create_channel(c).unwrap();
    b.publish("o", keyless(1, "first")).await.unwrap();
    let mut sub = b
        .subscribe(
            "o",
            "slow",
            info("s", 1),
            1,
            fluxmq_proto::model::StartFrom::Earliest,
        )
        .unwrap();
    // Holds its only credit: nothing else is dispatched (or read ahead past retention).
    let first = next(&mut sub).await;
    assert_eq!(first.message.offset, 0);
    for _ in 0..30 {
        b.publish("o", keyless(10, "x")).await.unwrap();
    }
    b.enforce_retention().await;
    let low = b.stats().channels[0].partitions[0].low_offset;
    assert!(low > 1, "retention moved past the group: low {low}");
    sub.ack(&[first.delivery_id]).unwrap();
    let d = next(&mut sub).await;
    assert!(d.message.offset >= low);
    let skipped = b.stats().channels[0].groups[0].retention_skipped_total;
    assert!(skipped > 0 && skipped < low, "skipped {skipped}, low {low}");
    b.shutdown().await;
}
