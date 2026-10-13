//! v0.9 commit path: per-partition completion and the deeper commit pipeline
//! (docs/design/v0.9-commit-path.md). Fsyncs are slowed or failed with the broker's test
//! hooks (`debug_delay_fsyncs`, `debug_delay_next_fsync`, `debug_fail_next_fsync`).

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::*;
use fluxmq_core::{Broker, BrokerConfig};
use fluxmq_proto::model::{ChannelMode, MessageId, PublishMessage};

fn msgs(n: usize, tag: &str) -> Vec<PublishMessage> {
    (0..n).map(|i| msg(None, &format!("{tag}-{i}"))).collect()
}

fn high(b: &Broker, ch: &str, p: u32) -> u64 {
    let s = b.stats();
    let c = s.channels.iter().find(|c| c.config.name == ch).unwrap();
    c.partitions[p as usize].high_offset
}

fn cfg(dir: &std::path::Path, pipeline: bool, depth: usize) -> BrokerConfig {
    let mut c = config(dir);
    c.commit_pipeline = pipeline;
    c.commit_pipeline_depth = depth;
    c
}

/// Spawns a publish to one partition; returns (handle, completion time).
fn spawn_publish(
    b: &Arc<Broker>,
    ch: &'static str,
    p: u32,
    n: usize,
    tag: &str,
) -> tokio::task::JoinHandle<(fluxmq_core::error::Result<Vec<MessageId>>, Instant)> {
    let (b, tag) = (b.clone(), tag.to_string());
    tokio::spawn(async move {
        let r = b.publish_to_partition(ch, p, msgs(n, &tag)).await;
        (r, Instant::now())
    })
}

/// Waits until the writer has queued (appended and submitted) everything sent so far: a
/// publish to another partition round-trips through the same writer queue.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(30)).await;
}

/// Commit k's fsync of a partition finishes *after* commits k+1 and k+2 (same partition,
/// pipelined with depth 3, overlapping fsyncs): their marks must wait for k's. Nothing of the
/// partition becomes visible or acked before k is durable, and acks come in offset order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_marks_publish_in_order_per_partition() {
    let dir = tempfile::tempdir().unwrap();
    let b = Arc::new(Broker::open(cfg(dir.path(), true, 3)).await.unwrap());
    b.create_channel(channel("o", ChannelMode::Stream, 2))
        .unwrap();
    const HOLD: Duration = Duration::from_millis(400);
    b.debug_delay_next_fsync("o", 0, HOLD).unwrap();
    let t0 = Instant::now();
    let a = spawn_publish(&b, "o", 0, 3, "a");
    settle().await;
    let bb = spawn_publish(&b, "o", 0, 4, "b");
    settle().await;
    let c = spawn_publish(&b, "o", 0, 5, "c");
    settle().await;
    // b and c are in later commits whose fsyncs already finished; a's is held.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(t0.elapsed() < HOLD, "test machine too slow for the timing");
    assert_eq!(
        high(&b, "o", 0),
        0,
        "a later mark was published before an earlier one"
    );
    assert!(
        !bb.is_finished() && !c.is_finished(),
        "acked before an earlier mark"
    );
    // Another partition is not held back by partition 0.
    let other = b.publish_to_partition("o", 1, msgs(2, "x")).await.unwrap();
    assert_eq!(other[0].offset, 0);
    assert!(t0.elapsed() < HOLD);
    let (ra, ta) = a.await.unwrap();
    let (rb, tb) = bb.await.unwrap();
    let (rc, tc) = c.await.unwrap();
    let (ra, rb, rc) = (ra.unwrap(), rb.unwrap(), rc.unwrap());
    // Neither later batch is acked before the held one is durable (the acks themselves are
    // sent in offset order in one pass; the tasks' wake-up order is up to the scheduler).
    for t in [ta, tb, tc] {
        assert!(
            t.duration_since(t0) >= HOLD,
            "acked before the earlier mark"
        );
    }
    assert_eq!(ra[0].offset, 0);
    assert_eq!(rb[0].offset, 3);
    assert_eq!(rc[0].offset, 7);
    assert_eq!(high(&b, "o", 0), 12);
    b.shutdown().await;
}

/// Durable marks observed by a concurrent reader only ever grow, and every acked offset is
/// below the mark at ack time, under many publishers with overlapping commits and rolls.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_marks_are_monotonic_under_load() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = cfg(dir.path(), true, 3);
    c.segment_bytes = 16 * 1024; // many rolls (deferred dir/index syncs) inside commits
    let b = Arc::new(Broker::open(c.clone()).await.unwrap());
    b.create_channel(channel("m", ChannelMode::Stream, 4))
        .unwrap();
    // Uneven fsync speeds: partition 0 slow, the others fast.
    b.debug_delay_fsyncs("m", 0, Duration::from_millis(3))
        .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let watcher = {
        let (b, stop) = (b.clone(), stop.clone());
        tokio::spawn(async move {
            let mut last = [0u64; 4];
            while !stop.load(Ordering::Relaxed) {
                for (p, l) in last.iter_mut().enumerate() {
                    let h = high(&b, "m", p as u32);
                    assert!(h >= *l, "partition {p}: durable mark went back {l} -> {h}");
                    *l = h;
                }
                tokio::task::yield_now().await;
            }
        })
    };
    let mut tasks = Vec::new();
    for t in 0..16u32 {
        let b = b.clone();
        tasks.push(tokio::spawn(async move {
            let mut got = Vec::new();
            for r in 0..20 {
                let p = (t + r) % 4;
                let ids = b
                    .publish_to_partition("m", p, msgs(5, &format!("t{t}-{r}")))
                    .await
                    .unwrap();
                let h = high(&b, "m", p);
                assert!(
                    ids.last().unwrap().offset < h,
                    "acked {:?} above the durable mark {h}",
                    ids.last()
                );
                got.extend(ids);
            }
            got
        }));
    }
    let mut per: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
    for t in tasks {
        for id in t.await.unwrap() {
            per.entry(id.partition).or_default().push(id.offset);
        }
    }
    stop.store(true, Ordering::Relaxed);
    watcher.await.unwrap();
    for (p, offs) in per.iter_mut() {
        offs.sort_unstable();
        assert!(
            offs.iter().enumerate().all(|(i, o)| *o == i as u64),
            "partition {p}: acked offsets are not 0..n"
        );
    }
    b.shutdown().await;
    drop(b);
    // Everything acked survived.
    let b = Broker::open(c).await.unwrap();
    for (p, offs) in &per {
        assert_eq!(high(&b, "m", *p), offs.len() as u64);
    }
    b.shutdown().await;
}

/// A partition whose fsyncs are slow does not delay the acks of another partition — in the
/// same commit or in later ones (v0.8: everyone waited for the slowest partition of the
/// commit and the writer blocked meanwhile).
async fn slow_partition_case(pipeline: bool) -> Duration {
    let dir = tempfile::tempdir().unwrap();
    let b = Arc::new(Broker::open(cfg(dir.path(), pipeline, 1)).await.unwrap());
    b.create_channel(channel("s", ChannelMode::Stream, 2))
        .unwrap();
    const SLOW: Duration = Duration::from_millis(600);
    b.debug_delay_fsyncs("s", 0, SLOW).unwrap();
    let t0 = Instant::now();
    // Same batch: both requests are queued before the writer commits.
    let pause = b.debug_pause_writer("s").await.unwrap();
    let slow = spawn_publish(&b, "s", 0, 3, "slow");
    let fast = spawn_publish(&b, "s", 1, 3, "fast");
    settle().await;
    drop(pause);
    let (rf, tf) = fast.await.unwrap();
    rf.unwrap();
    let mut worst = tf.duration_since(t0);
    // Later commits of the fast partition while the slow one is still fsyncing.
    for i in 0..5 {
        let t = Instant::now();
        b.publish_to_partition("s", 1, msgs(2, &format!("f{i}")))
            .await
            .unwrap();
        worst = worst.max(t.elapsed());
    }
    let (rs, ts) = slow.await.unwrap();
    rs.unwrap();
    assert!(ts.duration_since(t0) >= SLOW);
    b.debug_delay_fsyncs("s", 0, Duration::ZERO).unwrap();
    b.shutdown().await;
    worst
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_partition_does_not_delay_others() {
    let worst = slow_partition_case(true).await;
    assert!(
        worst < Duration::from_millis(300),
        "fast partition waited {worst:?} for the slow one"
    );
    // The same scenario with the pipeline off (v0.8 behaviour) does wait: the test is
    // sensitive to the property it checks.
    let worst_off = slow_partition_case(false).await;
    assert!(
        worst_off >= Duration::from_millis(500),
        "unpipelined writer did not wait ({worst_off:?})"
    );
}

/// An fsync failure fails every publish whose data was not durable yet — including later
/// commits of the same partition whose own fsyncs succeeded (fsync errors can be reported
/// to one caller only; data behind a failed fsync is never acked) — while partitions
/// published before the failure stay. Nothing failed is visible or survives a restart, and
/// the partition accepts writes again afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fsync_error_fails_publishes_and_never_acks() {
    for depth in [1usize, 3] {
        let dir = tempfile::tempdir().unwrap();
        let c = cfg(dir.path(), true, depth);
        let b = Arc::new(Broker::open(c.clone()).await.unwrap());
        b.create_channel(channel("e", ChannelMode::Stream, 2))
            .unwrap();
        const HOLD: Duration = Duration::from_millis(300);
        b.debug_fail_next_fsync("e", 0, HOLD).unwrap();
        let x = spawn_publish(&b, "e", 0, 3, "x");
        settle().await;
        // A later batch of the failing partition (in a commit of its own with depth 3,
        // queued behind x's with depth 1).
        let y = spawn_publish(&b, "e", 0, 4, "y");
        settle().await;
        // Another partition, durable long before the failure.
        let z = b.publish_to_partition("e", 1, msgs(2, "z")).await.unwrap();
        assert_eq!(z[0].offset, 0);
        let (rx, _) = x.await.unwrap();
        let (ry, _) = y.await.unwrap();
        assert!(
            rx.is_err(),
            "depth {depth}: publish acked after its fsync failed"
        );
        assert!(
            ry.is_err(),
            "depth {depth}: publish behind a failed fsync was acked"
        );
        assert_eq!(high(&b, "e", 0), 0, "depth {depth}: failed data visible");
        assert_eq!(high(&b, "e", 1), 2);
        // Writes work again; the failed offsets are reused (nothing was replicated).
        let w = b.publish_to_partition("e", 0, msgs(1, "w")).await.unwrap();
        assert_eq!(w[0].offset, 0);
        b.shutdown().await;
        drop(b);
        let b = Broker::open(c).await.unwrap();
        assert_eq!(high(&b, "e", 0), 1);
        assert_eq!(high(&b, "e", 1), 2);
        let m = b
            .get_message(
                "e",
                MessageId {
                    partition: 0,
                    offset: 0,
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(&m.0.payload[..], b"w-0");
        b.shutdown().await;
    }
}

/// A request spanning two partitions is acked only when both are durable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_partition_request_waits_for_every_partition() {
    let dir = tempfile::tempdir().unwrap();
    let b = Arc::new(Broker::open(cfg(dir.path(), true, 1)).await.unwrap());
    b.create_channel(channel("k", ChannelMode::Stream, 2))
        .unwrap();
    // Two keys that hash to different partitions.
    let (k0, k1) = {
        let mut k0 = None;
        let mut k1 = None;
        for i in 0..100 {
            let k = format!("key{i}");
            // The broker's key partitioner (see storage_pipeline.rs).
            match crc32fast::hash(k.as_bytes()) % 2 {
                0 if k0.is_none() => k0 = Some(k),
                1 if k1.is_none() => k1 = Some(k),
                _ => {}
            }
        }
        (k0.unwrap(), k1.unwrap())
    };
    const SLOW: Duration = Duration::from_millis(400);
    b.debug_delay_fsyncs("k", 0, SLOW).unwrap();
    let t0 = Instant::now();
    let h = {
        let b = b.clone();
        tokio::spawn(async move {
            b.publish("k", vec![msg(Some(&k0), "a"), msg(Some(&k1), "b")])
                .await
                .unwrap()
        })
    };
    let ids = h.await.unwrap();
    assert!(
        t0.elapsed() >= SLOW,
        "acked before the slow partition was durable"
    );
    assert_eq!(ids.len(), 2);
    b.debug_delay_fsyncs("k", 0, Duration::ZERO).unwrap();
    b.shutdown().await;
}

/// Abrupt stop with overlapping commits (depth 3) and frequent rolls: every acked message is
/// present after reopening and every partition is a contiguous prefix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abrupt_stop_with_deep_pipeline_keeps_acked_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = cfg(dir.path(), true, 3);
    c.segment_bytes = 8 * 1024;
    let acked = Arc::new(parking_lot::Mutex::new(Vec::<MessageId>::new()));
    {
        let b = Arc::new(Broker::open(c.clone()).await.unwrap());
        b.create_channel(channel("c", ChannelMode::Stream, 4))
            .unwrap();
        b.debug_delay_fsyncs("c", 2, Duration::from_millis(2))
            .unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::new();
        for t in 0..12u32 {
            let (b, acked, stop) = (b.clone(), acked.clone(), stop.clone());
            tasks.push(tokio::spawn(async move {
                let mut r = 0u32;
                while !stop.load(Ordering::Relaxed) {
                    let p = (t + r) % 4;
                    if let Ok(ids) = b
                        .publish_to_partition("c", p, msgs(5, &format!("t{t}-{r}")))
                        .await
                    {
                        acked.lock().extend(ids);
                    }
                    r += 1;
                }
            }));
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
        stop.store(true, Ordering::Relaxed);
        for t in &tasks {
            t.abort();
        }
        for t in tasks {
            let _ = t.await;
        }
        drop(b);
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let acked = acked.lock().clone();
    assert!(!acked.is_empty());
    let b = Broker::open(c).await.unwrap();
    for id in &acked {
        assert!(id.offset < high(&b, "c", id.partition), "acked {id:?} lost");
    }
    for p in 0..4u32 {
        let h = high(&b, "c", p);
        for o in (0..h).step_by(7) {
            let m = b.get_message(
                "c",
                MessageId {
                    partition: p,
                    offset: o,
                },
            );
            assert!(matches!(m, Ok(Some(_))), "partition {p} offset {o}: gap");
        }
    }
    b.shutdown().await;
}
