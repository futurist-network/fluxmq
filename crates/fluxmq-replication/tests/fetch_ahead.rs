//! v0.8 fetch-ahead: followers fetch the next chunk while earlier ones are fsynced and
//! report their durable end separately (`FetchExt`, `Progress`). Acks must still wait for
//! every ISR member's fsync, a lone publish must not wait for a long-poll, and a follower
//! whose fsyncs stall must still leave the ISR although it keeps fetching.

mod common;

use std::time::{Duration, Instant};

use common::*;
use fluxmq_core::FsyncPolicy;
use fluxmq_replication::ReplicaEndpoint;

const GC: FsyncPolicy = FsyncPolicy::GroupCommit {
    interval: Duration::from_millis(1),
};

fn wire_pair(a: &Node, b: &Node) {
    let eps = vec![
        ReplicaEndpoint {
            node_id: a.id.clone(),
            addr: a.mgr.local_addr(),
        },
        ReplicaEndpoint {
            node_id: b.id.clone(),
            addr: b.mgr.local_addr(),
        },
    ];
    a.mgr.set_endpoints(eps.clone());
    b.mgr.set_endpoints(eps);
}

/// Leader `a`, follower `b`, one partition, ISR {a, b} committed.
async fn pair(
    a_cfg: impl FnOnce(&mut fluxmq_replication::ReplicationConfig),
    b_cfg: impl FnOnce(&mut fluxmq_replication::ReplicationConfig),
    min_isr: u32,
) -> (Node, Node, tokio::task::JoinHandle<()>) {
    let a = Node::start_with("n1", tempfile::tempdir().unwrap(), GC, a_cfg).await;
    let b = Node::start_with("n2", tempfile::tempdir().unwrap(), GC, b_cfg).await;
    wire_pair(&a, &b);
    for n in [&a, &b] {
        n.broker.create_channel(channel(1)).unwrap();
    }
    let ctl = auto_commit(&a);
    a.lead(0, 1, &[&a, &b], min_isr);
    b.follow(0, 1, &a);
    wait_sync("ISR full", || a.status(0).isr.len() == 2).await;
    (a, b, ctl)
}

/// The follower holds a batch in its log (fetched ahead) but its fsync is stuck: the leader
/// must not acknowledge the publish, nor expose it, before the follower made it durable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ack_waits_for_the_follower_fsync_not_its_fetch() {
    let (a, b, _ctl) = pair(|_| {}, |_| {}, 2).await;
    a.broker.publish(CH, msgs(0..10)).await.unwrap();
    wait_sync("b durable", || b.leo(0) == 10).await;

    // Shorter than LAG_MAX (the follower would rightly leave the ISR otherwise).
    const HOLD: Duration = Duration::from_millis(250);
    // b's next fsync of the partition sleeps, then fails (EIO): b rolls the chunk back,
    // refetches it and makes it durable with its next commit.
    b.broker
        .replica(CH, 0)
        .unwrap()
        .fail_next_fsync_for_tests_after(HOLD);
    let t0 = Instant::now();
    let watch = {
        let ab = a.broker.clone();
        tokio::spawn(async move {
            let mut early_hw = Vec::new();
            while t0.elapsed() < HOLD - Duration::from_millis(50) {
                early_hw.push(ab.replica(CH, 0).unwrap().high_watermark());
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            early_hw
        })
    };
    let ids = a.broker.publish(CH, msgs(10..15)).await.unwrap();
    let took = t0.elapsed();
    assert_eq!(ids.last().unwrap().offset, 14);
    assert!(
        took >= HOLD - Duration::from_millis(20),
        "acked after {took:?}, before the follower's fsync finished"
    );
    let early_hw = watch.await.unwrap();
    assert!(
        early_hw.iter().all(|&hw| hw <= 10),
        "HW passed the batch while the follower's fsync was held: {early_hw:?}"
    );
    wait_sync("b durable again", || b.leo(0) == 15).await;
    assert_eq!(a.log(0), b.log(0));
    b.stop().await;
    a.stop().await;
}

/// A follower that fetched ahead parks its next fetch in the leader's long-poll; its durable
/// end must reach the leader by a progress report, not when that long-poll ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lone_publish_is_acked_without_waiting_for_the_long_poll() {
    let long = |c: &mut fluxmq_replication::ReplicationConfig| {
        c.fetch_max_wait = Duration::from_secs(4);
    };
    let (a, b, _ctl) = pair(long, long, 2).await;
    // Let both sides settle into the long-poll.
    tokio::time::sleep(Duration::from_millis(200)).await;
    for i in 0..3 {
        let t0 = Instant::now();
        a.broker.publish(CH, msgs(i..i + 1)).await.unwrap();
        let took = t0.elapsed();
        assert!(
            took < Duration::from_secs(2),
            "publish {i} took {took:?} (waited for the follower's long-poll)"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    wait_sync("b durable", || b.leo(0) == 3).await;
    b.stop().await;
    a.stop().await;
}

/// The follower keeps fetching (ahead) while its fsyncs are stuck. It must still leave the
/// ISR after `replica_lag_max`, so publishes with `min_isr = 1` resume long before the stall
/// ends (before v0.8 a stuck follower stopped fetching, and the fetch lag evicted it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_with_stalled_fsyncs_leaves_the_isr() {
    let (a, b, _ctl) = pair(|_| {}, |_| {}, 1).await;
    a.broker.publish(CH, msgs(0..5)).await.unwrap();
    wait_sync("b durable", || b.leo(0) == 5).await;

    const STALL: Duration = Duration::from_secs(6);
    b.broker
        .replica(CH, 0)
        .unwrap()
        .fail_next_fsync_for_tests_after(STALL);
    let t0 = Instant::now();
    // Keep data flowing so the follower keeps fetching while it is stuck.
    let feeder = {
        let ab = a.broker.clone();
        tokio::spawn(async move {
            let mut tasks = Vec::new();
            // Fewer chunks than the follower's fetch-ahead bound, so it never stops
            // fetching on its own: only the durability lag can evict it.
            for i in 0..12 {
                let ab = ab.clone();
                tasks.push(tokio::spawn(async move {
                    let r = ab.publish(CH, msgs(100 + i..101 + i)).await;
                    (r.is_ok(), Instant::now())
                }));
                tokio::time::sleep(Duration::from_millis(120)).await;
            }
            let mut out = Vec::new();
            for t in tasks {
                out.push(t.await.unwrap());
            }
            out
        })
    };
    wait_sync("b evicted from the ISR", || a.status(0).isr.len() == 1).await;
    let evicted_after = t0.elapsed();
    assert!(
        evicted_after < STALL - Duration::from_secs(2),
        "evicted only after {evicted_after:?}"
    );
    let done = feeder.await.unwrap();
    assert!(done.iter().all(|(ok, _)| *ok), "a publish failed");
    let last = done.iter().map(|(_, at)| *at).max().unwrap();
    assert!(
        last.duration_since(t0) < STALL - Duration::from_secs(1),
        "publishes waited for the stalled follower: last ack after {:?}",
        last.duration_since(t0)
    );
    // After the stall the follower recovers and rejoins.
    wait_sync("b rejoined", || {
        a.status(0).isr.len() == 2 && b.leo(0) == a.leo(0)
    })
    .await;
    assert_eq!(a.log(0), b.log(0));
    b.stop().await;
    a.stop().await;
}
