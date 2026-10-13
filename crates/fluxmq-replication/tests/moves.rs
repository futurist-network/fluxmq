//! v0.4 replica moves (the test plays the controller, driving roles through `set_role` as
//! `fluxmq-cluster` does): new-replica bootstrap at the leader's log start, the group-offset
//! / DLQ snapshot, removed-replica cleanup and the catch-up throttle.

mod common;

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use common::*;
use fluxmq_core::{BrokerConfig, FsyncPolicy};
use fluxmq_proto::model::{RetentionPolicy, StartFrom};
use fluxmq_replication::{ReplicationError, ReplicationManager};

const OS: FsyncPolicy = FsyncPolicy::OsBuffered;

fn dlq_payloads(node: &Node, p: u32) -> BTreeSet<String> {
    node.broker
        .dlq_list(Some(CH), None, None, 1_000_000)
        .0
        .into_iter()
        .filter(|d| d.message.partition == p)
        .map(|d| String::from_utf8(d.message.payload.to_vec()).unwrap())
        .collect()
}

fn committed(node: &Node, p: u32, group: &str) -> Option<u64> {
    node.broker
        .replica(CH, p)
        .unwrap()
        .group_offsets()
        .into_iter()
        .find(|(g, _)| g == group)
        .map(|(_, o)| o)
}

/// A node whose broker rolls segments every `segment_bytes`.
async fn node_with_segments(id: &str, segment_bytes: u64) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg: BrokerConfig = broker_config(dir.path(), id, OS);
    cfg.segment_bytes = segment_bytes;
    let broker = fluxmq_core::Broker::open(cfg).await.unwrap();
    let mgr = ReplicationManager::start(broker.clone(), repl_config(id))
        .await
        .unwrap();
    Node {
        id: id.to_string(),
        dir,
        broker,
        mgr,
    }
}

/// Consumes `n` deliveries of group `g` on `node`, dead-lettering every 10th message
/// (offset % 10 == 3) and acking the rest.
async fn consume(node: &Node, n: usize) {
    let mut sub = node
        .broker
        .subscribe(CH, "g", info("c"), 100, StartFrom::Earliest)
        .unwrap();
    for _ in 0..n {
        let d = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .unwrap()
            .unwrap();
        if d.message.offset % 10 == 3 {
            sub.nack(d.delivery_id, "poison", false).unwrap();
        } else {
            sub.ack(&[d.delivery_id]).unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_replica_bootstraps_at_leader_log_start_with_offsets_and_dlq() {
    let a = node_with_segments("n1", 4096).await;
    let b = Node::start("n2", OS).await;
    let n = vec![a, b];
    wire(&n);
    let (a, b) = (&n[0], &n[1]);
    for node in &n {
        node.broker.create_channel(channel(1)).unwrap();
    }
    let _ctl = auto_commit(a);
    a.lead(0, 1, &[a], 1);
    a.mgr.isr_committed(CH, 0, 1, vec![a.id.clone()]);
    a.broker
        .publish_to_partition(CH, 0, msgs(0..2000))
        .await
        .unwrap();
    // Group g consumed 1500 (150 dead-lettered), then retention removed old segments
    // (the state stream history of those DLQ adds is long gone for a new replica).
    consume(a, 1500).await;
    wait_sync("commits", || committed(a, 0, "g") == Some(1500)).await;
    let upd = fluxmq_proto::api::UpdateChannelRequest {
        retention: Some(RetentionPolicy {
            max_age_secs: None,
            max_bytes: Some(40_000),
        }),
        ..Default::default()
    };
    a.broker.update_channel(CH, upd).unwrap();
    a.broker.enforce_retention().await;
    let low = a.broker.replica(CH, 0).unwrap().log_start_offset();
    assert!(low > 0, "retention trimmed the leader's log");
    assert!(low < 1500, "unconsumed messages retained ({low})");
    let dead = dlq_payloads(a, 0);
    assert_eq!(dead.len(), 150);

    // Move: add b (empty) as a replica (epoch 2); it bootstraps at the leader's log start.
    a.lead(0, 2, &[a, b], 1);
    a.mgr.isr_committed(CH, 0, 2, vec![a.id.clone()]);
    b.follow(0, 2, a);
    wait_sync("b joins the ISR", || a.status(0).isr.contains(&b.id)).await;
    let bh = b.broker.replica(CH, 0).unwrap();
    assert_eq!(
        bh.log_start_offset(),
        low,
        "starts at the leader's low offset"
    );
    assert_eq!(bh.log_end_offset(), 2000);
    assert_eq!(b.log(0), a.log(0));
    // The snapshot arrived before b could join the ISR.
    assert_eq!(committed(b, 0, "g"), Some(1500));
    assert_eq!(dlq_payloads(b, 0), dead);

    // Finalize the move: b leads alone (epoch 3); a is removed and deletes its copy.
    b.lead(0, 3, &[b], 1);
    b.mgr.isr_committed(CH, 0, 3, vec![b.id.clone()]);
    a.offline(0);
    assert!(a.mgr.remove_replica(CH, 0, 3).await.unwrap());
    assert!(!a.dir.path().join("channels").join(CH).exists());
    // Consumers resume after the last committed offset; the DLQ is intact.
    let mut sub = b
        .broker
        .subscribe(CH, "g", info("c2"), 100, StartFrom::Latest)
        .unwrap();
    let d = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.message.offset, 1500);
    drop(sub);
    assert_eq!(dlq_payloads(b, 0), dead);
    for node in n {
        node.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removal_deletes_only_the_removed_partition() {
    let n = cluster(3, 2, OS).await;
    let (a, b, c) = (&n[0], &n[1], &n[2]);
    let _ctl: Vec<_> = n.iter().map(auto_commit).collect();
    // p0 led by a, p1 led by b; c follows both.
    a.lead(0, 1, &[a, c], 1);
    b.lead(1, 1, &[b, c], 1);
    c.follow(0, 1, a);
    c.follow(1, 1, b);
    a.broker
        .publish_to_partition(CH, 0, msgs(0..300))
        .await
        .unwrap();
    b.broker
        .publish_to_partition(CH, 1, msgs(0..200))
        .await
        .unwrap();
    wait_sync("c caught up", || c.leo(0) == 300 && c.leo(1) == 200).await;
    let cdir = c.dir.path().join("channels").join(CH);

    // A transient `None` (leader unknown) never deletes anything.
    c.offline(0);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(c.leo(0), 300);
    c.follow(0, 1, a);

    // Stale or contradictory removals are refused.
    let e = c.mgr.remove_replica(CH, 0, 0).await.unwrap_err();
    assert!(matches!(e, ReplicationError::StaleEpoch { .. }), "{e}");
    let e = c.mgr.remove_replica(CH, 0, 1).await.unwrap_err();
    assert!(
        matches!(
            e,
            ReplicationError::Core(fluxmq_core::CoreError::InvalidArgument(_))
        ),
        "{e}"
    );
    assert_eq!(c.leo(0), 300);

    // c is removed from p0 (epoch 2) but still replicates p1.
    a.lead(0, 2, &[a], 1);
    a.mgr.isr_committed(CH, 0, 2, vec![a.id.clone()]);
    c.offline(0);
    assert!(!c.mgr.remove_replica(CH, 0, 2).await.unwrap());
    assert_eq!(c.leo(0), 0, "p0 data deleted");
    assert_eq!(c.leo(1), 200, "p1 data kept");
    assert!(cdir.join("p1").is_dir());
    // p1 replication carries on.
    b.broker
        .publish_to_partition(CH, 1, msgs(200..250))
        .await
        .unwrap();
    wait_sync("p1 still followed", || c.leo(1) == 250).await;
    assert_eq!(c.log(1), b.log(1));
    // Idempotent.
    assert!(!c.mgr.remove_replica(CH, 0, 2).await.unwrap());

    // Re-added later (epoch 3): storage comes back, bootstrapped from the leader.
    a.lead(0, 3, &[a, c], 1);
    a.mgr.isr_committed(CH, 0, 3, vec![a.id.clone()]);
    c.follow(0, 3, a);
    wait_sync("c re-replicates p0", || c.leo(0) == 300).await;
    assert_eq!(c.log(0), a.log(0));
    // Removal of the last partitions releases the channel directory.
    a.lead(0, 4, &[a], 1);
    a.mgr.isr_committed(CH, 0, 4, vec![a.id.clone()]);
    b.lead(1, 2, &[b], 1);
    b.mgr.isr_committed(CH, 1, 2, vec![b.id.clone()]);
    assert!(!c.mgr.remove_replica(CH, 0, 4).await.unwrap());
    assert!(cdir.exists());
    assert!(c.mgr.remove_replica(CH, 1, 2).await.unwrap());
    assert!(
        !cdir.exists(),
        "no partition left: channel directory removed"
    );
    assert!(
        c.broker.channel(CH).is_some(),
        "the channel stays registered"
    );
    // Leaders are unaffected.
    assert_eq!(a.leo(0), 300);
    assert_eq!(b.leo(1), 250);
    for node in n {
        node.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catch_up_throttle_limits_only_out_of_isr_followers() {
    let n = cluster(3, 1, OS).await;
    let (a, b, c) = (&n[0], &n[1], &n[2]);
    let _ctl: Vec<_> = n.iter().map(auto_commit).collect();
    a.lead(0, 1, &[a, b], 2);
    b.follow(0, 1, a);
    // ~1.1 MB of records, replicated to b (in the ISR).
    let big = |i: usize| fluxmq_proto::model::PublishMessage {
        key: None,
        headers: Default::default(),
        payload: bytes::Bytes::from(vec![b'x' + (i % 3) as u8; 1000]),
    };
    for chunk in 0..11 {
        a.broker
            .publish_to_partition(CH, 0, (chunk * 100..chunk * 100 + 100).map(big).collect())
            .await
            .unwrap();
    }
    // c joins as a new replica under a 100 kB/s throttle.
    let rate = 100_000u64;
    for node in &n {
        node.mgr.set_catch_up_throttle(CH, 0, rate);
    }
    assert_eq!(a.mgr.catch_up_throttle(CH, 0), rate);
    a.lead(0, 2, &[a, b, c], 2);
    a.mgr
        .isr_committed(CH, 0, 2, vec![a.id.clone(), b.id.clone()]);
    b.follow(0, 2, a);
    c.follow(0, 2, a);
    let started = Instant::now();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    // In-sync b is not throttled: acked publishes (min_isr 2 → b must have them) stay fast.
    let t = Instant::now();
    for chunk in 11..13 {
        a.broker
            .publish_to_partition(CH, 0, (chunk * 100..chunk * 100 + 100).map(big).collect())
            .await
            .unwrap();
    }
    assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
    let elapsed = started.elapsed().as_secs_f64();
    let got = c.leo(0);
    // ~1050 bytes per record on disk: burst (1 s) + rate * elapsed, plus one fetch chunk.
    let budget = ((elapsed + 1.0) * rate as f64 / 1000.0) as u64 + 70;
    assert!(
        got <= budget,
        "c fetched {got} records in {elapsed:.1}s (budget {budget})"
    );
    assert!(got > 50, "c makes progress under the throttle ({got})");
    let prog = a.mgr.follower_progress(CH, 0).unwrap();
    let pc = prog.iter().find(|p| p.node_id == c.id).unwrap();
    assert!(pc.throttled && !pc.in_isr);
    assert!(pc.lag.unwrap() > 300, "{pc:?}");
    let pb = prog.iter().find(|p| p.node_id == b.id).unwrap();
    assert!(pb.in_isr && !pb.throttled);
    assert!(b.mgr.follower_progress(CH, 0).is_none(), "leader only");
    // Lifting the throttle lets c catch up and join the ISR.
    for node in &n {
        node.mgr.set_catch_up_throttle(CH, 0, 0);
    }
    wait_sync("c in ISR", || a.status(0).isr.contains(&c.id)).await;
    assert_eq!(c.log(0), a.log(0));
    let prog = a.mgr.follower_progress(CH, 0).unwrap();
    assert!(
        prog.iter()
            .all(|p| p.in_isr && !p.throttled && p.lag.is_some())
    );
    for node in n {
        node.stop().await;
    }
}
