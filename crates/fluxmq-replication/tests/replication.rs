//! Multi-broker replication tests: real brokers in temp dirs, replication managers on
//! ephemeral ports, roles set manually (the test plays the controller).

mod common;

use std::time::Duration;

use common::*;
use fluxmq_core::{CoreError, FsyncPolicy, ReplicaRole};
use fluxmq_proto::model::StartFrom;

const OS: FsyncPolicy = FsyncPolicy::OsBuffered;

fn gc() -> FsyncPolicy {
    FsyncPolicy::GroupCommit {
        interval: Duration::from_millis(2),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn followers_catch_up_and_logs_match() {
    let n = cluster(3, 2, gc()).await;
    let (a, b, c) = (&n[0], &n[1], &n[2]);
    // Data written before the followers exist must replicate too.
    a.lead(0, 1, &[a, b, c], 2);
    a.mgr.isr_committed(CH, 0, 1, vec![a.id.clone()]);
    a.lead(1, 1, &[a, b, c], 1);
    a.mgr.isr_committed(CH, 1, 1, vec![a.id.clone()]);
    // min_isr 2 with ISR {a} only after the commit above → fail fast.
    let err = a
        .broker
        .publish_to_partition(CH, 0, msgs(0..1))
        .await
        .unwrap_err();
    assert!(matches!(err, CoreError::NotEnoughReplicas { .. }), "{err}");
    a.broker
        .publish_to_partition(CH, 1, msgs(0..500))
        .await
        .unwrap();
    for f in [b, c] {
        f.follow(0, 1, a);
        f.follow(1, 1, a);
    }
    wait_sync("ISR of p0 expands", || a.status(0).isr.len() == 3).await;
    for chunk in 0..10 {
        a.broker
            .publish_to_partition(CH, 0, msgs(chunk * 100..chunk * 100 + 100))
            .await
            .unwrap();
    }
    // An acked publish is on every ISR member.
    assert!(b.leo(0) >= 1000 && c.leo(0) >= 1000);
    wait_sync("p1 catches up", || b.leo(1) == 500 && c.leo(1) == 500).await;
    for p in 0..2 {
        let want = a.log(p);
        assert_eq!(b.log(p), want, "p{p} on b");
        assert_eq!(c.log(p), want, "p{p} on c");
    }
    wait_sync("follower HW", || b.hw(0) == 1000).await;
    let st = a.mgr.status();
    assert_eq!(st.len(), 2);
    assert_eq!(st[0].leader.as_deref(), Some("n1"));
    assert_eq!(st[0].log_end_offset, 1000);
    assert_eq!(st[0].high_watermark, 1000);
    // Followers reject publishes with the leader's id and serve no consumers.
    let err = b
        .broker
        .publish_to_partition(CH, 0, msgs(0..1))
        .await
        .unwrap_err();
    match &err {
        CoreError::NotLeader { leader, .. } => assert_eq!(leader.as_deref(), Some("n1")),
        e => panic!("unexpected {e}"),
    }
    let w = fluxmq_proto::wire::WireError::from(&err);
    assert_eq!(
        (w.code, w.message.as_str()),
        (fluxmq_proto::wire::ErrorCode::NotLeader, "n1")
    );
    let mut sub = b
        .broker
        .subscribe(CH, "g", info("c"), 10, StartFrom::Earliest)
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(200), sub.next())
            .await
            .is_err()
    );
    drop(sub);
    for node in n {
        node.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_wait_for_followers_and_consumers_stay_below_hw() {
    let n = cluster(2, 1, OS).await;
    let (a, b) = (&n[0], &n[1]);
    a.lead(0, 1, &[a, b], 1);
    // b has not reported in: the HW cannot pass it (it may be elected), the ack waits.
    let a_broker = a.broker.clone();
    let publish =
        tokio::spawn(async move { a_broker.publish_to_partition(CH, 0, msgs(0..10)).await });
    let mut sub = a
        .broker
        .subscribe(CH, "g", info("c"), 100, StartFrom::Earliest)
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(!publish.is_finished(), "ack must wait for the follower");
    assert_eq!(a.leo(0), 10);
    assert_eq!(a.hw(0), 0);
    // Browsing never shows offsets above the HW (a failover may discard them).
    let id0 = fluxmq_proto::model::MessageId {
        partition: 0,
        offset: 0,
    };
    assert!(a.broker.get_message(CH, id0).unwrap().is_none());
    let q = fluxmq_proto::api::MessageQuery {
        partition: Some(0),
        offset: Some(0),
        ..Default::default()
    };
    assert!(a.broker.read_messages(CH, &q).unwrap().0.is_empty());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), sub.next())
            .await
            .is_err(),
        "no delivery above the HW"
    );
    b.follow(0, 1, a);
    let ids = publish.await.unwrap().unwrap();
    assert_eq!(ids.len(), 10);
    assert!(b.leo(0) >= 10);
    let d = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.message.offset, 0);
    assert!(a.broker.get_message(CH, id0).unwrap().is_some());
    assert_eq!(a.broker.read_messages(CH, &q).unwrap().0.len(), 10);
    drop(sub);
    for node in n {
        node.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_follower_leaves_isr_hw_waits_for_commit() {
    let n = cluster(3, 1, OS).await;
    let (a, b, c) = (&n[0], &n[1], &n[2]);
    a.lead(0, 1, &[a, b, c], 1);
    b.follow(0, 1, a);
    c.follow(0, 1, a);
    a.broker
        .publish_to_partition(CH, 0, msgs(0..10))
        .await
        .unwrap();
    let mut isr_rx = a.mgr.isr_changes();
    // c stops fetching.
    c.offline(0);
    let a_broker = a.broker.clone();
    let publish =
        tokio::spawn(async move { a_broker.publish_to_partition(CH, 0, msgs(10..20)).await });
    // Eviction after replica_lag_max is reported...
    let change = tokio::time::timeout(Duration::from_secs(5), isr_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(change.isr, vec!["n1".to_string(), "n2".to_string()]);
    assert_eq!(change.epoch, 1);
    // ...but until the controller commits it, c still counts for the HW (it is electable).
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!publish.is_finished());
    assert_eq!(a.hw(0), 10, "HW must not pass the uncommitted slow replica");
    // Stale epochs are ignored.
    a.mgr.isr_committed(CH, 0, 0, change.isr.clone());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(a.hw(0), 10);
    a.mgr.isr_committed(CH, 0, 1, change.isr.clone());
    publish.await.unwrap().unwrap();
    assert_eq!(a.hw(0), 20);
    // c comes back, catches up and rejoins (expansion is immediate).
    c.follow(0, 1, a);
    let change = tokio::time::timeout(Duration::from_secs(5), isr_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(change.isr.len(), 3);
    assert_eq!(c.log(0), a.log(0));
    for node in n {
        node.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn min_isr_enforced_and_pending_acks_fail_fast() {
    let n = cluster(2, 1, OS).await;
    let (a, b) = (&n[0], &n[1]);
    let _ctl = auto_commit(a);
    a.lead(0, 1, &[a, b], 2);
    b.follow(0, 1, a);
    a.broker
        .publish_to_partition(CH, 0, msgs(0..5))
        .await
        .unwrap();
    b.offline(0);
    let a_broker = a.broker.clone();
    let pending =
        tokio::spawn(async move { a_broker.publish_to_partition(CH, 0, msgs(5..6)).await });
    let err = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(
            err,
            CoreError::NotEnoughReplicas {
                isr: 1,
                min_isr: 2,
                ..
            }
        ),
        "{err}"
    );
    let start = std::time::Instant::now();
    let err = a
        .broker
        .publish_to_partition(CH, 0, msgs(6..7))
        .await
        .unwrap_err();
    assert!(matches!(err, CoreError::NotEnoughReplicas { .. }));
    assert!(start.elapsed() < Duration::from_millis(50), "fails fast");
    // Wire mapping: Overloaded with an explanatory message.
    let w = fluxmq_proto::wire::WireError::from(&err);
    assert_eq!(w.code, fluxmq_proto::wire::ErrorCode::Overloaded);
    b.follow(0, 1, a);
    wait_sync("ISR recovers", || a.status(0).isr.len() == 2).await;
    a.broker
        .publish_to_partition(CH, 0, msgs(7..8))
        .await
        .unwrap();
    for node in n {
        node.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_change_truncates_divergent_former_leader() {
    let n = cluster(2, 1, OS).await;
    let (a, b) = (&n[0], &n[1]);
    a.lead(0, 1, &[a, b], 1);
    b.follow(0, 1, a);
    a.broker
        .publish_to_partition(CH, 0, msgs(0..100))
        .await
        .unwrap();
    wait_sync("b caught up", || b.leo(0) == 100).await;
    // b stops following; a appends a suffix that is never acked (b is still committed).
    b.offline(0);
    let a_broker = a.broker.clone();
    let lost =
        tokio::spawn(async move { a_broker.publish_to_partition(CH, 0, msgs(1000..1020)).await });
    wait_sync("divergent suffix on a", || a.leo(0) == 120).await;
    assert_eq!(b.leo(0), 100);
    // Failover to b (epoch 2); a is demoted and follows b.
    b.lead(0, 2, &[a, b], 1);
    b.mgr
        .isr_committed(CH, 0, 2, vec![b.id.clone(), a.id.clone()]);
    a.follow(0, 2, b);
    let err = lost.await.unwrap().unwrap_err();
    assert!(matches!(err, CoreError::NotLeader { .. }), "{err}");
    // Stale roles are rejected.
    let stale = a.mgr.set_role(
        CH,
        0,
        fluxmq_replication::PartitionRole::Leader {
            epoch: 1,
            replicas: vec![a.id.clone(), b.id.clone()],
            min_isr: 1,
        },
    );
    assert!(matches!(
        stale,
        Err(fluxmq_replication::ReplicationError::StaleEpoch { got: 1, current: 2 })
    ));
    b.broker
        .publish_to_partition(CH, 0, msgs(100..150))
        .await
        .unwrap();
    wait_sync("a truncated and caught up", || a.leo(0) == 150).await;
    let log = a.log(0);
    assert_eq!(log, b.log(0));
    assert!(
        log.iter().all(|(_, _, p, _)| !p.starts_with(b"m001")),
        "divergent records removed"
    );
    assert_eq!(&log[120].2[..], b"m000120");
    // Epoch history: b recorded epoch 2 at offset 100; a adopted it.
    assert_eq!(
        a.status(0).role,
        ReplicaRole::Follower {
            leader: "n2".into()
        }
    );
    // Restart a: the history and epoch survive (persisted), stale roles still rejected.
    let mut it = n.into_iter();
    let a = it.next().unwrap();
    let b = it.next().unwrap();
    b.stop().await;
    let id = a.id.clone();
    let dir = a.stop().await;
    let a = Node::start_in(&id, dir, OS).await;
    a.broker.replica(CH, 0).unwrap();
    assert_eq!(a.broker.replica(CH, 0).unwrap().epoch(), 2);
    assert_eq!(a.broker.replica(CH, 0).unwrap().last_epoch(), Some(2));
    assert!(
        a.broker
            .publish_to_partition(CH, 0, msgs(0..1))
            .await
            .is_err(),
        "offline after restart"
    );
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_offsets_and_dlq_survive_failover() {
    let n = cluster(2, 1, OS).await;
    let (a, b) = (&n[0], &n[1]);
    a.lead(0, 1, &[a, b], 1);
    b.follow(0, 1, a);
    a.broker
        .publish_to_partition(CH, 0, msgs(0..100))
        .await
        .unwrap();
    let mut sub = a
        .broker
        .subscribe(CH, "g", info("c1"), 10, StartFrom::Earliest)
        .unwrap();
    // Ack 0..50, dead-letter 3 of them.
    let mut dead = 0;
    for _ in 0..50 {
        let d = sub.next().await.unwrap();
        if d.message.offset % 20 == 7 {
            sub.nack(d.delivery_id, "poison", false).unwrap();
            dead += 1;
        } else {
            sub.ack(&[d.delivery_id]).unwrap();
        }
    }
    assert_eq!(dead, 3);
    // One DLQ entry is discarded on the leader (removal replicates too).
    let (list, _) = a.broker.dlq_list(Some(CH), None, None, 10);
    assert_eq!(list.len(), 3);
    a.broker
        .dlq_discard(fluxmq_core::DlqSelector::Ids(vec![(CH.into(), list[0].id)]))
        .unwrap();
    // Messages 50..60 are in flight (unacked) when the leader goes away.
    let mut inflight = Vec::new();
    for _ in 0..10 {
        inflight.push(sub.next().await.unwrap());
    }
    wait_sync("DLQ replicated", || {
        b.broker.dlq_list(Some(CH), None, None, 10).0.len() == 2
    })
    .await;
    wait_sync("group replicated", || b.broker.has_group(CH, "g").unwrap()).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Failover.
    b.lead(0, 2, &[a, b], 1);
    b.mgr
        .isr_committed(CH, 0, 2, vec![a.id.clone(), b.id.clone()]);
    a.follow(0, 2, b);
    // The old leader no longer serves the partition: acks of its in-flight deliveries fail.
    assert!(sub.ack(&[inflight[0].delivery_id]).is_err());
    drop(sub);
    let mut sub = b
        .broker
        .subscribe(CH, "g", info("c2"), 100, StartFrom::Latest)
        .unwrap();
    let mut got = Vec::new();
    while got.len() < 50 {
        let d = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .unwrap()
            .unwrap();
        sub.ack(&[d.delivery_id]).unwrap();
        got.push(d.message.offset);
    }
    // Resumes after the last committed offset (50): unacked in-flight ones are redelivered,
    // nothing committed is, nothing is skipped.
    got.sort_unstable();
    assert_eq!(got, (50..100).collect::<Vec<_>>());
    let (list, _) = b.broker.dlq_list(Some(CH), None, None, 10);
    assert_eq!(list.len(), 2);
    assert!(
        list.iter()
            .all(|d| d.reason == "poison" && d.message.offset % 20 == 7)
    );
    // New DLQ ids on the new leader never collide with replicated ones.
    let max_old = list.iter().map(|d| d.id).max().unwrap();
    b.broker
        .publish_to_partition(CH, 0, msgs(100..101))
        .await
        .unwrap();
    let d = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .unwrap()
        .unwrap();
    sub.nack(d.delivery_id, "boom", false).unwrap();
    let (list, _) = b.broker.dlq_list(Some(CH), Some("boom"), None, 10);
    assert!(list[0].id > max_old);
    // ...and the old leader (now follower) converges on the new leader's DLQ.
    wait_sync("dlq on a", || {
        a.broker.dlq_list(Some(CH), None, None, 10).0.len() == 3
    })
    .await;
    // Group deletion replicates with the offsets snapshot.
    drop(sub);
    b.broker.delete_group(CH, "g").unwrap();
    wait_sync("group deleted on follower", || {
        !a.broker.has_group(CH, "g").unwrap()
    })
    .await;
    for node in n {
        node.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_crash_and_restart_mid_stream() {
    let mut n = cluster(3, 1, gc()).await;
    let _ctl = auto_commit(&n[0]);
    {
        let (a, b, c) = (&n[0], &n[1], &n[2]);
        a.lead(0, 1, &[a, b, c], 2);
        b.follow(0, 1, a);
        c.follow(0, 1, a);
    }
    let a_broker = n[0].broker.clone();
    let producer = tokio::spawn(async move {
        let mut acked = 0usize;
        for i in 0..400 {
            match a_broker
                .publish_to_partition(CH, 0, msgs(i * 5..i * 5 + 5))
                .await
            {
                Ok(_) => acked += 5,
                Err(e) => panic!("publish failed with ISR >= 2: {e}"),
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        acked
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    // Crash c (stop without any handoff) and restart it on the same data dir.
    let c = n.pop().unwrap();
    let dir = c.stop().await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let c = Node::start_in("n3", dir, gc()).await;
    n.push(c);
    wire(&n);
    n[2].follow(0, 1, &n[0]);
    let acked = producer.await.unwrap();
    assert_eq!(acked, 2000);
    wait_sync("c caught up", || n[2].leo(0) == 2000).await;
    wait_sync("c back in ISR", || n[0].status(0).isr.len() == 3).await;
    let want = n[0].log(0);
    assert_eq!(n[1].log(0), want);
    assert_eq!(n[2].log(0), want);
    for node in n {
        node.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_shrink_unblocks_publish_after_follower_connection_dies() {
    let mut n = cluster(3, 1, OS).await;
    {
        let (a, b, c) = (&n[0], &n[1], &n[2]);
        a.lead(0, 1, &[a, b, c], 2);
        b.follow(0, 1, a);
        c.follow(0, 1, a);
        a.broker
            .publish_to_partition(CH, 0, msgs(0..10))
            .await
            .unwrap();
    }
    // c's replication dies (connection dropped, no more fetches); its broker keeps running.
    let c = n.pop().unwrap();
    c.mgr.shutdown().await;
    let a_broker = n[0].broker.clone();
    let start = std::time::Instant::now();
    let pending =
        tokio::spawn(async move { a_broker.publish_to_partition(CH, 0, msgs(10..20)).await });
    // The leader's write path is not blocked: appends proceed, only the ack waits.
    wait_sync("appended", || n[0].leo(0) == 20).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!pending.is_finished());
    // The controller marks c down and commits the shrink before the leader evicts it itself
    // (replica_lag_max = 400 ms).
    n[0].mgr
        .isr_committed(CH, 0, 1, vec!["n1".into(), "n2".into()]);
    let r = tokio::time::timeout(Duration::from_millis(200), pending).await;
    let ids = r
        .expect("publish completes promptly after the committed shrink")
        .unwrap()
        .unwrap();
    assert_eq!(ids.len(), 10);
    assert!(
        start.elapsed() < Duration::from_millis(350),
        "{:?}",
        start.elapsed()
    );
    assert_eq!(n[0].status(0).isr, vec!["n1".to_string(), "n2".to_string()]);
    c.broker.shutdown().await;
    for node in n {
        node.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_ack_wait_is_bounded() {
    let a = Node::start_with("n1", tempfile::tempdir().unwrap(), OS, |c| {
        c.publish_ack_timeout = Duration::from_millis(300);
    })
    .await;
    let b = Node::start("n2", OS).await;
    let nodes = [a, b];
    wire(&nodes);
    for node in &nodes {
        node.broker.create_channel(channel(1)).unwrap();
    }
    let (a, b) = (&nodes[0], &nodes[1]);
    // b never fetches and nobody commits a shrink: the ack must not hang forever.
    a.lead(0, 1, &[a, b], 1);
    let start = std::time::Instant::now();
    let err = a
        .broker
        .publish_to_partition(CH, 0, msgs(0..1))
        .await
        .unwrap_err();
    assert!(matches!(err, CoreError::AckTimeout { .. }), "{err}");
    assert!(
        start.elapsed() >= Duration::from_millis(300) && start.elapsed() < Duration::from_secs(2)
    );
    assert_eq!(
        fluxmq_proto::wire::WireError::from(&err).code,
        fluxmq_proto::wire::ErrorCode::Overloaded
    );
    for node in nodes {
        node.stop().await;
    }
}

/// Regression (rolling-upgrade hang): an ISR expansion whose metadata write is lost must
/// reach the controller anyway. The leader's own ISR already holds the follower, so nothing
/// changes locally any more; before the fix the expansion was never reported again and the
/// controller kept the shrunken ISR forever (min ISR at risk, a node looking unhealthy).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_isr_report_is_sent_again_until_committed() {
    let n = cluster(3, 1, gc()).await;
    let (a, b, c) = (&n[0], &n[1], &n[2]);
    let mut rx = a.mgr.isr_changes();
    a.lead(0, 1, &[a, b, c], 2);
    a.mgr.isr_committed(CH, 0, 1, vec![a.id.clone()]);
    b.follow(0, 1, a);
    c.follow(0, 1, a);
    wait_sync("ISR of p0 expands locally", || a.status(0).isr.len() == 3).await;
    // The controller "loses" every report: drain them without committing.
    let t0 = tokio::time::Instant::now();
    while tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .is_ok()
    {
        assert!(t0.elapsed() < Duration::from_secs(10), "reports never stop");
    }
    // Re-sent (ISR_RESEND = 2 s) with the full ISR, without any local change.
    let again = tokio::time::timeout(Duration::from_secs(6), rx.recv())
        .await
        .expect("the uncommitted ISR is reported again")
        .unwrap();
    assert_eq!((again.partition, again.epoch), (0, 1));
    let mut isr = again.isr.clone();
    isr.sort();
    assert_eq!(isr, vec!["n1", "n2", "n3"]);
    // Once committed, the re-sends stop.
    a.mgr.isr_committed(CH, 0, 1, again.isr);
    while let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {}
    assert!(
        tokio::time::timeout(Duration::from_secs(4), rx.recv())
            .await
            .is_err(),
        "no re-send after the commit"
    );
}

/// Regression (ISR flap): a committed ISR that drops a caught-up follower (e.g. the
/// reconciler applying the revision from before the leader's last expansion) must not make
/// the follower rejoin on its very next fetch, or every stale revision flips it out again
/// (seen for 90 s at ~20 metadata writes/s). It rejoins after the hold-off (1 s).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_follower_rejoins_only_after_holdoff() {
    let n = cluster(3, 1, gc()).await;
    let (a, b, c) = (&n[0], &n[1], &n[2]);
    a.lead(0, 1, &[a, b, c], 2);
    a.mgr.isr_committed(CH, 0, 1, vec![a.id.clone()]);
    b.follow(0, 1, a);
    c.follow(0, 1, a);
    wait_sync("ISR of p0 expands", || a.status(0).isr.len() == 3).await;
    let mut rx = a.mgr.isr_changes();
    // A stale committed value without b: b leaves (shrinks are always honoured).
    a.mgr
        .isr_committed(CH, 0, 1, vec![a.id.clone(), c.id.clone()]);
    let dropped = std::time::Instant::now();
    assert_eq!(a.status(0).isr, vec!["n1".to_string(), "n3".to_string()]);
    // b keeps fetching (50 ms long polls) but stays out for the hold-off.
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(a.status(0).isr.len(), 2, "rejoined during the hold-off");
    wait_sync("b rejoins after the hold-off", || {
        a.status(0).isr.len() == 3
    })
    .await;
    assert!(dropped.elapsed() >= Duration::from_millis(1000));
    // Reports in that window: the shrink and one expansion, no flapping.
    let mut reports = Vec::new();
    while let Ok(Ok(u)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
        reports.push(u.isr.len());
    }
    assert_eq!(reports, vec![2, 3], "{reports:?}");
    // Acks keep working with the rejoined follower.
    a.broker
        .publish_to_partition(CH, 0, msgs(0..10))
        .await
        .unwrap();
}
