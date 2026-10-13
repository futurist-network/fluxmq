//! Regression test (deterministic schedule): a publish admitted — and past the writer's epoch
//! fence — while this node led the partition, but not yet committed when it is demoted, must
//! not survive follower reconciliation. Before the fix the follower read its LEO *before* that
//! commit, concluded "nothing to truncate", and the stale record was then published at an
//! offset the new leader fills with different data (replica divergence; seen once as a
//! `failover_property` failure under heavy load).

mod common;

use std::time::Duration;

use common::*;
use fluxmq_core::{Broker, CoreError, LeaderParams};
use fluxmq_proto::model::ChannelMode;

async fn until(what: &str, mut f: impl FnMut() -> bool) {
    for _ in 0..500 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("timed out: {what}");
}

/// Leader at epoch 1 with 10 committed records and 5 more appended past the epoch fence but
/// uncommitted; the writer is parked before committing them. Returns the release handle.
async fn stale_append_scenario(
    b: &Broker,
) -> (
    std::sync::mpsc::Sender<()>,
    tokio::task::JoinHandle<fluxmq_core::Result<Vec<fluxmq_proto::model::MessageId>>>,
) {
    b.enable_replication();
    b.create_channel(channel("c", ChannelMode::Stream, 1))
        .unwrap();
    let h = b.replica("c", 0).unwrap();
    h.become_leader(LeaderParams {
        epoch: 1,
        replicas: vec!["test-node".into()],
        min_isr: 1,
        replica_lag_max: Duration::from_secs(10),
        ack_timeout: Duration::from_secs(10),
    })
    .unwrap();
    let batch = |n: usize| {
        (0..n)
            .map(|i| msg(None, &format!("m{i}")))
            .collect::<Vec<_>>()
    };
    b.publish_to_partition("c", 0, batch(10)).await.unwrap();
    // Park the writer, queue the publish behind it, then a second pause behind that.
    let pause1 = b.debug_pause_writer("c").await.unwrap();
    until("writer parked", || {
        b.debug_writer_queue_len("c").unwrap() == 0
    })
    .await;
    let b2 = b.clone();
    let publish = tokio::spawn(async move { b2.publish_to_partition("c", 0, batch(5)).await });
    until("publish queued", || {
        b.debug_writer_queue_len("c").unwrap() == 1
    })
    .await;
    let pause2 = b.debug_pause_writer("c").await.unwrap();
    drop(pause1);
    // The writer took the publish (fence passed: still leader of epoch 1) and parked on
    // pause2 before committing it.
    until("append buffered", || {
        b.debug_writer_queue_len("c").unwrap() == 0
    })
    .await;
    assert_eq!(h.log_end_offset(), 10, "appended but not committed");
    (pause2, publish)
}

#[tokio::test]
async fn reconciliation_barrier_removes_append_admitted_before_demotion() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    let (pause, publish) = stale_append_scenario(&b).await;
    let h = b.replica("c", 0).unwrap();
    // Demotion; the new leader's end offset for epoch 1 is 10.
    h.become_follower(2, "other").unwrap();
    let observed = h.log_end_offset();
    assert_eq!(observed, 10);
    let to = h.truncation_offset(Some(1), 10);
    assert_eq!(to, 10, "nothing seems to need truncating at this point");
    // The fetcher always issues the barrier; release the writer once it is queued.
    let h2 = h.clone();
    let trunc = tokio::spawn(async move { h2.truncate(2, to).await });
    until("barrier queued", || {
        b.debug_writer_queue_len("c").unwrap() == 1
    })
    .await;
    drop(pause);
    assert_eq!(trunc.await.unwrap().unwrap(), 10);
    assert_eq!(h.log_end_offset(), 10, "stale append removed");
    let err = publish.await.unwrap().unwrap_err();
    assert!(matches!(err, CoreError::NotLeader { .. }), "{err}");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(h.log_end_offset(), 10);
    b.shutdown().await;
}

/// Documents the hazard the barrier closes: deciding from the LEO observed before the drain
/// (the old `if to < leo { truncate }`) leaves the stale records in the log.
#[tokio::test]
async fn without_barrier_the_stale_append_lands_after_reconciliation() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    let (pause, publish) = stale_append_scenario(&b).await;
    let h = b.replica("c", 0).unwrap();
    h.become_follower(2, "other").unwrap();
    let (observed, to) = (h.log_end_offset(), h.truncation_offset(Some(1), 10));
    assert!(to >= observed, "old logic would skip truncation here");
    drop(pause);
    let _ = publish.await.unwrap();
    until("stale append committed", || h.log_end_offset() == 15).await;
    b.shutdown().await;
}
