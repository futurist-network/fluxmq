//! v0.4 replica moves, storage side: a partition log that starts at a non-zero offset
//! (new-replica bootstrap after the leader's retention), and removed-replica cleanup
//! (`ReplicaHandle::remove_local_data`, channel storage release / re-creation).

mod common;

use std::path::Path;
use std::time::Duration;

use common::*;
use fluxmq_core::{Broker, CoreError, FetchRequest, LeaderParams, ReplicaHandle, StateCursor};
use fluxmq_proto::model::{ChannelMode, MessageId, StartFrom};

const NODE: &str = "test-node";

fn lead_params(epoch: u64) -> LeaderParams {
    LeaderParams {
        epoch,
        replicas: vec![NODE.into()],
        min_isr: 1,
        replica_lag_max: Duration::from_secs(10),
        ack_timeout: Duration::from_secs(10),
    }
}

async fn open_with(dir: &Path, segment_bytes: u64) -> Broker {
    let mut c = config(dir);
    c.segment_bytes = segment_bytes;
    Broker::open(c).await.expect("open broker")
}

fn get(b: &Broker, ch: &str, p: u32, offset: u64) -> Option<String> {
    b.debug_get_uncapped(
        ch,
        MessageId {
            partition: p,
            offset,
        },
    )
    .unwrap()
    .map(|m| String::from_utf8_lossy(&m.payload).into_owned())
}

fn low_high(b: &Broker, ch: &str, p: u32) -> (u64, u64) {
    let st = b.stats();
    let c = st
        .channels
        .iter()
        .find(|c| c.config.name == ch)
        .expect("channel in stats");
    let ps = &c.partitions[p as usize];
    (ps.low_offset, ps.high_offset)
}

/// Copies `[from, to)` of the leader's partition into the follower (as the fetcher does).
async fn copy(leader: &ReplicaHandle, follower: &ReplicaHandle, epoch: u64, from: u64, to: u64) {
    let mut at = from;
    while at < to {
        let h = leader.clone();
        let data = tokio::task::spawn_blocking(move || {
            h.read_for_fetch(&FetchRequest {
                replica: "f".into(),
                epoch: 1,
                fetch_offset: at,
                durable_offset: at,
                max_bytes: 700,
                state: StateCursor::default(),
            })
        })
        .await
        .unwrap()
        .unwrap();
        let chunk = data.records.expect("records");
        at = follower
            .append(epoch, &chunk, data.high_watermark, &data.epochs)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn partition_log_starting_at_a_nonzero_offset() {
    // Leader with 1200 records.
    let ld = tempfile::tempdir().unwrap();
    let leader = open(ld.path()).await;
    leader.enable_replication();
    leader
        .create_channel(channel("c", ChannelMode::Stream, 1))
        .unwrap();
    let lh = leader.replica("c", 0).unwrap();
    lh.become_leader(lead_params(1)).unwrap();
    let batch: Vec<_> = (0..1200).map(|i| msg(None, &format!("m{i}"))).collect();
    leader.publish_to_partition("c", 0, batch).await.unwrap();

    // Follower bootstraps at 1000 (the leader's retention removed the rest, say).
    let fd = tempfile::tempdir().unwrap();
    let f = open_with(fd.path(), 4096).await;
    f.enable_replication();
    f.create_channel(channel("c", ChannelMode::Stream, 1))
        .unwrap();
    let fh = f.replica("c", 0).unwrap();
    fh.become_follower(1, "leader").unwrap();
    assert_eq!(fh.reset_to(1, 1000).await.unwrap(), 1000);
    assert_eq!(low_high(&f, "c", 0), (1000, 1000));
    assert_eq!(fh.log_start_offset(), 1000);

    // Restart with the empty log at base 1000: it stays at 1000.
    f.shutdown().await;
    drop(fh);
    let f = open_with(fd.path(), 4096).await;
    f.enable_replication();
    let fh = f.replica("c", 0).unwrap();
    assert_eq!(low_high(&f, "c", 0), (1000, 1000));
    fh.become_follower(2, "leader").unwrap();
    copy(&lh, &fh, 2, 1000, 1200).await;
    assert_eq!(low_high(&f, "c", 0), (1000, 1200));
    assert!(
        f.stats().channels[0].partitions[0].segments > 1,
        "rolled segments"
    );
    assert_eq!(get(&f, "c", 0, 999), None);
    assert_eq!(get(&f, "c", 0, 1000).as_deref(), Some("m1000"));
    assert_eq!(get(&f, "c", 0, 1199).as_deref(), Some("m1199"));
    // The epoch history learned from the leader covers what we hold.
    assert_eq!(fh.last_epoch(), Some(1));

    // Recovery of a multi-segment log whose first segment starts at 1000.
    f.shutdown().await;
    drop(fh);
    let f = open_with(fd.path(), 4096).await;
    f.enable_replication();
    let fh = f.replica("c", 0).unwrap();
    assert_eq!(low_high(&f, "c", 0), (1000, 1200));
    assert_eq!(get(&f, "c", 0, 1000).as_deref(), Some("m1000"));

    // Promote it; a group starting at "earliest" starts at the log start (1000).
    fh.become_leader(lead_params(3)).unwrap();
    let mut sub = f
        .subscribe("c", "g", info("c1", 1), 10, StartFrom::Earliest)
        .unwrap();
    let d = next(&mut sub).await;
    assert_eq!(d.message.offset, 1000);
    assert_eq!(payload(&d), "m1000");
    sub.ack(&[d.delivery_id]).unwrap();
    drop(sub);
    // Message reads below the log start return nothing, at the start the first record.
    let (msgs, _, _) = f
        .read_messages(
            "c",
            &fluxmq_proto::api::MessageQuery {
                partition: Some(0),
                offset: Some(0),
                limit: Some(5),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(msgs.first().map(|m| m.offset), Some(1000));

    // Retention on a log with a non-zero base removes whole old segments only.
    let upd = fluxmq_proto::api::UpdateChannelRequest {
        retention: Some(fluxmq_proto::model::RetentionPolicy {
            max_age_secs: None,
            max_bytes: Some(3000),
        }),
        ..Default::default()
    };
    f.update_channel("c", upd).unwrap();
    f.enforce_retention().await;
    let (low, high) = low_high(&f, "c", 0);
    assert!(low > 1000 && low < 1200, "low {low}");
    assert_eq!(high, 1200);
    assert_eq!(
        get(&f, "c", 0, low).as_deref(),
        Some(format!("m{low}").as_str())
    );
    assert_eq!(get(&f, "c", 0, low - 1), None);
    f.shutdown().await;
    leader.shutdown().await;
}

/// Two partitions led locally; p0 gets data, a group position and a DLQ entry.
async fn two_partitions(b: &Broker) {
    b.enable_replication();
    b.create_channel(channel("c", ChannelMode::Stream, 2))
        .unwrap();
    for p in 0..2 {
        b.replica("c", p)
            .unwrap()
            .become_leader(lead_params(1))
            .unwrap();
    }
    let batch: Vec<_> = (0..50).map(|i| msg(None, &format!("a{i}"))).collect();
    b.publish_to_partition("c", 0, batch).await.unwrap();
    let batch: Vec<_> = (0..10).map(|i| msg(None, &format!("b{i}"))).collect();
    b.publish_to_partition("c", 1, batch).await.unwrap();
    let mut sub = b
        .subscribe("c", "g", info("c1", 1), 100, StartFrom::Earliest)
        .unwrap();
    let mut seen = 0;
    while seen < 60 {
        let d = next(&mut sub).await;
        if d.message.partition == 0 && d.message.offset == 3 {
            sub.nack(d.delivery_id, "poison", false).unwrap();
        } else {
            sub.ack(&[d.delivery_id]).unwrap();
        }
        seen += 1;
    }
    drop(sub);
    assert_eq!(b.dlq_list(Some("c"), None, None, 10).0.len(), 1);
}

#[tokio::test]
async fn removed_replica_data_is_deleted_and_channel_storage_released() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    two_partitions(&b).await;
    let (h0, h1) = (b.replica("c", 0).unwrap(), b.replica("c", 1).unwrap());
    let cdir = dir.path().join("channels").join("c");

    // Still a replica at that epoch / stale epoch: refused, nothing deleted.
    let e = h0.remove_local_data(1).await.unwrap_err();
    assert!(matches!(e, CoreError::InvalidArgument(_)), "{e}");
    let e = h0.remove_local_data(0).await.unwrap_err();
    assert!(matches!(e, CoreError::StaleEpoch { .. }), "{e}");
    assert_eq!(h0.log_end_offset(), 50);

    // Removing p0 deletes only p0: p1 is still replicated here.
    assert!(!h0.remove_local_data(2).await.unwrap());
    assert_eq!((h0.log_start_offset(), h0.log_end_offset()), (0, 0));
    assert_eq!(h0.last_epoch(), None);
    assert_eq!(h0.epoch(), 2, "the epoch stays as a fence");
    assert!(h0.group_offsets().is_empty());
    assert_eq!(h1.log_end_offset(), 10);
    assert_eq!(h1.group_offsets(), vec![("g".to_string(), 10)]);
    assert_eq!(b.dlq_list(Some("c"), None, None, 10).0.len(), 0);
    let p0_bytes: u64 = std::fs::read_dir(cdir.join("p0"))
        .unwrap()
        .map(|e| e.unwrap().metadata().unwrap().len())
        .sum();
    assert!(p0_bytes < 200, "p0 data deleted ({p0_bytes} bytes left)");
    assert!(cdir.join("p1").is_dir());
    assert!(!h0.storage_released());
    // Idempotent.
    assert!(!h0.remove_local_data(2).await.unwrap());

    // Removing the last partition releases the channel directory; the channel stays.
    assert!(h1.remove_local_data(2).await.unwrap());
    assert!(!cdir.exists(), "channel directory removed");
    assert!(h1.storage_released());
    assert!(b.channel("c").is_some());
    assert_eq!(h1.log_end_offset(), 0);
    // Metadata writes do not recreate it.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!cdir.exists());

    // A new role + write re-creates the storage (config first), and it survives a restart.
    h0.become_leader(lead_params(3)).unwrap();
    b.publish_to_partition("c", 0, vec![msg(None, "again")])
        .await
        .unwrap();
    assert!(cdir.join("channel.json").is_file());
    assert!(cdir.join("p0").is_dir() && cdir.join("p1").is_dir());
    assert!(!h0.storage_released());
    b.shutdown().await;
    drop((h0, h1));
    let b = open(dir.path()).await;
    assert_eq!(get(&b, "c", 0, 0).as_deref(), Some("again"));
    b.shutdown().await;
}

#[tokio::test]
async fn released_channel_is_gone_after_restart_and_can_be_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.enable_replication();
    b.create_channel(channel("c", ChannelMode::Stream, 2))
        .unwrap();
    // A node that never held any partition: the skeleton goes on the first removal.
    let h = b.replica("c", 1).unwrap();
    assert!(h.remove_local_data(1).await.unwrap());
    // Config updates and deletion work without a directory.
    let upd = fluxmq_proto::api::UpdateChannelRequest {
        dlq_enabled: Some(false),
        ..Default::default()
    };
    b.update_channel("c", upd).unwrap();
    assert!(!dir.path().join("channels/c").exists());
    b.shutdown().await;
    drop(h);
    let b = open(dir.path()).await;
    assert!(b.channel("c").is_none(), "nothing to recover");
    b.create_channel(channel("c", ChannelMode::Stream, 1))
        .unwrap();
    b.enable_replication();
    let h = b.replica("c", 0).unwrap();
    assert!(h.remove_local_data(1).await.unwrap());
    b.delete_channel("c").unwrap();
    assert!(b.channel("c").is_none());
    b.shutdown().await;
}

#[tokio::test]
async fn purge_is_fenced_by_a_role_set_meanwhile() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    two_partitions(&b).await;
    let h0 = b.replica("c", 0).unwrap();
    h0.become_offline(1).unwrap();
    // Park the writer so the purge queues behind it, then re-add the replica.
    let pause = b.debug_pause_writer("c").await.unwrap();
    let h = h0.clone();
    let removal = tokio::spawn(async move { h.remove_local_data(2).await });
    for _ in 0..500 {
        if b.debug_writer_queue_len("c").unwrap() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(b.debug_writer_queue_len("c").unwrap(), 1, "purge queued");
    h0.become_follower(3, "other").unwrap();
    drop(pause);
    let e = removal.await.unwrap().unwrap_err();
    assert!(matches!(e, CoreError::StaleEpoch { .. }), "{e}");
    assert_eq!(h0.log_end_offset(), 50, "data kept");
    assert_eq!(b.dlq_list(Some("c"), None, None, 10).0.len(), 1);
    b.shutdown().await;
}
