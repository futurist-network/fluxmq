//! Backup cut (pins, consistency under concurrent publishing) and the offline restore writer.

mod common;

use std::io::Write;

use common::*;
use fluxmq_core::Broker;
use fluxmq_core::backup::{ChannelCut, RestoreWriter};
use fluxmq_proto::api::MessageQuery;
use fluxmq_proto::model::{ChannelMode, RetentionPolicy};

fn small_segments(dir: &std::path::Path) -> fluxmq_core::BrokerConfig {
    let mut c = config(dir);
    // Batch envelopes store these tiny messages in ~10 bytes each.
    c.segment_bytes = 256;
    c
}

/// Lays a cut down with the restore writer (copying bytes through `SegmentCut::read_at`).
fn lay_down(cut: &ChannelCut, dir: &std::path::Path) {
    let w = RestoreWriter::new(dir).unwrap().without_sync();
    w.create_channel(&cut.config).unwrap();
    for p in &cut.partitions {
        if p.segments.is_empty() {
            w.empty_partition(&cut.config.name, p.partition, p.end)
                .unwrap();
        }
        for s in &p.segments {
            let mut f = w
                .create_segment(&cut.config.name, p.partition, s.base_offset)
                .unwrap();
            let mut pos = 0;
            let mut buf = vec![0u8; 777];
            loop {
                let n = s.read_at(&mut buf, pos).unwrap();
                if n == 0 {
                    break;
                }
                f.write_all(&buf[..n]).unwrap();
                pos += n as u64;
            }
            assert_eq!(pos, s.len);
            w.finish_segment(
                &cut.config.name,
                p.partition,
                s.base_offset,
                s.end_offset,
                Some(&s.index),
            )
            .unwrap();
        }
    }
    w.write_offsets(&cut.config.name, &cut.group_offsets)
        .unwrap();
    w.write_dlq(&cut.config.name, &cut.dlq).unwrap();
    w.finish().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cut_under_concurrent_publishing_restores_exact_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let b = Broker::open(small_segments(&dir.path().join("src")))
        .await
        .unwrap();
    b.create_channel(channel("orders", ChannelMode::Stream, 2))
        .unwrap();
    publish_keyed(&b, "orders", 300, 7).await;
    // Publish continuously while cutting.
    let pb = b.clone();
    let publisher = tokio::spawn(async move {
        for i in 0..200 {
            let _ = pb
                .publish(
                    "orders",
                    vec![msg(Some(&format!("k{i}")), &format!("live{i}"))],
                )
                .await;
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let bc = b.clone();
    let cut = tokio::task::spawn_blocking(move || bc.backup_cut("orders").unwrap())
        .await
        .unwrap();
    publisher.await.unwrap();
    assert_eq!(cut.partitions.len(), 2);
    let ends: Vec<u64> = cut.partitions.iter().map(|p| p.end).collect();
    for p in &cut.partitions {
        assert_eq!(p.low, 0);
        assert!(p.segments.len() > 1, "small segments roll");
        // Contiguous chain ending at the cut.
        let mut next = p.low;
        for s in &p.segments {
            assert_eq!(s.base_offset, next);
            next = s.end_offset;
        }
        assert_eq!(next, p.end);
    }
    let restored = dir.path().join("restored");
    lay_down(&cut, &restored);
    drop(cut);
    let r = open(&restored).await;
    for (p, end) in ends.iter().enumerate() {
        let (msgs, _, _) = r
            .read_messages(
                "orders",
                &MessageQuery {
                    partition: Some(p as u32),
                    limit: Some(10_000),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(msgs.len() as u64, *end, "partition {p}");
        let (orig, _, _) = b
            .read_messages(
                "orders",
                &MessageQuery {
                    partition: Some(p as u32),
                    limit: Some(10_000),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut orig: Vec<_> = orig.into_iter().filter(|m| m.offset < *end).collect();
        let mut msgs = msgs;
        orig.sort_by_key(|m| m.offset);
        msgs.sort_by_key(|m| m.offset);
        assert_eq!(msgs, orig);
    }
    // New publishes continue after the restored end.
    let ids = r
        .publish_to_partition("orders", 0, vec![msg(None, "after")])
        .await
        .unwrap();
    assert_eq!(ids[0].offset, ends[0]);
    r.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn pinned_segments_survive_retention_until_the_cut_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let b = Broker::open(small_segments(dir.path())).await.unwrap();
    let mut c = channel("logs", ChannelMode::Stream, 1);
    c.retention = RetentionPolicy {
        max_age_secs: None,
        max_bytes: Some(1),
    };
    b.create_channel(c).unwrap();
    publish_keyed(&b, "logs", 200, 1).await;
    let segs = |b: &Broker| b.stats().channels[0].partitions[0].segments;
    let before = segs(&b);
    assert!(before > 3);
    let bc = b.clone();
    let cut = tokio::task::spawn_blocking(move || bc.backup_cut("logs").unwrap())
        .await
        .unwrap();
    b.enforce_retention().await;
    assert_eq!(segs(&b), before, "pinned segments are kept");
    // The cut still reads its first segment.
    let first = &cut.partitions[0].segments[0];
    let mut buf = vec![0u8; 16];
    assert_eq!(first.read_at(&mut buf, 0).unwrap(), 16);
    drop(cut);
    b.enforce_retention().await;
    assert_eq!(segs(&b), 1, "retention resumes after the pins are released");
    b.shutdown().await;
}

#[tokio::test]
async fn restore_writer_refuses_non_empty_dir_and_verifies_segments() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("x"), b"x").unwrap();
    assert!(RestoreWriter::new(dir.path()).is_err());
    let d2 = dir.path().join("fresh");
    let w = RestoreWriter::new(&d2).unwrap().without_sync();
    w.create_channel(&channel("c", ChannelMode::Stream, 1))
        .unwrap();
    let mut f = w.create_segment("c", 0, 0).unwrap();
    f.write_all(b"garbage that is not a record").unwrap();
    drop(f);
    assert!(w.finish_segment("c", 0, 0, 1, None).is_err());
}
