//! Restart recovery, reads, replay, retention, tail and stats.

mod common;

use std::time::Duration;

use common::*;
use fluxmq_core::{Broker, BrokerEvent, CoreError, FsyncPolicy};
use fluxmq_proto::api::{MessageQuery, UpdateChannelRequest};
use fluxmq_proto::model::{ChannelMode, MessageId, RetentionPolicy, StartFrom};

#[tokio::test]
async fn restart_recovers_messages_offsets_dlq_and_configs() {
    let dir = tempfile::tempdir().unwrap();
    let dlq_id;
    {
        let b = open(dir.path()).await;
        b.create_channel(channel("orders", ChannelMode::Stream, 2))
            .unwrap();
        b.create_channel(channel("jobs", ChannelMode::Queue, 1))
            .unwrap();
        b.update_channel(
            "jobs",
            UpdateChannelRequest {
                dlq_enabled: Some(true),
                retry: None,
                retention: None,
            },
        )
        .unwrap();
        publish_keyed(&b, "orders", 20, 4).await;
        publish_keyed(&b, "jobs", 10, 1).await;
        let mut sub = b
            .subscribe("jobs", "workers", info("w", 1), 100, StartFrom::Earliest)
            .unwrap();
        // Ack 0..5, dead-letter 5, leave 6..9 unacked/undelivered.
        for i in 0..6 {
            let d = next(&mut sub).await;
            assert_eq!(d.message.offset, i);
            if i == 5 {
                sub.nack(d.delivery_id, "poison", false).unwrap();
            } else {
                sub.ack(&[d.delivery_id]).unwrap();
            }
        }
        let d6 = next(&mut sub).await; // in flight at shutdown
        assert_eq!(d6.message.offset, 6);
        dlq_id = b.dlq_list(None, None, None, 10).0[0].id;
        drop(sub);
        b.shutdown().await;
        assert!(matches!(
            b.publish("orders", vec![msg(None, "late")]).await,
            Err(CoreError::ShuttingDown)
        ));
    }
    let b = open(dir.path()).await;
    let mut names: Vec<_> = b.channels().into_iter().map(|c| c.name).collect();
    names.sort();
    assert_eq!(names, vec!["jobs", "orders"]);
    assert_eq!(b.node_id(), "test-node");
    let (msgs, _, _) = b
        .read_messages(
            "orders",
            &MessageQuery {
                limit: Some(1000),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(msgs.len(), 20);
    // Group resumes from its committed offset (6: lowest unacked).
    let mut sub = b
        .subscribe("jobs", "workers", info("w", 2), 100, StartFrom::Latest)
        .unwrap();
    let d = next(&mut sub).await;
    assert_eq!(d.message.offset, 6);
    // DLQ survived, ids keep increasing.
    let (dl, total) = b.dlq_list(None, None, None, 10);
    assert_eq!(
        (total, dl[0].id, dl[0].reason.as_str()),
        (1, dlq_id, "poison")
    );
    for _ in 7..10 {
        let d = next(&mut sub).await;
        sub.nack(d.delivery_id, "again", false).unwrap();
    }
    let (dl, _) = b.dlq_list(None, None, None, 10);
    assert!(dl[0].id > dlq_id);
    // Publishing continues at the right offsets.
    let ids = b
        .publish("jobs", vec![msg(None, "after-restart")])
        .await
        .unwrap();
    assert_eq!(
        ids[0],
        MessageId {
            partition: 0,
            offset: 10
        }
    );
    b.shutdown().await;
}

#[tokio::test]
async fn os_buffered_policy_works_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.fsync = FsyncPolicy::OsBuffered;
    cfg.segment_bytes = 4096;
    {
        let b = Broker::open(cfg.clone()).await.unwrap();
        b.create_channel(channel("c", ChannelMode::Stream, 1))
            .unwrap();
        for _ in 0..10 {
            publish_keyed(&b, "c", 100, 10).await;
        }
        let s = b.stats();
        assert!(s.channels[0].partitions[0].segments > 1);
        b.shutdown().await;
    }
    let b = Broker::open(cfg).await.unwrap();
    assert_eq!(b.stats().channels[0].partitions[0].high_offset, 1000);
    b.shutdown().await;
}

#[tokio::test]
async fn read_messages_query_precedence() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("m", ChannelMode::Stream, 2))
        .unwrap();
    for i in 0..20 {
        b.publish(
            "m",
            vec![msg(
                Some(if i % 2 == 0 { "even" } else { "odd" }),
                &format!("m{i}"),
            )],
        )
        .await
        .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mid = fluxmq_proto::now_ms();
    tokio::time::sleep(Duration::from_millis(5)).await;
    for i in 20..30 {
        b.publish(
            "m",
            vec![msg(
                Some(if i % 2 == 0 { "even" } else { "odd" }),
                &format!("m{i}"),
            )],
        )
        .await
        .unwrap();
    }
    let p_even = b
        .read_messages(
            "m",
            &MessageQuery {
                key: Some("even".into()),
                limit: Some(1),
                ..Default::default()
            },
        )
        .unwrap()
        .0[0]
        .partition;

    // 1. offset wins over since_ms.
    let q = MessageQuery {
        partition: Some(p_even),
        offset: Some(3),
        since_ms: Some(mid),
        limit: Some(4),
        ..Default::default()
    };
    let (m, scanned, more) = b.read_messages("m", &q).unwrap();
    assert_eq!(
        m.iter().map(|m| m.offset).collect::<Vec<_>>(),
        vec![3, 4, 5, 6]
    );
    assert!(more);
    assert_eq!(scanned, 4);

    // 2. since_ms on all partitions: exactly the 10 later messages, ascending by timestamp.
    let (m, _, more) = b
        .read_messages(
            "m",
            &MessageQuery {
                since_ms: Some(mid),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(m.len(), 10);
    assert!(!more);
    assert!(m.iter().all(|m| m.timestamp_ms >= mid));
    assert!(m.windows(2).all(|w| w[0].timestamp_ms <= w[1].timestamp_ms));
    let (m, _, more) = b
        .read_messages(
            "m",
            &MessageQuery {
                since_ms: Some(mid),
                limit: Some(3),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(m.len(), 3);
    assert!(more);
    assert_eq!(m[0].payload, "m20");

    // 3. newest N.
    let (m, _, more) = b
        .read_messages(
            "m",
            &MessageQuery {
                limit: Some(5),
                ..Default::default()
            },
        )
        .unwrap();
    let payloads: Vec<_> = m
        .iter()
        .map(|m| String::from_utf8_lossy(&m.payload).into_owned())
        .collect();
    assert_eq!(payloads, vec!["m25", "m26", "m27", "m28", "m29"]);
    assert!(more);
    // Newest with key filter and until_ms.
    let q = MessageQuery {
        key: Some("odd".into()),
        until_ms: Some(mid),
        limit: Some(3),
        ..Default::default()
    };
    let (m, _, _) = b.read_messages("m", &q).unwrap();
    let payloads: Vec<_> = m
        .iter()
        .map(|m| String::from_utf8_lossy(&m.payload).into_owned())
        .collect();
    assert_eq!(payloads, vec!["m15", "m17", "m19"]);
    // A key search only scans the key's partition.
    let (m, scanned, _) = b
        .read_messages(
            "m",
            &MessageQuery {
                key: Some("even".into()),
                limit: Some(1000),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(m.len(), 15);
    let in_partition = b.stats().channels[0].partitions[p_even as usize].high_offset;
    assert_eq!(scanned, in_partition);
    // Everything, no more.
    let (m, _, more) = b
        .read_messages(
            "m",
            &MessageQuery {
                limit: Some(1000),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!((m.len(), more), (30, false));
    // Bad partition.
    assert!(
        b.read_messages(
            "m",
            &MessageQuery {
                partition: Some(9),
                ..Default::default()
            }
        )
        .is_err()
    );
    b.shutdown().await;
}

#[tokio::test]
async fn get_message_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("src", ChannelMode::Stream, 1))
        .unwrap();
    b.create_channel(channel("dst", ChannelMode::Stream, 1))
        .unwrap();
    let mut events = b.events();
    let ids = publish_keyed(&b, "src", 3, 3).await;
    let (m, hist) = b.get_message("src", ids[1]).unwrap().unwrap();
    assert_eq!(m.payload, "m1");
    assert!(hist.is_empty());
    assert!(
        b.get_message(
            "src",
            MessageId {
                partition: 0,
                offset: 99
            }
        )
        .unwrap()
        .is_none()
    );
    let new = b.replay("src", &ids[..2], Some("dst")).await.unwrap();
    assert_eq!(new.len(), 2);
    let (m, _) = b.get_message("dst", new[1]).unwrap().unwrap();
    assert_eq!(m.headers["flux-replayed-from"], "src/0/1");
    assert_eq!(m.key.as_deref(), Some("k1"));
    let same = b.replay("src", &ids[..1], None).await.unwrap();
    assert_eq!(same[0].offset, 3);
    let mut saw = false;
    while let Ok(ev) = events.try_recv() {
        if ev
            == (BrokerEvent::MessagesReplayed {
                channel: "dst".into(),
                count: 2,
            })
        {
            saw = true;
        }
    }
    assert!(saw);
    b.shutdown().await;
}

#[tokio::test]
async fn retention_by_size_age_and_queue_consumption() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    // Batch envelopes store these tiny messages in ~10 bytes each (300 ≈ 3 KiB).
    cfg.segment_bytes = 256;
    let b = Broker::open(cfg).await.unwrap();
    let mut size = channel("size", ChannelMode::Stream, 1);
    size.retention = RetentionPolicy {
        max_age_secs: None,
        max_bytes: Some(1024),
    };
    b.create_channel(size).unwrap();
    let mut age = channel("age", ChannelMode::Stream, 1);
    age.retention = RetentionPolicy {
        max_age_secs: Some(1),
        max_bytes: None,
    };
    b.create_channel(age).unwrap();
    b.create_channel(channel("q", ChannelMode::Queue, 1))
        .unwrap();
    for ch in ["size", "age", "q"] {
        publish_keyed(&b, ch, 300, 3).await;
    }
    let mut sub = b
        .subscribe("q", "g", info("w", 1), 1000, StartFrom::Earliest)
        .unwrap();
    for _ in 0..200 {
        let d = next(&mut sub).await;
        sub.ack(&[d.delivery_id]).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(1100)).await;
    b.enforce_retention().await;
    let s = b.stats();
    let p = |n: &str| {
        s.channels
            .iter()
            .find(|c| c.config.name == n)
            .unwrap()
            .partitions[0]
            .clone()
    };
    let sz = p("size");
    assert!(
        sz.storage_bytes <= 1024 + 256,
        "size retention: {} bytes",
        sz.storage_bytes
    );
    assert!(sz.low_offset > 0);
    assert_eq!(sz.high_offset, 300);
    assert_eq!(
        p("age").segments,
        1,
        "only the active segment survives age retention"
    );
    let q = p("q");
    assert!(
        q.low_offset > 0 && q.low_offset <= 200,
        "queue consumed segments removed: low={}",
        q.low_offset
    );
    // Reads start at the new low offset.
    let (m, _, _) = b
        .read_messages(
            "size",
            &MessageQuery {
                partition: Some(0),
                offset: Some(0),
                limit: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(m[0].offset, sz.low_offset);
    // A group lagging behind retention skips to the low offset.
    let mut late = b
        .subscribe("size", "late", info("l", 2), 10, StartFrom::Earliest)
        .unwrap();
    assert!(next(&mut late).await.message.offset >= sz.low_offset);
    b.shutdown().await;
}

#[tokio::test]
async fn tail_is_sampled_and_counts_everything() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.tail_max_rate = 40;
    let b = Broker::open(cfg).await.unwrap();
    b.create_channel(channel("t", ChannelMode::Stream, 1))
        .unwrap();
    publish_keyed(&b, "t", 10, 1).await; // no receivers: nothing buffered
    let mut tail = b.tail("t").unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    publish_keyed(&b, "t", 1000, 10).await;
    let mut got = 0;
    while let Ok(Some(m)) = tokio::time::timeout(Duration::from_millis(50), tail.recv()).await {
        assert!(m.offset >= 10);
        got += 1;
    }
    assert!((1..=10).contains(&got), "sampled {got}");
    assert_eq!(tail.take_published_count(), 1000);
    assert_eq!(tail.take_published_count(), 0);
    b.shutdown().await;
}

#[tokio::test]
async fn stats_sanity_and_validation() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    assert!(matches!(
        b.create_channel(channel("bad name", ChannelMode::Stream, 1)),
        Err(CoreError::InvalidArgument(_))
    ));
    assert!(matches!(
        b.create_channel(channel("p0", ChannelMode::Stream, 0)),
        Err(CoreError::InvalidArgument(_))
    ));
    assert!(matches!(
        b.create_channel(channel("p2k", ChannelMode::Stream, 2000)),
        Err(CoreError::InvalidArgument(_))
    ));
    let mut zero = channel("z", ChannelMode::Stream, 1);
    zero.retry.max_attempts = 0;
    assert!(b.create_channel(zero).is_err());
    b.create_channel(channel("s", ChannelMode::Stream, 3))
        .unwrap();
    for name in ["s", "S"] {
        // Case-insensitively: on macOS `S` would be the directory of `s`.
        assert!(matches!(
            b.create_channel(channel(name, ChannelMode::Stream, 3)),
            Err(CoreError::ChannelExists(_))
        ));
    }
    assert!(matches!(
        b.subscribe("s", "g", info("w", 1), 0, StartFrom::Latest),
        Err(CoreError::InvalidArgument(_))
    ));
    assert!(matches!(
        b.subscribe("nope", "g", info("w", 1), 1, StartFrom::Latest),
        Err(CoreError::ChannelNotFound(_))
    ));
    b.connection_opened();
    b.connection_opened();
    b.connection_closed();
    let mut sub = b
        .subscribe("s", "g", info("w", 7), 10, StartFrom::Earliest)
        .unwrap();
    publish_keyed(&b, "s", 30, 5).await;
    for _ in 0..10 {
        let d = next(&mut sub).await;
        sub.ack(&[d.delivery_id]).unwrap();
    }
    let s = b.stats();
    assert_eq!(s.node_id, "test-node");
    assert_eq!(s.connections, 1);
    assert!(s.storage_bytes > 0 && s.storage_capacity_bytes > s.storage_bytes);
    assert!(!s.fsync_latency_us.is_empty());
    let c = &s.channels[0];
    assert_eq!(c.partitions.len(), 3);
    assert_eq!(c.partitions.iter().map(|p| p.high_offset).sum::<u64>(), 30);
    assert_eq!(
        c.partitions.iter().map(|p| p.published_total).sum::<u64>(),
        30
    );
    assert_eq!(c.published_total, 30);
    assert!(c.published_bytes_total > 0);
    assert_eq!(c.publish_latency_us.len(), 30);
    assert!(c.oldest_timestamp_ms.is_some());
    let g = &c.groups[0];
    assert_eq!((g.lag, g.acked_total, g.delivered_total), (20, 10, 10));
    let i = &g.instances[0];
    assert_eq!(i.id, "w#7");
    assert_eq!(i.lag, 20);
    assert_eq!(i.assigned_partitions, vec![0, 1, 2]);
    sub.report_stats(fluxmq_proto::wire::ClientStats {
        subscription_id: sub.id(),
        cpu_percent: Some(12.5),
        ..Default::default()
    });
    assert_eq!(
        b.stats().channels[0].groups[0].instances[0]
            .client_stats
            .as_ref()
            .unwrap()
            .cpu_percent,
        Some(12.5)
    );
    // Update persists across restart.
    let mut r = b.channel("s").unwrap().retry;
    r.max_attempts = 9;
    b.update_channel(
        "s",
        UpdateChannelRequest {
            retry: Some(r),
            ..Default::default()
        },
    )
    .unwrap();
    drop(sub);
    b.shutdown().await;
    let b = open(dir.path()).await;
    assert_eq!(b.channel("s").unwrap().retry.max_attempts, 9);
    assert_eq!(b.stats().channels[0].groups[0].lag, 20);
    b.shutdown().await;
}

fn texts(m: &[fluxmq_proto::model::Message]) -> Vec<String> {
    m.iter()
        .map(|m| String::from_utf8_lossy(&m.payload).into_owned())
        .collect()
}

#[tokio::test]
async fn read_messages_newest_first_time_window() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("w", ChannelMode::Stream, 2))
        .unwrap();
    let mut marks = Vec::new();
    for phase in 0..3 {
        for i in phase * 10..phase * 10 + 10 {
            let key = if i % 2 == 0 { "even" } else { "odd" };
            b.publish("w", vec![msg(Some(key), &format!("m{i}"))])
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        marks.push(fluxmq_proto::now_ms());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let (t1, t2) = (marks[0], marks[1]);
    let q = |since, until, key: Option<&str>, limit| MessageQuery {
        since_ms: since,
        until_ms: until,
        key: key.map(str::to_string),
        limit: Some(limit),
        newest_first: Some(true),
        ..Default::default()
    };
    // "Last N" of an open-ended window: the newest 3, ascending; older ones exist.
    let (m, _, more) = b.read_messages("w", &q(Some(t1), None, None, 3)).unwrap();
    assert_eq!(texts(&m), vec!["m27", "m28", "m29"]);
    assert!(more);
    // Without newest_first the same window returns its oldest page.
    let (m, _, _) = b
        .read_messages(
            "w",
            &MessageQuery {
                newest_first: None,
                ..q(Some(t1), None, None, 3)
            },
        )
        .unwrap();
    assert_eq!(texts(&m), vec!["m10", "m11", "m12"]);
    // Bounded window [t1, t2] = phase 1 only.
    let (m, _, more) = b
        .read_messages("w", &q(Some(t1), Some(t2), None, 4))
        .unwrap();
    assert_eq!(texts(&m), vec!["m16", "m17", "m18", "m19"]);
    assert!(more);
    let (m, _, more) = b
        .read_messages("w", &q(Some(t1), Some(t2), None, 10))
        .unwrap();
    assert_eq!(m.len(), 10);
    assert!(!more, "exactly the window, nothing older inside it");
    // Key filter within the window.
    let (m, _, more) = b
        .read_messages("w", &q(Some(t1), Some(t2), Some("odd"), 2))
        .unwrap();
    assert_eq!(texts(&m), vec!["m17", "m19"]);
    assert!(more);
    // Only an upper bound, single partition.
    let (m, _, _) = b
        .read_messages(
            "w",
            &MessageQuery {
                partition: Some(m[0].partition),
                ..q(None, Some(t1), Some("odd"), 2)
            },
        )
        .unwrap();
    assert_eq!(texts(&m), vec!["m7", "m9"]);
    // Offset still takes precedence over newest_first.
    let (m, _, _) = b
        .read_messages(
            "w",
            &MessageQuery {
                partition: Some(0),
                offset: Some(0),
                ..q(Some(t1), None, None, 1)
            },
        )
        .unwrap();
    assert_eq!(m[0].offset, 0);
    // Empty window.
    let (m, _, more) = b
        .read_messages("w", &q(Some(u64::MAX - 1), None, None, 5))
        .unwrap();
    assert!(m.is_empty() && !more);
    b.shutdown().await;
}

#[tokio::test]
async fn newest_first_scans_backwards_not_whole_window() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("big", ChannelMode::Stream, 1))
        .unwrap();
    for _ in 0..20 {
        publish_keyed(&b, "big", 500, 7).await;
    }
    let q = MessageQuery {
        since_ms: Some(0),
        limit: Some(10),
        newest_first: Some(true),
        ..Default::default()
    };
    let (m, scanned, more) = b.read_messages("big", &q).unwrap();
    assert_eq!(m.len(), 10);
    assert_eq!(m.last().unwrap().offset, 9_999);
    assert!(more);
    assert!(
        scanned < 100,
        "scanned {scanned} records for a 10-message page of a 10k window"
    );
    b.shutdown().await;
}
