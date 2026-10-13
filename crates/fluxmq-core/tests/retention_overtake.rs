//! Size retention overtaking a slow consumer group (v0.7 bench: 1 KiB 1×1 saturation, a
//! drain check found acknowledged ids neither delivered nor counted as retention skips).
//!
//! Invariants, checked per partition once the producer stopped and the group drained:
//! * every offset below the high watermark was delivered, or it is below the log start
//!   (removed by retention) — a stored, undelivered offset is an at-least-once violation;
//! * every removed, undelivered offset is counted in `retention_skipped_total` (and warned):
//!   undelivered = skipped exactly, never silently;
//! * without retention (cap never reached) every offset is delivered.
//!
//! Run in raw (`DeliverRecords`) and decoded mode, with the recent cache tiny so the group
//! reads behind it through the catch-up read-ahead, while retention runs every few ms.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use common::*;
use fluxmq_core::{Broker, RawDelivery, SharedDelivery, SubMode};
use fluxmq_proto::model::{ChannelMode, PublishMessage, RetentionPolicy, StartFrom};
use fluxmq_proto::wire::DeliveryRange;

const P: u32 = 4;

struct Outcome {
    delivered: Vec<BTreeSet<u64>>,
    low: Vec<u64>,
    high: Vec<u64>,
    skipped_counter: u64,
}

async fn run(raw: bool, retention: Option<u64>, seed: u64) -> Outcome {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.segment_bytes = 32 * 1024;
    cfg.partition_cache_bytes = 16 * 1024;
    cfg.fsync = fluxmq_core::FsyncPolicy::OsBuffered;
    let b = Broker::open(cfg).await.unwrap();
    let mut ch = channel("r", ChannelMode::Stream, P);
    ch.retention = RetentionPolicy {
        max_age_secs: None,
        max_bytes: retention.map(|per_part| per_part * u64::from(P)),
    };
    b.create_channel(ch).unwrap();
    let mode = SubMode {
        cumulative: false,
        raw_records: raw,
    };
    let mut sub = b
        .subscribe_with("r", "g", info("c", 1), 512, StartFrom::Earliest, mode)
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    // Producer: batches of 64 × 256 B, as fast as it can for a while.
    let producer = {
        let b = b.clone();
        tokio::spawn(async move {
            let payload = Bytes::from(vec![b'x'; 256]);
            for _ in 0..600 {
                let msgs = (0..64)
                    .map(|_| PublishMessage {
                        key: None,
                        headers: Default::default(),
                        payload: payload.clone(),
                    })
                    .collect();
                b.publish("r", msgs).await.unwrap();
                tokio::task::yield_now().await;
            }
        })
    };
    // Retention, unpaced, every 2 ms.
    let ret = {
        let (b, stop) = (b.clone(), stop.clone());
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                b.enforce_retention().await;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
    };
    // Slow consumer: small frames, a pause between them, acks in ranges / ids.
    let mut delivered: Vec<BTreeSet<u64>> = vec![BTreeSet::new(); P as usize];
    let mut rng = seed | 1;
    let mut idle = 0;
    loop {
        let mut out: Vec<SharedDelivery> = Vec::new();
        let mut raws: Vec<RawDelivery> = Vec::new();
        let got = tokio::time::timeout(
            Duration::from_millis(300),
            sub.next_frames(&mut out, &mut raws, 64, 64 * 1024),
        )
        .await
        .is_ok();
        if !got || (out.is_empty() && raws.is_empty()) {
            if producer.is_finished() {
                idle += 1;
                if idle >= 5 {
                    break;
                }
            }
            continue;
        }
        idle = 0;
        let mut ranges = Vec::new();
        for r in &raws {
            for i in 0..u64::from(r.count) {
                delivered[r.partition as usize].insert(r.from_offset + i);
            }
            ranges.push(DeliveryRange {
                first: r.first_delivery_id,
                count: r.count,
            });
        }
        sub.ack_ranges(&ranges).unwrap();
        for d in &out {
            delivered[d.message.partition as usize].insert(d.message.offset);
        }
        sub.ack(&out.iter().map(|d| d.delivery_id).collect::<Vec<_>>())
            .unwrap();
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        if !producer.is_finished() {
            tokio::time::sleep(Duration::from_micros(200 + rng % 800)).await;
        }
    }
    stop.store(true, Ordering::Relaxed);
    producer.await.unwrap();
    ret.await.unwrap();
    let st = b.stats();
    let c = st.channels.iter().find(|c| c.config.name == "r").unwrap();
    let g = c.groups.iter().find(|g| g.name == "g").unwrap();
    let out = Outcome {
        delivered,
        low: c.partitions.iter().map(|p| p.low_offset).collect(),
        high: c.partitions.iter().map(|p| p.high_offset).collect(),
        skipped_counter: g.retention_skipped_total,
    };
    drop(sub);
    b.shutdown().await;
    out
}

fn check(o: &Outcome, what: &str) -> (u64, u64) {
    let mut removed_undelivered = 0u64;
    for p in 0..P as usize {
        let stored_missing: Vec<u64> = (o.low[p]..o.high[p])
            .filter(|x| !o.delivered[p].contains(x))
            .take(10)
            .collect();
        assert!(
            stored_missing.is_empty(),
            "{what}: p{p}: stored offsets never delivered (at-least-once violated): {stored_missing:?} \
             (log {}..{}, delivered {})",
            o.low[p],
            o.high[p],
            o.delivered[p].len()
        );
        removed_undelivered += (0..o.low[p])
            .filter(|x| !o.delivered[p].contains(x))
            .count() as u64;
    }
    assert_eq!(
        removed_undelivered, o.skipped_counter,
        "{what}: retention removed {removed_undelivered} undelivered messages but the group counted \
         {} skips (a silent skip)",
        o.skipped_counter
    );
    (removed_undelivered, o.high.iter().sum())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_overtaking_a_slow_group_is_counted_and_never_skips_stored_data() {
    let mut overtaken = 0;
    for seed in 1..=3u64 {
        for raw in [true, false] {
            let o = run(raw, Some(64 * 1024), seed).await;
            let (skipped, total) = check(&o, &format!("raw={raw} seed={seed}"));
            eprintln!(
                "raw={raw} seed={seed}: {skipped} of {total} removed before delivery (counted)"
            );
            overtaken += skipped;
        }
    }
    assert!(
        overtaken > 0,
        "the scenario must make retention overtake the group"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_group_without_retention_gets_everything() {
    for raw in [true, false] {
        let o = run(raw, None, 7).await;
        let (skipped, total) = check(&o, &format!("no retention raw={raw}"));
        assert_eq!(skipped, 0);
        let delivered: usize = o.delivered.iter().map(BTreeSet::len).sum();
        assert_eq!(delivered as u64, total, "raw={raw}");
    }
}
