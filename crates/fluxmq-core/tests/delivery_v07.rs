//! v0.7 delivery: range-based ack state vs a naive model, restart exactness, cumulative
//! ack mode, raw delivery (`DeliverRecords`) id math and redelivery of raw entries.

mod common;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use common::*;
use fluxmq_core::{Broker, CoreError, RawDelivery, SharedDelivery, SubMode, decode_chunk};
use fluxmq_proto::model::{AttemptOutcome, ChannelMode, RetryPolicy, StartFrom};
use fluxmq_proto::wire::DeliveryRange;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn group_stats(b: &Broker, ch: &str, g: &str) -> fluxmq_core::GroupStats {
    b.stats()
        .channels
        .into_iter()
        .find(|c| c.config.name == ch)
        .and_then(|c| c.groups.into_iter().find(|x| x.name == g))
        .expect("group stats")
}

async fn publish_n(b: &Broker, ch: &str, n: usize, prefix: &str) {
    for chunk in (0..n).collect::<Vec<_>>().chunks(50) {
        let msgs = chunk
            .iter()
            .map(|i| msg(None, &format!("{prefix}{i}")))
            .collect();
        b.publish(ch, msgs).await.expect("publish");
    }
}

/// Takes what is ready (waiting up to `ms` for the first delivery).
async fn take(sub: &mut fluxmq_core::Subscription, max: usize, ms: u64) -> Vec<SharedDelivery> {
    let mut out = Vec::new();
    let _ = tokio::time::timeout(
        Duration::from_millis(ms),
        sub.next_batch(&mut out, max, usize::MAX),
    )
    .await;
    out
}

/// Random ack / ack-ranges / nack / disconnect sequences against a naive model of which
/// offsets are finished. Checks lag after every step, that a finished message is never
/// delivered again, and that everything is eventually finished exactly as the model says.
#[tokio::test]
async fn range_ack_state_matches_naive_model() {
    for seed in [1u64, 7, 42, 1234, 99_991] {
        let dir = tempfile::tempdir().unwrap();
        let b = open(dir.path()).await;
        let mut c = channel("m", ChannelMode::Stream, 2);
        c.retry = RetryPolicy {
            max_attempts: 1000,
            backoff_ms: vec![0],
            ack_timeout_ms: 60_000,
        };
        b.create_channel(c).unwrap();
        const N: usize = 400;
        publish_n(&b, "m", N, "x").await;
        let mut rng = Rng(seed);
        // (partition, offset) finished in the model.
        let mut done: BTreeSet<(u32, u64)> = BTreeSet::new();
        // delivery id -> (partition, offset) in flight on the current subscription.
        let mut inflight: BTreeMap<u64, (u32, u64)> = BTreeMap::new();
        let mut conn = 1;
        let mut sub = b
            .subscribe("m", "g", info("c", conn), 64, StartFrom::Earliest)
            .unwrap();
        let mut steps = 0;
        while done.len() < N && steps < 5000 {
            steps += 1;
            match rng.below(10) {
                0..=3 => {
                    for d in take(&mut sub, 1 + rng.below(40) as usize, 50).await {
                        let key = (d.message.partition, d.message.offset);
                        assert!(
                            !done.contains(&key),
                            "seed {seed}: finished {key:?} redelivered"
                        );
                        assert!(inflight.insert(d.delivery_id, key).is_none());
                    }
                }
                4..=5 if !inflight.is_empty() => {
                    // Ack a random subset by id.
                    let ids: Vec<u64> = inflight
                        .keys()
                        .copied()
                        .filter(|_| rng.below(3) != 0)
                        .collect();
                    sub.ack(&ids).unwrap();
                    for id in ids {
                        done.insert(inflight.remove(&id).unwrap());
                    }
                }
                6 if !inflight.is_empty() => {
                    // Ack one contiguous id range of in-flight ids via AckRanges.
                    let ids: Vec<u64> = inflight.keys().copied().collect();
                    let a = rng.below(ids.len() as u64) as usize;
                    let mut e = a;
                    while e + 1 < ids.len() && ids[e + 1] == ids[e] + 1 && rng.below(5) != 0 {
                        e += 1;
                    }
                    let r = DeliveryRange {
                        first: ids[a],
                        count: (ids[e] - ids[a] + 1) as u32,
                    };
                    sub.ack_ranges(&[r]).unwrap();
                    for id in &ids[a..=e] {
                        done.insert(inflight.remove(id).unwrap());
                    }
                }
                7 if !inflight.is_empty() => {
                    let ids: Vec<u64> = inflight.keys().copied().collect();
                    let id = ids[rng.below(ids.len() as u64) as usize];
                    let dead = rng.below(4) == 0;
                    sub.nack(id, "boom", !dead).unwrap();
                    let key = inflight.remove(&id).unwrap();
                    if dead {
                        done.insert(key); // dead-lettered = finished for the group
                    }
                }
                8 if rng.below(4) == 0 => {
                    // Disconnect: in-flight deliveries return to the group.
                    drop(sub);
                    inflight.clear();
                    conn += 1;
                    sub = b
                        .subscribe("m", "g", info("c", conn), 64, StartFrom::Earliest)
                        .unwrap();
                }
                _ => {
                    // Unknown / already-settled ids are refused and change nothing.
                    let bogus = 10_000_000 + rng.below(1000);
                    assert!(matches!(
                        sub.ack(&[bogus]),
                        Err(CoreError::UnknownDelivery(_))
                    ));
                    assert!(
                        sub.ack_ranges(&[DeliveryRange {
                            first: bogus,
                            count: 1 << 30
                        }])
                        .is_err()
                    );
                }
            }
            let gs = group_stats(&b, "m", "g");
            assert_eq!(
                gs.lag,
                (N - done.len()) as u64,
                "seed {seed} step {steps}: lag"
            );
            assert_eq!(
                gs.in_flight,
                inflight.len() as u64,
                "seed {seed}: in_flight"
            );
        }
        assert_eq!(done.len(), N, "seed {seed}: all finished");
        drop(sub);
        b.shutdown().await;
        // Committed offsets = log end on every partition after a clean shutdown.
        let off: serde_json::Value = serde_json::from_slice(
            &std::fs::read(dir.path().join("channels/m/offsets.json")).unwrap(),
        )
        .unwrap();
        let committed: u64 = off["groups"]["g"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .sum();
        assert_eq!(committed, N as u64, "seed {seed}: committed = end");
        assert!(off.get("acked").is_none(), "nothing above the watermark");
    }
}

/// The same model check for a raw-records subscription (v0.8: `AckRanges` on raw runs is
/// applied a bit-word at a time). Raw frames and decoded redeliveries mix; ranges span
/// several 64-entry words, start and end mid-word and cross run boundaries.
#[tokio::test]
async fn raw_range_ack_state_matches_naive_model() {
    for seed in [3u64, 11, 77, 2024, 65_537] {
        let dir = tempfile::tempdir().unwrap();
        let b = open(dir.path()).await;
        let mut c = channel("rm", ChannelMode::Stream, 2);
        c.retry = RetryPolicy {
            max_attempts: 1000,
            backoff_ms: vec![0],
            ack_timeout_ms: 60_000,
        };
        b.create_channel(c).unwrap();
        const N: usize = 1500;
        // Larger publishes: raw runs of up to a few hundred entries.
        for chunk in (0..N).collect::<Vec<_>>().chunks(250) {
            let msgs = chunk.iter().map(|i| msg(None, &format!("r{i}"))).collect();
            b.publish("rm", msgs).await.expect("publish");
        }
        let mode = SubMode {
            cumulative: false,
            raw_records: true,
        };
        let mut rng = Rng(seed);
        let mut done: BTreeSet<(u32, u64)> = BTreeSet::new();
        let mut inflight: BTreeMap<u64, (u32, u64)> = BTreeMap::new();
        let mut conn = 1;
        let mut sub = b
            .subscribe_with("rm", "g", info("c", conn), 700, StartFrom::Earliest, mode)
            .unwrap();
        let mut steps = 0;
        let mut raw_seen = 0usize;
        while done.len() < N && steps < 5000 {
            steps += 1;
            match rng.below(10) {
                0..=3 => {
                    let (out, raw) = take_frames(&mut sub, 1 + rng.below(400) as usize, 50).await;
                    for r in raw {
                        raw_seen += r.count as usize;
                        for i in 0..u64::from(r.count) {
                            let key = (r.partition, r.from_offset + i);
                            assert!(!done.contains(&key), "seed {seed}: {key:?} redelivered");
                            assert!(inflight.insert(r.first_delivery_id + i, key).is_none());
                        }
                    }
                    for d in out {
                        let key = (d.message.partition, d.message.offset);
                        assert!(!done.contains(&key), "seed {seed}: {key:?} redelivered");
                        assert!(inflight.insert(d.delivery_id, key).is_none());
                    }
                }
                4..=6 if !inflight.is_empty() => {
                    // Several AckRanges of contiguous in-flight ids.
                    let ids: Vec<u64> = inflight.keys().copied().collect();
                    let mut ranges = Vec::new();
                    let mut acked = Vec::new();
                    for _ in 0..1 + rng.below(3) {
                        let a = rng.below(ids.len() as u64) as usize;
                        let mut e = a;
                        let want = rng.below(300);
                        while e + 1 < ids.len()
                            && ids[e + 1] == ids[e] + 1
                            && ((e - a) as u64) < want
                        {
                            e += 1;
                        }
                        if acked.iter().any(|&(x, y)| a <= y && x <= e) {
                            continue;
                        }
                        acked.push((a, e));
                        ranges.push(DeliveryRange {
                            first: ids[a],
                            count: (ids[e] - ids[a] + 1) as u32,
                        });
                    }
                    sub.ack_ranges(&ranges).unwrap();
                    for (a, e) in acked {
                        for id in &ids[a..=e] {
                            done.insert(inflight.remove(id).unwrap());
                        }
                    }
                }
                7 if !inflight.is_empty() => {
                    let ids: Vec<u64> = inflight.keys().copied().collect();
                    let id = ids[rng.below(ids.len() as u64) as usize];
                    let dead = rng.below(4) == 0;
                    sub.nack(id, "boom", !dead).unwrap();
                    let key = inflight.remove(&id).unwrap();
                    if dead {
                        done.insert(key);
                    }
                }
                8 if rng.below(4) == 0 => {
                    drop(sub);
                    inflight.clear();
                    conn += 1;
                    sub = b
                        .subscribe_with("rm", "g", info("c", conn), 700, StartFrom::Earliest, mode)
                        .unwrap();
                }
                _ if !inflight.is_empty() => {
                    // A range over settled and live ids: refused, the live ones still acked.
                    let (&lo, _) = inflight.iter().next().unwrap();
                    let r = DeliveryRange {
                        first: lo.saturating_sub(5),
                        count: 10,
                    };
                    let res = sub.ack_ranges(&[r]);
                    let covered: Vec<u64> = inflight
                        .range(r.first..r.first + 10)
                        .map(|(&id, _)| id)
                        .collect();
                    assert_eq!(res.is_err(), covered.len() < 10);
                    for id in covered {
                        done.insert(inflight.remove(&id).unwrap());
                    }
                }
                _ => {}
            }
            let gs = group_stats(&b, "rm", "g");
            assert_eq!(
                gs.lag,
                (N - done.len()) as u64,
                "seed {seed} step {steps}: lag"
            );
            assert_eq!(
                gs.in_flight,
                inflight.len() as u64,
                "seed {seed}: in_flight"
            );
        }
        assert_eq!(done.len(), N, "seed {seed}: all finished");
        assert!(
            raw_seen >= N / 2,
            "seed {seed}: mostly raw frames ({raw_seen})"
        );
        drop(sub);
        b.shutdown().await;
        let off: serde_json::Value = serde_json::from_slice(
            &std::fs::read(dir.path().join("channels/rm/offsets.json")).unwrap(),
        )
        .unwrap();
        let committed: u64 = off["groups"]["g"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .sum();
        assert_eq!(committed, N as u64, "seed {seed}: committed = end");
    }
}

/// Acks out of order, restarts, and gets back exactly the unacked messages.
#[tokio::test]
async fn restart_redelivers_exactly_the_unfinished_messages() {
    let dir = tempfile::tempdir().unwrap();
    {
        let b = open(dir.path()).await;
        b.create_channel(channel("r", ChannelMode::Stream, 1))
            .unwrap();
        publish_n(&b, "r", 100, "m").await;
        let mut sub = b
            .subscribe("r", "g", info("c", 1), 1000, StartFrom::Earliest)
            .unwrap();
        let mut got = Vec::new();
        while got.len() < 100 {
            got.extend(take(&mut sub, 100, 1000).await);
        }
        let keep: BTreeSet<u64> = [10, 50, 51, 77].into();
        let ids: Vec<u64> = got
            .iter()
            .filter(|d| !keep.contains(&d.message.offset))
            .map(|d| d.delivery_id)
            .collect();
        sub.ack(&ids).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await; // periodic checkpoint
        let off: serde_json::Value = serde_json::from_slice(
            &std::fs::read(dir.path().join("channels/r/offsets.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(off["groups"]["g"][0], 10);
        assert_eq!(
            off["acked"]["g"][0],
            serde_json::json!([[11, 50], [52, 77], [78, 100]])
        );
        drop(sub);
        b.shutdown().await;
    }
    let b = open(dir.path()).await;
    let mut sub = b
        .subscribe("r", "g", info("c", 2), 1000, StartFrom::Earliest)
        .unwrap();
    let mut got = Vec::new();
    loop {
        let d = take(&mut sub, 100, 300).await;
        if d.is_empty() {
            break;
        }
        got.extend(d);
    }
    let offsets: Vec<u64> = got.iter().map(|d| d.message.offset).collect();
    assert_eq!(offsets, vec![10, 50, 51, 77]);
    sub.ack(&got.iter().map(|d| d.delivery_id).collect::<Vec<_>>())
        .unwrap();
    assert_eq!(group_stats(&b, "r", "g").lag, 0);
    drop(sub);
    b.shutdown().await;
}

/// An offsets.json written by v0.6 (no `acked` field) still loads.
#[tokio::test]
async fn old_offsets_checkpoint_loads() {
    let dir = tempfile::tempdir().unwrap();
    {
        let b = open(dir.path()).await;
        b.create_channel(channel("o", ChannelMode::Stream, 1))
            .unwrap();
        publish_n(&b, "o", 10, "m").await;
        b.shutdown().await;
    }
    std::fs::write(
        dir.path().join("channels/o/offsets.json"),
        br#"{"groups":{"g":[5]}}"#,
    )
    .unwrap();
    let b = open(dir.path()).await;
    let mut sub = b
        .subscribe("o", "g", info("c", 1), 100, StartFrom::Earliest)
        .unwrap();
    let mut got = Vec::new();
    while got.len() < 5 {
        let d = take(&mut sub, 100, 1000).await;
        assert!(!d.is_empty());
        got.extend(d);
    }
    assert_eq!(
        got.iter().map(|d| d.message.offset).collect::<Vec<_>>(),
        vec![5, 6, 7, 8, 9]
    );
    drop(sub);
    b.shutdown().await;
}

#[tokio::test]
async fn cumulative_mode_commits_positions() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    let mut c = channel("s", ChannelMode::Stream, 1);
    c.retry.ack_timeout_ms = 100;
    b.create_channel(c).unwrap();
    b.create_channel(channel("q", ChannelMode::Queue, 1))
        .unwrap();
    let cum = SubMode {
        cumulative: true,
        raw_records: false,
    };
    assert!(matches!(
        b.subscribe_with("q", "g", info("c", 1), 10, StartFrom::Earliest, cum),
        Err(CoreError::InvalidArgument(_))
    ));
    publish_n(&b, "s", 20, "m").await;
    let mut sub = b
        .subscribe_with("s", "g", info("c", 1), 100, StartFrom::Earliest, cum)
        .unwrap();
    let mut got = Vec::new();
    while got.len() < 20 {
        got.extend(take(&mut sub, 100, 1000).await);
    }
    // No ack timeout in cumulative mode.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(take(&mut sub, 100, 50).await.is_empty(), "no redelivery");
    assert_eq!(group_stats(&b, "s", "g").in_flight, 20);
    // Nack doesn't apply.
    assert!(matches!(
        sub.nack(got[0].delivery_id, "x", true),
        Err(CoreError::InvalidArgument(_))
    ));
    // Commit below 12: settles offsets 0..12; a lower commit later is a no-op.
    sub.commit(0, 12).unwrap();
    sub.commit(0, 3).unwrap();
    let gs = group_stats(&b, "s", "g");
    assert_eq!(gs.in_flight, 8);
    assert_eq!(gs.lag, 8);
    // The subscription ends: delivery resumes from the committed offset.
    drop(sub);
    let mut sub = b
        .subscribe_with("s", "g", info("c", 2), 100, StartFrom::Earliest, cum)
        .unwrap();
    let mut again = Vec::new();
    while again.len() < 8 {
        again.extend(take(&mut sub, 100, 1000).await);
    }
    assert_eq!(
        again.iter().map(|d| d.message.offset).collect::<Vec<_>>(),
        (12..20).collect::<Vec<_>>()
    );
    sub.commit(0, 20).unwrap();
    assert_eq!(group_stats(&b, "s", "g").lag, 0);
    // An individual-mode subscription can't Commit.
    let ind = b
        .subscribe("s", "g2", info("c", 3), 10, StartFrom::Earliest)
        .unwrap();
    assert!(matches!(
        ind.commit(0, 5),
        Err(CoreError::InvalidArgument(_))
    ));
    drop((sub, ind));
    b.shutdown().await;
}

async fn take_frames(
    sub: &mut fluxmq_core::Subscription,
    max: usize,
    ms: u64,
) -> (Vec<SharedDelivery>, Vec<RawDelivery>) {
    let mut out = Vec::new();
    let mut raw = Vec::new();
    let _ = tokio::time::timeout(
        Duration::from_millis(ms),
        sub.next_frames(&mut out, &mut raw, max, 1 << 20),
    )
    .await;
    (out, raw)
}

/// Raw frames: exactly `count` records in range, ids `first + (o - from)`, unique ids
/// across raw and decoded deliveries, and a nacked raw entry comes back decoded.
#[tokio::test]
async fn raw_delivery_ids_and_redelivery() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    let mut c = channel("raw", ChannelMode::Stream, 2);
    c.retry.backoff_ms = vec![0];
    b.create_channel(c).unwrap();
    publish_n(&b, "raw", 500, "p").await;
    let mode = SubMode {
        cumulative: false,
        raw_records: true,
    };
    let mut sub = b
        .subscribe_with("raw", "g", info("c", 1), 300, StartFrom::Earliest, mode)
        .unwrap();
    let mut by_id: HashMap<u64, (u32, u64, String)> = HashMap::new();
    let mut raw_frames = 0;
    let mut nacked = None;
    let mut redelivered = None;
    while by_id.len() < 500 || redelivered.is_none() {
        let (out, raw) = take_frames(&mut sub, 256, 2000).await;
        assert!(!out.is_empty() || !raw.is_empty(), "progress");
        let mut ranges = Vec::new();
        for r in &raw {
            raw_frames += 1;
            let recs: Vec<_> = decode_chunk(r.partition, &r.envelopes)
                .map(|m| m.unwrap())
                .filter(|m| {
                    m.offset >= r.from_offset && m.offset < r.from_offset + u64::from(r.count)
                })
                .collect();
            assert_eq!(
                recs.len() as u32,
                r.count,
                "every offset of the range is present"
            );
            for m in recs {
                let id = r.first_delivery_id + (m.offset - r.from_offset);
                let p = String::from_utf8(m.payload.to_vec()).unwrap();
                assert!(
                    by_id.insert(id, (m.partition, m.offset, p)).is_none(),
                    "unique id"
                );
            }
            ranges.push(DeliveryRange {
                first: r.first_delivery_id,
                count: r.count,
            });
        }
        // Nack one raw delivery (the first one seen), ack everything else.
        if nacked.is_none()
            && let Some(r) = ranges.first_mut()
        {
            let id = r.first;
            nacked = Some((id, by_id[&id].clone()));
            sub.nack(id, "retry me", true).unwrap();
            r.first += 1;
            r.count -= 1;
        }
        sub.ack_ranges(&ranges).unwrap();
        for d in &out {
            assert!(
                !by_id.contains_key(&d.delivery_id),
                "ids unique across forms"
            );
            if let Some((_, (p, o, payload))) = &nacked
                && d.message.partition == *p
                && d.message.offset == *o
            {
                assert_eq!(d.attempt, 2);
                assert_eq!(
                    &String::from_utf8(d.message.payload.to_vec()).unwrap(),
                    payload
                );
                redelivered = Some(d.delivery_id);
            }
        }
        sub.ack(&out.iter().map(|d| d.delivery_id).collect::<Vec<_>>())
            .unwrap();
    }
    assert!(raw_frames > 0);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let gs = group_stats(&b, "raw", "g");
    assert_eq!(gs.lag, 0);
    assert_eq!(gs.in_flight, 0);
    // Payloads of all 500 offsets seen exactly once in raw form.
    let seen: BTreeSet<(u32, u64)> = by_id.values().map(|(p, o, _)| (*p, *o)).collect();
    assert_eq!(seen.len(), 500);
    // Attempt history (v0.8: first attempts kept as spans, failures per message) reads as
    // before: the nacked entry has a nacked first attempt and an acked second one, every
    // other message one acked first attempt.
    let (_, (np, no, _)) = nacked.clone().unwrap();
    let mid = |p: u32, o: u64| fluxmq_proto::model::MessageId {
        partition: p,
        offset: o,
    };
    let (_, h) = b.get_message("raw", mid(np, no)).unwrap().unwrap();
    let g = &h["g"];
    assert_eq!(g.len(), 2, "{g:?}");
    assert_eq!((g[0].attempt, g[1].attempt), (1, 2));
    assert_eq!(
        g[0].outcome,
        AttemptOutcome::Nacked {
            reason: "retry me".into()
        }
    );
    assert_eq!(g[0].consumer, "c#1");
    assert!(g[0].finished_at_ms.unwrap() >= g[0].delivered_at_ms);
    assert_eq!(g[1].outcome, AttemptOutcome::Acked);
    for (p, o) in seen.iter().filter(|&&(p, o)| (p, o) != (np, no)) {
        let (_, h) = b.get_message("raw", mid(*p, *o)).unwrap().unwrap();
        let g = &h["g"];
        assert_eq!(g.len(), 1, "{p}/{o}: {g:?}");
        assert_eq!(g[0].attempt, 1);
        assert_eq!(g[0].outcome, AttemptOutcome::Acked);
        assert_eq!(g[0].consumer, "c#1");
        assert!(g[0].finished_at_ms.unwrap() >= g[0].delivered_at_ms);
    }
    drop(sub);
    b.shutdown().await;
}

/// A raw subscription that disconnects with raw ranges in flight gets them back decoded
/// (redelivery decodes the stored envelopes), nothing lost, nothing acked twice.
#[tokio::test]
async fn raw_inflight_returns_on_disconnect() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("rd", ChannelMode::Stream, 1))
        .unwrap();
    publish_n(&b, "rd", 100, "p").await;
    let mode = SubMode {
        cumulative: false,
        raw_records: true,
    };
    let mut sub = b
        .subscribe_with("rd", "g", info("c", 1), 1000, StartFrom::Earliest, mode)
        .unwrap();
    let (_, raw) = take_frames(&mut sub, 256, 2000).await;
    assert!(!raw.is_empty());
    let r = &raw[0];
    // Ack the odd ids of the first range only.
    let odd: Vec<u64> = (0..u64::from(r.count))
        .filter(|i| i % 2 == 1)
        .map(|i| r.first_delivery_id + i)
        .collect();
    sub.ack(&odd).unwrap();
    let raw_total: u64 = raw.iter().map(|r| u64::from(r.count)).sum();
    let acked = odd.len() as u64;
    drop(sub);
    let mut sub = b
        .subscribe("rd", "g", info("c", 2), 1000, StartFrom::Earliest)
        .unwrap();
    let mut got = Vec::new();
    loop {
        let d = take(&mut sub, 256, 300).await;
        if d.is_empty() {
            break;
        }
        got.extend(d);
    }
    let expect = 100 - acked;
    assert_eq!(
        got.len() as u64,
        expect,
        "raw in flight {raw_total}, acked {acked}"
    );
    let offs: BTreeSet<u64> = got.iter().map(|d| d.message.offset).collect();
    assert_eq!(offs.len() as u64, expect);
    for o in &odd {
        assert!(!offs.contains(&(o - r.first_delivery_id + r.from_offset)));
    }
    sub.ack(&got.iter().map(|d| d.delivery_id).collect::<Vec<_>>())
        .unwrap();
    assert_eq!(group_stats(&b, "rd", "g").lag, 0);
    drop(sub);
    b.shutdown().await;
}

/// A raw range acked except for a few stragglers is compacted (the stragglers keep their
/// own payload copies); they are still redelivered intact after a disconnect.
#[tokio::test]
async fn raw_stragglers_survive_compaction() {
    // v0.8: a mostly settled run is compacted only once it is `COMPACT_AFTER` old (by a later
    // ack batch or the reaper tick); stragglers are intact either way.
    for (group, wait_ms) in [("g", 0u64), ("late", 600)] {
        let dir = tempfile::tempdir().unwrap();
        let b = open(dir.path()).await;
        b.create_channel(channel("st", ChannelMode::Stream, 1))
            .unwrap();
        // One big envelope of 200 records.
        let msgs = (0..200).map(|i| msg(None, &format!("s{i}"))).collect();
        b.publish("st", msgs).await.unwrap();
        let mode = SubMode {
            cumulative: false,
            raw_records: true,
        };
        let mut sub = b
            .subscribe_with("st", group, info("c", 1), 1000, StartFrom::Earliest, mode)
            .unwrap();
        let (_, raw) = take_frames(&mut sub, 256, 2000).await;
        assert_eq!(raw.len(), 1);
        let r = &raw[0];
        assert_eq!(r.count, 200);
        let keep = [3u64, 100, 199];
        let ids: Vec<u64> = (0..200u64)
            .filter(|i| !keep.contains(i))
            .map(|i| r.first_delivery_id + i)
            .collect();
        sub.ack(&ids).unwrap();
        assert_eq!(group_stats(&b, "st", group).in_flight, 3);
        if wait_ms > 0 {
            // Compacted by the reaper tick meanwhile; then one straggler acked by id.
            tokio::time::sleep(Duration::from_millis(wait_ms)).await;
            assert_eq!(group_stats(&b, "st", group).in_flight, 3);
        }
        drop(sub);
        let mut sub = b
            .subscribe("st", group, info("c", 2), 1000, StartFrom::Earliest)
            .unwrap();
        let got = take(&mut sub, 100, 1000).await;
        let mut p: Vec<String> = got
            .iter()
            .map(|d| String::from_utf8(d.message.payload.to_vec()).unwrap())
            .collect();
        p.sort();
        assert_eq!(p, vec!["s100", "s199", "s3"], "{group}");
        sub.ack(&got.iter().map(|d| d.delivery_id).collect::<Vec<_>>())
            .unwrap();
        assert_eq!(group_stats(&b, "st", group).lag, 0);
        drop(sub);
        b.shutdown().await;
    }
}

/// Catch-up from disk with the recent cache disabled: raw subscriptions read whole
/// envelopes (read-ahead), and a timestamp start (which the raw fast path declines) still
/// gets every message, through decoded read-ahead.
#[tokio::test]
async fn raw_catch_up_from_disk_and_timestamp_start() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.partition_cache_bytes = 0;
    let b = Broker::open(cfg).await.unwrap();
    b.create_channel(channel("d", ChannelMode::Stream, 2))
        .unwrap();
    publish_n(&b, "d", 3000, "d").await;
    let mode = SubMode {
        cumulative: false,
        raw_records: true,
    };
    for (group, start) in [
        ("raw", StartFrom::Earliest),
        ("ts", StartFrom::TimestampMs(1)),
    ] {
        let mut sub = b
            .subscribe_with("d", group, info("c", 1), 5000, StartFrom::Earliest, mode)
            .unwrap();
        if group == "ts" {
            b.reset_offsets("d", group, start).unwrap();
        }
        let mut seen = BTreeSet::new();
        let mut raw_frames = 0;
        while seen.len() < 3000 {
            let (out, raw) = take_frames(&mut sub, 256, 3000).await;
            assert!(
                !out.is_empty() || !raw.is_empty(),
                "{group}: progress at {}",
                seen.len()
            );
            let mut ranges = Vec::new();
            for r in &raw {
                raw_frames += 1;
                for i in 0..u64::from(r.count) {
                    assert!(seen.insert((r.partition, r.from_offset + i)));
                }
                ranges.push(DeliveryRange {
                    first: r.first_delivery_id,
                    count: r.count,
                });
            }
            sub.ack_ranges(&ranges).unwrap();
            for d in &out {
                seen.insert((d.message.partition, d.message.offset));
            }
            sub.ack(&out.iter().map(|d| d.delivery_id).collect::<Vec<_>>())
                .unwrap();
        }
        if group == "raw" {
            assert!(raw_frames > 0, "raw frames from disk read-ahead");
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(group_stats(&b, "d", group).lag, 0, "{group}");
        drop(sub);
    }
    assert!(group_stats(&b, "d", "raw").catchup_read_bytes_total > 0);
    b.shutdown().await;
}
