//! v0.7 replication of raw fetch payloads: wire corruption, mixed-version followers.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::*;
use fluxmq_core::FsyncPolicy;
use fluxmq_replication::ReplicaEndpoint;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A TCP proxy in front of a replication listener that flips one byte inside the record
/// payload of the first `budget` fetch responses carrying records (never in frame headers:
/// the replication protocol relies on TCP/TLS for those, like Kafka's).
struct CorruptingProxy {
    addr: SocketAddr,
    corrupted: Arc<AtomicUsize>,
}

impl CorruptingProxy {
    async fn start(upstream: SocketAddr, budget: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let corrupted = Arc::new(AtomicUsize::new(0));
        let c = corrupted.clone();
        tokio::spawn(async move {
            while let Ok((down, _)) = listener.accept().await {
                let Ok(up) = TcpStream::connect(upstream).await else {
                    continue;
                };
                let c = c.clone();
                tokio::spawn(async move {
                    let (mut dr, mut dw) = down.into_split();
                    let (mut ur, mut uw) = up.into_split();
                    tokio::spawn(async move {
                        let _ = tokio::io::copy(&mut dr, &mut uw).await;
                    });
                    // Upstream → follower, frame by frame:
                    // u32 BE body_len | u64 BE req_id | u32 BE header_len | header | payload.
                    loop {
                        let mut len = [0u8; 4];
                        if ur.read_exact(&mut len).await.is_err() {
                            return;
                        }
                        let body_len = u32::from_be_bytes(len) as usize;
                        let mut body = vec![0u8; body_len];
                        if ur.read_exact(&mut body).await.is_err() {
                            return;
                        }
                        let hlen = u32::from_be_bytes(body[8..12].try_into().unwrap()) as usize;
                        let payload = 12 + hlen..body_len;
                        if payload.len() > 64 {
                            let n = c.load(Ordering::SeqCst);
                            if n < budget {
                                c.fetch_add(1, Ordering::SeqCst);
                                // Spread hits over headers, offsets and payload bytes.
                                let at = payload.start
                                    + (payload.len() * (2 * n + 1) / 17) % payload.len();
                                body[at] ^= 0x20;
                            }
                        }
                        if dw.write_all(&len).await.is_err() || dw.write_all(&body).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        Self { addr, corrupted }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_fetch_payloads_are_refetched_never_appended() {
    let n1 = Node::start("n1", FsyncPolicy::OsBuffered).await;
    let n2 = Node::start("n2", FsyncPolicy::OsBuffered).await;
    let nodes = [n1, n2];
    wire(&nodes);
    let [n1, n2] = nodes;
    for n in [&n1, &n2] {
        n.broker.create_channel(channel(1)).unwrap();
    }
    const BUDGET: usize = 6;
    let proxy = CorruptingProxy::start(n1.mgr.local_addr(), BUDGET).await;
    // n2 reaches n1 only through the proxy.
    n2.mgr.set_endpoints(vec![
        ReplicaEndpoint {
            node_id: "n1".into(),
            addr: proxy.addr,
        },
        ReplicaEndpoint {
            node_id: "n2".into(),
            addr: n2.mgr.local_addr(),
        },
    ]);
    let _c = auto_commit(&n1);
    n1.lead(0, 1, &[&n1, &n2], 1);
    n2.follow(0, 1, &n1);
    for i in 0..20 {
        n1.broker
            .publish_to_partition(CH, 0, msgs(i * 50..(i + 1) * 50))
            .await
            .unwrap();
    }
    eventually("follower caught up", Duration::from_secs(20), || {
        let done = n2.leo(0) == 1000;
        async move { done }
    })
    .await;
    assert_eq!(proxy.corrupted.load(Ordering::SeqCst), BUDGET);
    assert_eq!(
        n2.log(0),
        n1.log(0),
        "follower log differs from the leader's"
    );
    n2.stop().await;
    n1.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_follower_still_replicates_legacy_records() {
    // A follower registered with a pre-0.7 release is served legacy records as before.
    let nodes = cluster(2, 1, FsyncPolicy::OsBuffered).await;
    let [n1, n2] = <[Node; 2]>::try_from(nodes).ok().unwrap();
    n1.mgr
        .set_batch_capable_nodes(["n1".to_string()].into_iter().collect());
    assert!(!n1.mgr.node_supports_batches("n2"));
    assert!(n1.mgr.node_supports_batches("n1"));
    assert!(!n1.mgr.node_supports_batches("unknown"));
    let _c = auto_commit(&n1);
    n1.lead(0, 1, &[&n1, &n2], 2);
    n2.follow(0, 1, &n1);
    wait_sync("ISR full", || n1.status(0).isr.len() == 2).await;
    n1.broker
        .publish_to_partition(CH, 0, msgs(0..100))
        .await
        .unwrap();
    assert_eq!(n1.hw(0), 100);
    wait_sync("follower caught up", || n2.leo(0) == 100).await;
    assert_eq!(n2.log(0), n1.log(0));
    assert_eq!(n1.mgr.refused_old_follower_fetches(), 0);
    n2.stop().await;
    n1.stop().await;
}

// ---------------------------------------------------------------------------------------------
// Batch envelopes (v0.7)
// ---------------------------------------------------------------------------------------------

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use fluxmq_core::CoreError;
use fluxmq_proto::batch::{Compression, RecordsSection, codec, encode_record};

/// A records section of `n` keyless records `"{tag}-{i}"` (padded so compression helps).
fn section(tag: &str, n: u32, c: Compression) -> RecordsSection {
    let mut b = Vec::new();
    for i in 0..n {
        let payload = format!("{tag}-{i:05}-{}", "x".repeat(150));
        encode_record(&mut b, i, None, &BTreeMap::new(), payload.as_bytes());
    }
    let (compression, body) = match codec::compress(c, &b) {
        Some(z) => (c, z),
        None => (Compression::None, b),
    };
    RecordsSection {
        compression,
        record_count: n,
        body: body.into(),
    }
}

async fn publish_batch(n: &Node, p: u32, s: RecordsSection) -> Result<(u64, u32), CoreError> {
    n.broker
        .publish_batch(CH, Some(p), s)
        .await
        .map(|(_, base, count)| (base, count))
}

fn segment_files(dir: &Path, p: u32, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let path = e.path();
        if path.is_dir() {
            if path
                .file_name()
                .is_some_and(|f| f == format!("p{p}").as_str())
                && path.parent().is_some_and(|d| d.ends_with(CH))
            {
                let mut logs: Vec<PathBuf> = std::fs::read_dir(&path)
                    .unwrap()
                    .flatten()
                    .map(|e| e.path())
                    .filter(|f| f.extension().is_some_and(|x| x == "log"))
                    .collect();
                logs.sort();
                out.extend(logs);
            } else {
                segment_files(&path, p, out);
            }
        }
    }
}

/// Partition `p`'s stored bytes on `n` (all segments, in order) — replicas append the
/// leader's bytes verbatim, so these must be identical across replicas.
fn stored_bytes(n: &Node, p: u32) -> Vec<u8> {
    let mut files = Vec::new();
    segment_files(n.dir.path(), p, &mut files);
    assert!(!files.is_empty(), "{}: no segments for p{p}", n.id);
    files
        .iter()
        .flat_map(|f| std::fs::read(f).unwrap())
        .collect()
}

/// (batches, legacy records) stored on `n` for `p`, by walking the entry framing.
fn stored_kinds(n: &Node, p: u32) -> (usize, usize) {
    let b = stored_bytes(n, p);
    let (mut batches, mut legacy, mut at) = (0, 0, 0usize);
    while at + 4 <= b.len() {
        let w = u32::from_le_bytes(b[at..at + 4].try_into().unwrap());
        if w == 0 {
            break; // preallocated tail
        }
        if fluxmq_proto::batch::is_batch(w) {
            batches += 1;
        } else {
            legacy += 1;
        }
        at += 4 + (w & !fluxmq_proto::batch::BATCH_FLAG) as usize;
    }
    (batches, legacy)
}

async fn start_small_segments(id: &str) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = broker_config(dir.path(), id, FsyncPolicy::OsBuffered);
    cfg.segment_bytes = 8 * 1024;
    let broker = fluxmq_core::Broker::open(cfg).await.unwrap();
    let mgr = fluxmq_replication::ReplicationManager::start(broker.clone(), repl_config(id))
        .await
        .unwrap();
    Node {
        id: id.to_string(),
        dir,
        broker,
        mgr,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_legacy_and_compressed_batches_replicate_verbatim() {
    let mut nodes = Vec::new();
    for i in 1..=3 {
        nodes.push(start_small_segments(&format!("n{i}")).await);
    }
    wire(&nodes);
    for n in &nodes {
        n.broker.create_channel(channel(1)).unwrap();
    }
    let (a, b, c) = (&nodes[0], &nodes[1], &nodes[2]);
    let _ctl = auto_commit(a);
    a.lead(0, 1, &[a, b, c], 2);
    b.follow(0, 1, a);
    c.follow(0, 1, a);
    wait_sync("ISR full", || a.status(0).isr.len() == 3).await;
    // Replicated mode starts with legacy writes (rolling-upgrade default).
    assert_eq!(a.mgr.batch_writes(CH, 0), Some(false));
    let mut expect = 0u64;
    a.broker
        .publish_to_partition(CH, 0, msgs(0..50))
        .await
        .unwrap();
    expect += 50;
    // Batch writes off: a PublishBatch is stored as legacy records.
    let (base, n) = publish_batch(a, 0, section("legacy-batch", 20, Compression::None))
        .await
        .unwrap();
    assert_eq!((base, n), (expect, 20));
    expect += 20;
    assert_eq!(stored_kinds(a, 0), (0, 70));
    a.mgr.set_batch_writes(CH, 0, true).unwrap();
    for (i, comp) in [Compression::None, Compression::Lz4, Compression::Zstd]
        .into_iter()
        .cycle()
        .take(60)
        .enumerate()
    {
        let k = 1 + (i as u32 * 7) % 40;
        let (base, n) = publish_batch(a, 0, section(&format!("b{i}"), k, comp))
            .await
            .unwrap();
        assert_eq!((base, n), (expect, k));
        expect += u64::from(k);
        // Acked: the whole batch is below the HW, which sits on an envelope boundary.
        assert!(a.hw(0) >= expect);
        if i % 10 == 9 {
            a.broker
                .publish_to_partition(CH, 0, msgs(0..5))
                .await
                .unwrap();
            expect += 5;
        }
    }
    // With batch writes on, plain `Publish` requests are stored as envelopes too.
    assert_eq!(stored_kinds(a, 0), (60 + 6, 70));
    for f in [b, c] {
        wait_sync("follower caught up", || f.leo(0) == expect).await;
    }
    let leader_bytes = stored_bytes(a, 0);
    for f in [b, c] {
        assert_eq!(stored_bytes(f, 0), leader_bytes, "{} not verbatim", f.id);
        assert_eq!(f.log(0), a.log(0));
    }
    let mut files = Vec::new();
    segment_files(a.dir.path(), 0, &mut files);
    assert!(files.len() > 2, "test should span several segments");
    for n in nodes {
        n.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn hw_and_acks_fall_on_envelope_boundaries() {
    let n = Arc::new(cluster(3, 1, FsyncPolicy::OsBuffered).await);
    let _ctl = auto_commit(&n[0]);
    {
        let (a, b, c) = (&n[0], &n[1], &n[2]);
        a.lead(0, 1, &[a, b, c], 2);
        b.follow(0, 1, a);
        c.follow(0, 1, a);
        wait_sync("ISR full", || a.status(0).isr.len() == 3).await;
        a.mgr.set_batch_writes(CH, 0, true).unwrap();
    }
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler = {
        let (n, stop) = (n.clone(), stop.clone());
        tokio::spawn(async move {
            let mut seen = BTreeSet::new();
            while !stop.load(Ordering::Relaxed) {
                for x in n.iter() {
                    seen.insert(x.hw(0));
                    seen.insert(x.leo(0));
                }
                tokio::task::yield_now().await;
            }
            seen
        })
    };
    let mut tasks = Vec::new();
    for t in 0..4u32 {
        let n = n.clone();
        tasks.push(tokio::spawn(async move {
            let mut out = Vec::new();
            for i in 0..40u32 {
                let k = 1 + (t * 13 + i * 7) % 50;
                let comp = [Compression::None, Compression::Lz4][(i % 2) as usize];
                let (base, cnt) = publish_batch(&n[0], 0, section(&format!("t{t}"), k, comp))
                    .await
                    .unwrap();
                // Ack implies the ISR holds the whole envelope.
                assert!(n[0].hw(0) >= base + u64::from(cnt));
                out.push((base, cnt));
            }
            out
        }));
    }
    let mut boundaries = BTreeSet::from([0u64]);
    let mut total = 0u64;
    for t in tasks {
        for (base, cnt) in t.await.unwrap() {
            boundaries.insert(base);
            boundaries.insert(base + u64::from(cnt));
            total += u64::from(cnt);
        }
    }
    for x in n.iter() {
        wait_sync("converged", || x.leo(0) == total && x.hw(0) == total).await;
    }
    stop.store(true, Ordering::Relaxed);
    let seen = sampler.await.unwrap();
    let off: Vec<_> = seen.difference(&boundaries).collect();
    assert!(off.is_empty(), "HW/LEO inside an envelope: {off:?}");
    let leader = stored_bytes(&n[0], 0);
    assert_eq!(stored_bytes(&n[1], 0), leader);
    assert_eq!(stored_bytes(&n[2], 0), leader);
    let n = Arc::try_unwrap(n).ok().unwrap();
    for x in n {
        x.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_crash_with_torn_batch_recovers_and_refetches() {
    let nodes = cluster(2, 1, FsyncPolicy::OsBuffered).await;
    let [a, b] = <[Node; 2]>::try_from(nodes).ok().unwrap();
    let _ctl = auto_commit(&a);
    a.lead(0, 1, &[&a, &b], 1);
    b.follow(0, 1, &a);
    a.mgr.set_batch_writes(CH, 0, true).unwrap();
    let mut expect = 0;
    for i in 0..10 {
        expect += u64::from(
            publish_batch(&a, 0, section(&format!("pre{i}"), 30, Compression::Lz4))
                .await
                .unwrap()
                .1,
        );
    }
    wait_sync("b caught up", || b.leo(0) == expect).await;
    // Crash b, leaving a torn half of the next envelope at the tail of its active segment.
    let b_id = b.id.clone();
    let dir = b.stop().await;
    let next = publish_batch(&a, 0, section("next", 30, Compression::None))
        .await
        .unwrap();
    expect += u64::from(next.1);
    let mut files = Vec::new();
    segment_files(dir.path(), 0, &mut files);
    let active = files.last().unwrap().clone();
    let leader_tail = {
        let all = stored_bytes(&a, 0);
        let env = codec::verify_envelope(&all[all.len() - next_len(&all)..]).unwrap();
        all[all.len() - env.total_len()..all.len() - env.total_len() / 2].to_vec()
    };
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&active)
            .unwrap();
        f.write_all(&leader_tail).unwrap();
    }
    let b = Node::start_in(&b_id, dir, FsyncPolicy::OsBuffered).await;
    wire_pair(&a, &b);
    b.follow(0, 2, &a);
    a.lead(0, 2, &[&a, &b], 1);
    a.mgr
        .isr_committed(CH, 0, 2, vec![a.id.clone(), b.id.clone()]);
    for i in 0..5 {
        expect += u64::from(
            publish_batch(&a, 0, section(&format!("post{i}"), 25, Compression::Zstd))
                .await
                .unwrap()
                .1,
        );
    }
    wait_sync("b recovered and caught up", || b.leo(0) == expect).await;
    assert_eq!(stored_bytes(&b, 0), stored_bytes(&a, 0));
    assert_eq!(b.log(0), a.log(0));
    b.stop().await;
    a.stop().await;
}

/// Total length of the last envelope in `all` (walks the framing from the start).
fn next_len(all: &[u8]) -> usize {
    let mut at = 0usize;
    let mut last = 0usize;
    while at + 4 <= all.len() {
        let w = u32::from_le_bytes(all[at..at + 4].try_into().unwrap());
        if w == 0 {
            break;
        }
        last = 4 + (w & !fluxmq_proto::batch::BATCH_FLAG) as usize;
        at += last;
    }
    last
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_change_mid_batches_truncates_whole_envelopes() {
    let nodes = cluster(2, 1, FsyncPolicy::OsBuffered).await;
    let [a, b] = <[Node; 2]>::try_from(nodes).ok().unwrap();
    a.lead(0, 1, &[&a, &b], 1);
    b.follow(0, 1, &a);
    a.mgr.set_batch_writes(CH, 0, true).unwrap();
    let mut expect = 0;
    for i in 0..5 {
        expect += u64::from(
            publish_batch(&a, 0, section(&format!("common{i}"), 20, Compression::None))
                .await
                .unwrap()
                .1,
        );
    }
    wait_sync("b caught up", || b.leo(0) == expect).await;
    // b stops following; a appends batches that are never acked to b.
    b.offline(0);
    let a_broker = a.broker.clone();
    let lost = tokio::spawn(async move {
        for i in 0..3 {
            let _ = a_broker
                .publish_batch(
                    CH,
                    Some(0),
                    section(&format!("lost{i}"), 17, Compression::Lz4),
                )
                .await;
        }
    });
    wait_sync("divergent batches on a", || a.leo(0) >= expect + 17).await;
    // Failover to b (epoch 2): it writes differently sized batches over the same offsets.
    b.lead(0, 2, &[&a, &b], 1);
    b.mgr
        .isr_committed(CH, 0, 2, vec![b.id.clone(), a.id.clone()]);
    b.mgr.set_batch_writes(CH, 0, true).unwrap();
    a.follow(0, 2, &b);
    let _ = lost.await;
    for i in 0..6 {
        expect += u64::from(
            publish_batch(&b, 0, section(&format!("new{i}"), 11, Compression::Zstd))
                .await
                .unwrap()
                .1,
        );
    }
    wait_sync("a truncated and caught up", || a.leo(0) == expect).await;
    assert_eq!(stored_bytes(&a, 0), stored_bytes(&b, 0));
    let log = a.log(0);
    assert_eq!(log, b.log(0));
    assert!(
        log.iter().all(|(_, _, p, _)| !p.starts_with(b"lost")),
        "divergent batches removed as whole envelopes"
    );
    b.stop().await;
    a.stop().await;
}

/// Leader overlap (v0.7): followers fetch the leader's written-but-unsynced tail. If the
/// leader then loses that tail (power cut before its fsync), nothing acked is lost and the
/// replicas reconverge whichever node leads next.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_losing_its_unsynced_tail_reconverges_both_ways() {
    for new_leader_is_old_leader in [false, true] {
        let nodes = cluster(2, 1, FsyncPolicy::OsBuffered).await;
        let [a, b] = <[Node; 2]>::try_from(nodes).ok().unwrap();
        let _ctl = auto_commit(&a);
        a.lead(0, 1, &[&a, &b], 2);
        b.follow(0, 1, &a);
        wait_sync("ISR full", || a.status(0).isr.len() == 2).await;
        a.mgr.set_batch_writes(CH, 0, true).unwrap();
        let mut acked = 0;
        for i in 0..8 {
            acked += u64::from(
                publish_batch(&a, 0, section(&format!("acked{i}"), 20, Compression::None))
                    .await
                    .unwrap()
                    .1,
            );
        }
        // One more batch b fetched (b's LEO passes it) but a "loses" after the crash: we
        // chop it off a's segment while a is down, as a power cut would before a's fsync.
        let (tail_base, tail_n) = publish_batch(&a, 0, section("tail", 20, Compression::None))
            .await
            .unwrap();
        assert_eq!(tail_base, acked);
        wait_sync("b holds the tail", || b.leo(0) == acked + u64::from(tail_n)).await;
        let a_id = a.id.clone();
        let a_dir = a.stop().await;
        let mut files = Vec::new();
        segment_files(a_dir.path(), 0, &mut files);
        let active = files.last().unwrap();
        let bytes = std::fs::read(active).unwrap();
        let used = {
            let mut at = 0usize;
            while at + 4 <= bytes.len() {
                let w = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
                if w == 0 {
                    break;
                }
                at += 4 + (w & !fluxmq_proto::batch::BATCH_FLAG) as usize;
            }
            at
        };
        let last = next_len(&bytes[..used]);
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(active)
            .unwrap();
        f.set_len((used - last) as u64).unwrap();
        drop(f);
        let a = Node::start_in(&a_id, a_dir, FsyncPolicy::OsBuffered).await;
        wire_pair(&a, &b);
        let _ctl2 = auto_commit(&a);
        let _ctl3 = auto_commit(&b);
        assert_eq!(a.leo(0), acked, "a lost exactly the unsynced tail");
        let (leader, follower) = if new_leader_is_old_leader {
            // The restarted node leads again: always under a new epoch (the controller
            // demotes a restarted leader), so b truncates the tail a lost.
            (&a, &b)
        } else {
            // b (in the ISR) leads: the never-acked tail survives, which is allowed.
            (&b, &a)
        };
        leader.lead(0, 2, &[&a, &b], 1);
        leader
            .mgr
            .isr_committed(CH, 0, 2, vec![leader.id.clone(), follower.id.clone()]);
        leader.mgr.set_batch_writes(CH, 0, true).unwrap();
        follower.follow(0, 2, leader);
        let base = leader.leo(0);
        let (nb, nn) = publish_batch(leader, 0, section("after", 13, Compression::Lz4))
            .await
            .unwrap();
        assert_eq!(nb, base);
        let end = base + u64::from(nn);
        wait_sync("reconverged", || follower.leo(0) == end).await;
        assert_eq!(stored_bytes(&a, 0), stored_bytes(&b, 0));
        let log = a.log(0);
        assert_eq!(log, b.log(0));
        // Every acked record is there on both.
        assert!(
            log[..acked as usize]
                .iter()
                .all(|(_, _, p, _)| p.starts_with(b"acked"))
        );
        let tail_survived = log.iter().any(|(_, _, p, _)| p.starts_with(b"tail"));
        assert_eq!(tail_survived, !new_leader_is_old_leader);
        b.stop().await;
        a.stop().await;
    }
}

/// A follower that doesn't declare batch support in its handshake (a release before v0.7)
/// gets the legacy records before the first envelope, then a refusal — never an envelope.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn envelopes_are_never_served_to_an_old_follower() {
    use fluxmq_replication::protocol::{
        FetchReq, PROTOCOL_VERSION, Request, Response, read_frame, write_frame,
    };
    let nodes = cluster(2, 1, FsyncPolicy::OsBuffered).await;
    let [a, b] = <[Node; 2]>::try_from(nodes).ok().unwrap();
    // b exists only as a replica id; a fake v0.6 follower speaks for it. b leaves the ISR
    // (lag) so a's publishes are acked alone.
    b.offline(0);
    let _ctl = auto_commit(&a);
    a.lead(0, 1, &[&a, &b], 1);
    a.broker
        .publish_to_partition(CH, 0, msgs(0..10))
        .await
        .unwrap();
    a.mgr.set_batch_writes(CH, 0, true).unwrap();
    publish_batch(&a, 0, section("env", 10, Compression::None))
        .await
        .unwrap();
    let mut s = TcpStream::connect(a.mgr.local_addr()).await.unwrap();
    let hello = Request::Hello {
        node_id: b.id.clone(),
        version: PROTOCOL_VERSION,
        secret: Some(SECRET.into()),
    };
    write_frame(&mut s, 0, &hello, &[]).await.unwrap(); // no capabilities: v0.6
    let (_, r, caps) = read_frame::<_, Response>(&mut s).await.unwrap().unwrap();
    assert!(matches!(r, Response::HelloOk { .. }));
    assert!(fluxmq_replication::protocol::Capabilities::decode(&caps).batch_records());
    let fetch = |offset| {
        Request::Fetch(FetchReq {
            channel: CH.into(),
            partition: 0,
            epoch: 1,
            fetch_offset: offset,
            max_bytes: 1 << 20,
            max_wait_ms: 10,
            state: Default::default(),
        })
    };
    write_frame(&mut s, 1, &fetch(0), &[]).await.unwrap();
    let (_, r, payload) = read_frame::<_, Response>(&mut s).await.unwrap().unwrap();
    let Response::Fetch(Ok(f)) = r else {
        panic!("{r:?}")
    };
    assert_eq!(
        (f.base_offset, f.record_count),
        (0, 10),
        "legacy prefix only"
    );
    assert!(!fluxmq_replication::chunk::has_batches(&payload));
    write_frame(&mut s, 2, &fetch(10), &[]).await.unwrap();
    let (_, r, payload) = read_frame::<_, Response>(&mut s).await.unwrap().unwrap();
    assert!(
        matches!(
            r,
            Response::Fetch(Err(fluxmq_core::FetchError::Internal(_)))
        ),
        "{r:?}"
    );
    assert!(payload.is_empty());
    assert_eq!(a.mgr.refused_old_follower_fetches(), 1);
    b.stop().await;
    a.stop().await;
}

/// Leader overlap safety (v0.7): the leader's fsync fails after the follower already
/// fetched the unsynced batch (the fsync is held 200 ms, then returns EIO). The publish
/// fails (never acked) and, because the follower read past the rollback point, the leader's
/// writer is poisoned: it never reassigns those offsets to other data. Failover to the
/// follower at a new epoch, a restart of the old leader as follower: everything acked
/// survives and the replicas reconverge byte for byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_fsync_failure_after_unsynced_fetch_loses_nothing_acked() {
    let gc = FsyncPolicy::GroupCommit {
        interval: Duration::from_millis(1),
    };
    let a = Node::start("n1", gc).await;
    let b = Node::start("n2", FsyncPolicy::OsBuffered).await;
    wire_pair(&a, &b);
    for n in [&a, &b] {
        n.broker.create_channel(channel(1)).unwrap();
    }
    let _ctl = auto_commit(&a);
    a.lead(0, 1, &[&a, &b], 2);
    b.follow(0, 1, &a);
    wait_sync("ISR full", || a.status(0).isr.len() == 2).await;
    a.mgr.set_batch_writes(CH, 0, true).unwrap();
    let mut acked = 0;
    for i in 0..5 {
        acked += u64::from(
            publish_batch(&a, 0, section(&format!("acked{i}"), 20, Compression::None))
                .await
                .unwrap()
                .1,
        );
    }
    wait_sync("b caught up", || b.leo(0) == acked).await;
    a.broker
        .replica(CH, 0)
        .unwrap()
        .fail_next_fsync_for_tests_after(Duration::from_millis(200));
    let t0 = std::time::Instant::now();
    // Samples both LEOs while the publish (and its held fsync) is in flight.
    let watch = {
        let bb = b.broker.clone();
        let ab = a.broker.clone();
        tokio::spawn(async move {
            let mut last = (0, 0);
            let mut out = Vec::new();
            for _ in 0..80 {
                let cur = (
                    bb.replica(CH, 0).unwrap().log_end_offset(),
                    ab.replica(CH, 0).unwrap().log_end_offset(),
                );
                if cur != last {
                    out.push((t0.elapsed().as_millis(), cur));
                    last = cur;
                }
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
            out
        })
    };
    let failed = publish_batch(&a, 0, section("failed", 20, Compression::None)).await;
    let seen = watch.await.unwrap();
    assert!(failed.is_err(), "publish acked despite its fsync failing");
    assert!(a.hw(0) <= acked, "HW passed an unsynced batch");
    // (elapsed ms, (b's LEO, a's LEO)) transitions.
    let (fetched_at, _) = *seen
        .iter()
        .find(|(_, (b_leo, _))| *b_leo == acked + 20)
        .unwrap_or_else(|| panic!("follower never fetched the unsynced batch: {seen:?}"));
    assert!(
        fetched_at < 150,
        "fetched only after the fsync failed: {seen:?}"
    );
    assert!(
        seen.iter().all(|(_, (_, a_leo))| *a_leo <= acked),
        "leader made the unsynced batch durable-visible: {seen:?}"
    );
    assert!(
        b.hw(0) <= acked,
        "follower adopted a HW past the unsynced batch"
    );
    // The leader rolled the batch back; the follower may already have dropped its copy too
    // (its next fetch is past the leader's written end → epoch reconciliation, same epoch).
    assert!([acked, acked + 20].contains(&b.leo(0)));
    // b holds offsets a rolled back: a is poisoned and refuses to reuse them.
    let again = publish_batch(&a, 0, section("again", 5, Compression::None)).await;
    assert!(
        again.is_err(),
        "poisoned leader accepted a publish: {again:?}"
    );
    assert!(a.leo(0) <= acked);
    // Failover to b at epoch 2 (the never-acked batch survives there, which is allowed);
    // a poisoned writer heals only by restart.
    let a_dir = a.stop().await;
    b.mgr
        .set_role(
            CH,
            0,
            fluxmq_replication::PartitionRole::Leader {
                epoch: 2,
                replicas: vec!["n1".into(), "n2".into()],
                min_isr: 1,
            },
        )
        .unwrap();
    b.mgr.isr_committed(CH, 0, 2, vec![b.id.clone()]);
    b.mgr.set_batch_writes(CH, 0, true).unwrap();
    let a = Node::start_in("n1", a_dir, gc).await;
    wire_pair(&a, &b);
    a.follow(0, 2, &b);
    let (base, n) = publish_batch(&b, 0, section("epoch2", 9, Compression::Lz4))
        .await
        .unwrap();
    assert!(
        [acked, acked + 20].contains(&base),
        "epoch 2 starts at {base}"
    );
    let end = base + u64::from(n);
    wait_sync("a reconverged", || a.leo(0) == end).await;
    assert_eq!(stored_bytes(&a, 0), stored_bytes(&b, 0));
    let log = b.log(0);
    assert_eq!(log, a.log(0));
    assert!(
        log[..acked as usize]
            .iter()
            .all(|(_, _, p, _)| p.starts_with(b"acked")),
        "acked records lost"
    );
    b.stop().await;
    a.stop().await;
}
