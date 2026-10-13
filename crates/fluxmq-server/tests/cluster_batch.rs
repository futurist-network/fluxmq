//! v0.7 record batches in a cluster: `PublishBatch` routing / forwarding, verbatim
//! replication, and the rolling-upgrade rule (batches only once every replica advertises
//! them), with in-process nodes on loopback.
//!
//! Mixed-version nodes are simulated with `fluxmq_cluster::rpc::override_features_for_tests`
//! (what a node advertises in the registry, as a v0.6 node — or a v0.6 controller dropping
//! the field — would). Each test uses its own node-id prefix: the override is process-wide.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use fluxmq_cluster::PeerSpec;
use fluxmq_cluster::rpc::override_features_for_tests;
use fluxmq_proto::batch::{self, Compression, RecordsSection, codec, encode_record};
use fluxmq_proto::cluster::{ClusterMetadata, NodeStatus};
use fluxmq_proto::model::PublishMessage;
use fluxmq_proto::wire::{ErrorCode, WireError};
use fluxmq_server::{ClusterSettings, Server, ServerConfig};
use serde_json::json;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const SECRET: &str = "test-cluster-secret";
const NODE_TIMEOUT: Duration = Duration::from_secs(3);

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Node {
    id: String,
    data: PathBuf,
    raft_port: u16,
    http_addr: SocketAddr,
    cluster: fluxmq_cluster::Cluster,
    shutdown: CancellationToken,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
}

impl Node {
    async fn start(id: &str, data: PathBuf, raft_port: u16, peers: &[PeerSpec]) -> Node {
        let mut config = ServerConfig::ephemeral(&data);
        config.ui = fluxmq_api::UiSource::Off;
        config.fsync = fluxmq_server::FsyncMode::Os;
        config.node_id = Some(id.to_string());
        config.shutdown_timeout = Duration::from_secs(5);
        config.cluster = Some(ClusterSettings {
            peers: peers.to_vec(),
            raft_addr: SocketAddr::from(([127, 0, 0, 1], raft_port)),
            replication_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            advertise_host: Some("127.0.0.1".into()),
            replication_factor: 3,
            min_isr: 2,
            cluster_secret: Some(SECRET.into()),
            node_timeout: NODE_TIMEOUT,
        });
        let server = Server::bind(config).await.expect("node binds");
        let http_addr = server.http_addr();
        let cluster = server.cluster().clone();
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(server.run(shutdown.clone().cancelled_owned()));
        Node {
            id: id.to_string(),
            data,
            raft_port,
            http_addr,
            cluster,
            shutdown,
            handle: Some(handle),
        }
    }

    async fn stop(&mut self) {
        self.shutdown.cancel();
        if let Some(h) = self.handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(20), h).await;
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.http_addr, path)
    }

    async fn metadata(&self) -> Option<ClusterMetadata> {
        reqwest::get(self.url("/api/cluster/metadata"))
            .await
            .ok()?
            .json()
            .await
            .ok()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

struct TestCluster {
    _dir: tempfile::TempDir,
    peers: Vec<PeerSpec>,
    nodes: Vec<Node>,
}

impl TestCluster {
    async fn start(prefix: &str, n: usize) -> TestCluster {
        let dir = tempfile::tempdir().unwrap();
        let ports: Vec<u16> = (0..n).map(|_| free_port()).collect();
        let peers: Vec<PeerSpec> = (0..n)
            .map(|i| PeerSpec {
                id: format!("{prefix}{}", i + 1),
                raft_addr: format!("127.0.0.1:{}", ports[i]),
            })
            .collect();
        let starts = peers.iter().enumerate().map(|(i, p)| {
            let data = dir.path().join(&p.id);
            let peers = peers.clone();
            let port = ports[i];
            let id = p.id.clone();
            async move { Node::start(&id, data, port, &peers).await }
        });
        let nodes = futures::future::join_all(starts).await;
        let c = TestCluster {
            _dir: dir,
            peers,
            nodes,
        };
        c.wait_converged(n).await;
        c
    }

    fn node(&self, id: &str) -> &Node {
        self.nodes.iter().find(|n| n.id == id).unwrap()
    }

    fn running(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter().filter(|n| n.handle.is_some())
    }

    async fn wait_converged(&self, up: usize) {
        wait_for("metadata convergence", Duration::from_secs(40), || async {
            let mut controllers = BTreeSet::new();
            for n in self.running() {
                let md = n.metadata().await?;
                let ups = md
                    .nodes
                    .iter()
                    .filter(|x| x.status == NodeStatus::Up)
                    .count();
                if ups != up || md.controller.is_none() {
                    return None;
                }
                controllers.insert(md.controller.clone());
            }
            (controllers.len() == 1).then_some(())
        })
        .await;
    }

    /// Restarts node `id` (same data dir and Raft port).
    async fn restart(&mut self, id: &str) {
        let peers = self.peers.clone();
        let n = self.nodes.iter_mut().find(|n| n.id == id).unwrap();
        n.stop().await;
        let (data, port) = (n.data.clone(), n.raft_port);
        *n = Node::start(id, data, port, &peers).await;
    }

    async fn create_channel(&self, name: &str, partitions: u32) {
        let resp = reqwest::Client::new()
            .post(self.nodes[1].url("/api/channels"))
            .json(&json!({"name": name, "mode": "stream", "partitions": partitions}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            201,
            "{}",
            resp.text().await.unwrap_or_default()
        );
        self.wait_isr_full(name, partitions).await;
    }

    async fn wait_isr_full(&self, name: &str, partitions: u32) {
        wait_for("leaders and full ISRs", Duration::from_secs(40), || async {
            for n in self.running() {
                let md = n.metadata().await?;
                let parts = md.channels.get(name)?;
                if parts.len() != partitions as usize
                    || parts.iter().any(|p| p.leader.is_none() || p.isr.len() != 3)
                {
                    return None;
                }
            }
            Some(())
        })
        .await;
    }

    async fn leaders(&self, name: &str) -> BTreeMap<u32, String> {
        let md = self.running().next().unwrap().metadata().await.unwrap();
        md.channels[name]
            .iter()
            .map(|p| (p.partition, p.leader.clone().unwrap()))
            .collect()
    }
}

async fn wait_for<T, F, Fut>(what: &str, timeout: Duration, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = f().await {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `n` records `"{tag}-{i}"`, each with `key` when given.
fn section(tag: &str, n: u32, key: Option<&str>, c: Compression) -> RecordsSection {
    let mut b = Vec::new();
    for i in 0..n {
        let payload = format!("{tag}-{i:05}-{}", "y".repeat(100));
        encode_record(&mut b, i, key, &BTreeMap::new(), payload.as_bytes());
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

fn segment_files(dir: &Path, channel: &str, p: u32) -> Vec<PathBuf> {
    fn walk(dir: &Path, channel: &str, p: u32, out: &mut Vec<PathBuf>) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for e in rd.flatten() {
            let path = e.path();
            if !path.is_dir() {
                continue;
            }
            if path
                .file_name()
                .is_some_and(|f| f == format!("p{p}").as_str())
                && path.parent().is_some_and(|d| d.ends_with(channel))
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
                walk(&path, channel, p, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, channel, p, &mut out);
    out
}

/// Stored entries of a partition on `node`: (is_batch, record count) in log order.
fn stored_entries(node: &Node, channel: &str, p: u32) -> Vec<(bool, u64)> {
    let mut out = Vec::new();
    for f in segment_files(&node.data, channel, p) {
        let b = std::fs::read(&f).unwrap_or_default();
        let mut at = 0usize;
        while at + 28 <= b.len() {
            let w = u32::from_le_bytes(b[at..at + 4].try_into().unwrap());
            if w == 0 {
                break;
            }
            let total = 4 + (w & !batch::BATCH_FLAG) as usize;
            if at + total > b.len() {
                break;
            }
            if batch::is_batch(w) {
                let n = u32::from_le_bytes(b[at + 24..at + 28].try_into().unwrap());
                out.push((true, u64::from(n)));
            } else {
                out.push((false, 1));
            }
            at += total;
        }
    }
    out
}

fn stored_bytes(node: &Node, channel: &str, p: u32) -> Vec<u8> {
    segment_files(&node.data, channel, p)
        .iter()
        .flat_map(|f| std::fs::read(f).unwrap_or_default())
        .collect()
}

/// Every running replica holds byte-identical data for every partition (verbatim
/// replication), with at least `min_records` records in total.
async fn wait_replicas_identical(
    c: &TestCluster,
    channel: &str,
    partitions: u32,
    min_records: u64,
) {
    wait_for(
        "replicas byte-identical",
        Duration::from_secs(30),
        || async {
            let mut total = 0;
            for p in 0..partitions {
                let mut it = c.running().map(|n| stored_bytes(n, channel, p));
                let first = it.next()?;
                if it.any(|b| b != first) {
                    return None;
                }
                let n0 = c.running().next()?;
                total += stored_entries(n0, channel, p)
                    .iter()
                    .map(|e| e.1)
                    .sum::<u64>();
            }
            (total >= min_records).then_some(())
        },
    )
    .await;
}

/// All stored message offsets of `channel` via `n` (fan-out read).
async fn read_all(n: &Node, channel: &str, partitions: u32) -> BTreeSet<(u32, u64)> {
    let mut out = BTreeSet::new();
    for p in 0..partitions {
        let mut offset = 0u64;
        loop {
            let resp = reqwest::get(n.url(&format!(
                "/api/channels/{channel}/messages?partition={p}&offset={offset}&limit=1000"
            )))
            .await
            .unwrap();
            assert!(resp.status().is_success(), "read failed: {}", resp.status());
            let page: serde_json::Value = resp.json().await.unwrap();
            let msgs = page["messages"].as_array().unwrap();
            for m in msgs {
                out.insert((p, m["offset"].as_u64().unwrap()));
            }
            match msgs.last() {
                Some(m) if msgs.len() == 1000 => offset = m["offset"].as_u64().unwrap() + 1,
                _ => break,
            }
        }
    }
    out
}

/// `publish_batch` retried on transient cluster errors, like an SDK.
async fn publish_batch_retry(
    n: &Node,
    channel: &str,
    partition: Option<u32>,
    s: RecordsSection,
) -> (u32, u64, u32) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match n.cluster.publish_batch(channel, partition, s.clone()).await {
            Ok(r) => return r,
            Err(e)
                if matches!(e.code, ErrorCode::NotLeader | ErrorCode::Overloaded)
                    && Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("publish_batch failed: {e:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn publish_batch_routing_and_verbatim_replication() {
    let c = TestCluster::start("r", 3).await;
    const CH: &str = "bt";
    const P: u32 = 4;
    c.create_channel(CH, P).await;
    let leaders = c.leaders(CH).await;
    let mut total = 0u64;

    // Some(p): only the leader serves it; others answer NotLeader naming the leader.
    for (&p, leader) in &leaders {
        for n in c.running() {
            let s = section(&format!("p{p}-{}", n.id), 25, None, Compression::None);
            if &n.id == leader {
                let (pp, _, cnt) = publish_batch_retry(n, CH, Some(p), s).await;
                assert_eq!((pp, cnt), (p, 25));
                total += 25;
            } else {
                let e = n.cluster.publish_batch(CH, Some(p), s).await.unwrap_err();
                assert_eq!(
                    e,
                    WireError::new(ErrorCode::NotLeader, leader.clone()),
                    "via {}",
                    n.id
                );
            }
        }
    }
    // None, keyless: routed whole (sticky) from any node, forwarded raw when remote;
    // compressed sections stay compressed.
    let mut used = BTreeSet::new();
    for i in 0..24usize {
        let n = &c.nodes[i % 3];
        let comp = [Compression::None, Compression::Lz4, Compression::Zstd][i % 3];
        let (p, _, cnt) =
            publish_batch_retry(n, CH, None, section(&format!("rr{i}"), 10, None, comp)).await;
        assert!(p < P);
        assert_eq!(cnt, 10);
        used.insert(p);
        total += 10;
    }
    assert!(
        used.len() > 1,
        "keyless batches spread over partitions: {used:?}"
    );
    // None, keyed: the partition of the key, from every node.
    let want = batch::key_partition("user-42", P);
    for n in c.running() {
        let (p, _, _) = publish_batch_retry(
            n,
            CH,
            None,
            section("keyed", 5, Some("user-42"), Compression::Lz4),
        )
        .await;
        assert_eq!(p, want);
        total += 5;
    }
    // A keyed batch spanning partitions is refused whole.
    let mut b = Vec::new();
    let other = (0..100)
        .map(|i| format!("k{i}"))
        .find(|k| batch::key_partition(k, P) != want)
        .unwrap();
    encode_record(&mut b, 0, Some("user-42"), &BTreeMap::new(), b"a");
    encode_record(&mut b, 1, Some(&other), &BTreeMap::new(), b"b");
    let spanning = RecordsSection {
        compression: Compression::None,
        record_count: 2,
        body: b.into(),
    };
    for n in c.running() {
        let e = n
            .cluster
            .publish_batch(CH, None, spanning.clone())
            .await
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::BadRequest, "{e:?}");
    }

    wait_replicas_identical(&c, CH, P, total).await;
    let seen = read_all(c.node("r1"), CH, P).await;
    assert_eq!(seen.len() as u64, total);
    // Every node advertises batches: the stored format is envelopes.
    for p in 0..P {
        let entries = stored_entries(c.node(&leaders[&p]), CH, p);
        assert!(entries.iter().any(|e| e.0), "p{p}: no envelopes stored");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn controller_dropping_features_does_not_stall_replication() {
    let c = TestCluster::start("f", 3).await;
    const CH: &str = "fd";
    const P: u32 = 3;
    c.create_channel(CH, P).await;
    let mut total = 0u64;
    for i in 0..12 {
        let n = &c.nodes[i % 3];
        total += u64::from(
            publish_batch_retry(
                n,
                CH,
                None,
                section(&format!("a{i}"), 20, None, Compression::Lz4),
            )
            .await
            .2,
        );
    }
    wait_replicas_identical(&c, CH, P, total).await;
    let batches_before: usize = (0..P)
        .map(|p| {
            stored_entries(&c.nodes[0], CH, p)
                .iter()
                .filter(|e| e.0)
                .count()
        })
        .sum();
    assert!(batches_before > 0);
    // The registry loses every node's features (as a v0.6 controller rewriting the records
    // would): leaders go back to legacy writes, but followers still replicate the envelopes
    // already written — they declare batch support on their own connections.
    for n in &c.nodes {
        override_features_for_tests(&n.id, Some(vec![]));
    }
    let isr_watch = {
        let stop = Arc::new(AtomicBool::new(false));
        let s2 = stop.clone();
        let http = c.nodes[0].http_addr;
        let h = tokio::spawn(async move {
            let mut min_isr = usize::MAX;
            while !s2.load(Ordering::Relaxed) {
                if let Ok(r) = reqwest::get(format!("http://{http}/api/cluster/metadata")).await
                    && let Ok(md) = r.json::<ClusterMetadata>().await
                    && let Some(parts) = md.channels.get(CH)
                {
                    for p in parts {
                        min_isr = min_isr.min(p.isr.len());
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            min_isr
        });
        (stop, h)
    };
    // Wait until a fresh batch lands as legacy records.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (p, _, _) = publish_batch_retry(
            &c.nodes[1],
            CH,
            None,
            section("probe", 3, None, Compression::None),
        )
        .await;
        total += 3;
        let leader = c.leaders(CH).await[&p].clone();
        if stored_entries(c.node(&leader), CH, p).last() == Some(&(false, 1)) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "leaders never went back to legacy writes"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for i in 0..12 {
        total += u64::from(
            publish_batch_retry(
                &c.nodes[i % 3],
                CH,
                None,
                section(&format!("b{i}"), 7, None, Compression::Zstd),
            )
            .await
            .2,
        );
    }
    wait_replicas_identical(&c, CH, P, total).await;
    let (stop, h) = isr_watch;
    stop.store(true, Ordering::Relaxed);
    assert_eq!(h.await.unwrap(), 3, "an ISR shrank: replication stalled");
    assert_eq!(read_all(&c.nodes[2], CH, P).await.len() as u64, total);
    for n in &c.nodes {
        override_features_for_tests(&n.id, None);
    }
}

/// Rolling upgrade v0.6 → v0.7 (simulated in process): two nodes start as "v0.6" (no
/// batch feature). Under continuous load every node restarts in turn, the old ones coming
/// back upgraded. Nothing acked is lost, nothing is stored as an envelope while any replica
/// is old, and batches are written once the last node is upgraded.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rolling_upgrade_under_load_enables_batches_last() {
    override_features_for_tests("u2", Some(vec![]));
    override_features_for_tests("u3", Some(vec![]));
    let mut c = TestCluster::start("u", 3).await;
    const CH: &str = "ru";
    const P: u32 = 3;
    c.create_channel(CH, P).await;

    // Load: alternating plain publishes and batches through every node, recording acks.
    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(std::sync::Mutex::new(BTreeSet::<(u32, u64)>::new()));
    let mut load = Vec::new();
    for t in 0..3usize {
        let (stop, acked) = (stop.clone(), acked.clone());
        let addrs: Vec<fluxmq_cluster::Cluster> =
            c.nodes.iter().map(|n| n.cluster.clone()).collect();
        load.push(tokio::spawn(async move {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let cl = &addrs[(t + i) % 3];
                i += 1;
                if i.is_multiple_of(2) {
                    let s = section(&format!("t{t}-{i}"), 8, None, Compression::Lz4);
                    if let Ok((p, base, n)) = cl.publish_batch(CH, None, s).await {
                        let mut a = acked.lock().unwrap();
                        a.extend((base..base + u64::from(n)).map(|o| (p, o)));
                    }
                } else {
                    let msgs = (0..5)
                        .map(|k| PublishMessage {
                            key: None,
                            headers: Default::default(),
                            payload: format!("legacy-{t}-{i}-{k}").into(),
                        })
                        .collect();
                    if let Ok(ids) = cl.publish(CH, None, msgs).await {
                        acked
                            .lock()
                            .unwrap()
                            .extend(ids.iter().map(|id| (id.partition, id.offset)));
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }));
    }
    let no_envelopes = |c: &TestCluster| {
        c.running()
            .all(|n| (0..P).all(|p| stored_entries(n, CH, p).iter().all(|e| !e.0)))
    };
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        no_envelopes(&c),
        "envelopes written while replicas are v0.6"
    );
    for (id, upgrade) in [("u2", true), ("u1", false), ("u3", true)] {
        let before = acked.lock().unwrap().len();
        if upgrade {
            override_features_for_tests(id, None);
        }
        c.restart(id).await;
        c.wait_converged(3).await;
        c.wait_isr_full(CH, P).await;
        wait_for("load continues", Duration::from_secs(20), || async {
            (acked.lock().unwrap().len() > before + 50).then_some(())
        })
        .await;
        if id != "u3" {
            assert!(
                no_envelopes(&c),
                "envelopes written before {id}'s upgrade finished"
            );
        }
    }
    // All upgraded: leaders switch to envelopes.
    wait_for(
        "batches enabled after the last upgrade",
        Duration::from_secs(20),
        || async {
            c.running()
                .any(|n| (0..P).any(|p| stored_entries(n, CH, p).iter().any(|e| e.0)))
                .then_some(())
        },
    )
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    stop.store(true, Ordering::Relaxed);
    for l in load {
        l.await.unwrap();
    }
    let acked = acked.lock().unwrap().clone();
    assert!(acked.len() > 300, "too little load: {}", acked.len());
    wait_replicas_identical(&c, CH, P, 0).await;
    for n in c.running() {
        let seen = read_all(n, CH, P).await;
        let lost: Vec<_> = acked.difference(&seen).take(5).collect();
        assert!(
            lost.is_empty(),
            "{}: acked but missing, e.g. {lost:?}",
            n.id
        );
    }
}
