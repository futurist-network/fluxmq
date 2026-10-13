//! v0.4 automatic rebalancing, end to end: in-process cluster nodes on loopback (same harness
//! shape as `cluster.rs`) with a short trigger interval and `node_timeout`.
//!
//! * a 4th node joins under steady publish + consume → replicas balance automatically within
//!   the threshold, no acknowledged message is lost, publishes only ever needed retries;
//! * a node stops for longer than `replace_after_secs` → its replicas are replaced (RF back);
//! * the controller stops in the middle of a plan → the new controller resumes the same plan;
//! * a dry run returns a plan and changes nothing; mode `off` never moves anything by itself;
//! * single node: `GET` reports mode off / idle, mutations are `unsupported_single_node`.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::RawConn;
use fluxmq_cluster::PeerSpec;
use fluxmq_proto::cluster::{
    ClusterMetadata, MoveState, NodeStatus, RebalanceConfig, RebalanceMode, RebalanceState,
    RebalanceStatus,
};
use fluxmq_proto::model::PublishMessage;
use fluxmq_proto::wire::{ClientRequest, ServerResponse};
use fluxmq_server::{ClusterSettings, Server, ServerConfig};
use serde_json::json;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const SECRET: &str = "test-cluster-secret";
const NODE_TIMEOUT: Duration = Duration::from_secs(3);
/// Trigger evaluation interval in these tests (30 s in production).
const INTERVAL: Duration = Duration::from_secs(1);

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Node {
    id: String,
    client_addr: SocketAddr,
    http_addr: SocketAddr,
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
        config.auto_rebalance = RebalanceMode::Full;
        config.rebalance_interval = INTERVAL;
        config.cluster = Some(ClusterSettings {
            peers: peers.to_vec(),
            raft_addr: SocketAddr::from(([127, 0, 0, 1], raft_port)),
            replication_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            advertise_host: Some("127.0.0.1".into()),
            replication_factor: 2,
            min_isr: 1,
            cluster_secret: Some(SECRET.into()),
            node_timeout: NODE_TIMEOUT,
        });
        let server = Server::bind(config).await.expect("node binds");
        let (client_addr, http_addr) = (server.client_addr(), server.http_addr());
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(server.run(shutdown.clone().cancelled_owned()));
        Node {
            id: id.to_string(),
            client_addr,
            http_addr,
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

    async fn metadata(&self) -> ClusterMetadata {
        get_json(&self.url("/api/cluster/metadata")).await
    }

    async fn rebalance(&self) -> RebalanceStatus {
        get_json(&self.url("/api/cluster/rebalance")).await
    }

    async fn put_config(&self, cfg: &RebalanceConfig) -> RebalanceStatus {
        let resp = reqwest::Client::new()
            .put(self.url("/api/cluster/rebalance/config"))
            .json(cfg)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "{}",
            resp.text().await.unwrap_or_default()
        );
        resp.json().await.unwrap()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

async fn get_json<T: serde::de::DeserializeOwned>(url: &str) -> T {
    let resp = reqwest::get(url).await.unwrap();
    assert!(resp.status().is_success(), "GET {url}: {}", resp.status());
    resp.json().await.unwrap()
}

struct TestCluster {
    dir: tempfile::TempDir,
    peers: Vec<PeerSpec>,
    nodes: Vec<Node>,
}

impl TestCluster {
    async fn start(n: usize) -> TestCluster {
        let dir = tempfile::tempdir().unwrap();
        let ports: Vec<u16> = (0..n).map(|_| free_port()).collect();
        let peers: Vec<PeerSpec> = (0..n)
            .map(|i| PeerSpec {
                id: format!("n{}", i + 1),
                raft_addr: format!("127.0.0.1:{}", ports[i]),
            })
            .collect();
        let starts = (0..n).map(|i| {
            let data = dir.path().join(format!("n{}", i + 1));
            let peers = peers.clone();
            let port = ports[i];
            async move { Node::start(&format!("n{}", i + 1), data, port, &peers).await }
        });
        let nodes = futures::future::join_all(starts).await;
        let c = TestCluster { dir, peers, nodes };
        c.wait_up(n).await;
        c
    }

    /// Starts a new node that joins through the initial seeds.
    async fn join(&mut self, id: &str) {
        let data = self.dir.path().join(id);
        let node = Node::start(id, data, free_port(), &self.peers).await;
        self.nodes.push(node);
    }

    fn node(&self, id: &str) -> &Node {
        self.nodes.iter().find(|n| n.id == id).unwrap()
    }

    fn running(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter().filter(|n| n.handle.is_some())
    }

    fn any(&self) -> &Node {
        self.running().next().unwrap()
    }

    async fn wait_up(&self, n: usize) {
        wait_for("nodes up", Duration::from_secs(40), || async {
            let md = self.any().metadata().await;
            let up = md
                .nodes
                .iter()
                .filter(|x| x.status == NodeStatus::Up)
                .count();
            (up == n && md.controller.is_some()).then_some(())
        })
        .await;
    }

    async fn controller(&self) -> String {
        wait_for("controller", Duration::from_secs(20), || async {
            self.any().metadata().await.controller
        })
        .await
    }

    async fn stop(&mut self, id: &str) {
        self.nodes
            .iter_mut()
            .find(|n| n.id == id)
            .unwrap()
            .stop()
            .await;
    }

    async fn stop_all(&mut self) {
        for n in &mut self.nodes {
            n.stop().await;
        }
    }

    async fn create_channel(&self, name: &str, partitions: u32) {
        let resp = reqwest::Client::new()
            .post(self.any().url("/api/channels"))
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
    }
}

/// Polls `f` until `Some` or `timeout`.
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

fn test_config() -> RebalanceConfig {
    RebalanceConfig {
        mode: RebalanceMode::Full,
        max_concurrent_moves: 2,
        imbalance_threshold: 1,
        replace_after_secs: 600,
        throttle_bytes_per_sec: 0,
        move_timeout_secs: 60,
    }
}

/// Replica count per node over every channel.
fn replica_counts(md: &ClusterMetadata) -> BTreeMap<String, usize> {
    let mut load: BTreeMap<String, usize> = md
        .nodes
        .iter()
        .filter(|n| n.status != NodeStatus::Down)
        .map(|n| (n.id.clone(), 0))
        .collect();
    for p in md.channels.values().flatten() {
        for r in &p.replicas {
            if let Some(c) = load.get_mut(r) {
                *c += 1;
            }
        }
    }
    load
}

/// Every partition of every channel has a leader and a full ISR.
fn healthy(md: &ClusterMetadata) -> bool {
    md.channels
        .values()
        .flatten()
        .all(|p| p.leader.is_some() && p.isr.len() == p.replicas.len())
}

/// All stored messages of `channel` as (partition, offset) via node `n` (fan-out read).
async fn read_all(n: &Node, channel: &str, partitions: u32) -> BTreeSet<(u32, u64)> {
    let mut out = BTreeSet::new();
    for p in 0..partitions {
        let mut offset = 0u64;
        loop {
            let page: serde_json::Value = wait_for("readable", Duration::from_secs(20), || async {
                let resp = reqwest::get(n.url(&format!(
                    "/api/channels/{channel}/messages?partition={p}&offset={offset}&limit=1000"
                )))
                .await
                .ok()?;
                if !resp.status().is_success() {
                    return None;
                }
                resp.json().await.ok()
            })
            .await;
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

fn msg(key: &str, body: String) -> PublishMessage {
    PublishMessage {
        key: Some(key.to_string()),
        headers: Default::default(),
        payload: Bytes::from(body),
    }
}

/// Publishes batches through one node like an SDK: every batch is retried until acknowledged
/// (transient cluster errors are expected while partitions move); a batch that is never
/// acknowledged within 30 s counts as a failure.
struct Publisher {
    stop: CancellationToken,
    state: Arc<Mutex<PubState>>,
    task: JoinHandle<()>,
}

#[derive(Default)]
struct PubState {
    acked: BTreeSet<(u32, u64)>,
    payloads: BTreeSet<String>,
    retries: u64,
    failed: u64,
}

impl Publisher {
    fn start(addr: SocketAddr, channel: &'static str, payload_bytes: usize) -> Self {
        let stop = CancellationToken::new();
        let state = Arc::new(Mutex::new(PubState::default()));
        let (s, st) = (stop.clone(), state.clone());
        let pad = "x".repeat(payload_bytes);
        let task = tokio::spawn(async move {
            // The entry node stays up in every test (it forwards to partition leaders).
            let mut conn = RawConn::connect_hello(addr, None).await;
            let mut k = 0u64;
            while !s.is_cancelled() {
                let batch: Vec<(String, PublishMessage)> = (0..10)
                    .map(|_| {
                        k += 1;
                        let body = format!("m-{k}-{pad}");
                        (format!("m-{k}"), msg(&format!("k{k}"), body))
                    })
                    .collect();
                let messages: Vec<PublishMessage> = batch.iter().map(|(_, m)| m.clone()).collect();
                let deadline = Instant::now() + Duration::from_secs(30);
                loop {
                    let r = conn
                        .request(ClientRequest::Publish {
                            channel: channel.into(),
                            partition: None,
                            messages: messages.clone(),
                        })
                        .await;
                    match r {
                        Ok(ServerResponse::PublishOk { ids }) => {
                            let mut st = st.lock().unwrap();
                            st.acked
                                .extend(ids.iter().map(|id| (id.partition, id.offset)));
                            st.payloads.extend(batch.iter().map(|(p, _)| p.clone()));
                            break;
                        }
                        _ if Instant::now() < deadline => {
                            st.lock().unwrap().retries += 1;
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        other => {
                            eprintln!("publish never acknowledged: {other:?}");
                            st.lock().unwrap().failed += 1;
                            break;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        Self { stop, state, task }
    }

    async fn wait_acked(&self, n: usize) {
        wait_for("publisher acks", Duration::from_secs(30), || async {
            (self.state.lock().unwrap().acked.len() >= n).then_some(())
        })
        .await;
    }

    async fn stop(self) -> PubState {
        self.stop.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(40), self.task).await;
        std::mem::take(&mut *self.state.lock().unwrap())
    }
}

/// An SDK consumer group over every node (cluster-aware), collecting payload ids.
struct Consumer {
    stop: CancellationToken,
    seen: Arc<Mutex<BTreeSet<String>>>,
    task: JoinHandle<()>,
}

impl Consumer {
    async fn start(seeds: Vec<String>, channel: &'static str) -> Self {
        use fluxmq_client::{Client, ClientOptions, StartFrom, SubscribeOptions};
        let client = Client::connect(ClientOptions::with_seeds(seeds, "rebalance-test"))
            .await
            .expect("sdk connect");
        let mut consumer = client
            .subscribe(
                channel,
                "audit",
                SubscribeOptions::default()
                    .start(StartFrom::Earliest)
                    .prefetch(64),
            )
            .await
            .expect("subscribe");
        let stop = CancellationToken::new();
        let seen = Arc::new(Mutex::new(BTreeSet::new()));
        let (s, sn) = (stop.clone(), seen.clone());
        let task = tokio::spawn(async move {
            let _client = client;
            loop {
                let d = tokio::select! {
                    () = s.cancelled() => return,
                    d = consumer.next() => d,
                };
                match d {
                    Some(Ok(d)) => {
                        let body = String::from_utf8_lossy(d.payload()).to_string();
                        let id: Vec<&str> = body.splitn(3, '-').take(2).collect();
                        sn.lock().unwrap().insert(id.join("-"));
                        let _ = d.ack().await;
                    }
                    Some(Err(_)) => tokio::time::sleep(Duration::from_millis(50)).await,
                    None => return,
                }
            }
        });
        Self { stop, seen, task }
    }

    fn count(&self) -> usize {
        self.seen.lock().unwrap().len()
    }

    async fn stop(self) -> BTreeSet<String> {
        self.stop.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(10), self.task).await;
        std::mem::take(&mut *self.seen.lock().unwrap())
    }
}

fn tracing_init() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();
}

/// Waits until the active plan (if any) finished and returns the status.
async fn wait_plan_finished(n: &Node, timeout: Duration) -> RebalanceStatus {
    wait_for("rebalance plan finished", timeout, || async {
        let st = n.rebalance().await;
        (st.state == RebalanceState::Idle && !st.moves.is_empty()).then_some(st)
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn new_node_is_balanced_in_under_load_without_loss() {
    tracing_init();
    let mut c = TestCluster::start(3).await;
    c.any().put_config(&test_config()).await;
    c.create_channel("orders", 6).await;

    let publisher = Publisher::start(c.node("n1").client_addr, "orders", 64);
    let seeds: Vec<String> = c.running().map(|n| n.client_addr.to_string()).collect();
    let consumer = Consumer::start(seeds, "orders").await;
    publisher.wait_acked(300).await;

    // Balanced 3-node placement. Initial placement balances leaders, not replica counts, so
    // depending on where the two system channels landed the replica spread can start at 2;
    // mode `full` then evens it out. Wait for that (not a property of this test).
    wait_for(
        "3-node placement balanced",
        Duration::from_secs(90),
        || async {
            let st = c.any().rebalance().await;
            (st.imbalance <= 1 && st.state == RebalanceState::Idle).then_some(())
        },
    )
    .await;

    c.join("n4").await;
    c.wait_up(4).await;
    // The controller plans once n4 has been up for `rebalance_stable`, then moves replicas.
    let done = wait_for("n4 balanced in", Duration::from_secs(90), || async {
        let st = c.node("n2").rebalance().await;
        let md = c.node("n2").metadata().await;
        let counts = replica_counts(&md);
        let spread = counts.values().max()? - counts.values().min()?;
        (st.state == RebalanceState::Idle
            && !st.moves.is_empty()
            && counts.get("n4").copied().unwrap_or(0) > 0
            && spread <= 1
            && healthy(&md))
        .then_some(st)
    })
    .await;
    assert!(
        done.moves.iter().all(|m| m.state == MoveState::Done),
        "{:#?}",
        done.moves
    );
    assert!(
        done.trigger.as_deref().unwrap_or("").contains("n4"),
        "{:?}",
        done.trigger
    );
    assert!(done.imbalance <= 1);
    assert_eq!(done.under_replicated, 0);
    let n4 = done.load.iter().find(|l| l.node == "n4").unwrap();
    assert!(n4.replicas >= 3, "{:?}", done.load);

    // Keep publishing a little on the new placement, then check nothing acked was lost.
    let acked_before = publisher.state.lock().unwrap().acked.len();
    publisher.wait_acked(acked_before + 200).await;
    let published = publisher.stop().await;
    eprintln!(
        "published {} messages, {} retried attempts, {} failed",
        published.acked.len(),
        published.retries,
        published.failed
    );
    assert_eq!(published.failed, 0, "publishes failed beyond retries");
    let stored = read_all(c.node("n3"), "orders", 6).await;
    let lost: Vec<_> = published.acked.difference(&stored).collect();
    assert!(
        lost.is_empty(),
        "{} acked messages lost: {lost:?}",
        lost.len()
    );

    // The consumer group receives every acknowledged message (at-least-once).
    wait_for("consumer caught up", Duration::from_secs(60), || async {
        (consumer.count() >= published.payloads.len()).then_some(())
    })
    .await;
    let seen = consumer.stop().await;
    let missing: Vec<_> = published.payloads.difference(&seen).take(10).collect();
    assert!(missing.is_empty(), "never consumed: {missing:?}");

    // The events feed recorded the plan (the controller appends to `$system.events`; any
    // node serves the feed, proxying to that channel's partition leader).
    let feed = wait_for("rebalance events", Duration::from_secs(20), || async {
        let resp = reqwest::get(c.node("n4").url("/api/events/feed?limit=1000"))
            .await
            .ok()?;
        let v: serde_json::Value = resp.json().await.ok()?;
        let s = v.to_string();
        (s.contains("rebalance_started")
            && s.contains("partition_moved")
            && s.contains("rebalance_completed"))
        .then_some(s)
    })
    .await;
    assert!(feed.contains("n4"));
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn lost_node_replicas_are_replaced() {
    tracing_init();
    let mut c = TestCluster::start(3).await;
    let mut cfg = test_config();
    cfg.replace_after_secs = 3;
    c.any().put_config(&cfg).await;
    c.create_channel("orders", 6).await;
    let controller = c.controller().await;
    let victim = ["n3", "n2", "n1"]
        .into_iter()
        .find(|n| *n != controller)
        .unwrap()
        .to_string();
    let entry = ["n1", "n2", "n3"]
        .into_iter()
        .find(|n| *n != victim)
        .unwrap()
        .to_string();
    let publisher = Publisher::start(c.node(&entry).client_addr, "orders", 64);
    publisher.wait_acked(200).await;
    let md = c.node(&entry).metadata().await;
    assert!(
        md.channels["orders"]
            .iter()
            .any(|p| p.replicas.contains(&victim))
    );

    c.stop(&victim).await;
    // Down → replaced after 3 s (+ trigger interval): RF 2 on the two remaining nodes.
    let st = wait_for("RF restored", Duration::from_secs(60), || async {
        let md = c.node(&entry).metadata().await;
        let ok =
            md.channels.values().flatten().all(|p| {
                p.replicas.len() == 2 && !p.replicas.contains(&victim) && p.isr.len() == 2
            }) && md.channels.values().flatten().all(|p| p.leader.is_some());
        if !ok {
            return None;
        }
        let st = c.node(&entry).rebalance().await;
        (st.state == RebalanceState::Idle).then_some(st)
    })
    .await;
    assert_eq!(st.under_replicated, 0, "{st:?}");
    assert!(
        st.trigger
            .as_deref()
            .is_some_and(|t| t.contains(&format!("node {victim} down"))),
        "{:?}",
        st.trigger
    );
    assert!(
        st.moves
            .iter()
            .all(|m| m.state == MoveState::Done && !m.to_replicas.contains(&victim))
    );

    let published = publisher.stop().await;
    assert_eq!(published.failed, 0);
    let stored = read_all(c.node(&entry), "orders", 6).await;
    let lost: Vec<_> = published.acked.difference(&stored).collect();
    assert!(
        lost.is_empty(),
        "{} acked messages lost: {lost:?}",
        lost.len()
    );
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn controller_failover_resumes_the_plan() {
    tracing_init();
    let mut c = TestCluster::start(3).await;
    // One move at a time, so the plan is still running when the controller goes away.
    let mut cfg = test_config();
    cfg.max_concurrent_moves = 1;
    cfg.mode = RebalanceMode::Off;
    c.any().put_config(&cfg).await;
    c.create_channel("orders", 6).await;
    let controller = c.controller().await;
    let entry = ["n1", "n2", "n3"]
        .into_iter()
        .find(|n| *n != controller)
        .unwrap()
        .to_string();
    // Some data per partition so new replicas have something to catch up on.
    let publisher = Publisher::start(c.node(&entry).client_addr, "orders", 1024);
    publisher.wait_acked(3000).await;

    c.join("n4").await;
    c.wait_up(4).await;
    // A manual rebalance (mode off: nothing automatic interferes).
    let resp = reqwest::Client::new()
        .post(c.node(&entry).url("/api/cluster/rebalance"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "{}",
        resp.text().await.unwrap_or_default()
    );
    let started: RebalanceStatus = resp.json().await.unwrap();
    assert_eq!(started.state, RebalanceState::Running, "{started:?}");
    assert!(started.moves.len() >= 2, "{:#?}", started.moves);
    let total = started.moves.len();
    // A second executing request conflicts; a dry run is always allowed.
    let again = reqwest::Client::new()
        .post(c.node(&entry).url("/api/cluster/rebalance"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(again.status(), 409);
    let body: serde_json::Value = again.json().await.unwrap();
    assert_eq!(body["error"], "conflict");
    let dry = reqwest::Client::new()
        .post(c.node(&entry).url("/api/cluster/rebalance"))
        .json(&json!({"dry_run": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(dry.status(), 200);
    let plan_started_at = started.started_at_ms;

    // Stop the controller while the plan is in flight.
    wait_for("plan in flight", Duration::from_secs(30), || async {
        let st = c.node(&entry).rebalance().await;
        st.moves
            .iter()
            .any(|m| m.state != MoveState::Pending)
            .then_some(())
    })
    .await;
    let mid = c.node(&entry).rebalance().await;
    assert_eq!(
        mid.state,
        RebalanceState::Running,
        "plan finished too early: {mid:?}"
    );
    c.stop(&controller).await;

    let new_controller = wait_for("new controller", Duration::from_secs(30), || async {
        c.node(&entry)
            .metadata()
            .await
            .controller
            .filter(|n| *n != controller)
    })
    .await;
    eprintln!("controller {controller} stopped mid-plan; {new_controller} took over");
    let done = wait_plan_finished(c.node(&entry), Duration::from_secs(90)).await;
    // The same plan (same start time), resumed to the end.
    assert_eq!(done.started_at_ms, plan_started_at);
    assert_eq!(done.moves.len(), total);
    assert_eq!(done.trigger.as_deref(), Some("manual"));
    for m in &done.moves {
        // A move whose target was the stopped controller can't complete; everything else did.
        if m.to_replicas.contains(&controller) && !m.from_replicas.contains(&controller) {
            continue;
        }
        assert_eq!(m.state, MoveState::Done, "{m:#?}");
    }
    let md = c.node(&entry).metadata().await;
    assert!(replica_counts(&md).get("n4").copied().unwrap_or(0) > 0);
    wait_for("leaders everywhere", Duration::from_secs(30), || async {
        let md = c.node(&entry).metadata().await;
        md.channels
            .values()
            .flatten()
            .all(|p| p.leader.as_ref().is_some_and(|l| *l != controller))
            .then_some(())
    })
    .await;

    let published = publisher.stop().await;
    assert_eq!(published.failed, 0);
    let stored = read_all(c.node(&entry), "orders", 6).await;
    let lost: Vec<_> = published.acked.difference(&stored).collect();
    assert!(
        lost.is_empty(),
        "{} acked messages lost: {lost:?}",
        lost.len()
    );
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn dry_run_and_mode_off_change_nothing() {
    tracing_init();
    let mut c = TestCluster::start(3).await;
    let mut cfg = test_config();
    cfg.mode = RebalanceMode::Off;
    let st = c.any().put_config(&cfg).await;
    assert_eq!(st.config.mode, RebalanceMode::Off);
    c.create_channel("orders", 6).await;
    c.join("n4").await;
    c.wait_up(4).await;
    // The controller creates the system channels on its own 2 s ensure tick, possibly after
    // n4 joined: wait for them so the snapshot is complete (test race, not a rebalance).
    let before = wait_for(
        "system channels created",
        Duration::from_secs(30),
        || async {
            let ch = c.node("n2").metadata().await.channels;
            (ch.contains_key("$system.events") && ch.contains_key("$system.notifications"))
                .then_some(ch)
        },
    )
    .await;

    // Mode off: n4 stays empty well past `rebalance_stable` + several intervals.
    tokio::time::sleep(NODE_TIMEOUT + Duration::from_secs(4) + INTERVAL * 4).await;
    let st = c.node("n3").rebalance().await;
    assert_eq!(st.state, RebalanceState::Idle);
    assert!(st.moves.is_empty(), "{:#?}", st.moves);
    assert!(st.imbalance > 1, "{st:?}");
    assert_eq!(c.node("n2").metadata().await.channels, before);

    // Dry run (through a non-controller node too): a plan, and nothing changes.
    for n in ["n1", "n4"] {
        let resp = reqwest::Client::new()
            .post(c.node(n).url("/api/cluster/rebalance"))
            .json(&json!({"dry_run": true, "channels": ["orders"]}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "{}",
            resp.text().await.unwrap_or_default()
        );
        let plan: RebalanceStatus = resp.json().await.unwrap();
        assert_eq!(plan.state, RebalanceState::Idle);
        assert!(!plan.moves.is_empty());
        assert!(plan.moves.iter().all(|m| m.state == MoveState::Pending));
        assert!(plan.moves.iter().all(|m| m.channel == "orders"));
        let projected_n4 = plan.projected_load.iter().find(|l| l.node == "n4").unwrap();
        assert!(projected_n4.replicas >= 3, "{:?}", plan.projected_load);
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(c.node("n2").metadata().await.channels, before);
    let st = c.node("n1").rebalance().await;
    assert_eq!(st.state, RebalanceState::Idle);
    assert!(st.moves.is_empty());

    // Validation and state errors.
    let http = reqwest::Client::new();
    let bad = http
        .put(c.node("n2").url("/api/cluster/rebalance/config"))
        .json(
            &json!({"mode": "full", "max_concurrent_moves": 0, "imbalance_threshold": 1,
                      "replace_after_secs": 600, "throttle_bytes_per_sec": 0,
                      "move_timeout_secs": 60}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
    let none = http
        .post(c.node("n2").url("/api/cluster/rebalance/pause"))
        .send()
        .await
        .unwrap();
    assert_eq!(none.status(), 400, "nothing to pause");
    let unknown = http
        .post(c.node("n2").url("/api/cluster/rebalance"))
        .json(&json!({"dry_run": true, "channels": ["nope"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(unknown.status(), 404);

    // Manual plan, one move at a time: pause, resume, cancel. Pending moves become
    // `cancelled`; in-flight ones finish; nothing is left half-moved.
    let mut cfg = cfg.clone();
    cfg.max_concurrent_moves = 1;
    c.node("n1").put_config(&cfg).await;
    let post = |path: &'static str, body: serde_json::Value| {
        let url = c.node("n3").url(path);
        async move {
            let resp = reqwest::Client::new()
                .post(url)
                .json(&body)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200, "{path}");
            resp.json::<RebalanceStatus>().await.unwrap()
        }
    };
    let st = post("/api/cluster/rebalance", json!({})).await;
    assert_eq!(st.state, RebalanceState::Running);
    assert!(st.moves.len() >= 3, "{:#?}", st.moves);
    let st = post("/api/cluster/rebalance/pause", json!({})).await;
    assert_eq!(st.state, RebalanceState::Paused);
    let st = post("/api/cluster/rebalance/resume", json!({})).await;
    assert_eq!(st.state, RebalanceState::Running);
    let st = post("/api/cluster/rebalance/cancel", json!({})).await;
    assert_eq!(st.state, RebalanceState::Cancelling);
    let done = wait_plan_finished(c.node("n2"), Duration::from_secs(60)).await;
    assert!(
        done.moves
            .iter()
            .all(|m| matches!(m.state, MoveState::Done | MoveState::Cancelled)),
        "{:#?}",
        done.moves
    );
    assert!(
        done.moves.iter().any(|m| m.state == MoveState::Cancelled),
        "{:#?}",
        done.moves
    );
    wait_for("healthy after cancel", Duration::from_secs(30), || async {
        healthy(&c.node("n2").metadata().await).then_some(())
    })
    .await;
    c.stop_all().await;
}

#[tokio::test]
async fn single_node_reports_off_and_rejects_mutations() {
    let srv = common::TestServer::start().await;
    let st: RebalanceStatus = srv.get_json("/api/cluster/rebalance").await.pipe();
    assert_eq!(st.config.mode, RebalanceMode::Off);
    assert_eq!(st.state, RebalanceState::Idle);
    assert_eq!(st.load.len(), 1);
    for (method, path) in [
        (reqwest::Method::POST, "/api/cluster/rebalance"),
        (reqwest::Method::POST, "/api/cluster/rebalance/pause"),
        (reqwest::Method::POST, "/api/cluster/rebalance/resume"),
        (reqwest::Method::POST, "/api/cluster/rebalance/cancel"),
    ] {
        let resp = srv.req(method, path).send().await.unwrap();
        assert_eq!(resp.status(), 400, "{path}");
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            body["error"],
            fluxmq_proto::api::ERROR_UNSUPPORTED_SINGLE_NODE,
            "{path}"
        );
    }
    let resp = srv
        .req(reqwest::Method::PUT, "/api/cluster/rebalance/config")
        .json(&RebalanceConfig::default())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    srv.stop().await;
}

trait Pipe {
    fn pipe<T: serde::de::DeserializeOwned>(self) -> T;
}

impl Pipe for serde_json::Value {
    fn pipe<T: serde::de::DeserializeOwned>(self) -> T {
        serde_json::from_value(self).unwrap()
    }
}

/// `$system.events` has one partition on RF=2 of 3 nodes, so one node holds no copy. Every
/// node still serves `GET /api/events/feed` (proxied to the partition leader): the same
/// cursors as the leader for `tail`, and a long-poll on the node without the replica wakes up
/// on a new event.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn events_feed_is_served_by_every_node() {
    tracing_init();
    let mut c = TestCluster::start(3).await;
    let mut cfg = test_config();
    cfg.mode = RebalanceMode::Off;
    c.any().put_config(&cfg).await;
    let (leader, outsider) = wait_for("system events placed", Duration::from_secs(30), || async {
        let md = c.any().metadata().await;
        let p = md.channels.get("$system.events")?.first()?.clone();
        let leader = p.leader.clone()?;
        let outsider = ["n1", "n2", "n3"]
            .into_iter()
            .find(|n| !p.replicas.iter().any(|r| r == n))?
            .to_string();
        Some((leader, outsider))
    })
    .await;
    let feed = |node: &str, query: &str| {
        let url = c.node(node).url(&format!("/api/events/feed?{query}"));
        async move {
            let resp = reqwest::get(url).await.unwrap();
            assert_eq!(resp.status(), 200);
            resp.json::<serde_json::Value>().await.unwrap()
        }
    };
    // The feed has startup events (controller elected, nodes up) once the channel exists.
    wait_for("feed has items", Duration::from_secs(30), || async {
        let page = feed(&leader, "tail=5").await;
        (!page["items"].as_array()?.is_empty()).then_some(())
    })
    .await;
    // The outsider proxies once its metadata replica knows the leader; until then (startup
    // under load) it answers from its own, empty copy by design: compare eventually.
    wait_for(
        "outsider serves the leader's feed",
        Duration::from_secs(30),
        || async {
            let on_leader = feed(&leader, "tail=5").await;
            let on_outsider = feed(&outsider, "tail=5").await;
            (on_leader["next_cursor"] == on_outsider["next_cursor"]
                && on_leader["items"] == on_outsider["items"])
                .then_some(())
        },
    )
    .await;

    // Long-poll on the node without a replica; a new channel produces an event.
    let latest = feed(&outsider, "after=latest").await;
    let cursor = latest["next_cursor"].as_str().unwrap().to_string();
    let url = c
        .node(&outsider)
        .url(&format!("/api/events/feed?after={cursor}&wait_ms=20000"));
    let started = Instant::now();
    let poll = tokio::spawn(async move {
        let resp = reqwest::get(url).await.unwrap();
        assert_eq!(resp.status(), 200);
        resp.json::<serde_json::Value>().await.unwrap()
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    c.create_channel("fresh", 6).await;
    let page = poll.await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "long-poll didn't wake"
    );
    let items = page["items"].as_array().unwrap();
    assert!(!items.is_empty(), "{page}");
    let first: u64 = items[0]["cursor"].as_str().unwrap().parse().unwrap();
    assert!(first > cursor.parse::<u64>().unwrap());
    assert_eq!(page["next_cursor"], items.last().unwrap()["cursor"]);
    c.stop_all().await;
}
