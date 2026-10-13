//! v0.5 node decommission / forget and persistent notification history, end to end:
//! in-process cluster nodes on loopback (same harness shape as `rebalance.rs`).
//!
//! * decommission a node of a 4-node cluster under steady publish + consume → no acknowledged
//!   message lost, the node ends with 0 replicas, out of the Raft membership and the registry;
//!   restarting it with the same id and data dir joins as a fresh node;
//! * decommission the controller → it moves its replicas, removes itself and steps down;
//! * refusals: RF can't be held (409), Raft quorum unsafe (409), unknown node (404), single
//!   node (400), forgetting a live node (409);
//! * forget a dead node after its replicas were repaired;
//! * the notification history survives a controller failover and a full restart.

mod common;

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::RawConn;
use fluxmq_cluster::PeerSpec;
use fluxmq_proto::cluster::{
    ClusterMetadata, ClusterStatus, DecommissionState, DecommissionStatus, MoveState, NodeStatus,
    RebalanceConfig, RebalanceMode, RebalanceState, RebalanceStatus,
};
use fluxmq_proto::model::PublishMessage;
use fluxmq_proto::wire::{ClientRequest, ServerResponse};
use fluxmq_server::{ClusterSettings, Server, ServerConfig};
use serde_json::json;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const SECRET: &str = "test-cluster-secret";
const NODE_TIMEOUT: Duration = Duration::from_secs(3);
const INTERVAL: Duration = Duration::from_secs(1);

/// HTTP client whose calls fail (instead of hanging the test) after 30 s.
fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap()
}

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
    rf: u32,
    client_addr: SocketAddr,
    http_addr: SocketAddr,
    shutdown: CancellationToken,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
}

impl Node {
    async fn start(id: &str, data: PathBuf, raft_port: u16, peers: &[PeerSpec], rf: u32) -> Node {
        let mut config = ServerConfig::ephemeral(&data);
        config.ui = fluxmq_api::UiSource::Off;
        config.fsync = fluxmq_server::FsyncMode::Os;
        config.node_id = Some(id.to_string());
        config.shutdown_timeout = Duration::from_secs(5);
        config.auto_rebalance = RebalanceMode::Off;
        config.rebalance_interval = INTERVAL;
        config.cluster = Some(ClusterSettings {
            peers: peers.to_vec(),
            raft_addr: SocketAddr::from(([127, 0, 0, 1], raft_port)),
            replication_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            advertise_host: Some("127.0.0.1".into()),
            replication_factor: rf,
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
            data,
            raft_port,
            rf,
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

    async fn status(&self) -> ClusterStatus {
        get_json(&self.url("/api/cluster/status")).await
    }

    async fn rebalance(&self) -> RebalanceStatus {
        get_json(&self.url("/api/cluster/rebalance")).await
    }

    async fn put_config(&self, cfg: &RebalanceConfig) {
        let resp = http()
            .put(self.url("/api/cluster/rebalance/config"))
            .json(cfg)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    /// `(status, body)` of an HTTP call.
    async fn call(&self, method: reqwest::Method, path: &str) -> (u16, serde_json::Value) {
        let resp = http().request(method, self.url(path)).send().await.unwrap();
        let status = resp.status().as_u16();
        let body = resp.json().await.unwrap_or(serde_json::Value::Null);
        (status, body)
    }

    async fn decommission(&self, node: &str) -> (u16, serde_json::Value) {
        self.call(
            reqwest::Method::POST,
            &format!("/api/cluster/nodes/{node}/decommission"),
        )
        .await
    }

    /// The decommission record of `node` as seen by this node (`None` = 404).
    async fn decommission_status(&self, node: &str) -> Option<DecommissionStatus> {
        let (s, body) = self
            .call(
                reqwest::Method::GET,
                &format!("/api/cluster/nodes/{node}/decommission"),
            )
            .await;
        match s {
            200 => Some(serde_json::from_value(body).unwrap()),
            404 => None,
            other => panic!("GET decommission {node}: {other} {body}"),
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

async fn get_json<T: serde::de::DeserializeOwned>(url: &str) -> T {
    let resp = http().get(url).send().await.unwrap();
    assert!(resp.status().is_success(), "GET {url}: {}", resp.status());
    resp.json().await.unwrap()
}

struct TestCluster {
    _dir: tempfile::TempDir,
    peers: Vec<PeerSpec>,
    nodes: Vec<Node>,
}

impl TestCluster {
    async fn start(n: usize, rf: u32) -> TestCluster {
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
            async move { Node::start(&format!("n{}", i + 1), data, port, &peers, rf).await }
        });
        let nodes = futures::future::join_all(starts).await;
        let c = TestCluster {
            _dir: dir,
            peers,
            nodes,
        };
        c.wait_up(n).await;
        c
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

    /// Restarts a stopped node with the same id, data dir and Raft port.
    async fn restart(&mut self, id: &str) {
        let idx = self.nodes.iter().position(|n| n.id == id).unwrap();
        let (data, port, rf) = {
            let n = &self.nodes[idx];
            assert!(n.handle.is_none(), "{id} is still running");
            (n.data.clone(), n.raft_port, n.rf)
        };
        self.nodes[idx] = Node::start(id, data, port, &self.peers, rf).await;
    }

    async fn stop_all(&mut self) {
        for n in &mut self.nodes {
            n.stop().await;
        }
    }

    async fn create_channel(&self, name: &str, partitions: u32) {
        let resp = http()
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

fn test_config(mode: RebalanceMode) -> RebalanceConfig {
    RebalanceConfig {
        mode,
        max_concurrent_moves: 2,
        imbalance_threshold: 1,
        replace_after_secs: 600,
        throttle_bytes_per_sec: 0,
        move_timeout_secs: 60,
    }
}

fn replicas_on(md: &ClusterMetadata, node: &str) -> usize {
    md.channels
        .values()
        .flatten()
        .filter(|p| p.replicas.iter().any(|r| r == node))
        .count()
}

fn healthy(md: &ClusterMetadata) -> bool {
    md.channels
        .values()
        .flatten()
        .all(|p| p.leader.is_some() && p.isr.len() == p.replicas.len())
}

/// Waits until `node`'s decommission (seen through `via`) reaches `Removed` and the node is
/// out of the registry.
async fn wait_removed(via: &Node, node: &str, timeout: Duration) -> DecommissionStatus {
    wait_for(&format!("{node} removed"), timeout, || async {
        let st = via.decommission_status(node).await?;
        assert_ne!(st.state, DecommissionState::Failed, "{st:?}");
        let gone = via.metadata().await.nodes.iter().all(|n| n.id != node);
        (st.state == DecommissionState::Removed && gone).then_some(st)
    })
    .await
}

/// The node is gone from the registry, the Raft membership and every assignment.
async fn assert_gone(via: &Node, node: &str) {
    let md = via.metadata().await;
    assert!(md.nodes.iter().all(|n| n.id != node), "{:?}", md.nodes);
    assert_eq!(replicas_on(&md, node), 0);
    assert!(
        md.channels
            .values()
            .flatten()
            .all(|p| !p.isr.iter().any(|r| r == node) && p.leader.as_deref() != Some(node))
    );
    let st = via.status().await;
    assert!(
        !st.raft.voters.iter().any(|v| v == node) && !st.raft.learners.iter().any(|v| v == node),
        "{:?}",
        st.raft
    );
}

/// All stored messages of `channel` as (partition, offset) via node `n` (fan-out read).
async fn read_all(n: &Node, channel: &str, partitions: u32) -> BTreeSet<(u32, u64)> {
    let mut out = BTreeSet::new();
    for p in 0..partitions {
        let mut offset = 0u64;
        loop {
            let page: serde_json::Value = wait_for("readable", Duration::from_secs(20), || async {
                let resp = http()
                    .get(n.url(&format!(
                        "/api/channels/{channel}/messages?partition={p}&offset={offset}&limit=1000"
                    )))
                    .send()
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

/// Publishes batches through one node like an SDK: every batch is retried until acknowledged;
/// a batch never acknowledged within 30 s counts as a failure.
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

    fn acked(&self) -> usize {
        self.state.lock().unwrap().acked.len()
    }

    async fn wait_acked(&self, n: usize) {
        wait_for("publisher acks", Duration::from_secs(30), || async {
            (self.acked() >= n).then_some(())
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
        let client = Client::connect(ClientOptions::with_seeds(seeds, "decommission-test"))
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

/// Stops the publisher and consumer and checks: nothing failed, no acknowledged message lost
/// (read back via `via`), and the consumer group saw every acknowledged message.
async fn check_no_loss(publisher: Publisher, consumer: Consumer, via: &Node, partitions: u32) {
    let published = publisher.stop().await;
    eprintln!(
        "published {} messages, {} retried attempts, {} failed",
        published.acked.len(),
        published.retries,
        published.failed
    );
    assert_eq!(published.failed, 0, "publishes failed beyond retries");
    let stored = read_all(via, "orders", partitions).await;
    let lost: Vec<_> = published.acked.difference(&stored).collect();
    assert!(
        lost.is_empty(),
        "{} acked messages lost: {lost:?}",
        lost.len()
    );
    wait_for("consumer caught up", Duration::from_secs(60), || async {
        (consumer.count() >= published.payloads.len()).then_some(())
    })
    .await;
    let seen = consumer.stop().await;
    let missing: Vec<_> = published.payloads.difference(&seen).take(10).collect();
    assert!(missing.is_empty(), "never consumed: {missing:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn decommission_under_load_and_rejoin_fresh() {
    tracing_init();
    let mut c = TestCluster::start(4, 2).await;
    // Mode off: the decommission moves replicas on its own, whatever the rebalance mode.
    c.any().put_config(&test_config(RebalanceMode::Off)).await;
    c.create_channel("orders", 8).await;
    let controller = c.controller().await;
    let others: Vec<String> = ["n4", "n3", "n2", "n1"]
        .into_iter()
        .filter(|n| *n != controller)
        .map(String::from)
        .collect();
    let (victim, entry) = (others[0].clone(), others[1].clone());
    wait_for("victim holds replicas", Duration::from_secs(20), || async {
        let md = c.node(&entry).metadata().await;
        (replicas_on(&md, &victim) > 0 && healthy(&md)).then_some(())
    })
    .await;

    let publisher = Publisher::start(c.node(&entry).client_addr, "orders", 256);
    let seeds: Vec<String> = c.running().map(|n| n.client_addr.to_string()).collect();
    let consumer = Consumer::start(seeds, "orders").await;
    publisher.wait_acked(500).await;

    // Through a non-controller node (forwarded to the controller).
    let (s, body) = c.node(&entry).decommission(&victim).await;
    assert_eq!(s, 200, "{body}");
    let started: DecommissionStatus = serde_json::from_value(body).unwrap();
    assert_eq!(started.node, victim);
    assert_eq!(started.state, DecommissionState::MovingReplicas);
    assert!(started.replicas_remaining > 0, "{started:?}");
    assert_eq!(started.initial_replicas, started.replicas_remaining);
    // Idempotent while in progress; the registry shows the status; the plan is running.
    let (s, again) = c.node(&controller).decommission(&victim).await;
    assert_eq!(s, 200, "{again}");
    assert_eq!(again["state"], "moving_replicas");
    let md = c.node(&entry).metadata().await;
    let st = md.nodes.iter().find(|n| n.id == victim).map(|n| n.status);
    assert_eq!(st, Some(NodeStatus::Decommissioning));
    // Another executing rebalance request conflicts while the decommission plan runs.
    let rb = c.node(&entry).rebalance().await;
    if rb.state == RebalanceState::Running {
        let (s, body) = c
            .node(&entry)
            .call(reqwest::Method::POST, "/api/cluster/rebalance")
            .await;
        assert_eq!(s, 409, "{body}");
    }

    let done = wait_removed(c.node(&entry), &victim, Duration::from_secs(120)).await;
    assert_eq!(done.replicas_remaining, 0);
    assert_eq!(done.initial_replicas, started.initial_replicas);
    assert!(done.finished_at_ms.is_some());
    assert_gone(c.node(&entry), &victim).await;
    let list: Vec<DecommissionStatus> =
        get_json(&c.node(&entry).url("/api/cluster/decommissions")).await;
    assert!(
        list.iter()
            .any(|d| d.node == victim && d.state == DecommissionState::Removed),
        "{list:?}"
    );
    let md = c.node(&controller).metadata().await;
    assert!(healthy(&md), "{:#?}", md.channels);
    let rb = c.node(&entry).rebalance().await;
    assert_eq!(
        rb.trigger.as_deref(),
        Some(format!("decommission {victim}").as_str())
    );
    assert!(
        rb.moves.iter().all(|m| m.state == MoveState::Done),
        "{:#?}",
        rb.moves
    );
    // Every remaining node agrees (replicated record; the removed process itself keeps its
    // last local view, it no longer receives metadata).
    for n in c.running().filter(|n| n.id != victim) {
        let st = n.decommission_status(&victim).await.unwrap();
        assert_eq!(st.state, DecommissionState::Removed, "via {}", n.id);
    }

    // Keep publishing on the new placement, then check nothing acknowledged was lost.
    let before = publisher.acked();
    publisher.wait_acked(before + 200).await;
    check_no_loss(publisher, consumer, c.node(&entry), 8).await;

    // The removed process stopped its cluster roles and left the restart marker.
    wait_for("removal marker", Duration::from_secs(20), || async {
        c.node(&victim)
            .data
            .join(fluxmq_cluster::REMOVED_MARKER)
            .exists()
            .then_some(())
    })
    .await;

    // Restart with the same id and data dir: joins as a fresh, empty node.
    c.stop(&victim).await;
    c.restart(&victim).await;
    wait_for("victim rejoined fresh", Duration::from_secs(60), || async {
        let md = c.node(&entry).metadata().await;
        let up = md
            .nodes
            .iter()
            .any(|n| n.id == victim && n.status == NodeStatus::Up);
        let st = c.node(&entry).status().await;
        let member = st
            .raft
            .voters
            .iter()
            .chain(&st.raft.learners)
            .any(|v| *v == victim);
        (up && member).then_some(())
    })
    .await;
    assert_eq!(replicas_on(&c.node(&entry).metadata().await, &victim), 0);
    // The finished record stays readable (24 h) as history.
    let st = c.node(&entry).decommission_status(&victim).await.unwrap();
    assert_eq!(st.state, DecommissionState::Removed);
    assert!(
        !c.node(&victim)
            .data
            .join(fluxmq_cluster::REMOVED_MARKER)
            .exists()
    );
    // The rejoined node (fresh Raft state) catches up the metadata and serves the cluster.
    wait_for(
        "rejoined node has the metadata",
        Duration::from_secs(30),
        || async {
            let md = c.node(&victim).metadata().await;
            (md.nodes.len() == 4 && md.channels.contains_key("orders")).then_some(())
        },
    )
    .await;
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn decommission_the_controller() {
    tracing_init();
    let mut c = TestCluster::start(4, 2).await;
    c.any().put_config(&test_config(RebalanceMode::Full)).await;
    c.create_channel("orders", 6).await;
    let controller = c.controller().await;
    let entry = ["n1", "n2", "n3", "n4"]
        .into_iter()
        .find(|n| *n != controller)
        .unwrap()
        .to_string();
    wait_for("healthy", Duration::from_secs(20), || async {
        let md = c.node(&entry).metadata().await;
        (replicas_on(&md, &controller) > 0 && healthy(&md)).then_some(())
    })
    .await;
    let publisher = Publisher::start(c.node(&entry).client_addr, "orders", 256);
    let seeds: Vec<String> = c.running().map(|n| n.client_addr.to_string()).collect();
    let consumer = Consumer::start(seeds, "orders").await;
    publisher.wait_acked(300).await;

    let (s, body) = c.node(&entry).decommission(&controller).await;
    assert_eq!(s, 200, "{body}");
    assert_eq!(body["state"], "moving_replicas", "{body}");

    // The controller moves its replicas while still in charge, then removes itself; the next
    // controller finishes the removal.
    let done = wait_removed(c.node(&entry), &controller, Duration::from_secs(120)).await;
    assert_eq!(done.replicas_remaining, 0);
    let new_controller = wait_for("new controller", Duration::from_secs(30), || async {
        c.node(&entry)
            .metadata()
            .await
            .controller
            .filter(|n| *n != controller)
    })
    .await;
    eprintln!("controller {controller} decommissioned; {new_controller} took over");
    assert_gone(c.node(&entry), &controller).await;
    let md = c.node(&entry).metadata().await;
    assert_eq!(md.nodes.len(), 3);
    wait_for("healthy after", Duration::from_secs(30), || async {
        healthy(&c.node(&entry).metadata().await).then_some(())
    })
    .await;

    let before = publisher.acked();
    publisher.wait_acked(before + 200).await;
    check_no_loss(publisher, consumer, c.node(&entry), 6).await;

    // Controller commands work under the new controller (a dry run through every survivor).
    for n in c.running().filter(|n| n.id != controller) {
        let (s, body) = n.call(reqwest::Method::GET, "/api/cluster/rebalance").await;
        assert_eq!(s, 200, "{body}");
    }
    wait_for(
        "old controller retired",
        Duration::from_secs(20),
        || async {
            c.node(&controller)
                .data
                .join(fluxmq_cluster::REMOVED_MARKER)
                .exists()
                .then_some(())
        },
    )
    .await;
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn decommission_refusals() {
    tracing_init();
    // RF 3 on 3 nodes: no other node could take a replica.
    let mut c = TestCluster::start(3, 3).await;
    c.any().put_config(&test_config(RebalanceMode::Off)).await;
    c.create_channel("orders", 3).await;
    let controller = c.controller().await;
    let others: Vec<String> = ["n3", "n2", "n1"]
        .into_iter()
        .filter(|n| *n != controller)
        .map(String::from)
        .collect();
    wait_for("placed", Duration::from_secs(20), || async {
        let md = c.node(&others[1]).metadata().await;
        (replicas_on(&md, &others[0]) > 0).then_some(())
    })
    .await;
    let (s, body) = c.node(&others[1]).decommission(&others[0]).await;
    assert_eq!(s, 409, "{body}");
    assert_eq!(body["error"], "conflict");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("keeps 3 replicas"),
        "{body}"
    );
    let (s, body) = c
        .node(&others[1])
        .call(
            reqwest::Method::POST,
            &format!("/api/cluster/nodes/{}/decommission?dry_run=true", others[0]),
        )
        .await;
    assert_eq!(s, 409, "{body}");
    // Nothing changed: no record, status Up, nothing moved.
    assert!(
        c.node(&others[1])
            .decommission_status(&others[0])
            .await
            .is_none()
    );
    let md = c.node(&controller).metadata().await;
    assert!(md.nodes.iter().all(|n| n.status == NodeStatus::Up));

    // Unknown node: 404 (POST and GET).
    let (s, body) = c.node(&controller).decommission("nope").await;
    assert_eq!(s, 404, "{body}");
    // Forgetting a live node: 409; an unknown one: 404.
    let (s, body) = c
        .node(&others[1])
        .call(
            reqwest::Method::DELETE,
            &format!("/api/cluster/nodes/{}", others[0]),
        )
        .await;
    assert_eq!(s, 409, "{body}");
    let (s, _) = c
        .node(&others[1])
        .call(reqwest::Method::DELETE, "/api/cluster/nodes/nope")
        .await;
    assert_eq!(s, 404);

    // Quorum: with one of three voters down, removing another live voter would leave 1 of 2
    // remaining voters live.
    c.stop(&others[0]).await;
    wait_for("node down", Duration::from_secs(30), || async {
        let md = c.node(&controller).metadata().await;
        md.nodes
            .iter()
            .any(|n| n.id == others[0] && n.status == NodeStatus::Down)
            .then_some(())
    })
    .await;
    let (s, body) = c.node(&controller).decommission(&others[1]).await;
    assert_eq!(s, 409, "{body}");
    assert!(
        body["message"].as_str().unwrap().contains("quorum"),
        "{body}"
    );
    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn decommission_dry_run_and_cancel() {
    tracing_init();
    let mut c = TestCluster::start(3, 2).await;
    // Throttled catch-up: the decommission's moves take long enough to cancel them.
    let mut cfg = test_config(RebalanceMode::Off);
    cfg.max_concurrent_moves = 1;
    cfg.throttle_bytes_per_sec = 64 * 1024;
    c.any().put_config(&cfg).await;
    c.create_channel("orders", 6).await;
    let controller = c.controller().await;
    let others: Vec<String> = ["n3", "n2", "n1"]
        .into_iter()
        .filter(|n| *n != controller)
        .map(String::from)
        .collect();
    let (victim, entry) = (others[0].clone(), others[1].clone());
    let publisher = Publisher::start(c.node(&entry).client_addr, "orders", 1024);
    publisher.wait_acked(3000).await;
    let published = publisher.stop().await;
    assert_eq!(published.failed, 0);
    let before = c.node(&entry).metadata().await;
    let held = replicas_on(&before, &victim);
    assert!(held > 0);

    // Dry run: the plan (every replica off the victim, nothing onto it), nothing written.
    let (s, body) = c
        .node(&entry)
        .call(
            reqwest::Method::POST,
            &format!("/api/cluster/nodes/{victim}/decommission?dry_run=true"),
        )
        .await;
    assert_eq!(s, 200, "{body}");
    let plan: RebalanceStatus = serde_json::from_value(body).unwrap();
    assert_eq!(plan.state, RebalanceState::Idle);
    let off: usize = plan
        .moves
        .iter()
        .filter(|m| m.from_replicas.contains(&victim) && !m.to_replicas.contains(&victim))
        .count();
    assert_eq!(off, held, "{:#?}", plan.moves);
    assert!(
        plan.moves
            .iter()
            .all(|m| !m.to_replicas.contains(&victim) || m.from_replicas.contains(&victim))
    );
    let projected = plan
        .projected_load
        .iter()
        .find(|l| l.node == victim)
        .unwrap();
    assert_eq!(projected.replicas, 0);
    assert!(c.node(&entry).decommission_status(&victim).await.is_none());
    assert_eq!(c.node(&entry).metadata().await.channels, before.channels);

    // Start, then cancel through the rebalance API while replicas are moving.
    let (s, body) = c.node(&entry).decommission(&victim).await;
    assert_eq!(s, 200, "{body}");
    let (s, body) = c
        .node(&entry)
        .call(reqwest::Method::POST, "/api/cluster/rebalance/cancel")
        .await;
    assert_eq!(s, 200, "{body}");
    let failed = wait_for(
        "decommission cancelled",
        Duration::from_secs(90),
        || async {
            let st = c.node(&entry).decommission_status(&victim).await?;
            (st.state == DecommissionState::Failed).then_some(st)
        },
    )
    .await;
    assert_eq!(failed.error.as_deref(), Some("cancelled"), "{failed:?}");
    assert!(failed.finished_at_ms.is_some());
    // The node is Up again and still a member; nothing is half-moved.
    wait_for("victim up again", Duration::from_secs(20), || async {
        let md = c.node(&entry).metadata().await;
        md.nodes
            .iter()
            .any(|n| n.id == victim && n.status == NodeStatus::Up)
            .then_some(())
    })
    .await;
    wait_for("healthy after cancel", Duration::from_secs(60), || async {
        healthy(&c.node(&entry).metadata().await).then_some(())
    })
    .await;
    let st = c.node(&entry).status().await;
    assert!(st.raft.voters.contains(&victim), "{:?}", st.raft);
    let list: Vec<DecommissionStatus> =
        get_json(&c.node(&controller).url("/api/cluster/decommissions")).await;
    assert_eq!(list.len(), 1, "{list:?}");
    assert_eq!(list[0].state, DecommissionState::Failed);
    assert_eq!(list[0].initial_replicas as usize, held);
    // Nothing acknowledged was lost.
    let stored = read_all(c.node(&entry), "orders", 6).await;
    assert!(published.acked.is_subset(&stored));
    c.stop_all().await;
}

#[tokio::test]
async fn single_node_rejects_node_removal() {
    let srv = common::TestServer::start().await;
    for (method, path) in [
        (reqwest::Method::POST, "/api/cluster/nodes/x/decommission"),
        (reqwest::Method::GET, "/api/cluster/nodes/x/decommission"),
        (reqwest::Method::DELETE, "/api/cluster/nodes/x"),
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
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn forget_dead_node_after_repair() {
    tracing_init();
    let mut c = TestCluster::start(3, 2).await;
    // Repair mode, but the short replacement delay only once the cluster has settled: with
    // 3 s from the start, a heartbeat missed under load (parallel test binaries) already
    // started repair moves and the cluster never looked healthy in time.
    let mut cfg = test_config(RebalanceMode::Repair);
    c.any().put_config(&cfg).await;
    c.create_channel("orders", 6).await;
    let controller = c.controller().await;
    let others: Vec<String> = ["n3", "n2", "n1"]
        .into_iter()
        .filter(|n| *n != controller)
        .map(String::from)
        .collect();
    let (victim, entry) = (others[0].clone(), others[1].clone());
    wait_for("victim holds replicas", Duration::from_secs(60), || async {
        let md = c.node(&entry).metadata().await;
        (replicas_on(&md, &victim) > 0 && healthy(&md)).then_some(())
    })
    .await;
    cfg.replace_after_secs = 3;
    c.any().put_config(&cfg).await;
    c.stop(&victim).await;
    // Down but still holding replicas: refused.
    wait_for("victim down", Duration::from_secs(30), || async {
        let md = c.node(&entry).metadata().await;
        md.nodes
            .iter()
            .any(|n| n.id == victim && n.status == NodeStatus::Down)
            .then_some(())
    })
    .await;
    let path = format!("/api/cluster/nodes/{victim}");
    let md = c.node(&entry).metadata().await;
    if replicas_on(&md, &victim) > 0 {
        let (s, body) = c.node(&entry).call(reqwest::Method::DELETE, &path).await;
        assert_eq!(s, 409, "{body}");
        assert!(
            body["message"].as_str().unwrap().contains("replica"),
            "{body}"
        );
    }
    // The repair replaces its replicas (RF 2 on the two survivors).
    wait_for("repaired", Duration::from_secs(60), || async {
        let md = c.node(&entry).metadata().await;
        (replicas_on(&md, &victim) == 0 && healthy(&md)).then_some(())
    })
    .await;
    let (s, body) = c.node(&entry).call(reqwest::Method::DELETE, &path).await;
    assert_eq!(s, 200, "{body}");
    let st: DecommissionStatus = serde_json::from_value(body).unwrap();
    assert_eq!(st.state, DecommissionState::Removed, "{st:?}");
    assert_gone(c.node(&entry), &victim).await;
    assert_eq!(c.node(&controller).metadata().await.nodes.len(), 2);
    // Gone: a second forget is 404; the cluster keeps working (2 voters).
    let (s, _) = c.node(&entry).call(reqwest::Method::DELETE, &path).await;
    assert_eq!(s, 404);
    c.create_channel("after", 2).await;
    wait_for("new channel led", Duration::from_secs(20), || async {
        let md = c.node(&entry).metadata().await;
        md.channels
            .get("after")
            .is_some_and(|ps| ps.iter().all(|p| p.leader.is_some()))
            .then_some(())
    })
    .await;
    c.stop_all().await;
}

/// History records whose notifier is `nid`, via `n`.
async fn history_of(n: &Node, nid: &str) -> Vec<serde_json::Value> {
    let v: serde_json::Value = get_json(&n.url("/api/notify/history?limit=1000")).await;
    v.as_array()
        .unwrap()
        .iter()
        .filter(|r| r["notifier_id"] == nid)
        .cloned()
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn notification_history_survives_controller_failover_and_restart() {
    tracing_init();
    let mut c = TestCluster::start(3, 2).await;
    c.any().put_config(&test_config(RebalanceMode::Off)).await;
    let controller = c.controller().await;
    let other = ["n1", "n2", "n3"]
        .into_iter()
        .find(|n| *n != controller)
        .unwrap()
        .to_string();
    let http = http();
    let n: serde_json::Value = http
        .post(c.node(&other).url("/api/notify/notifiers"))
        .json(&json!({"name": "hook", "kind": {"type": "webhook", "url": "http://127.0.0.1:9/x", "secret": null, "headers": {}}}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let nid = n["id"].as_str().unwrap().to_string();
    // Test sends on the controller and on another node (forwarded to the controller).
    for via in [&controller, &other] {
        let t: serde_json::Value = http
            .post(
                c.node(via)
                    .url(&format!("/api/notify/notifiers/{nid}/test")),
            )
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(t["ok"], false, "{t}");
    }
    // Both records on every node right away (persisted before the test call returned).
    for node in c.running() {
        let h = history_of(node, &nid).await;
        assert_eq!(h.len(), 2, "via {}: {h:?}", node.id);
    }
    let ids: BTreeSet<String> = history_of(c.node(&other), &nid)
        .await
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();

    // The controller (the only dispatcher, whose in-memory ring held the records) goes away.
    c.stop(&controller).await;
    let new_controller = wait_for("new controller", Duration::from_secs(30), || async {
        c.node(&other)
            .metadata()
            .await
            .controller
            .filter(|n| *n != controller)
    })
    .await;
    eprintln!("controller {controller} stopped; {new_controller} took over");
    let survivors: Vec<String> = c.running().map(|n| n.id.clone()).collect();
    for s in &survivors {
        let got = wait_for(
            "history after failover",
            Duration::from_secs(30),
            || async {
                let h = history_of(c.node(s), &nid).await;
                let got: BTreeSet<String> = h
                    .iter()
                    .map(|r| r["id"].as_str().unwrap().to_string())
                    .collect();
                (got == ids).then_some(got)
            },
        )
        .await;
        assert_eq!(got, ids, "via {s}");
    }
    // A new record under the new controller lands in the same history.
    let t: serde_json::Value = http
        .post(
            c.node(&other)
                .url(&format!("/api/notify/notifiers/{nid}/test")),
        )
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(t["ok"], false, "{t}");
    assert_eq!(history_of(c.node(&other), &nid).await.len(), 3);

    // Full restart: every node stops, then all start again from their data dirs.
    eprintln!("phase: full stop");
    c.stop_all().await;
    eprintln!("phase: restart");
    for id in ["n1", "n2", "n3"] {
        c.restart(id).await;
        eprintln!("phase: {id} restarted");
    }
    c.wait_up(3).await;
    eprintln!("phase: up");
    let h = wait_for("history after restart", Duration::from_secs(60), || async {
        let h = history_of(c.node(&controller), &nid).await;
        (h.len() == 3).then_some(h)
    })
    .await;
    // Newest first, and the record shape is unchanged.
    let ts: Vec<u64> = h.iter().map(|r| r["ts"].as_u64().unwrap()).collect();
    assert!(ts.windows(2).all(|w| w[0] >= w[1]), "{ts:?}");
    for r in &h {
        assert_eq!(r["status"], "failed");
        assert_eq!(r["summary"], "test message");
        assert_eq!(r["error"], "connection refused");
    }
    c.stop_all().await;
}
