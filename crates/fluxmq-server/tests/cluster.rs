//! Multi-node end-to-end: three in-process cluster nodes on loopback (ephemeral client/HTTP/
//! replication ports, pre-picked Raft ports, separate temp data dirs).
//!
//! Covers metadata convergence, replicated metadata writes, channel creation with spread
//! assignments, the wire `Metadata` request, publishing via any node (server-side
//! forwarding), `NotLeader` for partition-targeted publishes, controller/leader failover
//! without losing acknowledged messages, and a node rejoining and catching up.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::RawConn;
use fluxmq_cluster::PeerSpec;
use fluxmq_proto::MessageId;
use fluxmq_proto::cluster::{ClusterMetadata, NodeStatus};
use fluxmq_proto::model::PublishMessage;
use fluxmq_proto::wire::{ClientRequest, ErrorCode, ServerResponse, WireError};
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
    client_addr: SocketAddr,
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
            min_isr: 1,
            cluster_secret: Some(SECRET.into()),
            node_timeout: NODE_TIMEOUT,
        });
        let server = Server::bind(config).await.expect("node binds");
        let (client_addr, http_addr) = (server.client_addr(), server.http_addr());
        let cluster = server.cluster().clone();
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(server.run(shutdown.clone().cancelled_owned()));
        Node {
            id: id.to_string(),
            data,
            raft_port,
            client_addr,
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

    async fn metadata(&self) -> ClusterMetadata {
        reqwest::get(self.url("/api/cluster/metadata"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
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
        TestCluster {
            _dir: dir,
            peers,
            nodes,
        }
    }

    fn node(&self, id: &str) -> &Node {
        self.nodes.iter().find(|n| n.id == id).unwrap()
    }

    fn running(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter().filter(|n| n.handle.is_some())
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

fn msg(key: &str, body: String) -> PublishMessage {
    PublishMessage {
        key: Some(key.to_string()),
        headers: Default::default(),
        payload: Bytes::from(body),
    }
}

/// Publishes through `addr`, retrying transient cluster errors (NotLeader / unavailable)
/// like an SDK would, until `timeout`.
async fn publish_retry(
    addr: SocketAddr,
    channel: &str,
    messages: Vec<PublishMessage>,
    timeout: Duration,
) -> Vec<MessageId> {
    let deadline = Instant::now() + timeout;
    let mut conn = RawConn::connect_hello(addr, None).await;
    loop {
        let r = conn
            .request(ClientRequest::Publish {
                channel: channel.into(),
                partition: None,
                messages: messages.clone(),
            })
            .await;
        match r {
            Ok(ServerResponse::PublishOk { ids }) => return ids,
            Err(e) if Instant::now() < deadline => {
                tracing::debug!(error = %e, "publish retry");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            other => panic!("publish failed: {other:?}"),
        }
    }
}

/// All stored messages of `channel` as (partition, offset) via node `n` (fan-out read).
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

fn leaders(md: &ClusterMetadata, channel: &str) -> BTreeMap<u32, Option<String>> {
    md.channels
        .get(channel)
        .map(|ps| ps.iter().map(|p| (p.partition, p.leader.clone())).collect())
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_node_cluster_end_to_end() {
    let _ = tracing_subscriber_init();
    let mut c = TestCluster::start(3).await;

    // 1. Metadata converges: every node sees 3 Up nodes and the same controller.
    wait_for("metadata convergence", Duration::from_secs(30), || async {
        let mut controllers = BTreeSet::new();
        for n in c.running() {
            let md = n.metadata().await;
            let up = md
                .nodes
                .iter()
                .filter(|x| x.status == NodeStatus::Up)
                .count();
            if up != 3 || md.controller.is_none() {
                return None;
            }
            controllers.insert(md.controller.clone());
        }
        (controllers.len() == 1).then_some(())
    })
    .await;

    // 2. A metadata write on one node is visible on all others.
    let rev = c
        .node("n3")
        .cluster
        .meta()
        .put("test/key", json!({"v": 42}), None)
        .await
        .unwrap();
    for n in c.running() {
        n.cluster.meta().wait_for(rev).await.unwrap();
        assert_eq!(
            n.cluster.meta().get("test/key").unwrap().value,
            json!({"v": 42})
        );
    }

    // 3. Create a channel through a non-controller node: assignments spread over all nodes.
    let resp = reqwest::Client::new()
        .post(c.node("n2").url("/api/channels"))
        .json(&json!({"name": "orders", "mode": "stream", "partitions": 6}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "{}",
        resp.text().await.unwrap_or_default()
    );
    let md = c.node("n1").metadata().await;
    let parts = md.channels.get("orders").expect("assignment written");
    assert_eq!(parts.len(), 6);
    for p in parts {
        assert_eq!(p.replicas.len(), 3, "RF = min(3, live nodes)");
        assert_eq!(p.leader.as_ref(), p.replicas.first());
    }
    // Leaderships are balanced cluster-wide (the `$system.events` feed channel, created by
    // the controller on start, counts too).
    let mut load: BTreeMap<String, usize> = BTreeMap::new();
    for p in md.channels.values().flatten() {
        *load.entry(p.leader.clone().unwrap()).or_default() += 1;
    }
    let (min, max) = (
        load.values().min().copied().unwrap_or(0),
        load.values().max().copied().unwrap_or(0),
    );
    assert_eq!(load.len(), 3, "{load:?}");
    assert!(max - min <= 1, "{load:?}");
    for n in c.running() {
        wait_for("channel on every node", Duration::from_secs(20), || async {
            reqwest::get(n.url("/api/channels/orders"))
                .await
                .ok()
                .filter(|r| r.status().is_success())
                .map(drop)
        })
        .await;
    }
    // Duplicate create is a conflict cluster-wide.
    let dup = reqwest::Client::new()
        .post(c.node("n3").url("/api/channels"))
        .json(&json!({"name": "orders", "mode": "stream"}))
        .send()
        .await
        .unwrap();
    assert_eq!(dup.status(), 409);

    // 4. Wire `Metadata` request.
    let mut conn = RawConn::connect_hello(c.node("n3").client_addr, None).await;
    match conn
        .request(ClientRequest::Metadata {
            channels: vec!["orders".into()],
        })
        .await
    {
        Ok(ServerResponse::Metadata { json }) => {
            let wire: ClusterMetadata = serde_json::from_str(&json).unwrap();
            assert_eq!(wire.nodes.len(), 3);
            assert_eq!(wire.channels["orders"].len(), 6);
        }
        other => panic!("metadata: {other:?}"),
    }

    // 5. Publish through every node (server-side forwarding to partition leaders).
    let mut acked: BTreeSet<(u32, u64)> = BTreeSet::new();
    for (i, n) in c.nodes.iter().enumerate() {
        let batch: Vec<PublishMessage> = (0..100)
            .map(|k| msg(&format!("k{i}-{k}"), format!("{{\"n\":{i},\"k\":{k}}}")))
            .collect();
        let ids = publish_retry(n.client_addr, "orders", batch, Duration::from_secs(15)).await;
        assert_eq!(ids.len(), 100);
        acked.extend(ids.iter().map(|id| (id.partition, id.offset)));
    }
    assert_eq!(acked.len(), 300);

    // 6. Partition-targeted publish to a non-leader: NotLeader naming the leader.
    let md = c.node("n1").metadata().await;
    let (p, leader) = leaders(&md, "orders")
        .into_iter()
        .find(|(_, l)| l.as_deref() != Some("n1"))
        .unwrap();
    let mut conn = RawConn::connect_hello(c.node("n1").client_addr, None).await;
    let r = conn
        .request(ClientRequest::Publish {
            channel: "orders".into(),
            partition: Some(p),
            messages: vec![msg("x", "x".into())],
        })
        .await;
    match r {
        Err(WireError {
            code: ErrorCode::NotLeader,
            message,
        }) => assert_eq!(Some(message), leader),
        other => panic!("expected NotLeader, got {other:?}"),
    }

    // 7. Fan-out read from any node sees every acknowledged message.
    let seen = read_all(c.node("n2"), "orders", 6).await;
    assert!(
        acked.is_subset(&seen),
        "missing {:?}",
        acked.difference(&seen).count()
    );

    // Telemetry aggregation sees all nodes.
    let metrics = wait_for("cluster metrics", Duration::from_secs(20), || async {
        let body = reqwest::get(c.node("n1").url("/metrics"))
            .await
            .ok()?
            .text()
            .await
            .ok()?;
        body.lines()
            .any(|l| l.starts_with("fluxmq_cluster_nodes") && l.trim_end().ends_with(" 3"))
            .then_some(body)
    })
    .await;
    assert!(metrics.contains("fluxmq_cluster_nodes"));

    // 8. Kill the controller: partitions it led fail over within the deadline.
    let controller = c.node("n1").metadata().await.controller.unwrap();
    let survivor = c
        .nodes
        .iter()
        .find(|n| n.id != controller)
        .unwrap()
        .id
        .clone();
    let publisher = BackgroundPublisher::start(c.node(&survivor).client_addr, "orders");
    publisher.wait_acks(20).await;
    let killed_at = Instant::now();
    c.nodes
        .iter_mut()
        .find(|n| n.id == controller)
        .unwrap()
        .stop()
        .await;
    let md = wait_for("failover", Duration::from_secs(20), || async {
        let md = c.node(&survivor).metadata().await;
        let ls = leaders(&md, "orders");
        (ls.values()
            .all(|l| l.is_some() && l.as_deref() != Some(controller.as_str()))
            && md.controller.as_deref().is_some_and(|x| x != controller))
        .then_some(md)
    })
    .await;
    let failover = killed_at.elapsed();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let (gap, acks) = publisher.stop().await;
    eprintln!("controller {controller} killed: max ack gap {gap:?} ({acks} acks)");
    // Down detection after a new election (+ grace) — well under the 10 s replica lag.
    assert!(gap < Duration::from_secs(10), "publishes stalled {gap:?}");
    eprintln!("failover after killing controller {controller}: {failover:?}");
    assert!(
        failover < Duration::from_secs(12),
        "failover took {failover:?}"
    );
    assert!(
        md.nodes
            .iter()
            .any(|n| n.id == controller && n.status == NodeStatus::Down)
    );

    // 9. Publishing keeps working through a survivor.
    let batch: Vec<PublishMessage> = (0..50)
        .map(|k| msg(&format!("after-{k}"), "a".into()))
        .collect();
    let ids = publish_retry(
        c.node(&survivor).client_addr,
        "orders",
        batch,
        Duration::from_secs(15),
    )
    .await;
    acked.extend(ids.iter().map(|id| (id.partition, id.offset)));

    // 10. No acknowledged message lost.
    let seen = read_all(c.node(&survivor), "orders", 6).await;
    let lost: Vec<_> = acked.difference(&seen).collect();
    assert!(
        lost.is_empty(),
        "lost {} acked messages: {:?}",
        lost.len(),
        &lost[..lost.len().min(10)]
    );

    // 11. The killed node rejoins, catches up (back in every ISR) and regains its preferred
    // leaderships.
    let (data, port) = {
        let n = c.nodes.iter().find(|n| n.id == controller).unwrap();
        (n.data.clone(), n.raft_port)
    };
    let restarted = Node::start(&controller, data, port, &c.peers).await;
    let idx = c.nodes.iter().position(|n| n.id == controller).unwrap();
    c.nodes[idx] = restarted;
    wait_for("rejoin + ISR catch-up", Duration::from_secs(40), || async {
        let md = c.node(&survivor).metadata().await;
        let up = md
            .nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Up)
            .count()
            == 3;
        let parts = md.channels.get("orders")?;
        let full_isr = parts.iter().all(|p| p.isr.len() == 3);
        let preferred = parts
            .iter()
            .all(|p| p.leader.as_ref() == p.replicas.first());
        (up && full_isr && preferred).then_some(())
    })
    .await;
    let seen = read_all(c.node(&controller), "orders", 6).await;
    assert!(acked.is_subset(&seen));

    for n in &mut c.nodes {
        n.stop().await;
    }
}

#[tokio::test]
async fn cluster_mode_requires_secret_and_node_id() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = ServerConfig::ephemeral(dir.path());
    config.node_id = Some("n1".into());
    config.cluster = Some(ClusterSettings {
        peers: vec![PeerSpec {
            id: "n1".into(),
            raft_addr: "127.0.0.1:1".into(),
        }],
        raft_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        replication_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        advertise_host: None,
        replication_factor: 3,
        min_isr: 1,
        cluster_secret: None,
        node_timeout: NODE_TIMEOUT,
    });
    let err = Server::bind(config.clone()).await.err().expect("refused");
    assert!(err.to_string().contains("cluster-secret"), "{err}");
    config.node_id = None;
    if let Some(c) = config.cluster.as_mut() {
        c.cluster_secret = Some("s".into());
    }
    let err = Server::bind(config).await.err().expect("refused");
    assert!(err.to_string().contains("node-id"), "{err}");
}

fn tracing_subscriber_init() -> Option<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init()
        .ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn admin_operations_push_and_internal_auth() {
    let _ = tracing_subscriber_init();
    let mut c = TestCluster::start(3).await;
    wait_for("3 nodes up", Duration::from_secs(30), || async {
        let md = c.node("n2").metadata().await;
        (md.nodes
            .iter()
            .filter(|x| x.status == NodeStatus::Up)
            .count()
            == 3
            && md.controller.is_some())
        .then_some(())
    })
    .await;

    // A client connected to n1 is told about topology changes.
    let mut watcher = RawConn::connect_hello(c.node("n1").client_addr, None).await;

    // Channel creation over the wire protocol (through the controller).
    let mut conn = RawConn::connect_hello(c.node("n3").client_addr, None).await;
    let mut cfg = fluxmq_proto::ChannelConfig::new("events", fluxmq_proto::ChannelMode::Stream);
    cfg.partitions = 3;
    let r = conn
        .request(ClientRequest::CreateChannel(cfg.clone()))
        .await;
    assert!(matches!(r, Ok(ServerResponse::ChannelCreated)), "{r:?}");
    let r = conn.request(ClientRequest::CreateChannel(cfg)).await;
    assert!(
        matches!(&r, Err(e) if e.code == ErrorCode::AlreadyExists),
        "{r:?}"
    );
    loop {
        match watcher.recv().await {
            Some(fluxmq_proto::wire::ServerFrame::MetadataChanged { epoch }) => {
                assert!(epoch > 0);
                break;
            }
            Some(_) => continue,
            None => panic!("watcher closed"),
        }
    }

    // Internal routes require the cluster secret.
    let http = reqwest::Client::new();
    let url = c.node("n2").url("/internal/v1/replication-status");
    assert_eq!(http.get(&url).send().await.unwrap().status(), 401);
    let bad = http
        .get(&url)
        .header(fluxmq_cluster::CLUSTER_SECRET_HEADER, "nope")
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 401);
    let ok = http
        .get(&url)
        .header(fluxmq_cluster::CLUSTER_SECRET_HEADER, SECRET)
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);

    // Publish so every partition has data, then check status / DLQ fan-out answer.
    let batch: Vec<PublishMessage> = (0..30).map(|k| msg(&format!("e{k}"), "e".into())).collect();
    publish_retry(
        c.node("n2").client_addr,
        "events",
        batch,
        Duration::from_secs(15),
    )
    .await;
    let status: serde_json::Value = reqwest::get(c.node("n2").url("/api/cluster/status"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        status["metadata"]["channels"]["events"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(status["offline_partitions"], 0);
    let parts = status["partitions"].as_array().unwrap();
    assert_eq!(
        parts.iter().filter(|p| p["channel"] == "events").count(),
        3,
        "replication status gathered from all leaders: {parts:?}"
    );
    let dlq = reqwest::get(c.node("n3").url("/api/dlq/summary"))
        .await
        .unwrap();
    assert_eq!(dlq.status(), 200);
    let page: serde_json::Value =
        reqwest::get(c.node("n3").url("/api/channels/events/messages?limit=1000"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(page["messages"].as_array().unwrap().len(), 30);

    // Drain n1: it leads nothing afterwards and is marked Leaving.
    // Every partition n1 leads moves (incl. the `$system.events` feed channel).
    let md = c.node("n2").metadata().await;
    let led_by_n1 = md
        .channels
        .keys()
        .flat_map(|ch| leaders(&md, ch).into_values())
        .filter(|l| l.as_deref() == Some("n1"))
        .count();
    let resp = http
        .post(c.node("n2").url("/api/cluster/nodes/n1/drain"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let drained: serde_json::Value = resp.json().await.unwrap();
    // The snapshot above can race a preferred-leader election (nodes just became stable), so
    // check the count loosely and the end state strictly: n1 leads nothing in any channel.
    let moved = drained["moved_leaderships"].as_u64().unwrap() as usize;
    assert!(
        moved >= 1 && led_by_n1 >= 1,
        "moved {moved}, n1 led {led_by_n1}"
    );
    let md = c.node("n2").metadata().await;
    for ch in md.channels.keys() {
        assert!(
            leaders(&md, ch)
                .values()
                .all(|l| l.as_deref() != Some("n1")),
            "{ch} still led by n1"
        );
    }
    assert!(
        md.nodes
            .iter()
            .any(|n| n.id == "n1" && n.status == NodeStatus::Leaving)
    );
    // Stays drained: no preferred-leader move back while n1 is Leaving.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let md = c.node("n3").metadata().await;
    assert!(
        leaders(&md, "events")
            .values()
            .all(|l| l.as_deref() != Some("n1"))
    );

    // Reassign partition 0 to {n2, n3}.
    let resp = http
        .post(c.node("n1").url("/api/cluster/partitions/reassign"))
        .json(&json!({"channel": "events", "partition": 0, "replicas": ["n3", "n2"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "{}",
        resp.text().await.unwrap_or_default()
    );
    // Read-your-writes on the node that served the request.
    let md = c.node("n1").metadata().await;
    let p0 = &md.channels["events"][0];
    assert_eq!(p0.replicas, vec!["n3".to_string(), "n2".to_string()]);
    assert!(p0.leader.as_deref().is_some_and(|l| l != "n1"));
    let bad = http
        .post(c.node("n1").url("/api/cluster/partitions/reassign"))
        .json(&json!({"channel": "events", "partition": 0, "replicas": ["ghost"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);

    // Kill a follower-only node of a single-partition channel: acks resume once the
    // controller marks it Down and shrinks it out of the ISR (not after the leader's 10 s
    // lag eviction).
    let resp = http
        .post(c.node("n2").url("/api/channels"))
        .json(&json!({"name": "single", "mode": "stream", "partitions": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let md = c.node("n2").metadata().await;
    let single_leader = md.channels["single"][0].leader.clone().unwrap();
    let controller = md.controller.clone().unwrap();
    let victim = ["n1", "n2", "n3"]
        .into_iter()
        .find(|n| *n != single_leader && *n != controller)
        .unwrap()
        .to_string();
    let publisher = BackgroundPublisher::start(c.node(&single_leader).client_addr, "single");
    publisher.wait_acks(20).await;
    let killed = Instant::now();
    c.nodes
        .iter_mut()
        .find(|n| n.id == victim)
        .unwrap()
        .stop()
        .await;
    tokio::time::sleep(NODE_TIMEOUT * 3).await;
    let (gap, acks) = publisher.stop().await;
    eprintln!("follower {victim} killed: max ack gap {gap:?} ({acks} acks)");
    assert!(acks > 20);
    assert!(
        gap < NODE_TIMEOUT + Duration::from_millis(2500),
        "publishes stalled {gap:?} after killing follower {victim} (killed {:?} ago)",
        killed.elapsed()
    );

    // The node comes back (Down → Up), then restarts again quickly (before being declared
    // Down): metadata must never name it leader of a partition it can't serve — a
    // partition-targeted publish to it gets NotLeader, never NotFound.
    let peers = c.peers.clone();
    let (data, port) = {
        let n = c.nodes.iter().find(|n| n.id == victim).unwrap();
        (n.data.clone(), n.raft_port)
    };
    let i = c.nodes.iter().position(|n| n.id == victim).unwrap();
    c.nodes[i] = Node::start(&victim, data.clone(), port, &peers).await;
    wait_until_rebalanced(&c, &controller, &["events", "single"]).await;
    let epoch_before = c.node(&controller).metadata().await.epoch;
    c.nodes[i].stop().await;
    c.nodes[i] = Node::start(&victim, data, port, &peers).await;
    let probe = NotFoundProbe::start(c.nodes[i].client_addr, vec![("events", 3), ("single", 1)]);
    // The controller notices the new incarnation and demotes the node (ISR shrink /
    // leadership move), then it catches up and gets its preferred leaderships back.
    wait_for("restart processed", Duration::from_secs(15), || async {
        let md = c.node(&controller).metadata().await;
        let demoted = md
            .channels
            .values()
            .flatten()
            .any(|p| !p.isr.contains(&victim));
        (md.epoch > epoch_before && demoted).then_some(())
    })
    .await;
    wait_until_rebalanced(&c, &controller, &["events", "single"]).await;
    let (not_found, probes) = probe.stop().await;
    eprintln!("rejoin probe: {probes} partition-targeted publishes, {not_found} NotFound");
    assert!(probes > 0);
    assert_eq!(not_found, 0, "rejoining node answered NotFound");
    let md = c.node(&controller).metadata().await;
    assert!(
        md.channels["events"]
            .iter()
            .any(|p| p.leader.as_deref() == Some(victim.as_str()))
            || md.channels["events"]
                .iter()
                .all(|p| p.replicas.first().map(String::as_str) != Some(victim.as_str()))
    );

    for n in &mut c.nodes {
        n.stop().await;
    }
}

/// Waits until every node is Up, every partition of `channels` has a full ISR and its
/// preferred leader.
async fn wait_until_rebalanced(c: &TestCluster, via: &str, channels: &[&str]) {
    wait_for("rebalanced", Duration::from_secs(40), || async {
        let md = c.node(via).metadata().await;
        let leaving: BTreeSet<&str> = md
            .nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Leaving)
            .map(|n| n.id.as_str())
            .collect();
        let up = md.nodes.iter().all(|n| n.status != NodeStatus::Down);
        let ok = channels.iter().all(|ch| {
            md.channels.get(*ch).is_some_and(|ps| {
                ps.iter().all(|p| {
                    let preferred = p.replicas.first().map(String::as_str);
                    p.isr.len() == p.replicas.len()
                        && p.leader.is_some()
                        && (p.leader.as_deref() == preferred
                            || preferred.is_some_and(|x| leaving.contains(x)))
                })
            })
        });
        (up && ok).then_some(())
    })
    .await;
}

/// Publishes one message at a time (retrying like an SDK) and records ack times.
struct BackgroundPublisher {
    stop: CancellationToken,
    acks: std::sync::Arc<std::sync::Mutex<Vec<Instant>>>,
    task: JoinHandle<()>,
}

impl BackgroundPublisher {
    fn start(addr: SocketAddr, channel: &'static str) -> Self {
        let stop = CancellationToken::new();
        let acks = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let (s, a) = (stop.clone(), acks.clone());
        let task = tokio::spawn(async move {
            let mut conn = RawConn::connect_hello(addr, None).await;
            let mut k = 0u64;
            while !s.is_cancelled() {
                k += 1;
                let r = conn
                    .request(ClientRequest::Publish {
                        channel: channel.into(),
                        partition: None,
                        messages: vec![msg(&format!("bg-{k}"), "b".into())],
                    })
                    .await;
                match r {
                    Ok(ServerResponse::PublishOk { .. }) => a.lock().unwrap().push(Instant::now()),
                    _ => tokio::time::sleep(Duration::from_millis(50)).await,
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        Self { stop, acks, task }
    }

    async fn wait_acks(&self, n: usize) {
        wait_for("publisher acks", Duration::from_secs(15), || async {
            (self.acks.lock().unwrap().len() >= n).then_some(())
        })
        .await;
    }

    /// Stops; returns the longest gap between consecutive acks and the ack count.
    async fn stop(self) -> (Duration, usize) {
        self.stop.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(20), self.task).await;
        let acks = self.acks.lock().unwrap();
        let gap = acks
            .windows(2)
            .map(|w| w[1] - w[0])
            .max()
            .unwrap_or_default();
        (gap, acks.len())
    }
}

/// Repeatedly sends partition-targeted publishes to one node and counts `NotFound` answers.
struct NotFoundProbe {
    stop: CancellationToken,
    task: JoinHandle<(usize, usize)>,
}

impl NotFoundProbe {
    fn start(addr: SocketAddr, targets: Vec<(&'static str, u32)>) -> Self {
        let stop = CancellationToken::new();
        let s = stop.clone();
        let task = tokio::spawn(async move {
            let (mut not_found, mut probes) = (0, 0);
            let mut conn = RawConn::connect_hello(addr, None).await;
            while !s.is_cancelled() {
                for (ch, parts) in &targets {
                    for p in 0..*parts {
                        probes += 1;
                        let r = conn
                            .request(ClientRequest::Publish {
                                channel: (*ch).into(),
                                partition: Some(p),
                                messages: vec![msg("probe", "p".into())],
                            })
                            .await;
                        if let Err(e) = &r
                            && e.code == ErrorCode::NotFound
                        {
                            eprintln!("probe {ch}/{p}: {e}");
                            not_found += 1;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
            (not_found, probes)
        });
        Self { stop, task }
    }

    async fn stop(self) -> (usize, usize) {
        self.stop.cancel();
        self.task.await.unwrap()
    }
}

/// Calls the internal group check on every node: true when no node has `group` on `channel`.
async fn group_gone(c: &TestCluster, channel: &str, group: &str) -> bool {
    let http = reqwest::Client::new();
    for n in c.running() {
        let r = http
            .delete(n.url(&format!(
                "/internal/v1/channels/{channel}/groups/{}?check=true",
                group.replace('$', "%24")
            )))
            .header(fluxmq_cluster::CLUSTER_SECRET_HEADER, SECRET)
            .send()
            .await
            .unwrap();
        if r.status() != 404 {
            return false;
        }
    }
    true
}

/// Cluster replay / DLQ fan-out / group deletion through partition leaders, then a graceful
/// stop of a partition leader: leadership is handed over and the node leaves every ISR at
/// once, so publishes barely pause (no `node_timeout` wait).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cluster_group_ops_replay_and_graceful_leave() {
    let _ = tracing_subscriber_init();
    let mut c = TestCluster::start(3).await;
    wait_for("3 nodes up", Duration::from_secs(30), || async {
        let md = c.node("n2").metadata().await;
        (md.nodes
            .iter()
            .filter(|x| x.status == NodeStatus::Up)
            .count()
            == 3
            && md.controller.is_some())
        .then_some(())
    })
    .await;
    let http = reqwest::Client::new();
    for (name, parts) in [("orders", 6), ("orders.copy", 1)] {
        let r = http
            .post(c.node("n2").url("/api/channels"))
            .json(&json!({"name": name, "mode": "stream", "partitions": parts}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 201);
    }
    wait_for("full ISRs", Duration::from_secs(20), || async {
        let md = c.node("n1").metadata().await;
        md.channels
            .values()
            .flatten()
            .all(|p| p.isr.len() == 3 && p.leader.is_some())
            .then_some(())
    })
    .await;

    // Replay through a node that leads only some partitions: leaders read, copies are
    // published via the cluster path.
    let ids = publish_retry(
        c.node("n1").client_addr,
        "orders",
        (0..30)
            .map(|k| msg(&format!("k{k}"), format!("v{k}")))
            .collect(),
        Duration::from_secs(15),
    )
    .await;
    // Metadata names leaders before their roles are active (lease, reconciler) and n3 learns
    // about `orders.copy` asynchronously: wait until every node serves both channels and
    // fan-out reads cover every partition, so the replay below isn't refused as transient.
    wait_for(
        "leaders active everywhere",
        Duration::from_secs(20),
        || async {
            for n in c.running() {
                for path in [
                    "/api/channels/orders.copy",
                    "/api/channels/orders",
                    "/api/dlq/summary",
                ] {
                    if reqwest::get(n.url(path)).await.ok()?.status() != 200 {
                        return None;
                    }
                }
            }
            Some(())
        },
    )
    .await;
    let replay: Vec<_> = ids.iter().take(6).collect();
    let r = http
        .post(c.node("n3").url("/api/channels/orders/replay"))
        .json(&json!({"ids": replay, "target_channel": "orders.copy"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "{}", r.text().await.unwrap_or_default());
    let copied = wait_for("replayed copies", Duration::from_secs(10), || async {
        let page: serde_json::Value =
            reqwest::get(c.node("n2").url("/api/channels/orders.copy/messages"))
                .await
                .ok()?
                .json()
                .await
                .ok()?;
        let msgs = page["messages"].as_array()?.clone();
        (msgs.len() == 6).then_some(msgs)
    })
    .await;
    assert!(copied.iter().all(|m| {
        m["headers"]["flux-replayed-from"]
            .as_str()
            .unwrap()
            .starts_with("orders/")
    }));

    // DLQ reads fan out with the coverage check (all partitions led, so 200). A preferred
    // leader election may move a leadership right now (nodes just became stable): 503 is the
    // documented transient answer while a role settles, so poll.
    for path in ["/api/dlq", "/api/dlq/summary", "/api/dlq/export"] {
        wait_for(path, Duration::from_secs(10), || async {
            let status = reqwest::get(c.node("n1").url(path)).await.ok()?.status();
            assert!(status == 200 || status == 503, "{path}: {status}");
            (status == 200).then_some(())
        })
        .await;
    }

    // A route creates `$router` on the partition leaders; it is reserved while the route
    // exists and released (on every leader) when the last route is deleted.
    let r = http
        .post(c.node("n1").url("/api/routes"))
        .json(&json!({"name": "all", "source": "orders", "targets": ["orders.copy"], "conditions": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let route: serde_json::Value = r.json().await.unwrap();
    wait_for(
        "$router group on leaders",
        Duration::from_secs(10),
        || async { (!group_gone(&c, "orders", "$router").await).then_some(()) },
    )
    .await;
    let r = http
        .delete(c.node("n2").url("/api/channels/orders/groups/$router"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 409);
    // Metadata is replicated asynchronously: n2 learns about the route via Raft.
    let route_url = c
        .node("n2")
        .url(&format!("/api/routes/{}", route["id"].as_str().unwrap()));
    wait_for("route visible on n2", Duration::from_secs(10), || async {
        (reqwest::get(&route_url).await.ok()?.status() == 200).then_some(())
    })
    .await;
    let r = http.delete(&route_url).send().await.unwrap();
    assert_eq!(r.status(), 204);
    wait_for("$router released", Duration::from_secs(15), || async {
        group_gone(&c, "orders", "$router").await.then_some(())
    })
    .await;
    let r = http
        .delete(c.node("n3").url("/api/channels/orders/groups/nope"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);

    // Graceful stop of a non-controller partition leader while publishing via a survivor.
    let md = c.node("n1").metadata().await;
    let controller = md.controller.clone().unwrap();
    let leaving = c
        .nodes
        .iter()
        .map(|n| n.id.clone())
        .find(|id| {
            *id != controller
                && leaders(&md, "orders")
                    .values()
                    .any(|l| l.as_deref() == Some(id.as_str()))
        })
        .expect("a non-controller leader");
    let survivor = controller.clone();
    let publisher = BackgroundPublisher::start(c.node(&survivor).client_addr, "orders");
    publisher.wait_acks(20).await;
    let t = Instant::now();
    c.nodes
        .iter_mut()
        .find(|n| n.id == leaving)
        .unwrap()
        .stop()
        .await;
    let stop_took = t.elapsed();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let (gap, acks) = publisher.stop().await;
    eprintln!(
        "graceful stop of leader {leaving}: stop took {stop_took:?}, max ack gap {gap:?} ({acks} acks)"
    );
    assert!(
        gap < NODE_TIMEOUT / 2,
        "publishes stalled {gap:?} (node_timeout {NODE_TIMEOUT:?})"
    );
    let md = c.node(&survivor).metadata().await;
    assert!(
        md.nodes
            .iter()
            .any(|n| n.id == leaving && n.status == NodeStatus::Down)
    );
    for p in md.channels.values().flatten() {
        assert_ne!(p.leader.as_deref(), Some(leaving.as_str()));
        assert!(!p.isr.contains(&leaving), "{p:?}");
    }
    // Fan-out reads still cover every partition.
    let r = reqwest::get(c.node(&survivor).url("/api/dlq"))
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    for n in &mut c.nodes {
        n.stop().await;
    }
}

/// v0.3 cluster backups: started via a non-controller node (proxied to the controller), every
/// leader uploads its partitions, the manifest covers every partition; restore into a fresh
/// single node holds every message. Also: notification history on any node is the
/// controller's, and `$system.events` exists exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cluster_backup_restore_and_controller_history() {
    let _ = tracing_subscriber_init();
    let mut c = TestCluster::start(3).await;
    let md = wait_for("3 nodes up", Duration::from_secs(30), || async {
        let md = c.node("n1").metadata().await;
        (md.nodes
            .iter()
            .filter(|x| x.status == NodeStatus::Up)
            .count()
            == 3
            && md.controller.is_some())
        .then_some(md)
    })
    .await;
    let controller = md.controller.clone().unwrap();
    let other = c
        .nodes
        .iter()
        .find(|n| n.id != controller)
        .unwrap()
        .id
        .clone();
    let http = reqwest::Client::new();

    // A channel spread over all three nodes, with data.
    let r = http
        .post(c.node(&other).url("/api/channels"))
        .json(&json!({"name": "bk", "mode": "stream", "partitions": 6}))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());
    let all_led = wait_for("bk partitions led", Duration::from_secs(30), || async {
        let md = c.node("n1").metadata().await;
        let l = leaders(&md, "bk");
        (l.len() == 6 && l.values().all(Option::is_some)).then_some(l)
    })
    .await;
    let leader_nodes: BTreeSet<String> = all_led.values().flatten().cloned().collect();
    let msgs: Vec<PublishMessage> = (0..60)
        .map(|i| msg(&format!("k{i}"), format!("m{i}")))
        .collect();
    publish_retry(
        c.node("n1").client_addr,
        "bk",
        msgs,
        Duration::from_secs(20),
    )
    .await;
    // A retried publish may have stored a group twice (at-least-once): count what is stored.
    let stored = read_all(c.node("n1"), "bk", 6).await.len() as u64;
    assert!(stored >= 60, "{stored}");

    // `$system.events` exists once (created by the controller), 1 partition.
    wait_for("system events channel", Duration::from_secs(30), || async {
        let chans: serde_json::Value = http
            .get(c.node(&other).url("/api/channels"))
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        let sys: Vec<&serde_json::Value> = chans
            .as_array()?
            .iter()
            .filter(|ch| ch["name"] == "$system.events")
            .collect();
        (sys.len() == 1 && sys[0]["partitions"] == 1).then_some(())
    })
    .await;
    // Both system channels led before the backup: the controller creates
    // `$system.notifications` on its own 2 s tick after the election, and a backup that runs
    // while a partition has no leader fails by design ("retry").
    wait_for("system channels led", Duration::from_secs(30), || async {
        let md = c.node(&controller).metadata().await;
        ["$system.events", "$system.notifications"]
            .iter()
            .all(|ch| {
                let l = leaders(&md, ch);
                !l.is_empty() && l.values().all(Option::is_some)
            })
            .then_some(())
    })
    .await;

    // Backup config (replicated meta), run via a non-controller.
    let target = c._dir.path().join("backup-target");
    let r = http
        .put(c.node(&other).url("/api/backups/config"))
        .json(&json!({
            "enabled": false,
            "target": {"type": "filesystem", "path": target.display().to_string()},
            "interval_secs": 3600, "keep_last": 5, "keep_days": 0,
            "include_messages": true, "encryption_key": null
        }))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());
    // Let the config replicate everywhere before the run (each node checks its own copy):
    // wait until every node serves it instead of a fixed sleep (too short under load).
    for n in c.running() {
        wait_for(
            "backup config replicated",
            Duration::from_secs(20),
            || async {
                let cfg: serde_json::Value = http
                    .get(n.url("/api/backups/config"))
                    .send()
                    .await
                    .ok()?
                    .json()
                    .await
                    .ok()?;
                (!cfg.is_null()).then_some(())
            },
        )
        .await;
    }
    let r = http
        .post(c.node(&other).url("/api/backups/run"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202, "{}", r.text().await.unwrap());
    let started: serde_json::Value = r.json().await.unwrap();
    let id = started["id"].as_str().unwrap().to_string();
    let status = wait_for("backup finished", Duration::from_secs(60), || async {
        let st: serde_json::Value = http
            .get(c.node(&other).url("/api/backups/status"))
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        (st["running"].is_null()
            && (st["last_success"]["id"] == id.as_str() || st["last_failure"]["id"] == id.as_str()))
        .then_some(st)
    })
    .await;
    assert_eq!(status["last_success"]["id"], id.as_str(), "{status}");
    let m: fluxmq_proto::backup::BackupManifest = http
        .get(c.node(&other).url(&format!("/api/backups/{id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(m.node_id, controller, "the controller wrote the manifest");
    let bk = m.channels.iter().find(|ch| ch.name == "bk").unwrap();
    assert_eq!(bk.partitions, 6);
    assert_eq!(bk.messages, stored);
    let uploaders: BTreeSet<String> = m.nodes.iter().cloned().collect();
    assert!(
        leader_nodes.is_subset(&uploaders),
        "every leader uploaded: {leader_nodes:?} vs {uploaders:?}"
    );

    // Restore into a fresh single-node data dir and count the messages.
    let restored = c._dir.path().join("restored");
    let t = fluxmq_backup::Target::open(&fluxmq_proto::backup::BackupTarget::Filesystem {
        path: target.display().to_string(),
    })
    .unwrap();
    fluxmq_backup::restore::restore(
        &t,
        &id,
        &restored,
        None,
        &fluxmq_backup::restore::RestoreOptions { fsync: false },
        &|_| {},
    )
    .await
    .unwrap();
    let mut cfg = ServerConfig::ephemeral(&restored);
    cfg.ui = fluxmq_api::UiSource::Off;
    let single = Server::bind(cfg).await.unwrap();
    let addr = single.http_addr();
    let stop = CancellationToken::new();
    let h = tokio::spawn(single.run(stop.clone().cancelled_owned()));
    let mut total = 0;
    for p in 0..6 {
        let page: serde_json::Value = http
            .get(format!(
                "http://{addr}/api/channels/bk/messages?partition={p}&offset=0&limit=1000"
            ))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        total += page["messages"].as_array().unwrap().len();
    }
    assert_eq!(total as u64, stored);
    stop.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(20), h).await;

    // Notification history: a test send on the controller is visible from another node.
    let ctrl = c.node(&controller);
    let n: serde_json::Value = http
        .post(ctrl.url("/api/notify/notifiers"))
        .json(&json!({"name": "hook", "kind": {"type": "webhook", "url": "http://127.0.0.1:9/x", "secret": null, "headers": {}}}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let nid = n["id"].as_str().unwrap().to_string();
    let t: serde_json::Value = http
        .post(ctrl.url(&format!("/api/notify/notifiers/{nid}/test")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(t["ok"], false);
    assert_eq!(t["error"], "connection refused", "{t}");
    // Persisted before the test call returned, but `other` proxies to the partition leader
    // only once its metadata replica routes `$system.notifications`: read eventually.
    wait_for(
        "controller history via another node",
        Duration::from_secs(20),
        || async {
            let hist: serde_json::Value = http
                .get(c.node(&other).url("/api/notify/history"))
                .send()
                .await
                .ok()?
                .json()
                .await
                .ok()?;
            hist.as_array()?
                .iter()
                .any(|r| r["notifier_id"] == nid.as_str())
                .then_some(())
        },
    )
    .await;

    for n in c.nodes.iter_mut() {
        n.stop().await;
    }
}

/// `GET /api/cluster/status` on any node has exactly one row per partition, the leader's:
/// with RF = 3 every node also follows partitions, and before the v0.4 integration a node's
/// own follower rows (no follower lag, stale ISR) could win the per-partition dedup.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cluster_status_rows_come_from_partition_leaders() {
    let _ = tracing_subscriber_init();
    let mut c = TestCluster::start(3).await;
    wait_for("3 nodes up", Duration::from_secs(30), || async {
        let md = c.node("n1").metadata().await;
        (md.nodes
            .iter()
            .filter(|x| x.status == NodeStatus::Up)
            .count()
            == 3
            && md.controller.is_some())
        .then_some(())
    })
    .await;
    let http = reqwest::Client::new();
    let r = http
        .post(c.node("n1").url("/api/channels"))
        .json(&json!({"name": "st", "mode": "stream", "partitions": 6}))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());
    for node in ["n1", "n2", "n3"] {
        let rows = wait_for("status with full ISRs", Duration::from_secs(30), || async {
            let st: serde_json::Value = http
                .get(c.node(node).url("/api/cluster/status"))
                .send()
                .await
                .ok()?
                .json()
                .await
                .ok()?;
            let md = c.node(node).metadata().await;
            let led = leaders(&md, "st");
            let rows: Vec<serde_json::Value> = st["partitions"]
                .as_array()?
                .iter()
                .filter(|p| p["channel"] == "st")
                .cloned()
                .collect();
            (rows.len() == 6
                && led.values().all(Option::is_some)
                && rows
                    .iter()
                    .all(|p| p["isr"].as_array().map(Vec::len) == Some(3)))
            .then_some((rows, led))
        })
        .await;
        let (rows, led) = rows;
        for p in &rows {
            let part = p["partition"].as_u64().unwrap() as u32;
            assert_eq!(
                p["leader"].as_str(),
                led[&part].as_deref(),
                "{node}: row of st/{part}"
            );
            // Only the leader's row carries its followers' lag.
            assert_eq!(
                p["follower_lag"].as_object().unwrap().len(),
                2,
                "{node}: st/{part} row is not the leader's: {p}"
            );
        }
    }
    for n in &mut c.nodes {
        n.stop().await;
    }
}

/// A metadata write on a leader that lost its quorum (both other voters stopped, still in the
/// membership) is bounded: `Unavailable` within a few seconds instead of blocking forever in
/// openraft's `client_write` (the v0.5 restart-as-quorum-less-leader hang).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn metadata_write_without_quorum_fails_fast() {
    let _ = tracing_subscriber_init();
    let mut c = TestCluster::start(3).await;
    let controller = wait_for("controller", Duration::from_secs(30), || async {
        let md = c.node("n1").metadata().await;
        let up = md
            .nodes
            .iter()
            .filter(|x| x.status == NodeStatus::Up)
            .count();
        (up == 3).then_some(md.controller.clone()).flatten()
    })
    .await;
    // A write with the quorum works.
    c.node(&controller)
        .cluster
        .meta()
        .put("test/before", json!(1), None)
        .await
        .unwrap();
    for n in c.nodes.iter_mut().filter(|n| n.id != controller) {
        n.stop().await;
    }
    let leader = c.node(&controller);
    let started = Instant::now();
    let res = tokio::time::timeout(
        Duration::from_secs(30),
        leader.cluster.meta().put("test/after", json!(2), None),
    )
    .await
    .expect("a quorum-less metadata write must not block");
    let took = started.elapsed();
    match res {
        Err(fluxmq_meta::MetaError::Unavailable(m)) => {
            assert!(took < Duration::from_secs(15), "took {took:?}: {m}")
        }
        other => panic!("expected Unavailable, got {other:?}"),
    }
    for n in &mut c.nodes {
        n.stop().await;
    }
}

/// Payloads (utf8) of every stored message of `channel` via node `n`.
async fn read_payloads(n: &Node, channel: &str, partitions: u32) -> Vec<String> {
    let mut out = Vec::new();
    for p in 0..partitions {
        let resp = reqwest::get(n.url(&format!(
            "/api/channels/{channel}/messages?partition={p}&offset=0&limit=1000"
        )))
        .await
        .unwrap();
        if !resp.status().is_success() {
            continue;
        }
        let page: serde_json::Value = resp.json().await.unwrap();
        for m in page["messages"].as_array().unwrap() {
            out.push(
                m["payload"]["data"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
            );
        }
    }
    out
}

/// Delayed messages in a cluster (v0.12): listed from every node, cancelled through a node
/// that does not hold them, and still released when a node holding some of them goes down
/// before they are due (its `$scheduled` partitions fail over; the new leaders rebuild).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cluster_delayed_messages_survive_a_node_loss() {
    let _ = tracing_subscriber_init();
    let mut c = TestCluster::start(3).await;
    wait_for("3 nodes up", Duration::from_secs(30), || async {
        let md = c.node("n1").metadata().await;
        (md.nodes
            .iter()
            .filter(|x| x.status == NodeStatus::Up)
            .count()
            == 3
            && md.controller.is_some())
        .then_some(())
    })
    .await;
    let http = reqwest::Client::new();
    let r = http
        .post(c.node("n1").url("/api/channels"))
        .json(&json!({"name": "dl", "mode": "stream", "partitions": 3}))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());
    let items: Vec<serde_json::Value> = (0..20)
        .map(|i| json!({ "payload": { "encoding": "utf8", "data": format!("m{i}") }, "delay_ms": 7000 }))
        .collect();
    wait_for(
        "delayed publish accepted",
        Duration::from_secs(30),
        || async {
            let r = http
                .post(c.node("n1").url("/api/channels/dl/publish"))
                .json(&json!({ "messages": items }))
                .send()
                .await
                .ok()?;
            r.status().is_success().then_some(())
        },
    )
    .await;
    let list = wait_for("all 20 listed via n2", Duration::from_secs(20), || async {
        let l: serde_json::Value = http
            .get(c.node("n2").url("/api/scheduled?limit=1000"))
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        (l["total"] == 20).then_some(l)
    })
    .await;
    let items = list["items"].as_array().unwrap();
    // Cancel one through a node that is not its partition's leader (forwarded).
    let victim = &items[0];
    let holder = victim["node"].as_str().unwrap().to_string();
    let via = ["n1", "n2", "n3"]
        .into_iter()
        .find(|n| *n != holder)
        .unwrap();
    let r = http
        .delete(c.node(via).url(&format!(
            "/api/scheduled/{}/{}",
            victim["id"]["partition"], victim["id"]["offset"]
        )))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204, "cancel via {via}");
    // 19 distinct payloads must arrive (the cancelled one never).
    // Stop a node that holds some entries (not n1, which we read through).
    let down = items
        .iter()
        .filter_map(|i| i["node"].as_str())
        .find(|n| *n != "n1")
        .unwrap_or("n3")
        .to_string();
    let idx = c.nodes.iter().position(|n| n.id == down).unwrap();
    c.nodes[idx].stop().await;
    let got = wait_for(
        "19 delivered after failover",
        Duration::from_secs(60),
        || async {
            let all = read_payloads(c.node("n1"), "dl", 3).await;
            let distinct: BTreeSet<String> = all.into_iter().collect();
            (distinct.len() >= 19).then_some(distinct)
        },
    )
    .await;
    // Give a straggling duplicate of the cancelled one a chance to show up, then check.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let distinct: BTreeSet<String> = read_payloads(c.node("n1"), "dl", 3)
        .await
        .into_iter()
        .collect();
    assert_eq!(
        distinct.len(),
        19,
        "cancelled message was delivered: {got:?}"
    );
    for n in &mut c.nodes {
        n.stop().await;
    }
}

/// Idempotent publishing across a failover (v0.12): a batch acknowledged by the old leader and
/// retried on the new one is answered with the stored offsets, not stored twice (the new
/// leader rebuilds its producer table from the replicated log).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cluster_retry_after_failover_is_deduplicated() {
    use fluxmq_proto::batch::{Compression, RecordsSection, encode_record};
    let _ = tracing_subscriber_init();
    let mut c = TestCluster::start(3).await;
    wait_for("3 nodes up", Duration::from_secs(30), || async {
        let md = c.node("n1").metadata().await;
        (md.nodes
            .iter()
            .filter(|x| x.status == NodeStatus::Up)
            .count()
            == 3
            && md.controller.is_some())
        .then_some(())
    })
    .await;
    let http = reqwest::Client::new();
    let r = http
        .post(c.node("n1").url("/api/channels"))
        .json(&json!({"name": "idc", "mode": "stream", "partitions": 1}))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());
    let section = || {
        let mut body = Vec::new();
        for i in 0..4u32 {
            let mut h = std::collections::BTreeMap::new();
            if i == 0 {
                h.insert("flux-pseq".to_string(), "00000000000000ee.5".to_string());
            }
            encode_record(&mut body, i, None, &h, format!("idc-{i}").as_bytes());
        }
        RecordsSection {
            compression: Compression::None,
            record_count: 4,
            body: Bytes::from(body),
        }
    };
    let publish_on = |addr: SocketAddr| async move {
        let mut conn = RawConn::connect_hello(addr, None).await;
        conn.request(ClientRequest::PublishBatch {
            channel: "idc".into(),
            partition: Some(0),
            records: section(),
        })
        .await
    };
    let leader = wait_for(
        "idc leader with full ISR",
        Duration::from_secs(30),
        || async {
            let md = c.node("n1").metadata().await;
            let p = md
                .channels
                .get("idc")?
                .iter()
                .find(|p| p.partition == 0)?
                .clone();
            (p.isr.len() == 3).then_some(p.leader?)
        },
    )
    .await;
    let first = match publish_on(c.node(&leader).client_addr).await {
        Ok(ServerResponse::PublishBatchOk {
            base_offset, count, ..
        }) => (base_offset, count),
        other => panic!("publish: {other:?}"),
    };
    let idx = c.nodes.iter().position(|n| n.id == leader).unwrap();
    c.nodes[idx].stop().await;
    let other = c.running().next().unwrap().id.clone();
    let new_leader = wait_for("new idc leader", Duration::from_secs(30), || async {
        let md = c.node(&other).metadata().await;
        md.channels
            .get("idc")?
            .iter()
            .find(|p| p.partition == 0)?
            .leader
            .clone()
            .filter(|l| *l != leader)
    })
    .await;
    // The retry: same batch, same mark, to the new leader (retrying until it accepts writes).
    let again = wait_for("retry accepted", Duration::from_secs(30), || async {
        match publish_on(c.node(&new_leader).client_addr).await {
            Ok(ServerResponse::PublishBatchOk {
                base_offset, count, ..
            }) => Some((base_offset, count)),
            _ => None,
        }
    })
    .await;
    assert_eq!(first, again, "retry on the new leader got new offsets");
    let payloads = read_payloads(c.node(&new_leader), "idc", 1).await;
    assert_eq!(payloads.len(), 4, "{payloads:?}");
    for n in &mut c.nodes {
        n.stop().await;
    }
}

/// Priority queues in a cluster (v0.12): forwarded publishes (REST via any node, SDK with the
/// cluster's metadata) land in their level's partition, wherever its leader is.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cluster_priority_queue_places_by_level() {
    let _ = tracing_subscriber_init();
    let mut c = TestCluster::start(3).await;
    wait_for("3 nodes up", Duration::from_secs(30), || async {
        let md = c.node("n1").metadata().await;
        (md.nodes
            .iter()
            .filter(|x| x.status == NodeStatus::Up)
            .count()
            == 3
            && md.controller.is_some())
        .then_some(())
    })
    .await;
    let http = reqwest::Client::new();
    let r = http
        .post(c.node("n1").url("/api/channels"))
        .json(&json!({"name": "pq", "mode": "priority", "partitions": 3}))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());
    let md = wait_for(
        "pq in metadata with leaders",
        Duration::from_secs(30),
        || async {
            let md = c.node("n2").metadata().await;
            (md.priority_channels.contains(&"pq".to_string())
                && leaders(&md, "pq").values().all(Option::is_some)
                && leaders(&md, "pq").len() == 3)
                .then_some(md)
        },
    )
    .await;
    assert_eq!(md.priority_channels, vec!["pq".to_string()]);
    for node in ["n1", "n2", "n3"] {
        let r: serde_json::Value = wait_for("REST publish", Duration::from_secs(20), || async {
            let r = http
                .post(c.node(node).url("/api/channels/pq/publish"))
                .json(&json!({ "messages": [
                    { "payload": { "encoding": "utf8", "data": "hi" }, "headers": { "flux-priority": "2" } },
                    { "payload": { "encoding": "utf8", "data": "lo" } },
                    { "payload": { "encoding": "utf8", "data": "mid" }, "headers": { "flux-priority": "1" } },
                ]}))
                .send()
                .await
                .ok()?;
            if !r.status().is_success() {
                return None;
            }
            r.json().await.ok()
        })
        .await;
        let parts: Vec<u64> = r["ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["partition"].as_u64().unwrap())
            .collect();
        assert_eq!(parts, vec![2, 0, 1], "via {node}");
    }
    let client = fluxmq_client::Client::connect(fluxmq_client::ClientOptions::new(
        c.node("n3").client_addr.to_string(),
        "prio-sdk",
    ))
    .await
    .unwrap();
    let ids = client
        .producer("pq")
        .send_batch((0..9u8).map(|i| fluxmq_client::message(format!("s{i}")).priority(i % 3)))
        .await
        .unwrap();
    for (i, id) in ids.iter().enumerate() {
        assert_eq!(id.partition, (i % 3) as u32);
    }
    drop(client);
    for n in &mut c.nodes {
        n.stop().await;
    }
}
