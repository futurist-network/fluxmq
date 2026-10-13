//! Full-TLS cluster end to end (v0.4): three in-process nodes with `gen-dev` certificates —
//! cluster mTLS (Raft, replication, internal HTTP), client port TLS + mTLS and HTTPS. Publish
//! and consume over `tls://` through the SDK, then stop the controller (which leads
//! partitions too) and keep publishing / consuming: no acknowledged message is lost.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fluxmq_client::{
    ChannelConfig, ChannelMode, Client, ClientOptions, StartFrom, SubscribeOptions, TlsOptions,
    message,
};
use fluxmq_cluster::PeerSpec;
use fluxmq_proto::cluster::{ClusterMetadata, NodeStatus};
use fluxmq_server::{ClusterSettings, Server, ServerConfig};
use fluxmq_tls::ClientTlsBuilder;
use fluxmq_tls::dev::GenDevOptions;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const SECRET: &str = "tls-cluster-secret";

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
    raft_addr: SocketAddr,
    tls: fluxmq_tls::TlsRegistry,
    shutdown: CancellationToken,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
}

impl Node {
    /// `full`: client + HTTP TLS too; else cluster TLS only (HTTP / client ports plaintext).
    async fn start(
        id: &str,
        data: PathBuf,
        raft_port: u16,
        peers: &[PeerSpec],
        certs: &Path,
        full: bool,
    ) -> Node {
        let mut config = ServerConfig::ephemeral(&data);
        config.ui = fluxmq_api::UiSource::Off;
        config.fsync = fluxmq_server::FsyncMode::Os;
        config.node_id = Some(id.to_string());
        config.shutdown_timeout = Duration::from_secs(5);
        let raft_addr = SocketAddr::from(([127, 0, 0, 1], raft_port));
        config.cluster = Some(ClusterSettings {
            peers: peers.to_vec(),
            raft_addr,
            replication_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            advertise_host: Some("127.0.0.1".into()),
            replication_factor: 3,
            min_isr: 1,
            cluster_secret: Some(SECRET.into()),
            node_timeout: Duration::from_secs(3),
        });
        if full {
            config.tls.cert = Some(certs.join("server.pem"));
            config.tls.key = Some(certs.join("server-key.pem"));
            config.tls.client_ca = Some(certs.join("ca.pem"));
        }
        config.tls.cluster_cert = Some(certs.join(format!("node-{id}.pem")));
        config.tls.cluster_key = Some(certs.join(format!("node-{id}-key.pem")));
        config.tls.cluster_ca = Some(certs.join("ca.pem"));
        let server = Server::bind(config).await.expect("node binds");
        let (client_addr, http_addr) = (server.client_addr(), server.http_addr());
        let tls = server.tls().clone();
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(server.run(shutdown.clone().cancelled_owned()));
        Node {
            id: id.to_string(),
            client_addr,
            http_addr,
            raft_addr,
            tls,
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
        let scheme = if self.tls.status().http.enabled {
            "https"
        } else {
            "http"
        };
        format!("{scheme}://127.0.0.1:{}{path}", self.http_addr.port())
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.shutdown.cancel();
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

async fn metadata(https: &reqwest::Client, n: &Node) -> Option<ClusterMetadata> {
    https
        .get(n.url("/api/cluster/metadata"))
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()
}

/// Generates certificates and starts three nodes.
async fn start_cluster(full: bool) -> (tempfile::TempDir, PathBuf, Vec<Node>) {
    let dir = tempfile::tempdir().unwrap();
    let certs = dir.path().join("certs");
    fluxmq_tls::dev::generate(&GenDevOptions {
        out: certs.clone(),
        nodes: vec!["n1".into(), "n2".into(), "n3".into()],
        hosts: vec!["localhost".into(), "127.0.0.1".into()],
        force: false,
        ..Default::default()
    })
    .unwrap();
    let ports: Vec<u16> = (0..3).map(|_| free_port()).collect();
    let peers: Vec<PeerSpec> = (0..3)
        .map(|i| PeerSpec {
            id: format!("n{}", i + 1),
            raft_addr: format!("127.0.0.1:{}", ports[i]),
        })
        .collect();
    let nodes =
        futures::future::join_all(
            (0..3).map(|i| {
                let data = dir.path().join(format!("n{}", i + 1));
                let (peers, certs, port) = (peers.clone(), certs.clone(), ports[i]);
                async move {
                    Node::start(&format!("n{}", i + 1), data, port, &peers, &certs, full).await
                }
            }),
        )
        .await;
    (dir, certs, nodes)
}

async fn wait_converged(https: &reqwest::Client, nodes: &[Node]) {
    wait_for(
        "TLS cluster convergence",
        Duration::from_secs(40),
        || async {
            let mut controllers = BTreeSet::new();
            for n in nodes {
                let md = metadata(https, n).await?;
                let up = md
                    .nodes
                    .iter()
                    .filter(|x| x.status == NodeStatus::Up)
                    .count();
                if up != 3 || md.controller.is_none() {
                    return None;
                }
                controllers.insert(md.controller);
            }
            (controllers.len() == 1).then_some(())
        },
    )
    .await;
}

/// Partitions of `channel` in node 0's `/api/cluster/status` come from every leader: the
/// rows of partitions led by peers arrive over node-to-node internal HTTP. Each partition's
/// row must be its metadata leader's (it carries the follower lag), and at least one of them
/// must be led by a peer. (Which nodes lead the channel depends on the leader balance, the
/// system channels included, so not every node necessarily leads one of its partitions.)
async fn wait_status_from_all_leaders(https: &reqwest::Client, nodes: &[Node], channel: &str) {
    let last = std::sync::Mutex::new(String::new());
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let ok = async {
            let md = metadata(https, &nodes[0]).await?;
            let want: BTreeMap<u32, String> = md
                .channels
                .get(channel)?
                .iter()
                .map(|p| Some((p.partition, p.leader.clone()?)))
                .collect::<Option<_>>()?;
            let st: fluxmq_proto::cluster::ClusterStatus = https
                .get(nodes[0].url("/api/cluster/status"))
                .send()
                .await
                .ok()?
                .json()
                .await
                .ok()?;
            let rows: BTreeMap<u32, (Option<String>, usize)> = st
                .partitions
                .iter()
                .filter(|p| p.channel == channel)
                .map(|p| (p.partition, (p.leader.clone(), p.follower_lag.len())))
                .collect();
            *last.lock().unwrap() = format!("want {want:?}, rows {rows:?}");
            let peer_led = want.values().any(|l| *l != nodes[0].id);
            let all = want.iter().all(|(part, leader)| {
                rows.get(part)
                    .is_some_and(|(l, lag)| l.as_ref() == Some(leader) && *lag == 2)
            });
            Some(peer_led && all)
        }
        .await;
        if ok == Some(true) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for: replication status from every leader; {}",
            last.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Cluster telemetry works over cluster TLS: every node pulls its peers' node-stats through
/// the internal (mTLS) client. Regression: the pull used a plain `http://` client, so in a TLS
/// cluster every peer showed as down (`node_down` alerts on every node) and each pull counted
/// as a failed HTTPS handshake.
async fn assert_peer_telemetry(client: &reqwest::Client, nodes: &[Node], https: bool) {
    // Longer than the stale-peer window (3.5 s) plus the alert hysteresis.
    tokio::time::sleep(Duration::from_secs(8)).await;
    for n in nodes {
        let alerts: serde_json::Value = client
            .get(n.url("/api/alerts"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let down: Vec<&String> = alerts
            .as_object()
            .unwrap()
            .keys()
            .filter(|k| k.starts_with("node_down:"))
            .collect();
        assert!(down.is_empty(), "{}: peers seen as down: {down:?}", n.id);
        if https {
            assert_eq!(
                n.tls.status().http.handshake_failures_total,
                0,
                "{}: failed HTTPS handshakes without any bad client",
                n.id
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_node_full_tls_cluster_with_failover() {
    let (_dir, certs, mut nodes) = start_cluster(true).await;

    let tls = ClientTlsBuilder::new()
        .ca(certs.join("ca.pem"))
        .build()
        .unwrap();
    let https = reqwest::Client::builder()
        .tls_backend_preconfigured((*tls.config()).clone())
        .build()
        .unwrap();

    // The cluster forms over mutual TLS: three Up nodes, one controller, seen by everyone.
    wait_converged(&https, &nodes).await;
    assert_peer_telemetry(&https, &nodes, true).await;
    for n in &nodes {
        let st = n.tls.status();
        assert!(st.client.enabled && st.client.mutual);
        assert!(st.http.enabled);
        assert!(st.cluster.enabled && st.cluster.mutual);
        assert!(
            st.cluster
                .certificate
                .as_ref()
                .unwrap()
                .subject
                .contains(&format!("CN={}", n.id))
        );
    }

    // Node-to-node surfaces refuse anything but cluster mTLS.
    let n1 = &nodes[0];
    let plain_raft = reqwest::Client::new()
        .post(format!("http://{}/raft/vote", n1.raft_addr))
        .header(fluxmq_cluster::CLUSTER_SECRET_HEADER, SECRET)
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(plain_raft.status(), 400, "plaintext Raft RPC refused");
    // HTTPS with the right secret but no node certificate: /internal/* is refused.
    let internal = https
        .get(n1.url("/internal/v1/node-stats"))
        .header(fluxmq_cluster::CLUSTER_SECRET_HEADER, SECRET)
        .send()
        .await
        .unwrap();
    assert_eq!(internal.status(), 401);
    assert!(internal.text().await.unwrap().contains("cluster mTLS"));

    // SDK over tls:// with a client certificate, seeded with every node.
    let seeds: Vec<String> = nodes
        .iter()
        .map(|n| format!("tls://{}", n.client_addr))
        .collect();
    let sdk_tls = TlsOptions::new()
        .ca_file(certs.join("ca.pem"))
        .identity_files(certs.join("client.pem"), certs.join("client-key.pem"));
    let client = Client::connect(
        ClientOptions::with_seeds(seeds.clone(), "tls-cluster")
            .tls(sdk_tls.clone())
            .publish_timeout(Duration::from_secs(60)),
    )
    .await
    .expect("SDK connects over TLS");
    let mut channel = ChannelConfig::new("tls.events", ChannelMode::Stream);
    channel.partitions = 6;
    client.ensure_channel(channel).await.unwrap();
    // Every node leads some partition, so the SDK opens TLS connections to all of them.
    wait_for("assignments", Duration::from_secs(20), || async {
        let md = metadata(&https, &nodes[0]).await?;
        let parts = md.channels.get("tls.events")?;
        (parts.len() == 6 && parts.iter().all(|p| p.leader.is_some() && p.isr.len() == 3))
            .then_some(())
    })
    .await;

    // Cluster-wide fan-out reads go node-to-node over internal HTTP (mTLS).
    wait_status_from_all_leaders(&https, &nodes, "tls.events").await;

    let producer = client.producer("tls.events");
    let mut acked = BTreeSet::new();
    for i in 0..300 {
        let body = format!("before-{i}");
        producer
            .send(message(body.clone()).key(format!("k{i}")))
            .await
            .expect("publish over TLS");
        acked.insert(body);
    }

    let mut consumer = client
        .subscribe(
            "tls.events",
            "tls-group",
            SubscribeOptions::default().start(StartFrom::Earliest),
        )
        .await
        .unwrap();
    let mut seen = BTreeSet::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while seen.len() < acked.len() && Instant::now() < deadline {
        if let Ok(Some(Ok(d))) = tokio::time::timeout(Duration::from_secs(1), consumer.next()).await
        {
            seen.insert(String::from_utf8_lossy(d.payload()).into_owned());
            d.ack().await.ok();
        }
    }
    assert_eq!(
        seen, acked,
        "every message consumed over TLS before failover"
    );

    // Failover: stop the controller (Raft re-elects over mTLS, partition leadership moves).
    let md = metadata(&https, &nodes[0]).await.unwrap();
    let controller = md.controller.clone().unwrap();
    let idx = nodes.iter().position(|n| n.id == controller).unwrap();
    nodes[idx].stop().await;
    let survivors: Vec<usize> = (0..3).filter(|i| *i != idx).collect();
    wait_for("new controller", Duration::from_secs(30), || async {
        let md = metadata(&https, &nodes[survivors[0]]).await?;
        let parts = md.channels.get("tls.events")?;
        (md.controller.as_deref().is_some_and(|c| c != controller)
            && parts
                .iter()
                .all(|p| p.leader.as_deref().is_some_and(|l| l != controller)))
        .then_some(())
    })
    .await;

    for i in 0..300 {
        let body = format!("after-{i}");
        producer
            .send(message(body.clone()).key(format!("k{i}")))
            .await
            .expect("publish over TLS after failover");
        acked.insert(body);
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    while !acked.is_subset(&seen) && Instant::now() < deadline {
        if let Ok(Some(Ok(d))) = tokio::time::timeout(Duration::from_secs(1), consumer.next()).await
        {
            seen.insert(String::from_utf8_lossy(d.payload()).into_owned());
            d.ack().await.ok();
        }
    }
    let missing: Vec<_> = acked.difference(&seen).take(10).collect();
    assert!(
        missing.is_empty(),
        "acked but not consumed after failover: {missing:?}"
    );

    // The survivors' cluster TLS saw no bad handshakes from peers.
    for i in &survivors {
        assert_eq!(nodes[*i].tls.status().cluster.last_error, None);
    }

    client.close();
    for n in nodes.iter_mut() {
        n.stop().await;
    }
}

/// Cluster TLS only: client and HTTP ports stay plaintext; peers reach each other's HTTP port
/// with cluster mTLS (the port detects the TLS handshake), browsers and tools use plain HTTP.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cluster_tls_only_keeps_public_ports_plaintext() {
    let (_dir, _certs, mut nodes) = start_cluster(false).await;
    let http = reqwest::Client::new();
    wait_converged(&http, &nodes).await;
    assert_peer_telemetry(&http, &nodes, false).await;
    for n in &nodes {
        let st = n.tls.status();
        assert!(!st.client.enabled && !st.http.enabled);
        assert!(st.cluster.enabled && st.cluster.mutual);
    }
    // Plain SDK on the plaintext client port.
    let client = Client::connect(ClientOptions::new(
        nodes[1].client_addr.to_string(),
        "plain",
    ))
    .await
    .unwrap();
    let mut channel = ChannelConfig::new("plain.events", ChannelMode::Stream);
    channel.partitions = 6;
    client.ensure_channel(channel).await.unwrap();
    wait_status_from_all_leaders(&http, &nodes, "plain.events").await;
    let producer = client.producer("plain.events");
    for i in 0..30 {
        producer
            .send(message(format!("m-{i}")).key(format!("k{i}")))
            .await
            .unwrap();
    }
    // `/internal/*` over plain HTTP is refused even with the right secret.
    let internal = http
        .get(nodes[0].url("/internal/v1/node-stats"))
        .header(fluxmq_cluster::CLUSTER_SECRET_HEADER, SECRET)
        .send()
        .await
        .unwrap();
    assert_eq!(internal.status(), 401);
    client.close();
    for n in nodes.iter_mut() {
        n.stop().await;
    }
}
