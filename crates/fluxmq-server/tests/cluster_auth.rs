//! v0.5 security across a three-node cluster: a token revoked through one node closes the
//! live connections authenticated with it on another node within 1 s (auth tokens live in
//! the Raft-replicated meta store and every node watches them).

mod common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use common::RawConn;
use fluxmq_cluster::PeerSpec;
use fluxmq_proto::wire::{ClientRequest, ErrorCode, PROTOCOL_VERSION, ServerResponse};
use fluxmq_server::{ClusterSettings, Server, ServerConfig};
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const SECRET: &str = "test-cluster-secret";
const ADMIN: &str = "boot-admin";

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Node {
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
        config.admin_token = Some(ADMIN.into());
        config.shutdown_timeout = Duration::from_secs(5);
        config.cluster = Some(ClusterSettings {
            peers: peers.to_vec(),
            raft_addr: SocketAddr::from(([127, 0, 0, 1], raft_port)),
            replication_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            advertise_host: Some("127.0.0.1".into()),
            replication_factor: 3,
            min_isr: 1,
            cluster_secret: Some(SECRET.into()),
            node_timeout: Duration::from_secs(3),
        });
        let server = Server::bind(config).await.expect("node binds");
        let (client_addr, http_addr) = (server.client_addr(), server.http_addr());
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(server.run(shutdown.clone().cancelled_owned()));
        Node {
            client_addr,
            http_addr,
            shutdown,
            handle: Some(handle),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.http_addr, path)
    }

    async fn stop(&mut self) {
        self.shutdown.cancel();
        if let Some(h) = self.handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(20), h).await;
        }
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

/// Hello with `token` on `addr`; `true` when accepted.
async fn hello_ok(addr: SocketAddr, token: &str) -> bool {
    let mut c = RawConn::connect(addr).await;
    matches!(
        c.hello(PROTOCOL_VERSION, Some(token)).await,
        Ok(ServerResponse::HelloOk { .. })
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn revoke_on_one_node_kicks_connections_on_another() {
    let dir = tempfile::tempdir().unwrap();
    let ports: Vec<u16> = (0..3).map(|_| free_port()).collect();
    let peers: Vec<PeerSpec> = (0..3)
        .map(|i| PeerSpec {
            id: format!("n{}", i + 1),
            raft_addr: format!("127.0.0.1:{}", ports[i]),
        })
        .collect();
    let mut nodes = futures::future::join_all((0..3).map(|i| {
        let data = dir.path().join(format!("n{}", i + 1));
        let peers = peers.clone();
        let port = ports[i];
        async move { Node::start(&format!("n{}", i + 1), data, port, &peers).await }
    }))
    .await;
    let http = reqwest::Client::new();

    // Token creation needs a Raft leader: retry until the cluster has formed.
    let created: Value = wait_for("token created via n1", Duration::from_secs(30), || async {
        let resp = http
            .post(nodes[0].url("/api/auth/tokens"))
            .bearer_auth(ADMIN)
            .json(&json!({"name": "svc", "role": "operator"}))
            .send()
            .await
            .ok()?;
        if resp.status() != 201 {
            return None;
        }
        resp.json().await.ok()
    })
    .await;
    let token = created["token"].as_str().unwrap().to_string();
    let id = created["info"]["id"].as_str().unwrap().to_string();

    // The token replicates to n2; connect there (two connections) and to n3.
    let n2 = nodes[1].client_addr;
    let n3 = nodes[2].client_addr;
    wait_for(
        "token visible on n2 and n3",
        Duration::from_secs(15),
        || {
            let token = token.clone();
            async move { (hello_ok(n2, &token).await && hello_ok(n3, &token).await).then_some(()) }
        },
    )
    .await;
    let mut a = RawConn::connect_hello(n2, Some(&token)).await;
    let mut b = RawConn::connect_hello(n2, Some(&token)).await;
    let mut c = RawConn::connect_hello(n3, Some(&token)).await;
    let mut boot = RawConn::connect_hello(n2, Some(ADMIN)).await;
    for conn in [&mut a, &mut b, &mut c] {
        assert!(matches!(
            conn.request(ClientRequest::Ping).await,
            Ok(ServerResponse::Pong)
        ));
    }

    // Revoke through n1.
    let resp = http
        .delete(nodes[0].url(&format!("/api/auth/tokens/{id}")))
        .bearer_auth(ADMIN)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    let since = Instant::now();
    let (ra, rb, rc) = tokio::join!(a.expect_goodbye(), b.expect_goodbye(), c.expect_goodbye());
    let elapsed = since.elapsed();
    for r in [ra, rb, rc] {
        assert_eq!(r.as_deref(), Some("token revoked"));
    }
    assert!(
        elapsed < Duration::from_millis(1500),
        "remote sessions closed after {elapsed:?}"
    );
    // The bootstrap admin's session is unaffected; reconnecting with the token fails on n2.
    assert!(matches!(
        boot.request(ClientRequest::Ping).await,
        Ok(ServerResponse::Pong)
    ));
    let mut again = RawConn::connect(n2).await;
    assert_eq!(
        again
            .hello(PROTOCOL_VERSION, Some(&token))
            .await
            .unwrap_err()
            .code,
        ErrorCode::Unauthorized
    );

    for n in &mut nodes {
        n.stop().await;
    }
}
