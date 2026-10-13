//! Shared end-to-end test harness: an in-process server on ephemeral ports plus a raw
//! protocol connection.

#![allow(dead_code)]

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::Duration;

use fluxmq_proto::wire::{
    ClientFrame, ClientRequest, Delivery, PROTOCOL_VERSION, ServerFrame, ServerResponse, WireError,
};
use fluxmq_server::net::codec::ClientCodec;
use fluxmq_server::{Server, ServerConfig};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

pub const WAIT: Duration = Duration::from_secs(10);

pub struct TestServer {
    pub client_addr: SocketAddr,
    pub http_addr: SocketAddr,
    pub token: Option<String>,
    pub broker: fluxmq_core::Broker,
    shutdown: CancellationToken,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
    pub dir: tempfile::TempDir,
    pub http: reqwest::Client,
}

impl TestServer {
    pub async fn start() -> TestServer {
        Self::start_with(|_| {}).await
    }

    pub async fn start_with(tweak: impl FnOnce(&mut ServerConfig)) -> TestServer {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = ServerConfig::ephemeral(dir.path().join("data"));
        config.ui = fluxmq_api::UiSource::Dir(dir.path().join("no-ui"));
        config.fsync = fluxmq_server::FsyncMode::Os;
        tweak(&mut config);
        let token = config.admin_token.clone();
        let server = Server::bind(config).await.expect("server binds");
        let (client_addr, http_addr) = (server.client_addr(), server.http_addr());
        let broker = server.broker().clone();
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(server.run(shutdown.clone().cancelled_owned()));
        TestServer {
            client_addr,
            http_addr,
            token,
            broker,
            shutdown,
            handle: Some(handle),
            dir,
            http: reqwest::Client::new(),
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.http_addr, path)
    }

    /// Request builder with the admin token attached when configured.
    pub fn req(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let b = self.http.request(method, self.url(path));
        match &self.token {
            Some(t) => b.bearer_auth(t),
            None => b,
        }
    }

    pub async fn get_json(&self, path: &str) -> serde_json::Value {
        let resp = self
            .req(reqwest::Method::GET, path)
            .send()
            .await
            .expect("GET");
        assert!(
            resp.status().is_success(),
            "GET {path} -> {}",
            resp.status()
        );
        resp.json().await.expect("json body")
    }

    pub async fn post_json(&self, path: &str, body: serde_json::Value) -> reqwest::Response {
        self.req(reqwest::Method::POST, path)
            .json(&body)
            .send()
            .await
            .expect("POST")
    }

    /// Triggers graceful shutdown and waits for it.
    pub async fn stop(mut self) {
        self.shutdown.cancel();
        if let Some(h) = self.handle.take() {
            tokio::time::timeout(Duration::from_secs(20), h)
                .await
                .expect("server stops in time")
                .expect("server task")
                .expect("server run ok");
        }
    }

    /// Current lag of `group` on `channel` (from the broker's stats snapshot).
    pub fn group_lag(&self, channel: &str, group: &str) -> Option<u64> {
        self.broker
            .stats()
            .channels
            .into_iter()
            .find(|c| c.config.name == channel)?
            .groups
            .into_iter()
            .find(|g| g.name == group)
            .map(|g| g.lag)
    }

    pub fn trigger_shutdown(&self) {
        self.shutdown.cancel();
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// A raw client-protocol connection.
pub struct RawConn {
    framed: Framed<TcpStream, ClientCodec>,
    next_req: u64,
    /// Deliveries received while waiting for a response.
    pub deliveries: VecDeque<(u64, Delivery)>,
}

impl RawConn {
    pub async fn connect(addr: SocketAddr) -> RawConn {
        let stream = TcpStream::connect(addr).await.expect("connect");
        RawConn {
            framed: Framed::new(stream, ClientCodec::new()),
            next_req: 1,
            deliveries: VecDeque::new(),
        }
    }

    pub async fn connect_hello(addr: SocketAddr, token: Option<&str>) -> RawConn {
        let mut c = Self::connect(addr).await;
        let r = c.hello(PROTOCOL_VERSION, token).await;
        assert!(
            matches!(r, Ok(ServerResponse::HelloOk { .. })),
            "hello failed: {r:?}"
        );
        c
    }

    pub async fn hello(
        &mut self,
        version: u16,
        token: Option<&str>,
    ) -> Result<ServerResponse, WireError> {
        self.request(ClientRequest::Hello {
            protocol_version: version,
            client_name: "e2e-raw".into(),
            sdk_version: "test".into(),
            auth_token: token.map(str::to_string),
        })
        .await
    }

    pub async fn send(&mut self, req_id: u64, body: ClientRequest) {
        self.framed
            .send(ClientFrame { req_id, body })
            .await
            .expect("send frame");
    }

    pub async fn send_raw(&mut self, bytes: &[u8]) {
        use tokio::io::AsyncWriteExt;
        self.framed
            .get_mut()
            .write_all(bytes)
            .await
            .expect("write raw");
    }

    /// Next frame from the broker (`None` = connection closed).
    pub async fn recv(&mut self) -> Option<ServerFrame> {
        match tokio::time::timeout(WAIT, self.framed.next())
            .await
            .expect("frame within timeout")
        {
            Some(Ok(f)) => Some(f),
            Some(Err(e)) => panic!("decode error: {e}"),
            None => None,
        }
    }

    /// Sends a request and waits for its response, buffering deliveries.
    pub async fn request(&mut self, body: ClientRequest) -> Result<ServerResponse, WireError> {
        let req_id = self.next_req;
        self.next_req += 1;
        self.send(req_id, body).await;
        loop {
            match self.recv().await {
                Some(ServerFrame::Response { req_id: id, result }) if id == req_id => {
                    return result;
                }
                Some(ServerFrame::Response { req_id: id, .. }) => {
                    panic!("unexpected response for {id}")
                }
                Some(ServerFrame::Deliver {
                    subscription_id,
                    delivery,
                }) => {
                    self.deliveries.push_back((subscription_id, delivery));
                }
                Some(ServerFrame::DeliverBatch {
                    subscription_id,
                    deliveries,
                }) => {
                    for d in deliveries {
                        self.deliveries.push_back((subscription_id, d));
                    }
                }
                Some(ServerFrame::Goodbye { reason }) => {
                    panic!("goodbye while waiting for {req_id}: {reason}")
                }
                Some(ServerFrame::MetadataChanged { .. }) => {}
                Some(ServerFrame::DeliverRecords { .. }) => {
                    panic!("unexpected DeliverRecords (raw records not requested)")
                }
                None => panic!("connection closed while waiting for {req_id}"),
            }
        }
    }

    pub async fn next_delivery(&mut self) -> (u64, Delivery) {
        if let Some(d) = self.deliveries.pop_front() {
            return d;
        }
        match self.recv().await {
            Some(ServerFrame::Deliver {
                subscription_id,
                delivery,
            }) => (subscription_id, delivery),
            Some(ServerFrame::DeliverBatch {
                subscription_id,
                deliveries,
            }) => {
                assert!(!deliveries.is_empty(), "empty DeliverBatch");
                let mut it = deliveries.into_iter();
                let first = it.next().expect("non-empty");
                self.deliveries.extend(it.map(|d| (subscription_id, d)));
                (subscription_id, first)
            }
            Some(other) => panic!("expected delivery, got {other:?}"),
            None => panic!("connection closed while waiting for a delivery"),
        }
    }

    /// Reads until `Goodbye` (returning its reason) or close (`None`).
    pub async fn expect_goodbye(&mut self) -> Option<String> {
        loop {
            match self.recv().await {
                Some(ServerFrame::Goodbye { reason }) => return Some(reason),
                Some(_) => continue,
                None => return None,
            }
        }
    }
}

/// Polls `f` until it returns `Some` or the timeout elapses.
pub async fn eventually<T, F, Fut>(mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        if let Some(v) = f().await {
            return v;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met in time"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
