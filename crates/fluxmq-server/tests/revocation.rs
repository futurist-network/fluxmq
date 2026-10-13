//! v0.5 security end to end, single node: revocation and expiry close live client-protocol
//! connections (`Goodbye`) and WS sessions (close code 4401) within 1 s, the last-admin
//! guard, and certificate bindings over client-port mTLS.

mod common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{RawConn, TestServer, WAIT};
use fluxmq_proto::model::{ChannelConfig, ChannelMode, PublishMessage, StartFrom};
use fluxmq_proto::wire::{
    ClientFrame, ClientRequest, ErrorCode, PROTOCOL_VERSION, ServerFrame, ServerResponse, WireError,
};
use fluxmq_server::net::codec::ClientCodec;
use fluxmq_server::{Server, ServerConfig};
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Budget for "within 1 s" plus scheduling slack on a loaded CI box.
const CLOSE_BUDGET: Duration = Duration::from_millis(1500);

fn channel(name: &str) -> ChannelConfig {
    let mut c = ChannelConfig::new(name, ChannelMode::Queue);
    c.partitions = 1;
    c
}

fn publish(channel: &str) -> ClientRequest {
    ClientRequest::Publish {
        channel: channel.into(),
        partition: None,
        messages: vec![PublishMessage {
            key: None,
            headers: Default::default(),
            payload: Bytes::from_static(b"x"),
        }],
    }
}

fn subscribe(channel: &str) -> ClientRequest {
    ClientRequest::Subscribe {
        channel: channel.into(),
        group: "g".into(),
        prefetch: 10,
        start: StartFrom::Earliest,
    }
}

async fn create_token(srv: &TestServer, body: Value) -> (String, Value) {
    let resp = srv.post_json("/api/auth/tokens", body).await;
    assert_eq!(resp.status(), 201);
    let v: Value = resp.json().await.unwrap();
    (v["token"].as_str().unwrap().to_string(), v["info"].clone())
}

async fn ws_connect(http_addr: SocketAddr, token: &str) -> Ws {
    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://{http_addr}/ws?token={token}"))
            .await
            .expect("ws connect");
    // hello
    let first = tokio::time::timeout(WAIT, ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(first, Message::Text(_)), "{first:?}");
    ws
}

/// Reads until the close frame; returns (code, reason).
async fn ws_close(ws: &mut Ws) -> (u16, String) {
    loop {
        let msg = tokio::time::timeout(WAIT, ws.next())
            .await
            .expect("ws message in time");
        match msg {
            Some(Ok(Message::Close(Some(frame)))) => {
                return (u16::from(frame.code), frame.reason.to_string());
            }
            Some(Ok(Message::Close(None))) | None => panic!("closed without a close frame"),
            Some(Ok(_)) => continue,
            Some(Err(e)) => panic!("ws error before close frame: {e}"),
        }
    }
}

/// Connected consumer (subscribed, with a delivery in flight), producer, and WS session for
/// `token`.
async fn sessions(srv: &TestServer, token: &str) -> (RawConn, RawConn, Ws) {
    let mut producer = RawConn::connect_hello(srv.client_addr, Some(token)).await;
    assert!(matches!(
        producer.request(publish("live")).await,
        Ok(ServerResponse::PublishOk { .. })
    ));
    let mut consumer = RawConn::connect_hello(srv.client_addr, Some(token)).await;
    assert!(matches!(
        consumer.request(subscribe("live")).await,
        Ok(ServerResponse::Subscribed { .. })
    ));
    consumer.next_delivery().await;
    let ws = ws_connect(srv.http_addr, token).await;
    (consumer, producer, ws)
}

async fn assert_all_closed(
    mut consumer: RawConn,
    mut producer: RawConn,
    mut ws: Ws,
    since: Instant,
    reason: &str,
) {
    let (c, p, w) = tokio::join!(
        consumer.expect_goodbye(),
        producer.expect_goodbye(),
        ws_close(&mut ws)
    );
    let elapsed = since.elapsed();
    assert_eq!(c.as_deref(), Some(reason), "consumer");
    assert_eq!(p.as_deref(), Some(reason), "producer");
    assert_eq!(w, (4401, reason.to_string()), "ws");
    assert!(elapsed < CLOSE_BUDGET, "closed after {elapsed:?}");
    // The connections are really gone.
    assert!(consumer.recv().await.is_none());
    assert!(producer.recv().await.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn revoke_closes_live_connections_and_ws() {
    let srv = TestServer::start_with(|c| c.admin_token = Some("s3cret".into())).await;
    srv.broker.create_channel(channel("live")).unwrap();
    let (op, info) = create_token(&srv, json!({"name": "ops", "role": "operator"})).await;
    let (other, _) = create_token(&srv, json!({"name": "other", "role": "operator"})).await;
    let (consumer, producer, ws) = sessions(&srv, &op).await;
    // Sessions of other tokens and of the bootstrap token survive.
    let mut bystander = RawConn::connect_hello(srv.client_addr, Some(&other)).await;
    let mut boot = RawConn::connect_hello(srv.client_addr, Some("s3cret")).await;

    let id = info["id"].as_str().unwrap();
    let resp = srv
        .req(reqwest::Method::DELETE, &format!("/api/auth/tokens/{id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    let since = Instant::now();
    assert_all_closed(consumer, producer, ws, since, "token revoked").await;

    for c in [&mut bystander, &mut boot] {
        assert!(matches!(
            c.request(ClientRequest::Ping).await,
            Ok(ServerResponse::Pong)
        ));
    }
    // Reconnecting with the revoked token fails.
    let mut again = RawConn::connect(srv.client_addr).await;
    let err = again.hello(PROTOCOL_VERSION, Some(&op)).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::Unauthorized);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn expiry_closes_live_connections_and_ws() {
    let srv = TestServer::start_with(|c| c.admin_token = Some("s3cret".into())).await;
    srv.broker.create_channel(channel("live")).unwrap();
    let (tok, info) = create_token(
        &srv,
        json!({"name": "short", "role": "operator", "ttl_secs": 2}),
    )
    .await;
    let expires_at = info["expires_at_ms"].as_u64().unwrap();
    let (consumer, producer, ws) = sessions(&srv, &tok).await;
    let now = fluxmq_proto::now_ms();
    assert!(now < expires_at, "sessions set up before the expiry");
    let since = Instant::now() + Duration::from_millis(expires_at - now);
    let (mut consumer, mut producer, mut ws) = (consumer, producer, ws);
    let (c, p, w) = tokio::join!(
        consumer.expect_goodbye(),
        producer.expect_goodbye(),
        ws_close(&mut ws)
    );
    let closed_at = fluxmq_proto::now_ms();
    assert_eq!(c.as_deref(), Some("token expired"));
    assert_eq!(p.as_deref(), Some("token expired"));
    assert_eq!(w, (4401, "token expired".to_string()));
    assert!(closed_at >= expires_at, "not before the expiry");
    let late = Instant::now().saturating_duration_since(since);
    assert!(late < CLOSE_BUDGET, "closed {late:?} after the expiry");
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn last_admin_guard_over_rest() {
    // Open mode: no bootstrap token.
    let mut srv = TestServer::start().await;
    let (a, a_info) = create_token(&srv, json!({"name": "a", "role": "admin"})).await;
    srv.token = Some(a.clone());
    let (_, b_info) = create_token(&srv, json!({"name": "b", "role": "admin"})).await;
    let (_, v_info) = create_token(&srv, json!({"name": "v", "role": "viewer"})).await;
    let delete = |id: &str| {
        srv.req(reqwest::Method::DELETE, &format!("/api/auth/tokens/{id}"))
            .send()
    };
    assert_eq!(
        delete(b_info["id"].as_str().unwrap())
            .await
            .unwrap()
            .status(),
        204
    );
    let resp = delete(a_info["id"].as_str().unwrap()).await.unwrap();
    assert_eq!(resp.status(), 409);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"], fluxmq_proto::auth::ERROR_LAST_ADMIN);
    // Still works: the admin token was kept.
    assert_eq!(
        srv.req(reqwest::Method::GET, "/api/auth/tokens")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    // Non-admin tokens can still be revoked; the very last token stays a 400.
    assert_eq!(
        delete(v_info["id"].as_str().unwrap())
            .await
            .unwrap()
            .status(),
        204
    );
    assert_eq!(
        delete(a_info["id"].as_str().unwrap())
            .await
            .unwrap()
            .status(),
        400
    );
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cert_binding_rest_without_mtls() {
    let srv = TestServer::start_with(|c| c.admin_token = Some("s3cret".into())).await;
    let list = srv.get_json("/api/auth/cert-bindings").await;
    assert_eq!(list["client_mtls_enabled"], false);
    assert_eq!(list["bindings"], json!([]));
    let resp = srv
        .post_json(
            "/api/auth/cert-bindings",
            json!({"cn": "svc", "role": "viewer"}),
        )
        .await;
    assert_eq!(resp.status(), 201);
    let dup = srv
        .post_json(
            "/api/auth/cert-bindings",
            json!({"cn": "svc", "role": "admin"}),
        )
        .await;
    assert_eq!(dup.status(), 409);
    assert_eq!(dup.json::<Value>().await.unwrap()["error"], "conflict");
    for bad in ["", "  ", "a/b"] {
        let r = srv
            .post_json(
                "/api/auth/cert-bindings",
                json!({"cn": bad, "role": "viewer"}),
            )
            .await;
        assert_eq!(r.status(), 400, "cn {bad:?}");
    }
    // Admin only (a viewer token gets 403 on every binding route).
    let (viewer, _) = create_token(&srv, json!({"name": "dash", "role": "viewer"})).await;
    let r = srv
        .http
        .get(srv.url("/api/auth/cert-bindings"))
        .bearer_auth(&viewer)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    let r = srv
        .http
        .delete(srv.url("/api/auth/cert-bindings/svc"))
        .bearer_auth(&viewer)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    // Audit.
    let audit = srv.get_json("/api/audit").await.to_string();
    assert!(audit.contains("cert_binding.create"), "{audit}");
    let r = srv
        .req(reqwest::Method::DELETE, "/api/auth/cert-bindings/svc")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let r = srv
        .req(reqwest::Method::DELETE, "/api/auth/cert-bindings/svc")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    srv.stop().await;
}

// --- mTLS -------------------------------------------------------------------------------------

struct Certs {
    dir: tempfile::TempDir,
}

impl Certs {
    fn generate() -> Certs {
        let dir = tempfile::tempdir().unwrap();
        fluxmq_tls::dev::generate(&fluxmq_tls::dev::GenDevOptions {
            out: dir.path().to_path_buf(),
            nodes: vec![],
            hosts: vec!["localhost".into(), "127.0.0.1".into()],
            force: false,
            ..Default::default()
        })
        .unwrap();
        Certs { dir }
    }

    fn p(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }
}

type TlsFramed = Framed<fluxmq_tls::tokio_rustls::client::TlsStream<TcpStream>, ClientCodec>;

/// A raw client-protocol connection over TLS presenting the dev `client` certificate
/// (CN `client`).
struct TlsConn {
    framed: TlsFramed,
    next_req: u64,
}

impl TlsConn {
    async fn connect(addr: SocketAddr, certs: &Certs) -> TlsConn {
        let tls = fluxmq_tls::ClientTlsBuilder::new()
            .ca(certs.p("ca.pem"))
            .identity(certs.p("client.pem"), certs.p("client-key.pem"))
            .build()
            .unwrap();
        let tcp = TcpStream::connect(addr).await.unwrap();
        let stream = tls
            .connect(fluxmq_tls::server_name_for("127.0.0.1").unwrap(), tcp)
            .await
            .unwrap();
        TlsConn {
            framed: Framed::new(stream, ClientCodec::new()),
            next_req: 1,
        }
    }

    async fn recv(&mut self) -> Option<ServerFrame> {
        match tokio::time::timeout(WAIT, self.framed.next())
            .await
            .expect("frame in time")
        {
            Some(Ok(f)) => Some(f),
            Some(Err(_)) | None => None,
        }
    }

    async fn request(&mut self, body: ClientRequest) -> Result<ServerResponse, WireError> {
        let req_id = self.next_req;
        self.next_req += 1;
        self.framed
            .send(ClientFrame { req_id, body })
            .await
            .unwrap();
        loop {
            match self.recv().await {
                Some(ServerFrame::Response { req_id: id, result }) if id == req_id => {
                    return result;
                }
                Some(ServerFrame::Goodbye { reason }) => panic!("goodbye: {reason}"),
                Some(_) => continue,
                None => panic!("closed while waiting for {req_id}"),
            }
        }
    }

    async fn hello(&mut self, token: Option<&str>) -> Result<ServerResponse, WireError> {
        self.request(ClientRequest::Hello {
            protocol_version: PROTOCOL_VERSION,
            client_name: "mtls".into(),
            sdk_version: "test".into(),
            auth_token: token.map(str::to_string),
        })
        .await
    }

    async fn expect_goodbye(&mut self) -> Option<String> {
        loop {
            match self.recv().await {
                Some(ServerFrame::Goodbye { reason }) => return Some(reason),
                Some(_) => continue,
                None => return None,
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cert_binding_authenticates_over_mtls() {
    let certs = Certs::generate();
    let data = tempfile::tempdir().unwrap();
    let mut config = ServerConfig::ephemeral(data.path().join("data"));
    config.ui = fluxmq_api::UiSource::Off;
    config.fsync = fluxmq_server::FsyncMode::Os;
    config.admin_token = Some("s3cret".into());
    config.tls.cert = Some(certs.p("server.pem"));
    config.tls.key = Some(certs.p("server-key.pem"));
    config.tls.client_ca = Some(certs.p("ca.pem"));
    let server = Server::bind(config).await.unwrap();
    let (client_addr, http_addr) = (server.client_addr(), server.http_addr());
    let broker = server.broker().clone();
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(server.run(shutdown.clone().cancelled_owned()));
    for name in ["certs.a", "other"] {
        broker.create_channel(channel(name)).unwrap();
    }
    // The HTTP port reuses the client-port certificate (HTTPS, no client auth).
    let http = reqwest::Client::builder()
        .tls_backend_preconfigured(
            (*fluxmq_tls::ClientTlsBuilder::new()
                .ca(certs.p("ca.pem"))
                .build()
                .unwrap()
                .config())
            .clone(),
        )
        .build()
        .unwrap();
    let base = format!("https://{http_addr}");
    let api =
        |m: reqwest::Method, p: &str| http.request(m, format!("{base}{p}")).bearer_auth("s3cret");

    // No binding yet: a certificate alone is not enough.
    let mut c = TlsConn::connect(client_addr, &certs).await;
    let err = c.hello(None).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::Unauthorized);

    let resp = api(reqwest::Method::POST, "/api/auth/cert-bindings")
        .json(&json!({
            "cn": "client",
            "role": "viewer",
            "scopes": {"publish": ["certs.*"], "consume": ["certs.*"]}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let list: Value = api(reqwest::Method::GET, "/api/auth/cert-bindings")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["client_mtls_enabled"], true);
    assert_eq!(list["bindings"][0]["cn"], "client");
    assert_eq!(list["bindings"][0]["created_by"], "admin");

    // Certificate alone → the binding's role and scopes.
    let mut c = TlsConn::connect(client_addr, &certs).await;
    assert!(matches!(
        c.hello(None).await,
        Ok(ServerResponse::HelloOk { .. })
    ));
    assert!(matches!(
        c.request(publish("certs.a")).await,
        Ok(ServerResponse::PublishOk { .. })
    ));
    assert_eq!(
        c.request(publish("other")).await.unwrap_err().code,
        ErrorCode::Forbidden
    );
    assert!(matches!(
        c.request(subscribe("certs.a")).await,
        Ok(ServerResponse::Subscribed { .. })
    ));
    // last_used_ms is recorded.
    let list: Value = api(reqwest::Method::GET, "/api/auth/cert-bindings")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(list["bindings"][0]["last_used_ms"].as_u64().is_some());

    // A token wins over the certificate: a scope-less viewer token cannot publish.
    let created: Value = api(reqwest::Method::POST, "/api/auth/tokens")
        .json(&json!({"name": "dash", "role": "viewer"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let viewer = created["token"].as_str().unwrap();
    let mut t = TlsConn::connect(client_addr, &certs).await;
    assert!(matches!(
        t.hello(Some(viewer)).await,
        Ok(ServerResponse::HelloOk { .. })
    ));
    assert_eq!(
        t.request(publish("certs.a")).await.unwrap_err().code,
        ErrorCode::Forbidden
    );
    // ...and an invalid token is not rescued by the certificate.
    let mut bad = TlsConn::connect(client_addr, &certs).await;
    assert_eq!(
        bad.hello(Some("fmq_nope")).await.unwrap_err().code,
        ErrorCode::Unauthorized
    );

    // Deleting the binding kicks the certificate session (not the token one).
    let resp = api(reqwest::Method::DELETE, "/api/auth/cert-bindings/client")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    let since = Instant::now();
    assert_eq!(
        c.expect_goodbye().await.as_deref(),
        Some("certificate binding revoked")
    );
    assert!(since.elapsed() < CLOSE_BUDGET);
    assert!(matches!(
        t.request(ClientRequest::Ping).await,
        Ok(ServerResponse::Pong)
    ));
    let mut again = TlsConn::connect(client_addr, &certs).await;
    assert_eq!(
        again.hello(None).await.unwrap_err().code,
        ErrorCode::Unauthorized
    );

    let audit: Value = api(reqwest::Method::GET, "/api/audit")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let audit = audit.to_string();
    assert!(audit.contains("cert_binding.delete"), "{audit}");

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
