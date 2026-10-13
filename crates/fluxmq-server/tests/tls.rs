//! TLS end to end (v0.4), single node: client port TLS + mTLS through the SDK, plaintext
//! clients refused cleanly and counted, the HTTP port over TLS (REST, metrics, `/api/tls`,
//! WebSocket upgrade), certificate rotation without a restart, reload failures.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use fluxmq_client::{
    ChannelConfig, ChannelMode, Client, ClientError, ClientOptions, ErrorCode, ReconnectPolicy,
    StartFrom, SubscribeOptions, TlsOptions, message,
};
use fluxmq_server::{Server, ServerConfig};
use fluxmq_tls::dev::{GenDevOptions, Usage, write_file};
use fluxmq_tls::{ClientTlsBuilder, TlsRegistry};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const WAIT: Duration = Duration::from_secs(10);

/// `fluxmq tls gen-dev` output in a temp dir.
struct Certs {
    dir: tempfile::TempDir,
}

impl Certs {
    fn generate() -> Certs {
        let dir = tempfile::tempdir().unwrap();
        fluxmq_tls::dev::generate(&GenDevOptions {
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

    fn sdk(&self, identity: bool) -> TlsOptions {
        let t = TlsOptions::new().ca_file(self.p("ca.pem"));
        if identity {
            t.identity_files(self.p("client.pem"), self.p("client-key.pem"))
        } else {
            t
        }
    }

    /// reqwest client trusting the dev CA.
    fn https(&self) -> reqwest::Client {
        let tls = ClientTlsBuilder::new()
            .ca(self.p("ca.pem"))
            .build()
            .unwrap();
        reqwest::Client::builder()
            .tls_backend_preconfigured((*tls.config()).clone())
            .build()
            .unwrap()
    }
}

struct TlsServer {
    client_addr: SocketAddr,
    http_addr: SocketAddr,
    tls: TlsRegistry,
    broker: fluxmq_core::Broker,
    shutdown: CancellationToken,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
    _data: tempfile::TempDir,
}

impl TlsServer {
    async fn start(tweak: impl FnOnce(&mut ServerConfig)) -> TlsServer {
        let data = tempfile::tempdir().unwrap();
        let mut config = ServerConfig::ephemeral(data.path().join("data"));
        config.ui = fluxmq_api::UiSource::Off;
        config.fsync = fluxmq_server::FsyncMode::Os;
        tweak(&mut config);
        let server = Server::bind(config).await.expect("server binds");
        let srv = TlsServer {
            client_addr: server.client_addr(),
            http_addr: server.http_addr(),
            tls: server.tls().clone(),
            broker: server.broker().clone(),
            shutdown: CancellationToken::new(),
            handle: None,
            _data: data,
        };
        let token = srv.shutdown.clone();
        TlsServer {
            handle: Some(tokio::spawn(server.run(token.cancelled_owned()))),
            ..srv
        }
    }

    fn tls_addr(&self) -> String {
        format!("tls://{}", self.client_addr)
    }

    async fn stop(mut self) {
        self.shutdown.cancel();
        if let Some(h) = self.handle.take() {
            tokio::time::timeout(Duration::from_secs(20), h)
                .await
                .expect("server stops in time")
                .expect("server task")
                .expect("server run ok");
        }
    }
}

fn with_tls(certs: &Certs, mtls: bool) -> impl FnOnce(&mut ServerConfig) + '_ {
    move |c| {
        c.tls.cert = Some(certs.p("server.pem"));
        c.tls.key = Some(certs.p("server-key.pem"));
        if mtls {
            c.tls.client_ca = Some(certs.p("ca.pem"));
        }
    }
}

fn no_retry(opts: ClientOptions) -> ClientOptions {
    opts.reconnect(ReconnectPolicy::disabled())
}

async fn eventually<T, F, Fut>(what: &str, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(v) = f().await {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Returns the consumer so callers can inspect the live subscription (dropping it
/// unsubscribes, which races with reading the group's instances).
async fn publish_and_consume(client: &Client, channel: &str, n: usize) -> fluxmq_client::Consumer {
    client
        .ensure_channel(ChannelConfig::new(channel, ChannelMode::Stream))
        .await
        .unwrap();
    let producer = client.producer(channel);
    for i in 0..n {
        producer
            .send(message(format!("m-{i}")).key(format!("k{i}")))
            .await
            .expect("publish over TLS");
    }
    let mut consumer = client
        .subscribe(
            channel,
            "g",
            SubscribeOptions::default().start(StartFrom::Earliest),
        )
        .await
        .unwrap();
    for _ in 0..n {
        let d = tokio::time::timeout(WAIT, consumer.next())
            .await
            .expect("delivery in time")
            .expect("open")
            .expect("ok");
        assert!(d.payload().starts_with(b"m-"));
        d.ack().await.unwrap();
    }
    consumer
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_and_mtls_publish_consume() {
    let certs = Certs::generate();
    let srv = TlsServer::start(with_tls(&certs, true)).await;

    // mTLS client: publish + consume; the CN shows up as the principal hint.
    let client =
        Client::connect(ClientOptions::new(srv.tls_addr(), "tls-e2e").tls(certs.sdk(true)))
            .await
            .expect("mTLS connect");
    let _consumer = publish_and_consume(&client, "tls.orders", 50).await;
    let remote = srv
        .broker
        .stats()
        .channels
        .into_iter()
        .find(|c| c.config.name == "tls.orders")
        .unwrap()
        .groups[0]
        .instances[0]
        .remote_addr
        .clone();
    assert!(remote.ends_with("(tls:client)"), "{remote}");

    // Without a client certificate the broker refuses the connection; counted.
    let before = srv.tls.status().client.handshake_failures_total;
    let anon = Client::connect(no_retry(
        ClientOptions::new(srv.tls_addr(), "anon").tls(certs.sdk(false)),
    ))
    .await;
    assert!(
        anon.is_err(),
        "mTLS must reject a client without a certificate"
    );
    eventually("handshake failure counted", || async {
        (srv.tls.status().client.handshake_failures_total > before).then_some(())
    })
    .await;

    // A client certificate from another CA is refused too.
    let rogue = Certs::generate();
    let r = Client::connect(no_retry(
        ClientOptions::new(srv.tls_addr(), "rogue").tls(
            certs
                .sdk(false)
                .identity_files(rogue.p("client.pem"), rogue.p("client-key.pem")),
        ),
    ))
    .await;
    assert!(r.is_err());

    // A client that doesn't trust the broker's CA fails verification.
    let r = Client::connect(no_retry(
        ClientOptions::new(srv.tls_addr(), "untrusting").tls(rogue.sdk(true)),
    ))
    .await;
    let Err(e) = r else {
        panic!("must not trust a foreign CA")
    };
    assert!(e.to_string().contains("TLS handshake"), "{e}");

    let st = srv.tls.status();
    assert!(st.client.enabled && st.client.mutual);
    assert!(st.http.enabled && !st.http.mutual);
    assert!(!st.cluster.enabled);
    client.close();
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn plaintext_client_on_tls_port_fails_cleanly() {
    let certs = Certs::generate();
    let srv = TlsServer::start(with_tls(&certs, false)).await;

    // The SDK without TLS gets a readable protocol error, not a hang or a reset.
    let started = Instant::now();
    let r = Client::connect(no_retry(ClientOptions::new(
        srv.client_addr.to_string(),
        "plain",
    )))
    .await;
    let Err(e) = r else {
        panic!("plaintext must be refused")
    };
    assert_eq!(e.code(), Some(ErrorCode::ProtocolViolation), "{e:?}");
    assert!(e.to_string().contains("requires TLS"), "{e}");
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(srv.tls.status().client.handshake_failures_total, 1);

    // Raw garbage is counted as well and the socket is closed.
    let mut raw = tokio::net::TcpStream::connect(srv.client_addr)
        .await
        .unwrap();
    raw.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(WAIT, raw.read_to_end(&mut buf)).await;
    assert_eq!(srv.tls.status().client.handshake_failures_total, 2);

    // TLS without a client certificate works (no mTLS configured), and reconnects.
    let client = Client::connect(ClientOptions::new(srv.tls_addr(), "ok").tls(certs.sdk(false)))
        .await
        .unwrap();
    client.ping().await.unwrap();
    // `tls://` alone means TLS with the OS trust store: the dev CA isn't in it.
    let r = Client::connect(no_retry(ClientOptions::new(srv.tls_addr(), "sys"))).await;
    assert!(matches!(r, Err(ClientError::Io(_))), "{r:?}");
    client.close();
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn http_port_over_tls() {
    let certs = Certs::generate();
    let srv = TlsServer::start(with_tls(&certs, false)).await;
    let https = certs.https();
    let url = |p: &str| format!("https://127.0.0.1:{}{p}", srv.http_addr.port());

    let health = https.get(url("/api/health")).send().await.unwrap();
    assert!(health.status().is_success());
    let st: serde_json::Value = https
        .get(url("/api/tls"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(st["client"]["enabled"], true);
    assert_eq!(st["http"]["enabled"], true);
    assert_eq!(st["cluster"]["enabled"], false);
    let cert = &st["http"]["certificate"];
    assert!(cert["subject"].as_str().unwrap().contains("fluxmq-server"));
    assert!(
        cert["sans"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s == "127.0.0.1")
    );
    assert_eq!(cert["sha256_fingerprint"].as_str().unwrap().len(), 64);

    // Plain HTTP on the HTTPS port: a readable 400, counted.
    let plain = reqwest::get(format!("http://{}/api/health", srv.http_addr))
        .await
        .unwrap();
    assert_eq!(plain.status(), 400);
    assert!(plain.text().await.unwrap().contains("HTTPS"));
    assert_eq!(srv.tls.status().http.handshake_failures_total, 1);

    // Prometheus families.
    let metrics = https
        .get(url("/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("fluxmq_tls_handshake_failures_total{"));
    assert!(metrics.contains("surface=\"http\"} 1"), "{metrics}");
    assert!(metrics.contains("fluxmq_tls_cert_expiry_seconds{"));

    // WebSocket upgrade over TLS (wss://) works through the same listener.
    let tls = ClientTlsBuilder::new()
        .ca(certs.p("ca.pem"))
        .build()
        .unwrap();
    let tcp = tokio::net::TcpStream::connect(srv.http_addr).await.unwrap();
    let mut s = tls
        .connect(fluxmq_tls::server_name_for("127.0.0.1").unwrap(), tcp)
        .await
        .unwrap();
    s.write_all(
        b"GET /ws HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\
          Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
    )
    .await
    .unwrap();
    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(WAIT, s.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert!(
        std::str::from_utf8(&buf[..n])
            .unwrap()
            .starts_with("HTTP/1.1 101"),
        "{:?}",
        String::from_utf8_lossy(&buf[..n])
    );
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn http_tls_off_keeps_http_plaintext() {
    let certs = Certs::generate();
    let srv = TlsServer::start(|c| {
        with_tls(&certs, false)(c);
        c.tls.http = Some(false);
    })
    .await;
    let st: serde_json::Value = reqwest::get(format!("http://{}/api/tls", srv.http_addr))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(st["client"]["enabled"], true);
    assert_eq!(st["http"]["enabled"], false);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn no_tls_configured_is_plaintext_everywhere() {
    let srv = TlsServer::start(|_| {}).await;
    let st: serde_json::Value = reqwest::get(format!("http://{}/api/tls", srv.http_addr))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    for s in ["client", "http", "cluster"] {
        assert_eq!(st[s]["enabled"], false);
        assert_eq!(st[s]["handshake_failures_total"], 0);
    }
    let metrics = reqwest::get(format!("http://{}/metrics", srv.http_addr))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!metrics.contains("fluxmq_tls_"));
    let client = Client::connect(ClientOptions::new(srv.client_addr.to_string(), "plain"))
        .await
        .unwrap();
    client.ping().await.unwrap();
    client.close();
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn certificate_rotation_without_restart() {
    let certs = Certs::generate();
    // Serve copies so the test can replace them.
    let cert = certs.p("live.crt");
    let key = certs.p("live.key");
    std::fs::copy(certs.p("server.pem"), &cert).unwrap();
    std::fs::copy(certs.p("server-key.pem"), &key).unwrap();
    let srv = TlsServer::start(|c| {
        c.tls.cert = Some(cert.clone());
        c.tls.key = Some(key.clone());
    })
    .await;
    let https = certs.https();
    let tls_url = format!("https://127.0.0.1:{}/api/tls", srv.http_addr.port());
    let fingerprint = |v: &serde_json::Value, s: &str| {
        v[s]["certificate"]["sha256_fingerprint"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let before: serde_json::Value = https
        .get(&tls_url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    // A live connection established with the old certificate.
    let client = Client::connect(ClientOptions::new(srv.tls_addr(), "live").tls(certs.sdk(false)))
        .await
        .unwrap();
    client.ping().await.unwrap();

    // Rotate: a new certificate from the same CA (rename-replace, as deploy tools do).
    let ca = dev_ca_from(&certs);
    let pair = ca
        .issue(
            "fluxmq-server-rotated",
            &["127.0.0.1".into()],
            Usage::Server,
        )
        .unwrap();
    write_file(&key, pair.key_pem.as_bytes(), true).unwrap();
    write_file(&cert, pair.cert_pem.as_bytes(), false).unwrap();
    srv.tls.check_all(); // what the 30 s watch / SIGHUP do

    let after: serde_json::Value = https
        .get(&tls_url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    for s in ["client", "http"] {
        assert_ne!(
            fingerprint(&before, s),
            fingerprint(&after, s),
            "{s} rotated"
        );
        assert!(
            after[s]["certificate"]["subject"]
                .as_str()
                .unwrap()
                .contains("rotated")
        );
        assert!(after[s]["last_error"].is_null());
    }
    // The old connection is kept; new handshakes see the new certificate.
    client
        .ping()
        .await
        .expect("established connection survives rotation");
    let peer_cn = handshake_cn(srv.client_addr, &certs).await;
    assert_eq!(peer_cn.as_deref(), Some("fluxmq-server-rotated"));

    // A broken rotation keeps serving the current certificate and reports the error.
    write_file(&key, b"not a key", true).unwrap();
    srv.tls.check_all();
    let st = srv.tls.status();
    assert!(st.client.last_error.is_some());
    assert!(
        st.client
            .certificate
            .as_ref()
            .unwrap()
            .subject
            .contains("rotated")
    );
    let again = Client::connect(ClientOptions::new(srv.tls_addr(), "again").tls(certs.sdk(false)))
        .await
        .expect("still serving the last good certificate");
    again.ping().await.unwrap();

    // The failure surfaces as an alert and an event.
    let alerts_url = format!("https://127.0.0.1:{}/api/alerts", srv.http_addr.port());
    let alerts = eventually("tls_reload_failed alert", || {
        let https = https.clone();
        let url = alerts_url.clone();
        async move {
            let v: serde_json::Value = https.get(url).send().await.ok()?.json().await.ok()?;
            let s = v.to_string();
            s.contains("tls_reload_failed").then_some(s)
        }
    })
    .await;
    assert!(alerts.contains("tls_reload_failed:"), "{alerts}");

    again.close();
    client.close();
    srv.stop().await;
}

/// CN of the certificate a fresh TLS handshake with `addr` presents.
async fn handshake_cn(addr: SocketAddr, certs: &Certs) -> Option<String> {
    let tls = ClientTlsBuilder::new()
        .ca(certs.p("ca.pem"))
        .build()
        .unwrap();
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let s = tls
        .connect(fluxmq_tls::server_name_for("127.0.0.1").unwrap(), tcp)
        .await
        .unwrap();
    fluxmq_tls::info::peer_common_name(s.get_ref().1.peer_certificates())
}

/// Re-creates an issuer from the generated CA files (rcgen needs the key + params).
fn dev_ca_from(certs: &Certs) -> fluxmq_tls::dev::DevCa {
    fluxmq_tls::dev::DevCa::from_pem(
        &std::fs::read_to_string(certs.p("ca.pem")).unwrap(),
        &std::fs::read_to_string(certs.p("ca-key.pem")).unwrap(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_certificate_paths_fail_startup() {
    let data = tempfile::tempdir().unwrap();
    let mut config = ServerConfig::ephemeral(data.path());
    config.tls.cert = Some("/nonexistent/server.pem".into());
    config.tls.key = Some("/nonexistent/server-key.pem".into());
    let Err(e) = Server::bind(config).await else {
        panic!("must fail")
    };
    assert!(
        format!("{e:#}").contains("/nonexistent/server.pem"),
        "{e:#}"
    );
}
