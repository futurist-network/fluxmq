//! Serving the axum app (REST, WebSocket, UI, `/internal/*`) on the HTTP port, optionally over
//! TLS: hyper-util's auto connection builder on top of tokio-rustls, with graceful shutdown.
//!
//! Modes ([`HttpMode`]):
//! * `Plain` — exactly `axum::serve` (v0.3 behaviour);
//! * `Tls` — every connection must complete a TLS handshake. A plaintext HTTP request gets a
//!   plain `400` explaining that the port speaks HTTPS; every failed handshake is counted. In a
//!   TLS cluster client certificates are *requested* (optional) and verified against the
//!   cluster CA so peer nodes authenticate their `/internal/*` calls;
//! * `PlainWithClusterTls` — the HTTP surface is plaintext (`--http-tls off` or no
//!   `--tls-cert`) but cluster TLS is on: a connection starting with a TLS handshake record is
//!   served as cluster mTLS (node certificate, peer certificate required), anything else as
//!   plain HTTP. Only peer nodes open TLS connections to a plaintext HTTP port.
//!
//! Every request carries a [`PeerIdentity`] extension; [`require_cluster_peer`] rejects
//! `/internal/*` requests that did not arrive over a cluster-verified connection.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::server::graceful::GracefulShutdown;
use rustls::ServerConfig;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

use crate::cluster::ClusterTls;
use crate::info;
use crate::registry::HandshakeCounter;

/// Upper bound for the first byte + TLS handshake of an HTTP connection.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// First byte of a TLS record carrying a handshake message (ClientHello).
pub const TLS_HANDSHAKE_RECORD: u8 = 0x16;

/// Who is on the other end of an HTTP connection (request extension).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PeerIdentity {
    /// Subject CN of a verified client certificate.
    pub cn: Option<String>,
    /// The client certificate chains to the cluster CA (a peer node).
    pub cluster_verified: bool,
}

/// How the HTTP port is served.
#[derive(Clone)]
pub enum HttpMode {
    Plain,
    Tls {
        config: Arc<ServerConfig>,
        failures: HandshakeCounter,
        /// Client certificates are verified against the cluster CA (optional client auth).
        cluster_client_auth: bool,
    },
    PlainWithClusterTls {
        cluster: ClusterTls,
    },
}

impl std::fmt::Debug for HttpMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpMode::Plain => f.write_str("Plain"),
            HttpMode::Tls {
                cluster_client_auth,
                ..
            } => f
                .debug_struct("Tls")
                .field("cluster_client_auth", cluster_client_auth)
                .finish_non_exhaustive(),
            HttpMode::PlainWithClusterTls { .. } => f.write_str("PlainWithClusterTls"),
        }
    }
}

/// Peeks the first byte: `Some(true)` = TLS handshake record, `Some(false)` = something else,
/// `None` = closed or nothing within `timeout`.
pub async fn peek_is_tls(tcp: &TcpStream, timeout: Duration) -> Option<bool> {
    let mut b = [0u8; 1];
    match tokio::time::timeout(timeout, tcp.peek(&mut b)).await {
        Ok(Ok(1)) => Some(b[0] == TLS_HANDSHAKE_RECORD),
        _ => None,
    }
}

/// Serves `app` on `listener` until `shutdown` fires, then lets open connections finish
/// (WebSocket sessions close via the app's own shutdown token).
pub async fn serve(
    listener: TcpListener,
    app: Router,
    mode: HttpMode,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    if matches!(mode, HttpMode::Plain) {
        return axum::serve(listener, app)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await;
    }
    let graceful = GracefulShutdown::new();
    let builder = Builder::new(TokioExecutor::new());
    loop {
        let (tcp, peer) = tokio::select! {
            () = shutdown.cancelled() => break,
            r = listener.accept() => match r {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!(error = %e, "HTTP accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        };
        let _ = tcp.set_nodelay(true);
        let watcher = graceful.watcher();
        let app = app.clone();
        let mode = mode.clone();
        let builder = builder.clone();
        tokio::spawn(async move {
            match mode {
                HttpMode::Plain => unreachable!("handled above"),
                HttpMode::Tls {
                    config,
                    failures,
                    cluster_client_auth,
                } => match peek_is_tls(&tcp, HANDSHAKE_TIMEOUT).await {
                    Some(true) => {
                        let accepted = tokio::time::timeout(
                            HANDSHAKE_TIMEOUT,
                            TlsAcceptor::from(config).accept(tcp),
                        )
                        .await;
                        match accepted {
                            Ok(Ok(tls)) => {
                                let certs = tls.get_ref().1.peer_certificates();
                                // With optional client auth the verifier only accepts chains
                                // to the cluster CA, so any presented certificate is a peer.
                                let ident = PeerIdentity {
                                    cluster_verified: cluster_client_auth && certs.is_some(),
                                    cn: info::peer_common_name(certs),
                                };
                                let conn = builder
                                    .serve_connection_with_upgrades(
                                        TokioIo::new(tls),
                                        service(app, ident),
                                    )
                                    .into_owned();
                                if let Err(e) = watcher.watch(conn).await {
                                    tracing::debug!(%peer, error = %e, "HTTPS connection ended");
                                }
                            }
                            Ok(Err(e)) => {
                                failures.inc();
                                tracing::debug!(%peer, error = %e, "HTTPS handshake failed");
                            }
                            Err(_) => {
                                failures.inc();
                                tracing::debug!(%peer, "HTTPS handshake timed out");
                            }
                        }
                    }
                    Some(false) => {
                        failures.inc();
                        tracing::debug!(%peer, "plaintext request on the HTTPS port");
                        reject_plaintext_http(tcp).await;
                    }
                    None => {}
                },
                HttpMode::PlainWithClusterTls { cluster } => {
                    match peek_is_tls(&tcp, HANDSHAKE_TIMEOUT).await {
                        Some(true) => match cluster.accept(tcp).await {
                            Ok((tls, cn)) => {
                                let ident = PeerIdentity {
                                    cn,
                                    cluster_verified: true,
                                };
                                let conn = builder
                                    .serve_connection_with_upgrades(
                                        TokioIo::new(tls),
                                        service(app, ident),
                                    )
                                    .into_owned();
                                if let Err(e) = watcher.watch(conn).await {
                                    tracing::debug!(%peer, error = %e, "cluster HTTPS connection ended");
                                }
                            }
                            Err(e) => {
                                tracing::debug!(%peer, error = %e, "cluster TLS handshake on the HTTP port failed");
                            }
                        },
                        Some(false) => {
                            let conn = builder
                                .serve_connection_with_upgrades(
                                    TokioIo::new(tcp),
                                    service(app, PeerIdentity::default()),
                                )
                                .into_owned();
                            if let Err(e) = watcher.watch(conn).await {
                                tracing::debug!(%peer, error = %e, "HTTP connection ended");
                            }
                        }
                        None => {}
                    }
                }
            }
        });
    }
    drop(listener);
    graceful.shutdown().await;
    Ok(())
}

fn service(
    app: Router,
    ident: PeerIdentity,
) -> impl hyper::service::Service<
    hyper::Request<Incoming>,
    Response = Response,
    Error = Infallible,
    Future = impl Future<Output = Result<Response, Infallible>> + Send + 'static,
> + Clone
+ Send
+ 'static {
    hyper::service::service_fn(move |mut req: hyper::Request<Incoming>| {
        req.extensions_mut().insert(ident.clone());
        let mut app = app.clone();
        async move { tower_service::Service::call(&mut app, req).await }
    })
}

/// Best-effort plain `400` for an HTTP/1 request that hit the HTTPS port, then close.
async fn reject_plaintext_http<S: AsyncRead + AsyncWrite + Unpin>(mut io: S) {
    const BODY: &str = "This port speaks HTTPS: use https:// instead of http://.\n";
    let resp = format!(
        "HTTP/1.1 400 Bad Request\r\ncontent-type: text/plain; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{BODY}",
        BODY.len()
    );
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        io.write_all(resp.as_bytes()).await?;
        io.shutdown().await
    })
    .await;
}

/// Middleware: `/internal/*` requires a cluster-verified connection ([`PeerIdentity`]).
/// Installed only when cluster TLS is enabled.
pub async fn require_cluster_peer(req: Request, next: Next) -> Response {
    let path = req.uri().path();
    if path.starts_with("/internal/")
        && !req
            .extensions()
            .get::<PeerIdentity>()
            .is_some_and(|p| p.cluster_verified)
    {
        return (
            StatusCode::UNAUTHORIZED,
            "cluster mTLS required: /internal/* only accepts peer nodes presenting a certificate \
             signed by the cluster CA",
        )
            .into_response();
    }
    next.run(req).await
}
