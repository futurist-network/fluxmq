//! TLS wiring (v0.4): turns [`TlsSettings`] into the per-surface rustls configs, the
//! [`TlsRegistry`] behind `GET /api/tls` / metrics / alerts, and the `fluxmq tls gen-dev`
//! subcommand. Nothing here runs when TLS is not configured.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use clap::{Args, Subcommand};
use fluxmq_proto::wire::{ClientRequest, ErrorCode, ServerFrame, ServerResponse, WireError};
use fluxmq_tls::http::HttpMode;
use fluxmq_tls::registry::Surface;
use fluxmq_tls::rustls::ServerConfig as RustlsServerConfig;
use fluxmq_tls::server::{ClientAuth, HTTP_ALPN, server_config};
use fluxmq_tls::{ClusterTls, HandshakeCounter, ReloadableCert, TlsRegistry};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::config::TlsSettings;
use crate::net::codec::ServerCodec;

/// Everything the listeners need.
#[derive(Clone)]
pub struct TlsRuntime {
    pub registry: TlsRegistry,
    /// Client (binary protocol) port; `None` = plaintext.
    pub client: Option<ClientPortTls>,
    /// HTTP port.
    pub http: HttpMode,
    /// Node-to-node mutual TLS; `None` = plaintext.
    pub cluster: Option<ClusterTls>,
}

impl TlsRuntime {
    /// Plaintext everywhere (no TLS configured).
    pub fn disabled() -> Self {
        Self {
            registry: TlsRegistry::disabled(),
            client: None,
            http: HttpMode::Plain,
            cluster: None,
        }
    }

    /// Loads every configured certificate (fails fast on unreadable / invalid files).
    pub fn build(settings: &TlsSettings, node_id: &str) -> anyhow::Result<Self> {
        if !settings.any_enabled() {
            return Ok(Self::disabled());
        }
        fluxmq_tls::install_default_provider();
        let mut registry = TlsRegistry::builder();

        let client_cert = match settings.client_pair() {
            Some((c, k)) => {
                Some(ReloadableCert::load(c, k).context("loading --tls-cert / --tls-key")?)
            }
            None => None,
        };
        let client_auth = match &settings.client_ca {
            Some(ca) => ClientAuth::Required(
                fluxmq_tls::pem::load_roots(ca).context("loading --tls-client-ca")?,
            ),
            None => ClientAuth::None,
        };
        let mutual = matches!(client_auth, ClientAuth::Required(_));
        if let Some(c) = &client_cert {
            registry = registry.surface(Surface::Client, c.clone(), mutual);
        }

        let cluster_cert = match settings.cluster_files() {
            Some((c, k, _)) => Some(
                ReloadableCert::load(c, k)
                    .context("loading --cluster-tls-cert / --cluster-tls-key")?,
            ),
            None => None,
        };
        if let Some(c) = &cluster_cert {
            registry = registry.surface(Surface::Cluster, c.clone(), true);
        }

        // An HTTP certificate that is the client one is still loaded separately so each
        // surface reports its own reload state.
        let http_cert = match settings.http_pair() {
            Some((c, k)) => {
                Some(ReloadableCert::load(c, k).context("loading the HTTP TLS certificate")?)
            }
            None => None,
        };
        if let Some(c) = &http_cert {
            registry = registry.surface(Surface::Http, c.clone(), false);
        }
        let registry = registry.build();

        let cluster = match (settings.cluster_files(), cluster_cert) {
            (Some((_, _, ca)), Some(cert)) => Some(
                ClusterTls::new(node_id, cert, ca, registry.counter(Surface::Cluster))
                    .context("loading --cluster-tls-ca")?,
            ),
            _ => None,
        };
        if let Some(c) = &cluster
            && let Some(cn) = fluxmq_tls::pem::load_certs(c.cert().cert_path())
                .ok()
                .and_then(|chain| fluxmq_tls::info::common_name(&chain[0]))
        {
            fluxmq_tls::cluster::warn_on_mismatch(node_id, Some(&cn), "this node");
        }

        let client = match client_cert {
            Some(cert) => Some(ClientPortTls {
                config: server_config(cert, client_auth, &[])?,
                failures: registry.counter(Surface::Client),
                imp: fluxmq_tls::TlsImpl::from_env(),
            }),
            None => None,
        };

        let http = match (http_cert, &cluster) {
            (Some(cert), cluster) => HttpMode::Tls {
                config: server_config(
                    cert,
                    match cluster {
                        Some(c) => ClientAuth::Optional(c.roots().clone()),
                        None => ClientAuth::None,
                    },
                    HTTP_ALPN,
                )?,
                failures: registry.counter(Surface::Http),
                cluster_client_auth: cluster.is_some(),
            },
            (None, Some(c)) => HttpMode::PlainWithClusterTls { cluster: c.clone() },
            (None, None) => HttpMode::Plain,
        };

        Ok(Self {
            registry,
            client,
            http,
            cluster,
        })
    }

    /// `https` when the HTTP port speaks TLS.
    pub fn http_scheme(&self) -> &'static str {
        match self.http {
            HttpMode::Tls { .. } => "https",
            _ => "http",
        }
    }
}

/// Client port TLS: acceptor config + failure counter.
#[derive(Clone)]
pub struct ClientPortTls {
    pub config: Arc<RustlsServerConfig>,
    pub failures: HandshakeCounter,
    /// Record-layer implementation (`FLUXMQ_TLS_IMPL`: fast (default) | ktls | classic).
    pub imp: fluxmq_tls::TlsImpl,
}

/// An accepted client-port TLS connection: owned halves + the client certificate's CN.
pub type AcceptedTls = (
    fluxmq_tls::TlsRead<TcpStream>,
    fluxmq_tls::TlsWrite<TcpStream>,
    Option<String>,
);

/// Message a plaintext client gets on a TLS client port.
pub const PLAINTEXT_REJECTION: &str =
    "this port requires TLS: connect with a tls:// address or ClientOptions::tls";

impl ClientPortTls {
    /// TLS handshake of an accepted client connection (bounded by `timeout`). A plaintext
    /// client is answered with a protocol error it can display (its `Hello` gets
    /// `ProtocolViolation` + `Goodbye`) instead of a dead socket. Every failure is counted.
    /// Returns the stream and the verified client certificate's CN (mTLS).
    pub async fn accept(
        &self,
        tcp: TcpStream,
        peer: SocketAddr,
        timeout: Duration,
    ) -> Option<AcceptedTls> {
        match fluxmq_tls::http::peek_is_tls(&tcp, timeout).await {
            Some(true) => {}
            Some(false) => {
                self.failures.inc();
                tracing::debug!(%peer, "plaintext client on the TLS client port");
                reject_plaintext_client(tcp, timeout).await;
                return None;
            }
            None => return None,
        }
        let accept = fluxmq_tls::stream::accept(self.imp, self.config.clone(), tcp);
        match tokio::time::timeout(timeout, accept).await {
            Ok(Ok(s)) => {
                let cn = fluxmq_tls::info::peer_common_name(s.info.peer_certificates.as_deref());
                Some((s.read, s.write, cn))
            }
            Ok(Err(e)) => {
                self.failures.inc();
                tracing::debug!(%peer, error = %e, "client TLS handshake failed");
                None
            }
            Err(_) => {
                self.failures.inc();
                tracing::debug!(%peer, "client TLS handshake timed out");
                None
            }
        }
    }
}

/// Reads the plaintext client's first frame and answers it with `ProtocolViolation` and a
/// `Goodbye`, so SDKs report "this port requires TLS" rather than a reset connection.
async fn reject_plaintext_client(tcp: TcpStream, timeout: Duration) {
    let (rd, wr) = tcp.into_split();
    let mut reader = FramedRead::new(rd, ServerCodec::new());
    let mut writer = FramedWrite::new(wr, ServerCodec::new());
    let _ = tokio::time::timeout(timeout, async {
        if let Some(Ok(frame)) = reader.next().await
            && matches!(frame.body, ClientRequest::Hello { .. })
        {
            let err = WireError::new(ErrorCode::ProtocolViolation, PLAINTEXT_REJECTION);
            writer
                .feed(ServerFrame::Response {
                    req_id: frame.req_id,
                    result: Err::<ServerResponse, _>(err),
                })
                .await?;
        }
        writer
            .feed(ServerFrame::Goodbye {
                reason: PLAINTEXT_REJECTION.into(),
            })
            .await?;
        writer.flush().await?;
        writer.close().await
    })
    .await;
}

// ---------------------------------------------------------------------------------------
// `fluxmq tls …`
// ---------------------------------------------------------------------------------------

#[derive(Debug, Clone, Subcommand)]
pub enum TlsAction {
    /// Write a development CA plus server / node / client certificates (never for
    /// production: the CA key is written next to them).
    GenDev(GenDevArgs),
}

#[derive(Debug, Clone, Args)]
pub struct GenDevArgs {
    /// Output directory (created if missing).
    #[arg(long)]
    pub out: PathBuf,
    /// Cluster node ids that get a `node-<id>.pem` / `node-<id>-key.pem` (CN = node id).
    #[arg(long, value_delimiter = ',')]
    pub nodes: Vec<String>,
    /// DNS names / IPs for the server and node certificates' SANs.
    #[arg(long, value_delimiter = ',', default_value = "localhost,127.0.0.1")]
    pub hosts: Vec<String>,
    /// Overwrite existing files in `--out`.
    #[arg(long)]
    pub force: bool,
    /// Validity of the issued (non-CA) certificates in days (default 397). A short value
    /// makes an expiring certificate (the `tls_cert_expiring` alert fires under 14 days).
    #[arg(long)]
    pub valid_days: Option<u32>,
    /// Issue from the existing `ca.pem` / `ca-key.pem` in `--out` (certificate rotation
    /// without changing the CA); the CA files are kept. Use with `--force`.
    #[arg(long)]
    pub reuse_ca: bool,
}

pub fn run(action: TlsAction) -> anyhow::Result<()> {
    match action {
        TlsAction::GenDev(a) => {
            let opts = fluxmq_tls::dev::GenDevOptions {
                out: a.out.clone(),
                nodes: a
                    .nodes
                    .iter()
                    .map(|n| n.trim().to_string())
                    .filter(|n| !n.is_empty())
                    .collect(),
                hosts: a
                    .hosts
                    .iter()
                    .map(|h| h.trim().to_string())
                    .filter(|h| !h.is_empty())
                    .collect(),
                force: a.force,
                valid_days: a.valid_days,
                reuse_ca: a.reuse_ca,
            };
            let files = fluxmq_tls::dev::generate(&opts)?;
            println!(
                "Development certificates written to {} (DO NOT use in production):",
                a.out.display()
            );
            for f in &files {
                println!("  {}", f.display());
            }
            let d = a.out.display();
            println!();
            println!("Single node, TLS + mTLS:");
            println!(
                "  fluxmq --tls-cert {d}/server.pem --tls-key {d}/server-key.pem --tls-client-ca {d}/ca.pem"
            );
            if let Some(n) = opts.nodes.first() {
                println!("Cluster node {n} (all nodes alike):");
                println!(
                    "  fluxmq --node-id {n} ... --cluster-tls-cert {d}/node-{n}.pem \
                     --cluster-tls-key {d}/node-{n}-key.pem --cluster-tls-ca {d}/ca.pem"
                );
            }
            println!(
                "Clients: --tls-ca {d}/ca.pem [--tls-cert {d}/client.pem --tls-key {d}/client-key.pem]"
            );
            Ok(())
        }
    }
}
