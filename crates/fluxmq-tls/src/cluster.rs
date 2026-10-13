//! [`ClusterTls`]: node-to-node mutual TLS (Raft RPC, replication, internal HTTP).
//!
//! Every node serves its node certificate and requires the peer's, both verified against the
//! cluster CA. Peers are addressed by whatever host the operator advertised (often an IP), so
//! server-name checks are relaxed: a certificate that chains to the cluster CA is accepted
//! whatever its SANs (the cluster CA is a private trust domain; node-id pinning compares the
//! subject CN with the expected node id and only warns on mismatch). The cluster secret header
//! stays required on top (defense in depth).
//!
//! The internal HTTP client may also reach a peer's public HTTP port when that port serves
//! its own (`--tls-cert` / `--http-tls-cert`) certificate: that certificate must chain to the
//! cluster CA or to the OS trust store.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig,
    SignatureScheme,
};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::{TlsAcceptor, TlsConnector, client, server};

use crate::registry::HandshakeCounter;
use crate::reload::ReloadableCert;
use crate::server::{ClientAuth, server_config};
use crate::{Result, TlsError, info, pem, provider};

/// Upper bound for a node-to-node TLS handshake.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

struct Inner {
    node_id: String,
    cert: Arc<ReloadableCert>,
    roots: RootCertStore,
    server: Arc<ServerConfig>,
    client: Arc<ClientConfig>,
    http_client: ClientConfig,
    failures: HandshakeCounter,
}

/// Cluster mTLS material. Cheap to clone.
#[derive(Clone)]
pub struct ClusterTls {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for ClusterTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterTls")
            .field("node_id", &self.inner.node_id)
            .field("cert", &self.inner.cert)
            .finish_non_exhaustive()
    }
}

impl ClusterTls {
    /// `cert` = this node's (reloadable) certificate, `ca` = the cluster CA bundle.
    pub fn new(
        node_id: impl Into<String>,
        cert: Arc<ReloadableCert>,
        ca: &Path,
        failures: HandshakeCounter,
    ) -> Result<Self> {
        let roots = pem::load_roots(ca)?;
        Self::with_roots(node_id, cert, roots, failures)
    }

    pub fn with_roots(
        node_id: impl Into<String>,
        cert: Arc<ReloadableCert>,
        roots: RootCertStore,
        failures: HandshakeCounter,
    ) -> Result<Self> {
        let server = server_config(cert.clone(), ClientAuth::Required(roots.clone()), &[])?;
        let ca_verifier =
            WebPkiServerVerifier::builder_with_provider(Arc::new(roots.clone()), provider())
                .build()
                .map_err(|e| TlsError::Config(format!("cluster CA: {e}")))?;
        let strict = Arc::new(ClusterServerVerifier {
            ca: ca_verifier.clone(),
            fallback: None,
        });
        let mut client = ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(strict)
            .with_client_cert_resolver(cert.clone());
        client.enable_secret_extraction = true;
        let platform = rustls_platform_verifier::Verifier::new(provider())
            .map_err(|e| TlsError::Config(format!("system trust store: {e}")))?;
        let with_fallback = Arc::new(ClusterServerVerifier {
            ca: ca_verifier,
            fallback: Some(Arc::new(platform)),
        });
        let http_client = ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(with_fallback)
            .with_client_cert_resolver(cert.clone());
        Ok(Self {
            inner: Arc::new(Inner {
                node_id: node_id.into(),
                cert,
                roots,
                server,
                client: Arc::new(client),
                http_client,
                failures,
            }),
        })
    }

    pub fn node_id(&self) -> &str {
        &self.inner.node_id
    }

    pub fn cert(&self) -> &Arc<ReloadableCert> {
        &self.inner.cert
    }

    /// The cluster CA roots (the HTTP port verifies optional peer certificates with them).
    pub fn roots(&self) -> &RootCertStore {
        &self.inner.roots
    }

    /// Server side: node certificate, peer certificate required (cluster CA).
    pub fn server_config(&self) -> Arc<ServerConfig> {
        self.inner.server.clone()
    }

    /// Client side for Raft RPC / replication: node certificate, peer must chain to the CA.
    pub fn client_config(&self) -> Arc<ClientConfig> {
        self.inner.client.clone()
    }

    /// Client config for the internal HTTP client (`reqwest::ClientBuilder::
    /// tls_backend_preconfigured`): like [`client_config`](Self::client_config), but a peer's
    /// public HTTP certificate may also chain to the OS trust store.
    pub fn http_client_config(&self) -> ClientConfig {
        self.inner.http_client.clone()
    }

    pub fn failures(&self) -> HandshakeCounter {
        self.inner.failures.clone()
    }

    pub fn acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(self.inner.server.clone())
    }

    pub fn connector(&self) -> TlsConnector {
        TlsConnector::from(self.inner.client.clone())
    }

    /// Server-side handshake (bounded by [`HANDSHAKE_TIMEOUT`]); failures are counted.
    /// Returns the stream and the peer certificate's CN.
    pub async fn accept<IO>(
        &self,
        io: IO,
    ) -> std::io::Result<(server::TlsStream<IO>, Option<String>)>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        let r = tokio::time::timeout(HANDSHAKE_TIMEOUT, self.acceptor().accept(io)).await;
        match r {
            Ok(Ok(s)) => {
                let cn = info::peer_common_name(s.get_ref().1.peer_certificates());
                Ok((s, cn))
            }
            Ok(Err(e)) => {
                self.inner.failures.inc();
                Err(e)
            }
            Err(_) => {
                self.inner.failures.inc();
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "cluster TLS handshake timed out",
                ))
            }
        }
    }

    /// Client-side handshake to `addr` (`host:port`; the host only feeds SNI). Failures are
    /// counted. With `expect_node`, warns when the peer's CN differs (node-id pinning).
    pub async fn connect<IO>(
        &self,
        addr: &str,
        expect_node: Option<&str>,
        io: IO,
    ) -> std::io::Result<client::TlsStream<IO>>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        let name = crate::server_name_for(addr)
            .unwrap_or_else(|_| ServerName::try_from("fluxmq-node").expect("valid name"));
        let r = tokio::time::timeout(HANDSHAKE_TIMEOUT, self.connector().connect(name, io)).await;
        match r {
            Ok(Ok(s)) => {
                if let Some(want) = expect_node {
                    let cn = info::peer_common_name(s.get_ref().1.peer_certificates());
                    warn_on_mismatch(want, cn.as_deref(), addr);
                }
                Ok(s)
            }
            Ok(Err(e)) => {
                self.inner.failures.inc();
                Err(e)
            }
            Err(_) => {
                self.inner.failures.inc();
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "cluster TLS handshake timed out",
                ))
            }
        }
    }
}

impl ClusterTls {
    /// Like [`accept`](Self::accept), as owned halves on the record layer chosen by
    /// `FLUXMQ_TLS_IMPL` (replication: the bulk node-to-node stream).
    pub async fn accept_split<IO: crate::stream::SplitIo>(
        &self,
        io: IO,
    ) -> std::io::Result<(crate::TlsSplit<IO>, Option<String>)> {
        let accept =
            crate::stream::accept(crate::TlsImpl::from_env(), self.inner.server.clone(), io);
        match tokio::time::timeout(HANDSHAKE_TIMEOUT, accept).await {
            Ok(Ok(s)) => {
                let cn = info::peer_common_name(s.info.peer_certificates.as_deref());
                Ok((s, cn))
            }
            Ok(Err(e)) => {
                self.inner.failures.inc();
                Err(e)
            }
            Err(_) => {
                self.inner.failures.inc();
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "cluster TLS handshake timed out",
                ))
            }
        }
    }

    /// Like [`connect`](Self::connect), as owned halves (see [`accept_split`](Self::accept_split)).
    pub async fn connect_split<IO: crate::stream::SplitIo>(
        &self,
        addr: &str,
        expect_node: Option<&str>,
        io: IO,
    ) -> std::io::Result<crate::TlsSplit<IO>> {
        let name = crate::server_name_for(addr)
            .unwrap_or_else(|_| ServerName::try_from("fluxmq-node").expect("valid name"));
        let connect = crate::stream::connect(
            crate::TlsImpl::from_env(),
            self.inner.client.clone(),
            name,
            io,
        );
        match tokio::time::timeout(HANDSHAKE_TIMEOUT, connect).await {
            Ok(Ok(s)) => {
                if let Some(want) = expect_node {
                    let cn = info::peer_common_name(s.info.peer_certificates.as_deref());
                    warn_on_mismatch(want, cn.as_deref(), addr);
                }
                Ok(s)
            }
            Ok(Err(e)) => {
                self.inner.failures.inc();
                Err(e)
            }
            Err(_) => {
                self.inner.failures.inc();
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "cluster TLS handshake timed out",
                ))
            }
        }
    }
}

/// Node-id pinning: the peer certificate's CN should be the node id. Logs a warning on
/// mismatch (the chain to the cluster CA is what authenticates).
pub fn warn_on_mismatch(expected: &str, cn: Option<&str>, peer: &str) {
    if cn != Some(expected) {
        tracing::warn!(
            expected_node = expected,
            certificate_cn = cn.unwrap_or("<none>"),
            %peer,
            "cluster peer certificate CN does not match its node id"
        );
    }
}

/// Accepts a server certificate that chains to the cluster CA regardless of its names; else
/// (when set) defers to `fallback` with full name checks.
#[derive(Debug)]
struct ClusterServerVerifier {
    ca: Arc<WebPkiServerVerifier>,
    fallback: Option<Arc<dyn ServerCertVerifier>>,
}

impl ServerCertVerifier for ClusterServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self
            .ca
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
        {
            Ok(v) => Ok(v),
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            Err(e) => match &self.fallback {
                Some(f) => f
                    .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
                    .map_err(|_| e),
                None => Err(e),
            },
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.ca.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.ca.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.ca.supported_verify_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dev::{DevCa, Usage, write_file};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn node(dir: &Path, ca: &DevCa, id: &str, sans: &[String]) -> Arc<ReloadableCert> {
        let p = ca.issue(id, sans, Usage::Node).unwrap();
        let c = dir.join(format!("{id}.crt"));
        let k = dir.join(format!("{id}.key"));
        write_file(&c, p.cert_pem.as_bytes(), false).unwrap();
        write_file(&k, p.key_pem.as_bytes(), true).unwrap();
        ReloadableCert::load(c, k).unwrap()
    }

    fn roots(ca: &DevCa) -> RootCertStore {
        pem::roots_from_pem(ca.pair.cert_pem.as_bytes()).unwrap()
    }

    async fn pair(
        server: &ClusterTls,
        client: &ClusterTls,
        addr_name: &str,
    ) -> (std::io::Result<Option<String>>, std::io::Result<()>) {
        let (a, b) = tokio::io::duplex(64 * 1024);
        let s = async {
            let (mut st, cn) = server.accept(a).await?;
            let mut buf = [0u8; 4];
            st.read_exact(&mut buf).await?;
            st.write_all(&buf).await?;
            st.flush().await?;
            Ok(cn)
        };
        let c = async {
            let mut st = client.connect(addr_name, Some("n1"), b).await?;
            st.write_all(b"ping").await?;
            st.flush().await?;
            let mut buf = [0u8; 4];
            st.read_exact(&mut buf).await?;
            assert_eq!(&buf, b"ping");
            Ok(())
        };
        tokio::join!(s, c)
    }

    #[tokio::test]
    async fn mutual_handshake_ignores_names_but_not_the_ca() {
        let dir = tempfile::tempdir().unwrap();
        let ca = DevCa::generate("Cluster CA").unwrap();
        // SANs that don't match the address used to connect.
        let n1 = ClusterTls::with_roots(
            "n1",
            node(dir.path(), &ca, "n1", &["n1.internal".into()]),
            roots(&ca),
            HandshakeCounter::default(),
        )
        .unwrap();
        let n2 = ClusterTls::with_roots(
            "n2",
            node(dir.path(), &ca, "n2", &[]),
            roots(&ca),
            HandshakeCounter::default(),
        )
        .unwrap();
        let (s, c) = pair(&n1, &n2, "10.9.9.9:7423").await;
        assert_eq!(s.unwrap().as_deref(), Some("n2"));
        c.unwrap();

        // A node from another CA is rejected on both sides and counted.
        let other = DevCa::generate("Rogue CA").unwrap();
        let rogue = ClusterTls::with_roots(
            "n1",
            node(dir.path(), &other, "rogue", &[]),
            roots(&other),
            HandshakeCounter::default(),
        )
        .unwrap();
        let (s, c) = pair(&n1, &rogue, "127.0.0.1:1").await;
        assert!(s.is_err() && c.is_err());
        assert_eq!(n1.failures().get(), 1);
        assert_eq!(rogue.failures().get(), 1);
    }

    #[tokio::test]
    async fn plaintext_peer_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let ca = DevCa::generate("Cluster CA").unwrap();
        let n1 = ClusterTls::with_roots(
            "n1",
            node(dir.path(), &ca, "n1", &[]),
            roots(&ca),
            HandshakeCounter::default(),
        )
        .unwrap();
        let (a, mut b) = tokio::io::duplex(1024);
        let srv = tokio::spawn({
            let n1 = n1.clone();
            async move { n1.accept(a).await.map(|_| ()) }
        });
        b.write_all(b"\x00\x00\x00\x10plaintext-hello!")
            .await
            .unwrap();
        assert!(srv.await.unwrap().is_err());
        assert_eq!(n1.failures().get(), 1);
    }
}
