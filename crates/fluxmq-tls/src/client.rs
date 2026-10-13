//! Client-side TLS: [`ClientTlsBuilder`] → [`ClientTls`] (a shareable `rustls::ClientConfig`).
//!
//! Server verification:
//! * CA configured, system roots off → webpki against exactly those CAs (deterministic; the
//!   usual choice for a private broker CA);
//! * CA configured and system roots on → the platform verifier plus the extra CAs;
//! * no CA → the platform verifier (OS trust store);
//! * `danger_accept_invalid_certs` → no chain or name verification at all (development only;
//!   handshake signatures are still checked).

use std::path::PathBuf;
use std::sync::Arc;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::{Result, TlsError, pem, provider};

/// PEM material given either as a file path or as bytes.
#[derive(Clone, PartialEq, Eq)]
pub enum PemSource {
    Path(PathBuf),
    Pem(Vec<u8>),
}

impl std::fmt::Debug for PemSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PemSource::Path(p) => f.debug_tuple("Path").field(p).finish(),
            // Never print key material.
            PemSource::Pem(b) => write!(f, "Pem({} bytes)", b.len()),
        }
    }
}

impl From<PathBuf> for PemSource {
    fn from(p: PathBuf) -> Self {
        PemSource::Path(p)
    }
}

impl From<&std::path::Path> for PemSource {
    fn from(p: &std::path::Path) -> Self {
        PemSource::Path(p.to_path_buf())
    }
}

impl From<Vec<u8>> for PemSource {
    fn from(b: Vec<u8>) -> Self {
        PemSource::Pem(b)
    }
}

impl PemSource {
    fn bytes(&self) -> Result<Vec<u8>> {
        match self {
            PemSource::Pem(b) => Ok(b.clone()),
            PemSource::Path(p) => std::fs::read(p).map_err(|source| TlsError::Io {
                path: p.display().to_string(),
                source,
            }),
        }
    }

    fn label(&self) -> String {
        match self {
            PemSource::Path(p) => p.display().to_string(),
            PemSource::Pem(_) => "<inline PEM>".into(),
        }
    }

    pub fn certs(&self) -> Result<Vec<CertificateDer<'static>>> {
        pem::certs_from_pem(&self.bytes()?)
            .map_err(|e| TlsError::Pem(format!("{}: {e}", self.label())))
    }

    pub fn key(&self) -> Result<PrivateKeyDer<'static>> {
        pem::key_from_pem(&self.bytes()?)
            .map_err(|e| TlsError::Pem(format!("{}: {e}", self.label())))
    }
}

/// Builder for a client-side TLS configuration.
#[derive(Debug, Clone, Default)]
pub struct ClientTlsBuilder {
    /// CA certificates (PEM bundles) that may sign the server certificate.
    pub ca: Vec<PemSource>,
    /// Also trust the OS trust store (always on when no CA is given).
    pub use_system_roots: bool,
    /// Client certificate chain (PEM) for mTLS; requires `client_key`.
    pub client_cert: Option<PemSource>,
    pub client_key: Option<PemSource>,
    /// Skip certificate verification entirely. Development only.
    pub danger_accept_invalid_certs: bool,
    /// ALPN protocols to offer (none by default).
    pub alpn: Vec<Vec<u8>>,
}

impl ClientTlsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ca(mut self, ca: impl Into<PemSource>) -> Self {
        self.ca.push(ca.into());
        self
    }

    pub fn use_system_roots(mut self, on: bool) -> Self {
        self.use_system_roots = on;
        self
    }

    pub fn identity(mut self, cert: impl Into<PemSource>, key: impl Into<PemSource>) -> Self {
        self.client_cert = Some(cert.into());
        self.client_key = Some(key.into());
        self
    }

    pub fn danger_accept_invalid_certs(mut self, on: bool) -> Self {
        self.danger_accept_invalid_certs = on;
        self
    }

    pub fn alpn(mut self, protocols: &[&[u8]]) -> Self {
        self.alpn = protocols.iter().map(|p| p.to_vec()).collect();
        self
    }

    /// The server certificate verifier this builder describes.
    pub fn verifier(&self) -> Result<Arc<dyn ServerCertVerifier>> {
        let provider = provider();
        if self.danger_accept_invalid_certs {
            return Ok(Arc::new(AcceptAnyServerCert(provider)));
        }
        let mut extra = Vec::new();
        for ca in &self.ca {
            extra.extend(ca.certs()?);
        }
        if extra.is_empty() || self.use_system_roots {
            let v = rustls_platform_verifier::Verifier::new_with_extra_roots(extra, provider)
                .map_err(|e| TlsError::Config(format!("system trust store: {e}")))?;
            return Ok(Arc::new(v));
        }
        let mut roots = rustls::RootCertStore::empty();
        let (added, _) = roots.add_parsable_certificates(extra);
        if added == 0 {
            return Err(TlsError::Config("no usable CA certificate".into()));
        }
        WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider)
            .build()
            .map(|v| v as Arc<dyn ServerCertVerifier>)
            .map_err(|e| TlsError::Config(format!("CA certificates: {e}")))
    }

    pub fn build(&self) -> Result<ClientTls> {
        let verifier = self.verifier()?;
        let builder = ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(verifier);
        let mut config = match (&self.client_cert, &self.client_key) {
            (Some(cert), Some(key)) => builder.with_client_auth_cert(cert.certs()?, key.key()?)?,
            (None, None) => builder.with_no_client_auth(),
            _ => {
                return Err(TlsError::Config(
                    "a client certificate needs both the certificate and the key".into(),
                ));
            }
        };
        config.alpn_protocols = self.alpn.clone();
        // See `server::server_config`.
        config.enable_secret_extraction = true;
        Ok(ClientTls {
            config: Arc::new(config),
        })
    }
}

/// A built client configuration. Cheap to clone.
#[derive(Clone)]
pub struct ClientTls {
    config: Arc<ClientConfig>,
}

impl std::fmt::Debug for ClientTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientTls")
            .field("alpn", &self.config.alpn_protocols)
            .field(
                "client_auth",
                &self.config.client_auth_cert_resolver.has_certs(),
            )
            .finish()
    }
}

impl ClientTls {
    pub fn from_config(config: Arc<ClientConfig>) -> Self {
        Self { config }
    }

    pub fn config(&self) -> Arc<ClientConfig> {
        self.config.clone()
    }

    pub fn connector(&self) -> TlsConnector {
        TlsConnector::from(self.config.clone())
    }

    /// TLS handshake over a TCP connection, as owned read/write halves on the record layer
    /// chosen by `FLUXMQ_TLS_IMPL` ([`crate::TlsImpl::from_env`], default fast; `ktls` opt-in).
    pub async fn connect_split<IO: crate::stream::SplitIo>(
        &self,
        server_name: ServerName<'static>,
        io: IO,
    ) -> std::io::Result<crate::TlsSplit<IO>> {
        crate::stream::connect(
            crate::TlsImpl::from_env(),
            self.config.clone(),
            server_name,
            io,
        )
        .await
    }

    /// TLS handshake over an established stream (tokio-rustls stream).
    pub async fn connect<IO>(
        &self,
        server_name: ServerName<'static>,
        io: IO,
    ) -> std::io::Result<TlsStream<IO>>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        self.connector().connect(server_name, io).await
    }
}

/// `danger_accept_invalid_certs`: accepts any server certificate (signatures are verified).
#[derive(Debug)]
pub(crate) struct AcceptAnyServerCert(pub(crate) Arc<CryptoProvider>);

impl ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_needs_both_halves() {
        let b = ClientTlsBuilder {
            client_cert: Some(PemSource::Pem(b"x".to_vec())),
            danger_accept_invalid_certs: true,
            ..Default::default()
        };
        assert!(b.build().is_err());
    }

    #[test]
    fn danger_and_system_roots_build() {
        assert!(
            ClientTlsBuilder::new()
                .danger_accept_invalid_certs(true)
                .build()
                .is_ok()
        );
        assert!(ClientTlsBuilder::new().build().is_ok());
        assert!(
            ClientTlsBuilder::new()
                .ca(b"nope".to_vec())
                .build()
                .is_err()
        );
    }

    #[test]
    fn debug_hides_pem_bytes() {
        let s = format!(
            "{:?}",
            PemSource::Pem(b"-----BEGIN PRIVATE KEY-----".to_vec())
        );
        assert!(!s.contains("PRIVATE"));
    }
}
