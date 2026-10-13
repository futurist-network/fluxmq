//! Server-side `rustls::ServerConfig` builders.

use std::sync::Arc;

use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};

use crate::reload::ReloadableCert;
use crate::{Result, TlsError, provider};

/// Client-certificate policy of a listener.
#[derive(Debug, Clone)]
pub enum ClientAuth {
    /// No client certificates requested.
    None,
    /// Requested; when presented it must chain to these roots, but anonymous clients are
    /// allowed (the HTTP port in a TLS cluster: browsers connect without one, peer nodes
    /// present their node certificate for `/internal/*`).
    Optional(RootCertStore),
    /// Required and verified against these roots (mTLS).
    Required(RootCertStore),
}

/// A server config serving `cert` (reloadable) with the given client-auth policy and ALPN.
pub fn server_config(
    cert: Arc<ReloadableCert>,
    client_auth: ClientAuth,
    alpn: &[&[u8]],
) -> Result<Arc<ServerConfig>> {
    let builder =
        ServerConfig::builder_with_provider(provider()).with_safe_default_protocol_versions()?;
    let builder = match client_auth {
        ClientAuth::None => builder.with_no_client_auth(),
        ClientAuth::Optional(roots) => {
            let v = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider())
                .allow_unauthenticated()
                .build()
                .map_err(|e| TlsError::Config(format!("client CA: {e}")))?;
            builder.with_client_cert_verifier(v)
        }
        ClientAuth::Required(roots) => {
            let v = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider())
                .build()
                .map_err(|e| TlsError::Config(format!("client CA: {e}")))?;
            builder.with_client_cert_verifier(v)
        }
    };
    let mut config = builder.with_cert_resolver(cert);
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    // Lets the fast path take the traffic secrets over after the handshake (in-process
    // only; nothing changes on the wire).
    config.enable_secret_extraction = true;
    Ok(Arc::new(config))
}

/// ALPN for the HTTP port. HTTP/1.1 only: WebSocket upgrades (`/ws`) need it, and the API
/// gains nothing from h2 multiplexing.
pub const HTTP_ALPN: &[&[u8]] = &[b"http/1.1"];
