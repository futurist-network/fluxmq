//! FluxMQ TLS: one crypto stack (rustls with the aws-lc-rs provider, TLS 1.2 + 1.3) for every
//! surface of the broker and for the SDK. The contract (surfaces, flags, reload semantics)
//! lives in [`fluxmq_proto::tls`].
//!
//! Always compiled (what the SDK needs):
//! * [`pem`] — PEM loading (certificate chains, private keys, root stores).
//! * [`client`] — [`ClientTlsBuilder`]: roots (CA file / PEM bytes / system trust), optional
//!   client identity for mTLS, `danger_accept_invalid_certs` for development.
//! * [`fast`] / [`stream`] — the connection itself: the in-place record layer (default) or
//!   tokio-rustls (`FLUXMQ_TLS_IMPL=classic`), as owned read/write halves.
//!
//! With the default `server` feature (the broker):
//! * [`reload`] — [`ReloadableCert`]: a PEM cert/key pair served through rustls'
//!   `ResolvesServerCert` / `ResolvesClientCert`, re-read when the files change. A failed
//!   reload keeps the previous certificate and records the error.
//! * [`server`] — server-side `rustls::ServerConfig` builders (optional / required client
//!   certificates).
//! * [`cluster`] — [`ClusterTls`]: node-to-node mutual TLS against the cluster CA (acceptor,
//!   connector, the `reqwest` client config).
//! * [`registry`] — [`TlsRegistry`]: the per-surface state behind `GET /api/tls`, handshake
//!   failure counters, the reload loop (mtime check every 30 s + SIGHUP), reload-failure events.
//! * [`http`] — serving an axum app over TLS (hyper-util + tokio-rustls).
//! * [`info`] — [`CertificateInfo`] extraction (x509-parser) and peer common names.
//! * [`dev`] — `fluxmq tls gen-dev`: a development CA plus server / node / client certificates.

pub mod client;
pub mod fast;
pub mod pem;
pub mod stream;

#[cfg(feature = "server")]
pub mod cluster;
#[cfg(feature = "server")]
pub mod dev;
#[cfg(feature = "server")]
pub mod http;
#[cfg(feature = "server")]
pub mod info;
#[cfg(feature = "server")]
pub mod registry;
#[cfg(feature = "server")]
pub mod reload;
#[cfg(feature = "server")]
pub mod server;

use std::sync::{Arc, OnceLock};

pub use client::{ClientTls, ClientTlsBuilder, PemSource};
pub use fluxmq_proto::tls::{CertificateInfo, TlsEndpointStatus, TlsStatus};
pub use rustls;
pub use rustls_pki_types as pki_types;
pub use stream::{TlsImpl, TlsRead, TlsSplit, TlsWrite};
pub use tokio_rustls;

#[cfg(feature = "server")]
pub use cluster::ClusterTls;
#[cfg(feature = "server")]
pub use registry::{HandshakeCounter, ReloadFailure, Surface, TlsRegistry};
#[cfg(feature = "server")]
pub use reload::ReloadableCert;

/// Errors building TLS configuration.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("{path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("{0}")]
    Pem(String),
    #[error("{0}")]
    Config(String),
    #[error(transparent)]
    Rustls(#[from] rustls::Error),
}

pub type Result<T, E = TlsError> = std::result::Result<T, E>;

/// The process-wide crypto provider: aws-lc-rs (the one reqwest and lettre already use, so
/// the binary carries a single crypto stack), with the default suite set in FluxMQ's order:
/// see [`prefer_aes128`].
pub fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    static PROVIDER: OnceLock<Arc<rustls::crypto::CryptoProvider>> = OnceLock::new();
    PROVIDER
        .get_or_init(|| {
            let mut p = rustls::crypto::aws_lc_rs::default_provider();
            prefer_aes128(&mut p.cipher_suites);
            Arc::new(p)
        })
        .clone()
}

/// v0.8: TLS 1.3 AES-128-GCM ahead of AES-256-GCM (rustls' default order is 256 first).
/// Same suites, same versions — only the preference among TLS 1.3 suites changes: our
/// clients offer 128 first and the server, which follows the client's order, therefore
/// negotiates it with FluxMQ peers (AES-128 is ~15 % cheaper per byte and has twice the
/// rustls confidentiality limit, 2^24 records per key). TLS 1.2 order is untouched.
pub fn prefer_aes128(suites: &mut [rustls::SupportedCipherSuite]) {
    use rustls::CipherSuite::{TLS13_AES_128_GCM_SHA256, TLS13_AES_256_GCM_SHA384};
    let pos = |s: &[rustls::SupportedCipherSuite], c| s.iter().position(|x| x.suite() == c);
    if let (Some(a256), Some(a128)) = (
        pos(suites, TLS13_AES_256_GCM_SHA384),
        pos(suites, TLS13_AES_128_GCM_SHA256),
    ) && a128 > a256
    {
        suites[a256..=a128].rotate_right(1);
    }
}

/// Installs aws-lc-rs as the rustls process default (idempotent). Libraries that build their
/// own rustls configs without an explicit provider (e.g. `reqwest` with
/// `use_preconfigured_tls` callers elsewhere) then agree with ours.
pub fn install_default_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// `ServerName` for a `host[:port]` / `[v6]:port` / `tls://host:port` address: the host part
/// as an IP address or a DNS name.
pub fn server_name_for(addr: &str) -> Result<rustls_pki_types::ServerName<'static>> {
    let host = host_of(addr);
    rustls_pki_types::ServerName::try_from(host.to_string())
        .map_err(|e| TlsError::Config(format!("invalid TLS server name '{host}': {e}")))
}

/// Host part of `host:port`, `[v6]:port`, `tls://host:port` or a bare host.
pub fn host_of(addr: &str) -> &str {
    let a = addr.strip_prefix("tls://").unwrap_or(addr);
    if let Some(rest) = a.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(rest);
    }
    match a.rsplit_once(':') {
        // A bare IPv6 address (several colons, no brackets) has no port.
        Some((h, _)) if !h.contains(':') => h,
        _ => a,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_parsing() {
        assert_eq!(host_of("localhost:7420"), "localhost");
        assert_eq!(host_of("tls://mq.example.com:7420"), "mq.example.com");
        assert_eq!(host_of("[::1]:7420"), "::1");
        assert_eq!(host_of("10.0.0.1"), "10.0.0.1");
        assert_eq!(host_of("::1"), "::1");
        assert!(matches!(
            server_name_for("127.0.0.1:1").unwrap(),
            rustls_pki_types::ServerName::IpAddress(_)
        ));
        assert!(matches!(
            server_name_for("tls://node-a:1").unwrap(),
            rustls_pki_types::ServerName::DnsName(_)
        ));
    }

    #[test]
    fn provider_is_aws_lc() {
        assert!(!provider().cipher_suites.is_empty());
    }

    #[test]
    fn suite_set_unchanged_aes128_first() {
        let default = rustls::crypto::aws_lc_rs::default_provider().cipher_suites;
        let ours = &provider().cipher_suites;
        let mut a: Vec<_> = default.iter().map(|s| s.suite()).collect();
        let mut b: Vec<_> = ours.iter().map(|s| s.suite()).collect();
        assert_eq!(b[0], rustls::CipherSuite::TLS13_AES_128_GCM_SHA256);
        assert_eq!(b[1], rustls::CipherSuite::TLS13_AES_256_GCM_SHA384);
        // TLS 1.2 suites keep their order.
        let tls12 = |v: &[rustls::CipherSuite]| -> Vec<_> {
            v.iter()
                .filter(|c| !format!("{c:?}").starts_with("TLS13"))
                .copied()
                .collect()
        };
        assert_eq!(tls12(&a), tls12(&b));
        a.sort_by_key(|c| u16::from(*c));
        b.sort_by_key(|c| u16::from(*c));
        assert_eq!(a, b, "same suite set");
    }
}
