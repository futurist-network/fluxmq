//! Fast TLS path (v0.8): rustls handshake, then records opened in place in our receive
//! buffer and sealed straight from the caller's slices, with independent read/write halves.
//!
//! Why: rustls' buffered API (what `tokio-rustls` wraps) copies every received byte twice
//! (decrypted record → `Vec` → caller) and allocates per record; its unbuffered API still
//! copies each decrypted record into a `Vec` (`ReadTraffic::next_record`, "TODO … in-place
//! decryption" in rustls 0.23) and its `WriteTraffic::encrypt` copies twice. Here a received
//! byte is copied once (decrypted buffer → caller) and `recv` reads up to 64 KiB at a time
//! instead of 4 KiB; a sent byte is copied once (into the record the AEAD seals in place).
//!
//! Security model: everything cryptographic stays in rustls (handshake, certificate
//! verification, key schedule, KeyUpdate secrets, AEAD, nonces); see [`record`] for the part
//! this module owns and `docs/design/v0.8-tls.md` for the analysis.
//!
//! Selection: [`crate::stream`] (`FLUXMQ_TLS_IMPL=classic` keeps tokio-rustls,
//! `ktls` this record layer with kernel TLS when available).

mod handshake;
mod io;
pub mod ktls;
mod locked;
mod record;
#[cfg(all(test, feature = "server"))]
mod tests;

use std::sync::Arc;

use rustls::client::UnbufferedClientConnection;
use rustls::server::UnbufferedServerConnection;
use rustls::{ClientConfig, ProtocolVersion, ServerConfig, SupportedCipherSuite};
use rustls_pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncRead, AsyncWrite};

pub use io::{TlsReadHalf, TlsWriteHalf};
pub use ktls::RawTcp;

/// What the handshake established.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub peer_certificates: Option<Vec<CertificateDer<'static>>>,
    pub cipher_suite: Option<SupportedCipherSuite>,
    pub protocol_version: Option<ProtocolVersion>,
    pub alpn_protocol: Option<Vec<u8>>,
    /// Records are sealed and opened by the kernel (kTLS, v0.10).
    pub kernel_tls: bool,
}

/// The config with secret extraction enabled (required for the hand-over to the record
/// layer; it has no effect on the wire). FluxMQ's own builders set it already, so this only
/// clones configs supplied from outside.
fn extractable_server(config: Arc<ServerConfig>) -> Arc<ServerConfig> {
    if config.enable_secret_extraction {
        return config;
    }
    let mut c = (*config).clone();
    c.enable_secret_extraction = true;
    Arc::new(c)
}

fn extractable_client(config: Arc<ClientConfig>) -> Arc<ClientConfig> {
    if config.enable_secret_extraction {
        return config;
    }
    let mut c = (*config).clone();
    c.enable_secret_extraction = true;
    Arc::new(c)
}

/// Server-side handshake over separate read/write halves of one transport. `kernel_tls`:
/// hand TLS 1.3 records to the kernel when it can take them ([`ktls`]).
pub async fn accept<R, W>(
    config: Arc<ServerConfig>,
    mut rd: R,
    mut wr: W,
    kernel_tls: bool,
) -> std::io::Result<(TlsReadHalf<R>, TlsWriteHalf<W>, SessionInfo)>
where
    R: AsyncRead + Unpin + RawTcp,
    W: AsyncWrite + Unpin,
{
    let conn = UnbufferedServerConnection::new(extractable_server(config))
        .map_err(handshake::tls_error)?;
    let est = handshake::handshake(conn, &mut rd, &mut wr, kernel_tls).await?;
    let info = est.info.clone();
    let (r, w) = io::split(est, rd, wr);
    Ok((r, w, info))
}

/// Client-side handshake over separate read/write halves of one transport. `kernel_tls`:
/// as for [`accept`].
pub async fn connect<R, W>(
    config: Arc<ClientConfig>,
    name: ServerName<'static>,
    mut rd: R,
    mut wr: W,
    kernel_tls: bool,
) -> std::io::Result<(TlsReadHalf<R>, TlsWriteHalf<W>, SessionInfo)>
where
    R: AsyncRead + Unpin + RawTcp,
    W: AsyncWrite + Unpin,
{
    let conn = UnbufferedClientConnection::new(extractable_client(config), name)
        .map_err(handshake::tls_error)?;
    let est = handshake::handshake(conn, &mut rd, &mut wr, kernel_tls).await?;
    let info = est.info.clone();
    let (r, w) = io::split(est, rd, wr);
    Ok((r, w, info))
}
