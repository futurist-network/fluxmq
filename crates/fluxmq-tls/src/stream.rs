//! TLS connections as owned read/write halves, on either implementation:
//!
//! * [`TlsImpl::Fast`] (default) — [`crate::fast`], the in-place userspace record layer (v0.8).
//! * [`TlsImpl::Kernel`] — [`crate::fast`] with kernel TLS when the kernel offers it (Linux
//!   `tls` module, v0.10), otherwise the same as `Fast`. Opt-in: `FLUXMQ_TLS_IMPL=ktls`. Not the
//!   default because the kernel's AES-GCM can be slower than aws-lc (catch-up −20…−27 % on the
//!   v0.10 A/B host, see docs/design/v0.10-ktls.md).
//! * [`TlsImpl::Classic`] — tokio-rustls behind `tokio::io::split` (the v0.7 path), kept for
//!   one release as a fallback: `FLUXMQ_TLS_IMPL=classic`.
//!
//! Both speak the same TLS (rustls handshake, same configs, same suites), so either side can
//! use either implementation.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use rustls::{ClientConfig, ServerConfig};
use rustls_pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::{TlsAcceptor, TlsConnector, client, server};

use crate::fast::{self, RawTcp, SessionInfo, TlsReadHalf, TlsWriteHalf};

/// Which TLS record-layer implementation carries the client protocol and replication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsImpl {
    Fast,
    Kernel,
    Classic,
}

/// Environment variable selecting the implementation (`fast` | `ktls` | `classic`).
pub const TLS_IMPL_ENV: &str = "FLUXMQ_TLS_IMPL";

impl TlsImpl {
    /// Parses `fast` (alias `userspace`) / `ktls` (alias `kernel`) / `classic`
    /// (case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "fast" | "userspace" | "" => Some(TlsImpl::Fast),
            "ktls" | "kernel" => Some(TlsImpl::Kernel),
            "classic" => Some(TlsImpl::Classic),
            _ => None,
        }
    }

    /// Process-wide choice from `FLUXMQ_TLS_IMPL` (read once; unknown values warn and use
    /// the default).
    pub fn from_env() -> Self {
        static IMPL: OnceLock<TlsImpl> = OnceLock::new();
        *IMPL.get_or_init(|| match std::env::var(TLS_IMPL_ENV) {
            Err(_) => TlsImpl::Fast,
            Ok(v) => TlsImpl::parse(&v).unwrap_or_else(|| {
                tracing::warn!(
                    value = %v,
                    "{TLS_IMPL_ENV}: expected fast|ktls|classic, using fast"
                );
                TlsImpl::Fast
            }),
        })
    }
}

/// A transport that splits into owned halves (`TcpStream::into_split` keeps the halves
/// lock-free).
pub trait SplitIo: AsyncRead + AsyncWrite + Unpin + Send + 'static {
    type Read: AsyncRead + Unpin + Send + RawTcp + 'static;
    type Write: AsyncWrite + Unpin + Send + RawTcp + 'static;
    fn split_io(self) -> (Self::Read, Self::Write);
}

impl SplitIo for tokio::net::TcpStream {
    type Read = tokio::net::tcp::OwnedReadHalf;
    type Write = tokio::net::tcp::OwnedWriteHalf;
    fn split_io(self) -> (Self::Read, Self::Write) {
        self.into_split()
    }
}

impl SplitIo for tokio::io::DuplexStream {
    type Read = tokio::io::ReadHalf<tokio::io::DuplexStream>;
    type Write = tokio::io::WriteHalf<tokio::io::DuplexStream>;
    fn split_io(self) -> (Self::Read, Self::Write) {
        tokio::io::split(self)
    }
}

/// Read half of a TLS connection.
pub enum TlsRead<IO: SplitIo> {
    Fast(TlsReadHalf<IO::Read>),
    ClassicServer(tokio::io::ReadHalf<server::TlsStream<IO>>),
    ClassicClient(tokio::io::ReadHalf<client::TlsStream<IO>>),
}

/// Write half of a TLS connection.
pub enum TlsWrite<IO: SplitIo> {
    Fast(TlsWriteHalf<IO::Write>),
    ClassicServer(tokio::io::WriteHalf<server::TlsStream<IO>>),
    ClassicClient(tokio::io::WriteHalf<client::TlsStream<IO>>),
}

/// An established TLS connection.
pub struct TlsSplit<IO: SplitIo> {
    pub read: TlsRead<IO>,
    pub write: TlsWrite<IO>,
    pub info: SessionInfo,
}

fn info_of(c: &rustls::CommonState) -> SessionInfo {
    SessionInfo {
        peer_certificates: c.peer_certificates().map(|c| c.to_vec()),
        cipher_suite: c.negotiated_cipher_suite(),
        protocol_version: c.protocol_version(),
        alpn_protocol: c.alpn_protocol().map(|p| p.to_vec()),
        kernel_tls: false,
    }
}

/// Server-side handshake with the given implementation.
pub async fn accept<IO: SplitIo>(
    imp: TlsImpl,
    config: Arc<ServerConfig>,
    io: IO,
) -> io::Result<TlsSplit<IO>> {
    match imp {
        TlsImpl::Kernel | TlsImpl::Fast => {
            let (rd, wr) = io.split_io();
            let (r, w, info) = fast::accept(config, rd, wr, imp == TlsImpl::Kernel).await?;
            Ok(TlsSplit {
                read: TlsRead::Fast(r),
                write: TlsWrite::Fast(w),
                info,
            })
        }
        TlsImpl::Classic => {
            let s = TlsAcceptor::from(config).accept(io).await?;
            let info = info_of(s.get_ref().1);
            let (r, w) = tokio::io::split(s);
            Ok(TlsSplit {
                read: TlsRead::ClassicServer(r),
                write: TlsWrite::ClassicServer(w),
                info,
            })
        }
    }
}

/// Client-side handshake with the given implementation.
pub async fn connect<IO: SplitIo>(
    imp: TlsImpl,
    config: Arc<ClientConfig>,
    name: ServerName<'static>,
    io: IO,
) -> io::Result<TlsSplit<IO>> {
    match imp {
        TlsImpl::Kernel | TlsImpl::Fast => {
            let (rd, wr) = io.split_io();
            let (r, w, info) = fast::connect(config, name, rd, wr, imp == TlsImpl::Kernel).await?;
            Ok(TlsSplit {
                read: TlsRead::Fast(r),
                write: TlsWrite::Fast(w),
                info,
            })
        }
        TlsImpl::Classic => {
            let s = TlsConnector::from(config).connect(name, io).await?;
            let info = info_of(s.get_ref().1);
            let (r, w) = tokio::io::split(s);
            Ok(TlsSplit {
                read: TlsRead::ClassicClient(r),
                write: TlsWrite::ClassicClient(w),
                info,
            })
        }
    }
}

impl<IO: SplitIo> AsyncRead for TlsRead<IO> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TlsRead::Fast(r) => Pin::new(r).poll_read(cx, buf),
            TlsRead::ClassicServer(r) => Pin::new(r).poll_read(cx, buf),
            TlsRead::ClassicClient(r) => Pin::new(r).poll_read(cx, buf),
        }
    }
}

impl<IO: SplitIo> AsyncWrite for TlsWrite<IO> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            TlsWrite::Fast(w) => Pin::new(w).poll_write(cx, buf),
            TlsWrite::ClassicServer(w) => Pin::new(w).poll_write(cx, buf),
            TlsWrite::ClassicClient(w) => Pin::new(w).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            TlsWrite::Fast(w) => Pin::new(w).poll_write_vectored(cx, bufs),
            TlsWrite::ClassicServer(w) => Pin::new(w).poll_write_vectored(cx, bufs),
            TlsWrite::ClassicClient(w) => Pin::new(w).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            TlsWrite::Fast(w) => w.is_write_vectored(),
            TlsWrite::ClassicServer(w) => w.is_write_vectored(),
            TlsWrite::ClassicClient(w) => w.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TlsWrite::Fast(w) => Pin::new(w).poll_flush(cx),
            TlsWrite::ClassicServer(w) => Pin::new(w).poll_flush(cx),
            TlsWrite::ClassicClient(w) => Pin::new(w).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TlsWrite::Fast(w) => Pin::new(w).poll_shutdown(cx),
            TlsWrite::ClassicServer(w) => Pin::new(w).poll_shutdown(cx),
            TlsWrite::ClassicClient(w) => Pin::new(w).poll_shutdown(cx),
        }
    }
}
