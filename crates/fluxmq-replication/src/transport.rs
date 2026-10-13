//! Replication connection setup: plain TCP or cluster mutual TLS ([`fluxmq_tls::ClusterTls`]).
//!
//! [`Stream`] splits into owned read/write halves that dispatch statically (an enum, no
//! boxing): plaintext keeps tokio's zero-cost `TcpStream::into_split`; TLS uses
//! [`fluxmq_tls::stream`] (by default the fast record layer with independent halves,
//! `FLUXMQ_TLS_IMPL=classic` for tokio-rustls behind `tokio::io::split`).

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use fluxmq_tls::{ClusterTls, TlsRead, TlsSplit, TlsWrite};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

/// An established replication connection.
pub enum Stream {
    Plain(TcpStream),
    /// TLS; carries the peer certificate's CN on the accepting (leader) side.
    Tls(Box<TlsSplit<TcpStream>>, Option<String>),
}

impl Stream {
    /// Leader side: TLS handshake when cluster TLS is on (failures are counted by
    /// [`ClusterTls::accept_split`]).
    pub async fn accept(tcp: TcpStream, tls: Option<&ClusterTls>) -> io::Result<Stream> {
        let _ = tcp.set_nodelay(true);
        match tls {
            None => Ok(Stream::Plain(tcp)),
            Some(t) => {
                let (s, cn) = t.accept_split(tcp).await?;
                Ok(Stream::Tls(Box::new(s), cn))
            }
        }
    }

    /// Follower side: TLS handshake to `addr` when cluster TLS is on.
    pub async fn connect(
        tcp: TcpStream,
        addr: &str,
        tls: Option<&ClusterTls>,
    ) -> io::Result<Stream> {
        let _ = tcp.set_nodelay(true);
        match tls {
            None => Ok(Stream::Plain(tcp)),
            Some(t) => Ok(Stream::Tls(
                Box::new(t.connect_split(addr, None, tcp).await?),
                None,
            )),
        }
    }

    /// Subject CN of the peer's certificate (leader side, TLS only).
    pub fn peer_cn(&self) -> Option<&str> {
        match self {
            Stream::Tls(_, cn) => cn.as_deref(),
            _ => None,
        }
    }

    pub fn into_split(self) -> (ReadHalf, WriteHalf) {
        match self {
            Stream::Plain(s) => {
                let (r, w) = s.into_split();
                (ReadHalf::Plain(r), WriteHalf::Plain(w))
            }
            Stream::Tls(s, _) => {
                let TlsSplit { read, write, .. } = *s;
                (ReadHalf::Tls(read), WriteHalf::Tls(write))
            }
        }
    }
}

pub enum ReadHalf {
    Plain(OwnedReadHalf),
    Tls(TlsRead<TcpStream>),
}

pub enum WriteHalf {
    Plain(OwnedWriteHalf),
    Tls(TlsWrite<TcpStream>),
}

impl AsyncRead for ReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ReadHalf::Plain(r) => Pin::new(r).poll_read(cx, buf),
            ReadHalf::Tls(r) => Pin::new(r).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for WriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            WriteHalf::Plain(w) => Pin::new(w).poll_write(cx, buf),
            WriteHalf::Tls(w) => Pin::new(w).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            WriteHalf::Plain(w) => Pin::new(w).poll_write_vectored(cx, bufs),
            WriteHalf::Tls(w) => Pin::new(w).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            WriteHalf::Plain(w) => w.is_write_vectored(),
            WriteHalf::Tls(w) => w.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            WriteHalf::Plain(w) => Pin::new(w).poll_flush(cx),
            WriteHalf::Tls(w) => Pin::new(w).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            WriteHalf::Plain(w) => Pin::new(w).poll_shutdown(cx),
            WriteHalf::Tls(w) => Pin::new(w).poll_shutdown(cx),
        }
    }
}
