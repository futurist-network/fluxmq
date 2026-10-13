//! Handshake through rustls' unbuffered API, then hand-over to the in-place record layer
//! ([`super::record`]) via rustls' kernel API (`dangerous_into_kernel_connection`).

use std::io;
use std::ops::DerefMut;

use rustls::client::{ClientConnectionData, UnbufferedClientConnection};
use rustls::server::{ServerConnectionData, UnbufferedServerConnection};
use rustls::unbuffered::{
    ConnectionState, EncodeError, EncodeTlsData, UnbufferedConnectionCommon, UnbufferedStatus,
};
use rustls::{Error, ExtractedSecrets};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::SessionInfo;
use super::ktls::{self, Dir, KeyMaterial, RawTcp};
use super::locked::{AnyConn, LockedState};
use super::record::{
    Control, HEADER, Inbound, Kernel, RecordReader, RecordWriter, Shared, decrypter, encrypter,
};

/// Handshake messages larger than this are refused (rustls' default `max_handshake_size`
/// is 64 KiB per message; a flight holds a few).
const MAX_HANDSHAKE_BUFFER: usize = 1 << 20;

/// The two unbuffered connection flavours.
pub(crate) trait Unbuffered:
    DerefMut<Target = UnbufferedConnectionCommon<Self::Data>> + Send
{
    type Data;
    fn process<'c, 'i>(&'c mut self, buf: &'i mut [u8]) -> UnbufferedStatus<'c, 'i, Self::Data>;
    fn into_kernel(self) -> Result<(ExtractedSecrets, Kernel), Error>;
    fn into_any(self) -> AnyConn;
}

impl Unbuffered for UnbufferedClientConnection {
    type Data = ClientConnectionData;

    fn process<'c, 'i>(&'c mut self, buf: &'i mut [u8]) -> UnbufferedStatus<'c, 'i, Self::Data> {
        self.process_tls_records(buf)
    }

    fn into_kernel(self) -> Result<(ExtractedSecrets, Kernel), Error> {
        let (s, k) = self.dangerous_into_kernel_connection()?;
        Ok((s, Kernel::Client(k)))
    }

    fn into_any(self) -> AnyConn {
        AnyConn::Client(self)
    }
}

impl Unbuffered for UnbufferedServerConnection {
    type Data = ServerConnectionData;

    fn process<'c, 'i>(&'c mut self, buf: &'i mut [u8]) -> UnbufferedStatus<'c, 'i, Self::Data> {
        self.process_tls_records(buf)
    }

    fn into_kernel(self) -> Result<(ExtractedSecrets, Kernel), Error> {
        let (s, k) = self.dangerous_into_kernel_connection()?;
        Ok((s, Kernel::Server(k)))
    }

    fn into_any(self) -> AnyConn {
        AnyConn::Server(self)
    }
}

/// An established TLS 1.3 session on the in-place record layer.
pub(crate) struct Kernelized {
    pub(crate) reader: RecordReader,
    pub(crate) writer: RecordWriter,
    pub(crate) shared: std::sync::Arc<Shared>,
    /// Ciphertext received after the last handshake record (an incomplete record at most).
    pub(crate) leftover: Vec<u8>,
    /// Application data rustls already decrypted while finishing the handshake.
    pub(crate) early_plaintext: Vec<u8>,
    /// close_notify already received (while preparing kernel TLS).
    pub(crate) closed: bool,
}

/// An established TLS 1.3 session whose records the kernel seals and opens.
pub(crate) struct KernelTls {
    pub(crate) ctl: Control,
    pub(crate) shared: std::sync::Arc<Shared>,
    /// Application data decrypted in userspace before the kernel took over.
    pub(crate) early_plaintext: Vec<u8>,
}

/// An established session, ready for [`super::io`].
pub(crate) enum Session {
    /// TLS 1.3: in-place record layer.
    Kernel(Kernelized),
    /// TLS 1.3: kernel TLS (Linux).
    Ktls(KernelTls),
    /// TLS 1.2: rustls' unbuffered connection under a lock ([`super::locked`]).
    Locked(Box<LockedState>),
}

pub(crate) struct Established {
    pub(crate) session: Session,
    pub(crate) info: SessionInfo,
}

pub(crate) fn tls_error(e: Error) -> io::Error {
    // Same mapping as tokio-rustls (InvalidData carrying the rustls error).
    io::Error::new(io::ErrorKind::InvalidData, e)
}

pub(crate) fn encode<D>(s: &mut EncodeTlsData<'_, D>, out: &mut Vec<u8>) -> io::Result<()> {
    let at = out.len();
    out.resize(at + 2048, 0);
    loop {
        match s.encode(&mut out[at..]) {
            Ok(n) => {
                out.truncate(at + n);
                return Ok(());
            }
            Err(EncodeError::InsufficientSize(e)) => out.resize(at + e.required_size, 0),
            Err(EncodeError::AlreadyEncoded) => {
                out.truncate(at);
                return Ok(());
            }
        }
    }
}

/// After a handshake error rustls has queued a fatal alert: send it, so the peer reports
/// e.g. "received fatal alert: BadCertificate" exactly as with the classic path.
async fn send_pending_alerts<C: Unbuffered, W: AsyncWrite + Unpin>(
    conn: &mut C,
    wr: &mut W,
    inbuf: &mut [u8],
) {
    let mut out = Vec::new();
    for _ in 0..8 {
        // The same buffer as before the error: rustls may hold ranges into it.
        let status = conn.process(inbuf);
        match status.state {
            Ok(ConnectionState::EncodeTlsData(mut s)) => {
                if encode(&mut s, &mut out).is_err() {
                    break;
                }
            }
            Ok(ConnectionState::TransmitTlsData(t)) => {
                if wr.write_all(&out).await.is_err() || wr.flush().await.is_err() {
                    return;
                }
                out.clear();
                t.done();
            }
            _ => break,
        }
    }
    if !out.is_empty() {
        let _ = wr.write_all(&out).await;
        let _ = wr.flush().await;
    }
}

/// Drives the handshake to completion and converts the connection.
pub(crate) async fn handshake<C, R, W>(
    mut conn: C,
    rd: &mut R,
    wr: &mut W,
    kernel_tls: bool,
) -> io::Result<Established>
where
    C: Unbuffered,
    R: AsyncRead + Unpin + RawTcp,
    W: AsyncWrite + Unpin,
{
    let mut inbuf = vec![0u8; 16 * 1024];
    let mut used = 0usize;
    let mut outbuf: Vec<u8> = Vec::new();
    let mut early = Vec::new();
    loop {
        let mut need_read = false;
        let mut write_traffic = false;
        let mut failed = None;
        let status = conn.process(&mut inbuf[..used]);
        let mut discard = status.discard;
        match status.state {
            Ok(ConnectionState::EncodeTlsData(mut s)) => encode(&mut s, &mut outbuf)?,
            Ok(ConnectionState::TransmitTlsData(t)) => {
                wr.write_all(&outbuf).await?;
                wr.flush().await?;
                outbuf.clear();
                t.done();
            }
            Ok(ConnectionState::BlockedHandshake) => need_read = true,
            Ok(ConnectionState::WriteTraffic(_)) => write_traffic = true,
            Ok(ConnectionState::ReadTraffic(mut r)) => {
                while let Some(rec) = r.next_record() {
                    let rec = rec.map_err(tls_error)?;
                    discard += rec.discard;
                    early.extend_from_slice(rec.payload);
                }
            }
            Ok(ConnectionState::PeerClosed) | Ok(ConnectionState::Closed) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "TLS peer closed the connection during the handshake",
                ));
            }
            Ok(ConnectionState::ReadEarlyData(_)) => {
                return Err(tls_error(Error::General(
                    "unexpected TLS early data".into(),
                )));
            }
            Ok(_) => {
                return Err(tls_error(Error::General(
                    "unsupported TLS connection state".into(),
                )));
            }
            Err(e) => failed = Some(e),
        }
        if let Some(e) = failed {
            if discard > 0 {
                inbuf.copy_within(discard..used, 0);
                used -= discard;
            }
            send_pending_alerts(&mut conn, wr, &mut inbuf[..used]).await;
            return Err(tls_error(e));
        }
        if discard > 0 {
            inbuf.copy_within(discard..used, 0);
            used -= discard;
        }
        if write_traffic {
            // A TLS 1.3 server may send (0.5-RTT) before the client's Finished — and before
            // the client certificate is verified. Only a finished handshake counts.
            if conn.is_handshaking() {
                need_read = true;
            } else {
                break;
            }
        }
        if need_read {
            if used == inbuf.len() {
                if inbuf.len() >= MAX_HANDSHAKE_BUFFER {
                    return Err(tls_error(Error::General(
                        "TLS handshake message too large".into(),
                    )));
                }
                inbuf.resize(inbuf.len() * 2, 0);
            }
            let n = rd.read(&mut inbuf[used..]).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "tls handshake eof",
                ));
            }
            used += n;
        }
    }

    let info = SessionInfo {
        peer_certificates: conn.peer_certificates().map(|c| c.to_vec()),
        cipher_suite: conn.negotiated_cipher_suite(),
        protocol_version: conn.protocol_version(),
        alpn_protocol: conn.alpn_protocol().map(|p| p.to_vec()),
        kernel_tls: false,
    };
    let (suite, version) = match (info.cipher_suite, info.protocol_version) {
        (Some(s), Some(v)) => (s, v),
        _ => return Err(tls_error(Error::HandshakeNotComplete)),
    };
    if version != rustls::ProtocolVersion::TLSv1_3 {
        return Ok(Established {
            session: Session::Locked(Box::new(LockedState::new(
                conn.into_any(),
                inbuf,
                used,
                early,
            ))),
            info,
        });
    }
    let (secrets, kernel) = conn.into_kernel().map_err(tls_error)?;
    let shared = Shared::new(kernel, suite);
    // Kernel TLS: copy the key material before rustls' secrets are consumed below.
    let ktls_keys = match (kernel_tls && ktls::available(), rd.tcp().is_some()) {
        (true, true) => KeyMaterial::of(&secrets.tx.1).zip(KeyMaterial::of(&secrets.rx.1)),
        _ => None,
    };
    let mut reader = RecordReader::new(
        decrypter(suite, secrets.rx.1).map_err(tls_error)?,
        secrets.rx.0,
        shared.clone(),
    );
    inbuf.truncate(used);
    let mut closed = false;
    if let Some((tx_keys, rx_keys)) = ktls_keys {
        let at_boundary =
            to_record_boundary(&mut reader, &shared, &mut inbuf, &mut early, rd).await?;
        closed = at_boundary == Boundary::Closed;
        if at_boundary == Boundary::Ready {
            let tcp = rd.tcp().expect("checked above");
            match enable(tcp, &tx_keys, secrets.tx.0, &rx_keys, reader.seq()) {
                Ok(true) => {
                    return Ok(Established {
                        session: Session::Ktls(KernelTls {
                            ctl: reader.into_control(),
                            shared,
                            early_plaintext: early,
                        }),
                        info: SessionInfo {
                            kernel_tls: true,
                            ..info
                        },
                    });
                }
                Ok(false) => {}
                Err(e) => {
                    shared.fail();
                    return Err(e);
                }
            }
        }
    }
    let writer = RecordWriter::new(
        encrypter(suite, secrets.tx.1).map_err(tls_error)?,
        secrets.tx.0,
        shared.clone(),
    );
    Ok(Established {
        session: Session::Kernel(Kernelized {
            reader,
            writer,
            shared,
            leftover: inbuf,
            early_plaintext: early,
            closed,
        }),
        info,
    })
}

/// Outcome of [`to_record_boundary`].
#[derive(Debug, PartialEq, Eq)]
enum Boundary {
    /// Every received byte is processed: the kernel can take the receive direction.
    Ready,
    /// The peer already rotated its keys (rustls holds only the newest secret inside the
    /// decrypter): stay on the userspace record layer.
    KeysRotated,
    /// close_notify received.
    Closed,
}

/// Opens every record received after the handshake in userspace (their plaintext is
/// appended to `early`) and, if the last one is incomplete, reads exactly its missing bytes,
/// so the kernel starts receiving at a record boundary. `inbuf` keeps any unprocessed rest.
async fn to_record_boundary<R: AsyncRead + Unpin>(
    reader: &mut RecordReader,
    shared: &Shared,
    inbuf: &mut Vec<u8>,
    early: &mut Vec<u8>,
    rd: &mut R,
) -> io::Result<Boundary> {
    let fail = |e: Error| {
        shared.fail();
        tls_error(e)
    };
    let mut pos = 0;
    let outcome = loop {
        match reader.record_len(&inbuf[pos..]).map_err(fail)? {
            Some(len) => {
                match reader.open(&mut inbuf[pos..pos + len], len).map_err(fail)? {
                    Inbound::Data(r) => early.extend_from_slice(&inbuf[pos + r.start..pos + r.end]),
                    Inbound::Control => {}
                    Inbound::Close => {
                        pos += len;
                        break Boundary::Closed;
                    }
                }
                pos += len;
                if reader.epoch() != 0 {
                    break Boundary::KeysRotated;
                }
            }
            None if pos == inbuf.len() => break Boundary::Ready,
            None => {
                // A record arrives whole from a TLS peer; only its tail is still in flight.
                let have = inbuf.len() - pos;
                let need = if have < HEADER {
                    HEADER - have
                } else {
                    HEADER + u16::from_be_bytes([inbuf[pos + 3], inbuf[pos + 4]]) as usize - have
                };
                let at = inbuf.len();
                inbuf.resize(at + need, 0);
                rd.read_exact(&mut inbuf[at..]).await?;
            }
        }
    };
    inbuf.drain(..pos);
    Ok(outcome)
}

/// Hands both directions to the kernel. `Ok(false)`: the kernel refused before any key was
/// installed (the socket is still plain TCP, use the userspace record layer); `Err`: send
/// keys are in the kernel but receive keys were refused, the connection cannot continue.
fn enable(
    tcp: &tokio::net::TcpStream,
    tx: &KeyMaterial,
    tx_seq: u64,
    rx: &KeyMaterial,
    rx_seq: u64,
) -> io::Result<bool> {
    if let Err(e) = ktls::attach(tcp) {
        tracing::debug!(error = %e, "kernel TLS: ULP refused");
        return Ok(false);
    }
    // With the ULP attached but no keys, the socket still behaves as plain TCP.
    if let Err(e) = ktls::set_keys(tcp, Dir::Tx, tx, tx_seq) {
        tracing::debug!(error = %e, "kernel TLS: send keys refused");
        return Ok(false);
    }
    if let Err(e) = ktls::set_keys(tcp, Dir::Rx, rx, rx_seq) {
        tracing::warn!(error = %e, "kernel TLS: receive keys refused after send keys");
        return Err(io::Error::other(format!(
            "kernel TLS: receive keys refused: {e}"
        )));
    }
    ktls::expect_no_pad(tcp);
    Ok(true)
}
