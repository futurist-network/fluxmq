//! TLS 1.2 data path of the fast implementation: rustls' unbuffered connection does the
//! whole record layer (as in the classic path), both halves share it under a mutex.
//!
//! Why TLS 1.2 cannot use the in-place record layer: rustls seals AES-GCM records with
//! aws-lc's `TlsRecordSealingKey`, which takes the first nonce it sees as sequence 0 and then
//! enforces strictly increasing sequence numbers. A TLS 1.2 connection hands over at write
//! sequence 1 (the Finished record used 0) and TLS 1.2 has no KeyUpdate to start a fresh key
//! at 0. (TLS 1.3 restarts at 0 with a KeyUpdate, see `record::RecordWriter`.) TLS 1.2 peers
//! are rare for FluxMQ (every FluxMQ client and node negotiates TLS 1.3), so this path is
//! about correctness, not speed: one extra copy per direction compared to tokio-rustls'.

use std::collections::VecDeque;
use std::task::Waker;

use rustls::Error;
use rustls::client::UnbufferedClientConnection;
use rustls::server::UnbufferedServerConnection;
use rustls::unbuffered::{ConnectionState, EncryptError, InsufficientSizeError};

use super::handshake::{Unbuffered, encode};
use super::record::MAX_FRAGMENT;

/// Upper bound of the shared receive buffer (a handshake-sized message plus slack).
pub(crate) const MAX_INBUF: usize = 1 << 20;

pub(crate) enum AnyConn {
    Client(UnbufferedClientConnection),
    Server(UnbufferedServerConnection),
}

/// Everything both halves share (behind one mutex).
pub(crate) struct LockedState {
    pub(crate) conn: AnyConn,
    /// Received ciphertext not yet consumed by rustls.
    pub(crate) inbuf: Vec<u8>,
    pub(crate) used: usize,
    /// Decrypted application data for the read half.
    pub(crate) plain: Vec<u8>,
    pub(crate) plain_pos: usize,
    /// Records for the write half to send (handshake-layer replies produced while reading,
    /// e.g. alerts, land here too).
    pub(crate) out: VecDeque<Vec<u8>>,
    pub(crate) peer_closed: bool,
    pub(crate) error: Option<Error>,
    /// Read half waiting for the socket while the write half may produce plaintext.
    pub(crate) reader_waker: Option<Waker>,
}

/// What a pump should do once rustls can take application data.
pub(crate) struct Work<'a> {
    pub(crate) data: Option<&'a [&'a [u8]]>,
    pub(crate) limit: usize,
    pub(crate) close: bool,
}

impl LockedState {
    pub(crate) fn new(conn: AnyConn, inbuf: Vec<u8>, used: usize, early: Vec<u8>) -> Self {
        Self {
            conn,
            inbuf,
            used,
            plain: early,
            plain_pos: 0,
            out: VecDeque::new(),
            peer_closed: false,
            error: None,
            reader_waker: None,
        }
    }

    pub(crate) fn plain_pending(&self) -> usize {
        self.plain.len() - self.plain_pos
    }

    /// Runs rustls until it waits for input, processing buffered ciphertext and (when
    /// `work` asks) encrypting application data. Returns the plaintext bytes encrypted.
    pub(crate) fn pump(&mut self, work: Work<'_>) -> Result<usize, Error> {
        if let Some(e) = &self.error {
            return Err(e.clone());
        }
        let r = match &mut self.conn {
            AnyConn::Client(c) => pump_conn(
                c,
                &mut self.inbuf,
                &mut self.used,
                &mut self.plain,
                &mut self.out,
                &mut self.peer_closed,
                work,
            ),
            AnyConn::Server(c) => pump_conn(
                c,
                &mut self.inbuf,
                &mut self.used,
                &mut self.plain,
                &mut self.out,
                &mut self.peer_closed,
                work,
            ),
        };
        if self.plain_pos == self.plain.len() && self.plain_pos > 0 {
            self.plain.clear();
            self.plain_pos = 0;
        }
        if let Err(e) = &r {
            self.error = Some(e.clone());
        }
        if (self.plain_pending() > 0 || self.peer_closed || self.error.is_some())
            && let Some(w) = self.reader_waker.take()
        {
            w.wake();
        }
        r
    }
}

fn encrypt_into<D>(
    wt: &mut rustls::unbuffered::WriteTraffic<'_, D>,
    data: &[u8],
    out: &mut VecDeque<Vec<u8>>,
) -> Result<(), Error> {
    // Payload + per-record overhead (header, explicit nonce, tag) with room to spare.
    let mut v = vec![0u8; data.len() + (data.len() / MAX_FRAGMENT + 1) * 64];
    loop {
        match wt.encrypt(data, &mut v) {
            Ok(n) => {
                v.truncate(n);
                out.push_back(v);
                return Ok(());
            }
            Err(EncryptError::InsufficientSize(InsufficientSizeError { required_size })) => {
                v.resize(required_size, 0)
            }
            Err(EncryptError::EncryptExhausted) => return Err(Error::EncryptError),
        }
    }
}

fn pump_conn<C: Unbuffered>(
    conn: &mut C,
    inbuf: &mut [u8],
    used: &mut usize,
    plain: &mut Vec<u8>,
    out: &mut VecDeque<Vec<u8>>,
    peer_closed: &mut bool,
    mut work: Work<'_>,
) -> Result<usize, Error> {
    let mut encrypted = 0;
    // Keys exhausted (TLS 1.2 at the confidentiality limit): rustls queued a close_notify;
    // emit it before reporting the error.
    let mut exhausted = None;
    loop {
        let mut stop = false;
        let mut continue_after = false;
        let status = conn.process(&mut inbuf[..*used]);
        let mut discard = status.discard;
        match status.state? {
            ConnectionState::ReadTraffic(mut r) => {
                while let Some(rec) = r.next_record() {
                    let rec = rec?;
                    discard += rec.discard;
                    plain.extend_from_slice(rec.payload);
                }
            }
            ConnectionState::EncodeTlsData(mut s) => {
                let mut v = Vec::new();
                encode(&mut s, &mut v).map_err(|e| Error::General(e.to_string()))?;
                out.push_back(v);
            }
            ConnectionState::TransmitTlsData(t) => t.done(),
            ConnectionState::PeerClosed => *peer_closed = true,
            ConnectionState::Closed => {
                *peer_closed = true;
                stop = true;
            }
            ConnectionState::WriteTraffic(mut wt) => {
                if let Some(bufs) = work.data.take().filter(|_| exhausted.is_none()) {
                    // Coalesce the slices into ≤ 64 KiB pieces (one copy; slow path).
                    let mut piece = Vec::with_capacity(work.limit.min(64 * 1024));
                    'outer: for b in bufs {
                        let mut b: &[u8] = b;
                        while !b.is_empty() {
                            if encrypted + piece.len() == work.limit {
                                break 'outer;
                            }
                            let room =
                                (64 * 1024 - piece.len()).min(work.limit - encrypted - piece.len());
                            let n = b.len().min(room);
                            piece.extend_from_slice(&b[..n]);
                            b = &b[n..];
                            if piece.len() == 64 * 1024 {
                                if let Err(e) = encrypt_into(&mut wt, &piece, out) {
                                    exhausted = Some(e);
                                    break 'outer;
                                }
                                encrypted += piece.len();
                                piece.clear();
                            }
                        }
                    }
                    if !piece.is_empty() && exhausted.is_none() {
                        match encrypt_into(&mut wt, &piece, out) {
                            Ok(()) => encrypted += piece.len(),
                            Err(e) => exhausted = Some(e),
                        }
                    }
                    if exhausted.is_some() {
                        // Loop once more: rustls emits the queued close_notify.
                        continue_after = true;
                    }
                }
                if work.close && !continue_after {
                    work.close = false;
                    let mut v = vec![0u8; 64];
                    let n = wt
                        .queue_close_notify(&mut v)
                        .map_err(|e| Error::General(e.to_string()))?;
                    v.truncate(n);
                    out.push_back(v);
                }
                stop = !continue_after;
            }
            ConnectionState::BlockedHandshake => stop = true,
            _ => {
                return Err(Error::General(
                    "unexpected TLS state after the handshake".into(),
                ));
            }
        }
        if discard > 0 {
            inbuf.copy_within(discard..*used, 0);
            *used -= discard;
        }
        if stop {
            return match exhausted {
                Some(e) => Err(e),
                None => Ok(encrypted),
            };
        }
    }
}
