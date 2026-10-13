//! TLS 1.3 record layer of the fast path: records are opened in place inside our receive
//! buffer and sealed straight from the caller's (vectored) plaintext. (TLS 1.2 sessions use
//! rustls' own record layer, see [`super::locked`].)
//!
//! Division of labour (see `docs/design/v0.8-tls.md`):
//! * rustls owns the handshake, the key schedule (initial secrets, every KeyUpdate secret via
//!   [`KernelConnection`]), session tickets, the AEAD and the per-record nonce construction
//!   (its `MessageEncrypter` / `MessageDecrypter`, built by the negotiated suite).
//! * This module owns the record framing (5-byte header), one sequence counter per direction
//!   (starts at the value rustls hands over, +1 per record, 0 after a key update), the
//!   dispatch of post-handshake records (KeyUpdate, NewSessionTicket, alerts) and the
//!   confidentiality limit (a KeyUpdate before the suite's limit, as rustls does).
//!
//! Every rule mirrors what rustls' own record layer does (`record_layer.rs`,
//! `common_state.rs`, `conn.rs` of rustls 0.23); the tests in `fast::tests` run it against
//! rustls' buffered implementation in both directions.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rustls::client::ClientConnectionData;
use rustls::crypto::cipher::{
    InboundOpaqueMessage, MessageDecrypter, MessageEncrypter, OutboundChunks, OutboundPlainMessage,
};
use rustls::kernel::KernelConnection;
use rustls::server::ServerConnectionData;
use rustls::{
    AlertDescription, ConnectionTrafficSecrets, ContentType, Error, HandshakeType, PeerMisbehaved,
    ProtocolVersion, SupportedCipherSuite,
};

/// TLS record header length.
pub(crate) const HEADER: usize = 5;
/// Largest plaintext fragment (RFC 8446 5.1, RFC 5246 6.2.1).
pub(crate) const MAX_FRAGMENT: usize = 16_384;
/// Largest ciphertext body accepted (RFC 8446 5.2: 2^14 + 256).
const MAX_CIPHERTEXT: usize = MAX_FRAGMENT + 256;
/// Largest complete record on the wire.
pub(crate) const MAX_RECORD: usize = HEADER + MAX_CIPHERTEXT;
/// rustls `record_layer::SEQ_SOFT_LIMIT`: stop well before the 64-bit counter wraps.
const SEQ_SOFT_LIMIT: u64 = 0xffff_ffff_ffff_0000;
/// Bound for a post-handshake handshake message split over records (NewSessionTicket is
/// the only legitimately large one; rustls' default `max_handshake_size` is 64 KiB too).
const MAX_POST_HANDSHAKE: usize = 64 * 1024;

/// rustls `TemperCounters::INITIAL_KEY_UPDATE_REQUESTS` (BoringSSL `kMaxKeyUpdates`).
const KEY_UPDATES_BETWEEN_DATA: u8 = 32;

/// rustls' kernel-side connection, client or server flavour.
pub(crate) enum Kernel {
    Client(KernelConnection<ClientConnectionData>),
    Server(KernelConnection<ServerConnectionData>),
}

impl Kernel {
    fn update_tx(&mut self) -> Result<(u64, ConnectionTrafficSecrets), Error> {
        match self {
            Kernel::Client(k) => k.update_tx_secret(),
            Kernel::Server(k) => k.update_tx_secret(),
        }
    }

    fn update_rx(&mut self) -> Result<(u64, ConnectionTrafficSecrets), Error> {
        match self {
            Kernel::Client(k) => k.update_rx_secret(),
            Kernel::Server(k) => k.update_rx_secret(),
        }
    }
}

/// State both halves of a connection share. Touched only on rare events (key updates,
/// tickets, failures), never per record except one relaxed atomic load.
pub(crate) struct Shared {
    kernel: lock::Mutex<Kernel>,
    pub(crate) suite: SupportedCipherSuite,
    /// The peer sent KeyUpdate(update_requested): our next record must be a KeyUpdate.
    key_update_requested: AtomicBool,
    /// A fatal error on either half: the other half refuses further work.
    failed: AtomicBool,
    /// Test counters: key updates applied per direction.
    #[cfg(test)]
    pub(crate) rx_updates: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    pub(crate) tx_updates: std::sync::atomic::AtomicUsize,
}

/// `std::sync::Mutex` wrapper that ignores poisoning (the state stays consistent: every
/// critical section is a single rustls call).
mod lock {
    pub(crate) struct Mutex<T>(std::sync::Mutex<T>);

    impl<T> Mutex<T> {
        pub(crate) fn new(t: T) -> Self {
            Self(std::sync::Mutex::new(t))
        }

        pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, T> {
            self.0.lock().unwrap_or_else(|e| e.into_inner())
        }
    }
}

impl Shared {
    pub(crate) fn new(kernel: Kernel, suite: SupportedCipherSuite) -> Arc<Self> {
        Arc::new(Self {
            kernel: lock::Mutex::new(kernel),
            suite,
            key_update_requested: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            #[cfg(test)]
            rx_updates: Default::default(),
            #[cfg(test)]
            tx_updates: Default::default(),
        })
    }

    pub(crate) fn fail(&self) {
        self.failed.store(true, Ordering::Release);
    }

    pub(crate) fn has_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    /// The peer asked for a KeyUpdate: true once, for the writer that answers it.
    pub(crate) fn take_key_update_request(&self) -> bool {
        self.key_update_requested.load(Ordering::Acquire)
            && self.key_update_requested.swap(false, Ordering::AcqRel)
    }

    /// Next send secrets (rustls derives them), for a KeyUpdate we send.
    pub(crate) fn update_tx(&self) -> Result<(u64, ConnectionTrafficSecrets), Error> {
        let r = self.kernel.lock().update_tx();
        #[cfg(test)]
        if r.is_ok() {
            self.tx_updates.fetch_add(1, Ordering::Relaxed);
        }
        r
    }

    /// Records one send key may seal (the suite's confidentiality limit).
    pub(crate) fn seq_max(&self) -> u64 {
        seq_max(self.suite)
    }
}

/// The confidentiality limit of the suite (rustls `write_seq_max`).
fn seq_max(suite: SupportedCipherSuite) -> u64 {
    let limit = match suite {
        SupportedCipherSuite::Tls13(s) => s.common.confidentiality_limit,
        SupportedCipherSuite::Tls12(s) => s.common.confidentiality_limit,
    };
    // At the limit the writer rotates keys (KeyUpdate), like rustls.
    limit.min(SEQ_SOFT_LIMIT)
}

/// Builds rustls' encrypter for TLS 1.3 traffic secrets.
pub(crate) fn encrypter(
    suite: SupportedCipherSuite,
    secrets: ConnectionTrafficSecrets,
) -> Result<Box<dyn MessageEncrypter>, Error> {
    match (suite, secrets) {
        (
            SupportedCipherSuite::Tls13(s),
            ConnectionTrafficSecrets::Aes128Gcm { key, iv }
            | ConnectionTrafficSecrets::Aes256Gcm { key, iv }
            | ConnectionTrafficSecrets::Chacha20Poly1305 { key, iv },
        ) => Ok(s.aead_alg.encrypter(key, iv)),
        _ => Err(Error::General("fast TLS: not a TLS 1.3 session".into())),
    }
}

/// Builds rustls' decrypter for TLS 1.3 traffic secrets.
pub(crate) fn decrypter(
    suite: SupportedCipherSuite,
    secrets: ConnectionTrafficSecrets,
) -> Result<Box<dyn MessageDecrypter>, Error> {
    match (suite, secrets) {
        (
            SupportedCipherSuite::Tls13(s),
            ConnectionTrafficSecrets::Aes128Gcm { key, iv }
            | ConnectionTrafficSecrets::Aes256Gcm { key, iv }
            | ConnectionTrafficSecrets::Chacha20Poly1305 { key, iv },
        ) => Ok(s.aead_alg.decrypter(key, iv)),
        _ => Err(Error::General("fast TLS: not a TLS 1.3 session".into())),
    }
}

/// What one inbound record amounted to.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Inbound {
    /// Application data at this range of the buffer passed to [`RecordReader::open`]
    /// (decrypted in place; may be empty).
    Data(Range<usize>),
    /// A record with nothing for the application (KeyUpdate, NewSessionTicket, an ignored
    /// warning alert, a fragment of a handshake message).
    Control,
    /// close_notify: the peer will send nothing more.
    Close,
}

/// Receive direction.
pub(crate) struct RecordReader {
    dec: Box<dyn MessageDecrypter>,
    seq: u64,
    ctl: Control,
    /// Receive keys installed so far (+1 per KeyUpdate applied).
    epoch: u32,
}

impl RecordReader {
    pub(crate) fn new(dec: Box<dyn MessageDecrypter>, seq: u64, shared: Arc<Shared>) -> Self {
        Self {
            dec,
            seq,
            ctl: Control::new(shared),
            epoch: 0,
        }
    }

    /// True when the reader holds a fragment of a handshake message (EOF now = truncation).
    pub(crate) fn mid_message(&self) -> bool {
        self.ctl.mid_message()
    }

    /// Sequence number of the next record.
    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }

    /// Number of receive key updates applied (kTLS needs the original keys).
    pub(crate) fn epoch(&self) -> u32 {
        self.epoch
    }

    /// Hands the post-handshake rules over (kTLS takes over the receive direction).
    pub(crate) fn into_control(self) -> Control {
        self.ctl
    }

    /// Length of the complete record at the start of `buf`, `Ok(None)` if more bytes are
    /// needed. Validates the header (rustls deframer rules).
    pub(crate) fn record_len(&self, buf: &[u8]) -> Result<Option<usize>, Error> {
        if buf.len() < HEADER {
            return Ok(None);
        }
        let typ = ContentType::from(buf[0]);
        if matches!(typ, ContentType::Unknown(_)) {
            return Err(Error::InvalidMessage(
                rustls::InvalidMessage::InvalidContentType,
            ));
        }
        // rustls' deframer rule: a 0x03XX record version (the TLS 1.3 AAD fixes 0x0303, so
        // anything else then fails authentication).
        if buf[1] != 0x03 {
            return Err(Error::InvalidMessage(
                rustls::InvalidMessage::UnknownProtocolVersion,
            ));
        }
        let len = u16::from_be_bytes([buf[3], buf[4]]) as usize;
        if len > MAX_CIPHERTEXT {
            return Err(Error::PeerSentOversizedRecord);
        }
        if len == 0 {
            // An AEAD record always carries at least the tag.
            return Err(Error::DecryptError);
        }
        Ok((buf.len() >= HEADER + len).then_some(HEADER + len))
    }

    /// Opens the complete record at the start of `buf` (length from [`Self::record_len`]),
    /// decrypting it in place.
    pub(crate) fn open(&mut self, buf: &mut [u8], len: usize) -> Result<Inbound, Error> {
        if self.seq >= SEQ_SOFT_LIMIT {
            // rustls: `want_close_before_decrypt`.
            return Err(Error::General("TLS read sequence number exhausted".into()));
        }
        let outer = ContentType::from(buf[0]);
        if outer != ContentType::ApplicationData {
            // TLS 1.3 encrypts everything as application_data; a plaintext alert or CCS
            // after the handshake is a protocol violation (rustls: only before the first
            // decryption).
            return Err(Error::InappropriateMessage {
                expect_types: vec![ContentType::ApplicationData],
                got_type: outer,
            });
        }
        let base = buf.as_ptr() as usize;
        let msg = InboundOpaqueMessage::new(outer, ProtocolVersion::TLSv1_2, &mut buf[HEADER..len]);
        let plain = self.dec.decrypt(msg, self.seq)?;
        self.seq += 1;
        if plain.payload.len() > MAX_FRAGMENT {
            return Err(Error::PeerSentOversizedRecord);
        }
        let start = plain.payload.as_ptr() as usize - base;
        let range = start..start + plain.payload.len();
        let typ = plain.typ;
        let (dec, seq, epoch) = (&mut self.dec, &mut self.seq, &mut self.epoch);
        let suite = self.ctl.shared.suite;
        match self
            .ctl
            .record(typ, &buf[range.clone()], &mut |s, secrets| {
                *dec = decrypter(suite, secrets)?;
                *seq = s;
                *epoch += 1;
                Ok(())
            })? {
            Inbound::Data(_) => Ok(Inbound::Data(range)),
            other => Ok(other),
        }
    }
}

/// Post-handshake rules for records that are not application data (alerts, KeyUpdate,
/// NewSessionTicket), shared by the userspace record layer and kernel TLS.
pub(crate) struct Control {
    /// Partial post-handshake handshake message(s).
    hs: Vec<u8>,
    /// rustls `TemperCounters`: warning alerts allowed in total, KeyUpdates allowed
    /// between two application-data records.
    warnings_left: u8,
    key_updates_left: u8,
    pub(crate) shared: Arc<Shared>,
}

impl Control {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self {
            hs: Vec::new(),
            warnings_left: 4,
            key_updates_left: KEY_UPDATES_BETWEEN_DATA,
            shared,
        }
    }

    pub(crate) fn mid_message(&self) -> bool {
        !self.hs.is_empty()
    }

    /// Dispatches one decrypted record of inner type `typ`. `Data(0..0)` means application
    /// data (the payload stays where it is). `rekey_rx` installs new receive keys at the
    /// given sequence number after a KeyUpdate.
    pub(crate) fn record(
        &mut self,
        typ: ContentType,
        payload: &[u8],
        rekey_rx: &mut dyn FnMut(u64, ConnectionTrafficSecrets) -> Result<(), Error>,
    ) -> Result<Inbound, Error> {
        if typ != ContentType::Handshake && !self.hs.is_empty() {
            // RFC 8446 5.1: handshake messages must not be interleaved with other types.
            return Err(PeerMisbehaved::MessageInterleavedWithHandshakeMessage.into());
        }
        match typ {
            ContentType::ApplicationData => {
                self.key_updates_left = KEY_UPDATES_BETWEEN_DATA;
                Ok(Inbound::Data(0..0))
            }
            ContentType::Alert => self.alert(payload),
            ContentType::Handshake => self.handshake(payload, rekey_rx),
            other => Err(Error::InappropriateMessage {
                expect_types: vec![ContentType::ApplicationData, ContentType::Alert],
                got_type: other,
            }),
        }
    }

    /// rustls `CommonState::process_alert`.
    fn alert(&mut self, payload: &[u8]) -> Result<Inbound, Error> {
        let [level, desc] = payload else {
            return Err(Error::InvalidMessage(
                rustls::InvalidMessage::MessageTooShort,
            ));
        };
        let desc = AlertDescription::from(*desc);
        // level 1 = warning, 2 = fatal; anything else is rejected.
        if !matches!(level, 1 | 2) {
            return Err(Error::AlertReceived(desc));
        }
        if desc == AlertDescription::CloseNotify {
            return Ok(Inbound::Close);
        }
        if *level == 1 {
            if self.warnings_left == 0 {
                return Err(PeerMisbehaved::TooManyWarningAlertsReceived.into());
            }
            self.warnings_left -= 1;
            // TLS 1.3 outlaws warnings except user_canceled (rustls: same rule).
            if desc != AlertDescription::UserCanceled {
                return Err(Error::AlertReceived(desc));
            }
            tracing::debug!(?desc, "TLS warning alert ignored");
            return Ok(Inbound::Control);
        }
        Err(Error::AlertReceived(desc))
    }

    /// TLS 1.3 post-handshake messages: KeyUpdate and (client side) NewSessionTicket.
    fn handshake(
        &mut self,
        payload: &[u8],
        rekey_rx: &mut dyn FnMut(u64, ConnectionTrafficSecrets) -> Result<(), Error>,
    ) -> Result<Inbound, Error> {
        if payload.is_empty() {
            return Err(Error::InvalidMessage(
                rustls::InvalidMessage::InvalidEmptyPayload,
            ));
        }
        self.hs.extend_from_slice(payload);
        if self.hs.len() > MAX_POST_HANDSHAKE {
            return Err(Error::InvalidMessage(
                rustls::InvalidMessage::HandshakePayloadTooLarge,
            ));
        }
        let mut pos = 0;
        while self.hs.len() - pos >= 4 {
            let typ = HandshakeType::from(self.hs[pos]);
            let len = u32::from_be_bytes([0, self.hs[pos + 1], self.hs[pos + 2], self.hs[pos + 3]])
                as usize;
            if len > MAX_POST_HANDSHAKE {
                return Err(Error::InvalidMessage(
                    rustls::InvalidMessage::HandshakePayloadTooLarge,
                ));
            }
            if self.hs.len() - pos - 4 < len {
                break;
            }
            let body = pos + 4..pos + 4 + len;
            let at_record_end = body.end == self.hs.len();
            match typ {
                HandshakeType::KeyUpdate => {
                    // RFC 8446 5.1: a KeyUpdate must end its record (rustls checks the
                    // same "aligned handshake" condition).
                    if !at_record_end {
                        return Err(PeerMisbehaved::KeyEpochWithPendingFragment.into());
                    }
                    if self.key_updates_left == 0 {
                        return Err(PeerMisbehaved::TooManyKeyUpdateRequests.into());
                    }
                    self.key_updates_left -= 1;
                    let requested = match &self.hs[body.clone()] {
                        [0] => false,
                        [1] => true,
                        _ => {
                            return Err(Error::InvalidMessage(
                                rustls::InvalidMessage::InvalidKeyUpdate,
                            ));
                        }
                    };
                    let (seq, secrets) = self.shared.kernel.lock().update_rx()?;
                    rekey_rx(seq, secrets)?;
                    #[cfg(test)]
                    self.shared.rx_updates.fetch_add(1, Ordering::Relaxed);
                    if requested {
                        self.shared
                            .key_update_requested
                            .store(true, Ordering::Release);
                    }
                }
                HandshakeType::NewSessionTicket => match &mut *self.shared.kernel.lock() {
                    Kernel::Client(k) => k.handle_new_session_ticket(&self.hs[body.clone()])?,
                    Kernel::Server(_) => {
                        return Err(Error::InappropriateHandshakeMessage {
                            expect_types: vec![HandshakeType::KeyUpdate],
                            got_type: typ,
                        });
                    }
                },
                other => {
                    return Err(Error::InappropriateHandshakeMessage {
                        expect_types: vec![
                            HandshakeType::KeyUpdate,
                            HandshakeType::NewSessionTicket,
                        ],
                        got_type: other,
                    });
                }
            }
            pos = body.end;
        }
        self.hs.drain(..pos);
        Ok(Inbound::Control)
    }
}

/// `KeyUpdate(update_not_requested)` (RFC 8446 4.6.3): type 24, length 1, value 0.
pub(crate) const KEY_UPDATE_NOT_REQUESTED: [u8; 5] = [24, 0, 0, 1, 0];
/// `KeyUpdate(update_requested)`: test hook only (rustls never sends it either).
#[cfg(test)]
pub(crate) const KEY_UPDATE_REQUESTED: [u8; 5] = [24, 0, 0, 1, 1];
/// close_notify alert: level warning (1), description 0.
pub(crate) const CLOSE_NOTIFY: [u8; 2] = [1, 0];

/// Send direction.
pub(crate) struct RecordWriter {
    enc: Box<dyn MessageEncrypter>,
    seq: u64,
    seq_max: u64,
    /// Rotate before the first application record: the encrypter is fresh at a sequence
    /// number other than 0 (see [`RecordWriter::new`]).
    rotate_first: bool,
    shared: Arc<Shared>,
    #[cfg(test)]
    pub(crate) request_peer_update: bool,
}

impl RecordWriter {
    /// `seq` is rustls' write sequence at the hand-over. rustls seals AES-GCM with aws-lc's
    /// `TlsRecordSealingKey`, which treats the first nonce a key object sees as sequence 0
    /// and then enforces strictly increasing sequence numbers (a defence against nonce
    /// reuse). A server hands over after its NewSessionTicket records (seq 2 with rustls'
    /// defaults), so its first record from the fresh key object is a KeyUpdate (valid at
    /// that object's first nonce); every later key starts at 0, as in rustls itself.
    pub(crate) fn new(enc: Box<dyn MessageEncrypter>, seq: u64, shared: Arc<Shared>) -> Self {
        let seq_max = seq_max(shared.suite);
        Self {
            enc,
            seq,
            seq_max,
            rotate_first: seq != 0,
            shared,
            #[cfg(test)]
            request_peer_update: false,
        }
    }

    /// Encrypts one record of `chunks` (total ≤ [`MAX_FRAGMENT`]) and returns it encoded
    /// (header + ciphertext), ready for the socket.
    fn seal_one(&mut self, typ: ContentType, chunks: &[&[u8]]) -> Result<Vec<u8>, Error> {
        if self.seq >= SEQ_SOFT_LIMIT {
            return Err(Error::EncryptError);
        }
        let payload = if chunks.is_empty() {
            OutboundChunks::new_empty()
        } else {
            OutboundChunks::new(chunks)
        };
        let msg = OutboundPlainMessage {
            typ,
            version: ProtocolVersion::TLSv1_2,
            payload,
        };
        let sealed = self.enc.encrypt(msg, self.seq)?;
        self.seq += 1;
        Ok(sealed.encode())
    }

    /// Runs before every application-data record: answers a requested key update and keeps
    /// the record count under the suite's confidentiality limit (rustls
    /// `pre_encrypt_action` + `maybe_refresh_traffic_keys`).
    fn before_app_data(&mut self, out: &mut impl FnMut(Vec<u8>)) -> Result<(), Error> {
        let requested = self.shared.take_key_update_request();
        #[cfg(test)]
        let ask_peer = std::mem::take(&mut self.request_peer_update);
        #[cfg(not(test))]
        let ask_peer = false;
        let rotate_first = std::mem::take(&mut self.rotate_first);
        if !(requested || ask_peer || rotate_first || self.seq >= self.seq_max) {
            return Ok(());
        }
        #[cfg(test)]
        let msg: &[u8] = if ask_peer {
            &KEY_UPDATE_REQUESTED
        } else {
            &KEY_UPDATE_NOT_REQUESTED
        };
        #[cfg(not(test))]
        let msg: &[u8] = &KEY_UPDATE_NOT_REQUESTED;
        // The KeyUpdate itself goes out under the old key, then we switch.
        out(self.seal_one(ContentType::Handshake, &[msg])?);
        let (seq, secrets) = self.shared.update_tx()?;
        self.enc = encrypter(self.shared.suite, secrets)?;
        self.seq = seq;
        Ok(())
    }

    /// Encrypts application data given as a list of slices into records of at most
    /// [`MAX_FRAGMENT`] bytes, passing each encoded record to `out`. Encrypts exactly
    /// `limit` bytes (or everything, if less).
    pub(crate) fn seal_app(
        &mut self,
        bufs: &[&[u8]],
        limit: usize,
        mut out: impl FnMut(Vec<u8>),
    ) -> Result<usize, Error> {
        let mut parts: Vec<&[u8]> = Vec::with_capacity(bufs.len().min(64));
        let mut taken = 0;
        let mut in_record = 0;
        let mut i = 0;
        let mut off = 0;
        while taken < limit && i < bufs.len() {
            let b = &bufs[i][off..];
            if b.is_empty() {
                i += 1;
                off = 0;
                continue;
            }
            let n = b.len().min(MAX_FRAGMENT - in_record).min(limit - taken);
            parts.push(&b[..n]);
            in_record += n;
            taken += n;
            off += n;
            if off == bufs[i].len() {
                i += 1;
                off = 0;
            }
            if in_record == MAX_FRAGMENT || taken == limit || i == bufs.len() {
                self.before_app_data(&mut out)?;
                out(self.seal_one(ContentType::ApplicationData, &parts)?);
                parts.clear();
                in_record = 0;
            }
        }
        if !parts.is_empty() {
            self.before_app_data(&mut out)?;
            out(self.seal_one(ContentType::ApplicationData, &parts)?);
        }
        Ok(taken)
    }

    /// The encoded close_notify alert.
    pub(crate) fn close_notify(&mut self) -> Result<Vec<u8>, Error> {
        self.seal_one(ContentType::Alert, &[&CLOSE_NOTIFY])
    }
}
