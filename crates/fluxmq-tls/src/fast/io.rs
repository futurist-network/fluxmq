//! Async halves of a fast-path TLS connection.
//!
//! TLS 1.3 (kernel mode): the halves are independent (separate socket halves, separate
//! crypto state), so reading never waits for writing and vice versa — unlike
//! `tokio::io::split` over one `TlsStream` (one mutex around both directions, held during
//! encryption and decryption).
//!
//! TLS 1.2 (locked mode, [`super::locked`]): rustls' unbuffered connection behind a mutex
//! held only while rustls runs, never during socket I/O.

use std::collections::VecDeque;
use std::io::{self, IoSlice};
use std::ops::Range;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, ready};

use rustls::ContentType;
use tokio::io::{AsyncRead, AsyncWrite, Interest, ReadBuf};

use super::handshake::{Established, KernelTls, Kernelized, Session, tls_error};
use super::ktls::{self, Dir, KeyMaterial, RawTcp};
use super::locked::{LockedState, MAX_INBUF, Work};
use super::record::{
    CLOSE_NOTIFY, Control, Inbound, KEY_UPDATE_NOT_REQUESTED, MAX_FRAGMENT, MAX_RECORD,
    RecordReader, RecordWriter, Shared,
};

/// Receive buffer of a busy connection: up to 64 KiB per `recv` on top of one maximal record
/// (a partial record is moved to the front before the next read).
const READ_BUF: usize = 64 * 1024 + MAX_RECORD;
/// Initial receive buffer: one maximal record plus a little (≈ rustls' own deframer buffer),
/// so idle connections stay small; it grows to [`READ_BUF`] once a `recv` fills it.
const READ_BUF_SMALL: usize = MAX_RECORD + 4096;
/// Ciphertext queued in a write half before `poll_write` applies backpressure.
const WRITE_LIMIT: usize = 256 * 1024;
/// iovecs per `writev`.
const MAX_IOV: usize = 64;

type Locked = Arc<Mutex<LockedState>>;

fn lock(l: &Locked) -> MutexGuard<'_, LockedState> {
    l.lock().unwrap_or_else(|e| e.into_inner())
}

pub(crate) fn split<R, W>(est: Established, rd: R, wr: W) -> (TlsReadHalf<R>, TlsWriteHalf<W>) {
    let (rmode, wmode) = match est.session {
        Session::Kernel(Kernelized {
            reader,
            writer,
            shared,
            leftover,
            early_plaintext,
            closed,
        }) => {
            let mut buf = vec![0u8; READ_BUF_SMALL.max(leftover.len() + MAX_RECORD)];
            buf[..leftover.len()].copy_from_slice(&leftover);
            let r = KernelRead {
                rx: reader,
                buf,
                start: 0,
                end: leftover.len(),
                plain: 0..0,
                early: early_plaintext,
                early_pos: 0,
                closed,
                error: None,
                grow: false,
                shared: shared.clone(),
            };
            (
                ReadMode::Kernel(Box::new(r)),
                WriteMode::Kernel { tx: writer, shared },
            )
        }
        Session::Ktls(KernelTls {
            ctl,
            shared,
            early_plaintext,
        }) => {
            let seq_max = shared.seq_max();
            let r = KtlsRead {
                ctl,
                early: early_plaintext,
                early_pos: 0,
                stash: Vec::new(),
                stashed: 0..0,
                closed: false,
                error: None,
                shared: shared.clone(),
            };
            (
                ReadMode::Ktls(Box::new(r)),
                WriteMode::Ktls(KtlsWrite {
                    shared,
                    records: 0,
                    seq_max,
                    rekey_pending: false,
                    #[cfg(test)]
                    ask_peer: false,
                }),
            )
        }
        Session::Locked(state) => {
            let l = Arc::new(Mutex::new(*state));
            (ReadMode::Locked(l.clone()), WriteMode::Locked(l))
        }
    };
    let read = TlsReadHalf {
        io: rd,
        mode: rmode,
    };
    let write = TlsWriteHalf {
        io: wr,
        mode: wmode,
        queue: VecDeque::new(),
        head: 0,
        queued: 0,
        close_queued: false,
    };
    (read, write)
}

/// Read half of a fast-path TLS connection.
pub struct TlsReadHalf<R> {
    io: R,
    mode: ReadMode,
}

enum ReadMode {
    Kernel(Box<KernelRead>),
    Ktls(Box<KtlsRead>),
    Locked(Locked),
}

/// TLS 1.3 receive state: records are decrypted in place in `buf` and copied once, into the
/// caller's buffer.
struct KernelRead {
    rx: RecordReader,
    buf: Vec<u8>,
    /// Unprocessed ciphertext.
    start: usize,
    end: usize,
    /// Decrypted application data not yet returned.
    plain: Range<usize>,
    /// Application data rustls decrypted during the handshake (returned first).
    early: Vec<u8>,
    early_pos: usize,
    /// close_notify received: EOF.
    closed: bool,
    /// An error found after some data was already returned in the same call.
    error: Option<io::Error>,
    /// The last `recv` filled all free space: switch to the large buffer.
    grow: bool,
    shared: Arc<Shared>,
}

fn eof_without_close_notify(mid_record: bool) -> io::Error {
    // Possible truncation attack, never a clean end (same error kind as tokio-rustls).
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        if mid_record {
            "peer closed connection in the middle of a TLS record"
        } else {
            "peer closed connection without sending TLS close_notify"
        },
    )
}

impl<R> TlsReadHalf<R> {
    /// The underlying transport.
    pub fn get_ref(&self) -> &R {
        &self.io
    }

    #[cfg(all(test, feature = "server"))]
    pub(crate) fn shared(&self) -> &Arc<Shared> {
        match &self.mode {
            ReadMode::Kernel(k) => &k.shared,
            ReadMode::Ktls(k) => &k.shared,
            ReadMode::Locked(_) => panic!("TLS 1.2 session"),
        }
    }

    /// Records are sealed and opened by the kernel.
    pub fn is_kernel_tls(&self) -> bool {
        matches!(self.mode, ReadMode::Ktls(_))
    }
}

impl KernelRead {
    fn fail(&mut self, e: rustls::Error) -> io::Error {
        self.shared.fail();
        tls_error(e)
    }

    fn poll_read<R: AsyncRead + Unpin>(
        &mut self,
        io: &mut R,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(e) = self.error.take() {
            return Poll::Ready(Err(e));
        }
        if self.early_pos < self.early.len() {
            let n = (self.early.len() - self.early_pos).min(out.remaining());
            out.put_slice(&self.early[self.early_pos..self.early_pos + n]);
            self.early_pos += n;
            if self.early_pos == self.early.len() {
                self.early = Vec::new();
                self.early_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }
        let mut wrote = false;
        loop {
            if !self.plain.is_empty() {
                let n = self.plain.len().min(out.remaining());
                out.put_slice(&self.buf[self.plain.start..self.plain.start + n]);
                self.plain.start += n;
                wrote = true;
                if out.remaining() == 0 {
                    return Poll::Ready(Ok(()));
                }
                continue;
            }
            if self.closed {
                return Poll::Ready(Ok(()));
            }
            // A complete record buffered?
            let step = match self.rx.record_len(&self.buf[self.start..self.end]) {
                Ok(Some(len)) => {
                    let at = self.start;
                    let r = self.rx.open(&mut self.buf[at..at + len], len);
                    if r.is_ok() {
                        self.start += len;
                        if self.start == self.end {
                            self.start = 0;
                            self.end = 0;
                        }
                    }
                    r.map(|inbound| Some((at, inbound)))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            };
            match step {
                Ok(Some((at, Inbound::Data(r)))) => {
                    self.plain = at + r.start..at + r.end;
                    continue;
                }
                Ok(Some((_, Inbound::Control))) => continue,
                Ok(Some((_, Inbound::Close))) => {
                    self.closed = true;
                    continue;
                }
                Ok(None) => {}
                Err(e) => {
                    let e = self.fail(e);
                    if wrote {
                        self.error = Some(e);
                        return Poll::Ready(Ok(()));
                    }
                    return Poll::Ready(Err(e));
                }
            }
            if wrote {
                // Hand out what we have rather than wait for the socket.
                return Poll::Ready(Ok(()));
            }
            if self.grow {
                self.grow = false;
                self.buf.resize(READ_BUF, 0);
            }
            if self.buf.len() - self.end < MAX_RECORD && self.start > 0 {
                // Move the partial record to the front (at most one record's worth).
                self.buf.copy_within(self.start..self.end, 0);
                self.end -= self.start;
                self.start = 0;
            }
            let free = self.buf.len() - self.end;
            let mut rb = ReadBuf::new(&mut self.buf[self.end..]);
            ready!(Pin::new(&mut *io).poll_read(cx, &mut rb))?;
            let n = rb.filled().len();
            if n == free && self.buf.len() < READ_BUF {
                self.grow = true;
            }
            if n == 0 {
                return Poll::Ready(Err(eof_without_close_notify(
                    self.start != self.end || self.rx.mid_message(),
                )));
            }
            self.end += n;
        }
    }
}

/// TLS 1.3 receive state with kernel TLS: the kernel decrypts straight into the caller's
/// buffer; only non-data records (and reads into small buffers) pass through `stash`.
struct KtlsRead {
    ctl: Control,
    /// Application data decrypted in userspace before the hand-over (returned first).
    early: Vec<u8>,
    early_pos: usize,
    /// Receive buffer for calls with less than one record of room.
    stash: Vec<u8>,
    /// Application data in `stash` not yet returned.
    stashed: Range<usize>,
    closed: bool,
    error: Option<io::Error>,
    shared: Arc<Shared>,
}

/// Maps a kernel TLS receive error (`EBADMSG`: authentication failed, `EMSGSIZE`: oversized
/// record, `EINVAL`/`EIO`: malformed) to the error kinds of the userspace record layer.
fn ktls_error(e: io::Error) -> io::Error {
    match e.raw_os_error() {
        Some(libc::EBADMSG | libc::EMSGSIZE | libc::EINVAL | libc::EIO | libc::EKEYEXPIRED) => {
            io::Error::new(io::ErrorKind::InvalidData, format!("kernel TLS: {e}"))
        }
        _ => e,
    }
}

impl KtlsRead {
    fn poll_read<R: RawTcp>(
        &mut self,
        io: &R,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(e) = self.error.take() {
            return Poll::Ready(Err(e));
        }
        if out.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.early_pos < self.early.len() {
            let n = (self.early.len() - self.early_pos).min(out.remaining());
            out.put_slice(&self.early[self.early_pos..self.early_pos + n]);
            self.early_pos += n;
            if self.early_pos == self.early.len() {
                self.early = Vec::new();
                self.early_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }
        if !self.stashed.is_empty() {
            let n = self.stashed.len().min(out.remaining());
            out.put_slice(&self.stash[self.stashed.start..self.stashed.start + n]);
            self.stashed.start += n;
            return Poll::Ready(Ok(()));
        }
        if self.closed {
            return Poll::Ready(Ok(()));
        }
        let tcp = io.tcp().expect("kernel TLS runs on a TCP socket");
        // One record always fits: a control record is never split over two calls.
        let direct = out.remaining() >= MAX_FRAGMENT;
        if !direct && self.stash.len() < MAX_FRAGMENT {
            self.stash.resize(MAX_FRAGMENT, 0);
        }
        loop {
            ready!(tcp.poll_read_ready(cx))?;
            let (ptr, len) = if direct {
                // SAFETY: the kernel only writes initialized bytes into this range, and
                // only `n` of them are marked initialized below.
                let dst = unsafe { out.unfilled_mut() };
                (dst.as_mut_ptr() as *mut u8, dst.len())
            } else {
                (self.stash.as_mut_ptr(), self.stash.len())
            };
            let (n, typ) = match tcp.try_io(Interest::READABLE, || ktls::recv_record(tcp, ptr, len))
            {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(e) => {
                    let e = ktls_error(e);
                    if e.kind() == io::ErrorKind::InvalidData {
                        self.shared.fail();
                    }
                    return Poll::Ready(Err(e));
                }
            };
            let typ = ContentType::from(typ);
            if typ == ContentType::ApplicationData {
                if n == 0 {
                    return Poll::Ready(Err(eof_without_close_notify(self.ctl.mid_message())));
                }
                if let Err(e) = self.ctl.record(typ, &[], &mut |_, _| Ok(())) {
                    self.shared.fail();
                    return Poll::Ready(Err(tls_error(e)));
                }
                if direct {
                    // SAFETY: the kernel wrote `n` bytes at the start of the unfilled part.
                    unsafe { out.assume_init(n) };
                    out.advance(n);
                } else {
                    let k = n.min(out.remaining());
                    out.put_slice(&self.stash[..k]);
                    self.stashed = k..n;
                }
                return Poll::Ready(Ok(()));
            }
            // A post-handshake record: small and rare, so copy it out of the way.
            // SAFETY: the kernel initialized `n` bytes at `ptr`.
            let payload = unsafe { std::slice::from_raw_parts(ptr, n) }.to_vec();
            let r = self.ctl.record(typ, &payload, &mut |seq, secrets| {
                let keys = KeyMaterial::of(&secrets).ok_or_else(|| {
                    rustls::Error::General("kernel TLS: unsupported cipher".into())
                })?;
                ktls::set_keys(tcp, Dir::Rx, &keys, seq).map_err(|e| {
                    rustls::Error::General(format!("kernel TLS: receive key update: {e}"))
                })
            });
            match r {
                Ok(Inbound::Close) => {
                    self.closed = true;
                    return Poll::Ready(Ok(()));
                }
                Ok(_) => continue,
                Err(e) => {
                    self.shared.fail();
                    return Poll::Ready(Err(tls_error(e)));
                }
            }
        }
    }
}

fn poll_read_locked<R: AsyncRead + Unpin>(
    l: &Locked,
    io: &mut R,
    cx: &mut Context<'_>,
    out: &mut ReadBuf<'_>,
) -> Poll<io::Result<()>> {
    let mut st = lock(l);
    loop {
        let pending = st.plain_pending();
        if pending > 0 {
            let n = pending.min(out.remaining());
            let p = st.plain_pos;
            out.put_slice(&st.plain[p..p + n]);
            st.plain_pos += n;
            return Poll::Ready(Ok(()));
        }
        if st.peer_closed {
            return Poll::Ready(Ok(()));
        }
        st.pump(Work {
            data: None,
            limit: 0,
            close: false,
        })
        .map_err(tls_error)?;
        if st.plain_pending() > 0 || st.peer_closed {
            continue;
        }
        if st.used == st.inbuf.len() {
            if st.inbuf.len() >= MAX_INBUF {
                return Poll::Ready(Err(tls_error(rustls::Error::General(
                    "TLS message too large".into(),
                ))));
            }
            let len = (st.inbuf.len() * 2).max(READ_BUF);
            st.inbuf.resize(len, 0);
        }
        let used = st.used;
        let mut rb = ReadBuf::new(&mut st.inbuf[used..]);
        match Pin::new(&mut *io).poll_read(cx, &mut rb) {
            Poll::Pending => {
                // The write half may decrypt buffered records meanwhile: let it wake us.
                st.reader_waker = Some(cx.waker().clone());
                return Poll::Pending;
            }
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }
        let n = rb.filled().len();
        if n == 0 {
            return Poll::Ready(Err(eof_without_close_notify(st.used > 0)));
        }
        st.used += n;
    }
}

impl<R: AsyncRead + Unpin + RawTcp> AsyncRead for TlsReadHalf<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        match &mut me.mode {
            ReadMode::Kernel(k) => k.poll_read(&mut me.io, cx, out),
            ReadMode::Ktls(k) => k.poll_read(&me.io, cx, out),
            ReadMode::Locked(l) => poll_read_locked(l, &mut me.io, cx, out),
        }
    }
}

/// Write half of a fast-path TLS connection: plaintext is encrypted straight from the
/// caller's slices (TLS 1.3: one copy, into the record the AEAD seals in place) into
/// records queued for vectored writes.
pub struct TlsWriteHalf<W> {
    io: W,
    mode: WriteMode,
    queue: VecDeque<Vec<u8>>,
    /// Bytes of the front record already written.
    head: usize,
    /// Bytes queued (including the written part of the front record).
    queued: usize,
    close_queued: bool,
}

enum WriteMode {
    Kernel {
        tx: RecordWriter,
        shared: Arc<Shared>,
    },
    Ktls(KtlsWrite),
    Locked(Locked),
}

/// The first `cap` bytes of `bufs`.
fn cap_slices<'b>(bufs: &'b [IoSlice<'_>], cap: usize) -> Vec<IoSlice<'b>> {
    let mut out = Vec::with_capacity(bufs.len().min(MAX_IOV));
    let mut left = cap;
    for b in bufs {
        if left == 0 {
            break;
        }
        let b: &'b [u8] = b;
        let n = b.len().min(left);
        out.push(IoSlice::new(&b[..n]));
        left -= n;
    }
    out
}

/// Send state with kernel TLS: the kernel seals records; we only rotate keys (a KeyUpdate
/// the peer asked for, or before the suite's confidentiality limit).
struct KtlsWrite {
    shared: Arc<Shared>,
    /// Upper bound of the records sealed under the current key: a `sendmsg` of `n` bytes
    /// ends at most `n / 2^14 + 1` records (the first may continue an open one).
    records: u64,
    seq_max: u64,
    /// A KeyUpdate is due (kept across `Pending`).
    rekey_pending: bool,
    #[cfg(test)]
    ask_peer: bool,
}

impl<W> TlsWriteHalf<W> {
    /// The underlying transport.
    pub fn get_ref(&self) -> &W {
        &self.io
    }

    /// Test hook: the next record is preceded by KeyUpdate(update_requested).
    #[cfg(all(test, feature = "server"))]
    pub(crate) fn request_key_update(&mut self) {
        match &mut self.mode {
            WriteMode::Kernel { tx, .. } => tx.request_peer_update = true,
            WriteMode::Ktls(k) => k.ask_peer = true,
            WriteMode::Locked(_) => panic!("TLS 1.2 session"),
        }
    }

    /// Records are sealed by the kernel: plaintext written to the socket (including
    /// `sendfile`) is encrypted on the way out.
    pub fn is_kernel_tls(&self) -> bool {
        matches!(self.mode, WriteMode::Ktls(_))
    }

    fn push(&mut self, rec: Vec<u8>) {
        self.queued += rec.len();
        self.queue.push_back(rec);
    }

    /// Locked mode: move records rustls produced (ours, or alerts from the read side).
    fn take_locked_output(&mut self, st: &mut LockedState) {
        while let Some(rec) = st.out.pop_front() {
            self.push(rec);
        }
    }
}

impl<W: AsyncWrite + Unpin + RawTcp> TlsWriteHalf<W> {
    /// Kernel TLS: sends one control record (content type `typ`) once the socket is
    /// writable.
    fn poll_send_control(
        &mut self,
        cx: &mut Context<'_>,
        typ: u8,
        data: &[u8],
    ) -> Poll<io::Result<()>> {
        let tcp = self.io.tcp().expect("kernel TLS runs on a TCP socket");
        loop {
            ready!(tcp.poll_write_ready(cx))?;
            match tcp.try_io(Interest::WRITABLE, || ktls::send_record(tcp, typ, data)) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                r => return Poll::Ready(r),
            }
        }
    }

    /// Kernel TLS: sends a due KeyUpdate and installs the next send key.
    fn poll_ktls_rekey(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let WriteMode::Ktls(k) = &mut self.mode else {
            return Poll::Ready(Ok(()));
        };
        #[cfg(test)]
        let ask_peer = std::mem::take(&mut k.ask_peer);
        #[cfg(not(test))]
        let ask_peer = false;
        if k.shared.take_key_update_request() || k.records >= k.seq_max || ask_peer {
            k.rekey_pending = true;
        }
        // One more call may end two records (it may continue an open one).
        if k.records.saturating_add(2) > k.seq_max {
            k.rekey_pending = true;
        }
        if !k.rekey_pending {
            return Poll::Ready(Ok(()));
        }
        #[cfg(test)]
        let msg: &[u8] = if ask_peer {
            &super::record::KEY_UPDATE_REQUESTED
        } else {
            &KEY_UPDATE_NOT_REQUESTED
        };
        #[cfg(not(test))]
        let msg: &[u8] = &KEY_UPDATE_NOT_REQUESTED;
        let shared = k.shared.clone();
        let fail = |e: io::Error| {
            shared.fail();
            e
        };
        // The KeyUpdate goes out under the old key, then the kernel gets the new one.
        ready!(self.poll_send_control(cx, 22, msg)).map_err(fail)?;
        let (seq, secrets) = shared.update_tx().map_err(tls_error).map_err(fail)?;
        let keys = KeyMaterial::of(&secrets)
            .ok_or_else(|| io::Error::other("kernel TLS: unsupported cipher"))
            .map_err(fail)?;
        let tcp = self.io.tcp().expect("kernel TLS runs on a TCP socket");
        ktls::set_keys(tcp, Dir::Tx, &keys, seq)
            .map_err(|e| io::Error::other(format!("kernel TLS: send key update: {e}")))
            .map_err(fail)?;
        if let WriteMode::Ktls(k) = &mut self.mode {
            k.rekey_pending = false;
            k.records = 0;
        }
        Poll::Ready(Ok(()))
    }

    /// Writes queued records until at most `target` bytes remain queued.
    fn poll_drain_to(&mut self, cx: &mut Context<'_>, target: usize) -> Poll<io::Result<()>> {
        while self.queued > target {
            let mut iov = [IoSlice::new(&[]); MAX_IOV];
            let mut k = 0;
            for (i, rec) in self.queue.iter().take(MAX_IOV).enumerate() {
                iov[i] = IoSlice::new(if i == 0 { &rec[self.head..] } else { rec });
                k += 1;
            }
            let mut n = ready!(Pin::new(&mut self.io).poll_write_vectored(cx, &iov[..k]))?;
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            self.queued -= n;
            while n > 0 {
                let front = self.queue.front().map_or(0, |r| r.len() - self.head);
                if n >= front {
                    n -= front;
                    self.queue.pop_front();
                    self.head = 0;
                } else {
                    self.head += n;
                    n = 0;
                }
            }
        }
        Poll::Ready(Ok(()))
    }

    fn check_open(&self) -> io::Result<()> {
        let shared = match &self.mode {
            WriteMode::Kernel { shared, .. } => Some(shared),
            WriteMode::Ktls(k) => Some(&k.shared),
            WriteMode::Locked(_) => None,
        };
        if shared.is_some_and(|s| s.has_failed()) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "TLS connection failed",
            ));
        }
        if self.close_queued {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "write after TLS close_notify",
            ));
        }
        Ok(())
    }

    /// Encrypts up to `take` bytes of `bufs` into the queue.
    fn seal(&mut self, bufs: &[IoSlice<'_>], take: usize) -> io::Result<usize> {
        let slices: Vec<&[u8]> = bufs.iter().map(|b| &**b).collect();
        match &mut self.mode {
            WriteMode::Kernel { tx, shared } => {
                let (queue, queued) = (&mut self.queue, &mut self.queued);
                tx.seal_app(&slices, take, |rec| {
                    *queued += rec.len();
                    queue.push_back(rec);
                })
                .map_err(|e| {
                    shared.fail();
                    tls_error(e)
                })
            }
            WriteMode::Ktls(_) => unreachable!("kernel TLS writes plaintext"),
            WriteMode::Locked(l) => {
                let l = l.clone();
                let mut st = lock(&l);
                let r = st.pump(Work {
                    data: Some(&slices),
                    limit: take,
                    close: false,
                });
                self.take_locked_output(&mut st);
                match r {
                    Ok(0) => Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "TLS connection closed",
                    )),
                    Ok(n) => Ok(n),
                    Err(e) => Err(tls_error(e)),
                }
            }
        }
    }
}

impl<W: AsyncWrite + Unpin + RawTcp> AsyncWrite for TlsWriteHalf<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored(cx, &[IoSlice::new(buf)])
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let me = self.get_mut();
        me.check_open()?;
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        if total == 0 {
            return Poll::Ready(Ok(0));
        }
        if let WriteMode::Ktls(_) = me.mode {
            ready!(me.poll_ktls_rekey(cx))?;
            let WriteMode::Ktls(k) = &mut me.mode else {
                unreachable!()
            };
            // A call of `n` bytes may end `n / 2^14 + 1` records: never let one call take the
            // current key past its limit.
            let budget = k.seq_max.saturating_sub(k.records).saturating_sub(1);
            let cap = usize::try_from(budget)
                .unwrap_or(usize::MAX)
                .saturating_mul(MAX_FRAGMENT);
            let n = if cap >= total {
                ready!(Pin::new(&mut me.io).poll_write_vectored(cx, bufs))?
            } else {
                let capped = cap_slices(bufs, cap.max(1));
                ready!(Pin::new(&mut me.io).poll_write_vectored(cx, &capped))?
            };
            if let WriteMode::Ktls(k) = &mut me.mode {
                k.records += (n / MAX_FRAGMENT) as u64 + 1;
            }
            return Poll::Ready(Ok(n));
        }
        if me.queued >= WRITE_LIMIT {
            ready!(me.poll_drain_to(cx, WRITE_LIMIT - MAX_FRAGMENT))?;
        }
        let take = total.min((WRITE_LIMIT - me.queued).max(MAX_FRAGMENT));
        let n = me.seal(bufs, take)?;
        // Start sending right away; a full socket is not an error here (the bytes are ours).
        if let Poll::Ready(Err(e)) = me.poll_drain_to(cx, 0) {
            return Poll::Ready(Err(e));
        }
        Poll::Ready(Ok(n))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        if let WriteMode::Locked(l) = &me.mode {
            let l = l.clone();
            let mut st = lock(&l);
            me.take_locked_output(&mut st);
        }
        ready!(me.poll_drain_to(cx, 0))?;
        Pin::new(&mut me.io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        if !me.close_queued {
            me.close_queued = true;
            match &mut me.mode {
                WriteMode::Kernel { tx, shared } => {
                    if !shared.has_failed() {
                        let rec = tx.close_notify().map_err(tls_error)?;
                        me.push(rec);
                    }
                }
                WriteMode::Ktls(k) => {
                    if !k.shared.has_failed() {
                        // close_queued stays false until the alert is out.
                        me.close_queued = false;
                        ready!(me.poll_send_control(cx, 21, &CLOSE_NOTIFY))?;
                        me.close_queued = true;
                    }
                }
                WriteMode::Locked(l) => {
                    let l = l.clone();
                    let mut st = lock(&l);
                    let _ = st.pump(Work {
                        data: None,
                        limit: 0,
                        close: true,
                    });
                    me.take_locked_output(&mut st);
                }
            }
        }
        ready!(me.poll_drain_to(cx, 0))?;
        Pin::new(&mut me.io).poll_shutdown(cx)
    }
}
