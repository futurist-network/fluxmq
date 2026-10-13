//! Kernel TLS (v0.10, Linux): after the rustls handshake the TLS 1.3 traffic keys go to the
//! kernel (`TCP_ULP "tls"`, `TLS_TX` / `TLS_RX`), which then seals and opens records itself.
//! A send is a plain `writev` of plaintext and a receive a `recvmsg` straight into the
//! caller's buffer: no userspace copy into a record, no userspace AEAD, and the socket can
//! carry `sendfile` (records are built by the kernel from page-cache pages).
//!
//! What stays in rustls: the handshake, every secret (initial ones via
//! `dangerous_into_kernel_connection`, KeyUpdate ones via `KernelConnection`), session
//! tickets. What this module owns on top of [`super::record`]'s rules: telling the kernel
//! the keys and sequence numbers, and the post-handshake records the kernel hands back
//! (alerts, KeyUpdate, NewSessionTicket), which go through the same [`Control`] checks as
//! the userspace record layer.
//!
//! Selection: `FLUXMQ_TLS_IMPL=ktls` (opt-in) uses kTLS when the kernel offers it; the
//! default `fast` keeps the v0.8 record layer (kTLS lost the v0.10 A/B, see
//! docs/design/v0.10-ktls.md). Any refusal before keys are installed falls back
//! to the userspace record layer for that connection; a missing `tls` module (`ENOENT`)
//! disables kTLS for the process.
//!
//! [`Control`]: super::record::Control

use std::io;
use std::sync::atomic::{AtomicU8, Ordering};

use rustls::ConnectionTrafficSecrets;

/// A transport half that may expose its TCP socket (only a real `TcpStream` can carry kTLS).
pub trait RawTcp {
    fn tcp(&self) -> Option<&tokio::net::TcpStream> {
        None
    }
}

impl RawTcp for tokio::net::tcp::OwnedReadHalf {
    fn tcp(&self) -> Option<&tokio::net::TcpStream> {
        Some(self.as_ref())
    }
}

impl RawTcp for tokio::net::tcp::OwnedWriteHalf {
    fn tcp(&self) -> Option<&tokio::net::TcpStream> {
        Some(self.as_ref())
    }
}

impl<T> RawTcp for tokio::io::ReadHalf<T> {}
impl<T> RawTcp for tokio::io::WriteHalf<T> {}
impl RawTcp for tokio::io::DuplexStream {}

/// Process-wide state: 0 = not tried yet, 1 = works, 2 = unavailable.
static STATE: AtomicU8 = AtomicU8::new(0);
const WORKS: u8 = 1;
const UNAVAILABLE: u8 = 2;

/// Is kernel TLS worth trying on a new connection (Linux, not found missing before)?
pub fn available() -> bool {
    cfg!(target_os = "linux") && STATE.load(Ordering::Relaxed) != UNAVAILABLE
}

/// True once a connection in this process got kernel keys installed.
pub fn active() -> bool {
    STATE.load(Ordering::Relaxed) == WORKS
}

fn mark_works() {
    if STATE.swap(WORKS, Ordering::Relaxed) != WORKS {
        tracing::info!("kernel TLS active (records sealed and opened by the kernel)");
    }
}

fn mark_unavailable(why: &io::Error) {
    if STATE.swap(UNAVAILABLE, Ordering::Relaxed) != UNAVAILABLE {
        tracing::info!(
            error = %why,
            "kernel TLS unavailable, using the userspace record layer (load it with `modprobe tls`)"
        );
    }
}

/// Key material of one direction, copied out of rustls' secrets (zeroed on drop).
pub(crate) struct KeyMaterial {
    cipher: Cipher,
    key: [u8; 32],
    iv: [u8; 12],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cipher {
    Aes128Gcm,
    Aes256Gcm,
    Chacha20Poly1305,
}

impl KeyMaterial {
    /// `None` for a cipher the kernel does not get from us.
    pub(crate) fn of(s: &ConnectionTrafficSecrets) -> Option<Self> {
        let (cipher, key, iv) = match s {
            ConnectionTrafficSecrets::Aes128Gcm { key, iv } => (Cipher::Aes128Gcm, key, iv),
            ConnectionTrafficSecrets::Aes256Gcm { key, iv } => (Cipher::Aes256Gcm, key, iv),
            ConnectionTrafficSecrets::Chacha20Poly1305 { key, iv } => {
                (Cipher::Chacha20Poly1305, key, iv)
            }
            _ => return None,
        };
        let (key, iv) = (key.as_ref(), iv.as_ref());
        let want = match cipher {
            Cipher::Aes128Gcm => 16,
            Cipher::Aes256Gcm | Cipher::Chacha20Poly1305 => 32,
        };
        if key.len() != want || iv.len() != 12 {
            return None;
        }
        let mut m = KeyMaterial {
            cipher,
            key: [0; 32],
            iv: [0; 12],
        };
        m.key[..want].copy_from_slice(key);
        m.iv.copy_from_slice(iv);
        Some(m)
    }
}

impl Drop for KeyMaterial {
    fn drop(&mut self) {
        wipe(&mut self.key);
        wipe(&mut self.iv);
    }
}

fn wipe(b: &mut [u8]) {
    for x in b.iter_mut() {
        // Volatile so the compiler keeps the stores.
        unsafe { std::ptr::write_volatile(x, 0) };
    }
    std::sync::atomic::compiler_fence(Ordering::SeqCst);
}

/// Direction for [`set_keys`].
#[derive(Clone, Copy, Debug)]
pub(crate) enum Dir {
    Tx,
    Rx,
}

pub(crate) use sys::{recv_record, send_record};

/// Attaches the TLS ULP. `Err` = kernel TLS cannot be used on this socket; a missing module
/// also disables it for the process.
pub(crate) fn attach(tcp: &tokio::net::TcpStream) -> io::Result<()> {
    match sys::attach_ulp(tcp) {
        Ok(()) => Ok(()),
        Err(e) => {
            // ENOENT: no `tls` module; EOPNOTSUPP/ENOPROTOOPT: kernel without CONFIG_TLS.
            if matches!(
                e.raw_os_error(),
                Some(libc_errno::ENOENT | libc_errno::ENOPROTOOPT | libc_errno::EOPNOTSUPP)
            ) {
                mark_unavailable(&e);
            }
            Err(e)
        }
    }
}

/// Installs keys for one direction at sequence number `seq`. Also used for KeyUpdate
/// (re-keying needs Linux 6.14+; older kernels refuse, which fails that connection).
pub(crate) fn set_keys(
    tcp: &tokio::net::TcpStream,
    dir: Dir,
    keys: &KeyMaterial,
    seq: u64,
) -> io::Result<()> {
    sys::set_keys(tcp, dir, keys, seq)?;
    mark_works();
    Ok(())
}

/// Optional receive hint: the peer does not pad records (FluxMQ peers never do; the kernel
/// retries correctly if one does).
pub(crate) fn expect_no_pad(tcp: &tokio::net::TcpStream) {
    sys::expect_no_pad(tcp);
}

mod libc_errno {
    pub(super) const ENOENT: i32 = 2;
    pub(super) const ENOPROTOOPT: i32 = 92;
    pub(super) const EOPNOTSUPP: i32 = 95;
}

#[cfg(target_os = "linux")]
mod sys {
    use std::io;
    use std::mem::{size_of, zeroed};
    use std::os::fd::AsRawFd;

    use super::{Cipher, Dir, KeyMaterial, wipe};

    const TCP_ULP: libc::c_int = 31;
    const SOL_TLS: libc::c_int = 282;
    const TLS_TX: libc::c_int = 1;
    const TLS_RX: libc::c_int = 2;
    const TLS_RX_EXPECT_NO_PAD: libc::c_int = 4;
    const TLS_SET_RECORD_TYPE: libc::c_int = 1;
    const TLS_GET_RECORD_TYPE: libc::c_int = 2;
    const TLS_1_3_VERSION: u16 = 0x0304;
    const TLS_CIPHER_AES_GCM_128: u16 = 51;
    const TLS_CIPHER_AES_GCM_256: u16 = 52;
    const TLS_CIPHER_CHACHA20_POLY1305: u16 = 54;
    /// TLS application_data content type.
    pub(crate) const APPLICATION_DATA: u8 = 23;

    #[repr(C)]
    struct Info {
        version: u16,
        cipher_type: u16,
    }

    // `struct tls12_crypto_info_*` from <linux/tls.h>.
    #[repr(C)]
    struct Aes128 {
        info: Info,
        iv: [u8; 8],
        key: [u8; 16],
        salt: [u8; 4],
        rec_seq: [u8; 8],
    }

    #[repr(C)]
    struct Aes256 {
        info: Info,
        iv: [u8; 8],
        key: [u8; 32],
        salt: [u8; 4],
        rec_seq: [u8; 8],
    }

    #[repr(C)]
    struct Chacha {
        info: Info,
        iv: [u8; 12],
        key: [u8; 32],
        rec_seq: [u8; 8],
    }

    fn setsockopt<T>(
        tcp: &tokio::net::TcpStream,
        level: libc::c_int,
        name: libc::c_int,
        val: &T,
        len: usize,
    ) -> io::Result<()> {
        let r = unsafe {
            libc::setsockopt(
                tcp.as_raw_fd(),
                level,
                name,
                val as *const T as *const libc::c_void,
                len as libc::socklen_t,
            )
        };
        if r != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(super) fn attach_ulp(tcp: &tokio::net::TcpStream) -> io::Result<()> {
        let name = *b"tls\0";
        setsockopt(tcp, libc::IPPROTO_TCP, TCP_ULP, &name, name.len())
    }

    pub(super) fn expect_no_pad(tcp: &tokio::net::TcpStream) {
        let one: libc::c_int = 1;
        let _ = setsockopt(
            tcp,
            SOL_TLS,
            TLS_RX_EXPECT_NO_PAD,
            &one,
            size_of::<libc::c_int>(),
        );
    }

    /// Wipes a crypto-info struct after the kernel copied it.
    fn wipe_struct<T>(v: &mut T) {
        let bytes =
            unsafe { std::slice::from_raw_parts_mut(v as *mut T as *mut u8, size_of::<T>()) };
        wipe(bytes);
    }

    pub(super) fn set_keys(
        tcp: &tokio::net::TcpStream,
        dir: Dir,
        k: &KeyMaterial,
        seq: u64,
    ) -> io::Result<()> {
        let name = match dir {
            Dir::Tx => TLS_TX,
            Dir::Rx => TLS_RX,
        };
        let rec_seq = seq.to_be_bytes();
        // The 12-byte TLS 1.3 IV is split into a 4-byte salt and an 8-byte "iv" for GCM.
        match k.cipher {
            Cipher::Aes128Gcm => {
                let mut ci: Aes128 = unsafe { zeroed() };
                ci.info = Info {
                    version: TLS_1_3_VERSION,
                    cipher_type: TLS_CIPHER_AES_GCM_128,
                };
                ci.salt.copy_from_slice(&k.iv[..4]);
                ci.iv.copy_from_slice(&k.iv[4..]);
                ci.key.copy_from_slice(&k.key[..16]);
                ci.rec_seq = rec_seq;
                let r = setsockopt(tcp, SOL_TLS, name, &ci, size_of::<Aes128>());
                wipe_struct(&mut ci);
                r
            }
            Cipher::Aes256Gcm => {
                let mut ci: Aes256 = unsafe { zeroed() };
                ci.info = Info {
                    version: TLS_1_3_VERSION,
                    cipher_type: TLS_CIPHER_AES_GCM_256,
                };
                ci.salt.copy_from_slice(&k.iv[..4]);
                ci.iv.copy_from_slice(&k.iv[4..]);
                ci.key.copy_from_slice(&k.key);
                ci.rec_seq = rec_seq;
                let r = setsockopt(tcp, SOL_TLS, name, &ci, size_of::<Aes256>());
                wipe_struct(&mut ci);
                r
            }
            Cipher::Chacha20Poly1305 => {
                let mut ci: Chacha = unsafe { zeroed() };
                ci.info = Info {
                    version: TLS_1_3_VERSION,
                    cipher_type: TLS_CIPHER_CHACHA20_POLY1305,
                };
                ci.iv.copy_from_slice(&k.iv);
                ci.key.copy_from_slice(&k.key);
                ci.rec_seq = rec_seq;
                let r = setsockopt(tcp, SOL_TLS, name, &ci, size_of::<Chacha>());
                wipe_struct(&mut ci);
                r
            }
        }
    }

    /// Room for one `cmsghdr` carrying one byte, 8-byte aligned (`CMSG_SPACE(1)` = 24).
    #[repr(C, align(8))]
    struct Cmsg([u8; 32]);

    /// One `recvmsg` (non-blocking) into `buf`: `(bytes, record content type)`. The kernel
    /// never mixes record types in one call; a non-data record is returned on its own.
    pub(crate) fn recv_record(
        tcp: &tokio::net::TcpStream,
        buf: *mut u8,
        len: usize,
    ) -> io::Result<(usize, u8)> {
        let mut iov = libc::iovec {
            iov_base: buf as *mut libc::c_void,
            iov_len: len,
        };
        let mut cbuf = Cmsg([0; 32]);
        let mut msg: libc::msghdr = unsafe { zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cbuf.0.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = cbuf.0.len() as _;
        let n = unsafe { libc::recvmsg(tcp.as_raw_fd(), &mut msg, libc::MSG_DONTWAIT) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        let mut typ = APPLICATION_DATA;
        unsafe {
            let mut c = libc::CMSG_FIRSTHDR(&msg);
            while !c.is_null() {
                if (*c).cmsg_level == SOL_TLS && (*c).cmsg_type == TLS_GET_RECORD_TYPE {
                    typ = *libc::CMSG_DATA(c);
                }
                c = libc::CMSG_NXTHDR(&msg, c);
            }
        }
        Ok((n as usize, typ))
    }

    /// Sends one record of content type `typ` (non-blocking, all or nothing for the small
    /// control records this is used for).
    pub(crate) fn send_record(tcp: &tokio::net::TcpStream, typ: u8, data: &[u8]) -> io::Result<()> {
        let mut iov = libc::iovec {
            iov_base: data.as_ptr() as *mut libc::c_void,
            iov_len: data.len(),
        };
        let mut cbuf = Cmsg([0; 32]);
        let mut msg: libc::msghdr = unsafe { zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cbuf.0.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = unsafe { libc::CMSG_SPACE(1) } as _;
        unsafe {
            let c = libc::CMSG_FIRSTHDR(&msg);
            (*c).cmsg_level = SOL_TLS;
            (*c).cmsg_type = TLS_SET_RECORD_TYPE;
            (*c).cmsg_len = libc::CMSG_LEN(1) as _;
            *libc::CMSG_DATA(c) = typ;
        }
        let n = unsafe {
            libc::sendmsg(
                tcp.as_raw_fd(),
                &msg,
                libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
            )
        };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        if n as usize != data.len() {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "kernel TLS: partial control record",
            ));
        }
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
mod sys {
    use std::io;

    use super::{Dir, KeyMaterial};

    fn unsupported() -> io::Error {
        io::Error::new(io::ErrorKind::Unsupported, "kernel TLS needs Linux")
    }

    pub(super) fn attach_ulp(_: &tokio::net::TcpStream) -> io::Result<()> {
        Err(unsupported())
    }

    pub(super) fn expect_no_pad(_: &tokio::net::TcpStream) {}

    pub(super) fn set_keys(
        _: &tokio::net::TcpStream,
        _: Dir,
        _: &KeyMaterial,
        _: u64,
    ) -> io::Result<()> {
        Err(unsupported())
    }

    pub(crate) fn recv_record(
        _: &tokio::net::TcpStream,
        _: *mut u8,
        _: usize,
    ) -> io::Result<(usize, u8)> {
        Err(unsupported())
    }

    pub(crate) fn send_record(_: &tokio::net::TcpStream, _: u8, _: &[u8]) -> io::Result<()> {
        Err(unsupported())
    }
}
