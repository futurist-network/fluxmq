//! Client-side object encryption: AES-256-GCM in a chunked, STREAM-style format so objects of
//! any size are encrypted and decrypted with bounded memory.
//!
//! ## Format (version 1)
//!
//! ```text
//! header (16 bytes): "FXBE" | version u8 = 1 | chunk_size u32 LE | nonce_prefix [u8; 7]
//! chunk*:            AES-256-GCM(ciphertext of ≤ chunk_size plaintext bytes) | tag (16 bytes)
//! ```
//!
//! * `nonce_prefix` is random per object (OS RNG). Chunk `i` uses the 96-bit nonce
//!   `nonce_prefix || i as u32 BE || last` where `last` is `1` for the final chunk and `0`
//!   otherwise, so chunks can't be reordered, dropped or the object truncated/extended
//!   without failing authentication (the STREAM construction of Hoang, Reyhanitabar, Rogaway
//!   and Vizár).
//! * Every chunk but the last holds exactly `chunk_size` plaintext bytes; the last holds
//!   `1..=chunk_size` bytes (or 0 for an empty object, which is one empty last chunk).
//! * The associated data of every chunk is the object's key relative to the target prefix
//!   (e.g. `segments/orders/0/00000000000000000000-1a2b3c4d.k0123abcd.log`), so an object
//!   can't be swapped for another one encrypted with the same key.
//! * Encrypted size = `16 + plaintext + 16 × chunks` (see [`encrypted_len`]).
//!
//! The key is 32 random bytes, configured base64-encoded. Its **fingerprint** (first 8 bytes
//! of `SHA-256("fluxmq-backup-key-v1" || key)`, hex) is stored in manifests and object names
//! so a wrong key is reported up front; it reveals nothing usable about the key.

use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce, Tag};
use base64::Engine as _;
use sha2::{Digest, Sha256};

use crate::BackupError;

pub const MAGIC: &[u8; 4] = b"FXBE";
pub const VERSION: u8 = 1;
pub const HEADER_LEN: usize = 16;
pub const TAG_LEN: usize = 16;
/// Plaintext bytes per chunk.
pub const CHUNK_SIZE: usize = 1 << 20;

/// A parsed AES-256 key. `Debug` never prints key material.
#[derive(Clone)]
pub struct Key {
    cipher: Aes256Gcm,
    fingerprint: String,
}

impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Key({})", self.fingerprint)
    }
}

impl Key {
    /// Parses a base64 (standard or URL-safe, padding optional) 32-byte key.
    pub fn parse(b64: &str) -> Result<Key, BackupError> {
        let s = b64.trim();
        let engines = [
            base64::engine::general_purpose::STANDARD,
            base64::engine::general_purpose::STANDARD_NO_PAD,
            base64::engine::general_purpose::URL_SAFE,
            base64::engine::general_purpose::URL_SAFE_NO_PAD,
        ];
        let bytes = engines
            .iter()
            .find_map(|e| e.decode(s).ok())
            .ok_or_else(|| BackupError::Invalid("encryption_key is not valid base64".into()))?;
        if bytes.len() != 32 {
            return Err(BackupError::Invalid(format!(
                "encryption_key must decode to 32 bytes (got {}); generate one with `openssl rand -base64 32`",
                bytes.len()
            )));
        }
        let mut h = Sha256::new();
        h.update(b"fluxmq-backup-key-v1");
        h.update(&bytes);
        let digest = h.finalize();
        let fingerprint = digest[..8].iter().map(|b| format!("{b:02x}")).collect();
        let cipher = Aes256Gcm::new_from_slice(&bytes)
            .map_err(|_| BackupError::Invalid("bad key length".into()))?;
        Ok(Key {
            cipher,
            fingerprint,
        })
    }

    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
}

/// Encrypted size of a `plain`-byte object.
pub fn encrypted_len(plain: u64) -> u64 {
    let chunks = if plain == 0 {
        1
    } else {
        plain.div_ceil(CHUNK_SIZE as u64)
    };
    HEADER_LEN as u64 + plain + chunks * TAG_LEN as u64
}

fn nonce(prefix: &[u8; 7], counter: u32, last: bool) -> [u8; 12] {
    let mut n = [0u8; 12];
    n[..7].copy_from_slice(prefix);
    n[7..11].copy_from_slice(&counter.to_be_bytes());
    n[11] = u8::from(last);
    n
}

/// Streaming encryptor of one object.
pub struct Sealer {
    cipher: Aes256Gcm,
    prefix: [u8; 7],
    counter: u32,
    aad: Vec<u8>,
    finished: bool,
}

impl Sealer {
    pub fn new(key: &Key, object_key: &str) -> Result<Sealer, BackupError> {
        let mut prefix = [0u8; 7];
        getrandom::fill(&mut prefix)
            .map_err(|e| BackupError::Crypto(format!("OS random generator failed: {e}")))?;
        Ok(Sealer {
            cipher: key.cipher.clone(),
            prefix,
            counter: 0,
            aad: object_key.as_bytes().to_vec(),
            finished: false,
        })
    }

    pub fn header(&self) -> [u8; HEADER_LEN] {
        let mut h = [0u8; HEADER_LEN];
        h[..4].copy_from_slice(MAGIC);
        h[4] = VERSION;
        h[5..9].copy_from_slice(&(CHUNK_SIZE as u32).to_le_bytes());
        h[9..16].copy_from_slice(&self.prefix);
        h
    }

    /// Encrypts one chunk in place and appends its tag. Every chunk but the last must be
    /// exactly [`CHUNK_SIZE`] bytes.
    pub fn seal_chunk(&mut self, buf: &mut Vec<u8>, last: bool) -> Result<(), BackupError> {
        if self.finished {
            return Err(BackupError::Crypto("chunk after the last chunk".into()));
        }
        if !last && buf.len() != CHUNK_SIZE || buf.len() > CHUNK_SIZE {
            return Err(BackupError::Crypto("bad chunk size".into()));
        }
        let n = nonce(&self.prefix, self.counter, last);
        let tag = self
            .cipher
            .encrypt_in_place_detached(Nonce::from_slice(&n), &self.aad, buf)
            .map_err(|_| BackupError::Crypto("encryption failed".into()))?;
        buf.extend_from_slice(&tag);
        self.counter = self
            .counter
            .checked_add(1)
            .ok_or_else(|| BackupError::Crypto("object too large".into()))?;
        self.finished = last;
        Ok(())
    }
}

/// Encrypts a whole in-memory object.
pub fn seal_all(key: &Key, object_key: &str, plain: &[u8]) -> Result<Vec<u8>, BackupError> {
    let mut s = Sealer::new(key, object_key)?;
    let mut out = Vec::with_capacity(encrypted_len(plain.len() as u64) as usize);
    out.extend_from_slice(&s.header());
    let mut chunks = plain.chunks(CHUNK_SIZE).peekable();
    if chunks.peek().is_none() {
        let mut b = Vec::new();
        s.seal_chunk(&mut b, true)?;
        out.extend_from_slice(&b);
    }
    while let Some(c) = chunks.next() {
        let mut b = c.to_vec();
        s.seal_chunk(&mut b, chunks.peek().is_none())?;
        out.extend_from_slice(&b);
    }
    Ok(out)
}

/// Streaming decryptor of one object: feed ciphertext with [`Opener::push`], then call
/// [`Opener::finish`]. Plaintext is only produced for authenticated chunks.
pub struct Opener {
    cipher: Aes256Gcm,
    aad: Vec<u8>,
    header: Option<([u8; 7], usize)>,
    counter: u32,
    buf: Vec<u8>,
}

impl Opener {
    pub fn new(key: &Key, object_key: &str) -> Opener {
        Opener {
            cipher: key.cipher.clone(),
            aad: object_key.as_bytes().to_vec(),
            header: None,
            counter: 0,
            buf: Vec::new(),
        }
    }

    fn open_chunk(&mut self, mut chunk: Vec<u8>, last: bool) -> Result<Vec<u8>, BackupError> {
        let (prefix, _) = self.header.expect("header parsed");
        if chunk.len() < TAG_LEN {
            return Err(BackupError::Crypto("truncated encrypted object".into()));
        }
        let tag_at = chunk.len() - TAG_LEN;
        let tag = Tag::clone_from_slice(&chunk[tag_at..]);
        chunk.truncate(tag_at);
        let n = nonce(&prefix, self.counter, last);
        self.cipher
            .decrypt_in_place_detached(Nonce::from_slice(&n), &self.aad, &mut chunk, &tag)
            .map_err(|_| {
                BackupError::Crypto(
                    "decryption failed: wrong encryption key or corrupted object".into(),
                )
            })?;
        self.counter = self.counter.wrapping_add(1);
        Ok(chunk)
    }

    /// Appends decrypted plaintext of every complete non-final chunk to `out`.
    pub fn push(&mut self, data: &[u8], out: &mut Vec<u8>) -> Result<(), BackupError> {
        self.buf.extend_from_slice(data);
        if self.header.is_none() {
            if self.buf.len() < HEADER_LEN {
                return Ok(());
            }
            if &self.buf[..4] != MAGIC || self.buf[4] != VERSION {
                return Err(BackupError::Crypto(
                    "object is not encrypted with a supported FluxMQ format (was the backup made without encryption?)"
                        .into(),
                ));
            }
            let chunk = u32::from_le_bytes(self.buf[5..9].try_into().expect("4 bytes")) as usize;
            if chunk == 0 || chunk > 64 << 20 {
                return Err(BackupError::Crypto("bad chunk size in header".into()));
            }
            let mut prefix = [0u8; 7];
            prefix.copy_from_slice(&self.buf[9..16]);
            self.header = Some((prefix, chunk));
            self.buf.drain(..HEADER_LEN);
        }
        let (_, chunk) = self.header.expect("header parsed");
        let sealed = chunk + TAG_LEN;
        // A chunk is known not to be the last only once more bytes follow it.
        while self.buf.len() > sealed {
            let rest = self.buf.split_off(sealed);
            let c = std::mem::replace(&mut self.buf, rest);
            out.extend_from_slice(&self.open_chunk(c, false)?);
        }
        Ok(())
    }

    /// Decrypts the final chunk.
    pub fn finish(mut self, out: &mut Vec<u8>) -> Result<(), BackupError> {
        if self.header.is_none() {
            return Err(BackupError::Crypto("truncated encrypted object".into()));
        }
        let c = std::mem::take(&mut self.buf);
        out.extend_from_slice(&self.open_chunk(c, true)?);
        Ok(())
    }
}

/// Decrypts a whole in-memory object.
pub fn open_all(key: &Key, object_key: &str, data: &[u8]) -> Result<Vec<u8>, BackupError> {
    let mut o = Opener::new(key, object_key);
    let mut out = Vec::with_capacity(data.len());
    o.push(data, &mut out)?;
    o.finish(&mut out)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(b: u8) -> Key {
        Key::parse(&base64::engine::general_purpose::STANDARD.encode([b; 32])).unwrap()
    }

    #[test]
    fn roundtrip_sizes_and_failures() {
        let k = key(7);
        for n in [
            0,
            1,
            CHUNK_SIZE - 1,
            CHUNK_SIZE,
            CHUNK_SIZE + 1,
            2 * CHUNK_SIZE + 5,
        ] {
            let plain: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
            let enc = seal_all(&k, "a/b", &plain).unwrap();
            assert_eq!(enc.len() as u64, encrypted_len(n as u64), "n={n}");
            // Streaming in odd pieces.
            let mut o = Opener::new(&k, "a/b");
            let mut out = Vec::new();
            for piece in enc.chunks(777_777) {
                o.push(piece, &mut out).unwrap();
            }
            o.finish(&mut out).unwrap();
            assert_eq!(out, plain);
            // Wrong key, wrong object key, truncation all fail.
            assert!(open_all(&key(8), "a/b", &enc).is_err());
            assert!(open_all(&k, "a/c", &enc).is_err());
            if n > CHUNK_SIZE {
                let cut = HEADER_LEN + CHUNK_SIZE + TAG_LEN;
                assert!(open_all(&k, "a/b", &enc[..cut]).is_err());
            }
        }
        assert_ne!(key(1).fingerprint(), key(2).fingerprint());
        assert!(Key::parse("c2hvcnQ=").is_err());
        assert!(Key::parse("!!!").is_err());
    }
}
