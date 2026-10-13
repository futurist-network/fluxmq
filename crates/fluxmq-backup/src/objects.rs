//! Streaming object transfer with bounded memory.
//!
//! Uploads read the source in [`READ_CHUNK`] pieces (segment bytes through positional reads
//! on a blocking thread), optionally encrypt each piece ([`crate::crypto`]) and either `PUT`
//! the object (≤ [`MULTIPART_THRESHOLD`]) or stream it as a multipart upload with
//! [`PART_SIZE`] parts and at most [`PART_CONCURRENCY`] parts in flight. Per object at most
//! about `PART_SIZE × (PART_CONCURRENCY + 1)` bytes are buffered.
//!
//! Downloads stream the body, decrypt chunk by chunk and verify plaintext length + CRC-32.

use std::sync::Arc;

use bytes::Bytes;
use fluxmq_core::backup::SegmentCut;
use futures::StreamExt;
use object_store::{ObjectStoreExt, PutPayload, WriteMultipart};
use tokio::io::AsyncWriteExt;

use crate::crypto::{self, Key, Opener, Sealer};
use crate::manifest::ObjectRef;
use crate::store::Target;
use crate::{BackupError, Result, join_err};

pub const READ_CHUNK: usize = crypto::CHUNK_SIZE;
pub const PART_SIZE: usize = 5 * 1024 * 1024;
pub const PART_CONCURRENCY: usize = 2;
pub const MULTIPART_THRESHOLD: u64 = PART_SIZE as u64;

/// Bytes to upload.
#[derive(Clone, Debug)]
pub enum Source {
    /// A pinned segment prefix (read with positional reads, never past its captured length).
    Segment(Arc<SegmentCut>),
    Bytes(Bytes),
}

impl Source {
    pub fn len(&self) -> u64 {
        match self {
            Source::Segment(s) => s.len,
            Source::Bytes(b) => b.len() as u64,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    async fn read(&self, pos: u64, n: usize) -> Result<Bytes> {
        match self {
            Source::Bytes(b) => Ok(b.slice(pos as usize..pos as usize + n)),
            Source::Segment(s) => {
                let s = s.clone();
                tokio::task::spawn_blocking(move || {
                    let mut buf = vec![0u8; n];
                    let got = s.read_at(&mut buf, pos)?;
                    if got != n {
                        return Err(BackupError::Other(format!(
                            "short read of segment {} at {pos}",
                            s.base_offset
                        )));
                    }
                    Ok(Bytes::from(buf))
                })
                .await
                .map_err(join_err)?
            }
        }
    }
}

/// CRC-32 of the whole source (streamed).
pub async fn crc_of(src: &Source) -> Result<u32> {
    if let Source::Bytes(b) = src {
        return Ok(crc32fast::hash(b));
    }
    let len = src.len();
    let mut h = crc32fast::Hasher::new();
    let mut pos = 0u64;
    while pos < len {
        let n = READ_CHUNK.min((len - pos) as usize);
        let b = src.read(pos, n).await?;
        h.update(&b);
        pos += n as u64;
    }
    Ok(h.finalize())
}

/// Stored size of a `plain`-byte object.
pub fn stored_len(plain: u64, key: Option<&Key>) -> u64 {
    match key {
        Some(_) => crypto::encrypted_len(plain),
        None => plain,
    }
}

/// `Ok(true)` when `rel` exists with exactly `stored` bytes (reusable), `Ok(false)` when
/// absent. A size mismatch is an error: the name encodes the content, so it means
/// corruption or a foreign object.
pub async fn exists_with_len(target: &Target, rel: &str, stored: u64) -> Result<bool> {
    match target.store.head(&target.path(rel)).await {
        Ok(meta) if meta.size == stored => Ok(true),
        Ok(meta) => Err(BackupError::Other(format!(
            "object {rel} exists with {} bytes, expected {stored}; delete it or use another prefix",
            meta.size
        ))),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Uploads `src` to `rel`, encrypting with `key` if set. `progress` is called with plaintext
/// bytes as they are handed to the store. Returns the stored size.
pub async fn put_object(
    target: &Target,
    rel: &str,
    src: &Source,
    key: Option<&Key>,
    progress: &(dyn Fn(u64) + Send + Sync),
) -> Result<u64> {
    let len = src.len();
    let stored = stored_len(len, key);
    let path = target.path(rel);
    if stored <= MULTIPART_THRESHOLD {
        let plain = src.read(0, len as usize).await?;
        let body = match key {
            Some(k) => Bytes::from(crypto::seal_all(k, rel, &plain)?),
            None => plain,
        };
        target.store.put(&path, PutPayload::from(body)).await?;
        progress(len);
        return Ok(stored);
    }
    let upload = target.store.put_multipart(&path).await?;
    let mut w = WriteMultipart::new_with_chunk_size(upload, PART_SIZE);
    match stream_parts(&mut w, rel, src, key, progress).await {
        Ok(()) => {
            w.finish().await?;
            Ok(stored)
        }
        Err(e) => {
            let _ = w.abort().await;
            Err(e)
        }
    }
}

async fn stream_parts(
    w: &mut WriteMultipart,
    rel: &str,
    src: &Source,
    key: Option<&Key>,
    progress: &(dyn Fn(u64) + Send + Sync),
) -> Result<()> {
    let len = src.len();
    let mut sealer = key.map(|k| Sealer::new(k, rel)).transpose()?;
    if let Some(s) = &sealer {
        w.write(&s.header());
    }
    let mut pos = 0u64;
    while pos < len {
        let n = READ_CHUNK.min((len - pos) as usize);
        let chunk = src.read(pos, n).await?;
        pos += n as u64;
        w.wait_for_capacity(PART_CONCURRENCY).await?;
        match &mut sealer {
            Some(s) => {
                let mut b = chunk.to_vec();
                s.seal_chunk(&mut b, pos == len)?;
                w.write(&b);
            }
            None => w.put(chunk),
        }
        progress(n as u64);
    }
    Ok(())
}

enum Sink<'a> {
    Vec(&'a mut Vec<u8>),
    File(&'a mut tokio::fs::File),
}

impl Sink<'_> {
    async fn write(&mut self, b: &[u8]) -> Result<()> {
        match self {
            Sink::Vec(v) => v.extend_from_slice(b),
            Sink::File(f) => f.write_all(b).await?,
        }
        Ok(())
    }
}

async fn download(
    target: &Target,
    expect: &ObjectRef,
    key: Option<&Key>,
    mut sink: Sink<'_>,
) -> Result<()> {
    let rel = expect.key.as_str();
    let res = match target.store.get(&target.path(rel)).await {
        Ok(r) => r,
        Err(object_store::Error::NotFound { .. }) => {
            return Err(BackupError::NotFound(format!(
                "object {rel} is missing from the backup target"
            )));
        }
        Err(e) => return Err(e.into()),
    };
    let mut stream = res.into_stream();
    let mut opener = key.map(|k| Opener::new(k, rel));
    let mut h = crc32fast::Hasher::new();
    let mut total = 0u64;
    let mut plain = Vec::new();
    while let Some(b) = stream.next().await {
        let b = b?;
        let out: &[u8] = match &mut opener {
            Some(o) => {
                plain.clear();
                o.push(&b, &mut plain)?;
                &plain
            }
            None => &b,
        };
        h.update(out);
        total += out.len() as u64;
        if total > expect.len {
            return Err(BackupError::Other(format!(
                "object {rel} is larger than recorded"
            )));
        }
        sink.write(out).await?;
    }
    if let Some(o) = opener {
        plain.clear();
        o.finish(&mut plain)?;
        h.update(&plain);
        total += plain.len() as u64;
        sink.write(&plain).await?;
    }
    let crc = h.finalize();
    if total != expect.len || crc != expect.crc {
        return Err(BackupError::Other(format!(
            "object {rel} failed verification: {total} bytes crc {crc:08x}, expected {} bytes crc {:08x}",
            expect.len, expect.crc
        )));
    }
    Ok(())
}

/// Downloads a (small) object into memory, decrypting and verifying it.
pub async fn get_bytes(target: &Target, expect: &ObjectRef, key: Option<&Key>) -> Result<Vec<u8>> {
    let mut v = Vec::with_capacity(expect.len.min(64 << 20) as usize);
    download(target, expect, key, Sink::Vec(&mut v)).await?;
    Ok(v)
}

/// Streams an object into `file`, decrypting and verifying it.
pub async fn download_to_file(
    target: &Target,
    expect: &ObjectRef,
    key: Option<&Key>,
    file: &mut tokio::fs::File,
) -> Result<()> {
    download(target, expect, key, Sink::File(file)).await?;
    file.flush().await?;
    Ok(())
}
