//! On-disk record format.
//!
//! Every record is self-delimiting and self-verifying (all integers little endian):
//!
//! ```text
//! offset  size  field
//! 0       4     len          bytes following this field (crc .. end of payload)
//! 4       4     crc32        CRC-32 (IEEE) of bytes [8, 4 + len)
//! 8       8     offset       absolute log offset (lets recovery detect stale/garbage data)
//! 16      8     timestamp_ms broker-assigned, monotonic per partition
//! 24      4     key_len      u32::MAX = no key
//! 28      k     key          UTF-8
//! 28+k    4     headers_len  0 = no headers
//! 32+k    h     headers      postcard `BTreeMap<String, String>`
//! 32+k+h  p     payload      remainder
//! ```
//!
//! Overhead is 32 bytes per record plus key/headers.

use std::collections::BTreeMap;

use bytes::Bytes;
use fluxmq_proto::model::Message;

/// Size of the `len` prefix.
pub const LEN_PREFIX: usize = 4;
/// Fixed bytes of a record excluding key, headers and payload.
pub const FIXED_OVERHEAD: usize = 32;
/// Upper bound of a single record (protects recovery from interpreting garbage as a huge length).
pub const MAX_RECORD_LEN: usize = 64 * 1024 * 1024;

const NO_KEY: u32 = u32::MAX;

/// Why a record could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// Not enough bytes (torn write at the tail, or the caller must read more).
    Incomplete { needed: usize },
    /// Bytes are present but invalid (bad length, CRC mismatch, malformed fields).
    Corrupt(String),
}

/// A decoded record (partition is implied by the file it came from).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub offset: u64,
    pub timestamp_ms: u64,
    pub key: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub payload: Bytes,
}

impl Record {
    pub fn into_message(self, partition: u32) -> Message {
        Message {
            partition,
            offset: self.offset,
            key: self.key,
            headers: self.headers,
            timestamp_ms: self.timestamp_ms,
            payload: self.payload,
        }
    }
}

/// Appends one encoded record to `buf`; returns the number of bytes written.
pub fn encode_into(
    buf: &mut Vec<u8>,
    offset: u64,
    timestamp_ms: u64,
    key: Option<&str>,
    headers: &BTreeMap<String, String>,
    payload: &[u8],
) -> usize {
    let start = buf.len();
    buf.extend_from_slice(&[0u8; 8]); // len + crc, patched below
    buf.extend_from_slice(&offset.to_le_bytes());
    buf.extend_from_slice(&timestamp_ms.to_le_bytes());
    match key {
        Some(k) => {
            buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
            buf.extend_from_slice(k.as_bytes());
        }
        None => buf.extend_from_slice(&NO_KEY.to_le_bytes()),
    }
    if headers.is_empty() {
        buf.extend_from_slice(&0u32.to_le_bytes());
    } else {
        let len_pos = buf.len();
        buf.extend_from_slice(&0u32.to_le_bytes());
        // Serializing a map of strings into a Vec cannot fail.
        let before = buf.len();
        let taken = std::mem::take(buf);
        *buf = postcard::to_extend(headers, taken)
            .expect("postcard encoding of string map into Vec is infallible");
        let hlen = (buf.len() - before) as u32;
        buf[len_pos..len_pos + 4].copy_from_slice(&hlen.to_le_bytes());
    }
    buf.extend_from_slice(payload);
    let total = buf.len() - start;
    let len = (total - LEN_PREFIX) as u32;
    let crc = crc32fast::hash(&buf[start + 8..]);
    buf[start..start + 4].copy_from_slice(&len.to_le_bytes());
    buf[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
    total
}

/// Exact encoded size, without encoding (used for segment roll decisions).
pub fn encoded_len(
    key: Option<&str>,
    headers: &BTreeMap<String, String>,
    payload_len: usize,
) -> usize {
    let hlen = if headers.is_empty() {
        0
    } else {
        postcard::experimental::serialized_size(headers).unwrap_or(0)
    };
    FIXED_OVERHEAD + key.map_or(0, str::len) + hlen + payload_len
}

/// Reads the `len` prefix: total record size including the prefix.
pub fn peek_total_len(buf: &[u8]) -> Result<usize, DecodeError> {
    if buf.len() < LEN_PREFIX {
        return Err(DecodeError::Incomplete { needed: LEN_PREFIX });
    }
    let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if !(FIXED_OVERHEAD - LEN_PREFIX..=MAX_RECORD_LEN).contains(&len) {
        return Err(DecodeError::Corrupt(format!("invalid record length {len}")));
    }
    Ok(len + LEN_PREFIX)
}

/// Decodes one record from the start of `buf`; returns it and its total size.
#[cfg(test)]
pub fn decode(buf: &[u8]) -> Result<(Record, usize), DecodeError> {
    decode_with(buf, Bytes::copy_from_slice)
}

/// Like [`decode`], but the payload is a zero-copy slice of `buf`.
pub fn decode_shared(buf: &Bytes) -> Result<(Record, usize), DecodeError> {
    decode_with(buf, |s| buf.slice_ref(s))
}

fn decode_with(
    buf: &[u8],
    payload_of: impl FnOnce(&[u8]) -> Bytes,
) -> Result<(Record, usize), DecodeError> {
    let total = peek_total_len(buf)?;
    if buf.len() < total {
        return Err(DecodeError::Incomplete { needed: total });
    }
    let rec = &buf[..total];
    let crc = u32::from_le_bytes([rec[4], rec[5], rec[6], rec[7]]);
    if crc32fast::hash(&rec[8..]) != crc {
        return Err(DecodeError::Corrupt("crc mismatch".into()));
    }
    let rd_u64 = |at: usize| u64::from_le_bytes(rec[at..at + 8].try_into().expect("8-byte slice"));
    let rd_u32 = |at: usize| u32::from_le_bytes(rec[at..at + 4].try_into().expect("4-byte slice"));
    let offset = rd_u64(8);
    let timestamp_ms = rd_u64(16);
    let key_len = rd_u32(24);
    let mut at = 28usize;
    let key = if key_len == NO_KEY {
        None
    } else {
        let k = key_len as usize;
        if at + k + 4 > total {
            return Err(DecodeError::Corrupt("key overruns record".into()));
        }
        let s = std::str::from_utf8(&rec[at..at + k])
            .map_err(|_| DecodeError::Corrupt("key is not utf-8".into()))?
            .to_owned();
        at += k;
        Some(s)
    };
    if at + 4 > total {
        return Err(DecodeError::Corrupt(
            "headers length overruns record".into(),
        ));
    }
    let hlen = rd_u32(at) as usize;
    at += 4;
    if at + hlen > total {
        return Err(DecodeError::Corrupt("headers overrun record".into()));
    }
    let headers = if hlen == 0 {
        BTreeMap::new()
    } else {
        postcard::from_bytes(&rec[at..at + hlen])
            .map_err(|e| DecodeError::Corrupt(format!("headers: {e}")))?
    };
    at += hlen;
    let payload = payload_of(&rec[at..total]);
    Ok((
        Record {
            offset,
            timestamp_ms,
            key,
            headers,
            payload,
        },
        total,
    ))
}

/// Reads only offset and timestamp of a record at the start of `buf` (CRC is verified).
pub fn decode_meta(buf: &[u8]) -> Result<(u64, u64, usize), DecodeError> {
    let total = peek_total_len(buf)?;
    if buf.len() < total {
        return Err(DecodeError::Incomplete { needed: total });
    }
    let rec = &buf[..total];
    let crc = u32::from_le_bytes([rec[4], rec[5], rec[6], rec[7]]);
    if crc32fast::hash(&rec[8..]) != crc {
        return Err(DecodeError::Corrupt("crc mismatch".into()));
    }
    let offset = u64::from_le_bytes(rec[8..16].try_into().expect("8-byte slice"));
    let ts = u64::from_le_bytes(rec[16..24].try_into().expect("8-byte slice"));
    Ok((offset, ts, total))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hdrs() -> BTreeMap<String, String> {
        let mut h = BTreeMap::new();
        h.insert("flux-trace-id".to_string(), "abc".to_string());
        h.insert("content-type".to_string(), "application/json".to_string());
        h
    }

    #[test]
    fn roundtrip_with_and_without_key_headers() {
        for (key, headers) in [
            (Some("k1"), hdrs()),
            (None, BTreeMap::new()),
            (Some(""), BTreeMap::new()),
        ] {
            let mut buf = Vec::new();
            let n = encode_into(&mut buf, 42, 1000, key, &headers, b"hello");
            assert_eq!(n, buf.len());
            assert_eq!(n, encoded_len(key, &headers, 5));
            let (rec, used) = decode(&buf).unwrap();
            assert_eq!(used, n);
            assert_eq!(rec.offset, 42);
            assert_eq!(rec.timestamp_ms, 1000);
            assert_eq!(rec.key.as_deref(), key);
            assert_eq!(rec.headers, headers);
            assert_eq!(&rec.payload[..], b"hello");
            assert_eq!(decode_meta(&buf).unwrap(), (42, 1000, n));
        }
    }

    #[test]
    fn detects_torn_and_corrupt() {
        let mut buf = Vec::new();
        encode_into(&mut buf, 1, 2, Some("k"), &hdrs(), b"payload");
        for cut in 0..buf.len() {
            assert!(
                matches!(decode(&buf[..cut]), Err(DecodeError::Incomplete { .. })),
                "cut {cut}"
            );
        }
        let mut bad = buf.clone();
        let last = bad.len() - 1;
        bad[last] ^= 0xff;
        assert!(matches!(decode(&bad), Err(DecodeError::Corrupt(_))));
        let mut zero = vec![0u8; 64];
        zero[0] = 0;
        assert!(matches!(decode(&zero), Err(DecodeError::Corrupt(_))));
    }
}
