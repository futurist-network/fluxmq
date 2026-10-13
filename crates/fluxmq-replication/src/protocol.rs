//! Replication wire protocol (internal, node ⇄ node, one TCP connection per follower→leader
//! node pair, multiplexed by request id).
//!
//! Frame: `u32 BE body_len | u64 BE req_id | u32 BE header_len | header | payload` where
//! `header` is a postcard-encoded [`Request`] or [`Response`] and `payload` carries raw log
//! records (fetch responses only) so they are never re-encoded: the leader writes the bytes
//! read from its segment, the follower hands the received buffer to its writer.
//!
//! Session: the follower sends `Hello` (req 0) with its node id and the cluster secret; the
//! leader answers `HelloOk` or `HelloErr` and closes. Since v0.7 both carry [`Capabilities`]
//! in the frame payload (empty from older peers). Then any number of concurrent `Fetch` /
//! `EpochEndOffset` requests, each answered exactly once (responses may be out of order).
//!
//! v0.8 (`durable-progress`, declared by the leader in `HelloOk`): a follower fetches ahead of
//! its durable end. Its `Fetch` then carries a [`FetchExt`] in the frame payload (the durable
//! end; `fetch_offset` is only where to read from), and it reports later durability with
//! [`Request::Progress`]. Followers send neither to a leader that did not declare the
//! feature: a v0.7 leader takes `fetch_offset` for the durable end.

use bytes::{Buf, Bytes, BytesMut};
use fluxmq_core::{FetchError, StateCursor};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PROTOCOL_VERSION: u16 = 1;
/// Largest accepted frame body (fetch_max_bytes is capped below this).
pub const MAX_FRAME_LEN: usize = 128 * 1024 * 1024;
/// Largest accepted `Hello` (read before the peer is authenticated).
pub const MAX_HELLO_LEN: usize = 64 * 1024;
const FIXED: usize = 12;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Request {
    Hello {
        node_id: String,
        version: u16,
        secret: Option<String>,
    },
    Fetch(FetchReq),
    /// Kafka `OffsetForLeaderEpoch`: end offset of `requested` in the leader's history.
    EpochEndOffset {
        channel: String,
        partition: u32,
        leader_epoch: u64,
        requested: u64,
    },
    /// v0.8 (`durable-progress` leaders only): the follower's durable end of a partition
    /// moved. Applied by the leader in the order requests arrive on the connection.
    Progress(ProgressReq),
}

/// A follower's durable log end (everything below is fsynced on it).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProgressReq {
    pub channel: String,
    pub partition: u32,
    pub epoch: u64,
    pub durable_offset: u64,
}

/// v0.8: payload of a `Fetch` from a follower that fetches ahead (see the module docs).
/// Without it (`Fetch` from a v0.7 follower) the durable end is `fetch_offset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchExt {
    pub durable_offset: u64,
}

impl FetchExt {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("postcard encoding into Vec is infallible")
    }

    /// From a fetch payload: `None` when empty (a v0.7 follower).
    pub fn decode(payload: &[u8]) -> Result<Option<Self>, FrameError> {
        if payload.is_empty() {
            return Ok(None);
        }
        postcard::from_bytes(payload)
            .map(Some)
            .map_err(|e| FrameError::Malformed(format!("fetch extension: {e}")))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FetchReq {
    pub channel: String,
    pub partition: u32,
    pub epoch: u64,
    pub fetch_offset: u64,
    pub max_bytes: u32,
    pub max_wait_ms: u32,
    pub state: StateCursor,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FetchResp {
    pub epoch: u64,
    pub high_watermark: u64,
    pub log_start_offset: u64,
    pub log_end_offset: u64,
    /// First offset of the records in the payload (meaningful when `record_count > 0`).
    pub base_offset: u64,
    pub record_count: u64,
    pub epochs: Vec<(u64, u64)>,
    pub state: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Response {
    HelloOk {
        node_id: String,
    },
    HelloErr(String),
    Fetch(Result<FetchResp, FetchError>),
    EpochEndOffset(Result<(Option<u64>, u64), FetchError>),
    /// Answer to [`Request::Progress`] (v0.8).
    Progress(Result<(), FetchError>),
}

/// v0.7: what a peer supports, sent in the **payload** of `Hello` (follower) and `HelloOk`
/// (leader), postcard encoded. Peers before v0.7 send an empty payload and ignore the
/// payload they receive, so the exchange is compatible both ways without a protocol version
/// bump (a v0.6 leader rejects any `Hello` version other than 1).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    pub features: Vec<String>,
}

/// Feature: the peer reads and stores v0.7 batch envelopes. A leader serves envelopes only to
/// followers that declared it on their connection.
pub const FEATURE_BATCH_RECORDS: &str = "batch-records";

/// Feature (leader, v0.8): understands [`FetchExt`] and [`Request::Progress`], so followers
/// may fetch ahead of their durable end.
pub const FEATURE_DURABLE_PROGRESS: &str = "durable-progress";

impl Capabilities {
    /// This build's capabilities.
    pub fn local() -> Self {
        Self {
            features: vec![
                FEATURE_BATCH_RECORDS.to_string(),
                FEATURE_DURABLE_PROGRESS.to_string(),
            ],
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("postcard encoding into Vec is infallible")
    }

    /// From a handshake payload: empty (a peer before v0.7) or undecodable → no features.
    pub fn decode(payload: &[u8]) -> Self {
        if payload.is_empty() {
            return Self::default();
        }
        postcard::from_bytes(payload).unwrap_or_default()
    }

    pub fn batch_records(&self) -> bool {
        self.features.iter().any(|f| f == FEATURE_BATCH_RECORDS)
    }

    pub fn durable_progress(&self) -> bool {
        self.features.iter().any(|f| f == FEATURE_DURABLE_PROGRESS)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame of {0} bytes exceeds limit")]
    TooLarge(usize),
    #[error("malformed frame: {0}")]
    Malformed(String),
}

/// Writes one frame (header encoded here, payload written as is).
pub async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(
    w: &mut W,
    req_id: u64,
    header: &T,
    payload: &[u8],
) -> Result<(), FrameError> {
    let h = postcard::to_stdvec(header).map_err(|e| FrameError::Malformed(e.to_string()))?;
    let body = FIXED + h.len() + payload.len();
    if body > MAX_FRAME_LEN {
        return Err(FrameError::TooLarge(body));
    }
    let mut head = Vec::with_capacity(4 + FIXED + h.len());
    head.extend_from_slice(&(body as u32).to_be_bytes());
    head.extend_from_slice(&req_id.to_be_bytes());
    head.extend_from_slice(&(h.len() as u32).to_be_bytes());
    head.extend_from_slice(&h);
    w.write_all(&head).await?;
    if !payload.is_empty() {
        w.write_all(payload).await?;
    }
    Ok(())
}

/// Reads one frame: `(req_id, header, payload)`. `Ok(None)` on a clean EOF between frames.
pub async fn read_frame<R: AsyncRead + Unpin, T: for<'a> Deserialize<'a>>(
    r: &mut R,
) -> Result<Option<(u64, T, Bytes)>, FrameError> {
    read_frame_max(r, MAX_FRAME_LEN).await
}

/// [`read_frame`] with a smaller size limit (e.g. [`MAX_HELLO_LEN`] before authentication).
pub async fn read_frame_max<R: AsyncRead + Unpin, T: for<'a> Deserialize<'a>>(
    r: &mut R,
    max: usize,
) -> Result<Option<(u64, T, Bytes)>, FrameError> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let body = u32::from_be_bytes(len) as usize;
    if body > max {
        return Err(FrameError::TooLarge(body));
    }
    if body < FIXED {
        return Err(FrameError::Malformed("short frame".into()));
    }
    let mut buf = BytesMut::zeroed(body);
    r.read_exact(&mut buf).await?;
    let req_id = buf.get_u64();
    let hlen = buf.get_u32() as usize;
    if hlen > buf.len() {
        return Err(FrameError::Malformed("header overruns frame".into()));
    }
    let header = buf.split_to(hlen);
    let value: T =
        postcard::from_bytes(&header).map_err(|e| FrameError::Malformed(e.to_string()))?;
    Ok(Some((req_id, value, buf.freeze())))
}

/// Constant-time equality (length is not secret).
pub fn secret_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frame_roundtrip_with_payload() {
        let (mut a, mut b) = tokio::io::duplex(1 << 16);
        let resp = Response::Fetch(Ok(FetchResp {
            epoch: 3,
            high_watermark: 10,
            log_start_offset: 0,
            log_end_offset: 12,
            base_offset: 10,
            record_count: 2,
            epochs: vec![(1, 0), (3, 7)],
            state: Some(vec![1, 2, 3]),
        }));
        write_frame(&mut a, 42, &resp, b"raw-records")
            .await
            .unwrap();
        write_frame(&mut a, 7, &Response::HelloErr("x".into()), &[])
            .await
            .unwrap();
        drop(a);
        let (id, got, payload) = read_frame::<_, Response>(&mut b).await.unwrap().unwrap();
        assert_eq!((id, &got, &payload[..]), (42, &resp, &b"raw-records"[..]));
        let (id, _, payload) = read_frame::<_, Response>(&mut b).await.unwrap().unwrap();
        assert_eq!((id, payload.len()), (7, 0));
        assert!(read_frame::<_, Response>(&mut b).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn hello_limit_refuses_large_frames_before_reading_them() {
        // Only the length prefix: the frame is refused before its body is read or allocated.
        let (mut a, mut b) = tokio::io::duplex(64);
        a.write_all(&((MAX_HELLO_LEN + 1) as u32).to_be_bytes())
            .await
            .unwrap();
        let r = read_frame_max::<_, Response>(&mut b, MAX_HELLO_LEN).await;
        assert!(matches!(r, Err(FrameError::TooLarge(_))));
    }

    #[tokio::test]
    async fn capabilities_ride_in_the_hello_payload_compatibly() {
        let (mut a, mut b) = tokio::io::duplex(1 << 16);
        let hello = Request::Hello {
            node_id: "n2".into(),
            version: PROTOCOL_VERSION,
            secret: None,
        };
        write_frame(&mut a, 0, &hello, &Capabilities::local().encode())
            .await
            .unwrap();
        write_frame(&mut a, 0, &hello, &[]).await.unwrap();
        // The header decodes exactly as before v0.7; the payload is ignored by old peers.
        let (_, got, payload) = read_frame::<_, Request>(&mut b).await.unwrap().unwrap();
        assert_eq!(got, hello);
        assert!(Capabilities::decode(&payload).batch_records());
        let (_, _, payload) = read_frame::<_, Request>(&mut b).await.unwrap().unwrap();
        assert!(!Capabilities::decode(&payload).batch_records());
        assert!(!Capabilities::decode(b"\xff\xff\xff").batch_records());
    }

    #[test]
    fn v08_additions_keep_the_v07_wire_compatible() {
        // Appended enum variants: a v0.7 peer decodes every message it can receive from a
        // v0.8 peer exactly as before (followers send `Progress` / `FetchExt` only to
        // leaders that declared `durable-progress`).
        let fetch = Request::Fetch(FetchReq {
            channel: "c".into(),
            partition: 1,
            epoch: 2,
            fetch_offset: 3,
            max_bytes: 4,
            max_wait_ms: 5,
            state: StateCursor::default(),
        });
        assert_eq!(postcard::to_stdvec(&fetch).unwrap()[0], 1);
        let progress = Request::Progress(ProgressReq {
            channel: "c".into(),
            partition: 1,
            epoch: 2,
            durable_offset: 3,
        });
        assert_eq!(postcard::to_stdvec(&progress).unwrap()[0], 3);
        assert_eq!(
            postcard::to_stdvec(&Response::Progress(Ok(()))).unwrap()[0],
            4
        );
        let back: Request = postcard::from_bytes(&postcard::to_stdvec(&progress).unwrap()).unwrap();
        assert_eq!(back, progress);
        // The fetch extension rides in the payload; empty = a v0.7 follower.
        let ext = FetchExt { durable_offset: 77 };
        assert_eq!(FetchExt::decode(&ext.encode()).unwrap(), Some(ext));
        assert_eq!(FetchExt::decode(&[]).unwrap(), None);
        assert!(FetchExt::decode(b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff").is_err());
        // Capabilities: a v0.7 leader declares only batch records.
        assert!(Capabilities::local().durable_progress());
        let v07 = Capabilities {
            features: vec![FEATURE_BATCH_RECORDS.into()],
        };
        assert!(!Capabilities::decode(&v07.encode()).durable_progress());
    }

    #[test]
    fn secrets() {
        assert!(secret_eq("abc", "abc"));
        assert!(!secret_eq("abc", "abd"));
        assert!(!secret_eq("abc", "abcd"));
    }
}
