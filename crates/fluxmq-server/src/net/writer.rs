//! Outgoing side of a client connection: frames encoded by [`OutboundEncoder`] as chunks
//! (scratch-buffer headers plus the stored envelope / payload buffers themselves) and written
//! with vectored writes, so envelope bytes are never copied in user space.
//!
//! Generic over [`AsyncWrite`]: plain TCP halves and TLS halves (tokio-rustls supports
//! vectored writes; rustls copies into its record buffer, which encryption needs anyway). A
//! writer without vectored support gets small chunks coalesced into one buffer first, so it
//! never sees a write per frame header.

use std::collections::VecDeque;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::AsyncWrite;

use super::codec::{FrameError, MAX_IOV, Outbound, OutboundEncoder};

/// Coalescing target for writers without vectored writes.
const COALESCE_BYTES: usize = 64 * 1024;

pub struct FrameWriter<W> {
    io: W,
    enc: OutboundEncoder,
    chunks: VecDeque<Bytes>,
    queued: usize,
    vectored: bool,
}

impl<W: AsyncWrite + Unpin> FrameWriter<W> {
    pub fn new(io: W) -> Self {
        let vectored = io.is_write_vectored();
        Self {
            io,
            enc: OutboundEncoder::new(),
            chunks: VecDeque::new(),
            queued: 0,
            vectored,
        }
    }

    /// Bytes encoded but not yet written.
    pub fn queued(&self) -> usize {
        self.queued
    }

    /// Encodes `frame` behind the queued ones (no I/O).
    pub fn push(&mut self, frame: Outbound) -> Result<(), FrameError> {
        self.queued += self.enc.encode(frame, &mut self.chunks)?;
        Ok(())
    }

    /// One write of the queued chunks (up to [`MAX_IOV`] slices).
    fn poll_write_some(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.vectored {
            self.coalesce_front();
        }
        let mut iov = [IoSlice::new(&[]); MAX_IOV];
        let mut k = 0;
        for c in self.chunks.iter().take(MAX_IOV) {
            iov[k] = IoSlice::new(c);
            k += 1;
        }
        let n = if self.vectored {
            ready!(Pin::new(&mut self.io).poll_write_vectored(cx, &iov[..k]))?
        } else {
            ready!(Pin::new(&mut self.io).poll_write(cx, &iov[0]))?
        };
        if n == 0 {
            return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
        }
        self.advance(n);
        Poll::Ready(Ok(()))
    }

    /// Drops `n` written bytes from the front of the queue.
    fn advance(&mut self, mut n: usize) {
        self.queued -= n;
        while n > 0 {
            let front = self.chunks.front_mut().expect("written bytes were queued");
            if front.len() <= n {
                n -= front.len();
                self.chunks.pop_front();
            } else {
                front.advance(n);
                n = 0;
            }
        }
    }

    /// Non-vectored writers: merge small leading chunks into one buffer (a copy, as before
    /// zero-copy encoding) so a write isn't spent on each frame header.
    fn coalesce_front(&mut self) {
        let small = |c: &Bytes| c.len() < COALESCE_BYTES;
        if self.chunks.len() < 2 || !self.chunks.front().is_some_and(small) {
            return;
        }
        let mut buf = BytesMut::with_capacity(COALESCE_BYTES);
        while let Some(c) = self.chunks.front() {
            if buf.len() + c.len() > COALESCE_BYTES && !buf.is_empty() {
                break;
            }
            buf.extend_from_slice(c);
            self.chunks.pop_front();
        }
        self.chunks.push_front(buf.freeze());
    }

    /// Writes until at most `limit` bytes are queued.
    pub fn poll_write_until(&mut self, cx: &mut Context<'_>, limit: usize) -> Poll<io::Result<()>> {
        while self.queued > limit {
            ready!(self.poll_write_some(cx))?;
        }
        Poll::Ready(Ok(()))
    }

    /// Writes everything queued and flushes the transport.
    pub fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.poll_write_until(cx, 0))?;
        Pin::new(&mut self.io).poll_flush(cx)
    }

    pub fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.poll_flush(cx))?;
        Pin::new(&mut self.io).poll_shutdown(cx)
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        std::future::poll_fn(|cx| self.poll_flush(cx)).await
    }

    pub async fn write_until(&mut self, limit: usize) -> io::Result<()> {
        std::future::poll_fn(|cx| self.poll_write_until(cx, limit)).await
    }

    pub async fn shutdown(&mut self) -> io::Result<()> {
        std::future::poll_fn(|cx| self.poll_shutdown(cx)).await
    }

    #[cfg(test)]
    pub fn into_inner(self) -> W {
        self.io
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fluxmq_core::SharedDelivery;
    use fluxmq_proto::wire::{self, ServerFrame, ServerResponse};

    use super::*;

    /// Accepts at most `max` bytes per write; optionally reports no vectored support.
    struct Trickle {
        out: Vec<u8>,
        max: usize,
        vectored: bool,
        writes: usize,
    }

    impl AsyncWrite for Trickle {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let n = buf.len().min(self.max);
            self.out.extend_from_slice(&buf[..n]);
            self.writes += 1;
            Poll::Ready(Ok(n))
        }
        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let mut left = self.max;
            let mut n = 0;
            for b in bufs {
                let take = b.len().min(left);
                self.out.extend_from_slice(&b[..take]);
                n += take;
                left -= take;
                if left == 0 {
                    break;
                }
            }
            self.writes += 1;
            Poll::Ready(Ok(n))
        }
        fn is_write_vectored(&self) -> bool {
            self.vectored
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn shared(id: u64, n: usize) -> SharedDelivery {
        SharedDelivery {
            delivery_id: id,
            attempt: 1 + (id % 3) as u32,
            message: Arc::new(fluxmq_proto::Message {
                partition: 2,
                offset: id,
                key: id.is_multiple_of(2).then(|| format!("k{id}")),
                headers: [("h".to_string(), "v".to_string())].into(),
                timestamp_ms: 1_700_000_000_000 + id,
                payload: Bytes::from(vec![id as u8; n]),
            }),
        }
    }

    /// (frame for the writer, equivalent reference frame).
    fn cases() -> Vec<(Outbound, ServerFrame)> {
        let mut v = Vec::new();
        for (n, size) in [
            (1usize, 10usize),
            (1, 5000),
            (3, 100),
            (50, 3000),
            (7, 2048),
        ] {
            let ds: Vec<SharedDelivery> =
                (0..n as u64).map(|i| shared(i * 977 + 3, size)).collect();
            let reference = if n == 1 {
                ServerFrame::Deliver {
                    subscription_id: 9,
                    delivery: ds[0].clone().into_wire(),
                }
            } else {
                ServerFrame::DeliverBatch {
                    subscription_id: 9,
                    deliveries: ds.iter().cloned().map(SharedDelivery::into_wire).collect(),
                }
            };
            v.push((
                Outbound::Deliveries {
                    subscription_id: 9,
                    deliveries: ds,
                },
                reference,
            ));
        }
        for len in [1usize, 255, 256, 70_000] {
            let f = ServerFrame::DeliverRecords {
                subscription_id: 300,
                partition: 7,
                first_delivery_id: u64::MAX / 3,
                from_offset: 1 << 40,
                count: 12345,
                envelopes: Bytes::from((0..len).map(|i| i as u8).collect::<Vec<_>>()),
            };
            v.push((Outbound::Frame(f.clone()), f));
        }
        let r = ServerFrame::Response {
            req_id: 5,
            result: Ok(ServerResponse::Pong),
        };
        v.push((Outbound::Frame(r.clone()), r));
        v
    }

    fn reference_stream(cases: &[(Outbound, ServerFrame)]) -> Vec<u8> {
        cases
            .iter()
            .flat_map(|(_, f)| wire::encode_frame(f).unwrap().to_vec())
            .collect()
    }

    fn take_cases() -> (Vec<Outbound>, Vec<u8>) {
        let c = cases();
        let expected = reference_stream(&c);
        (c.into_iter().map(|(o, _)| o).collect(), expected)
    }

    /// Byte-identical to the reference encoder for every frame kind, through vectored and
    /// non-vectored writers that accept only a few bytes per write.
    #[tokio::test]
    async fn stream_matches_reference_encoding() {
        for (max, vectored) in [(usize::MAX, true), (7, true), (1000, false), (3, false)] {
            let (frames, expected) = take_cases();
            let mut w = FrameWriter::new(Trickle {
                out: Vec::new(),
                max,
                vectored,
                writes: 0,
            });
            for f in frames {
                w.push(f).unwrap();
            }
            assert_eq!(w.queued(), expected.len());
            w.flush().await.unwrap();
            assert_eq!(w.queued(), 0);
            let t = w.into_inner();
            assert_eq!(t.out, expected, "max={max} vectored={vectored}");
        }
    }

    /// Envelope buffers are handed to the socket as they are (no copy), headers share one
    /// scratch allocation.
    #[test]
    fn envelopes_are_not_copied() {
        let env = Bytes::from(vec![1u8; 100_000]);
        let mut enc = OutboundEncoder::new();
        let mut out = VecDeque::new();
        for _ in 0..3 {
            enc.encode(
                Outbound::Frame(ServerFrame::DeliverRecords {
                    subscription_id: 1,
                    partition: 0,
                    first_delivery_id: 1,
                    from_offset: 0,
                    count: 10,
                    envelopes: env.clone(),
                }),
                &mut out,
            )
            .unwrap();
        }
        assert_eq!(out.len(), 6);
        for pair in out
            .as_slices()
            .0
            .chunks(2)
            .chain(out.as_slices().1.chunks(2))
        {
            if let [_, e] = pair {
                assert_eq!(e.as_ptr(), env.as_ptr());
            }
        }
    }

    /// Writes stop at the limit (backpressure) and resume.
    #[tokio::test]
    async fn write_until_limit() {
        let (frames, expected) = take_cases();
        let mut w = FrameWriter::new(Trickle {
            out: Vec::new(),
            max: 100,
            vectored: true,
            writes: 0,
        });
        for f in frames {
            w.push(f).unwrap();
        }
        w.write_until(1000).await.unwrap();
        assert!(w.queued() <= 1000 && w.queued() > 900);
        w.shutdown().await.unwrap();
        assert_eq!(w.into_inner().out, expected);
    }
}
