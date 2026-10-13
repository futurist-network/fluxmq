//! Follower side connection to one leader node, shared by every partition fetcher that
//! follows a partition led by that node. Requests are multiplexed by id; a reader task routes
//! responses to the waiting caller. Once the connection fails every pending and future call
//! errors and the pool replaces the client.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::io::BufWriter;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::protocol::{Capabilities, PROTOCOL_VERSION, Request, Response, read_frame, write_frame};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("connection: {0}")]
    Conn(String),
    #[error("request timed out")]
    Timeout,
    #[error("handshake rejected: {0}")]
    Rejected(String),
}

type Pending = Arc<Mutex<Option<HashMap<u64, oneshot::Sender<(Response, Bytes)>>>>>;

pub struct PeerClient {
    pub addr: SocketAddr,
    /// What the leader declared in `HelloOk` (empty for a leader before v0.7).
    pub leader_caps: Capabilities,
    tx: mpsc::Sender<(u64, Request, Vec<u8>)>,
    pending: Pending,
    next_id: AtomicU64,
    dead: Arc<AtomicBool>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for PeerClient {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

impl PeerClient {
    pub async fn connect(
        addr: SocketAddr,
        node_id: &str,
        secret: Option<String>,
    ) -> Result<PeerClient, ClientError> {
        Self::connect_with(addr, node_id, secret, None).await
    }

    /// [`connect`](Self::connect), over cluster mutual TLS when `tls` is set.
    pub async fn connect_with(
        addr: SocketAddr,
        node_id: &str,
        secret: Option<String>,
        tls: Option<&fluxmq_tls::ClusterTls>,
    ) -> Result<PeerClient, ClientError> {
        let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| ClientError::Conn(format!("connect to {addr} timed out")))?
            .map_err(|e| ClientError::Conn(format!("connect to {addr}: {e}")))?;
        let stream = crate::transport::Stream::connect(stream, &addr.to_string(), tls)
            .await
            .map_err(|e| ClientError::Conn(format!("TLS handshake with {addr}: {e}")))?;
        let (mut rd, wr) = stream.into_split();
        let mut wr = BufWriter::with_capacity(64 * 1024, wr);
        let hello = Request::Hello {
            node_id: node_id.to_string(),
            version: PROTOCOL_VERSION,
            secret,
        };
        let conn = |e: crate::protocol::FrameError| ClientError::Conn(e.to_string());
        write_frame(&mut wr, 0, &hello, &Capabilities::local().encode())
            .await
            .map_err(conn)?;
        tokio::io::AsyncWriteExt::flush(&mut wr)
            .await
            .map_err(|e| ClientError::Conn(e.to_string()))?;
        let leader_caps =
            match tokio::time::timeout(CONNECT_TIMEOUT, read_frame::<_, Response>(&mut rd)).await {
                Ok(Ok(Some((_, Response::HelloOk { .. }, payload)))) => {
                    Capabilities::decode(&payload)
                }
                Ok(Ok(Some((_, Response::HelloErr(e), _)))) => {
                    return Err(ClientError::Rejected(e));
                }
                Ok(Ok(_)) => return Err(ClientError::Conn("unexpected handshake reply".into())),
                Ok(Err(e)) => return Err(conn(e)),
                Err(_) => return Err(ClientError::Timeout),
            };
        let pending: Pending = Arc::new(Mutex::new(Some(HashMap::new())));
        let dead = Arc::new(AtomicBool::new(false));
        let (tx, mut rx) = mpsc::channel::<(u64, Request, Vec<u8>)>(1024);
        let fail = {
            let pending = pending.clone();
            let dead = dead.clone();
            move || {
                dead.store(true, Ordering::Release);
                // Dropping the senders fails every waiting call.
                pending.lock().take();
            }
        };
        let fail_w = fail.clone();
        let writer = tokio::spawn(async move {
            while let Some((id, req, payload)) = rx.recv().await {
                if write_frame(&mut wr, id, &req, &payload).await.is_err() {
                    break;
                }
                // Coalesce whatever else is queued before flushing.
                while let Ok((id, req, payload)) = rx.try_recv() {
                    if write_frame(&mut wr, id, &req, &payload).await.is_err() {
                        fail_w();
                        return;
                    }
                }
                if tokio::io::AsyncWriteExt::flush(&mut wr).await.is_err() {
                    break;
                }
            }
            fail_w();
        });
        let pending_r = pending.clone();
        let reader = tokio::spawn(async move {
            while let Ok(Some((id, resp, payload))) = read_frame::<_, Response>(&mut rd).await {
                let waiter = pending_r.lock().as_mut().and_then(|m| m.remove(&id));
                if let Some(w) = waiter {
                    let _ = w.send((resp, payload));
                }
            }
            fail();
        });
        Ok(PeerClient {
            addr,
            leader_caps,
            tx,
            pending,
            next_id: AtomicU64::new(1),
            dead,
            tasks: vec![writer, reader],
        })
    }

    pub fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Acquire)
    }

    /// Sends `req` and waits for its response (payload = raw records of a fetch).
    pub async fn call(
        &self,
        req: Request,
        timeout: Duration,
    ) -> Result<(Response, Bytes), ClientError> {
        self.call_with(req, Vec::new(), timeout).await
    }

    /// Sends `req` without waiting for (or keeping) its response; the reader drops it.
    /// Ordered with every other request of this connection.
    pub async fn notify(&self, req: Request) -> Result<(), ClientError> {
        if self.is_dead() {
            return Err(ClientError::Conn("connection closed".into()));
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.tx
            .send((id, req, Vec::new()))
            .await
            .map_err(|_| ClientError::Conn("connection closed".into()))
    }

    /// [`call`](Self::call) with a request payload (a v0.8 fetch extension).
    pub async fn call_with(
        &self,
        req: Request,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Result<(Response, Bytes), ClientError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        {
            let mut p = self.pending.lock();
            match p.as_mut() {
                Some(m) => {
                    m.insert(id, tx);
                }
                None => return Err(ClientError::Conn("connection closed".into())),
            }
        }
        let forget = || {
            if let Some(m) = self.pending.lock().as_mut() {
                m.remove(&id);
            }
        };
        if self.tx.send((id, req, payload)).await.is_err() {
            forget();
            return Err(ClientError::Conn("connection closed".into()));
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(_)) => Err(ClientError::Conn("connection closed".into())),
            Err(_) => {
                forget();
                Err(ClientError::Timeout)
            }
        }
    }
}
