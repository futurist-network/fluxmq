//! A logical connection to one broker node (or to "any seed" for the bootstrap connection),
//! kept alive by a supervisor task that reconnects with exponential backoff + jitter.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use fluxmq_proto::wire::ErrorCode;
use parking_lot::Mutex;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::client::Inner;
use crate::conn::{self, Conn};
use crate::error::{ClientError, Result};

/// Connection status of one node.
#[derive(Clone)]
pub(crate) enum Slot {
    /// Not connected. `attempts` = failed rounds since the last good connection, `ever` = this
    /// node was connected before.
    Down {
        attempts: u32,
        ever: bool,
        last_error: Option<String>,
    },
    Up(Arc<Conn>),
    /// Gave up (reconnect policy exhausted, auth failure, or client closed).
    Failed(ClientError),
}

pub(crate) struct NodeConn {
    /// `None` = the bootstrap connection (any seed / any known node).
    pub(crate) id: Option<String>,
    addrs: Mutex<Vec<String>>,
    cursor: AtomicUsize,
    slot: watch::Sender<Slot>,
    cancel: CancellationToken,
}

impl NodeConn {
    pub(crate) fn new(
        id: Option<String>,
        addrs: Vec<String>,
        cancel: CancellationToken,
        initial: Option<(Arc<Conn>, usize)>,
    ) -> Arc<Self> {
        let (slot, cursor) = match initial {
            Some((c, i)) => (Slot::Up(c), i),
            None => (
                Slot::Down {
                    attempts: 0,
                    ever: false,
                    last_error: None,
                },
                0,
            ),
        };
        Arc::new(Self {
            id,
            addrs: Mutex::new(addrs),
            cursor: AtomicUsize::new(cursor),
            slot: watch::Sender::new(slot),
            cancel,
        })
    }

    pub(crate) fn label(&self) -> String {
        match &self.id {
            Some(id) => format!("node {id}"),
            None => "bootstrap connection".into(),
        }
    }

    pub(crate) fn slot(&self) -> Slot {
        self.slot.borrow().clone()
    }

    pub(crate) fn watch(&self) -> watch::Receiver<Slot> {
        self.slot.subscribe()
    }

    /// The live connection, if any.
    pub(crate) fn current(&self) -> Option<Arc<Conn>> {
        match &*self.slot.borrow() {
            Slot::Up(c) if !c.is_dead() => Some(c.clone()),
            _ => None,
        }
    }

    pub(crate) fn is_failed(&self) -> bool {
        matches!(&*self.slot.borrow(), Slot::Failed(_))
    }

    pub(crate) fn set_addrs(&self, addrs: Vec<String>) {
        if !addrs.is_empty() {
            *self.addrs.lock() = addrs;
        }
    }

    /// Stops the supervisor (node left the cluster / client closed).
    pub(crate) fn stop(&self) {
        self.cancel.cancel();
    }

    fn current_addr(&self) -> (String, usize) {
        let addrs = self.addrs.lock();
        let n = addrs.len().max(1);
        let i = self.cursor.load(Ordering::Relaxed) % n;
        (addrs.get(i).cloned().unwrap_or_default(), n)
    }

    fn advance(&self) {
        self.cursor.fetch_add(1, Ordering::Relaxed);
    }

    /// Waits until the node is connected (or `deadline`). With `fail_fast`, gives up as soon as
    /// a full connect round failed instead of waiting for the reconnect loop.
    pub(crate) async fn get(&self, deadline: Instant, fail_fast: bool) -> Result<Arc<Conn>> {
        let mut rx = self.slot.subscribe();
        let mut last: Option<String> = None;
        loop {
            match &*rx.borrow_and_update() {
                Slot::Up(c) if !c.is_dead() => return Ok(c.clone()),
                Slot::Up(c) => last = c.closed_reason(),
                Slot::Failed(e) => return Err(e.clone()),
                Slot::Down {
                    last_error,
                    attempts,
                    ..
                } => {
                    if fail_fast && *attempts > 0 {
                        return Err(ClientError::Closed(format!(
                            "{} unreachable: {}",
                            self.label(),
                            last_error.as_deref().unwrap_or("connect failed")
                        )));
                    }
                    last.clone_from(last_error)
                }
            }
            match tokio::time::timeout_at(deadline, rx.changed()).await {
                Ok(Ok(())) => continue,
                Ok(Err(_)) => return Err(ClientError::Closed("client closed".into())),
                Err(_) => {
                    return Err(ClientError::Closed(format!(
                        "no connection to {} in time{}",
                        self.label(),
                        last.map(|l| format!(" (last error: {l})"))
                            .unwrap_or_default()
                    )));
                }
            }
        }
    }
}

/// Handshake failures that reconnecting cannot fix.
fn is_terminal(e: &ClientError) -> bool {
    matches!(
        e.code(),
        Some(
            ErrorCode::Unauthorized
                | ErrorCode::Forbidden
                | ErrorCode::ProtocolViolation
                | ErrorCode::BadRequest
        )
    )
}

/// Keeps `node` connected until it is stopped or the reconnect policy gives up.
pub(crate) async fn supervise(node: Arc<NodeConn>, inner: Arc<Inner>) {
    let policy = inner.opts.reconnect.clone();
    let mut ever = node.current().is_some();
    let mut attempts: u32 = 0;
    let mut round_pos: usize = 0;
    let stop_reason = |inner: &Inner| {
        inner
            .closed_reason()
            .unwrap_or_else(|| "closed by client".into())
    };
    loop {
        if let Some(c) = node.current() {
            tokio::select! {
                _ = c.dead() => {}
                _ = node.cancel.cancelled() => {
                    let reason = stop_reason(&inner);
                    c.fail(reason.clone());
                    node.slot.send_replace(Slot::Failed(ClientError::Closed(reason)));
                    break;
                }
            }
            let reason = c
                .closed_reason()
                .unwrap_or_else(|| "connection closed".into());
            if node.cancel.is_cancelled() {
                let r = stop_reason(&inner);
                node.slot.send_replace(Slot::Failed(ClientError::Closed(r)));
                break;
            }
            attempts = 0;
            round_pos = 0;
            if policy.exhausted(0) {
                tracing::debug!(node = %node.label(), %reason, "connection lost; reconnect disabled");
                node.slot
                    .send_replace(Slot::Failed(ClientError::Closed(reason)));
                inner.on_slot_change();
                break;
            }
            tracing::warn!(node = %node.label(), addr = %c.addr, %reason, "fluxmq connection lost; reconnecting");
            node.slot.send_replace(Slot::Down {
                attempts: 0,
                ever: true,
                last_error: Some(reason),
            });
            inner.on_slot_change();
            inner.signals.request_refresh();
            // Short pause so a broker that is shutting down isn't hammered.
            if !sleep_or_cancel(&node.cancel, policy.delay(0)).await {
                continue; // loop top handles cancellation via the Failed branch below
            }
        }
        if node.cancel.is_cancelled() {
            let r = stop_reason(&inner);
            node.slot.send_replace(Slot::Failed(ClientError::Closed(r)));
            break;
        }
        let (addr, n_addrs) = node.current_addr();
        let res = tokio::select! {
            r = conn::connect(&addr, inner.opts.clone(), inner.signals.clone()) => r,
            _ = node.cancel.cancelled() => continue,
        };
        match res {
            Ok(c) => {
                if let Some(id) = &node.id
                    && c.node_id() != id
                {
                    tracing::warn!(expected = %id, got = %c.node_id(), %addr, "node id mismatch");
                }
                let reconnected = ever;
                ever = true;
                attempts = 0;
                round_pos = 0;
                node.slot.send_replace(Slot::Up(c.clone()));
                inner.on_connected(&node, &c, reconnected);
            }
            Err(e) if is_terminal(&e) => {
                tracing::error!(node = %node.label(), %addr, error = %e, "fluxmq handshake rejected; giving up");
                node.slot.send_replace(Slot::Failed(e));
                inner.on_slot_change();
                break;
            }
            Err(e) => {
                tracing::debug!(node = %node.label(), %addr, error = %e, "connect attempt failed");
                node.advance();
                round_pos += 1;
                if round_pos < n_addrs {
                    // Try the next address right away; back off once per full round.
                    node.slot.send_replace(Slot::Down {
                        attempts,
                        ever,
                        last_error: Some(e.to_string()),
                    });
                    continue;
                }
                round_pos = 0;
                attempts += 1;
                if policy.exhausted(attempts) {
                    let err = ClientError::Closed(format!(
                        "gave up reconnecting to {} after {attempts} attempts: {e}",
                        node.label()
                    ));
                    node.slot.send_replace(Slot::Failed(err));
                    inner.on_slot_change();
                    break;
                }
                node.slot.send_replace(Slot::Down {
                    attempts,
                    ever,
                    last_error: Some(e.to_string()),
                });
                inner.on_slot_change();
                sleep_or_cancel(&node.cancel, policy.delay(attempts - 1)).await;
            }
        }
    }
}

/// `false` if cancelled.
async fn sleep_or_cancel(cancel: &CancellationToken, d: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(d) => true,
        _ = cancel.cancelled() => false,
    }
}
