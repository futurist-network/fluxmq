//! Per-WebSocket session: subscriptions, snapshots, coalesced merge-patch deltas.
//!
//! **Latest-wins.** A session remembers, per topic, the document it last sent and its hub
//! version. [`Session::next`] compares that with the topic's current version and sends one merge
//! patch from "what this client has" to "now". A slow client never accumulates a queue: it simply
//! diffs against whatever it last received (intermediate states are skipped). Topics are served
//! round-robin so one busy topic cannot starve the others.
//!
//! **Seq.** `seq` is per session *and* per topic: the snapshot of a new subscription has
//! `seq = 1` and every following message on that topic increments it by one. Re-subscribing to a
//! topic the session already has sends a fresh snapshot and continues the counter. A client that
//! sees a gap re-subscribes.
//!
//! **Tail topics** are special: the snapshot carries the recent buffer while deltas replace the
//! `messages` array with the newest batch. A session that falls behind by more than one flush
//! skips the intermediate batches (sampled data, latest-wins).

use std::collections::BTreeMap;
use std::sync::Arc;

use fluxmq_proto::now_ms;
use fluxmq_proto::telemetry::topics::{self, Topic};
use fluxmq_proto::telemetry::{CONTRACT_VERSION, ClientMsg, ServerMsg};
use serde_json::Value;
use tokio::sync::watch;

use crate::hub::Shared;
use crate::patch::diff;

#[derive(Debug)]
struct Sub {
    /// Hub version of the document last sent (0 = nothing sent yet: next message is a snapshot).
    version: u64,
    sent: Arc<Value>,
    seq: u64,
}

/// Per-WebSocket state machine. The transport loop is:
///
/// ```ignore
/// send(session.hello());
/// loop {
///     select! {
///         msg = ws.recv() => for out in session.handle(parse(msg)?) { send(out) },
///         out = session.next() => match out { Some(m) => send(m), None => break },
///     }
/// }
/// ```
pub struct Session {
    shared: Arc<Shared>,
    subs: BTreeMap<String, Sub>,
    rx: watch::Receiver<u64>,
    /// Round-robin position (topic name to start after).
    cursor: Option<String>,
}

impl Session {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        let rx = shared.subscribe_notify();
        Self {
            shared,
            subs: BTreeMap::new(),
            rx,
            cursor: None,
        }
    }

    pub fn hello(&self) -> ServerMsg {
        ServerMsg::Hello {
            contract_version: CONTRACT_VERSION,
            broker_version: fluxmq_proto::VERSION.to_string(),
            node_id: self.shared.node_id.clone(),
            server_time_ms: now_ms(),
        }
    }

    /// Applies a client message. Subscribing returns the topic snapshot(s) immediately (or an
    /// `Error` for unknown topics/channels); unsubscribing returns nothing; ping returns pong.
    pub fn handle(&mut self, msg: ClientMsg) -> Vec<ServerMsg> {
        match msg {
            ClientMsg::Subscribe { topics } => {
                let mut out = Vec::new();
                for topic in topics {
                    if let Some(m) = self.subscribe(topic) {
                        out.push(m);
                    }
                }
                out
            }
            ClientMsg::Unsubscribe { topics } => {
                for topic in topics {
                    if self.subs.remove(&topic).is_some() {
                        self.shared.release(&topic);
                    }
                }
                Vec::new()
            }
            ClientMsg::Ping { nonce } => vec![ServerMsg::Pong {
                nonce,
                server_time_ms: now_ms(),
            }],
        }
    }

    fn subscribe(&mut self, topic: String) -> Option<ServerMsg> {
        let error = |message: &str| {
            Some(ServerMsg::Error {
                topic: Some(topic.clone()),
                message: message.to_string(),
            })
        };
        if self.shared.is_shutdown() {
            return error("telemetry is shutting down");
        }
        let Some(parsed) = topics::parse(&topic) else {
            return error("unknown topic");
        };
        if let Topic::Channel(name) | Topic::Consumers(name) | Topic::Tail(name) = &parsed
            && self.shared.source.channel(name).is_none()
        {
            return error("unknown channel");
        }
        if !self.subs.contains_key(&topic) {
            if let Err(e) = self.shared.acquire(&topic, &parsed) {
                return error(&e);
            }
            self.subs.insert(
                topic.clone(),
                Sub {
                    version: 0,
                    sent: Arc::new(Value::Null),
                    seq: 0,
                },
            );
        }
        self.shared.ensure_doc(&topic, &parsed);
        let state = self.shared.topic(&topic);
        let sub = self.subs.get_mut(&topic)?;
        match state {
            Some(t) => {
                sub.seq += 1;
                sub.version = t.version;
                sub.sent = t.doc.clone();
                let data = t.snapshot.as_deref().unwrap_or(&t.doc).clone();
                Some(ServerMsg::Snapshot {
                    topic,
                    seq: sub.seq,
                    ts: now_ms(),
                    data,
                })
            }
            // Not computed yet (before the first collector tick): the snapshot follows via next().
            None => {
                sub.version = 0;
                None
            }
        }
    }

    /// Next outgoing delta for a subscribed topic (coalesced, latest-wins). Cancel-safe.
    /// Returns `None` when the hub shuts down.
    pub async fn next(&mut self) -> Option<ServerMsg> {
        loop {
            if self.shared.is_shutdown() {
                return None;
            }
            self.rx.borrow_and_update();
            if let Some(msg) = self.poll_pending() {
                return Some(msg);
            }
            if self.rx.changed().await.is_err() {
                return None;
            }
        }
    }

    /// Synchronously produces the next pending message, if any.
    fn poll_pending(&mut self) -> Option<ServerMsg> {
        let shared = self.shared.clone();
        // Round-robin: topics after the cursor first, then the rest.
        let names: Vec<String> = match &self.cursor {
            Some(c) => self
                .subs
                .range::<String, _>((std::ops::Bound::Excluded(c), std::ops::Bound::Unbounded))
                .chain(self.subs.range::<String, _>(..=c))
                .map(|(k, _)| k.clone())
                .collect(),
            None => self.subs.keys().cloned().collect(),
        };
        shared.with_topics(|topics| {
            for name in names {
                let Some(t) = topics.get(&name) else { continue };
                let Some(sub) = self.subs.get_mut(&name) else {
                    continue;
                };
                if t.version == sub.version {
                    continue;
                }
                if sub.version == 0 {
                    sub.seq += 1;
                    sub.version = t.version;
                    sub.sent = t.doc.clone();
                    let data = t.snapshot.as_deref().unwrap_or(&t.doc).clone();
                    self.cursor = Some(name.clone());
                    return Some(ServerMsg::Snapshot {
                        topic: name,
                        seq: sub.seq,
                        ts: now_ms(),
                        data,
                    });
                }
                let patch = match &t.last_patch {
                    Some((from, p)) if *from == sub.version => Some((**p).clone()),
                    _ => diff(&sub.sent, &t.doc),
                };
                sub.version = t.version;
                sub.sent = t.doc.clone();
                let Some(data) = patch else { continue };
                sub.seq += 1;
                self.cursor = Some(name.clone());
                return Some(ServerMsg::Delta {
                    topic: name,
                    seq: sub.seq,
                    ts: now_ms(),
                    data,
                });
            }
            None
        })
    }

    #[cfg(test)]
    pub(crate) fn topics(&self) -> Vec<String> {
        self.subs.keys().cloned().collect()
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        for topic in std::mem::take(&mut self.subs).into_keys() {
            self.shared.release(&topic);
        }
    }
}
