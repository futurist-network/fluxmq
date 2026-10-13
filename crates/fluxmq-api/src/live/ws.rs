//! WebSocket transport of the live gateway: several subscriptions per connection
//! (`subscribe` / `unsubscribe` ops), one writer draining a bounded frame queue, heartbeat,
//! idle timeout, close codes (design §1.4).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket, close_code};
use fluxmq_auth::Action;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior, timeout};

use super::envelope::{Frame, FrameData, Transport};
use super::handler::Admitted;
use super::params::ClientOp;
use super::sub::{self, Sub, SubEnd};
use super::{ChannelSlot, RateLimiter};
use crate::state::AppState;

pub const CLOSE_SLOW: u16 = 4008;
pub const CLOSE_UNAUTHORIZED: u16 = 4401;
pub const CLOSE_DELETED: u16 = 4404;
/// Longest subscription id.
const MAX_ID: usize = 64;

/// Running subscriptions of a connection; aborted on drop.
#[derive(Default)]
struct Subs(HashMap<String, JoinHandle<()>>);

impl Drop for Subs {
    fn drop(&mut self) {
        for h in self.0.values() {
            h.abort();
        }
    }
}

struct Conn {
    state: AppState,
    principal: fluxmq_auth::Principal,
    out: mpsc::Sender<Frame>,
    ends: mpsc::UnboundedSender<(String, SubEnd)>,
    rate: Arc<RateLimiter>,
    subs: Subs,
    url_channel: String,
}

impl Conn {
    fn spawn(
        &mut self,
        id: String,
        reader: fluxmq_core::LiveReader,
        filter: super::params::Filter,
        start: sub::Start,
        chan: ChannelSlot,
    ) {
        let feed = self.state.live.feed(&reader);
        let s = Sub::new(
            id.clone(),
            Transport::Ws,
            reader,
            self.state.live.clone(),
            filter,
            self.out.clone(),
            self.rate.clone(),
            &start,
        );
        let ends = self.ends.clone();
        let sub_id = id.clone();
        let h = tokio::spawn(async move {
            let end = s.run(feed, start.resets).await;
            drop(chan);
            let _ = ends.send((sub_id, end));
        });
        if let Some(old) = self.subs.0.insert(id, h) {
            old.abort();
        }
    }

    /// Handles one client command; returns replies to send right away.
    async fn command(&mut self, text: &str) -> Vec<serde_json::Value> {
        let op = match serde_json::from_str::<ClientOp>(text) {
            Ok(op) => op,
            Err(e) => {
                return vec![error(
                    None,
                    "bad_request",
                    &format!("malformed command: {e}"),
                )];
            }
        };
        match op {
            ClientOp::Ping => vec![json!({ "type": "pong" })],
            ClientOp::Unsubscribe { id } => {
                if let Some(h) = self.subs.0.remove(&id) {
                    h.abort();
                }
                Vec::new()
            }
            ClientOp::Subscribe(op) => {
                let id = op.id.clone();
                if id.is_empty() || id.len() > MAX_ID {
                    return vec![error(None, "bad_request", "id must be 1-64 characters")];
                }
                if self.subs.0.contains_key(&id) {
                    return vec![error(
                        Some(&id),
                        "bad_request",
                        "subscription id already in use",
                    )];
                }
                if self.subs.0.len() >= self.state.live.config().max_subscriptions {
                    self.state
                        .live
                        .metrics()
                        .reject(super::Reject::SubscriptionLimit);
                    return vec![error(
                        Some(&id),
                        "limit",
                        "subscription limit of this connection reached",
                    )];
                }
                let channel = op
                    .channel
                    .clone()
                    .unwrap_or_else(|| self.url_channel.clone());
                if let Err(e) = self
                    .state
                    .auth
                    .authorize(&self.principal, Action::Subscribe(&channel))
                {
                    self.state.live.metrics().reject(super::Reject::Forbidden);
                    return vec![error(Some(&id), "forbidden", &e.to_string())];
                }
                let spec = match op.into_spec() {
                    Ok(s) => s,
                    Err(e) => return vec![error(Some(&id), "bad_request", &e)],
                };
                let reader = match self.state.broker.live_reader(&channel) {
                    Ok(r) => r,
                    Err(e) => return vec![error(Some(&id), "not_found", &e.to_string())],
                };
                let start = match sub::resolve(&reader, &spec).await {
                    Ok(s) => s,
                    Err(e) => return vec![error(Some(&id), "bad_request", &e.to_string())],
                };
                let chan = match self.state.live.acquire_channel(&channel) {
                    Ok(c) => c,
                    Err(r) => {
                        return vec![error(
                            Some(&id),
                            "limit",
                            &format!("live {} reached", r.label()),
                        )];
                    }
                };
                self.spawn(id, reader, spec.filter, start, chan);
                Vec::new()
            }
        }
    }
}

fn error(sub: Option<&str>, code: &str, message: &str) -> serde_json::Value {
    let mut v = json!({ "type": "error", "code": code, "message": message });
    if let Some(s) = sub {
        v["sub"] = json!(s);
    }
    v
}

#[derive(Debug)]
enum End {
    ClientClosed,
    Shutdown,
    Idle,
    Slow,
    Gone,
    Deleted,
    Unauthorized(fluxmq_auth::SessionEnd),
}

pub async fn run(mut socket: WebSocket, state: AppState, a: Admitted) {
    let Admitted {
        principal,
        reader,
        spec,
        start,
        conn: _conn,
        chan,
        mut ended,
        cors_origin: _,
    } = a;
    let cfg = state.live.config().clone();
    let (out, mut frames) = mpsc::channel::<Frame>(cfg.send_buffer.max(1));
    let (ends, mut sub_ends) = mpsc::unbounded_channel();
    let mut conn = Conn {
        url_channel: reader.channel().to_string(),
        rate: Arc::new(RateLimiter::new(
            cfg.max_msgs_per_sec,
            cfg.max_bytes_per_sec,
        )),
        state: state.clone(),
        principal,
        out,
        ends,
        subs: Subs::default(),
    };
    conn.spawn("0".into(), reader, spec.filter, start, chan);

    let mut hb = tokio::time::interval_at(Instant::now() + cfg.heartbeat, cfg.heartbeat);
    hb.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_rx = Instant::now();
    let shutdown = state.shutdown.clone();
    let end = loop {
        tokio::select! {
            () = shutdown.cancelled() => break End::Shutdown,
            e = &mut ended => break End::Unauthorized(e),
            f = frames.recv() => {
                let Some(f) = f else { break End::Gone };
                let FrameData::Ws(text) = f.data else { continue };
                match timeout(cfg.send_timeout, socket.send(Message::Text(text))).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => break End::Gone,
                    Err(_) => break End::Slow,
                }
            }
            e = sub_ends.recv() => match e {
                Some((id, SubEnd::Slow)) => {
                    tracing::debug!(sub = %id, "live subscription too slow");
                    break End::Slow;
                }
                Some((id, SubEnd::Deleted)) => {
                    conn.subs.0.remove(&id);
                    if conn.subs.0.is_empty() {
                        break End::Deleted;
                    }
                    let v = error(Some(&id), "deleted", "the channel was deleted");
                    if send_json(&mut socket, &v, cfg.send_timeout).await.is_err() {
                        break End::Gone;
                    }
                }
                Some((id, SubEnd::Gone)) => {
                    conn.subs.0.remove(&id);
                }
                None => {}
            },
            incoming = socket.recv() => {
                last_rx = Instant::now();
                match incoming {
                    None | Some(Ok(Message::Close(_))) | Some(Err(_)) => break End::ClientClosed,
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                    Some(Ok(Message::Binary(_))) => {
                        let v = error(None, "bad_request", "binary frames are not supported; send JSON text");
                        if send_json(&mut socket, &v, cfg.send_timeout).await.is_err() {
                            break End::Gone;
                        }
                    }
                    Some(Ok(Message::Text(t))) => {
                        let mut failed = false;
                        for v in conn.command(&t).await {
                            if send_json(&mut socket, &v, cfg.send_timeout).await.is_err() {
                                failed = true;
                                break;
                            }
                        }
                        if failed {
                            break End::Gone;
                        }
                    }
                }
            }
            _ = hb.tick() => {
                if last_rx.elapsed() > cfg.idle_timeout {
                    break End::Idle;
                }
                let ping = socket.send(Message::Ping(Default::default()));
                if timeout(cfg.send_timeout, ping).await.map_or(true, |r| r.is_err()) {
                    break End::Slow;
                }
            }
        }
    };
    drop(conn);
    tracing::debug!(reason = ?end, "live ws connection ended");
    let close = match end {
        End::Shutdown => Some((close_code::AWAY, "server shutting down")),
        End::Idle => Some((close_code::POLICY, "idle timeout")),
        End::Slow => {
            state.live.metrics().slow(Transport::Ws);
            Some((CLOSE_SLOW, "slow"))
        }
        End::Deleted => Some((CLOSE_DELETED, "channel deleted")),
        End::Unauthorized(e) => Some((CLOSE_UNAUTHORIZED, e.reason())),
        End::ClientClosed | End::Gone => None,
    };
    if let Some((code, reason)) = close {
        let frame = CloseFrame {
            code,
            reason: Utf8Bytes::from_static(reason),
        };
        let _ = timeout(
            Duration::from_secs(1),
            socket.send(Message::Close(Some(frame))),
        )
        .await;
    }
}

async fn send_json(socket: &mut WebSocket, v: &serde_json::Value, t: Duration) -> Result<(), ()> {
    match timeout(t, socket.send(Message::Text(v.to_string().into()))).await {
        Ok(Ok(())) => Ok(()),
        _ => Err(()),
    }
}
