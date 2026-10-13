//! `GET /ws` — telemetry contract v1 gateway.
//!
//! Transport loop around [`fluxmq_telemetry::Session`]: send `hello`, then multiplex client
//! commands and session deltas. Malformed client frames produce a `ServerMsg::Error` and the
//! connection stays open. Slow or dead clients are dropped after `WsLimits::send_timeout`.
//! A session authenticated with a token closes with code [`CLOSE_UNAUTHORIZED`] (4401) when
//! that token is revoked (on any node) or expires (v0.5).

use std::collections::HashSet;
use std::time::Duration;

use axum::Extension;
use axum::extract::State;
use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket, WebSocketUpgrade, close_code};
use axum::response::Response;
use fluxmq_auth::{Principal, SessionEnd};
use fluxmq_proto::telemetry::{ClientMsg, ServerMsg};
use fluxmq_telemetry::Session;
use tokio::time::{Instant, MissedTickBehavior, timeout};

use crate::state::{AppState, WsLimits};

/// WS close code when the session's credential is revoked or expired.
pub const CLOSE_UNAUTHORIZED: u16 = 4401;

pub async fn upgrade(
    State(state): State<AppState>,
    principal: Option<Extension<Principal>>,
    ws: WebSocketUpgrade,
) -> Response {
    let max = state.ws.max_message_bytes;
    // Revocation / expiry watch of the authenticating token (never fires for the bootstrap
    // admin or open mode; `None` only when mounted without the auth middleware).
    let ended = principal.map(|Extension(p)| state.auth.session_watch(&p));
    ws.max_message_size(max)
        .max_frame_size(max)
        .on_upgrade(move |socket| async move {
            let session = state.telemetry.session();
            let ended = async move {
                match ended {
                    Some(f) => f.await,
                    None => std::future::pending().await,
                }
            };
            run(
                socket,
                session,
                state.ws.clone(),
                state.shutdown.clone(),
                ended,
            )
            .await;
        })
}

/// Why the loop ended.
#[derive(Debug)]
enum End {
    ClientClosed,
    HubClosed,
    Shutdown,
    Idle,
    SendFailed,
    /// The token was revoked / expired.
    Unauthorized(SessionEnd),
}

/// Tracks which topics this session holds so the per-session cap can be enforced.
#[derive(Default)]
struct Subscriptions {
    topics: HashSet<String>,
}

impl Subscriptions {
    /// Splits a subscribe request into topics to forward and rejection errors.
    fn admit(&mut self, topics: Vec<String>, max: usize) -> (Vec<String>, Vec<ServerMsg>) {
        let mut accepted = Vec::new();
        let mut rejected = Vec::new();
        for t in topics {
            if self.topics.contains(&t) || self.topics.len() < max {
                self.topics.insert(t.clone());
                accepted.push(t);
            } else {
                rejected.push(ServerMsg::Error {
                    topic: Some(t),
                    message: format!("subscription limit reached ({max} topics per connection)"),
                });
            }
        }
        (accepted, rejected)
    }

    fn remove(&mut self, topics: &[String]) {
        for t in topics {
            self.topics.remove(t);
        }
    }

    /// Topics the session rejected don't count against the cap.
    fn observe(&mut self, out: &[ServerMsg]) {
        for m in out {
            if let ServerMsg::Error { topic: Some(t), .. } = m {
                self.topics.remove(t);
            }
        }
    }
}

async fn run(
    mut socket: WebSocket,
    mut session: Session,
    limits: WsLimits,
    shutdown: tokio_util::sync::CancellationToken,
    ended: impl std::future::Future<Output = SessionEnd>,
) {
    let ended = std::pin::pin!(ended);
    let mut ended = ended;
    let mut subs = Subscriptions::default();
    let mut ping =
        tokio::time::interval_at(Instant::now() + limits.ping_interval, limits.ping_interval);
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_rx = Instant::now();

    let end = 'conn: {
        if send(&mut socket, &session.hello(), limits.send_timeout)
            .await
            .is_err()
        {
            break 'conn End::SendFailed;
        }
        loop {
            let outgoing: Vec<ServerMsg> = tokio::select! {
                () = shutdown.cancelled() => break 'conn End::Shutdown,
                end = &mut ended => break 'conn End::Unauthorized(end),
                incoming = socket.recv() => {
                    last_rx = Instant::now();
                    match incoming {
                        None | Some(Ok(Message::Close(_))) => break 'conn End::ClientClosed,
                        Some(Err(e)) => {
                            tracing::debug!(error = %e, "ws receive error");
                            break 'conn End::ClientClosed;
                        }
                        // tungstenite answers pings automatically; pongs just refresh liveness.
                        Some(Ok(Message::Ping(_) | Message::Pong(_))) => Vec::new(),
                        Some(Ok(Message::Binary(_))) => vec![error_msg("binary frames are not supported; send JSON text")],
                        Some(Ok(Message::Text(text))) => handle_text(&mut session, &mut subs, &limits, &text),
                    }
                }
                out = session.next() => match out {
                    Some(m) => vec![m],
                    None => break 'conn End::HubClosed,
                },
                _ = ping.tick() => {
                    if last_rx.elapsed() > limits.idle_timeout {
                        break 'conn End::Idle;
                    }
                    if timeout(limits.send_timeout, socket.send(Message::Ping(Default::default()))).await.map_or(true, |r| r.is_err()) {
                        break 'conn End::SendFailed;
                    }
                    Vec::new()
                }
            };
            for m in &outgoing {
                if send(&mut socket, m, limits.send_timeout).await.is_err() {
                    break 'conn End::SendFailed;
                }
            }
        }
    };

    tracing::debug!(reason = ?end, "ws session ended");
    let close = match end {
        End::Shutdown | End::HubClosed => Some((close_code::AWAY, "server shutting down")),
        End::Idle => Some((close_code::POLICY, "idle timeout")),
        End::Unauthorized(end) => Some((CLOSE_UNAUTHORIZED, end.reason())),
        End::ClientClosed | End::SendFailed => None,
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

fn handle_text(
    session: &mut Session,
    subs: &mut Subscriptions,
    limits: &WsLimits,
    text: &str,
) -> Vec<ServerMsg> {
    let msg = match serde_json::from_str::<ClientMsg>(text) {
        Ok(m) => m,
        Err(e) => return vec![error_msg(&format!("malformed message: {e}"))],
    };
    match msg {
        ClientMsg::Subscribe { topics } => {
            let (accepted, mut out) = subs.admit(topics, limits.max_subscriptions);
            if !accepted.is_empty() {
                let replies = session.handle(ClientMsg::Subscribe { topics: accepted });
                subs.observe(&replies);
                out.splice(0..0, replies);
            }
            out
        }
        ClientMsg::Unsubscribe { topics } => {
            subs.remove(&topics);
            session.handle(ClientMsg::Unsubscribe { topics })
        }
        ping @ ClientMsg::Ping { .. } => session.handle(ping),
    }
}

fn error_msg(message: &str) -> ServerMsg {
    ServerMsg::Error {
        topic: None,
        message: message.to_string(),
    }
}

async fn send(socket: &mut WebSocket, msg: &ServerMsg, send_timeout: Duration) -> Result<(), ()> {
    let text = match serde_json::to_string(msg) {
        Ok(t) => t,
        Err(e) => {
            tracing::error!(error = %e, "failed to serialize telemetry message");
            return Ok(());
        }
    };
    match timeout(send_timeout, socket.send(Message::Text(text.into()))).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => {
            tracing::debug!(error = %e, "ws send failed");
            Err(())
        }
        Err(_) => {
            tracing::info!("dropping slow websocket client (send timed out)");
            Err(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_cap() {
        let mut s = Subscriptions::default();
        let (a, r) = s.admit(vec!["a".into(), "b".into(), "c".into()], 2);
        assert_eq!(a, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(r.len(), 1);
        // Re-subscribing to a held topic is always allowed (seq-gap recovery).
        let (a, r) = s.admit(vec!["a".into()], 2);
        assert_eq!(a.len(), 1);
        assert!(r.is_empty());
        // Rejected-by-session topics free their slot.
        s.observe(&[ServerMsg::Error {
            topic: Some("b".into()),
            message: String::new(),
        }]);
        let (a, _) = s.admit(vec!["c".into()], 2);
        assert_eq!(a, vec!["c".to_string()]);
        s.remove(&["a".into(), "c".into()]);
        assert!(s.topics.is_empty());
    }
}
