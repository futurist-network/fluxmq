//! Server-Sent Events transport of the live gateway: one subscription per request, the
//! cursor as the event `id` (the browser sends it back as `Last-Event-ID` on reconnect),
//! comment heartbeats and a final `end` event (design §1.2).

use std::pin::Pin;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderValue, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::stream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, Interval, MissedTickBehavior};

use super::envelope::{Frame, FrameData, Transport, sse_event};
use super::handler::Admitted;
use super::sub::{Sub, SubEnd};
use super::{ConnSlot, RateLimiter};
use crate::state::AppState;

/// Everything the response body owns; dropping it (client gone) stops the subscription.
struct Stream {
    frames: mpsc::Receiver<Frame>,
    ends: mpsc::UnboundedReceiver<SubEnd>,
    heartbeat: Interval,
    ended: Pin<Box<dyn Future<Output = fluxmq_auth::SessionEnd> + Send>>,
    shutdown: tokio_util::sync::CancellationToken,
    task: JoinHandle<()>,
    state: AppState,
    _conn: ConnSlot,
    done: bool,
}

impl Drop for Stream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn end_event(reason: &str) -> Bytes {
    Bytes::from(sse_event(
        "end",
        None,
        &serde_json::json!({ "type": "end", "reason": reason }).to_string(),
    ))
}

impl Stream {
    async fn next(&mut self) -> Option<Bytes> {
        if self.done {
            return None;
        }
        let reason = tokio::select! {
            f = self.frames.recv() => match f {
                Some(Frame { data: FrameData::Sse(b), .. }) => return Some(b),
                Some(_) => return Some(Bytes::new()),
                None => "shutdown",
            },
            e = self.ends.recv() => match e {
                Some(SubEnd::Slow) => {
                    self.state.live.metrics().slow(Transport::Sse);
                    "slow"
                }
                Some(SubEnd::Deleted) => "deleted",
                Some(SubEnd::Gone) | None => "shutdown",
            },
            () = self.shutdown.cancelled() => "shutdown",
            _ = &mut self.ended => "revoked",
            _ = self.heartbeat.tick() => return Some(Bytes::from_static(b": hb\n\n")),
        };
        self.done = true;
        Some(end_event(reason))
    }
}

pub fn response(state: AppState, a: Admitted) -> Response {
    let Admitted {
        principal: _,
        reader,
        spec,
        start,
        conn,
        chan,
        ended,
        cors_origin,
    } = a;
    let cfg = state.live.config().clone();
    let (out, frames) = mpsc::channel::<Frame>(cfg.send_buffer.max(1));
    let (end_tx, ends) = mpsc::unbounded_channel();
    let feed = state.live.feed(&reader);
    let sub = Sub::new(
        "0".into(),
        Transport::Sse,
        reader,
        state.live.clone(),
        spec.filter,
        out,
        Arc::new(RateLimiter::new(
            cfg.max_msgs_per_sec,
            cfg.max_bytes_per_sec,
        )),
        &start,
    );
    let task = tokio::spawn(async move {
        let end = sub.run(feed, start.resets).await;
        drop(chan);
        let _ = end_tx.send(end);
    });
    let mut heartbeat = tokio::time::interval_at(Instant::now() + cfg.heartbeat, cfg.heartbeat);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let s = Stream {
        frames,
        ends,
        heartbeat,
        ended,
        shutdown: state.shutdown.clone(),
        task,
        state,
        _conn: conn,
        done: false,
    };
    let body = Body::from_stream(stream::unfold(s, |mut s| async move {
        s.next()
            .await
            .map(|b| (Ok::<_, std::convert::Infallible>(b), s))
    }));
    let mut resp = body.into_response();
    let h = resp.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    // Reverse proxies (nginx) must not buffer the stream.
    h.insert("x-accel-buffering", HeaderValue::from_static("no"));
    if let Some(o) = cors_origin.and_then(|o| HeaderValue::from_str(&o).ok()) {
        h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, o);
        h.insert(header::VARY, HeaderValue::from_static("Origin"));
    }
    resp
}
