//! Live tail pumps: one per channel with ≥ 1 `channel.{name}.tail` subscriber (refcounted in
//! [`crate::hub::Shared::acquire`]/`release`).
//!
//! The pump reads sampled messages from the broker, batches them and flushes every 250 ms:
//! the topic document is `{sample_ratio, shown_rate, total_rate, messages: <this batch>}` and
//! the snapshot for new subscribers carries the last [`RECENT_CAPACITY`] messages instead.

use std::collections::VecDeque;
use std::sync::Weak;
use std::time::{Duration, Instant};

use fluxmq_proto::model::Message;
use fluxmq_proto::telemetry::{TailMessage, TailState};

use crate::hub::Shared;
use crate::rates::round2;
use crate::source::TailFeed;

pub const FLUSH_INTERVAL: Duration = Duration::from_millis(250);
pub const RECENT_CAPACITY: usize = 200;
pub const MAX_BATCH: usize = 200;
pub const PREVIEW_BYTES: usize = 4096;

/// Converts a stored message into its UI preview (UTF-8 lossy, ≤ 4 KiB).
pub fn to_tail_message(m: &Message) -> TailMessage {
    let truncated = m.payload.len() > PREVIEW_BYTES;
    let bytes = if truncated {
        &m.payload[..PREVIEW_BYTES]
    } else {
        &m.payload[..]
    };
    let mut preview = String::from_utf8_lossy(bytes).into_owned();
    if truncated {
        // A multi-byte character cut at the boundary shows up as U+FFFD; drop it.
        if preview.ends_with('\u{FFFD}') {
            preview.pop();
        }
    }
    TailMessage {
        partition: m.partition,
        offset: m.offset,
        key: m.key.clone(),
        timestamp_ms: m.timestamp_ms,
        headers: m.headers.clone(),
        size: u32::try_from(m.payload.len()).unwrap_or(u32::MAX),
        payload_preview: preview,
        truncated,
    }
}

/// Batch + recent buffer state, independent of I/O.
#[derive(Debug, Default)]
pub struct TailBuffer {
    batch: Vec<TailMessage>,
    recent: VecDeque<TailMessage>,
}

impl TailBuffer {
    pub fn push(&mut self, m: TailMessage) {
        if self.batch.len() >= MAX_BATCH {
            self.batch.remove(0);
        }
        self.batch.push(m);
    }

    /// Closes the current batch. `published` = messages published to the channel during
    /// `elapsed`. Returns `(delta document, snapshot document)`.
    pub fn flush(&mut self, published: u64, elapsed: Duration) -> (TailState, TailState) {
        let secs = elapsed.as_secs_f64().max(1e-3);
        let batch = std::mem::take(&mut self.batch);
        let shown = batch.len() as u64;
        let total = published.max(shown);
        let sample_ratio = if total == 0 {
            1.0
        } else {
            shown as f64 / total as f64
        };
        for m in &batch {
            if self.recent.len() >= RECENT_CAPACITY {
                self.recent.pop_front();
            }
            self.recent.push_back(m.clone());
        }
        let delta = TailState {
            sample_ratio: (sample_ratio * 1000.0).round() / 1000.0,
            shown_rate: round2(shown as f64 / secs),
            total_rate: round2(total as f64 / secs),
            messages: batch,
        };
        let snapshot = TailState {
            messages: self.recent.iter().cloned().collect(),
            ..delta.clone()
        };
        (delta, snapshot)
    }
}

/// Runs until the channel disappears, the hub is dropped, or the task is aborted (last
/// subscriber left / shutdown).
pub(crate) async fn run_pump(
    shared: Weak<Shared>,
    topic: String,
    mut feed: Box<dyn TailFeed>,
    every: Duration,
) {
    let mut buf = TailBuffer::default();
    let mut interval = tokio::time::interval(every);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    // Discard the count accumulated before the first window.
    let _ = feed.take_published_count();
    let mut last = Instant::now();
    loop {
        let ended = tokio::select! {
            m = feed.recv() => match m {
                Some(m) => {
                    buf.push(to_tail_message(&m));
                    continue;
                }
                None => true,
            },
            _ = interval.tick() => false,
        };
        let Some(shared) = shared.upgrade() else {
            return;
        };
        if shared.is_shutdown() {
            return;
        }
        let now = Instant::now();
        let (delta, snapshot) = buf.flush(feed.take_published_count(), now - last);
        last = now;
        match (
            serde_json::to_value(&delta),
            serde_json::to_value(&snapshot),
        ) {
            (Ok(d), Ok(s)) => {
                if shared.publish(&topic, d, Some(s)) {
                    shared.notify();
                }
            }
            (Err(e), _) | (_, Err(e)) => {
                tracing::error!(%topic, error = %e, "tail serialization failed")
            }
        }
        if ended {
            tracing::debug!(%topic, "tail feed ended");
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(offset: u64, payload: Vec<u8>) -> Message {
        Message {
            partition: 0,
            offset,
            key: Some("k".into()),
            headers: Default::default(),
            timestamp_ms: 1,
            payload: payload.into(),
        }
    }

    #[test]
    fn preview_truncates_utf8_safely() {
        let m = to_tail_message(&msg(1, b"hello".to_vec()));
        assert_eq!(m.payload_preview, "hello");
        assert!(!m.truncated);
        assert_eq!(m.size, 5);

        let mut big = vec![b'a'; PREVIEW_BYTES - 1];
        big.extend("é".as_bytes()); // straddles the 4 KiB boundary
        big.extend(b"tail");
        let m = to_tail_message(&msg(2, big.clone()));
        assert!(m.truncated);
        assert_eq!(m.payload_preview.len(), PREVIEW_BYTES - 1);
        assert_eq!(m.size as usize, big.len());

        let m = to_tail_message(&msg(3, vec![0xff, 0xfe]));
        assert_eq!(m.payload_preview, "\u{FFFD}\u{FFFD}");
    }

    #[test]
    fn flush_rates_and_recent_buffer() {
        let mut b = TailBuffer::default();
        for i in 0..10 {
            b.push(to_tail_message(&msg(i, b"x".to_vec())));
        }
        let (d, s) = b.flush(40, Duration::from_millis(250));
        assert_eq!(d.messages.len(), 10);
        assert_eq!(d.sample_ratio, 0.25);
        assert_eq!(d.shown_rate, 40.0);
        assert_eq!(d.total_rate, 160.0);
        assert_eq!(s.messages.len(), 10);

        for i in 10..300 {
            b.push(to_tail_message(&msg(i, b"x".to_vec())));
        }
        let (d, s) = b.flush(290, Duration::from_millis(250));
        assert_eq!(d.messages.len(), MAX_BATCH);
        assert_eq!(d.messages.last().map(|m| m.offset), Some(299));
        assert_eq!(s.messages.len(), RECENT_CAPACITY);
        assert_eq!(s.messages[0].offset, 100);

        let (d, _) = b.flush(0, Duration::from_millis(250));
        assert!(d.messages.is_empty());
        assert_eq!(d.sample_ratio, 1.0);
    }
}
