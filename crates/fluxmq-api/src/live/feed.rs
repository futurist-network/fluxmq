//! Shared fan-out of one channel (design §3.2): a pump task reads every newly visible range
//! once and broadcasts it as [`Batch`]es; any number of subscriptions share the batches (and
//! their encoded envelopes). The pump runs while at least one subscription holds the [`Feed`].

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use fluxmq_core::LiveReader;
use fluxmq_proto::Message;
use tokio::sync::broadcast;

use super::LiveConfig;
use super::envelope::envelope_json;

/// Records / payload bytes per storage read of the pump (and of a subscription's catch-up).
pub const READ_RECORDS: usize = 1024;
pub const READ_BYTES: usize = 1 << 20;

/// Newly visible messages of one partition: they cover `[from, next)` (the first may be above
/// `from` when retention removed the start meanwhile).
pub struct Batch {
    pub partition: u32,
    pub from: u64,
    pub next: u64,
    pub messages: Vec<Arc<Message>>,
    /// Envelope JSON per message, built once for every receiver.
    envelopes: OnceLock<Vec<String>>,
    max_payload: usize,
}

impl Batch {
    pub fn new(partition: u32, from: u64, messages: Vec<Arc<Message>>, max_payload: usize) -> Self {
        let next = messages.last().map_or(from, |m| m.offset + 1);
        Self {
            partition,
            from,
            next,
            messages,
            envelopes: OnceLock::new(),
            max_payload,
        }
    }

    /// Envelopes in message order (built on first use).
    pub fn envelopes(&self) -> &[String] {
        self.envelopes.get_or_init(|| {
            self.messages
                .iter()
                .map(|m| envelope_json(m, self.max_payload))
                .collect()
        })
    }
}

/// One channel's pump. Dropping the last `Arc` stops it.
pub struct Feed {
    tx: broadcast::Sender<Arc<Batch>>,
    closed: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Feed {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Feed {
    pub fn start(reader: LiveReader, cfg: &LiveConfig) -> Arc<Feed> {
        let (tx, _) = broadcast::channel(cfg.fanout_buffer.max(1));
        let closed = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn(pump(
            reader,
            tx.clone(),
            closed.clone(),
            cfg.max_payload_bytes,
        ));
        Arc::new(Feed { tx, closed, task })
    }

    /// Subscribe **before** catching up from storage, so nothing published meanwhile is
    /// missed (anything older than the first batch received is read from storage).
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Batch>> {
        self.tx.subscribe()
    }

    /// The channel was deleted (or the pump stopped): no more batches.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

/// Reads `[from, end)` of `p` (bounded), from the recent cache when possible, else from
/// storage on a blocking thread.
pub async fn read_range(
    reader: &LiveReader,
    p: u32,
    from: u64,
    max_records: usize,
    max_bytes: usize,
) -> std::io::Result<Vec<Arc<Message>>> {
    let cached = reader.read_cached(p, from, max_records, max_bytes);
    if !cached.is_empty() {
        return Ok(cached);
    }
    let r = reader.clone();
    tokio::task::spawn_blocking(move || r.read(p, from, max_records, max_bytes))
        .await
        .unwrap_or_else(|e| Err(std::io::Error::other(e)))
}

async fn pump(
    reader: LiveReader,
    tx: broadcast::Sender<Arc<Batch>>,
    closed: Arc<AtomicBool>,
    max_payload: usize,
) {
    let mut watch = reader.watch();
    let n = reader.partitions();
    // Frontier per partition: everything below it was broadcast (or predates the feed;
    // subscriptions read that from storage themselves). `None` until the partition is served.
    let mut frontier: Vec<Option<u64>> = (0..n).map(|p| reader.visible_end(p)).collect();
    loop {
        watch.mark_seen();
        if reader.is_closed() {
            break;
        }
        for p in 0..n {
            let Some(end) = reader.visible_end(p) else {
                continue;
            };
            let f = frontier[p as usize].get_or_insert(end);
            while *f < end {
                let msgs = match read_range(&reader, p, *f, READ_RECORDS, READ_BYTES).await {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::warn!(channel = reader.channel(), partition = p, error = %e, "live feed read failed");
                        break;
                    }
                };
                if msgs.is_empty() {
                    // Nothing readable below `end` (retention removed it): jump over.
                    *f = end.max(reader.low(p)).min(end);
                    break;
                }
                let batch = Batch::new(p, *f, msgs, max_payload);
                *f = batch.next;
                // No receiver is fine: subscriptions catch up from storage.
                let _ = tx.send(Arc::new(batch));
            }
        }
        watch.changed().await;
    }
    closed.store(true, Ordering::Release);
}
