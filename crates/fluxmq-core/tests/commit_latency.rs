//! Publish → durable latency micro-benchmark (ignored by default), used for the v0.9
//! "local numbers" in docs/design/v0.9-commit-path.md. Open loop: requests are issued at a
//! fixed rate (independent of acks, so a slow commit does not hide its own latency), spread
//! round-robin over `PARTITIONS` partitions, group commit (fsync before ack). Each request's
//! latency is measured from its *scheduled* send time to its ack (no coordinated omission).
//!
//! `cargo test -p fluxmq-core --release --test commit_latency -- --ignored --nocapture`
//! Env: `BENCH_RATE` msg/s (default 50000), `BENCH_BATCH` messages per request (default 10),
//! `BENCH_SIZE` payload bytes (default 1024), `BENCH_SECS` (default 10), `BENCH_PARTITIONS`
//! (default 8), `BENCH_PIPELINE=off` for the unpipelined writer (v0.8 behaviour),
//! `BENCH_DEPTH` pipeline depth, `BENCH_SLOW_MS` = extra delay injected into every fsync of
//! partition 0 (a slow partition; the others are reported separately).

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::*;
use fluxmq_core::Broker;
use fluxmq_proto::model::{ChannelMode, PublishMessage};
use parking_lot::Mutex;

fn env<T: std::str::FromStr>(k: &str, d: T) -> T {
    std::env::var(k)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(d)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn commit_latency() {
    let rate: u64 = env("BENCH_RATE", 50_000);
    let batch: u64 = env("BENCH_BATCH", 10);
    let size: usize = env("BENCH_SIZE", 1024);
    let secs: u64 = env("BENCH_SECS", 10);
    let partitions: u32 = env("BENCH_PARTITIONS", 8);
    let dir = tempfile::tempdir().unwrap();
    #[allow(unused_mut)]
    let mut cfg = config(dir.path());
    let pipeline = std::env::var("BENCH_PIPELINE").as_deref() != Ok("off");
    cfg.commit_pipeline = pipeline;
    let depth: usize = env("BENCH_DEPTH", 0);
    if depth > 0 {
        cfg.commit_pipeline_depth = depth;
    }
    let slow_ms: u64 = env("BENCH_SLOW_MS", 0);
    let b = Arc::new(Broker::open(cfg).await.unwrap());
    b.create_channel(channel("lat", ChannelMode::Stream, partitions))
        .unwrap();
    if slow_ms > 0 {
        b.debug_delay_fsyncs("lat", 0, Duration::from_millis(slow_ms))
            .unwrap();
    }
    let payload = Bytes::from(vec![b'x'; size]);
    let new_hist = || hdrhistogram::Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    let hist = Arc::new(Mutex::new(new_hist()));
    // Partition 0 alone (the slow one with `BENCH_SLOW_MS`).
    let hist0 = Arc::new(Mutex::new(new_hist()));
    let warmup = Duration::from_secs(1);
    let total = warmup + Duration::from_secs(secs);
    let reqs_per_sec = rate / batch;
    let interval = Duration::from_nanos(1_000_000_000 / reqs_per_sec);
    let start = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    let mut k: u64 = 0;
    let mut late_max = Duration::ZERO;
    loop {
        let at = start + interval * k as u32;
        if at.duration_since(start) >= total {
            break;
        }
        let now = Instant::now();
        if at > now {
            tokio::time::sleep_until(at.into()).await;
        } else {
            late_max = late_max.max(now - at);
        }
        let p = (k % u64::from(partitions)) as u32;
        let msgs: Vec<PublishMessage> = (0..batch)
            .map(|_| PublishMessage {
                key: None,
                headers: Default::default(),
                payload: payload.clone(),
            })
            .collect();
        let (b, hist) = (
            b.clone(),
            if p == 0 && slow_ms > 0 {
                hist0.clone()
            } else {
                hist.clone()
            },
        );
        let measured = at.duration_since(start) >= warmup;
        tasks.spawn(async move {
            b.publish_to_partition("lat", p, msgs).await.unwrap();
            if measured {
                let us = at.elapsed().as_micros() as u64;
                let _ = hist.lock().record(us.clamp(1, 60_000_000));
            }
        });
        while tasks.try_join_next().is_some() {}
        k += 1;
    }
    while tasks.join_next().await.is_some() {}
    let s = b.stats();
    let c = &s.channels[0];
    let h = hist.lock().clone();
    let ms = |v: u64| v as f64 / 1000.0;
    if slow_ms > 0 {
        let h0 = hist0.lock().clone();
        println!(
            "RESULT slow partition 0 (+{slow_ms} ms per fsync): p50={:.2} p99={:.2} ms; the other partitions below",
            ms(h0.value_at_quantile(0.5)),
            ms(h0.value_at_quantile(0.99)),
        );
    }
    println!(
        "RESULT pipeline={pipeline} depth={depth} rate={rate} batch={batch} size={size} partitions={partitions}: \
         client publish->ack p50={:.2} p90={:.2} p99={:.2} p99.9={:.2} max={:.2} ms (n={}) | \
         broker publish->durable p50={:.2} p99={:.2} ms | fsyncs={} fsync p50={:.2} p99={:.2} ms | \
         pacer late max {late_max:?}",
        ms(h.value_at_quantile(0.5)),
        ms(h.value_at_quantile(0.9)),
        ms(h.value_at_quantile(0.99)),
        ms(h.value_at_quantile(0.999)),
        ms(h.max()),
        h.len(),
        ms(c.publish_latency_us.value_at_quantile(0.5)),
        ms(c.publish_latency_us.value_at_quantile(0.99)),
        s.fsync_latency_us.len(),
        ms(s.fsync_latency_us.value_at_quantile(0.5)),
        ms(s.fsync_latency_us.value_at_quantile(0.99)),
    );
    b.shutdown().await;
}
