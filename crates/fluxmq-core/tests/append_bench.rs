//! Storage append benchmark (ignored by default), used for the v0.7 "Results — storage"
//! numbers. Closed loop: `producers` producers, each keeping `IN_FLIGHT` publish requests of
//! `BENCH_BATCH` (default 64) messages outstanding on its own partition, group commit (fsync
//! before ack). `BENCH_SIZES` / `BENCH_PRODUCERS` (comma lists) pick the cases.
//!
//! `cargo test -p fluxmq-core --release --test append_bench -- --ignored --nocapture`
//! Env: `BENCH_SECS` (default 5), `BENCH_FSYNC=os` for OS-buffered, `BENCH_MODE=batch` to
//! publish pre-encoded record batches (`Broker::publish_batch`, the v0.7 `PublishBatch`
//! path; the section is built once, as an SDK would build it client-side), `BENCH_PIPELINE=off`
//! / `BENCH_DEPTH` for the commit pipeline (v0.9).

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::*;
use fluxmq_core::{Broker, FsyncPolicy};
use fluxmq_proto::model::{ChannelMode, PublishMessage};

const IN_FLIGHT: usize = 8;
const PARTITIONS: u32 = 8;

fn cpu_secs() -> f64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: valid out pointer.
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

async fn run(size: usize, producers: usize) {
    let batch: usize = std::env::var("BENCH_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);
    let secs: u64 = std::env::var("BENCH_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    if std::env::var("BENCH_FSYNC").as_deref() == Ok("os") {
        cfg.fsync = FsyncPolicy::OsBuffered;
    }
    if std::env::var("BENCH_PIPELINE").as_deref() == Ok("off") {
        cfg.commit_pipeline = false;
    }
    if let Some(d) = std::env::var("BENCH_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        cfg.commit_pipeline_depth = d;
    }
    let b = Arc::new(Broker::open(cfg).await.unwrap());
    b.create_channel(channel("bench", ChannelMode::Stream, PARTITIONS))
        .unwrap();
    let payload = Bytes::from(vec![b'x'; size]);
    let batch_mode = std::env::var("BENCH_MODE").as_deref() == Ok("batch");
    let section = {
        let mut body = Vec::new();
        for i in 0..batch {
            fluxmq_proto::batch::encode_record(
                &mut body,
                i as u32,
                None,
                &Default::default(),
                &payload,
            );
        }
        fluxmq_proto::batch::RecordsSection {
            compression: fluxmq_proto::batch::Compression::None,
            record_count: batch as u32,
            body: Bytes::from(body),
        }
    };
    let stop = Arc::new(AtomicBool::new(false));
    let count = Arc::new(AtomicU64::new(0));
    // Warm-up then measure.
    let mut tasks = Vec::new();
    for t in 0..producers {
        for _ in 0..IN_FLIGHT {
            let (b, payload, stop, count) =
                (b.clone(), payload.clone(), stop.clone(), count.clone());
            let section = section.clone();
            tasks.push(tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    if batch_mode {
                        b.publish_batch("bench", Some(t as u32 % PARTITIONS), section.clone())
                            .await
                            .unwrap();
                        count.fetch_add(batch as u64, Ordering::Relaxed);
                        continue;
                    }
                    let msgs = (0..batch)
                        .map(|_| PublishMessage {
                            key: None,
                            headers: Default::default(),
                            payload: payload.clone(),
                        })
                        .collect();
                    b.publish_to_partition("bench", t as u32 % PARTITIONS, msgs)
                        .await
                        .unwrap();
                    count.fetch_add(batch as u64, Ordering::Relaxed);
                }
            }));
        }
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let (c0, cpu0, t0) = (count.load(Ordering::Relaxed), cpu_secs(), Instant::now());
    tokio::time::sleep(Duration::from_secs(secs)).await;
    let (c1, cpu1, el) = (count.load(Ordering::Relaxed), cpu_secs(), t0.elapsed());
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        t.await.unwrap();
    }
    let n = (c1 - c0) as f64;
    let st = b.stats();
    println!(
        "RESULT mode={} size={size} producers={producers}: {:.0} msg/s, {:.2} us CPU/msg, {:.1} MB/s, {} timed fsyncs, publish p50 {} us",
        if batch_mode { "batch" } else { "publish" },
        n / el.as_secs_f64(),
        (cpu1 - cpu0) * 1e6 / n,
        n * size as f64 / el.as_secs_f64() / 1e6,
        st.fsync_latency_us.len(),
        st.channels[0].publish_latency_us.value_at_quantile(0.5),
    );
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn append_bench() {
    let list = |k: &str, d: &str| -> Vec<usize> {
        std::env::var(k)
            .unwrap_or_else(|_| d.to_string())
            .split(',')
            .map(|x| x.parse().unwrap())
            .collect()
    };
    for size in list("BENCH_SIZES", "100,1024") {
        for producers in list("BENCH_PRODUCERS", "1,4") {
            run(size, producers).await;
        }
    }
}
