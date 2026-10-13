//! Release-mode sanity benchmark (ignored by default):
//! `cargo test -p fluxmq-core --release --test throughput -- --ignored --nocapture`

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::*;
use fluxmq_core::{Broker, FsyncPolicy};
use fluxmq_proto::model::{ChannelMode, PublishMessage, StartFrom};

const TOTAL: usize = 1_000_000;

async fn run(policy: FsyncPolicy, publishers: usize, batch: usize) {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.fsync = policy;
    let b = Arc::new(Broker::open(cfg).await.unwrap());
    b.create_channel(channel("bench", ChannelMode::Stream, 8))
        .unwrap();
    let payload = Bytes::from(vec![b'x'; 100]);
    let per = TOTAL.div_ceil(publishers);
    let total = per * publishers;
    let start = Instant::now();
    let mut tasks = Vec::new();
    for t in 0..publishers {
        let b = b.clone();
        let payload = payload.clone();
        tasks.push(tokio::spawn(async move {
            let mut sent = 0;
            while sent < per {
                let n = batch.min(per - sent);
                let msgs = (0..n)
                    .map(|i| PublishMessage {
                        key: Some(format!("k{}", (t * 31 + i) % 1000)),
                        headers: Default::default(),
                        payload: payload.clone(),
                    })
                    .collect();
                b.publish("bench", msgs).await.unwrap();
                sent += n;
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let el = start.elapsed();
    let s = b.stats();
    let c = &s.channels[0];
    let h = &c.publish_latency_us;
    println!(
        "{policy:?} publishers={publishers} batch={batch}: {total} msgs in {el:?} = {:.0} msg/s | publish p50={}us p99={}us | fsyncs={} fsync p99={}us",
        total as f64 / el.as_secs_f64(),
        h.value_at_quantile(0.5),
        h.value_at_quantile(0.99),
        s.fsync_latency_us.len(),
        s.fsync_latency_us.value_at_quantile(0.99),
    );
    // Consume everything once.
    let mut sub = b
        .subscribe("bench", "g", info("c", 1), 10_000, StartFrom::Earliest)
        .unwrap();
    let start = Instant::now();
    let mut got = 0;
    let mut acks = Vec::with_capacity(1000);
    while got < total {
        let d = match tokio::time::timeout(Duration::from_secs(2), sub.next()).await {
            Ok(d) => d.unwrap(),
            Err(_) => {
                let s = b.stats();
                let g = &s.channels[0].groups[0];
                let parts: Vec<_> = s.channels[0]
                    .partitions
                    .iter()
                    .map(|p| (p.low_offset, p.high_offset))
                    .collect();
                panic!(
                    "stall at {got}: lag={} in_flight={} retry={} inst_in_flight={} parts={parts:?}",
                    g.lag, g.in_flight, g.retry_pending, g.instances[0].in_flight
                );
            }
        };
        acks.push(d.delivery_id);
        got += 1;
        if std::env::var("BENCH_TRACE").is_ok() && got % 100_000 == 0 {
            println!("    {got} at {:?}", start.elapsed());
        }
        if acks.len() == 1000 {
            sub.ack(&acks).unwrap();
            acks.clear();
        }
    }
    println!(
        "  consume+ack: {total} msgs in {:?} = {:.0} msg/s",
        start.elapsed(),
        total as f64 / start.elapsed().as_secs_f64()
    );
    drop(sub);
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn throughput_1m_small_messages() {
    if let Ok(only) = std::env::var("BENCH_ONLY") {
        let (p, n, b) = match only.as_str() {
            "os1" => (FsyncPolicy::OsBuffered, 256, 1),
            "gc100" => (
                FsyncPolicy::GroupCommit {
                    interval: Duration::from_millis(2),
                },
                8,
                100,
            ),
            _ => (FsyncPolicy::OsBuffered, 8, 100),
        };
        run(p, n, b).await;
        return;
    }
    run(FsyncPolicy::OsBuffered, 8, 100).await;
    run(
        FsyncPolicy::GroupCommit {
            interval: Duration::from_millis(2),
        },
        8,
        100,
    )
    .await;
    run(FsyncPolicy::OsBuffered, 256, 1).await;
    run(
        FsyncPolicy::GroupCommit {
            interval: Duration::from_millis(2),
        },
        256,
        1,
    )
    .await;
}
