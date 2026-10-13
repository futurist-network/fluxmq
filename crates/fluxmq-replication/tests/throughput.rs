//! Replicated-path benchmark (ignored by default; small: ~100k x 100 B per replica):
//! `CARGO_INCREMENTAL=0 cargo test -p fluxmq-replication --release --test throughput -- --ignored --nocapture`
//!
//! Three brokers on one machine, 6 partitions with leaders spread round-robin, RF=3,
//! min_isr=2, acks=all (publish returns once the ISR holds the batch).

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::*;
use fluxmq_core::FsyncPolicy;
use fluxmq_proto::model::PublishMessage;

const TOTAL: usize = 100_000;
const PARTITIONS: u32 = 6;

async fn run(policy: FsyncPolicy, rf: usize, publishers: usize, batch: usize) {
    let n = Arc::new(cluster(rf, PARTITIONS, policy).await);
    let ctl: Vec<_> = n.iter().map(auto_commit).collect();
    let min_isr = rf.min(2) as u32;
    for p in 0..PARTITIONS {
        let l = p as usize % rf;
        let all: Vec<&Node> = n.iter().collect();
        n[l].lead(p, 1, &all, min_isr);
        for (i, f) in n.iter().enumerate() {
            if i != l {
                f.follow(p, 1, &n[l]);
            }
        }
    }
    wait_sync("ISRs full", || {
        (0..PARTITIONS).all(|p| n[p as usize % rf].status(p).isr.len() == rf)
    })
    .await;
    let payload = Bytes::from(vec![b'x'; 100]);
    let per = TOTAL.div_ceil(publishers);
    let start = Instant::now();
    let mut tasks = Vec::new();
    for t in 0..publishers {
        let n = n.clone();
        let payload = payload.clone();
        tasks.push(tokio::spawn(async move {
            let mut lat = Vec::new();
            let mut sent = 0;
            let mut i = t;
            while sent < per {
                let k = batch.min(per - sent);
                let p = (i % PARTITIONS as usize) as u32;
                i += 1;
                let msgs = (0..k)
                    .map(|_| PublishMessage {
                        key: None,
                        headers: Default::default(),
                        payload: payload.clone(),
                    })
                    .collect();
                let t0 = Instant::now();
                n[p as usize % rf]
                    .broker
                    .publish_to_partition(CH, p, msgs)
                    .await
                    .unwrap();
                lat.push(t0.elapsed());
                sent += k;
            }
            lat
        }));
    }
    let mut lat = Vec::new();
    for t in tasks {
        lat.extend(t.await.unwrap());
    }
    let el = start.elapsed();
    lat.sort_unstable();
    let q = |f: f64| lat[((lat.len() as f64 * f) as usize).min(lat.len() - 1)].as_micros();
    println!(
        "RF={rf} {policy:?} publishers={publishers} batch={batch}: {} msgs in {el:?} = {:.0} msg/s | ack latency p50={}us p99={}us",
        per * publishers,
        (per * publishers) as f64 / el.as_secs_f64(),
        q(0.5),
        q(0.99)
    );
    // Every replica converged on identical logs.
    for p in 0..PARTITIONS {
        let leo = n[p as usize % rf].leo(p);
        wait_sync("converged", || n.iter().all(|x| x.leo(p) == leo)).await;
    }
    for c in ctl {
        c.abort();
    }
    let n = Arc::try_unwrap(n).ok().expect("no other references");
    for node in n {
        node.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn replicated_throughput() {
    let gc = FsyncPolicy::GroupCommit {
        interval: Duration::from_millis(2),
    };
    if std::env::var("BENCH_ONLY").as_deref() == Ok("gc3") {
        run(gc, 3, 8, 100).await;
        return;
    }
    for (policy, publishers, batch) in [
        (FsyncPolicy::OsBuffered, 8, 100),
        (FsyncPolicy::OsBuffered, 64, 1),
        (gc, 8, 100),
        (gc, 64, 1),
    ] {
        run(policy, 1, publishers, batch).await;
        run(policy, 3, publishers, batch).await;
    }
}
