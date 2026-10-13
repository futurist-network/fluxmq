//! Live gateway load run (design §9): many WebSocket tail subscribers on one channel while a
//! producer publishes at a fixed rate. Ignored by default; run in release:
//!
//! ```text
//! LIVE_SUBS=5000 LIVE_RATE=2000 LIVE_SECS=20 \
//!   cargo test --release -p fluxmq-server --test live_load -- --ignored --nocapture
//! ```
//!
//! Prints delivered messages per subscriber (every subscriber must get every message),
//! end-to-end latency percentiles (publish time in the payload → receipt), broker RSS and
//! the gateway's metrics.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::TestServer;
use futures::StreamExt;
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

fn rss_mb() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse::<f64>().ok())
        })
        .map_or(0.0, |kb| kb / 1024.0)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "load run: cargo test --release -p fluxmq-server --test live_load -- --ignored --nocapture"]
async fn many_tail_subscribers() {
    let subs = env("LIVE_SUBS", 2000) as usize;
    let rate = env("LIVE_RATE", 1000);
    let secs = env("LIVE_SECS", 10);
    let srv = TestServer::start_with(|c| {
        c.live.max_connections = subs + 10;
        c.live.max_connections_per_channel = subs + 10;
    })
    .await;
    let r = srv
        .post_json(
            "/api/channels",
            json!({ "name": "load", "mode": "stream", "partitions": 4 }),
        )
        .await;
    assert!(r.status().is_success());
    let rss0 = rss_mb();

    let url = format!("ws://{}/api/channels/load/live", srv.http_addr);
    let received = Arc::new(AtomicU64::new(0));
    let lat = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let t_connect = std::time::Instant::now();
    let mut tasks = Vec::with_capacity(subs);
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel::<()>(subs);
    for i in 0..subs {
        let url = url.clone();
        let received = received.clone();
        let lat = lat.clone();
        let ready = ready_tx.clone();
        tasks.push(tokio::spawn(async move {
            let (mut ws, _) = tokio_tungstenite::connect_async(url)
                .await
                .expect("connect");
            let mut count = 0u64;
            let mut sample = Vec::new();
            loop {
                let Some(Ok(m)) = ws.next().await else { break };
                let Message::Text(t) = m else { continue };
                let v: Value = serde_json::from_str(&t).unwrap();
                match v["type"].as_str() {
                    Some("hello") => {
                        let _ = ready.send(()).await;
                    }
                    Some("messages") => {
                        let now = now_us();
                        for m in v["messages"].as_array().unwrap() {
                            count += 1;
                            // Latency sampled on 1 subscriber in 50 to keep the client light.
                            if i % 50 == 0 {
                                let p: Value =
                                    serde_json::from_str(m["payload"].as_str().unwrap()).unwrap();
                                sample.push(now.saturating_sub(p["t"].as_u64().unwrap()));
                            }
                        }
                        received.fetch_add(
                            v["messages"].as_array().unwrap().len() as u64,
                            Ordering::Relaxed,
                        );
                    }
                    _ => {}
                }
            }
            lat.lock().unwrap().extend(sample);
            count
        }));
    }
    drop(ready_tx);
    for _ in 0..subs {
        ready_rx.recv().await.expect("every subscriber says hello");
    }
    let connect_secs = t_connect.elapsed().as_secs_f64();
    let rss1 = rss_mb();

    // Producer: `rate` msg/s in 10 ms batches.
    let total = rate * secs;
    let per_tick = (rate / 100).max(1);
    let mut sent = 0u64;
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    let t0 = std::time::Instant::now();
    while sent < total {
        tick.tick().await;
        let n = per_tick.min(total - sent);
        let t = now_us();
        let msgs: Vec<Value> = (0..n)
            .map(|k| {
                json!({ "key": format!("k{}", (sent + k) % 64),
                        "payload": { "encoding": "utf8",
                                     "data": format!("{{\"t\":{t},\"i\":{}}}", sent + k) } })
            })
            .collect();
        let r = srv
            .post_json("/api/channels/load/publish", json!({ "messages": msgs }))
            .await;
        assert!(r.status().is_success());
        sent += n;
    }
    let publish_secs = t0.elapsed().as_secs_f64();
    let want = total * subs as u64;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while received.load(Ordering::Relaxed) < want && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let drain_secs = t0.elapsed().as_secs_f64() - publish_secs;
    let metrics = srv
        .req(reqwest::Method::GET, "/metrics")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let rss2 = rss_mb();
    srv.stop().await;
    let mut per_sub = Vec::new();
    for t in tasks {
        per_sub.push(t.await.unwrap());
    }
    let mut l = lat.lock().unwrap().clone();
    l.sort_unstable();
    let pct = |q: f64| {
        l.get(((l.len() as f64 - 1.0) * q) as usize)
            .copied()
            .unwrap_or(0) as f64
            / 1000.0
    };
    let complete = per_sub.iter().filter(|&&c| c == total).count();
    println!("live load: {subs} WS subscribers, {rate} msg/s for {secs} s ({total} messages)");
    println!("  connect + hello: {connect_secs:.2} s");
    println!(
        "  published in {publish_secs:.2} s, drained {drain_secs:.2} s after the last publish"
    );
    println!(
        "  delivered: {} of {} ({:.1} M msg/s fan-out); subscribers with every message: {complete}/{subs}",
        received.load(Ordering::Relaxed),
        want,
        want as f64 / publish_secs / 1e6
    );
    println!(
        "  latency publish→receipt (ms): p50 {:.1}  p99 {:.1}  p99.9 {:.1}  max {:.1}  (n={})",
        pct(0.5),
        pct(0.99),
        pct(0.999),
        pct(1.0),
        l.len()
    );
    println!(
        "  process RSS (broker + clients): {rss0:.0} MB → {rss1:.0} MB with idle subscribers → {rss2:.0} MB after"
    );
    for line in metrics.lines().filter(|l| l.starts_with("fluxmq_live_")) {
        println!("  {line}");
    }
    assert_eq!(complete, subs, "every subscriber received every message");
}
