//! At-least-once check across broker crashes.
//!
//! A producer publishes sequence numbers (keyed, so they spread over partitions) and records
//! every *acknowledged* publish; a consumer acks everything it receives. Kill and restart the
//! broker while this runs (`kill -9`, same data dir and port). At the end the program checks
//! that every acknowledged publish was consumed (duplicates are allowed and reported).
//!
//! ```text
//! cargo run -p fluxmq-client --example resilience -- 127.0.0.1:7420 60
//! # a cluster over TLS (comma-separated seeds; `tls://` or the CA variable enables TLS):
//! FLUXMQ_CLIENT_TLS_CA=certs/ca.pem FLUXMQ_CLIENT_TLS_CERT=certs/client.pem \
//! FLUXMQ_CLIENT_TLS_KEY=certs/client-key.pem RESILIENCE_PARTITIONS=6 \
//!   cargo run -p fluxmq-client --example resilience -- tls://127.0.0.1:17420,tls://127.0.0.1:17430 120
//! ```

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use fluxmq_client::{
    ChannelConfig, ChannelMode, Client, ClientOptions, ConnectionState, ReconnectPolicy, StartFrom,
    SubscribeOptions, message,
};
use parking_lot::Mutex;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,fluxmq_client=info".into()),
        )
        .init();
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:7420".into());
    let secs: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(30);
    let channel = format!("resilience-{}", fluxmq_proto::now_ms());
    let partitions: u32 = std::env::var("RESILIENCE_PARTITIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let seeds: Vec<String> = addr.split(',').map(|s| s.trim().to_string()).collect();
    let env = |k: &str| std::env::var_os(k).map(std::path::PathBuf::from);
    let tls = match (
        env("FLUXMQ_CLIENT_TLS_CA"),
        env("FLUXMQ_CLIENT_TLS_CERT"),
        env("FLUXMQ_CLIENT_TLS_KEY"),
    ) {
        (None, None, None) => None,
        (ca, cert, key) => {
            let mut t = fluxmq_client::TlsOptions::new();
            if let Some(ca) = ca {
                t = t.ca_file(ca);
            }
            if let (Some(c), Some(k)) = (cert, key) {
                t = t.identity_files(c, k);
            }
            Some(t)
        }
    };
    let opts = |name: &str| {
        let o = ClientOptions::with_seeds(seeds.clone(), name);
        let o = match &tls {
            Some(t) => o.tls(t.clone()),
            None => o,
        };
        o.reconnect(
            ReconnectPolicy::default()
                .backoff(Duration::from_millis(100), Duration::from_secs(1))
                .retry_initial_connect(true),
        )
        .publish_timeout(Duration::from_secs(60))
    };

    let admin = Client::connect(opts("resilience-admin")).await?;
    let mut cfg = ChannelConfig::new(&channel, ChannelMode::Stream);
    cfg.partitions = partitions;
    admin.ensure_channel(cfg).await?;
    println!(
        "channel {channel} on {addr} ({})",
        match admin.metadata() {
            Some(m) => format!("cluster of {}", m.nodes.len()),
            None => "single node".into(),
        }
    );

    let consumer_client = Client::connect(opts("resilience-consumer")).await?;
    let producer_client = Client::connect(opts("resilience-producer")).await?;
    for (name, c) in [
        ("consumer", &consumer_client),
        ("producer", &producer_client),
    ] {
        let mut st = c.state();
        tokio::spawn(async move {
            while st.changed().await.is_ok() {
                let s = *st.borrow_and_update();
                println!("[{name}] state → {s:?}");
                if s == ConnectionState::Closed {
                    break;
                }
            }
        });
    }

    let received = Arc::new(Mutex::new((HashSet::<u64>::new(), 0u64))); // (unique, total)
    let mut consumer = consumer_client
        .subscribe(
            &channel,
            "resilience",
            SubscribeOptions::default()
                .prefetch(256)
                .start(StartFrom::Earliest),
        )
        .await?;
    let rec = received.clone();
    let consumer_task = tokio::spawn(async move {
        while let Some(d) = consumer.next().await {
            let d = match d {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("consumer terminal error: {e}");
                    break;
                }
            };
            let n: u64 = std::str::from_utf8(d.payload())
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(u64::MAX);
            {
                let mut r = rec.lock();
                r.0.insert(n);
                r.1 += 1;
            }
            let _ = d.ack().await;
        }
    });

    let acked = Arc::new(Mutex::new(Vec::<u64>::new()));
    let failed = Arc::new(Mutex::new(0u64));
    let stop = tokio_util::sync::CancellationToken::new();
    let mut producers = Vec::new();
    for w in 0..4u64 {
        let producer = producer_client.producer(&channel);
        let acked = acked.clone();
        let failed = failed.clone();
        let stop = stop.clone();
        producers.push(tokio::spawn(async move {
            let mut seq = w;
            while !stop.is_cancelled() {
                let batch: Vec<_> = (0..10)
                    .map(|i| {
                        let n = seq + i * 4;
                        message(n.to_string()).key(format!("k{}", n % 97))
                    })
                    .collect();
                let nums: Vec<u64> = (0..10).map(|i| seq + i * 4).collect();
                seq += 40;
                match producer.send_batch(batch).await {
                    Ok(_) => acked.lock().extend(nums),
                    Err(e) => {
                        *failed.lock() += nums.len() as u64;
                        eprintln!("publish failed (not counted as acked): {e}");
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }));
    }

    for s in 1..=secs {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if s % 5 == 0 {
            let r = received.lock();
            println!(
                "t={s}s acked {} received {} unique / {} total, reconnects p={} c={}",
                acked.lock().len(),
                r.0.len(),
                r.1,
                producer_client.reconnects(),
                consumer_client.reconnects()
            );
        }
    }
    stop.cancel();
    for p in producers {
        let _ = p.await;
    }
    let acked: Vec<u64> = acked.lock().clone();
    // Drain.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let missing = {
            let r = received.lock();
            acked.iter().filter(|n| !r.0.contains(n)).count()
        };
        if missing == 0 || std::time::Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    consumer_task.abort();
    let r = received.lock();
    let missing: Vec<u64> = acked.iter().copied().filter(|n| !r.0.contains(n)).collect();
    println!("=== result ===");
    println!("acked publishes      {}", acked.len());
    println!("failed publishes     {}", *failed.lock());
    println!("consumed unique      {}", r.0.len());
    println!(
        "consumed total       {} ({} redeliveries/duplicates)",
        r.1,
        r.1 - r.0.len() as u64
    );
    println!(
        "reconnects           producer {}, consumer {}",
        producer_client.reconnects(),
        consumer_client.reconnects()
    );
    println!(
        "acked but missing    {} {:?}",
        missing.len(),
        &missing[..missing.len().min(20)]
    );
    if missing.is_empty() {
        println!("OK: no acknowledged publish was lost");
        Ok(())
    } else {
        anyhow::bail!("{} acknowledged publishes were lost", missing.len())
    }
}
