//! FluxMQ via the Rust SDK (`fluxmq-client`): `BatchingProducer` per producer connection,
//! one consumer group, per-message ack.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use fluxmq_client::{
    AckMode, BatchingOptions, ChannelConfig, ChannelMode, Client, ClientOptions, Compression,
    PublishMessage, StartFrom, SubscribeOptions, TlsOptions,
};
use futures::FutureExt;
use serde_json::json;

use crate::{Args, Audit, Bits, Shared, Started, payload_id, producer_loop};

fn options(args: &Args, name: String) -> anyhow::Result<ClientOptions> {
    let mut o = ClientOptions::new(args.brokers[0].clone(), name);
    if args.brokers.len() > 1 {
        o = o.seeds(args.brokers.clone());
    }
    o.tls = TlsOptions::from_files(
        args.tls_ca.clone(),
        args.tls_cert.clone(),
        args.tls_key.clone(),
    )
    .map_err(anyhow::Error::msg)?;
    o.request_timeout = Duration::from_secs(30);
    o.publish_timeout = Duration::from_secs(30);
    // Kafka and RabbitMQ send no per-message producer header; keep the payload comparable.
    o.stamp_producer = false;
    o.compression = compression(args)?;
    Ok(o)
}

fn compression(args: &Args) -> anyhow::Result<Compression> {
    Ok(match args.fluxmq_compression.as_str() {
        "none" => Compression::None,
        "lz4" => Compression::Lz4,
        "zstd" => Compression::Zstd,
        other => anyhow::bail!("--fluxmq-compression {other}: expected none | lz4 | zstd"),
    })
}

fn ack_mode(args: &Args) -> anyhow::Result<AckMode> {
    Ok(match args.fluxmq_ack_mode.as_str() {
        "individual" => AckMode::Individual,
        "cumulative" => AckMode::Cumulative,
        other => anyhow::bail!("--fluxmq-ack-mode {other}: expected individual | cumulative"),
    })
}

pub async fn start(args: &Args, topic: &str, shared: Arc<Shared>) -> anyhow::Result<Started> {
    let admin = Client::connect(options(args, "mq-harness-admin".into())?)
        .await
        .context("connect")?;
    let mut cfg = ChannelConfig::new(topic, ChannelMode::Stream);
    cfg.partitions = args.partitions.max(1);
    cfg.retention.max_bytes = Some(args.retention_bytes * args.partitions.max(1) as u64);
    cfg.retention.max_age_secs = Some(3600);
    admin.ensure_channel(cfg).await.context("create channel")?;
    let broker_version = admin.server_info().broker_version.clone();
    let topology = match admin.metadata() {
        Some(m) if m.nodes.len() > 1 => format!("cluster of {} nodes", m.nodes.len()),
        Some(_) => "single node".into(),
        None => "single node".into(),
    };

    let group = format!("{topic}-g");
    let mode = ack_mode(args)?;
    let mut consumers = Vec::new();
    for i in 0..args.consumers {
        let client = Client::connect(options(args, format!("mq-harness-consumer-{i}"))?).await?;
        let sub = SubscribeOptions::default()
            .prefetch(args.prefetch)
            .start(StartFrom::Earliest)
            .ack_mode(mode)
            .raw_records(args.fluxmq_raw_records)
            .verify_crc(args.fluxmq_verify_crc);
        let subscribe = {
            let client = client.clone();
            let topic = topic.to_string();
            let group = group.clone();
            async move { client.subscribe(&topic, &group, sub).await }
        };
        // Without --preload subscribe now (setup errors surface here); with it, after the gate.
        let mut early = if args.preload == 0 {
            Some(subscribe.await.context("subscribe")?)
        } else {
            None
        };
        let topic_s = topic.to_string();
        let group_s = group.clone();
        let rec = shared.recorder();
        let shared = shared.clone();
        let faults = args.self_test_faults && i == 0;
        consumers.push(tokio::spawn(async move {
            shared.consume_gate().await;
            let mut consumer = match early.take() {
                Some(c) => c,
                None => match client.subscribe(&topic_s, &group_s, sub).await {
                    Ok(c) => c,
                    Err(e) => {
                        shared.error(format!("subscribe: {e}"));
                        return;
                    }
                },
            };
            let mut n = 0u64;
            while let Some(d) = consumer.next().await {
                match d {
                    Ok(d) => {
                        if faults && shared.in_window(shared.now_us()) {
                            n += 1;
                        }
                        if faults && n == 1000 {
                            continue; // dropped unacked: redelivered after the ack timeout
                        }
                        if faults && n == 2000 {
                            let _ = d.nack("harness self-test", false).await; // dead-lettered
                            continue;
                        }
                        rec.record_ext(d.payload(), || {
                            let id = d.id();
                            json!({"partition": id.partition, "offset": id.offset,
                                   "attempt": d.attempt(), "node": d.node_id()})
                        });
                        if d.attempt() > 1 {
                            rec.redelivered();
                        }
                        if let Err(e) = d.ack().await {
                            shared.error(format!("ack: {e}"));
                        }
                    }
                    Err(e) => {
                        shared.error(format!("consume: {e}"));
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
            drop(client);
        }));
    }

    let mut producers = Vec::new();
    for p in 0..args.producers {
        let client = Client::connect(options(args, format!("mq-harness-producer-{p}"))?).await?;
        let batching = BatchingOptions {
            linger: Duration::from_millis(args.linger_ms),
            queue_capacity: args.window.max(1024),
            max_in_flight: args.max_in_flight.max(1),
            ..BatchingOptions::default()
        };
        let producer = Arc::new(client.batching_producer(topic, batching));
        let shared = shared.clone();
        let args = args.clone();
        producers.push(tokio::spawn(async move {
            let _client = client;
            producer_loop(shared, args, p as u32, move |payload, _k| {
                let producer = producer.clone();
                async move {
                    let msg = PublishMessage {
                        key: None,
                        headers: Default::default(),
                        payload: payload.into(),
                    };
                    let fut = producer.send(msg).await?;
                    Ok(async move { fut.await.map(|_| ()).map_err(anyhow::Error::from) }.boxed())
                }
                .boxed()
            })
            .await;
        }));
    }

    let cleanup = {
        let admin = admin.clone();
        async move {
            // The SDK has no channel delete; the runner deletes the channel over REST.
            drop(admin);
            Ok(())
        }
        .boxed()
    };

    let audit: Audit = {
        let args = args.clone();
        let topic = topic.to_string();
        Box::new(move |missing| audit(args, topic, missing).boxed())
    };

    Ok(Started {
        producers,
        consumers,
        durability: format!(
            "FluxMQ {topology}: stream channel, {} partitions; publish acked after {} and, in a \
             cluster, after every ISR member has the batch (acks=all; min ISR per broker flag); \
             consumer acks every message",
            args.partitions,
            if args.fluxmq_fsync == "os" {
                "write(2) without fsync (FLUXMQ_FSYNC=os: the OS flushes; durability parity \
                 with Kafka defaults)"
            } else {
                "the broker's group commit (fsync)"
            }
        ),
        client: format!(
            "fluxmq-client (Rust SDK) BatchingProducer linger={}ms, max_in_flight={}, prefetch={}, \
             stamp_producer=false, compression={}, ack_mode={}, raw_records={}, verify_crc={}",
            args.linger_ms, args.max_in_flight, args.prefetch, args.fluxmq_compression,
            args.fluxmq_ack_mode, args.fluxmq_raw_records, args.fluxmq_verify_crc
        ),
        broker_version: Some(broker_version),
        cleanup: Some(cleanup),
        audit: Some(audit),
    })
}

/// Re-reads the channel from the earliest retained offset with a fresh consumer group and
/// looks for the missing `(producer, seq)` ids: found = stored (and committed below the HW)
/// but never delivered to the run's group; not found = not in the log (lost, or removed by
/// retention: `low_offsets` shows where each partition starts).
async fn audit(args: Args, topic: String, want: Vec<Bits>) -> serde_json::Value {
    let want_n: u64 = want.iter().map(Bits::count).sum();
    let run = async {
        let client = Client::connect(options(&args, "mq-harness-audit".into())?).await?;
        let group = format!("{topic}-audit-{}", crate::epoch_ms() % 1_000_000);
        let mut consumer = client
            .subscribe(
                &topic,
                &group,
                SubscribeOptions::default()
                    .prefetch(args.prefetch.max(64))
                    .start(StartFrom::Earliest),
            )
            .await?;
        let started = Instant::now();
        let mut scanned = 0u64;
        let mut found = Vec::new();
        let mut found_n = 0u64;
        let mut low: HashMap<u32, u64> = HashMap::new();
        let mut high: HashMap<u32, u64> = HashMap::new();
        while found_n < want_n && started.elapsed() < Duration::from_secs(600) {
            let d = match tokio::time::timeout(Duration::from_secs(5), consumer.next()).await {
                Ok(Some(Ok(d))) => d,
                Ok(Some(Err(e))) => anyhow::bail!("audit consume: {e}"),
                Ok(None) | Err(_) => break, // end of the log (idle 5 s)
            };
            scanned += 1;
            let id = d.id();
            low.entry(id.partition)
                .and_modify(|o| *o = (*o).min(id.offset))
                .or_insert(id.offset);
            high.entry(id.partition)
                .and_modify(|o| *o = (*o).max(id.offset))
                .or_insert(id.offset);
            if let Some(pk) = payload_id(d.payload())
                && want.get(pk.0 as usize).is_some_and(|b| b.contains(pk.1))
            {
                found_n += 1;
                if found.len() < args.dump_missing {
                    found.push(json!({
                    "producer": pk.0, "seq": pk.1, "partition": id.partition,
                    "offset": id.offset, "timestamp_ms": d.timestamp_ms(),
                    }));
                }
            }
            let _ = d.ack().await;
        }
        let _ = consumer.unsubscribe().await;
        Ok::<_, anyhow::Error>(json!({
            "group": group,
            "scanned": scanned,
            "seconds": (started.elapsed().as_secs_f64() * 10.0).round() / 10.0,
            "found_in_log": found_n,
            "not_found": want_n - found_n,
            "found": found,
            "low_offsets": low,
            "high_offsets_seen": high,
        }))
    };
    match run.await {
        Ok(v) => v,
        Err(e) => json!({"error": format!("{e:#}")}),
    }
}
