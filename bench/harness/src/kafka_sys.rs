//! Apache Kafka via rdkafka (librdkafka, built from source): one `FutureProducer` per producer
//! connection (`acks=all`, idempotent), one `StreamConsumer` per consumer in a fresh group.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use futures::FutureExt;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer as _};

use crate::{Args, Shared, Started, producer_loop};

fn base(args: &Args) -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", args.brokers.join(","));
    if let Some(ca) = &args.kafka_tls_ca {
        // TLS to the brokers' SSL listener, server certificate verified against `ca`
        // (librdkafka's default hostname verification stays on).
        c.set("security.protocol", "ssl");
        c.set("ssl.ca.location", ca.display().to_string());
    }
    c
}

pub async fn start(args: &Args, topic: &str, shared: Arc<Shared>) -> anyhow::Result<Started> {
    let admin: AdminClient<DefaultClientContext> = base(args).create().context("admin client")?;
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));
    let retention = args.retention_bytes.to_string();
    // Small segments (the same --segment-bytes FluxMQ gets) + 1 s delete delay keep disk
    // bounded at high rates (Kafka only deletes closed segments, and by default keeps deleted
    // ones for 60 s).
    let segment = args.segment_bytes.max(1 << 20).to_string();
    let min_isr = if args.replication > 1 {
        args.min_insync.min(args.replication).to_string()
    } else {
        "1".to_string()
    };
    let new_topic = NewTopic::new(
        topic,
        args.partitions.max(1) as i32,
        TopicReplication::Fixed(args.replication.max(1) as i32),
    )
    .set("min.insync.replicas", &min_isr)
    .set("retention.bytes", &retention)
    .set("segment.bytes", &segment)
    .set("file.delete.delay.ms", "1000");
    let res = admin
        .create_topics(&[new_topic], &opts)
        .await
        .context("create topic")?;
    for r in res {
        if let Err((t, code)) = r {
            if code != RDKafkaErrorCode::TopicAlreadyExists {
                anyhow::bail!("create topic {t}: {code}");
            }
        }
    }
    // Wait until the topic's metadata (all partitions with a leader) is visible.
    let probe: FutureProducer = base(args).create()?;
    for _ in 0..60 {
        let ok = probe
            .client()
            .fetch_metadata(Some(topic), Duration::from_secs(5))
            .ok()
            .and_then(|m| {
                m.topics().first().map(|t| {
                    t.error().is_none()
                        && t.partitions().len() == args.partitions as usize
                        && t.partitions().iter().all(|p| p.leader() >= 0)
                })
            })
            .unwrap_or(false);
        if ok {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    drop(probe);

    let group = format!("{topic}-g");
    let mut handles = Vec::new();
    let mut consumers = Vec::new();
    for i in 0..args.consumers {
        let c: StreamConsumer = base(args)
            .set("group.id", &group)
            .set("client.id", format!("mq-harness-consumer-{i}"))
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .set("session.timeout.ms", "10000")
            .set("heartbeat.interval.ms", "1000")
            .create()
            .context("consumer")?;
        // With --preload the consumers join the group only after the preload (no
        // prefetching while it runs); otherwise now.
        if args.preload == 0 {
            c.subscribe(&[topic]).context("subscribe")?;
        }
        let c = Arc::new(c);
        handles.push(c.clone());
        let rec = shared.recorder();
        let shared = shared.clone();
        let late_topic = (args.preload > 0).then(|| topic.to_string());
        consumers.push(tokio::spawn(async move {
            shared.consume_gate().await;
            if let Some(t) = late_topic
                && let Err(e) = c.subscribe(&[&t])
            {
                shared.error(format!("subscribe: {e}"));
                return;
            }
            loop {
                match c.recv().await {
                    Ok(m) => {
                        if let Some(p) = m.payload() {
                            rec.record(p);
                        }
                    }
                    Err(e) => {
                        shared.error(format!("consume: {e}"));
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }));
    }
    // Producers start only once the group owns every partition (otherwise the first
    // messages would wait for a rebalance and pollute the warm-up less predictably).
    if args.consumers > 0 && args.preload == 0 {
        let want = (args.partitions as usize).max(1);
        for _ in 0..120 {
            let assigned: usize = handles
                .iter()
                .map(|c| c.assignment().map(|a| a.count()).unwrap_or(0))
                .sum();
            if assigned >= want {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    let mut producers = Vec::new();
    for p in 0..args.producers {
        let producer: FutureProducer = base(args)
            .set("client.id", format!("mq-harness-producer-{p}"))
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("linger.ms", args.linger_ms.to_string())
            .set("compression.type", "none")
            .set("queue.buffering.max.messages", "1000000")
            .set("message.timeout.ms", "60000")
            .create()
            .context("producer")?;
        let producer = Arc::new(producer);
        let topic = topic.to_string();
        let shared = shared.clone();
        let args = args.clone();
        producers.push(tokio::spawn(async move {
            producer_loop(shared, args, p as u32, move |payload, _k| {
                let producer = producer.clone();
                let topic = topic.clone();
                async move {
                    loop {
                        let rec = FutureRecord::<(), [u8]>::to(&topic).payload(&payload);
                        match producer.send_result(rec) {
                            Ok(fut) => {
                                return Ok(async move {
                                    match fut.await {
                                        Ok(Ok(_)) => Ok(()),
                                        Ok(Err((e, _))) => Err(anyhow::anyhow!("{e}")),
                                        Err(_) => Err(anyhow::anyhow!("delivery canceled")),
                                    }
                                }
                                .boxed());
                            }
                            Err((
                                KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull),
                                _,
                            )) => {
                                tokio::time::sleep(Duration::from_millis(1)).await;
                            }
                            Err((e, _)) => return Err(anyhow::anyhow!("{e}")),
                        }
                    }
                }
                .boxed()
            })
            .await;
        }));
    }

    let cleanup = {
        let topic = topic.to_string();
        async move {
            let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(20)));
            admin.delete_topics(&[&topic], &opts).await?;
            Ok(())
        }
        .boxed()
    };

    let (_, version) = rdkafka::util::get_rdkafka_version();
    Ok(Started {
        producers,
        consumers,
        durability: format!(
            "Kafka: topic {} partitions, RF={}, min.insync.replicas={}; producer acks=all + \
             enable.idempotence=true; broker default flush policy (no fsync per message: \
             durability comes from replication, data reaches disk via the OS page cache); \
             consumer offsets auto-committed every 5 s",
            args.partitions, args.replication, min_isr
        ),
        client: format!(
            "librdkafka {version} (rdkafka crate) FutureProducer linger.ms={}, StreamConsumer",
            args.linger_ms
        ),
        broker_version: None,
        cleanup: Some(cleanup),
        audit: None,
    })
}
