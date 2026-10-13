//! RabbitMQ via lapin (AMQP 0-9-1): `--partitions` durable queues (classic or quorum) used
//! like partitions, persistent messages with publisher confirms, manual acks.
//!
//! Producers publish round-robin over the queues through the default exchange. Consumer `i`
//! consumes the queues `q` with `q % consumers == i` (or queue `i % queues` when there are
//! more consumers than queues). Connections are spread over the brokers; each queue is
//! declared through broker `q % n`, so quorum-queue leaders (client-local placement) are spread.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use futures::{FutureExt, StreamExt};
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::publisher_confirm::Confirmation;
use lapin::types::{AMQPValue, FieldTable};
use lapin::{BasicProperties, Connection, ConnectionProperties};

use crate::{Args, QueueType, Shared, Started, producer_loop};

async fn connect(uri: &str, name: String) -> anyhow::Result<Connection> {
    let props = ConnectionProperties::default()
        .with_executor(tokio_executor_trait::Tokio::current())
        .with_reactor(tokio_reactor_trait::Tokio)
        .with_connection_name(name.into());
    Connection::connect(uri, props)
        .await
        .with_context(|| format!("connect {uri}"))
}

fn queue_args(args: &Args) -> FieldTable {
    let mut t = FieldTable::default();
    match args.queue_type {
        QueueType::Quorum => {
            t.insert(
                "x-queue-type".into(),
                AMQPValue::LongString("quorum".into()),
            );
        }
        QueueType::Classic => {
            t.insert(
                "x-queue-type".into(),
                AMQPValue::LongString("classic".into()),
            );
        }
    }
    // Same disk cap as the other systems' retention (drop-head when exceeded).
    t.insert(
        "x-max-length-bytes".into(),
        AMQPValue::LongLongInt(args.retention_bytes as i64),
    );
    t
}

pub async fn start(args: &Args, topic: &str, shared: Arc<Shared>) -> anyhow::Result<Started> {
    let n = args.brokers.len();
    let queues: Vec<String> = (0..args.partitions.max(1))
        .map(|q| format!("{topic}-{q}"))
        .collect();

    // Declare each queue through broker q % n.
    let mut admins = Vec::new();
    for (i, uri) in args.brokers.iter().enumerate() {
        admins.push(connect(uri, format!("mq-harness-admin-{i}")).await?);
    }
    // The broker version is captured by the runner (rabbitmqctl version).
    let server_version: Option<String> = None;
    for (q, name) in queues.iter().enumerate() {
        let ch = admins[q % n].create_channel().await?;
        ch.queue_declare(
            name,
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            queue_args(args),
        )
        .await
        .with_context(|| format!("declare {name}"))?;
        let _ = ch.close(200, "ok").await;
    }

    let mut consumers = Vec::new();
    let nq = queues.len();
    for i in 0..args.consumers {
        let mine: Vec<String> = if args.consumers <= nq {
            queues
                .iter()
                .enumerate()
                .filter(|(q, _)| q % args.consumers == i)
                .map(|(_, s)| s.clone())
                .collect()
        } else {
            vec![queues[i % nq].clone()]
        };
        let conn = connect(&args.brokers[i % n], format!("mq-harness-consumer-{i}")).await?;
        let ch = conn.create_channel().await?;
        ch.basic_qos(
            args.prefetch.min(u16::MAX as u32) as u16,
            BasicQosOptions::default(),
        )
        .await?;
        let rec = shared.recorder();
        let shared = shared.clone();
        consumers.push(tokio::spawn(async move {
            let _conn = conn;
            // With --preload: start consuming only after the preload.
            shared.consume_gate().await;
            let mut streams = Vec::new();
            for (j, q) in mine.iter().enumerate() {
                match ch
                    .basic_consume(
                        q,
                        &format!("c{i}-{j}"),
                        BasicConsumeOptions::default(),
                        FieldTable::default(),
                    )
                    .await
                {
                    Ok(c) => streams.push(c),
                    Err(e) => {
                        shared.error(format!("consume {q}: {e}"));
                        return;
                    }
                }
            }
            let mut merged = futures::stream::select_all(streams);
            let _ch = ch;
            while let Some(d) = merged.next().await {
                match d {
                    Ok(d) => {
                        rec.record(&d.data);
                        if d.redelivered {
                            rec.redelivered();
                        }
                        if let Err(e) = d.ack(BasicAckOptions::default()).await {
                            shared.error(format!("ack: {e}"));
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

    let mut producers = Vec::new();
    for p in 0..args.producers {
        let conn = connect(&args.brokers[p % n], format!("mq-harness-producer-{p}")).await?;
        let ch = conn.create_channel().await?;
        ch.confirm_select(ConfirmSelectOptions::default()).await?;
        let ch = Arc::new(ch);
        let queues = Arc::new(queues.clone());
        let shared = shared.clone();
        let args = args.clone();
        producers.push(tokio::spawn(async move {
            let _conn = conn;
            let offset = p;
            producer_loop(shared, args, p as u32, move |payload, k| {
                let ch = ch.clone();
                let queues = queues.clone();
                async move {
                    let q = &queues[(k as usize + offset) % queues.len()];
                    let confirm = ch
                        .basic_publish(
                            "",
                            q,
                            BasicPublishOptions::default(),
                            &payload,
                            BasicProperties::default().with_delivery_mode(2),
                        )
                        .await?;
                    Ok(async move {
                        match confirm.await? {
                            Confirmation::Ack(_) => Ok(()),
                            Confirmation::Nack(_) => Err(anyhow::anyhow!("nack")),
                            Confirmation::NotRequested => Ok(()),
                        }
                    }
                    .boxed())
                }
                .boxed()
            })
            .await;
        }));
    }

    let cleanup = {
        let queues = queues.clone();
        async move {
            let ch = admins[0].create_channel().await?;
            for q in &queues {
                ch.queue_delete(q, QueueDeleteOptions::default()).await?;
            }
            Ok(())
        }
        .boxed()
    };

    Ok(Started {
        producers,
        consumers,
        durability: match args.queue_type {
            QueueType::Classic => format!(
                "RabbitMQ: {} durable classic queues, persistent messages (delivery_mode=2), \
                 publisher confirms (confirm after the message is written to disk / fsync'd in \
                 batches by the queue); manual ack per message",
                nq
            ),
            QueueType::Quorum => format!(
                "RabbitMQ: {} quorum queues (Raft, replicated to every cluster node), \
                 persistent messages, publisher confirms (confirm after a majority has the \
                 entry fsync'd to its WAL); manual ack per message",
                nq
            ),
        },
        client: format!(
            "lapin 2.x (AMQP 0-9-1), publisher confirms, prefetch={}",
            args.prefetch
        ),
        broker_version: server_version,
        cleanup: Some(cleanup),
        audit: None,
    })
}
