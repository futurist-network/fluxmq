---
title: What is FluxMQ
group: start
order: 1
summary: A single-binary message broker with built-in observability — what it does and how it is built.
---

FluxMQ is a message broker written in Rust. One binary, `fluxmq`, stores messages on disk, delivers them to consumers with acknowledgements and retries, computes its own metrics and alerts, and serves this web console. You don't need a separate Prometheus, Grafana or admin tool to see what is wrong and fix it.

## What it does {#features}

- **Channels (topics) in three modes** — `stream` (a replayable log, like a Kafka topic), `queue` (competing workers, like a RabbitMQ queue) and `keyed` (strict order per key). See [Core concepts](/docs/concepts).
- **Reliable delivery** — at-least-once, per-message acknowledgement, retries with backoff, and a **dead-letter queue (DLQ)** grouped by failure reason.
- **Durable by default** — a publish is acknowledged only after `fsync` to disk (group commit) and, in a cluster, after replication to every in-sync replica (ISR).
- **Observability built in** — rates, latency percentiles, consumer lag, health rules and alerts are computed by the broker; the console receives ready-made aggregates over one WebSocket.
- **Clustering** — Raft metadata, Kafka-style partition replication, automatic failover, rebalancing, node decommissioning.
- **Operations** — access tokens and roles (RBAC), TLS and mTLS on every port, content-based routing, HTTP webhooks, notifications (Telegram, Slack, Discord, email, PagerDuty, webhook), backups to S3 or disk, Prometheus metrics.

## Architecture in one picture {#architecture}

:::diagram architecture

- **Port 7420** — the binary client protocol: producers and consumers (Rust SDK `fluxmq-client`).
- **Port 7421** — HTTP: REST API `/api/*`, WebSocket telemetry `/ws`, `/metrics` and the console itself.
- **Ports 7422 and 7423** — cluster only: Raft (metadata, controller election) and partition data replication.

The core design rule: **the observability plane never slows the data plane**. The core only accumulates counters and the telemetry pulls them; live streams (events, live tail) go through bounded buffers and are sampled under load instead of delaying publishes.

## How to read these docs {#how-to-read}

- **Getting started** — concepts, first run and finding your way around the console.
- **Console sections** — one article per screen: what it shows, what each number means, what is normal and what is a problem, and which actions it offers.
- **How-to guides** — step by step: create a channel, connect a client, investigate lag or the DLQ, set up notifications, secure the broker, upgrade a cluster.
- **Reference** — flags and environment variables, the REST API, Prometheus metrics, error codes, the filter language, a glossary.

> [!TIP]
> On any console screen the "?" button opens the article about that screen. The search box on the left looks through the titles and text of every article; press <kbd>/</kbd> to focus it.

## Current limitations {#limits}

FluxMQ is suitable for development and evaluation. Not there yet: an index on payload fields (search scans up to 200,000 messages per query), `OR` in filters, OpenTelemetry export, rack/AZ awareness, a byte-aware rebalancing planner, decommissioning several nodes at once, CA reload without a restart, CRL/OCSP, restoring a backup straight into a cluster, and SSO. The official SDK is Rust only for now.
