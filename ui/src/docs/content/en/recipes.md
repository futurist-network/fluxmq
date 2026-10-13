---
title: Recipes
group: howto
order: 2.5
summary: Eight proven combinations of "mode + channel settings + groups, routes, webhooks" for typical tasks — orders, background jobs, audit, fan-out, routing, HTTP push, retries, telemetry.
---

Each recipe is a ready-made card: the task, the mode and why, exact channel settings, what to set up after creation, how to create it (console, REST, SDK), what to monitor and where people usually go wrong. The values in the settings tables are exactly what the **Create channel** wizard and `POST /api/channels` accept; copy them as they are.

Common to all recipes: a publish is acknowledged **after fsync** (in a cluster — after it is on every in-sync replica), and delivery is **at-least-once**. A message can arrive again after a crash, a publish retry or a rewind, so handlers must be **idempotent** — for example, by a business key (`order_id`, `email_id`).

## Which recipe {#chooser}

| Task | Recipe | Mode |
|---|---|---|
| Process orders / payments strictly in order per order or customer | [Orders with strict ordering](#orders-keyed) | `keyed` |
| Send emails, render PDFs, resize images with a worker pool | [Background jobs](#jobs-queue) | `queue` |
| Event log / audit trail that is re-read and consumed by several systems | [Event log and audit](#event-log) | `stream` |
| One event is needed by several services | [Fan-out to several services](#fanout) | `stream` |
| Split messages into specialised channels by content (country, amount, type) | [Content-based routing](#content-routing) | `stream` + routes |
| Deliver events to an external HTTP service without an SDK | [Push to external HTTP](#webhook-push) | `stream` + webhook |
| Flaky external dependency: retry with growing delays, set poison messages aside | [Delayed retry and poison messages](#retry-poison) | `queue` |
| Metrics, clicks, logs — hundreds of thousands of messages per second | [Telemetry ingestion](#telemetry-ingest) | `stream` + cumulative |

How the modes work — [Concepts](/docs/concepts#modes); how to pick each parameter — [Create a channel](/docs/howto-create-channel).

> [!NOTE]
> Partition count, mode and ordering key can't change after creation. Retention, retries, ack timeout and the DLQ can be changed at any time on the channel's [Settings](/docs/channel-detail#settings-tab) tab.

## 1. Orders with strict ordering {#orders-keyed}

**Task.** Commands for an order (`OrderCreated` → `OrderPaid` → `OrderShipped`) must be processed strictly in order within one order, while different orders run in parallel on several service instances.

**Mode and why.** `keyed`: the next message of a key isn't handed out until the previous one is acked or dead-lettered; other keys run in parallel. In `stream` a failed message is retried without blocking its partition, so later messages of the same order can overtake it.

| Setting | Value |
|---|---|
| Mode | `keyed` |
| Partitions | 16 |
| Keep messages for | 7 days |
| Size limit | unlimited |
| Max delivery attempts | 5 |
| Backoff list | `1s, 5s, 30s, 2m` |
| Ack timeout (seconds) | 30 |
| Dead-letter queue | on |
| Ordering key | `order_id` |

The delays are shorter than the defaults: while a message waits for a retry, its whole order waits.

**Groups.** One group per service (`order-processor`); as many instances as needed, the useful maximum is 16 (the partition count).

**How to create.** In the console: Channels → **Create channel** → "Preserve order per customer / order" → **Continue** → name `orders.commands`, "Keep messages for" 7 days, "Partitions" 16, "Ordering key" `order_id` → **Advanced**: attempts, backoff, ack timeout → **Create channel**.

```bash
curl -s -X POST http://localhost:7421/api/channels \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' -d '{
    "name": "orders.commands", "mode": "keyed", "partitions": 16,
    "retention": {"max_age_secs": 604800, "max_bytes": null},
    "retry": {"max_attempts": 5, "backoff_ms": [1000, 5000, 30000, 120000], "ack_timeout_ms": 30000},
    "dlq_enabled": true, "ordering_key": "order_id"
  }'
```

```rust
use fluxmq_client::{message, ChannelConfig, ChannelMode, SubscribeOptions};

let mut cfg = ChannelConfig::new("orders.commands", ChannelMode::Keyed);
cfg.partitions = 16;
cfg.retry.backoff_ms = vec![1_000, 5_000, 30_000, 120_000];
cfg.ordering_key = Some("order_id".into());
client.ensure_channel(cfg).await?;

// producer: key = order number
client.producer("orders.commands")
    .send(message(r#"{"order_id":8842,"status":"paid"}"#).key("8842").event_type("OrderPaid"))
    .await?;

// consumer
let mut consumer = client.subscribe("orders.commands", "order-processor", SubscribeOptions::default()).await?;
```

**What to monitor.** Consumers → the `order-processor` card: Lag, Retry pending, Processing p99. The channel's Storage tab — skewed partition sizes (a hot key). Alerts `lag_growth`, `consumer_stalled`, `dlq_growth`.

**Pitfalls.**
- Publish **every** message with a key — order exists only within a key.
- One hot key is limited to one partition's speed, and one stuck message holds up its whole key — check the [delivery history](/docs/channel-detail#inspector).
- Once a message is dead-lettered, later messages of its key carry on — the order for that order is already broken. The handler must be able to notice (e.g. by the order status in its own DB), and the DLQ must be handled promptly.
- Use `nack(reason, false)` for data that can never succeed: straight to the DLQ, the key doesn't wait out all attempts.

## 2. Background jobs with a worker pool {#jobs-queue}

**Task.** Sending emails (or PDFs, image resizing): many independent jobs, order irrelevant, each job taken by one worker of the group; transient SMTP failures retried, unrecoverable ones to the DLQ to be replayed later.

**Mode and why.** `queue`: competing consumers — any free instance of the group takes the next job; a message acked by every group is removed. One slow worker doesn't hold a partition as it would in `stream`.

| Setting | Value |
|---|---|
| Mode | `queue` |
| Partitions | 1 |
| Keep messages for | 24 hours |
| Size limit | unlimited |
| Max delivery attempts | 8 |
| Backoff list | `10s, 1m, 5m, 15m, 30m` |
| Ack timeout (seconds) | 60 |
| Dead-letter queue | on |

8 attempts with these delays span almost 2 hours (10 s + 1 min + 5 min + 15 min + 3 × 30 min) — enough to survive a mail server restart. Retention is the safety net for worker downtime: if the group doesn't catch up within 24 hours, unread jobs are deleted; set it higher if workers can be down longer.

**Groups.** One group `mailer`, N instances. To scale, just start more instances.

**How to create.** In the console: Channels → **Create channel** → "Distribute jobs between workers" → **Continue** → name `email.send`, "Keep messages for" 24 hours, "Partitions" 1 → **Advanced** → **Create channel**.

```bash
curl -s -X POST http://localhost:7421/api/channels \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' -d '{
    "name": "email.send", "mode": "queue", "partitions": 1,
    "retention": {"max_age_secs": 86400, "max_bytes": null},
    "retry": {"max_attempts": 8, "backoff_ms": [10000, 60000, 300000, 900000, 1800000], "ack_timeout_ms": 60000},
    "dlq_enabled": true
  }'
```

```rust
let mut consumer = client
    .subscribe("email.send", "mailer", SubscribeOptions::default().prefetch(16))
    .await?;
while let Some(d) = consumer.next().await {
    let d = d?;
    match send_email(d.payload()).await {
        Ok(()) => d.ack().await?,
        Err(e) if e.is_transient() => d.nack("SMTP unavailable", true).await?,  // retry per channel policy
        Err(_) => d.nack("Invalid address", false).await?,                    // straight to the DLQ
    }
}
```

**What to monitor.** The `mailer` group card: Lag, In flight (stuck at the prefetch?), Nacks. Dead Letters — the reason cards. Alerts `no_consumers`, `high_retry_rate`, `dlq_growth`. Once the cause is fixed — **Retry all** on the Dead Letters screen.

**Pitfalls.**
- The ack timeout must exceed the longest processing time of one job, otherwise a slow job is handed to a second worker and the email goes out twice. Deduplicate by `email_id`.
- Keep `prefetch` small (8–32): jobs handed to a stuck worker only come back after the ack timeout.
- Keep `nack` reasons short and stable — the DLQ groups by them.
- A second group on the same channel receives **all** jobs again (fan-out) rather than sharing them with the first.

## 3. Event log and audit {#event-log}

**Task.** Keep domain events or an audit trail for weeks, read them with several independent systems (archive, analytics, anti-fraud), and re-read history from a given moment — e.g. after a bug in a handler.

**Mode and why.** `stream`: a retained log; each group reads everything at its own pace, and its position can be rewound to the earliest message or a point in time.

| Setting | Value |
|---|---|
| Mode | `stream` |
| Partitions | 8 |
| Keep messages for | 30 days |
| Size limit | 100 GB |
| Max delivery attempts | 5 |
| Backoff list | `1s, 10s, 1m, 5m` |
| Ack timeout (seconds) | 30 |
| Dead-letter queue | on |

The size limit protects the disk: beyond it the oldest segments are deleted even if they are younger than 30 days. For an audit trail you can't lose, choose "Forever" and watch the disk (alert `storage_nearly_full`).

**Groups.** One group per system: `audit-archive`, `analytics`, `fraud`. A new group starts from `start`: `StartFrom::Earliest` — the whole history, `StartFrom::TimestampMs(…)` — from a moment.

**How to create.** In the console: Channels → **Create channel** → "Store events and replay them later" → **Continue** → name `audit.events`, "Keep messages for" 30 days, "Partitions" 8 → **Advanced** → "Size limit" 100 GB → **Create channel**.

```bash
curl -s -X POST http://localhost:7421/api/channels \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' -d '{
    "name": "audit.events", "mode": "stream", "partitions": 8,
    "retention": {"max_age_secs": 2592000, "max_bytes": 107374182400},
    "retry": {"max_attempts": 5, "backoff_ms": [1000, 10000, 60000, 300000], "ack_timeout_ms": 30000},
    "dlq_enabled": true
  }'

# re-read everything since 1 October 2026 (00:00 UTC) with group analytics
curl -s -X POST http://localhost:7421/api/channels/audit.events/groups/analytics/reset \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"to":{"timestamp_ms":1790812800000}}'
```

```rust
use fluxmq_client::{StartFrom, SubscribeOptions};

// a new group reads the whole retained history
let mut archive = client
    .subscribe("audit.events", "audit-archive", SubscribeOptions::default().start(StartFrom::Earliest))
    .await?;
```

In the console, rewinding is Consumers → group card → **Reset offsets** → "From a point in time".

**What to monitor.** Each group's lag separately (the channel page shows the maximum over groups). The Storage tab: "Oldest message" and the limit fill bar. The metric `fluxmq_group_retention_skipped_messages_total` must stay at 0.

**Pitfalls.**
- `start` only applies to a **new** group; an existing group's position is changed by resetting offsets.
- A group lagging longer than retention **skips** the deleted messages. Size retention for the downtime of the slowest reader.
- Resetting to the past re-delivers every message — handlers must be idempotent.
- Within a group, no more instances work in parallel than there are partitions.

## 4. Fan-out to several services {#fanout}

**Task.** A `UserRegistered` event is needed by the CRM, the notification service and the search index; each processes it independently, and one failing doesn't affect the others.

**Mode and why.** `stream` and **a separate group per service**. In every mode each group receives all of the channel's messages; each group has its own offsets, retries, lag and DLQ entries (retrying from the DLQ returns a message only to the group that failed it).

| Setting | Value |
|---|---|
| Mode | `stream` |
| Partitions | 8 |
| Keep messages for | 7 days |
| Size limit | unlimited |
| Max delivery attempts | 5 |
| Backoff list | `1s, 10s, 1m, 5m` |
| Ack timeout (seconds) | 30 |
| Dead-letter queue | on |

**Groups or routes — which one.**

| You need | Solution |
|---|---|
| Every service gets every event, shared channel settings | groups `crm`, `notifications`, `search-indexer` on one channel |
| A service needs only some events (by type, country, amount) | the group reads everything and acks what it doesn't need, or a [route](#content-routing) copies the relevant ones to a separate channel |
| Different retention, retries, ack timeout or mode (e.g. a queue for workers) | a route into a separate channel with its own settings |
| An external service without an SDK | a [webhook](#webhook-push) — also a separate group, `$webhook:<name>` |

A route is an extra copy on disk, but the target gets its own settings and permissions. If everything is the same, groups are enough.

**How to create.** The channel is created as in the [event log](#event-log) recipe, but named `user.events` with 7 days retention (or `ChannelConfig::new("user.events", ChannelMode::Stream)` — these are exactly the defaults). Groups need no creation: a group appears on its first subscription.

```rust
// in the CRM
let crm = client.subscribe("user.events", "crm", SubscribeOptions::default()).await?;
// in the notification service — same event, its own position
let notify = client.subscribe("user.events", "notifications", SubscribeOptions::default()).await?;
```

**What to monitor.** The channel's Consumers tab: a card per group. Lag in the channel list is the maximum over groups, so find the problem group in the cards. Alert `no_consumers` — a service stopped reading.

**Pitfalls.**
- A new group starts from `Latest` by default: it won't see events from before its first subscription. Need history — `StartFrom::Earliest`.
- Two instances of one service with **different** group names both receive every event — all copies of a service must use the same group name.
- In `queue` a message is removed only when **every** group has acked it: one dead group holds the storage.

## 5. Content-based routing {#content-routing}

**Task.** From the shared order stream, send orders from Moldova to channel `orders.md`, and large orders (1000 and up) to the manual review queue `orders.review`.

**Mode and why.** The source is a `stream` (other groups read it too); the router is built into the broker and reads the source as the system group `$router`. The targets get the mode their consumers need: `orders.md` a stream, `orders.review` a queue for operators.

Source `orders.events` settings:

| Setting | Value |
|---|---|
| Mode | `stream` |
| Partitions | 8 |
| Keep messages for | 7 days |
| Size limit | unlimited |
| Max delivery attempts | 5 |
| Backoff list | `1s, 10s, 1m, 5m` |
| Ack timeout (seconds) | 30 |
| Dead-letter queue | on |

Targets: `orders.md` — `stream`, 8 partitions, 7 days; `orders.review` — `queue`, 1 partition, 7 days; default retries and DLQ.

**Routes.**

| Route | FROM | WHEN | THEN |
|---|---|---|---|
| MD orders | `orders.events` | `header.flux-type = OrderCreated AND country = MD` | `orders.md` |
| Large orders | `orders.events` | `amount >= 1000` | `orders.review` |

Conditions use the [filter language](/docs/ref-filter): `key`, `partition`, `header.<name>`, JSON paths (`shipping.country`), operators `=`, `!=`, `>`, `>=`, `<`, `<=`, `in [..]`, `prefix`, `contains`, `exists`.

**How to create.** Create all three channels first. Then Routing → **New route** → name, FROM, WHEN conditions (as rows or in Text mode), THEN channels → check the **Preview** ("Matched … of … scanned") → **Create route**. Shortcut: on the source's Messages tab, build the filter and click **Create route from filter**.

```bash
H=http://localhost:7421; A="Authorization: Bearer $TOKEN"
# text → conditions (what the editor does)
curl -s -X POST $H/api/filter/parse -H "$A" -H 'content-type: application/json' \
  -d '{"filter":"header.flux-type = OrderCreated AND country = MD"}'

# create the route (admin role)
curl -s -X POST $H/api/routes -H "$A" -H 'content-type: application/json' -d '{
  "name": "MD orders", "source": "orders.events", "targets": ["orders.md"],
  "conditions": [
    {"field":{"kind":"header","name":"flux-type"},"op":"eq","value":"OrderCreated"},
    {"field":{"kind":"payload","name":"country"},"op":"eq","value":"MD"}]}'

# test conditions against stored messages of the last hour
curl -s -X POST $H/api/routes/test -H "$A" -H 'content-type: application/json' -d '{
  "source": "orders.events", "window_ms": 3600000, "sample_limit": 5,
  "conditions": [{"field":{"kind":"payload","name":"amount"},"op":"gte","value":1000}]}'
```

Routes are created in the console or over REST; the SDK has no API for them.

**What to monitor.** The Routing screen: Matched, Forwarded, Errors, Last match. The `$router` group on the Consumers screen — its lag. In the source's DLQ — group `router` with reason `route target unavailable: <channel>`.

**Pitfalls.**
- There's no `OR`: for "or", create several routes into the same target — the copy is still sent once.
- A route only handles messages published **after** it was created; catch up older ones with [replay](/docs/channel-detail#replay).
- Don't delete a target channel while its route is live: messages go into retries, then into the source's DLQ.
- Payload conditions never match a non-JSON payload; the event type is more reliable from the `flux-type` header.
- A copy is a new message in the target with the same key, headers and payload plus `flux-routed-from`, `flux-route-id`, `flux-route-path`.

## 6. Push to external HTTP (webhook) {#webhook-push}

**Task.** Deliver events to a partner's CRM that can only accept HTTP POSTs: signed, retried on `5xx`/timeouts, with a DLQ.

**Mode and why.** `stream` + a webhook. A webhook is a consumer group `$webhook:<name>`: the broker itself sends messages to the URL, and lag, retries and the DLQ are visible like for any group. If you need strict order per key (customer) rather than per whole partition, use a `keyed` channel.

| Setting | Value |
|---|---|
| Mode | `stream` |
| Partitions | 8 |
| Keep messages for | 3 days |
| Size limit | unlimited |
| Max delivery attempts | 10 |
| Backoff list | `5s, 30s, 2m, 10m, 30m` |
| Ack timeout (seconds) | 30 |
| Dead-letter queue | on |

10 attempts cover about three hours of receiver downtime (5 s + 30 s + 2 min + 10 min + 6 × 30 min). 3 days of retention leave room for a long partner outage.

**Webhook.** Name `crm-sync`, "Payload format" "Envelope (JSON)", "Signing secret", "Headers" if needed (`Authorization`), "Timeout" 10,000 ms, "Max in flight" 4 (or 1 for strict per-partition order), "Batch size" 1, "Start from" — "New messages only" or "Oldest retained message".

**How to create.** Channel `crm.events` — via **Create channel** ("Store events and replay them later", "Keep messages for" 3 days, attempts and backoff under **Advanced**). Then Webhooks → **Create webhook** → the fields above → **Send test** on the right (status and response latency; nothing is saved) → save.

```bash
H=http://localhost:7421; A="Authorization: Bearer $TOKEN"
curl -s -X POST $H/api/channels -H "$A" -H 'content-type: application/json' -d '{
  "name": "crm.events", "mode": "stream", "partitions": 8,
  "retention": {"max_age_secs": 259200, "max_bytes": null},
  "retry": {"max_attempts": 10, "backoff_ms": [5000, 30000, 120000, 600000, 1800000], "ack_timeout_ms": 30000},
  "dlq_enabled": true
}'

# test a draft without saving anything
curl -s -X POST $H/api/webhooks/test -H "$A" -H 'content-type: application/json' -d '{
  "name": "crm-sync", "channel": "crm.events", "url": "https://crm.example.com/hooks/fluxmq"}'

# create (admin role)
curl -s -X POST $H/api/webhooks -H "$A" -H 'content-type: application/json' -d '{
  "name": "crm-sync", "channel": "crm.events", "url": "https://crm.example.com/hooks/fluxmq",
  "secret": "'"$CRM_WEBHOOK_SECRET"'", "headers": {"Authorization": "Bearer partner-token"},
  "format": "envelope", "timeout_ms": 10000, "max_in_flight": 4, "batch_max": 1,
  "start": "latest"
}'
```

Need only some events — add `"filter"` (conditions in the same JSON form as routes) or set the filter in the editor; non-matching messages are acked without a request.

**What to monitor.** The Webhooks screen: Delivered / Failed / Skipped, p99, Last status; the sidebar badge counts failing webhooks. The `$webhook:crm-sync` group on the Consumers screen — lag. The DLQ — reasons `HTTP 503`, `timeout`, `connection refused`, `blocked by policy`.

**Pitfalls.**
- On the receiving side, deduplicate by `delivery_id` (`channel/partition/offset`, the same for all retries) and verify `X-FluxMQ-Signature` against the **raw** body.
- The webhook's "Timeout" plus "Batch linger" must stay below the channel's ack timeout — otherwise the webhook is rejected.
- Other `4xx` (except 408, 425, 429) and `3xx` go straight to the DLQ without retries; redirects aren't followed.
- With authorization enabled, private and loopback addresses are blocked (SSRF protection): an internal address needs the `--webhooks-allow-private` flag.
- "Max in flight" above 1 speeds delivery up, but order isn't guaranteed.

## 7. Delayed retry and poison messages {#retry-poison}

**Task.** Syncing with an external API that can be down for hours. Transient failures — retry with growing delays without hammering the API; messages that will never succeed ("poison") — set aside immediately, investigate, and bring back if needed.

**Mode and why.** `queue`: retries of one message don't hold up the others, and workers compete. The delay before a retry is the channel's retry policy: the broker itself holds the message until the next attempt.

:::diagram delivery

| Setting | Value |
|---|---|
| Mode | `queue` |
| Partitions | 1 |
| Keep messages for | 3 days |
| Size limit | unlimited |
| Max delivery attempts | 6 |
| Backoff list | `30s, 2m, 10m, 30m, 1h` |
| Ack timeout (seconds) | 60 |
| Dead-letter queue | on |

Six attempts span about 1 h 43 min. The last value of the list repeats, so with more attempts the delays stay at an hour.

**Groups.** One group `partner-sync`. The handler tells error kinds apart:

```rust
let mut consumer = client.subscribe("partner.sync", "partner-sync", SubscribeOptions::default()).await?;
while let Some(d) = consumer.next().await {
    let d = d?;
    match push_to_partner(d.payload()).await {
        Ok(()) => d.ack().await?,
        Err(e) if e.is_transient() => d.nack("Partner API 503", true).await?,  // waits per the backoff list
        Err(_) => d.nack("Invalid payload", false).await?,                      // poison — straight to the DLQ
    }
}
```

`d.attempt()` is the current attempt number; use it, for example, to log only the final failure.

**How to create.** Channels → **Create channel** → "Distribute jobs between workers" → name `partner.sync`, "Keep messages for" 3 days → **Advanced**: "Max delivery attempts" 6, "Backoff list" `30s, 2m, 10m, 30m, 1h`, "Ack timeout (seconds)" 60 → **Create channel**.

```bash
curl -s -X POST http://localhost:7421/api/channels \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' -d '{
    "name": "partner.sync", "mode": "queue", "partitions": 1,
    "retention": {"max_age_secs": 259200, "max_bytes": null},
    "retry": {"max_attempts": 6, "backoff_ms": [30000, 120000, 600000, 1800000, 3600000], "ack_timeout_ms": 60000},
    "dlq_enabled": true
  }'
```

**Working the DLQ.** Dead Letters → the reason card → open a couple of messages (the delivery history shows every attempt and delay) → fix the cause → **Retry all** (a fresh set of attempts, for the same group only). For what isn't needed — **Export NDJSON**, then **Discard**. A manually corrected message can be published again, or use **Replay…** on the Messages tab. Over REST:

```bash
H=http://localhost:7421; A="Authorization: Bearer $TOKEN"
curl -s -H "$A" "$H/api/dlq?channel=partner.sync&limit=20"
curl -s -H "$A" -X POST $H/api/dlq/retry -H 'content-type: application/json' \
  -d '{"reason":"Partner API 503","channel":"partner.sync"}'
```

**What to monitor.** The group card: Retry pending and Nacks. Alerts `high_retry_rate` and `dlq_growth`; the `dead_lettered` event — into [notifications](/docs/howto-alerts). Step by step — [Handle the DLQ](/docs/howto-handle-dlq).

**Pitfalls.**
- Until the cause is fixed, "Retry all" just runs the messages through every attempt again and back into the DLQ.
- The ack timeout also uses up an attempt: a handler that crashes without a `nack` burns attempts 60 s at a time.
- In `keyed`, a message waiting out a delay holds its whole key — long delays are dangerous there.
- With the DLQ off, messages that exhaust their attempts are **dropped**.

## 8. Telemetry ingestion at high throughput {#telemetry-ingest}

**Task.** Ingest device metrics, clicks or logs — hundreds of thousands of messages per second — and process them as a stream (aggregations, loading into an analytics store).

**Mode and why.** `stream`: maximum throughput, partitions are the unit of parallelism. The producer is a `BatchingProducer` with compression; the consumer uses `AckMode::Cumulative` (committing a position per partition instead of acking each message).

| Setting | Value |
|---|---|
| Mode | `stream` |
| Partitions | 32 |
| Keep messages for | 24 hours |
| Size limit | 100 GB |
| Max delivery attempts | 3 |
| Backoff list | `1s, 10s` |
| Ack timeout (seconds) | 30 |
| Dead-letter queue | on |

Retries, the ack timeout and the DLQ apply only to groups in `Individual` mode; cumulative groups don't use them. 32 partitions leave room for 32 parallel instances per group: the partition count can't change later.

**How to create.** Channels → **Create channel** → "Store events and replay them later" → name `telemetry.metrics`, "Keep messages for" 24 hours, "Partitions" 32 → **Advanced**: "Size limit" 100 GB, attempts 3, backoff `1s, 10s` → **Create channel**.

```bash
curl -s -X POST http://localhost:7421/api/channels \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' -d '{
    "name": "telemetry.metrics", "mode": "stream", "partitions": 32,
    "retention": {"max_age_secs": 86400, "max_bytes": 107374182400},
    "retry": {"max_attempts": 3, "backoff_ms": [1000, 10000], "ack_timeout_ms": 30000},
    "dlq_enabled": true
  }'
```

```rust
use fluxmq_client::{message, BatchingOptions, Client, ClientOptions, Compression, SubscribeOptions};

// producer: per-partition batches + lz4 (zstd when traffic and space matter more)
let client = Client::connect(
    ClientOptions::new("127.0.0.1:7420", "ingest-1").compression(Compression::Lz4),
).await?;
let producer = client.batching_producer("telemetry.metrics", BatchingOptions::default());
let fut = producer.send(message(r#"{"device":"d-17","cpu":0.42}"#).key("d-17")).await?;
// fut completes once the message is on disk; an error means it was not stored

// consumer: cumulative — an ack commits the partition up to and including this message
let mut consumer = client
    .subscribe("telemetry.metrics", "analytics", SubscribeOptions::default().prefetch(2000).cumulative())
    .await?;
while let Some(d) = consumer.next().await {
    let d = d?;
    aggregate(&d);
    d.ack().await?;
}
```

`BatchingOptions` (`linger`, `max_batch`, `max_in_flight`, `ordered`) and compression in detail — [Connect a client](/docs/howto-connect-client#batching).

**What to monitor.** The channel list: Rate (msg/s) and p99. The `analytics` group card: Lag and Ack rate. The Storage tab: partition balance and limit fill. Alerts `high_publish_latency`, `storage_nearly_full`, `lag_growth`.

**Pitfalls.**
- In cumulative mode `nack`, the ack timeout and the DLQ don't work (`nack` returns `BadRequest`); after a crash, everything after the last committed position arrives again. Ack a message only once all earlier ones in its partition are processed.
- Don't await each message's future one by one — that throws away the batching gain; collect them in `FuturesUnordered`.
- A key (`device_id`) gives per-device order, but a hot device is capped by one partition; without a key, messages are spread "sticky" per request.
- Don't switch to `--fsync os` for speed: it's a broker-wide flag, and on power loss the last acknowledged messages of every channel can be lost. Speed comes from batching and compression, not from weakening fsync.
- A 100 GB size limit deletes old segments before 24 hours if the stream is bigger than expected — a lagging group will skip them.

## What's missing and what to use instead {#not-supported}

| You want | In FluxMQ | What to do |
|---|---|---|
| Delayed delivery "at time X" | no | delays between attempts ([recipe 7](#retry-poison)) or a scheduler in your own service |
| Request/response, `reply-to` | no | two channels and your own correlation header (any headers are preserved) |
| Transactions, exactly-once, a broker-side outbox | no | at-least-once + idempotent handlers; an outbox in your own DB, published after commit |
| `OR` in route and webhook conditions | no | several routes into one target |
| Message priorities | no | separate channels (e.g. routed by a field) with their own groups |
| Change partition count, mode, key | not after creation | a new channel + a route or replay |
