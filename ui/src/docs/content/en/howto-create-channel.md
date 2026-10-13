---
title: Create a channel
group: howto
order: 2
summary: How to pick the mode, partition count, retention, retries and DLQ — and create the channel in the console, over REST or from the SDK.
---

## 1. Pick the mode {#mode}

| Question | Answer → mode |
|---|---|
| Do you need to keep events, re-read them and serve several independent consumers? | `stream` (like a Kafka topic) |
| Are these jobs for a worker pool, order irrelevant, each job once per group? | `queue` (like a RabbitMQ queue) |
| Must order be strict per customer / order / account, while different keys may run in parallel? | `keyed` |
| Must urgent jobs overtake waiting ones (alerts before newsletters, VIP before regular)? | `priority` (like a RabbitMQ priority queue) |

When in doubt, take `stream`: it can be replayed, and several groups receive every message in any mode anyway.

Ready-made combinations of mode and settings for typical tasks — see [Recipes](/docs/recipes#chooser).

## 2. Pick the partition count {#partitions}

- `stream` and `keyed`: partitions are the unit of parallelism within a group. Leave room for the future number of instances (default 8). **The partition count can't change after creation.**
- `queue`: 1 is usually enough — competing consumers share the work anyway.
- `priority`: the partitions are the priority levels, 2–10 (default 3). Workers always take the most urgent waiting message first; with a steady stream of urgent messages the lower levels wait (strict priority).
- In `keyed`, and whenever you publish with a key, a key's messages always land in one partition (`crc32(key) % partitions`). A single very "hot" key is bound by one partition's speed.

## 3. Retention {#retention}

- Age: how long you need to be able to re-read or replay messages (default 7 days).
- Size: insurance against a full disk (default unlimited). The oldest segments go first.
- In `queue`, a message is also removed once every group acked it.

> [!WARNING]
> If a group falls behind for longer than the retention period, messages it hasn't read are deleted and **skipped**. Size retention to cover the downtime of your slowest consumer.

## 4. Retries and DLQ {#retries}

| Setting | Default | How to choose |
|---|---|---|
| Max attempts | 5 | how many tries make sense before a human should look |
| Backoff | `1s, 10s, 1m, 5m` | match the failures: seconds for network blips, minutes for dependency restarts |
| Ack timeout | 30 s | above the longest processing time of one message, or a slow message is redelivered |
| DLQ | on | turn off only if losing failed messages is acceptable |

Ack timeout, retries and the DLQ can be changed at any time on the channel's [Settings](/docs/channel-detail#settings-tab) tab.

## 5. Create the channel {#create}

**In the console**: Channels → **Create channel** → pick the scenario → name, retention, partitions → **Advanced** if needed. Field details: [Channels](/docs/channels#create).

**Over REST** (admin role):

```bash
curl -s -X POST http://localhost:7421/api/channels \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' -d '{
    "name": "orders.events",
    "mode": "stream",
    "partitions": 8,
    "retention": {"max_age_secs": 604800, "max_bytes": null},
    "retry": {"max_attempts": 5, "backoff_ms": [1000, 10000, 60000, 300000], "ack_timeout_ms": 30000},
    "dlq_enabled": true,
    "ordering_key": "order_id"
  }'
```

Every field except `name` and `mode` is optional. The answer is `201` with the channel config; `409` — a channel with that name exists; `400` — invalid name or settings.

**From the SDK** — idempotent, handy at service start-up:

```rust
use fluxmq_client::{ChannelConfig, ChannelMode};

client.ensure_channel(ChannelConfig::new("orders.events", ChannelMode::Stream)).await?;
```

`ChannelConfig::new` uses the defaults (8 partitions, 1 for `queue`, 7 days, 5 attempts, DLQ on); change the struct's fields before the call if needed. Creating a channel over the client protocol requires the admin role.

## 6. Check it {#verify}

Open the channel in the console and publish a test message:

```bash
curl -s -X POST http://localhost:7421/api/channels/orders.events/publish \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' -d '{
  "messages":[{"key":"8842","headers":{"flux-type":"OrderCreated"},
               "payload":{"encoding":"utf8","data":"{\"order_id\":8842}"}}]}'
```

It shows up on the Messages tab (Latest). Next: [connect a client](/docs/howto-connect-client).
