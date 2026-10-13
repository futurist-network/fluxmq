---
title: Core concepts
group: start
order: 2
summary: Channels (topics), stream/queue/keyed modes, partitions, consumer groups, acknowledgements, retries, the DLQ, retention and durability.
---

## Channel (topic) {#channel}

A **channel** is a named stream of messages — the equivalent of a Kafka topic or a RabbitMQ queue. Producers publish to a channel, consumer groups read from it. Every channel is a durable on-disk log split into **partitions**.

- Name: 1–128 characters `[a-zA-Z0-9._-]`, not starting with a dot and not ending in `.consumers` or `.tail`. Dots are handy for namespaces: `orders.events`, `billing.invoices`.
- Channels whose name starts with `$` (for example `$system.events`, `$system.notifications`) are **system channels**: the broker owns them; you can read them but not change or delete them. The console marks them with a **system** badge.
- Mode, partition count and ordering key are set at creation and never change. Retention, retries and the DLQ can be changed at any time.

## The three modes {#modes}

| Mode | Use it to | Like | How a group shares messages |
|---|---|---|---|
| `stream` | keep events and replay them | a Kafka topic | partitions are spread across the group's instances; you can rewind to any position |
| `queue` | distribute jobs between workers | a RabbitMQ queue (competing consumers) | any free instance takes the next message |
| `keyed` | keep order per customer, order, etc. | a partitioned queue with per-key order | the next message of key K is not handed out until the previous one is acked or dead-lettered; other keys proceed in parallel |
| `priority` | serve urgent jobs first | a RabbitMQ priority queue | like `queue`, but any free instance takes a waiting message of the **highest** priority level first (header `flux-priority`, 0 = lowest); partitions are the levels (2–10) |

In **every** mode each consumer group receives **every** message of the channel (fan-out). Inside a group, its instances share the work.

Which mode to take for a specific task (orders, background jobs, audit, telemetry…) and with which settings — see [Recipes](/docs/recipes).

> [!NOTE]
> In `stream`, a failing message is retried without blocking its partition. In `queue`, a message counts as consumed once every group acked (or dead-lettered) it. In `keyed`, ordering is strict per key — a failing message delays only its own key.

## Partitions {#partitions}

- The partition is `crc32(key) % partitions` — messages with the same key always land in the same partition and are read in order.
- Messages **without a key** are spread "sticky": all keyless messages of one publish request (up to about 1 MiB) go to one partition, the next request to the next partition, round-robin. Keyless order is not guaranteed.
- Defaults: 8 partitions for `stream` and `keyed`, 1 for `queue`; 1–1024 allowed. For `stream`, the partition count is the maximum number of parallel instances in one group.
- `priority`: the partitions are the priority levels (2–10, default 3). A message goes to the partition of its level whatever its key; SDK: `message(..).priority(n)`, REST/other clients: header `flux-priority`.
- In a cluster every partition has a leader and replicas; see [Nodes and cluster](/docs/nodes).

## Consumer group {#groups}

A **consumer group** is a named reader of a channel with its own position (offset) in each partition. A group appears when the first consumer subscribes with that name.

- A new group's position comes from `start`: `Latest` (default — only new messages), `Earliest` (everything still retained) or a point in time. For an existing group `start` is ignored — move it with **Reset offsets**.
- Positions are checkpointed to disk every second. After a crash, some already acknowledged messages (up to ~1 s) can be delivered again.
- **Lag** — how many messages the group has not processed yet. It is the key health signal for consumers.
- System groups: `$router` (the built-in router) and `$webhook:<name>` (HTTP webhooks) are ordinary groups; their lag and DLQ show up like any other.

## Acknowledgements, retries and the DLQ {#acks}

:::diagram delivery

- A consumer receives a message and answers **ack** (done) or **nack** with a reason: `nack(reason, retry = true)` — try again later, `nack(reason, retry = false)` — straight to the DLQ.
- **Prefetch** — how many unacknowledged messages an instance may hold at once.
- **Ack timeout** (default 30 s) — without an ack for longer than that, the delivery counts as a failed attempt.
- **Retry policy** (default): 5 attempts, backoff `1s, 10s, 1m, 5m` — the delay before attempt 2, 3, …; the last value repeats.
- **Dead-letter queue (DLQ)** — when attempts run out, the message goes to the channel's DLQ with its last reason (`PostgreSQL timeout`, `HTTP 503`…). From there you can retry, discard or export it. With the DLQ turned off for a channel, such messages are dropped.
- **Cumulative mode** (`stream` only, broker 0.7+): the consumer commits a position per partition instead of acking each message. The cheapest mode, but `nack`, ack timeouts and the DLQ don't apply. See [Connect a client](/docs/howto-connect-client#ack-modes).

> [!WARNING]
> Delivery is **at-least-once**: after a publish retry, a crash or a rewind a message can arrive again. Make handlers idempotent, for example by a business key.

## Retention {#retention}

- Retention is **by age** (default 7 days, or forever) and/or **by channel size** (default unlimited).
- A partition's log is made of segments (64 MiB by default). Retention deletes only whole closed segments, oldest first, in small portions every second — so a channel can briefly exceed its limit by a few segments.
- In `queue` mode a message is also removed once every group has acked it.
- If a group falls so far behind that retention deletes messages it hasn't read, they are **skipped** — visible in `fluxmq_group_retention_skipped_messages_total`.

## Durability {#durability}

- `--fsync group-commit` (default): a publish is acknowledged **only after fsync**. Writes are batched for up to 2 ms, so an acknowledged message survives even a power cut.
- `--fsync os`: acknowledged right after the write to the OS — several times faster, but the last messages can be lost on power failure. For development and data you can afford to lose.
- In a cluster a publish is acknowledged once **every** in-sync replica has it (ISR, `acks=all`); consumers only see replicated data. `--min-isr` is the minimum ISR size to accept a publish.

## Message headers {#headers}

A message has a key, headers and a payload. The console understands a few headers:

| Header | Meaning |
|---|---|
| `flux-type` | event type (`OrderCreated`) — the message title in the list and the inspector |
| `flux-producer` | the sending client's name (the SDK sets it) |
| `flux-trace-id` | trace id for distributed tracing |
| `flux-routed-from`, `flux-route-id`, `flux-route-path` | where a routed copy came from and which route made it (see [Routing](/docs/routing)) |
