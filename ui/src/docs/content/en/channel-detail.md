---
title: Channel page
group: console
order: 3
summary: A channel's overview, message search and inspector, live tail, replay, consumer groups, routes, storage and settings.
console: /channels
---

Open it by clicking a channel in the list, from the command palette (<kbd>⌘</kbd>/<kbd>Ctrl</kbd> <kbd>K</kbd>) or via `/channels/<name>`. At the top: the name, the mode, a **system · read-only** badge on system channels, the health and a **Search messages** button.

## Channel metrics {#metrics}

| Metric | Meaning |
|---|---|
| Publish rate | messages per second; the subtitle is how many were consumed (acked by every group) |
| Publish p99 | 99th percentile of publish → durable on disk |
| Consumers | connected instances and the number of groups |
| Lag | the **maximum** over the channel's groups (not the sum) |
| Stored | bytes on disk and the number of retained messages |
| Dead letters | messages in the channel's DLQ; "needs review" when non-zero |

## Overview {#overview}

- **Traffic** — three lines: published, consumed (acked) and delivered to all groups (redeliveries included). If delivered is well above published × number of groups, there are many retries.
- **Configuration** — how the mode behaves, partitions, ordering key, retention, retries, ack timeout and the DLQ; Edit opens the settings.
- **Latency** — p50 / p95 / p99 / p99.9 / max for "Publish → durable" (what the producer waits for) and "Append → first delivery" (how fast a message reaches a consumer). Above it: retries and dead-letters per second.
- **Consumer groups** — each group with its instances, ack rate, lag and status.

## Messages {#messages}

The tab has two modes: **Search** stored messages and **Live tail**.

### Search {#search}

| Field | How it works |
|---|---|
| Filter | conditions in the [filter language](/docs/ref-filter), e.g. `order_id = 8842 AND shipping.country = "DE"`; the server checks it as you type and underlines the error position; the Examples menu suggests conditions from real messages' fields |
| Key | exact key match; only the key's partition is searched |
| Partition | all or one partition |
| From offset | read forward from that offset (together with a partition) |
| Time | Latest, Last 15 min, 1 h, 24 h or Custom with from–to dates |

The results header shows matches, how many messages were **scanned** and the order. There is no payload index: a filter scans messages until it fills a page of matches, at most 200,000 per query (about 0.1 s). When the limit is hit, a "scan limit reached" hint appears — narrow the time range, partition or key. **Create route from filter** opens the route editor with that condition.

The list is virtualised; move with the arrow keys or <kbd>j</kbd>/<kbd>k</kbd>; "Load older" is at the bottom.

### The message inspector {#inspector}

The selected message opens on the right (in a drawer on narrow screens):

- the title is the `flux-type`, with the key next to it;
- **Copy payload**, **Copy as JSON**, **Replay…**;
- metadata: timestamp, size, partition, offset, producer (`flux-producer`), trace (`flux-trace-id`, click to copy);
- **Payload** — highlighted JSON, text or binary;
- **Headers** — all other headers;
- **Delivery timeline** per group: published → attempt 1 → nacked with a reason or ack timeout → backoff → attempt 2 … → acked or moved to the DLQ.

> [!NOTE]
> The delivery timeline lives in broker memory — for the last 100,000 messages of a channel; in a cluster only on the node that delivered. Older messages may have none.

### Replay {#replay}

**Replay…** publishes a **copy** of the message — same key, headers and payload — to the channel you pick: this one or another. Consumers of the target channel receive it as a new message (new offset). Use it to reprocess a message, move it to another channel or "catch up" a route created later. Requires the operator role; recorded in the audit log.

### Live tail {#tail}

The broker sends a **sample** of new messages, not the full firehose: the header reads "Showing X samples/sec of Y messages/sec". There are **Pause** / **Resume** and **Clear**; click a message to fetch it in full into the inspector. Handy to confirm messages flow and look as expected.

## Consumers {#consumers-tab}

A card per consumer group of the channel, problems first. What the numbers mean, how to read a "slow instance" reason, how to **reset offsets** and **delete a group**: see [Consumers](/docs/consumers).

## Routing {#routing-tab}

- **Routes from the channel** — rules that read this channel (evaluated by the `$router` system group), with a **New route from …** button.
- **Routed here from** — other channels' routes that write into this one. Copies carry a `flux-routed-from` header.

Details: [Routing](/docs/routing).

## Live {#live-tab}

How many browsers and devices are subscribed to the channel over WebSocket / SSE right now, a **Try it** panel (a real subscription from the console, read-only) and ready-made SSE, WebSocket and curl code. Details: [Live subscriptions](/docs/live#live-tab).

## Storage {#storage}

| Stat | Meaning |
|---|---|
| Stored | the channel's bytes on disk |
| Retained messages | sum over partitions (high minus low offset) |
| Segments | number of segment files |
| Oldest message | age of the oldest retained message |
| Retention | the age limit and, if set, the size limit with a fill bar (amber > 80 %, red > 90 %) |

The partition table: low and high offset, message count, segments, publish rate and size with a bar relative to the largest partition. Heavily skewed sizes usually mean uneven keys: a single "hot" key always lands in one partition.

## Settings {#settings-tab}

**Retention & delivery** — applies immediately, no restart, every change goes to the audit log (admin role):

- keep messages for and size limit (the oldest segments are dropped first);
- max delivery attempts, backoff between attempts, ack timeout;
- dead-letter queue: on — exhausted messages go to the DLQ; off — they are dropped.

**Fixed at creation** — mode, partitions, ordering key and creation date (read only).

**Delete channel** — permanently deletes every message, group offset and the channel's DLQ; connected consumers are disconnected. You confirm by typing the channel name. System channels have the form locked and no delete block.
