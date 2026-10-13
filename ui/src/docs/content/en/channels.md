---
title: Channels
group: console
order: 2
summary: The channel list with its metrics and the create-channel wizard — what every column and field means.
console: /channels
---

The Channels screen lists every channel on the broker. Problem channels sort to the top and are highlighted. Click a row to open the [channel page](/docs/channel-detail).

## The list {#list}

Above the table: a name filter and a mode switch with counts — All, Stream, Queue, Keyed, Priority. The filter lives in the page URL, so you can share the link.

| Column | Meaning |
|---|---|
| Channel | the name; system channels (`$system.*`) carry a **system** badge — read only |
| Mode | Stream (`stream`), Queue (`queue`), Keyed (`keyed`) or Priority (`priority`) |
| Rate (msg/s) | publishes per second with a sparkline of the last minutes |
| Lag | the largest lag among the channel's consumer groups |
| p99 | 99th percentile publish latency (until durable on disk) |
| Consumers | connected instances |
| Stored | the channel's data on disk |
| DLQ | messages in the dead-letter queue |
| Status | the worst health of the channel's groups: Healthy, Warning, Critical |

> [!TIP]
> Lag is highlighted only when the channel is already unhealthy and lag exceeds 1,000. A large but stable lag on a channel that is read in bursts is no reason to worry; growth is.

## Creating a channel {#create}

**Create channel** (here, in the top bar or in the command palette) opens a two-step wizard. Requires the admin role.

### Step 1. What do you need? {#create-mode}

| Choice | Mode | Examples |
|---|---|---|
| Store events and replay them later (recommended) | `stream` | `orders.events`, `audit.logs`, clickstream |
| Distribute jobs between workers | `queue` | `email`, `image-resize`, `webhooks` |
| Preserve order per customer / order | `keyed` | `payments.events` keyed by `customer_id` |
| Urgent first | `priority` | `support.tickets` with `flux-priority` 0–2 |

Fan-out to several consumer groups works in every mode. How the modes differ: [Core concepts](/docs/concepts#modes).

### Step 2. Settings {#create-settings}

| Field | Default | What it sets |
|---|---|---|
| Name | — | letters, digits, `.`, `-`, `_`; up to 128 characters; dots are handy for namespaces |
| Keep messages for | 7 days | 1 hour, 24 hours, 3 days, 7 days, 30 days or forever; queue messages are also removed once every group acked them |
| Partitions | 8 (queue: 1) | 1–1024; the max parallel consumers per group; **fixed after creation** |
| Ordering key | — | for `keyed`, the field producers use as the message key; otherwise informational, shown next to keys |

The **Advanced** section:

| Field | Default | What it sets |
|---|---|---|
| Max delivery attempts | 5 | how many times a message is delivered before it goes to the DLQ |
| Ack timeout (seconds) | 30 | a delivery not acked within this counts as a failed attempt |
| Backoff list | `1s, 10s, 1m, 5m` | delay before attempt 2, 3, …; the last value repeats; units `ms`, `s`, `m`, `h` |
| Dead-letter queue | on | off — messages that exhaust their attempts are dropped |
| Size limit | unlimited | 1 GB, 10 GB, 100 GB, 1 TB; the oldest segments are dropped beyond it |

After creation the new channel's page opens. REST (`POST /api/channels`) and the SDK (`client.ensure_channel(…)`) do the same — see [Create a channel](/docs/howto-create-channel).

## What you can do later {#after}

- Change retention, retries and the DLQ — the channel's [Settings](/docs/channel-detail#settings-tab) tab.
- Delete the channel — same tab, confirmed by typing its name. All messages, group offsets and the channel's DLQ are deleted; connected consumers are disconnected.
- Mode, partition count and ordering key can't change — create a new channel and, if needed, move messages with a [route](/docs/routing) or replay.
