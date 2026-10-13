---
title: Consumers
group: console
order: 4
summary: Every consumer group and its instances — lag, rates, stalled and slow instances, resetting offsets and deleting a group.
console: /consumers
---

The Consumers screen shows **every consumer group of every channel**; lagging and unhealthy groups come first and open expanded. The same cards appear on each channel's Consumers tab.

## Summary and filter {#summary}

At the top: the number of groups, instances, the total ack rate and the **total lag** (with the number of unhealthy groups). The search filters by group or channel name; "Needs attention" keeps only unhealthy groups.

## Group card {#group-card}

| Stat | Meaning | Bad when |
|---|---|---|
| Instances | how many copies of the service are connected to the group | 0 with non-zero lag — critical: nobody reads the messages |
| Lag | messages the group hasn't processed yet | it grows minute after minute |
| Ack rate | acks per second | 0 for more than 30 s while there is work — the group is stalled |
| Processing p99 | time from receipt to ack/nack (reported by the SDK) | jumps — the handler or one of its dependencies is slow |
| In flight | handed to instances, not acked yet | stuck at the prefetch and not draining |
| Retry pending | messages waiting out a backoff before the next attempt | constantly high — processing keeps failing |
| Nacks | nacks per second | nack ratio > 5 % is a warning |

System groups are labelled: "System · router" (`$router`, with a link to the routes) and "System · webhook" (`$webhook:<name>`, with "View webhook").

## Instances {#instances}

The table under the card (expand with the arrow): instance, **node** (in a cluster; the id looks like `n2/worker#7`), rate, lag, in flight, processing p99, assigned partitions and health. When an instance stands out, the health shows why:

- processing p99 N× the group median (more than three times);
- ack rate below 50 % of the group median (with three or more instances);
- no acks — stalled (messages in flight but not a single ack).

Click a row to open the **instance drawer**: process CPU and memory, processing p99, ack rate, total retries, lag, the **last error** (the reason of its last nack, from the SDK), and the connection — client name, remote address (with mTLS also the certificate CN, e.g. `tls:orders-svc`), connected since, assigned partitions, "in flight / prefetch", delivered, nacks.

> [!TIP]
> The SDK reports processing time, CPU, memory and the last error to the broker every 2 seconds — nothing to configure. That's why the cause of a slowdown is usually visible right here, without the service's logs.

## Health rules {#health}

| Health | When |
|---|---|
| Critical | lag but no instance at all; lag grew more than 10× and above 10,000 within 5 minutes; no ack for 30 seconds while there is work |
| Warning | lag above 1,000 and growing for 60 seconds; nack ratio above 5 %; the DLQ grew in the last 5 minutes |

A channel's health is the worst of its groups. The same rules open [alerts](/docs/alerts).

## Reset offsets {#reset}

**Reset offsets** (operator role) moves the group's read position in every partition of the channel:

| Option | What happens |
|---|---|
| Latest | skip the backlog: lag drops to zero, unread messages are not delivered to this group |
| Earliest | redeliver everything still retained; consumers must be idempotent |
| From a point in time | redeliver messages published at or after that time |

Messages currently in flight may be delivered again. Over REST: `POST /api/channels/{channel}/groups/{group}/reset` with `{"to":"earliest"}`, `{"to":"latest"}` or `{"to":{"timestamp_ms":…}}`.

## Delete a group {#delete}

**Delete group** (admin role) is offered only for a group **with no connected instances**. It removes the group's offsets and pending retries; the messages stay in the channel, and a consumer that later joins with that name starts as a new group (from its `start` position). The server answers `409` while consumers are connected, or for the `$router` group of a channel that still has routes.

## What to do if… {#troubleshoot}

- **Lag keeps growing** — see [Investigate lag](/docs/howto-investigate-lag).
- **Zero instances** — the service isn't running or can't connect (check the token and address); a "No consumers" alert opens.
- **One instance is slower than the rest** — open its drawer: CPU, memory and the last error usually explain it at once.
