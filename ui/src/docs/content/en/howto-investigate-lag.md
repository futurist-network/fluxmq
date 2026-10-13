---
title: Investigate lag
group: howto
order: 4
summary: Step by step from a red Lag card to the cause — no consumers, a stall, a slow instance, retries, or simply not enough capacity.
---

**Lag** is how many messages a consumer group hasn't processed yet. On its own it isn't a problem: a group that reads in batches always trails a little. The problem is lag that **keeps growing** and never returns to zero.

## 1. Find the group {#find}

- [Dashboard](/docs/dashboard): the Lag card turned amber or red — click it.
- [Consumers](/docs/consumers): turn on "Needs attention" — problem groups come first, expanded.
- [Alerts](/docs/alerts): "Lag growth", "Consumer stalled", "No consumers" — Inspect leads straight to the group.

## 2. Identify the kind of problem {#diagnose}

| What the group card shows | Diagnosis | What to do |
|---|---|---|
| Instances = 0 | nobody reads ("No consumers" alert) | start the service; if it runs, check the address, the token (`Unauthorized`/`Forbidden` in its logs), the token's consume scopes |
| Ack rate = 0, In flight = prefetch and not moving | the group is stalled: the handler hangs on an external call or a deadlock | open the instance drawer — last error and CPU; restart the stuck instance; after the ack timeout its messages go to others |
| One instance flagged as slow | processing p99 3× the median or rate < 50 % of the median | instance drawer: high CPU — it lacks resources; high processing with low CPU — a dependency is slow (database, API); a partition with a hot key |
| Many "Retry pending" and nacks | processing keeps failing, messages loop through retries | the instance's last error or the [DLQ](/docs/dlq) shows the cause — fix it |
| All healthy, steady ack rate, but below the publish rate | not enough capacity | add instances (in `stream`/`keyed` at most one per partition), raise `prefetch`, speed up processing |

## 3. Check the mode's limits {#limits}

- **`stream`**: within a group one partition is handled by one instance. Instances beyond the partition count sit idle. The partition count can't change — more parallelism needs a new channel.
- **`keyed`**: a key's next message waits for the previous one's ack. One hot key or one stuck message holds back its whole key — look at the stuck message's [delivery timeline](/docs/channel-detail#inspector).
- **`queue`**: instances compete; adding instances simply helps.

## 4. When catching up isn't possible {#skip}

When the backlog is no longer useful (stale notifications, say), **Reset offsets → Latest**: lag drops to zero and unread messages are not delivered to this group. Conversely, to reprocess history use **Earliest** or **From a point in time**. See [Reset offsets](/docs/consumers#reset).

> [!WARNING]
> Watch retention: if a group falls behind for longer than it, unread messages are deleted and skipped (`fluxmq_group_retention_skipped_messages_total`).

## 5. Ahead of time {#prevent}

- Set up [notifications](/docs/howto-alerts) for `lag_growth`, `consumer_stalled` and `no_consumers`.
- In Prometheus: `sum by (channel, group) (fluxmq_group_lag_messages)` — see [Metrics](/docs/ref-metrics).
