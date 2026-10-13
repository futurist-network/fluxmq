---
title: Dashboard
group: console
order: 1
summary: The console's first screen — cluster health, the four key signals, traffic and latency charts, problem channels and recent events.
console: /
---

The Dashboard answers "is everything fine, and if not — where?". Only what needs attention is coloured: if everything is calm and neutral, there is no problem.

## Header: cluster health {#health}

- **Cluster name and health** — Healthy, Warning or Critical. Cluster health is the worst of its channels and nodes. On the right: broker version and uptime.
- **The stats row**:

| Stat | Meaning |
|---|---|
| Nodes | nodes up out of all (`N/M`) |
| Channels | number of channels (topics) |
| Consumers | connected instances and consumer groups |
| Publish p99 | 99th percentile of publish → durable on disk |
| Published | messages per second, all channels |
| Consumed (acked) | acks per second, summed over all groups |
| Storage | data on disk and, when known, disk capacity |

- **The "N issues need attention" strip** — up to three open alerts with an Inspect button and an "Alert center" link (the [Alerts](/docs/alerts) screen).

> [!NOTE]
> "Consumed" can be **higher** than "Published" — that's normal. Consumed is the sum of acks over all groups: when two groups read a channel, each message is acked twice.

## The four key signals {#kpi}

| Card | Value | Subtitle | Turns coloured when |
|---|---|---|---|
| Traffic | publishes per second | delivered to all groups, 1-minute trend | never — it's volume, not a problem |
| Latency p99 | p99 of publish → durable | p50 of the same | a "High publish latency" alert is open (p99 > 100 ms) |
| Lag | messages groups haven't processed yet (sum) | — | "Lag growth", "Consumer stalled" or "No consumers" is open |
| Errors | retries per second | messages in the DLQ and retries in 24 h | "DLQ growth" or "High retry rate" is open |

Card colours follow open alerts (the broker's health rules), not thresholds in the UI. Each card has a 5-minute sparkline. Clicking Lag opens [Consumers](/docs/consumers), clicking Errors opens [Dead Letters](/docs/dlq).

## Charts {#charts}

- **Published vs consumed** — two lines in messages per second. If Consumed stays below Published (allowing for the number of groups), lag is growing.
- **Publish latency** — p50, p95 and p99 in milliseconds.

The range switches between 5m, 15m, 1h and 24h. History is kept for up to 7 days (rolled up 1 s → 1 min → 30 min).

## Channels and events {#channels-events}

- **Channels** — up to eight channels, problems first and highlighted. Columns: mode, rate, lag, p99, status. "View all" opens the full list.
- **Recent events** — channels created and deleted, consumers connecting and disconnecting, alerts opening and resolving, dead-lettered messages, controller changes, rebalancing and so on.

## An empty broker {#empty}

With no channels the charts are replaced by "Your cluster is up — it just has nothing to do yet" and a "Create your first channel" button. To see the console with real load and problems, run `fluxmq-demo` (see [Quick start](/docs/quickstart)).

## What's normal and what's a problem {#normal}

| Signal | Normal | Problem |
|---|---|---|
| Latency p99 | single-digit ms on Linux with NVMe; 7–10 ms and more on macOS because of `F_FULLFSYNC` | consistently > 100 ms — an alert opens; check the disk ([fsync p99 per node](/docs/nodes)) |
| Lag | small and hovering around zero | growing minute after minute — see [Investigate lag](/docs/howto-investigate-lag) |
| Errors | occasional retries | a steady stream of retries or a growing DLQ — see [Handle the DLQ](/docs/howto-handle-dlq) |
| Storage | far from capacity | disk > 80 % is a warning, > 90 % critical |
