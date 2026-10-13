---
title: Alerts and events
group: console
order: 8
summary: Open alerts with a likely cause, every alert kind and when it fires, and the broker's events feed.
console: /alerts
---

The broker checks its health rules every second and opens **alerts** itself. No separate Alertmanager is needed; delivery to Telegram, Slack and so on is set up in [Notifications](/docs/notifications).

## The Alerts screen {#screen}

On the left, cards of open alerts, critical first. A card shows the title ("Email queue is growing rapidly"), severity (Critical or Warning), channel and group, how long it has been open, what happened and a **Possible reason**; **Inspect** opens the screen where the problem is easiest to see (the consumer group, the DLQ, the nodes).

On the right, **History**: the broker's latest events (see the feed below).

An alert **opens** after three failing checks in a row and **resolves by itself** after ten clean ones — that prevents flapping. Nothing needs closing by hand.

## Alert kinds {#kinds}

| Alert | Code | Opens when | Severity |
|---|---|---|---|
| Lag growth | `lag_growth` | a group's lag grows fast | warning / critical |
| Consumer stalled | `consumer_stalled` | a group stopped acking while there is work | critical |
| No consumers | `no_consumers` | a group with lag has no instance | critical |
| DLQ growth | `dlq_growth` | messages keep arriving in the DLQ | warning |
| High retry rate | `high_retry_rate` | a high share of retries / nacks | warning |
| Slow instance | `slow_instance` | an instance's ack rate < 50 % of the group median, or its processing p99 is 3× the group's | warning |
| Storage nearly full | `storage_nearly_full` | disk > 80 % full (warning) or > 90 % (critical) | warning / critical |
| High publish latency | `high_publish_latency` | a channel's publish p99 > 100 ms while it has traffic, or a node's fsync p99 > 50 ms ("Disk latency is slowing down publishes") | warning |
| Node down | `node_down` | a cluster node stopped responding (cluster only) | critical |
| Under-replicated partitions | `under_replicated_partitions` | the ISR has fewer replicas than assigned (cluster only) | warning |
| Offline partitions | `offline_partitions` | a partition has no live leader (cluster only) | critical |
| TLS certificate expiring | `tls_cert_expiring` | < 14 days left (warning), < 3 days or expired (critical) | warning / critical |
| TLS reload failed | `tls_reload_failed` | a new certificate file didn't load; the old one keeps serving | warning |

The codes are used in notification rules (`alert_kinds`) and in `GET /api/alerts`. Group health thresholds: [Consumers](/docs/consumers#health).

## Events feed {#events}

Events are facts, not problems: "channel created", "consumer disconnected", "message dead-lettered". They show in History here and in Recent events on the [Dashboard](/docs/dashboard).

| Area | Events |
|---|---|
| Channels | `channel_created`, `channel_deleted`, `channel_updated` |
| Consumers | `consumer_connected`, `consumer_disconnected` |
| Alerts | `alert_opened`, `alert_resolved` |
| DLQ and messages | `dead_lettered`, `dlq_retried`, `dlq_discarded`, `messages_replayed` |
| Cluster | `controller_changed`, `node_down`, `node_up` |
| Rebalancing | `rebalance_started`, `replica_repair_started`, `partition_moved`, `rebalance_completed`, `rebalance_failed` |
| Decommissioning | `node_decommission_started`, `node_decommissioned`, `node_decommission_failed`, `node_forgotten` |
| TLS | `tls_reload_failed` |

The broker also appends every alert transition and event to the system channel `$system.events` (kept 7 days). External systems long-poll it through `GET /api/events/feed` — see [Notifications](/docs/notifications#pull-feed).

## For external monitoring {#external}

```bash
curl -s http://localhost:7421/api/alerts          # open alerts (JSON)
curl -s "http://localhost:7421/api/events/feed?tail=20"
curl -s http://localhost:7421/metrics | grep fluxmq_alerts_open
```

`fluxmq_alerts_open{severity}` lets you build your own Prometheus alert — see [Metrics](/docs/ref-metrics).
