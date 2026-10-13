---
title: Prometheus metrics
group: reference
order: 3
summary: Every metric on /metrics — node, channels, groups, router, cluster, TLS — with types and labels, scrape setup and useful queries.
---

Every node serves `GET /metrics`: Prometheus text format 0.0.4, **this node's metrics only**. In a cluster scrape every node and sum in PromQL. With auth enforced, any role's token will do (viewer is enough), sent as a Bearer token.

```yaml
# prometheus.yml
scrape_configs:
  - job_name: fluxmq
    scrape_interval: 15s
    metrics_path: /metrics
    authorization:            # only when tokens are enabled
      type: Bearer
      credentials_file: /etc/prometheus/fluxmq.token
    static_configs:
      - targets: ["fluxmq-1:7421", "fluxmq-2:7421", "fluxmq-3:7421"]
```

Labels are only `channel`, `group`, `node` (plus `severity` on `fluxmq_alerts_open` and `surface` on TLS) — series don't multiply with consumers. Latencies are histograms in seconds with buckets from 100 µs to 60 s; `_sum` is approximate (an HDR histogram keeps no exact sum).

## Node {#node}

| Metric | Type | Meaning |
|---|---|---|
| `fluxmq_node_info` | gauge | always 1; labels `cluster`, `version` |
| `fluxmq_node_start_time_seconds` | gauge | start time (unix) |
| `fluxmq_node_uptime_seconds` | gauge | uptime |
| `fluxmq_node_connections` | gauge | open client connections |
| `fluxmq_node_cpu_percent` | gauge | process CPU |
| `fluxmq_node_memory_bytes`, `fluxmq_node_memory_total_bytes` | gauge | process and total memory |
| `fluxmq_node_disk_used_bytes`, `fluxmq_node_disk_total_bytes` | gauge | usage of the data directory's disk |
| `fluxmq_node_storage_bytes` | gauge | broker data size |
| `fluxmq_node_fsync_p99_seconds` | gauge | fsync p99 over the telemetry window (warning > 50 ms) |
| `fluxmq_node_fsync_latency_seconds` | histogram | fsync duration since start |
| `fluxmq_node_health`, `fluxmq_health` | gauge | node and overall health: 0 healthy, 1 warning, 2 critical |
| `fluxmq_alerts_open` | gauge | open alerts; label `severity` |

## Channels {#channel}

Label `channel`.

| Metric | Type | Meaning |
|---|---|---|
| `fluxmq_channel_published_messages_total`, `fluxmq_channel_published_bytes_total` | counter | messages / bytes published |
| `fluxmq_channel_delivered_messages_total`, `fluxmq_channel_delivered_bytes_total` | counter | delivered to all groups (redeliveries included) |
| `fluxmq_channel_acked_total`, `fluxmq_channel_nacked_total` | counter | acks and nacks of all groups |
| `fluxmq_channel_timed_out_total` | counter | deliveries whose ack timeout expired |
| `fluxmq_channel_retries_total` | counter | redeliveries |
| `fluxmq_channel_publish_duplicates_total` | counter | retried batches the broker recognized and did not store again (idempotent publishing, v0.12; per node) |
| `fluxmq_channel_dead_lettered_total` | counter | dead-lettered |
| `fluxmq_channel_dlq_messages` | gauge | current DLQ size |
| `fluxmq_channel_storage_bytes`, `fluxmq_channel_messages`, `fluxmq_channel_partitions` | gauge | size, retained messages, partitions |
| `fluxmq_channel_lag_messages` | gauge | lag (maximum over groups) |
| `fluxmq_channel_publish_latency_seconds` | histogram | publish → durable |
| `fluxmq_channel_delivery_latency_seconds` | histogram | append → first delivery |

## Consumer groups {#group}

Labels `channel`, `group`.

| Metric | Type | Meaning |
|---|---|---|
| `fluxmq_group_lag_messages` | gauge | the group's lag |
| `fluxmq_group_in_flight_messages` | gauge | handed out, not yet acked |
| `fluxmq_group_retry_pending_messages` | gauge | waiting for a retry |
| `fluxmq_group_consumers` | gauge | connected instances |
| `fluxmq_group_delivered_messages_total` | counter | delivered to the group |
| `fluxmq_group_acked_total`, `fluxmq_group_nacked_total` | counter | acks and nacks |
| `fluxmq_group_dead_lettered_total` | counter | dead-lettered |
| `fluxmq_group_retention_skipped_messages_total` | counter | removed by retention before the group read them (skipped) |
| `fluxmq_group_catchup_read_bytes_total` | counter | bytes read from disk while catching up (outside the recent-batch cache) |
| `fluxmq_group_processing_latency_seconds` | histogram | processing time (receipt → ack/nack, from the SDK) |

## Router {#router}

Sums over all routes of the node, label `node`: `fluxmq_router_routes` (gauge), `fluxmq_router_evaluated_total`, `fluxmq_router_matched_total`, `fluxmq_router_forwarded_total`, `fluxmq_router_failed_total` (counter).

## Cluster {#cluster}

Cluster mode only, label `node` (gauge): `fluxmq_cluster_is_controller`, `fluxmq_cluster_nodes`, `fluxmq_cluster_nodes_up`, `fluxmq_cluster_under_replicated_partitions`, `fluxmq_cluster_offline_partitions`, `fluxmq_cluster_raft_term`, `fluxmq_cluster_metadata_epoch`.

## TLS {#tls}

Only when TLS is configured, label `surface` (`client`, `http`, `cluster`):

| Metric | Type | Meaning |
|---|---|---|
| `fluxmq_tls_handshake_failures_total` | counter | failed handshakes |
| `fluxmq_tls_cert_expiry_seconds` | gauge | seconds until the certificate expires (negative — expired) |

## Useful queries {#queries}

```promql
# Publishes per second per channel (summed over nodes)
sum by (channel) (rate(fluxmq_channel_published_messages_total[1m]))
# Group lag
sum by (channel, group) (fluxmq_group_lag_messages)
# Publish latency p99
histogram_quantile(0.99, sum by (le, channel) (rate(fluxmq_channel_publish_latency_seconds_bucket[5m])))
# Offline partitions exist
max(fluxmq_cluster_offline_partitions) > 0
# A certificate expires within 14 days
min by (node, surface) (fluxmq_tls_cert_expiry_seconds) < 14 * 86400
# A group loses messages to retention
increase(fluxmq_group_retention_skipped_messages_total[10m]) > 0
```
