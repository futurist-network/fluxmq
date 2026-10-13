---
title: Метрики Prometheus
group: reference
order: 3
summary: Все метрики эндпоинта /metrics — узел, каналы, группы, маршрутизатор, кластер, TLS — с типами и метками, настройка сбора и полезные запросы.
---

`GET /metrics` есть на каждом узле: текстовый формат Prometheus 0.0.4, **только метрики этого узла**. В кластере собирайте со всех узлов и суммируйте в PromQL. При включённой авторизации нужен токен с любой ролью (хватит наблюдателя), передаётся как Bearer.

```yaml
# prometheus.yml
scrape_configs:
  - job_name: fluxmq
    scrape_interval: 15s
    metrics_path: /metrics
    authorization:            # только если включены токены
      type: Bearer
      credentials_file: /etc/prometheus/fluxmq.token
    static_configs:
      - targets: ["fluxmq-1:7421", "fluxmq-2:7421", "fluxmq-3:7421"]
```

Метки только `channel`, `group`, `node` (плюс `severity` у `fluxmq_alerts_open` и `surface` у TLS) — число рядов не растёт с числом получателей. Задержки — гистограммы в секундах с корзинами от 100 мкс до 60 с; `_sum` приблизительный (HDR-гистограмма не хранит точную сумму).

## Узел {#node}

| Метрика | Тип | Смысл |
|---|---|---|
| `fluxmq_node_info` | gauge | всегда 1; метки `cluster`, `version` |
| `fluxmq_node_start_time_seconds` | gauge | время старта (unix) |
| `fluxmq_node_uptime_seconds` | gauge | время работы |
| `fluxmq_node_connections` | gauge | открытые клиентские соединения |
| `fluxmq_node_cpu_percent` | gauge | загрузка CPU процессом |
| `fluxmq_node_memory_bytes`, `fluxmq_node_memory_total_bytes` | gauge | память процесса и всего |
| `fluxmq_node_disk_used_bytes`, `fluxmq_node_disk_total_bytes` | gauge | заполнение диска с каталогом данных |
| `fluxmq_node_storage_bytes` | gauge | объём данных брокера |
| `fluxmq_node_fsync_p99_seconds` | gauge | p99 fsync за окно телеметрии (предупреждение > 50 мс) |
| `fluxmq_node_fsync_latency_seconds` | histogram | длительность fsync с момента старта |
| `fluxmq_node_health`, `fluxmq_health` | gauge | здоровье узла и общее: 0 — норма, 1 — внимание, 2 — критично |
| `fluxmq_alerts_open` | gauge | открытых оповещений; метка `severity` |

## Каналы {#channel}

Метка `channel`.

| Метрика | Тип | Смысл |
|---|---|---|
| `fluxmq_channel_published_messages_total`, `fluxmq_channel_published_bytes_total` | counter | опубликовано сообщений / байт |
| `fluxmq_channel_delivered_messages_total`, `fluxmq_channel_delivered_bytes_total` | counter | доставлено всем группам (с повторами) |
| `fluxmq_channel_acked_total`, `fluxmq_channel_nacked_total` | counter | подтверждения и отказы всех групп |
| `fluxmq_channel_timed_out_total` | counter | доставки с истёкшим тайм-аутом подтверждения |
| `fluxmq_channel_retries_total` | counter | повторные доставки |
| `fluxmq_channel_publish_duplicates_total` | counter | повторно присланные пачки, которые брокер узнал и не записал второй раз (защита от дублей, v0.12; счётчик узла) |
| `fluxmq_channel_dead_lettered_total` | counter | отправлено в DLQ |
| `fluxmq_channel_dlq_messages` | gauge | текущий размер DLQ |
| `fluxmq_channel_storage_bytes`, `fluxmq_channel_messages`, `fluxmq_channel_partitions` | gauge | объём, число хранимых сообщений, разделов |
| `fluxmq_channel_lag_messages` | gauge | отставание (максимум по группам) |
| `fluxmq_channel_publish_latency_seconds` | histogram | публикация → запись на диск |
| `fluxmq_channel_delivery_latency_seconds` | histogram | запись → первая доставка |

## Группы получателей {#group}

Метки `channel`, `group`.

| Метрика | Тип | Смысл |
|---|---|---|
| `fluxmq_group_lag_messages` | gauge | отставание группы |
| `fluxmq_group_in_flight_messages` | gauge | выдано, но не подтверждено |
| `fluxmq_group_retry_pending_messages` | gauge | ждут повтора |
| `fluxmq_group_consumers` | gauge | подключённых экземпляров |
| `fluxmq_group_delivered_messages_total` | counter | доставлено группе |
| `fluxmq_group_acked_total`, `fluxmq_group_nacked_total` | counter | подтверждения и отказы |
| `fluxmq_group_dead_lettered_total` | counter | отправлено в DLQ |
| `fluxmq_group_retention_skipped_messages_total` | counter | удалено хранением раньше, чем группа прочитала (пропущено) |
| `fluxmq_group_catchup_read_bytes_total` | counter | байт, прочитанных с диска при догоне (вне кэша недавних пачек) |
| `fluxmq_group_processing_latency_seconds` | histogram | время обработки (получение → ack/nack, от SDK) |

## Маршрутизатор {#router}

Суммы по всем маршрутам узла, метка `node`: `fluxmq_router_routes` (gauge), `fluxmq_router_evaluated_total`, `fluxmq_router_matched_total`, `fluxmq_router_forwarded_total`, `fluxmq_router_failed_total` (counter).

## Кластер {#cluster}

Только в кластерном режиме, метка `node` (gauge): `fluxmq_cluster_is_controller`, `fluxmq_cluster_nodes`, `fluxmq_cluster_nodes_up`, `fluxmq_cluster_under_replicated_partitions`, `fluxmq_cluster_offline_partitions`, `fluxmq_cluster_raft_term`, `fluxmq_cluster_metadata_epoch`.

## TLS {#tls}

Только когда TLS настроен, метка `surface` (`client`, `http`, `cluster`):

| Метрика | Тип | Смысл |
|---|---|---|
| `fluxmq_tls_handshake_failures_total` | counter | неудачные рукопожатия |
| `fluxmq_tls_cert_expiry_seconds` | gauge | секунд до истечения сертификата (отрицательное — истёк) |

## Полезные запросы {#queries}

```promql
# Публикаций в секунду по каналам (сумма по узлам)
sum by (channel) (rate(fluxmq_channel_published_messages_total[1m]))
# Отставание групп
sum by (channel, group) (fluxmq_group_lag_messages)
# p99 задержки публикации
histogram_quantile(0.99, sum by (le, channel) (rate(fluxmq_channel_publish_latency_seconds_bucket[5m])))
# Есть недоступные разделы
max(fluxmq_cluster_offline_partitions) > 0
# Сертификат истекает меньше чем через 14 дней
min by (node, surface) (fluxmq_tls_cert_expiry_seconds) < 14 * 86400
# Группа теряет сообщения из-за хранения
increase(fluxmq_group_retention_skipped_messages_total[10m]) > 0
```
