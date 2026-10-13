---
title: Glossary
group: reference
order: 6
summary: FluxMQ and console terms in English and Russian, with their Kafka and RabbitMQ counterparts.
---

| Term | Russian (console) | Meaning |
|---|---|---|
| Channel | Канал (топик) | a named stream of messages; the counterpart of a Kafka topic or a RabbitMQ queue |
| Mode | Режим | `stream`, `queue` or `keyed` — see [Core concepts](/docs/concepts#modes) |
| Partition | Раздел (партиция) | a slice of a channel with its own ordered log; the unit of parallelism and replication |
| Offset | Смещение | a message's number within a partition; a group's position is the offset it has read up to |
| Producer | Отправитель | a service that publishes messages |
| Consumer | Получатель | a service that reads messages |
| Consumer group | Группа получателей | a named reader of a channel; every group gets every message, instances inside a group share the work |
| Instance | Экземпляр | one running copy of a consumer in a group |
| Ack | Подтверждение | "message processed" |
| Nack | Отказ | "not processed", with a reason: retry or straight to the DLQ |
| Prefetch | Prefetch | how many unacknowledged messages an instance may hold |
| Ack timeout | Тайм-аут подтверждения | after this a delivery without an ack counts as a failed attempt |
| Retry, backoff | Повтор, пауза | redelivery after a failure and the delay before it |
| Dead-letter queue (DLQ) | Очередь недоставленных (DLQ) | where messages that exhausted their attempts go, with the reason |
| Lag | Отставание | how many messages a group hasn't processed yet |
| Retention | Хранение | how long / how much a channel keeps |
| Segment | Сегмент | a file of a partition's log; retention deletes whole segments |
| Replay | Переигрывание | publishing a copy of a stored message into a channel |
| Reset offsets | Сброс смещений | moving a group's position to the start, the end or a point in time |
| Route | Маршрут | a rule copying matching messages into other channels |
| Webhook | Вебхук | delivering a channel's messages as POST requests to an HTTP endpoint |
| Alert | Оповещение | a problem detected by the broker's health rules |
| Event | Событие | a fact: channel created, consumer disconnected, etc. |
| Notifier | Канал уведомлений | where alerts are sent: Telegram, Slack, email, PagerDuty… |
| Notification rule | Правило уведомлений | which alerts and events go where |
| Node | Узел | one `fluxmq` process in a cluster |
| Controller | Контроллер | the Raft leader node; places partitions and fails them over |
| Leader, replica | Лидер, реплика | the node accepting a partition's writes, and the nodes holding copies |
| ISR | ISR | in-sync replicas; a publish is acknowledged once all of them have it |
| Under-replicated | Недореплицирован | the ISR has fewer replicas than assigned |
| Offline | Недоступен | the partition has no live leader |
| Replication factor (RF) | Фактор репликации | how many copies each partition has |
| Drain | Снятие лидерства | moving a node's partition leaderships away before maintenance |
| Decommission | Вывод узла | moving every replica away and removing the node from the cluster |
| Rebalance | Ребалансировка | repairing and evening out replica placement |
| Access token | Токен доступа | an `fmq_…` secret with a role and scopes |
| Role | Роль | viewer, operator, admin |
| Scopes | Области действия | channel globs for publishing and consuming over the client protocol |
| Certificate binding | Привязка сертификата | mapping a client certificate's CN to a role |
| Record batch | Пачка записей | the 0.7 format: a batch of messages written and replicated as a whole |
| Group commit | Group commit | batching writes so one fsync precedes the acknowledgement |
