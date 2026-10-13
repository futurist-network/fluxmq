---
title: REST API
group: reference
order: 2
summary: Обзор HTTP-эндпоинтов брокера — каналы, сообщения, группы, DLQ, маршруты, вебхуки, уведомления, бэкапы, токены, кластер — и нужные роли.
---

Всё, что делает консоль, доступно через REST на порту 7421. Тела и ответы — JSON. При включённой авторизации передавайте `Authorization: Bearer <токен>`. Колонка «Роль» — минимальная роль: R — наблюдатель, O — оператор, A — администратор.

```bash
H=http://localhost:7421
curl -s $H/api/health
curl -s -H "Authorization: Bearer $TOKEN" $H/api/channels
```

Ошибки всегда приходят как `{"error": "<код>", "message": "…"}` — коды в статье [Коды ошибок](/docs/ref-errors).

## Каналы и сообщения {#channels}

| Метод и путь | Что делает | Роль |
|---|---|---|
| `GET /api/health` | состояние, версия, id узла, время работы, необязательные `features` сборки (`profiling`) | открыт |
| `GET /api/channels` | список каналов | R |
| `POST /api/channels` | создать канал (`name`, `mode`, `partitions`, `retention`, `retry`, `dlq_enabled`, `ordering_key`) | A |
| `GET /api/channels/{name}` | конфигурация канала | R |
| `PATCH /api/channels/{name}` | изменить хранение, повторы, DLQ | A |
| `DELETE /api/channels/{name}` | удалить канал | A |
| `POST /api/channels/{name}/publish` | опубликовать: `{"messages":[{"key","headers","payload":{"encoding":"utf8"\|"base64","data"},"delay_ms"?}]}` (`delay_ms` / `deliver_at_ms` — [отложить](/docs/delayed)) | O |
| `GET /api/channels/{name}/messages` | поиск: `key`, `partition`, `offset`, `since_ms`, `until_ms`, `limit`, `newest_first`, `filter` | R |
| `GET /api/channels/{name}/messages/{partition}/{offset}` | одно сообщение и история его доставок | R |
| `POST /api/channels/{name}/replay` | переиграть сообщения в канал | O |

## Группы получателей {#groups}

| Метод и путь | Что делает | Роль |
|---|---|---|
| `POST /api/channels/{name}/groups/{group}/reset` | сброс смещений: `{"to":"earliest"\|"latest"\|{"timestamp_ms":…}}`; 404 — группы нет | O |
| `DELETE /api/channels/{name}/groups/{group}` | удалить группу; 409 — есть получатели или это `$router` с маршрутами | A |

## Очередь недоставленных {#dlq}

| Метод и путь | Что делает | Роль |
|---|---|---|
| `GET /api/dlq/summary` | число сообщений по причинам | R |
| `GET /api/dlq` | сообщения: `channel`, `reason`, `limit`, `before` | R |
| `POST /api/dlq/retry` | повторить: ровно один из `ids`, `reason` (+`channel`), `channel`, `all` | O |
| `POST /api/dlq/discard` | удалить, те же селекторы | O |
| `GET /api/dlq/export` | выгрузка NDJSON | O |

## Наблюдение {#observability}

| Метод и путь | Что делает | Роль |
|---|---|---|
| `GET /api/history?metric=…&range=5m\|15m\|1h\|24h\|7d` | история метрики (`ingress_rate`, `egress_rate`, `ack_rate`, `publish_p50/p95/p99`, `lag`, `retries_rate`, `dlq_rate`, `consumers`, `storage_bytes`; для канала — `channel.<имя>.<метрика>`) | R |
| `GET /api/alerts` | открытые оповещения | R |
| `GET /api/events/feed` | лента событий: `after`=курсор\|`latest` или `tail`=1–1000, `limit`, `wait_ms` (long-poll) | R |
| `GET /api/audit?limit=N` | журнал действий, сначала новые | R |
| `GET /api/search?q=…` | поиск каналов и групп (палитра команд) | R |
| `GET /api/tls` | состояние TLS узла | R |
| `GET /metrics` | метрики Prometheus этого узла | R |
| `GET /api/debug/pprof/profile?seconds=30&hz=99&format=svg\|folded` | профиль CPU этого узла (флейм-граф или свёрнутые стеки); только в сборке с `--features profiling`, по одному за раз (409, пока идёт другой). Есть и на странице «Узлы» | A |
| `GET /ws` | WebSocket-телеметрия консоли (токен — заголовком или `?token=`) | R |

## Маршруты и фильтры {#routes}

| Метод и путь | Что делает | Роль |
|---|---|---|
| `GET /api/routes`, `GET /api/routes/{id}` | маршруты | R |
| `POST /api/routes` | создать (`name`, `source`, `targets`, `conditions`) | A |
| `PATCH /api/routes/{id}`, `DELETE /api/routes/{id}` | изменить (например `{"enabled":false}`), удалить | A |
| `POST /api/routes/test` | проверить условия на хранимых сообщениях (`window_ms`, `sample_limit`) | R |
| `GET /api/routes/stats` | счётчики маршрутов этого узла | R |
| `POST /api/filter/parse` | текст фильтра → условия (400 с `position` при ошибке) | R |
| `POST /api/filter/render` | условия → текст | R |

## Вебхуки {#webhooks}

| Метод и путь | Что делает | Роль |
|---|---|---|
| `GET /api/webhooks`, `GET /api/webhooks/{id}` | подписки (секреты — `********`) | R |
| `POST /api/webhooks` | создать | A |
| `PATCH /api/webhooks/{id}`, `DELETE /api/webhooks/{id}` | изменить (`********` оставляет секрет, `""` удаляет), удалить | A |
| `POST /api/webhooks/test` | проверить черновик, ничего не сохраняя | A |
| `POST /api/webhooks/{id}/test` | тестовая доставка (тело — необязательная правка) | A |
| `GET /api/webhooks/stats` | счётчики, сумма по узлам | R |
| `GET /api/webhooks/{id}/attempts` | 100 последних запросов на узел, новые сверху (память узла) | R |
| `GET /api/webhooks/{id}/history?range=15m\|1h\|24h` | доставленные / неудачные сообщения и задержка по интервалам, сумма по узлам | R |

## Отложенные сообщения {#scheduled}

| Метод и путь | Что делает | Роль |
|---|---|---|
| `POST /api/channels/{name}/publish` с `delay_ms` / `deliver_at_ms` у элемента | доставить позже (не дальше 30 дней); вернувшийся номер — номер в расписании | O |
| `GET /api/scheduled?channel=&limit=` | ближайшие отложенные сообщения (все узлы) и счётчики по каналам | R |
| `DELETE /api/scheduled/{partition}/{offset}` | отменить (404, если уже отправлено или отменено) | O |
| `POST /api/scheduled/{partition}/{offset}/release` | отправить сейчас | O |

Подробно — [Отложенные сообщения](/docs/delayed).

## Живые подписки {#live}

| Метод и путь | Что делает | Роль |
|---|---|---|
| `GET /api/channels/{name}/live` | живая подписка по WebSocket (токен: подпротокол `fluxmq.token.<token>` или `Authorization`) | subscribe¹ |
| `GET /api/channels/{name}/events` | живая подписка по SSE (токен: `Authorization` или `?access_token=`) | subscribe¹ |
| `GET /api/live/status` | состояние по узлам: лимиты, соединения, подписки по каналам, счётчики | R |

¹ `viewer` и выше или токен `subscriber`, у которого `scopes.subscribe` подходит к каналу. Подробно — [Живые подписки](/docs/live).

## Уведомления {#notify}

| Метод и путь | Что делает | Роль |
|---|---|---|
| `GET /api/notify/notifiers`, `GET …/notifiers/{id}` | каналы уведомлений | R |
| `POST /api/notify/notifiers`, `PATCH`/`DELETE …/notifiers/{id}` | создать, изменить, удалить (409, если используется в правиле) | A |
| `POST /api/notify/notifiers/test`, `POST …/notifiers/{id}/test` | тестовая отправка черновика / сохранённого | A |
| `GET /api/notify/rules`, `GET …/rules/{id}` | правила | R |
| `POST /api/notify/rules`, `PATCH`/`DELETE …/rules/{id}` | создать, изменить, удалить | A |
| `GET /api/notify/history?limit=N` | история отправок (по умолчанию 1000, до 10 000) | R |

## Резервные копии {#backups}

| Метод и путь | Что делает | Роль |
|---|---|---|
| `GET /api/backups/config`, `PUT /api/backups/config` | настройки (секреты — `********`) | R / A |
| `GET /api/backups/status` | идущий бэкап, последний успех и ошибка, следующий запуск | R |
| `POST /api/backups/run` | запустить сейчас (202; 409 — уже идёт) | A |
| `POST /api/backups/test-target` | проверить хранилище | A |
| `GET /api/backups`, `GET /api/backups/{id}` | история, манифест (`latest` — последний) | R |
| `DELETE /api/backups/{id}` | удалить бэкап | A |

## Токены и привязки {#auth}

| Метод и путь | Что делает | Роль |
|---|---|---|
| `GET /api/auth/me` | кто я: роль, режим open/enforced | любой |
| `GET /api/auth/tokens`, `POST /api/auth/tokens` | список, выпуск (`name`, `role`, `scopes`, `ttl_secs`) | A |
| `DELETE /api/auth/tokens/{id}` | отозвать (409 `last_admin`) | A |
| `GET`/`POST /api/auth/cert-bindings`, `DELETE /api/auth/cert-bindings/{cn}` | привязки сертификатов | A |

## Кластер {#cluster}

| Метод и путь | Что делает | Роль |
|---|---|---|
| `GET /api/cluster/metadata` | топология и назначения разделов | R |
| `GET /api/cluster/status` | Raft, недореплицированные и недоступные разделы, HW/ISR | R |
| `POST /api/cluster/partitions/reassign` | переназначить реплики раздела | A |
| `POST /api/cluster/nodes/{id}/drain` | снять лидерство | A |
| `POST /api/cluster/nodes/{id}/decommission[?dry_run=true]` | вывести узел (или план) | A |
| `GET /api/cluster/nodes/{id}/decommission`, `GET /api/cluster/decommissions` | прогресс, текущие и недавние выводы | R |
| `DELETE /api/cluster/nodes/{id}` | забыть мёртвый узел | A |
| `GET /api/cluster/rebalance` | состояние ребалансировки | R |
| `PUT /api/cluster/rebalance/config` | настройки ребалансировки | A |
| `POST /api/cluster/rebalance` | запустить (`{"dry_run":true}` — предпросмотр) | A |
| `POST /api/cluster/rebalance/{pause,resume,cancel}` | пауза, продолжение, отмена | A |

Любой другой маршрут: `GET`/`HEAD` — наблюдатель, остальное — администратор (запрещено по умолчанию). На одиночном узле кластерные изменения отвечают `400 unsupported_single_node`.
