---
title: REST API
group: reference
order: 2
summary: An overview of the broker's HTTP endpoints — channels, messages, groups, DLQ, routes, webhooks, notifications, backups, tokens, cluster — and the roles they need.
---

Everything the console does is available over REST on port 7421. Bodies and answers are JSON. With auth enforced, send `Authorization: Bearer <token>`. The Role column is the minimum role: R — viewer, O — operator, A — admin.

```bash
H=http://localhost:7421
curl -s $H/api/health
curl -s -H "Authorization: Bearer $TOKEN" $H/api/channels
```

Errors always come as `{"error": "<code>", "message": "…"}` — codes in [Error codes](/docs/ref-errors).

## Channels and messages {#channels}

| Method and path | What it does | Role |
|---|---|---|
| `GET /api/health` | status, version, node id, uptime, optional build `features` (`profiling`) | open |
| `GET /api/channels` | list channels | R |
| `POST /api/channels` | create a channel (`name`, `mode`, `partitions`, `retention`, `retry`, `dlq_enabled`, `ordering_key`) | A |
| `GET /api/channels/{name}` | channel config | R |
| `PATCH /api/channels/{name}` | change retention, retries, DLQ | A |
| `DELETE /api/channels/{name}` | delete a channel | A |
| `POST /api/channels/{name}/publish` | publish: `{"messages":[{"key","headers","payload":{"encoding":"utf8"\|"base64","data"}}]}` (`delay_ms` / `deliver_at_ms`: [deliver later](/docs/delayed)) | O |
| `GET /api/channels/{name}/messages` | search: `key`, `partition`, `offset`, `since_ms`, `until_ms`, `limit`, `newest_first`, `filter` | R |
| `GET /api/channels/{name}/messages/{partition}/{offset}` | one message with its delivery history | R |
| `POST /api/channels/{name}/replay` | replay messages into a channel | O |

## Consumer groups {#groups}

| Method and path | What it does | Role |
|---|---|---|
| `POST /api/channels/{name}/groups/{group}/reset` | reset offsets: `{"to":"earliest"\|"latest"\|{"timestamp_ms":…}}`; 404 — no such group | O |
| `DELETE /api/channels/{name}/groups/{group}` | delete a group; 409 — consumers connected, or `$router` with routes | A |

## Dead-letter queue {#dlq}

| Method and path | What it does | Role |
|---|---|---|
| `GET /api/dlq/summary` | message counts per reason | R |
| `GET /api/dlq` | entries: `channel`, `reason`, `limit`, `before` | R |
| `POST /api/dlq/retry` | retry: exactly one of `ids`, `reason` (+`channel`), `channel`, `all` | O |
| `POST /api/dlq/discard` | discard, same selectors | O |
| `GET /api/dlq/export` | NDJSON export | O |

## Observability {#observability}

| Method and path | What it does | Role |
|---|---|---|
| `GET /api/history?metric=…&range=5m\|15m\|1h\|24h\|7d` | metric history (`ingress_rate`, `egress_rate`, `ack_rate`, `publish_p50/p95/p99`, `lag`, `retries_rate`, `dlq_rate`, `consumers`, `storage_bytes`; per channel `channel.<name>.<metric>`) | R |
| `GET /api/alerts` | open alerts | R |
| `GET /api/events/feed` | events feed: `after`=cursor\|`latest` or `tail`=1–1000, `limit`, `wait_ms` (long-poll) | R |
| `GET /api/audit?limit=N` | audit log, newest first | R |
| `GET /api/search?q=…` | search channels and groups (command palette) | R |
| `GET /api/tls` | this node's TLS status | R |
| `GET /metrics` | this node's Prometheus metrics | R |
| `GET /api/debug/pprof/profile?seconds=30&hz=99&format=svg\|folded` | CPU profile of this node (flame graph or folded stacks); builds with `--features profiling` only, one at a time (409 while one runs). Also on the Nodes page | A |
| `GET /ws` | the console's WebSocket telemetry (token as a header or `?token=`) | R |

## Routes and filters {#routes}

| Method and path | What it does | Role |
|---|---|---|
| `GET /api/routes`, `GET /api/routes/{id}` | routes | R |
| `POST /api/routes` | create (`name`, `source`, `targets`, `conditions`) | A |
| `PATCH /api/routes/{id}`, `DELETE /api/routes/{id}` | change (e.g. `{"enabled":false}`), delete | A |
| `POST /api/routes/test` | test conditions on stored messages (`window_ms`, `sample_limit`) | R |
| `GET /api/routes/stats` | this node's route counters | R |
| `POST /api/filter/parse` | filter text → conditions (400 with `position` on error) | R |
| `POST /api/filter/render` | conditions → text | R |

## Webhooks {#webhooks}

| Method and path | What it does | Role |
|---|---|---|
| `GET /api/webhooks`, `GET /api/webhooks/{id}` | subscriptions (secrets as `********`) | R |
| `POST /api/webhooks` | create | A |
| `PATCH /api/webhooks/{id}`, `DELETE /api/webhooks/{id}` | change (`********` keeps a secret, `""` removes it), delete | A |
| `POST /api/webhooks/test` | test a draft, saving nothing | A |
| `POST /api/webhooks/{id}/test` | a test delivery (optional draft edit as the body) | A |
| `GET /api/webhooks/stats` | counters, summed over nodes | R |
| `GET /api/webhooks/{id}/attempts` | the last 100 requests per node, newest first (node memory) | R |
| `GET /api/webhooks/{id}/history?range=15m\|1h\|24h` | delivered / failed messages and latency per bucket, summed over nodes | R |

## Delayed messages {#scheduled}

| Method and path | What it does | Role |
|---|---|---|
| `POST /api/channels/{name}/publish` with `delay_ms` / `deliver_at_ms` on an item | deliver later (at most 30 days); the returned id is the schedule id | O |
| `GET /api/scheduled?channel=&limit=` | the soonest delayed messages (all nodes) and counts per channel | R |
| `DELETE /api/scheduled/{partition}/{offset}` | cancel (404 when already sent or cancelled) | O |
| `POST /api/scheduled/{partition}/{offset}/release` | send now | O |

Details: [Delayed messages](/docs/delayed).

## Live subscriptions {#live}

| Method and path | What it does | Role |
|---|---|---|
| `GET /api/channels/{name}/live` | WebSocket live subscription (token: subprotocol `fluxmq.token.<token>` or `Authorization`) | subscribe¹ |
| `GET /api/channels/{name}/events` | SSE live subscription (token: `Authorization` or `?access_token=`) | subscribe¹ |
| `GET /api/live/status` | gateway status per node: limits, connections, subscriptions per channel, counters | R |

¹ `viewer` and higher, or a `subscriber` token whose `scopes.subscribe` matches the channel. Details: [Live subscriptions](/docs/live).

## Notifications {#notify}

| Method and path | What it does | Role |
|---|---|---|
| `GET /api/notify/notifiers`, `GET …/notifiers/{id}` | notifiers | R |
| `POST /api/notify/notifiers`, `PATCH`/`DELETE …/notifiers/{id}` | create, change, delete (409 when a rule uses it) | A |
| `POST /api/notify/notifiers/test`, `POST …/notifiers/{id}/test` | test send of a draft / a saved notifier | A |
| `GET /api/notify/rules`, `GET …/rules/{id}` | rules | R |
| `POST /api/notify/rules`, `PATCH`/`DELETE …/rules/{id}` | create, change, delete | A |
| `GET /api/notify/history?limit=N` | send history (default 1000, up to 10,000) | R |

## Backups {#backups}

| Method and path | What it does | Role |
|---|---|---|
| `GET /api/backups/config`, `PUT /api/backups/config` | settings (secrets as `********`) | R / A |
| `GET /api/backups/status` | running backup, last success and failure, next run | R |
| `POST /api/backups/run` | run now (202; 409 — one is running) | A |
| `POST /api/backups/test-target` | test a target | A |
| `GET /api/backups`, `GET /api/backups/{id}` | history, a manifest (`latest` = newest) | R |
| `DELETE /api/backups/{id}` | delete a backup | A |

## Tokens and bindings {#auth}

| Method and path | What it does | Role |
|---|---|---|
| `GET /api/auth/me` | who am I: role, open/enforced mode | any |
| `GET /api/auth/tokens`, `POST /api/auth/tokens` | list, issue (`name`, `role`, `scopes`, `ttl_secs`) | A |
| `DELETE /api/auth/tokens/{id}` | revoke (409 `last_admin`) | A |
| `GET`/`POST /api/auth/cert-bindings`, `DELETE /api/auth/cert-bindings/{cn}` | certificate bindings | A |

## Cluster {#cluster}

| Method and path | What it does | Role |
|---|---|---|
| `GET /api/cluster/metadata` | topology and partition assignments | R |
| `GET /api/cluster/status` | Raft, under-replicated and offline partitions, HW/ISR | R |
| `POST /api/cluster/partitions/reassign` | reassign a partition's replicas | A |
| `POST /api/cluster/nodes/{id}/drain` | drain leadership | A |
| `POST /api/cluster/nodes/{id}/decommission[?dry_run=true]` | decommission a node (or plan it) | A |
| `GET /api/cluster/nodes/{id}/decommission`, `GET /api/cluster/decommissions` | progress, current and recent decommissions | R |
| `DELETE /api/cluster/nodes/{id}` | forget a dead node | A |
| `GET /api/cluster/rebalance` | rebalancing state | R |
| `PUT /api/cluster/rebalance/config` | rebalancing settings | A |
| `POST /api/cluster/rebalance` | start (`{"dry_run":true}` — preview) | A |
| `POST /api/cluster/rebalance/{pause,resume,cancel}` | pause, resume, cancel | A |

Any other route: `GET`/`HEAD` need viewer, everything else admin (deny by default). On a single node, cluster changes answer `400 unsupported_single_node`.
