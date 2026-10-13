# FluxMQ Architecture (v0.1 single node; v0.2 cluster; v0.3 notifications, webhooks, backups; v0.4 TLS, rebalancing; v0.5 batched delivery)

Source of truth for types: `crates/fluxmq-proto` (model, wire, telemetry, api). This file
explains how the pieces fit and lists the HTTP routes. See `docs/ROADMAP.md` for the why.

## Components

```text
 producers/consumers ──TCP 7420 (postcard frames)──►┐
                                                    │   fluxmq-server (bin "fluxmq")
                         ┌──────────────────────────┴───────────────┐
                         │ net: connection tasks, wire codec         │
                         │        │                                  │
   DATA PLANE            │   fluxmq-core::Broker                     │
                         │   storage (segmented log, group commit)   │
                         │   channels / groups / retry / DLQ         │
                         │   hot-path metrics (atomics + HDR)        │
                         └────────┬─────────────────────────────────┘
                     stats() pull │  events / tail: bounded, try_send
   OBSERVABILITY PLANE   ┌────────▼─────────────────┐
                         │ fluxmq-telemetry          │ collector tick 250ms, diff → rates,
                         │ TelemetryHub / Session    │ percentiles, alerts, history rollups
                         └────────┬─────────────────┘
                         ┌────────▼─────────────────┐
                         │ fluxmq-api (axum)         │ HTTP 7421: /api/*, /ws, static UI
                         └────────┬─────────────────┘
                                  ▼
                              ui/ (React)
```

Crate dependency direction (no cycles): `proto ← core ← telemetry ← api ← server`;
`proto ← client`. `proto ← tls` (v0.4) is used by `replication`, `cluster`, `api`, `server`
(feature `server`) and `client` (client-side builder only). The server's integration tests use `client`.

**Rule:** nothing in the observability plane may block the data plane. Core exposes pull-based
cumulative stats; push paths (events, tail) use bounded channels with `try_send`.

## Channel semantics

| Mode | Partition choice | Within a group | Ack | Storage |
|---|---|---|---|---|
| `stream` | crc32(key) % P, else round-robin | partitions assigned to instances (rebalanced on join/leave); no head-of-line blocking — a failed message goes to retry and the partition continues | per message; group offset per partition = lowest unacked | retention by age/size |
| `queue` | as stream (default P=1) | competing consumers: any instance gets the next available message | per message | retention, and a message is logically consumed once every group acked/dead-lettered it |
| `keyed` | crc32(key) % P | per-key strict order: the next message for key K is not delivered until the previous one for K is acked or dead-lettered (other keys proceed) | per message | retention |

All modes: multiple groups each receive every message (fan-out). Delivery is at-least-once.
Retry: nack(retry=true) or ack timeout → redelivered after `backoff_ms[attempt-1]` until
`max_attempts`, then dead-lettered into the channel DLQ (`reason` = last nack reason or
`"ack timeout"`). DLQ retry gives a fresh attempt budget to the original group.

Durability: messages and channel configs are durable per `FsyncPolicy` (default group commit,
2 ms). Group offsets checkpoint every 1 s (on restart, up to 1 s of acked messages may be
redelivered — at-least-once). DLQ is persisted. Delivery-attempt history (retry timeline) is
in-memory and bounded (`attempt_history_capacity` messages per channel, default 100k; roughly
a few hundred bytes per message).

## Client protocol and batched delivery (v0.5, wire v3)

Frames: `u32` big-endian length + postcard body (`fluxmq_proto::wire`). The client opens with
`Hello { protocol_version }`; the broker accepts `MIN_PROTOCOL_VERSION ..= PROTOCOL_VERSION`
(2..=3) and remembers the negotiated version per connection. v3 adds one server frame:
`DeliverBatch { subscription_id, deliveries }` — semantically the same `Deliver` frames back
to back (each delivery counts against prefetch and is acked individually or in one
`Ack { delivery_ids }`); never empty. v2 connections only ever get `Deliver`.

| Piece | How |
|---|---|
| Core | `Subscription::next_batch(out, max, max_bytes)` waits for ≥ 1 delivery, then takes everything ready **now** in one group-lock critical section (`Group::poll_batch`): bounded by the remaining prefetch credit, `max` and `max_bytes` of estimated wire size (`delivery_wire_size`; the first delivery of a batch is always taken). Dispatch order and partition round-robin are exactly those of `max` consecutive `next()` calls; a message that does not fit the byte bound is put back at the head of its partition's ready queue (or the out-of-band queue). `try_next_batch` is the non-blocking form. Lock order unchanged (channel config read, then group state, leaf locks inside). |
| Server | The per-subscription pump (`net/connection.rs`) calls `next_batch(MAX_DELIVER_BATCH = 256, MAX_DELIVER_BATCH_BYTES = 1 MiB)` for v3 connections (`max = 1` for v2). No linger: a lone ready delivery is queued at once (as a plain `Deliver`); batches grow only when deliveries pile up (backlog, burst, freed credit). If the connection writer is busy, the parked batch is topped up (`try_next_batch`, strict byte bound) right before it is queued. The codec serializes frames straight into the socket write buffer (no intermediate `Vec`, no second copy of the body). |
| SDK | Speaks v3 and fans a `DeliverBatch` out into the subscription's queue (non-blocking `try_send` on the normal path). Acks are coalesced per subscription (`acks.rs`): a buffer flushed when `SubscribeOptions::ack_batch` acks are pending (default 64, **capped at prefetch/2** so the broker always has credit to keep delivering), after `ack_linger` (default `ClientOptions::ack_linger` = 1 ms, measured from the oldest buffered ack), and on unsubscribe, consumer drop (before the `Unsubscribe`, which would otherwise return the acked deliveries to the group), subscription replacement and client close (`Conn::fail` queues every buffer before the writer's final flush). Nacks are sent immediately. |

Semantics are unchanged: at-least-once, prefetch, retry/backoff, ack timeout, DLQ, keyed
per-key order (a batch never holds two unfinished messages of one key), order within a
partition. Measurements: `docs/BENCHMARKS.md` ("Batched delivery (v0.5)").

## Ports, config, auth

* `7420` client TCP, `7421` HTTP. Flags/env: `--data-dir` (`FLUXMQ_DATA_DIR`, default `./data`),
  `--client-addr`, `--http-addr`, `--cluster-name`, `--config` (TOML keyed by flag name), `--ui on|off` (console embedded from `ui/dist` at
  build time), `--ui-dir` (serve it from a directory instead),
  `--admin-token` (`FLUXMQ_ADMIN_TOKEN`), `--fsync` (`group-commit`|`os`), `--public-url`
  (`FLUXMQ_PUBLIC_URL`, absolute console URL used for links in notifications).
* Synthetic traffic for demos: `cargo run -p fluxmq-client --bin fluxmq-demo` (goes through the
  real client protocol). Benchmarks: `fluxmq-bench` in the same crate. `scripts/dev.sh` runs
  server + demo + UI dev server together.
* Auth is role-based (RBAC, v0.2 stage 7) — see [RBAC](#rbac-v02-stage-7) below. Bearer
  tokens: `Authorization: Bearer <token>` (WS: `?token=` also accepted, `/ws` only); the client
  protocol takes the token in `Hello.auth_token`. With neither `--admin-token` nor any access
  token everything is open (dev mode). `GET /api/health` is always open (liveness probes).
* TLS (v0.4, contract in `fluxmq_proto::tls`): `--tls-cert/--tls-key` (client + HTTP port),
  `--tls-client-ca` (mTLS on the client port), `--http-tls-cert/--http-tls-key`,
  `--http-tls on|off`, `--cluster-tls-cert/--cluster-tls-key/--cluster-tls-ca`; env
  `FLUXMQ_TLS_*`, `FLUXMQ_HTTP_TLS*`, `FLUXMQ_CLUSTER_TLS_*`. Without them every port is
  plaintext exactly as before. See [TLS](#tls-v04--fluxmq-tls).
* Every mutating REST call is written to the audit log (`GET /api/audit`), actor = the
  principal (`name#token_id` for access tokens, `admin` for the bootstrap token, `anonymous`
  in open mode).

## RBAC (v0.2, stage 7)

Crate `fluxmq-auth` (contract: `fluxmq_proto::auth`), HTTP enforcement in
`fluxmq-api/src/auth.rs`, client-protocol enforcement in `fluxmq-server/src/net/connection.rs`.

**Principals.** The bootstrap `--admin-token` (never stored) → principal `admin`, role admin.
Access tokens `fmq_<id>_<secret>` (`id` = 12 chars `[a-z2-7]`, public; `secret` = 32 bytes from
the OS CSPRNG, hex) are created by admins and stored in the meta store under `auth/tokens/<id>`
as `TokenInfo` + `sha256(secret)` — the secret is shown once and never stored, logged or
audited. Comparisons are constant-time (`subtle`; the bootstrap token is compared via its
SHA-256). Tokens may expire (`ttl_secs`); `last_used_ms` is persisted lazily (≤ 1 write/min per
token, off the request path, CAS so it can never resurrect a revoked token).

**Mode.** Open iff no bootstrap token and no stored token (everyone is an anonymous admin,
permissive CORS). The first token created in open mode must have role `admin` (anything else →
400, so nobody locks the admins out); it switches to enforced mode at once (CORS is evaluated
per request) and its `CreateTokenResponse.adopt` is `true` — the console stores that token as
its own. Revoking the last token without a bootstrap token is refused (400) — it would
silently reopen the broker. Token names `admin` and `anonymous` (any case) are reserved.

**Sync.** Every node keeps an in-memory index (hot path: no I/O per request), loaded at start
and kept current by `MetaStore::watch` (full reload on `Lagged`); a node's own writes update it
directly (read-your-writes). Per-key revisions drop stale events.

**Roles** (cumulative) and **scopes** (channel globs, `*` = any run incl. `.`, `?` = one char):

| Action | viewer | operator | admin | scopes |
|---|---|---|---|---|
| `Read` — REST GETs, `/ws`, `/metrics`, message browsing | ✓ | ✓ | ✓ | |
| `Subscribe(channel)` — live WS/SSE (v0.10; role `subscriber` below viewer may do only this) | ✓ | ✓ | ✓ | `subscribe` globs |
| `Operate` — DLQ retry/discard/export, replay, reset offsets, REST publish | | ✓ | ✓ | |
| `Admin` — channel create/update/delete, consumer-group delete, routes, tokens, cluster ops | | | ✓ | |
| wire `Publish(channel)` | scope | ✓ | ✓ | `publish` globs |
| wire `Subscribe(channel)` (`Consume`; Ack/Nack ride on the subscription) | scope | ✓ | ✓ | `consume` globs |
| wire `CreateChannel` | | | ✓ | |
| wire `Metadata` | ✓ | ✓ | ✓ | |

**HTTP permission table** (`fluxmq_api::auth::PERMISSIONS`, keyed by axum's matched route):

| Route | Requirement |
|---|---|
| `GET /api/health` | open |
| `GET /api/channels`, `GET /api/channels/{name}`, `GET …/messages`, `GET …/messages/{p}/{o}` | Read |
| `POST /api/channels`, `PATCH`/`DELETE /api/channels/{name}` | Admin |
| `POST /api/channels/{name}/publish`, `POST …/replay`, `POST …/groups/{group}/reset` | Operate |
| `DELETE /api/channels/{name}/groups/{group}` | Admin |
| `GET /api/dlq/summary`, `GET /api/dlq` | Read |
| `POST /api/dlq/retry`, `POST /api/dlq/discard`, `GET /api/dlq/export` | Operate |
| `GET /api/history`, `/api/alerts`, `/api/search`, `/api/audit`, `/ws`, `/metrics` | Read |
| `GET /api/tls` (v0.4) | Read |
| `GET /api/auth/me` | Read (any authenticated caller) |
| `GET`/`POST /api/auth/tokens`, `DELETE /api/auth/tokens/{id}` | Admin |
| `GET`/`POST /api/auth/cert-bindings`, `DELETE /api/auth/cert-bindings/{cn}` (v0.5) | Admin |
| `GET /api/routes`, `GET /api/routes/{id}`, `GET /api/routes/stats`, `POST /api/routes/test`, `POST /api/filter/parse`, `POST /api/filter/render` | Read |
| `POST /api/routes`, `PATCH`/`DELETE /api/routes/{id}` | Admin |
| `GET /api/webhooks`, `GET /api/webhooks/{id}`, `GET /api/webhooks/stats` | Read (secrets redacted) |
| `POST /api/webhooks`, `PATCH`/`DELETE /api/webhooks/{id}`, `POST /api/webhooks/test`, `POST /api/webhooks/{id}/test` | Admin (tests make outbound requests) |
| `GET /api/backups/config`, `GET /api/backups/status`, `GET /api/backups`, `GET /api/backups/{id}` | Read (secrets redacted) |
| `PUT /api/backups/config`, `POST /api/backups/run`, `POST /api/backups/test-target`, `DELETE /api/backups/{id}` | Admin |
| `GET /api/notify/notifiers`, `GET /api/notify/notifiers/{id}`, `GET /api/notify/rules`, `GET /api/notify/rules/{id}`, `GET /api/notify/history` | Read (secrets redacted) |
| `POST /api/notify/notifiers`, `PATCH`/`DELETE /api/notify/notifiers/{id}`, `POST /api/notify/notifiers/test`, `POST /api/notify/notifiers/{id}/test`, `POST /api/notify/rules`, `PATCH`/`DELETE /api/notify/rules/{id}` | Admin (tests make outbound requests) |
| `GET /api/events/feed` | Read (any token, incl. consume-scoped client tokens) |
| `GET /api/cluster/metadata`, `GET /api/cluster/status` | Read |
| `POST /api/cluster/partitions/reassign`, `POST /api/cluster/nodes/{id}/drain` | Admin |
| `GET /api/cluster/rebalance` | Read |
| `GET /api/cluster/nodes/{id}/decommission`, `GET /api/cluster/decommissions` | Read |
| `POST /api/cluster/nodes/{id}/decommission`, `DELETE /api/cluster/nodes/{id}` | Admin |
| `PUT /api/cluster/rebalance/config`, `POST /api/cluster/rebalance`, `POST /api/cluster/rebalance/{pause,resume,cancel}` | Admin |
| any other route | `GET`/`HEAD` → Read, anything else → Admin (deny by default) |

In short: every `GET` is viewer, except `GET /api/dlq/export` (operator — bulk payload dump)
and `GET /api/auth/tokens` / `GET /api/auth/cert-bindings` (admin).
| `/internal/*` | not RBAC: mounted outside the layer, cluster-secret auth |

Errors: 401 `unauthorized` (missing/invalid/expired token), 403 `forbidden` (authenticated,
insufficient role/scope); wire: `Unauthorized` + `Goodbye` at `Hello`, `Forbidden` per request
(the connection stays open). Credentials are authenticated once per connection/WS session;
since v0.5 the session is then *watched* (below), so revocation cuts it.

### v0.5: live-session revocation, last-admin guard, certificate bindings

Contract: the v0.5 block of `fluxmq_proto::auth`.

**Session watch.** Every change to the auth index — a local write, a replicated meta event
(`auth/tokens/*`, `auth/cert-bindings/*`) applied by the node's watch task, or a full reload —
bumps a `tokio::sync::watch` generation inside `AuthService`.
`AuthService::session_watch(&principal)` returns a `'static` future that re-checks the
principal on every bump and otherwise sleeps until the token's `expires_at_ms` (capped at
30 s as a guard against wall-clock jumps). It resolves with `SessionEnd::{TokenRevoked,
TokenExpired, BindingRevoked}`; it never resolves for the bootstrap admin or the open-mode
anonymous principal (no `token_id`, no `cert_cn`). Long-lived sessions race it against their
work:

- client protocol (`net/connection.rs`): `select!` between `Connection::serve` and the watch →
  `Exit::Kick(reason)` → `Goodbye { reason: "token revoked" | "token expired" |
  "certificate binding revoked" }`; dropping the connection ends its pumps as usual;
- WS (`fluxmq-api/src/ws.rs`): the upgrade handler takes the request's `Principal` extension;
  the loop closes with code `4401` and the same reason.

Because each node applies replicated meta events to its own index, a revoke on any node
closes sessions on every node; latency is the meta replication delay plus a wake-up (well
under 1 s). Each wake-up costs one read lock and a hash lookup per session.

**Last-admin guard.** Without a bootstrap token, revoking the last active (unexpired) admin
token while other tokens exist → 409 `last_admin` (`AuthError::LastAdmin`). Revoking the very
last token stays 400. When exactly one *other* active admin remains, the delete carries a
meta precondition on that admin's key (`Precondition.key` may name any key), so two nodes
revoking the last two admins concurrently cannot both win; a spurious conflict (that admin's
`last_used_ms` write) is retried after reloading the index.

**Certificate bindings.** `auth/cert-bindings/<cn>` holds a `CertBinding` (exact CN, role,
scopes, `created_by`, lazily persisted `last_used_ms` — same ≤ 1/min CAS scheme as tokens).
The index keeps bindings next to tokens, with their own per-key revisions. The client-port
handshake calls `AuthService::authenticate_client(token, tls_cn)`: open mode → anonymous
admin; a non-empty `auth_token` → token auth (a token always wins, an invalid one is not
rescued); otherwise the verified mTLS CN (the `tls:<cn>` hint from v0.4, only present with
`--tls-client-ca`) → `Principal { name: "cert:<cn>", cert_cn: Some(cn), .. }`, actor
`cert:<cn>`. HTTP never uses bindings. `Server::bind` tells the service whether the client
port verifies certificates (`set_client_mtls`), surfaced as `client_mtls_enabled` in
`GET /api/auth/cert-bindings`. Validation: CN non-empty, ≤ 256 bytes, no `/` or control
characters (it is part of the meta key and the REST path); duplicate CN → 409 `conflict`
(create-only precondition). Bindings don't affect open/enforced mode.

## Routing (v0.2, stage 8) — `fluxmq-router`

Content-based routes: "messages of channel S matching conditions C are copied to channels T…".
Contract: `fluxmq_proto::routing`; storage: meta store `routes/<id>` (10-char random id),
hot-reloaded on every node via `meta.watch()` (a lagged watcher reloads the whole prefix).

| Aspect | Behaviour |
|---|---|
| Execution | One worker per source channel with ≥ 1 enabled route = consumer group `$router` (prefetch 256, `StartFrom::Latest` when the group is new). Never on the publish path; its lag/health show up like any group. |
| Evaluation | Every enabled route of the source, in creation order; conditions are ANDed; the JSON payload is parsed at most once per message and only when a payload field is referenced (key/header/partition conditions first). |
| Forwarding | Matches are grouped per target and published with one `Broker::publish` per target (targets in parallel), then the batch is acked. One copy per (message, target) even if several routes match. Key, headers and payload are preserved. |
| Headers | `flux-routed-from: {channel}/{partition}/{offset}`, `flux-route-id`, `flux-route-path` (comma list of visited channels, the copy's own channel last). |
| Loops | A target already on `flux-route-path` is skipped; at most 8 hops per original message. |
| Failures | Target publish fails (deleted channel, shutdown, …) → `nack(retry)` with reason `route target unavailable: <channel>` → the source channel's retry policy → DLQ entry with group `$router`. Partial failures retry all targets (at-least-once). |
| Lifecycle | A route applies only to messages published **after** it exists (a new `$router` group starts at the log end; history is never re-routed — the backtest only previews it, replay re-processes it). Workers stop when no enabled route remains for the source (a disabled route pauses: the group keeps its committed offset), end when the source channel is deleted (routes are kept and resume if the channel is re-created) and are reconciled every 5 s and on channel create/delete events. When the **last** route of a source is deleted, the API handler that deleted it releases the source's `$router` group: it retries (≤ 10 s) until the workers on every node let go, then deletes the group — `Broker::delete_group` on a single node, the two-phase leader fan-out in a cluster. Graceful shutdown finishes the batch in progress. |
| Validation | name 1–128; source and targets exist; target ≠ source, no duplicates; 1–16 targets; ≤ 32 conditions; `in`/`not in` need a list of 1–1000 scalars; `>`/`>=`/`<`/`<=` need a number or a string (lexical); other ops a scalar; `exists` values are dropped. |
| Backtest | `POST /api/routes/test` scans the source newest-first over the window (default 24 h), at most 200 000 records (`truncated`), returns `matched` and up to 100 newest samples. |
| Stats | Per route, since process start, per node: `evaluated`, `matched` (messages), `forwarded`, `failed` (**per target**: a match of a route with 3 targets adds 3 to `forwarded`), `last_matched_ms`. |

Filter DSL (routes' text mode, message search):

```text
expr  := cond ( AND cond )*                      -- OR is rejected
cond  := field op value | field exists | field not exists
field := key | partition | header.<name> | payload.<path> | <path>   -- path: a.b.0.c
op    := = (==) | != (<>) | > | >= | < | <= | in | not in | prefix | contains
value := number | "string" | 'string' | true | false | null | bare-word | [v, …] / (v, …)
```

Keywords are case-insensitive; strings support JSON escapes; numbers are JSON numbers
(integers keep i64/u64 precision). Comparison: numbers numerically, strings lexically, and a number
equals its numeric string (`order_id = 8842` matches `"8842"`, also for `in` and ordering).
An absent field fails every op except `!=`, `not in`, `not exists`; payload conditions never match
a non-JSON payload; `exists` is true for JSON `null`; `contains` = substring, or element equality
for arrays; header names match exactly, then case-insensitively. `key` and `partition` always
mean the message key/partition: a payload field literally named `key` or `partition` must be
written `payload.key` / `payload.partition`. `render` produces the canonical
text (`country = "MD" AND amount >= 100`) and `parse(render(c)) == c` for validated conditions.

## HTTP push webhooks (v0.3) — `fluxmq-webhooks`

The broker POSTs a channel's messages to an HTTP endpoint (consumers need no SDK). Contract:
`fluxmq_proto::webhooks`; storage: meta store `webhooks/<id>`, hot-reloaded on every node like
routes. Architecture mirrors the router.

| Aspect | Behaviour |
|---|---|
| Execution | One worker per enabled subscription = consumer group `$webhook:<name>` on its channel (`start` applies when the group is new; prefetch = `max_in_flight × batch_max`). In a cluster each node serves the partitions it leads. Lag, retries, the retry timeline and DLQ entries show up like any group's. `$`-prefixed (system) channels may be subscribed. |
| Request | `POST url`, `Content-Type: application/json`, configured headers, `User-Agent: FluxMQ-Webhooks/<v>`, `X-FluxMQ-Delivery-Id` (`{channel}/{partition}/{offset}`, stable across retries; first message of a batch), `X-FluxMQ-Attempt` (max of the batch), `X-FluxMQ-Batch-Size` (batches only), `X-FluxMQ-Signature: t=<unix>,v1=<hex hmac-sha256(secret, "<t>.<body>")>` when a secret is set. Body: one `WebhookEnvelope`; a JSON array of them when `batch_max > 1`; the raw payload when `format = raw` (metadata in `X-FluxMQ-Channel/-Partition/-Offset/-Key/-Timestamp/-Header-<name>`; non-visible-ASCII values percent-encoded). |
| Batching | Up to `batch_max` deliveries ready at once, or collected for `batch_linger_ms` after the first; a full batch is sent at once. |
| Concurrency | At most `max_in_flight` requests per worker (node). Filter (`fluxmq_router::filter::matches_in`): non-matching deliveries are acked without a request and counted as `skipped_by_filter`. |
| Outcome | 2xx → ack. 408/425/429/5xx, timeout, connection error → `nack(retry)` (channel retry policy). Other 4xx and 3xx (redirects are never followed) → `nack(dead-letter)` unless `retry_on_4xx` (4xx only). A target **blocked by the SSRF policy at request time** (DNS now resolves to a private address, or auth became enforced) → `nack(dead-letter)` at once with reason `blocked by policy` (no retries — the policy decides, not the network). Reasons are short classes (`fluxmq_proto::api::short_error::classify`): `HTTP <status>`, `timeout`, `connection refused`, `connection failed`, `TLS error`, `DNS error`, `blocked by policy`, `request failed`; the full cause chain (URL stripped) goes to `last_error_detail` / `error_detail`. |
| Retry-After | The core has no per-nack delay, so the worker **holds** the delivery for `min(Retry-After, max backoff) − next backoff` before the nack (capped 500 ms below the ack timeout, skipped on the last attempt). Seconds and HTTP-date forms. |
| Ordering | `max_in_flight = 1`: after a retryable failure the worker pulls nothing until that retry is due; a due retry is served before newer messages of its partition, so per-partition order holds (a failing message pauses the whole subscription for its backoff). Exact with `batch_max = 1`; with batching it is best-effort (a batch's messages can come back with different attempt counts). `> 1`: no order. |
| Limits | name `[a-z0-9._-]{1,64}`; `timeout_ms` 100–60000 and `timeout_ms + batch_linger_ms` < the channel's `ack_timeout_ms` (else the broker would expire in-flight requests; a later ack of an expired delivery is ignored quietly); `max_in_flight` 1–64; `batch_max` 1–500; `batch_linger_ms` 0–10000; raw ⇒ `batch_max = 1`; ≤ 32 headers, reserved names (`Content-Type`, `Host`, `X-FluxMQ-*`, …) rejected. `name`, `channel`, `start` are immutable. Name uniqueness is checked against the local cache (two nodes creating the same name concurrently could both succeed). |
| SSRF guard | Loopback, private, link-local, CGNAT, unspecified, broadcast, multicast and reserved addresses (incl. IPv4-mapped/NAT64 forms) are rejected when a subscription is saved/tested (the host is resolved) **and** at every request (the client resolves through a filtering resolver, IP literals are checked; no redirects, no proxy). `--webhooks-allow-private[=true|false]` (`FLUXMQ_WEBHOOKS_ALLOW_PRIVATE`); unset = allowed while auth is open (dev), denied once auth is enforced — re-evaluated per request. |
| HTTP client | One `reqwest` client (rustls, connection pool: 64 idle/host, 60 s idle timeout, 10 s connect timeout), per-request timeout = `timeout_ms`; response bodies are read up to 16 KiB (connection reuse) and dropped. |
| Lifecycle | Any definition change restarts the worker (the old one finishes first). Disable stops the worker and keeps the group (paused, committed offsets kept). Delete stops it; the API handler then deletes `$webhook:<name>` once every node let go (retries ≤ `timeout_ms` + 10 s; `Broker::delete_group` or the cluster fan-out). The groups API refuses to delete a group of an existing subscription (409). Workers end when the channel is deleted and are reconciled every 5 s. Graceful stop/shutdown: no new pulls, the pending batch is sent, in-flight requests finish (≤ `timeout_ms` + 2 s; a `Retry-After` hold is cut short and nacked); unresolved deliveries return to the group. Restart resumes from committed offsets. |
| Stats | Per subscription since process start: `delivered`/`failed` (messages; a failed attempt counts each message once), `skipped_by_filter`, `in_flight`, `last_status` (HTTP status of the last attempt, `null` when it got no response), `last_error` + `last_error_detail` (last failed attempt), `last_delivery_ms`, latency p50/p99 (request → response headers) from a mergeable log-linear histogram (≤ 12.5 % error). `GET /api/webhooks/stats` sums `GET /internal/v1/webhooks/stats` of every node when clustered (unreachable nodes are skipped). |
| Tests | `POST /api/webhooks/test` (create body, nothing stored) and `POST /api/webhooks/{id}/test` with an optional `UpdateWebhookRequest` draft merged over the stored subscription (`********` = stored secret / header value; validated like an update incl. the SSRF check; nothing persisted). |
| Throughput | Local receiver, release build, 8 partitions, 200-byte payloads: ~26 k msg/s with `max_in_flight = 1`, ~65–68 k msg/s with 16–64, ~500 k msg/s with `max_in_flight = 4, batch_max = 100` (v0.3.0 run; `cargo test -p fluxmq-webhooks --release --test webhooks bench -- --ignored --nocapture`; see BENCHMARKS.md). |

Receiver-side verification: `fluxmq_webhooks::signature::verify` (Rust); Node.js, Python and Go
snippets in `docs/GUIDE.ru.md` ("HTTP-вебхуки (push)").

## Backups (v0.3) — `fluxmq-backup`

Consistent, incremental backups to S3-compatible storage (AWS S3, MinIO, Cloudflare R2,
Backblaze B2 via the `object_store` crate, rustls) or a filesystem path; offline restore into an
empty data directory. Contract: `fluxmq_proto::backup`; config in the meta store at
`backup/config`.

| Aspect | Behaviour |
|---|---|
| Cut (core) | `Broker::backup_cut(channel)` (blocking; `fluxmq_core::backup`). Per partition: end = committed end (`high`); replicated mode: only partitions led locally, end = high watermark. The segment-list read guard is held only to clone and **pin** the segments below the end (retention stops at a pinned segment until the cut is dropped). Each segment's byte length holding exactly the records below the end is found by a short index-guided scan of record headers after the guard is released; its index bytes come from the in-memory index. Bytes are read later with positional reads on the segment's own read-only handle, bounded by that length (works even after unlink). Then group committed offsets and the DLQ export are taken (after the cut ⇒ restore may redeliver, never lose). The writer is never blocked. Lock order: see `fluxmq-core/src/lib.rs`. |
| Objects | `manifests/<id>.json` (plain JSON, written last — a backup exists once its manifest does), `segments/<channel>/<p>/<base:020>-<crc32:08x>.log` + `.idx` (content-addressed: identical bytes ⇒ same name ⇒ uploaded once; encrypted objects add the key fingerprint: `….k<fp>.log`), `state/<id>/meta.json` (meta-store dump without `backup/*`), `state/<id>/channels/<name>.json` (group offsets + DLQ). Ids: `<yyyymmddThhmmssZ>-<ms><3 hex>` (sortable). |
| Incremental | Sealed segments keep their name, so later backups `HEAD` them (size check) and reuse (`segments_reused`). The active segment is uploaded up to the cut as a new object each time. Segment-prefix CRCs are cached per (channel incarnation, partition, base, length). |
| Streaming | 1 MiB reads, `PUT` up to 5 MiB, larger objects as multipart (5 MiB parts, ≤ 2 in flight), ≤ 3 segments in parallel: ≲ 50 MiB buffered per run. Downloads stream to disk with decryption, length and CRC checks; restored segments are re-verified record by record (CRC + contiguous offsets). |
| Encryption | Optional AES-256-GCM, base64 32-byte key. Chunked STREAM format (`fluxmq_backup::crypto` docs): 16-byte header (`FXBE`, version, chunk size, random 7-byte nonce prefix), 1 MiB chunks each with a 16-byte tag, nonce = prefix ‖ counter ‖ last-flag, AAD = object key. The manifest stays plain and records the key fingerprint; restore rejects a wrong/missing key up front. Lost key ⇒ backup unrecoverable. |
| Service | `BackupService`: config (validation; secrets redacted on read, `********` keeps / `""` removes), scheduler (every `interval_secs` after the previous start; the first run 60 s after start unless the target's newest backup is recent), manual run (one at a time, `409` otherwise), live progress in `BackupOverview`, history = manifests (+ running / last failure in memory), retention after each success (`keep_last`, `keep_days`; the newest backup is always kept) then GC of `segments/`/`state/` objects no manifest references. Events → audit log (`backup.succeeded`, `backup.failed`, `backup.expired`, scheduled `backup.run`; API actions are audited by their handlers). |
| Cluster | Coordinated by the **controller** (`fluxmq_backup::cluster`): only its scheduler runs; `POST /api/backups/run` and `GET /api/backups/status` on another node are proxied to it (`/internal/v1/backup/run`, `/internal/v1/backup/status`). The controller snapshots the layout (channel → partition count), cuts and uploads its own led partitions and asks every Up/Joining peer (`POST /internal/v1/backup/partial`, 6 h timeout) to do the same with the existing code; each node reads the config from the replicated meta store and refuses if its target/key fingerprint differs from the controller's. Partials return the uploaded `PartitionEntry`s plus group offsets/DLQ of their partitions; `cluster::merge` unions them (a partition reported twice — leadership moved — keeps the higher end; a partition reported by nobody fails the run; group offsets per partition from the node that cut it), then the controller writes state objects, the meta snapshot and the manifest (`node_id` = controller, `nodes` = uploaders). Cuts of different leaders are taken at slightly different moments (each partition is consistent on its own). A node older than 0.3.0 → `400 unsupported_in_cluster`. Restore always produces a single node (it drops `cluster/*` meta keys); seed a new cluster from it. |
| CLI | `fluxmq backup list`, `fluxmq backup run --data-dir <stopped broker>`, `fluxmq restore --backup <id\|latest> --data-dir <empty>` with `--fs-path` or `--s3-bucket/--s3-endpoint/--s3-region/--s3-prefix/--s3-path-style`, credentials from `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, key from `--encryption-key`/`FLUXMQ_BACKUP_ENCRYPTION_KEY`. Plain `fluxmq` (no subcommand) runs the broker as before. |
| Restore | Manifest → key check → `meta.json` (minus `cluster/*`) → per channel: `channel.json`, segments, `offsets.json`, `dlq.log` (`fluxmq_core::backup::RestoreWriter`) → fsync. Refuses a non-empty directory. A metadata-only backup restores empty partitions starting at the backed-up end offsets, so offsets continue. |
| API view | `GET /api/backups/{id}` returns `fluxmq_proto::backup::BackupManifest` — info, encryption fingerprint, `nodes`, and a per-channel summary (partitions, messages, bytes, segments, groups, DLQ entries); object keys and CRCs stay internal (`manifest::Manifest::summary`). |
| Errors | `TestResult.error` of the target probe is short (`bucket not found`, `access denied`, `invalid credentials`, `connection failed`, `timeout`, …) with the full chain in `error_detail`. |
| Speed | See BENCHMARKS.md ("Backup and restore"). |

## Notifications and the events feed (v0.3) — `fluxmq-notify`

Alerts and broker events → Telegram, Slack, Discord, email, PagerDuty and generic webhooks;
plus a pull feed for external systems. Contract: `fluxmq_proto::notify`; storage: meta store
`notify/notifiers/<id>` and `notify/rules/<id>` (10-char ids), hot-reloaded on every node.

| Aspect | Behaviour |
|---|---|
| Input | `TelemetryHub::notifications()` — a broadcast of `TelemetrySignal::{AlertOpened, AlertResolved, Event}` emitted by the collector from the same alert-engine transitions / events-feed entries that feed the `alerts`/`events` topics (computed once; capacity 1024 per listener). Coalesced dead-letter updates are not re-emitted. |
| Who dispatches | Only the controller (single node: always). `ClusterLeadership` checks `metadata().controller == node_id` on every signal, on `watch_epoch()` changes and every 5 s. Every node computes the same cluster-wide alerts, so a new controller (follower → controller while running) seeds the "already notified" set from the alerts open right now (they come back at the repeat interval, not as duplicates) and a node losing leadership drops its pending groups. |
| Matching | Rule matches an alert when `severity ≥ min_severity` (warning/critical), kind ∈ `alert_kinds` (empty = any) and its channel matches a `channels` glob (empty = any; alerts without a channel match only an empty list). Events only when their kind ∈ `event_kinds` (empty = none), channel derived from the subject for channel-scoped kinds. Globs: `*` any run (dots included), `?` one char. |
| Grouping | Per (rule, notifier): the first match starts a `group_wait_secs` window (default 30, 0–3600); everything matching inside it goes out as one message per status (`firing`, `resolved`, `event`). Opened and resolved within the window → nothing is sent. |
| Repeat / resolved | Notified alerts that stay open are re-sent every `repeat_interval_secs` (default 4 h, 0 = never, ≤ 7 d) with their latest text (`TelemetryHub::alerts()`); a resolve of a notified alert sends a recovery message when `send_resolved` (default true). A disabled notifier records `suppressed` in the history. |
| Delivery | Each send is its own task: up to 3 attempts, backoff 2 s / 10 s; `Retry-After` / Telegram `retry_after` honoured (≤ 60 s). 2xx = sent; 429/5xx/timeouts/connection errors retried; other 4xx and 3xx (redirects are never followed) permanent. HTTP via one `reqwest` client (rustls, 10 s timeout); email via `lettre` (rustls, 15 s timeout). |
| History | v0.5 (`history.rs`): every `NotificationRecord` is appended as JSON to the replicated system channel `$system.notifications` (1 partition, 7 days + 64 MiB backstop; reads expose the newest 10 000), created like `$system.events`. Only the controller appends (dispatch records via a batched background writer; test sends synchronously; a record produced on another node is forwarded to the controller's `POST /internal/v1/notify/history/append`). `GET /api/notify/history` (`limit` default 1000, ≤ 10 000) is proxied to the channel's partition leader (`GET /internal/v1/notify/history`, never re-proxied) and merged with that node's in-memory ring of the last 1000 records (dedup by id, newest first; tests have `rule_id = null`). Survives controller failover and full restart. |
| Errors | `error` is short (`connection refused`, `timeout`, `HTTP 503`, `TLS error`, `DNS error`, `SMTP 550`, …), `error_detail` holds the cause chain / response snippet (URLs stripped, no secrets). Same for `TestResult`. |
| Tests | `POST /api/notify/notifiers/test` (create body) and `POST /api/notify/notifiers/{id}/test` with an optional `UpdateNotifierRequest` draft merged over the stored notifier (`********` = stored secret), validated, not persisted. |
| Message copy | `fluxmq_notify::render`: one marker per item (🔴 critical, 🟠 warning, ✅ resolved, ℹ️ info), severity label, title, detail, "Possible reason", channel/group/"open for", item link `<public-url><alert.link>` and a console link when `--public-url` is set. Several alerts: "N issues need attention". |
| Telegram | `POST {api}/bot<token>/sendMessage`, `parse_mode=HTML` (`& < > "` escaped), `chat_id` numeric or `@channel`, `message_thread_id` for forum topics, link previews off. Text kept ≤ 4000 chars (items that don't fit → "… and N more"). |
| Slack | Incoming webhook, Block Kit: `header`, a `section` (mrkdwn, `& < >` escaped) + `context` per alert (≤ 20, "… and N more"), footer `context`; `text` fallback. |
| Discord | Webhook: `content` headline, one embed per item (≤ 10, ≤ 5500 chars total), colour by severity, `url` = item link, fields Channel/Group; `allowed_mentions` empty. |
| PagerDuty | Events API v2 `https://events.pagerduty.com/v2/enqueue`: `trigger` per firing alert (`dedup_key` = alert id, severity critical/warning, `component` = channel, `group`, `class` = kind, `custom_details`), `resolve` with the same `dedup_key`; events trigger `info` incidents keyed `fluxmq-event-<id>`; a test triggers and immediately resolves `fluxmq-test-<id>`. |
| Webhook | `POST url`, JSON `NotificationPayload`, custom headers, `X-FluxMQ-Signature: t=<unix>,v1=<hex hmac-sha256(secret, "<t>.<body>")>` when a secret is set (same scheme as HTTP push webhooks; reference: `fluxmq_notify::senders::verify_signature`). |
| Email | SMTP `tls`: `tls` (implicit, 465), `starttls` (required upgrade, 587), `none` (plaintext, local relays). Plain-text body, subject `[FluxMQ CRITICAL] <headline>`. |
| Secrets | `bot_token`, Slack/Discord `webhook_url`, SMTP `password`, `routing_key`, webhook `secret` and every webhook header value are returned as `********`; on update `********` keeps, `""` removes (optional fields / headers), other values replace. The generic webhook `url` is not a secret. Audit entries carry redacted copies; send errors are stripped of URLs (Telegram puts the token in the path); nothing secret is logged. |
| Validation | name 1–128; Telegram token `<digits>:<token>`, chat id numeric or `@name`; http(s) URLs; email addresses parsed, 1–20 recipients, password needs a username; PagerDuty key alphanumeric ≤ 64; ≤ 20 webhook headers, reserved names (`Content-Type`, `Host`, `User-Agent`, `X-FluxMQ-Signature`, …) rejected. Rules: 1–20 existing notifiers, `min_severity` ≠ healthy, ≤ 50 globs `[A-Za-z0-9._-$*?]`. A notifier used by a rule can't be deleted (409). Limits: 100 notifiers, 200 rules. |
| Feed | System channel `$system.events` (1 partition, retention 7 d, DLQ off) created on start (cluster: through the controller; checked every 250 ms until it exists, then
every 2 s). v0.5: the controller never writes an assignment without replicas — a channel
created before any node is alive (right after an election) stays unplaced until the controller
tick places it, and the tick re-places replica-less partitions left by older versions. For the
first 5 s as controller it also defers placing a new channel on fewer live nodes than the
cluster is known to have (registered Up/Joining nodes + initial peers not registered yet,
capped by the RF), so a channel created at bootstrap doesn't get a reduced RF. The controller appends one JSON `FeedItem` per alert transition and per broker event (`alert_opened`/`alert_resolved` event entries and events about `$` channels are skipped — the transition itself is in the feed); stored items carry `cursor: ""`, the reader fills in the offset. `GET /api/events/feed?after=<offset\|latest>|tail=<1..1000>&limit=1..1000&wait_ms=0..30000`: `tail=N` (exclusive with `after`) = the newest N items oldest-first, to seed a view; long-poll (wakes on a local append, re-reads every 250 ms for appends replicated from the leader); `next_cursor` = last item's offset (or the request's position when empty); `gap = true` when retention removed items after `after`. Omitting `after` starts at the oldest retained item. Cluster (v0.4): a node that isn't the leader of `$system.events`/0 proxies the validated query to the leader's `/internal/v1/events/feed` (timeout = `wait_ms` + 10 s, shutdown re-asks with `wait_ms=0`); with no leader it answers from its own copy. |
| System channels | `$`-prefixed names are valid for the broker (`fluxmq_proto::model::is_system_channel`) but reserved: REST/wire create → 400; REST update/delete/publish/replay-into and wire `Publish` → 403 "system channel is read-only"; a route may read from one but never target it. Consuming, browsing and HTTP push webhooks work as for any channel. |

## HTTP routes (`fluxmq-api`)

All JSON. Errors: non-2xx with `ApiError { error, message, position? }` — `position` (byte
offset) is present only for filter DSL syntax errors (`filter=` on message search,
`POST /api/filter/parse`). `error` is a machine-readable code clients branch on (never on `message`): `bad_request`,
`unauthorized`, `forbidden`, `not_found`, `conflict`, `unavailable` (503, transient — retry),
`internal`, and `unsupported_in_cluster` (400: the operation can't run in this cluster, e.g.
backups with a node older than 0.3.0).

| Method | Path | Body / query | Response |
|---|---|---|---|
| GET | `/api/health` | | `HealthResponse` |
| GET | `/api/channels` | | `ChannelConfig[]` |
| POST | `/api/channels` | `CreateChannelRequest` | `201 ChannelConfig` |
| GET | `/api/channels/{name}` | | `ChannelConfig` |
| PATCH | `/api/channels/{name}` | `UpdateChannelRequest` | `ChannelConfig` |
| DELETE | `/api/channels/{name}` | | `204` |
| POST | `/api/channels/{name}/publish` | `PublishRequest` | `PublishResponse` |
| GET | `/api/channels/{name}/messages` | `MessageQuery` | `MessagePage` |
| GET | `/api/channels/{name}/messages/{partition}/{offset}` | | `MessageDetail` |
| POST | `/api/channels/{name}/replay` | `ReplayRequest` | `PublishResponse` |
| POST | `/api/channels/{name}/groups/{group}/reset` | `ResetOffsetsRequest` (`{ "to": StartFrom }`) | `204`; `404` if the group doesn't exist |
| DELETE | `/api/channels/{name}/groups/{group}` | | `204`; `404` unknown group; `409` while it has active consumers, or for `$router` while a route uses the channel |
| GET | `/api/dlq/summary` | | `DlqSummary` |
| GET | `/api/dlq` | `DlqQuery` | `DlqPage` |
| POST | `/api/dlq/retry` | `DlqActionRequest` | `DlqActionResponse` |
| POST | `/api/dlq/discard` | `DlqActionRequest` | `DlqActionResponse` |
| GET | `/api/dlq/export` | `DlqQuery` | NDJSON of `DeadLetterView` |
| GET | `/api/history` | `metric` (see below), `range` (`5m`/`15m`/`1h`/`24h`/`7d`) | `HistorySeries` |
| GET | `/api/alerts` | | `AlertsState` |
| GET | `/api/tls` | | `TlsStatus` (v0.4): per surface `client` / `http` / `cluster` — enabled, mutual, served certificate (subject, issuer, SANs, validity, SHA-256), last (re)load, last reload error, failed handshakes. Node-local. |
| GET | `/api/search` | `q` | `SearchResult[]` (channels, groups; for the command palette) |
| GET | `/api/audit` | `limit` | `AuditEntry[]` newest first |
| GET | `/api/routes` | | `Route[]` (creation order) |
| POST | `/api/routes` | `CreateRouteRequest` | `201 Route`; 400 invalid, 404 unknown source/target |
| GET | `/api/routes/{id}` | | `Route` |
| PATCH | `/api/routes/{id}` | `UpdateRouteRequest` | `Route`; 409 on a concurrent modification |
| DELETE | `/api/routes/{id}` | | `204` |
| POST | `/api/routes/test` | `RouteTestRequest` | `RouteTestResponse` (backtest) |
| GET | `/api/routes/stats` | | `RouteStats[]` (since process start, this node) |
| GET | `/api/webhooks` | | `WebhookSubscription[]` (creation order; `secret` and header values = `********`) |
| POST | `/api/webhooks` | `CreateWebhookRequest` | `201 WebhookSubscription`; 400 invalid / SSRF-blocked, 404 unknown channel, 409 duplicate name |
| GET | `/api/webhooks/{id}` | | `WebhookSubscription` (redacted) |
| PATCH | `/api/webhooks/{id}` | `UpdateWebhookRequest` (`********` keeps a secret, `""` removes) | `WebhookSubscription`; 409 on a concurrent modification |
| DELETE | `/api/webhooks/{id}` | | `204`; the `$webhook:<name>` group is then released |
| POST | `/api/webhooks/test` | `CreateWebhookRequest` | `WebhookTestResult` (test before saving; nothing persisted) |
| POST | `/api/webhooks/{id}/test` | optional `UpdateWebhookRequest` draft | `WebhookTestResult` (one synthetic envelope, `delivery_id` = `test/<ms>`; short `error` + `error_detail`) |
| GET | `/api/webhooks/stats` | | `WebhookStats[]` (since process start; summed over nodes when clustered) |
| GET | `/api/backups/config` | | `BackupConfig` (`secret_access_key`, `encryption_key` = `********`) or `null` when unset |
| PUT | `/api/backups/config` | `BackupConfig` (`********` keeps a secret, `""` removes) | `BackupConfig` (redacted); 400 invalid |
| GET | `/api/backups/status` | | `BackupOverview` (running backup with `progress_bytes / total_bytes`, last success/failure, next run) |
| POST | `/api/backups/run` | | `202 BackupInfo` (status `running`; cluster: started on the controller); 400 not configured, 400 `unsupported_in_cluster`, 409 one is running |
| POST | `/api/backups/test-target` | `BackupTarget` | `TestResult` (write + read + delete of `probe/<random>.txt`; nothing persisted) |
| GET | `/api/backups` | | `BackupInfo[]` newest first (`[]` when not configured) |
| GET | `/api/backups/{id}` | | `BackupManifest` (info, encryption fingerprint, nodes, per-channel summary); `latest` = newest |
| DELETE | `/api/backups/{id}` | | `204` (manifest deleted, then unreferenced objects collected); 409 while a backup runs |
| GET | `/api/notify/notifiers` | | `Notifier[]` (creation order; secrets `********`) |
| POST | `/api/notify/notifiers` | `CreateNotifierRequest` | `201 Notifier`; 400 invalid |
| GET | `/api/notify/notifiers/{id}` | | `Notifier` (redacted) |
| PATCH | `/api/notify/notifiers/{id}` | `UpdateNotifierRequest` (`********` keeps a secret, `""` removes) | `Notifier`; 409 on a concurrent modification |
| DELETE | `/api/notify/notifiers/{id}` | | `204`; 409 while a rule uses it |
| POST | `/api/notify/notifiers/test` | `CreateNotifierRequest` | `TestResult` (test before saving; nothing persisted) |
| POST | `/api/notify/notifiers/{id}/test` | optional `UpdateNotifierRequest` draft | `TestResult` (synchronous, one attempt, also when disabled) |
| GET | `/api/notify/rules` | | `NotificationRule[]` |
| POST | `/api/notify/rules` | `CreateRuleRequest` | `201 NotificationRule` (defaults: 30 s / 4 h / resolved on) |
| GET/PATCH/DELETE | `/api/notify/rules/{id}` | `UpdateRuleRequest` | `NotificationRule` / `204` |
| GET | `/api/notify/history` | `limit` (default 1000, ≤ 10 000) | `NotificationRecord[]` newest first, from `$system.notifications` (any node) |
| GET | `/api/events/feed` | `after` \| `tail`, `limit`, `wait_ms` | `FeedPage` (long-poll; `cursor` always filled) |
| POST | `/api/filter/parse` | `ParseFilterRequest` | `ParseFilterResponse`; 400 `filter syntax error at <byte>: …` with `position` |
| POST | `/api/filter/render` | `RenderFilterRequest` (`{conditions}`) | `RenderFilterResponse` (`{rendered}`); 400 for invalid conditions (`condition N: …`) |
| GET | `/ws` | | WebSocket, telemetry contract v1 |
| GET | `/api/channels/{name}/live` | `from`, `cursor`, `partitions`, `key`, `header.<name>`, `filter`; token as WS subprotocol `fluxmq.token.<t>` | WebSocket, live messages (v0.10, `docs/design/v0.10-live-gateway.md`); `Subscribe(channel)` |
| GET | `/api/channels/{name}/events` | same, plus `access_token`; `Last-Event-ID` resumes | SSE, live messages (v0.10); `Subscribe(channel)` |
| GET | `/metrics` | | Prometheus text 0.0.4, node-local (RBAC `Read`; see Monitoring) |
| GET | `/api/cluster/metadata` | | `ClusterMetadata` (topology + assignments) |
| GET | `/api/cluster/status` | | `ClusterStatus` (Raft, under-replicated/offline counts, per-partition HW/ISR from every leader) |
| POST | `/api/cluster/partitions/reassign` | `ReassignRequest` | `ClusterMetadata` of the channel; 400 if not clustered / invalid |
| POST | `/api/cluster/nodes/{id}/drain` | | `DrainResponse` |
| POST | `/api/cluster/nodes/{id}/decommission` | | `DecommissionStatus` (v0.5; idempotent while running); 409 `conflict` (plan running, another decommission, RF can't be held, no live leader, unsafe quorum), 404 unknown, 400 `unsupported_single_node` |
| POST | `/api/cluster/nodes/{id}/decommission?dry_run=true` | | `RebalanceStatus` of the plan it would start (same checks), nothing written |
| GET | `/api/cluster/nodes/{id}/decommission` | | `DecommissionStatus` from the replicated record (live `replicas_remaining`, `initial_replicas`); 404 no record |
| GET | `/api/cluster/decommissions` | | `DecommissionStatus[]` current and recent (24 h), newest first |
| DELETE | `/api/cluster/nodes/{id}` | | `DecommissionStatus` (forget a `Down` node holding no replicas); 409 otherwise, 404 unknown |
| GET | `/api/cluster/rebalance` | | `RebalanceStatus` (single node: mode off, idle) |
| PUT | `/api/cluster/rebalance/config` | `RebalanceConfig` | `RebalanceStatus`; 400 invalid / `unsupported_single_node` |
| POST | `/api/cluster/rebalance` | `RebalanceRequest` (optional body) | `RebalanceStatus` (dry run: the plan, nothing written); 409 `conflict` while a plan is active, 404 unknown channel, 400 `unsupported_single_node` |
| POST | `/api/cluster/rebalance/{pause,resume,cancel}` | | `RebalanceStatus`; 400 when no plan is active / `unsupported_single_node` |
| GET | `/internal/v1/node-stats` | header `x-fluxmq-cluster-secret` | `application/x-fluxmq-node-stats` (postcard); 404 on a single node |
| * | `/internal/v1/*` | header `x-fluxmq-cluster-secret` | node-to-node fan-out (see `docs/CLUSTER.md`); 401 without the secret, 404 on a single node |
| GET | `/*` | | static UI from `--ui-dir`, SPA fallback to `index.html` |

Message query (`MessageQuery`) precedence: `offset` set → forward from (`partition`, `offset`);
else `newest_first=true` → the newest `limit` matches within [`since_ms`, `until_ms`] (both
optional, inclusive; `has_more` = older matches exist); else `since_ms` → forward from that time;
else the newest `limit` messages (before `until_ms` if given). With `key` and no `partition`,
only the key's partition (`crc32(key) % P`) is scanned. Results are always sorted by
timestamp ascending; the UI's "Latest / Last 15 min / 1 h / 24 h" presets send
`newest_first=true` and display newest first, paging backwards with `until_ms` = oldest shown.

`filter=<DSL>` on the messages query: the same filter as routes (see Routing). Parsed first
(400 with the byte position on a syntax error); then the query above is executed as a scan over
`read_messages` pages, keeping matches until `limit`, visiting at most 200 000 records
(`scanned` = records visited, `has_more` = the scan stopped before exhausting the range). Without
`filter` (or with a blank one) the plain read path is used unchanged.

History metric ids: `ingress_rate`, `egress_rate`, `ack_rate`, `publish_p50`, `publish_p95`,
`publish_p99`, `lag`, `retries_rate`, `dlq_rate`, `consumers`, `storage_bytes`; per channel
`channel.<name>.<m>` for `ingress_rate`, `egress_rate`, `ack_rate`, `publish_p99`, `lag`,
`dlq_rate`.

Rates: `ingress_rate` = messages published/s. `egress_rate` = deliveries/s summed over all
groups, including redeliveries (with two groups, egress ≈ 2× ingress). `ack_rate` = acks/s
summed over all groups — the "consumed" number the dashboard compares against published.

## Cluster (v0.2, stage 9) — `fluxmq-cluster`

Operations guide and flags: `docs/CLUSTER.md`. No `--peers` → `Cluster::single_node` (local
`meta.json`, this node leads everything, v0.1 behaviour).

* **Metadata**: openraft 0.9 on every node. State machine = the `MetaStore` KV
  (revision = +1 per applied write, preconditions evaluated at apply, watch events in log
  order). Persistent log/vote/snapshot under `data/raft/`. Transport: HTTP+JSON on
  `--raft-addr`. Writes on followers are forwarded to the leader and retried across elections
  (≤ 8 s, only when certainly not applied). `put`/`delete` wait until the local replica has
  applied them (read-your-writes on the calling node).
* **Controller** (Raft leader, `controller.rs`): it keeps the node registry `cluster/nodes/*`
  from heartbeats (Up / Down after `node_timeout` / Leaving / restart detection by
  incarnation). It adds learners and promotes them up to 5 voters. It writes assignments
  `cluster/assignments/*` for `cluster/channels/*`. It runs failover from the ISR (epoch + 1,
  no unclean election), drops dead nodes from ISRs, and does preferred-leader rebalancing,
  `reassign` and `drain`. Every assignment write is a CAS on the key's `mod_revision`. Ticks
  and operator commands are serialized.
* **Leases / fencing** (`runtime.rs`): an acked heartbeat (leadership confirmed by a quorum)
  grants a 4 s lease from its send time. Without a valid lease a node applies no leader roles
  and routes nothing locally. The controller declares `Down` only after the lease must have
  expired, so a deposed leader never accepts writes concurrently with its successor.
* **Reconciler** (every node, `reconcile.rs`): local channels follow `cluster/channels`.
  Replication endpoints follow `cluster/nodes`. Roles follow assignments: `set_role`, then
  `isr_committed` with the committed ISR. The metadata epoch (revision of the last
  `cluster/*` write) feeds `Cluster::watch_epoch`, and the server pushes
  `ServerFrame::MetadataChanged`. ISR changes reported by local leaders are persisted by CAS
  (only while leader at that epoch). The leader only uses a shrunk ISR for its HW after
  `isr_committed`.
* **Client protocol**: `Metadata` → JSON `ClusterMetadata`. `Publish{partition: Some(p)}` is
  served by `p`'s leader only (`NotLeader` + leader id otherwise, empty during elections).
  `Publish{partition: None}` picks the partition per message (`crc32(key) % P`, round-robin)
  and forwards non-local groups to their leaders via `POST /internal/v1/publish`.
  `CreateChannel` goes through the controller.
* **HTTP**: channel create/update/delete go through the cluster. Publish is routed like the
  wire. Messages, message detail, DLQ list/summary/retry/discard/export, replay and group
  reset/delete fan out to partition leaders over `/internal/v1/*` (cluster-secret middleware).
  Each node answers only for partitions it currently leads and says which ones; messages, DLQ
  and replay verify that the answers cover every partition with a leader (503 "changing
  leaders; retry" otherwise — offline partitions are skipped). DLQ retry/discard check coverage
  after acting (a 503 means "applied where reachable", retrying is safe); the export probes
  coverage before streaming (proper 503) and re-checks every page (a leadership move mid-export
  aborts the download). Replay: leaders read their partitions (below the HW), the copies are
  published through the cluster publish path. Group delete is two-phase (every leader checks
  404/409 first, then the leaders holding the group delete it), so a group with consumers on
  one node is never half-deleted. Metadata reads on a node are local replica reads: a write
  made through another node becomes visible after Raft replication (milliseconds).
* **Graceful shutdown** (`Cluster::leave`, SIGINT/SIGTERM): the node drains its leaderships
  (`Drain`), stops following (reconciler, ISR persister, heartbeats, replication fetchers), then
  sends `Leave{node, incarnation}`: the controller marks it `Down`, fails over anything it still
  leads, drops it from every ISR and ignores that incarnation's heartbeats. Peers' publish acks
  therefore don't wait for `node_timeout` (measured: < 100 ms ack gap). Budget 4 s; on failure
  the normal Down detection applies. A leaving controller still costs a Raft election
  (openraft 0.9 has no leadership transfer).

## Automatic rebalancing (v0.4) — `fluxmq-cluster`

Operator view and the move protocol table: `docs/CLUSTER.md` → *Automatic rebalancing*.
Contract: `fluxmq_proto::cluster` (v0.4 block).

* **Planner** (`rebalance.rs`, pure, deterministic): `plan(assignments, PlacementView
  {alive, eligible, replace, decommission}, rf, RebalanceConfig, storage, PlanOptions) →
  Vec<PartitionMove>`. Repair (replace replicas on long-down nodes and — v0.5 — every replica
  on a decommissioning node, alive or not; grow capped RF), then greedy replica balance
  (most → least loaded eligible node while they differ by ≥ 2; runs when `max − min` exceeds
  the threshold or on a manual request), then greedy preferred-leader balance by reordering.
  Each greedy step lowers the sum of squared loads (terminates); triggers are evaluated on
  post-repair loads, so re-planning a plan's result is empty. Also pure: the move
  transitions (`add_replicas` at the same epoch, `commit_replicas` with epoch + 1 on removal
  or leader change, `phase` = where an assignment stands relative to a move). Unit tests +
  seeded randomized property tests (no extra dependency).
* **Executor + triggers** (`rebalancer.rs`, a controller task every 500 ms, under the
  controller lock): drives `pending → catching_up → finalizing → done` / `rolled_back` with
  CAS assignment writes (retried on conflict), ≤ `max_concurrent_moves` in flight, timeouts
  from the persisted `started_at_ms`, pause/resume/cancel, lag of new replicas from the
  leaders' `/internal/v1/replication-status` (every 2 s, held in memory and attached to
  status responses). Plan at `cluster/rebalance/plan` (assignment written first, plan second;
  `phase` lets a new controller adopt either order). Config at `cluster/rebalance/config`,
  seeded once from `--auto-rebalance`. Triggers at most every `rebalance_interval` (30 s).
  Down-since comes from the controller's heartbeat bookkeeping; storage bytes ride on
  heartbeats (`Heartbeat.storage_bytes`, every 10th beat).
* **Reconciler additions** (`reconcile.rs`, every node): catch-up throttles for partitions of
  the active plan in `catching_up` (`set_catch_up_throttle`, cleared afterwards), and
  `remove_replica(channel, partition, epoch)` once per epoch for every partition whose
  committed replica list doesn't contain the node (after the `None` role is applied and only
  once caught up with the controller).
* **API / ops**: `Cluster::{rebalance_status, set_rebalance_config, rebalance,
  rebalance_control, rebalance_plan}`; new `ControllerOp`s (`RebalanceStatus`,
  `RebalanceConfig`, `Rebalance`, `RebalanceControl`) carry the status back in
  `ControllerOk.rebalance`, so any node serves the REST routes.
* **Events**: the telemetry collector diffs the replicated plan (`ClusterFacts.rebalance`)
  like peer up/down: `RebalanceStarted`, `ReplicaRepairStarted`, `PartitionMoved`,
  `RebalanceCompleted`, `RebalanceFailed`.

## Node decommission and forget (v0.5) — `fluxmq-cluster`

Operator view: `docs/CLUSTER.md` → *Node decommission and forget*. Contract:
`fluxmq_proto::cluster` (decommission block). Code: `decommission.rs`.

* **State** at `cluster/decommission/<id>` (`DecommissionDoc` = `DecommissionStatus` +
  incarnation, plan id / count, forget flag), so a new controller resumes. Driven by
  `decommission::step` at the start of every rebalance step (controller lock held); operator
  commands are `ControllerOp::{Decommission, Forget}` with the status in
  `ControllerOk.decommission`; `Cluster::{decommission, decommission_status, forget_node}`.
* **Liveness / membership**: nodes with an in-progress record keep registry status
  `Decommissioning` (any incarnation) and are in `View.leaving` (never leaders, never placed);
  `membership()` never adds or promotes them. `Snapshot.decommission` feeds
  `PlacementView.decommission` (never eligible, every replica repaired) in every plan; the
  decommission's own repair-only plans use any live `Up` node (`decommission_moves`) and run in
  every mode.
* **Membership removal**: openraft `change_membership(RemoveVoters | RemoveNodes, retain =
  false)` after a quorum check (≥ 1 remaining voter, live majority of the remaining and of the
  current voters). The controller removes itself the same way; openraft steps it down after
  committing and applying the change, and the next controller deletes the registry entry
  (no leadership transfer in openraft 0.9).
* **Removed processes**: `handle_heartbeat` refuses incarnations ≤ the recorded one with
  `RpcErrorKind::Removed`; the tick purges any raced `seen` entry. The refused process
  `retire`s (fenced, heartbeats/reconciler stop, replication and Raft shut down) and writes
  `<data-dir>/cluster-removed`; on the next start the Raft dir is wiped and marked
  `no-bootstrap`, and the node joins as a new learner (newer incarnation). The refused
  incarnation lives in `cluster/removed/<id>` (deleted once a newer incarnation registers);
  finished records are deleted 24 h after they end.
* **Cancel / failure**: a cancelled decommission plan (`POST /api/cluster/rebalance/cancel`)
  ends the decommission `failed` (`cancelled`); a failed decommission drops out of the
  in-progress set, so liveness writes the node back as `Up`.
* **Audit / dry run**: `Runtime::decommission_events` (broadcast of start / end on the
  controller) → `node.decommission` audit entries (server task); `ControllerOp::
  DecommissionDryRun` returns the checks' verdict and the unpersisted repair plan.
* **Events**: `Cluster::decommission_records()` (every record with its origin: decommission or
  forget) is part of the telemetry `ClusterFacts`; the collector diffs it per tick like the
  rebalance plan (first observation = baseline, record identity = node + `started_at_ms`) and
  emits `node_decommission_started` / `node_decommissioned` / `node_decommission_failed` /
  `node_forgotten` on every node.

## Replica moves: storage and replication support (v0.4)

`fluxmq-replication` + `fluxmq-core`. Operator view: `docs/CLUSTER.md` → *Replica moves*.

* **Non-zero base offset (storage).** A partition log's first segment may start at any
  offset N: `PartitionWriter::truncate` outside `[low, high]` (follower `reset_to`) replaces
  the log with one empty segment `{N:020}.log` (old files removed first; a crash in between
  leaves either a contiguous suffix of the old segments or an empty directory, and both heal
  on the next fetch). Recovery derives `low`/`high` from the segment names and records.
  Reads below `low` start at `low`. Retention removes whole non-active segments as usual.
  Group "earliest" and the cursor clamp use `low`. Stats report `low_offset = N`.
* **Bootstrap.** The fetcher answers the leader's `OffsetOutOfRange` below its log start
  with `reset_to(log_start)`.
* **State snapshot.** Every leadership starts every follower with a snapshot (all group
  offsets + the partition's DLQ). `record_fetch` admits a follower to the ISR only when its
  `StateCursor.epoch` is the current epoch, i.e. it has applied that snapshot. A leader
  re-elected at a higher epoch without a leader change keeps its group dispatch state (no
  redelivery for replica-set changes).
* **Removed replica.** `ReplicationManager::remove_replica` → `ReplicaHandle::remove_local_data(epoch)`:
  offline at `epoch` (stale epochs and "still a replica at this epoch" are refused), writer
  command `Purge` (barrier, fenced on "offline at `epoch`": log restarts empty at 0), epoch
  history cleared, group positions and DLQ entries of the partition dropped, then writer
  command `Release`: if every partition is offline, empty at 0 and has no DLQ entries, the
  channel directory is removed and `ChannelFiles` is marked *released* (metadata writes are
  skipped, `update`/`delete` work without a directory). The writer *materializes* released
  storage before the next append, replicated append or truncation: directory +
  `channel.json` first, then the partition directories, then the replica files.
  `set_role` for a partition being removed fails with `RemovalInProgress` (retry).
* **Throttle.** `ReplicationManager::set_catch_up_throttle(channel, partition, bytes_per_sec)`.
  The leader's fetch handler applies a per-follower token bucket only when the follower is
  behind and not in the ISR. A fetch waits at most 500 ms for budget, then returns metadata
  and state without records (`FetchRequest.max_bytes = 0`).
* **Lock order.** Unchanged. The writer may additionally read `Channel::config` while
  materializing (released before the `ChannelFiles` lock); see `fluxmq-core/src/lib.rs`.

## TLS (v0.4) — `fluxmq-tls`

One crypto stack: rustls with the aws-lc-rs provider (the one `reqwest` and `lettre` already
use; `ring` is not linked), TLS 1.2 + 1.3. Three independently configured surfaces
(`fluxmq_proto::tls`):

| Surface | Listener | Server config | Clients |
|---|---|---|---|
| `client` | `net::listener` (7420) | `--tls-cert`; `--tls-client-ca` → client certificate required | SDK `ClientOptions::tls` / `tls://` addresses |
| `http` | `fluxmq_tls::http::serve` (7421): hyper-util auto builder + tokio-rustls, ALPN `http/1.1` (WebSocket) | `--http-tls-cert` or `--tls-cert`; in a TLS cluster client certificates are requested (optional, cluster CA) for peers' `/internal/*` | browsers, REST, peers |
| `cluster` | Raft RPC (axum over `fluxmq_tls::http::serve`), replication (`fluxmq-replication::transport`), internal HTTP | node certificate, client certificate required (cluster CA) | `RpcClient::with_tls` (reqwest, preconfigured rustls), `PeerClient::connect_with` |

**Crate layout.** `pem` (rustls-pki-types parser), `client` (`ClientTlsBuilder`: CA file/bytes
→ webpki, or the platform verifier for the OS trust store; identity; danger mode) — the only
modules the SDK compiles. Feature `server`: `reload::ReloadableCert` (implements
`ResolvesServerCert` and `ResolvesClientCert`), `server` (config builders), `cluster::ClusterTls`
(acceptor/connector, a verifier that requires a chain to the cluster CA but ignores names, plus
an internal-HTTP variant falling back to the OS trust store for a public HTTP certificate),
`registry::TlsRegistry` (per-surface state, counters, reload loop, alerts), `http` (serving +
`PeerIdentity` request extension + the `/internal/*` guard), `info` (x509-parser),
`dev` (rcgen, `fluxmq tls gen-dev`).

**Connections are generic over the stream.** `net::connection::run_io<R, W>` takes any
`AsyncRead`/`AsyncWrite` halves: plaintext keeps `TcpStream::into_split` (no change on the hot
path), TLS uses `tokio::io::split`. Replication uses a static-dispatch enum of plain/TLS halves.
The SDK's reader/writer tasks are generic the same way.

**Handshake failures** (counted per surface): plaintext on a TLS port (detected from the first
byte — a binary-protocol client gets its `Hello` answered with `ProtocolViolation: this port
requires TLS` + `Goodbye`, an HTTP client a plain `400`), bad / missing / foreign client
certificate, protocol mismatch, handshake timeout (the client handshake timeout, 10 s for HTTP
and cluster).

**Reload.** `ReloadableCert::check` compares (mtime, size, inode) of cert and key with the last
attempt; the registry runs it every 30 s on a blocking thread and `reload` on SIGHUP (handler
installed only when TLS is configured, so SIGHUP keeps its default action otherwise). A pair
that fails to load (bad PEM, key ≠ certificate) leaves the previous one in place, sets
`last_error`, emits `ReloadFailure` → `EventKind::TlsReloadFailed` (subject = surface) and the
`tls_reload_failed:{node}/{surface}` alert; it is retried when either file changes again. New
handshakes use the new pair; live connections are untouched. CA bundles are read at startup.

**Alerts** come from `TlsRegistry::alerts` through `TelemetryHub::set_alert_source` (same
hysteresis, events and notifications as built-in alerts): `tls_cert_expiring:{node}/{surface}`
warning within `CERT_EXPIRY_WARNING_DAYS` (14), critical within 3 days or expired. They are
computed node-locally and shared cluster-wide: `snapshot::node_stats` appends the node's
alert-source output to its `NodeStatsSnapshot` as an optional postcard trailer
(`Vec<Alert>`; format stays 1 — older decoders ignore trailing bytes, a missing trailer
decodes as no alerts), `ClusterState::view` collects the live peers' alerts into
`ClusterInfo::peer_alerts`, and `alerts::cluster_candidates` adds them. Every node therefore
shows `tls_cert_expiring:n2/cluster`, and the controller (the only notifier) notifies it once.
The `TlsReloadFailed` *event* stays node-local (the feed only carries the controller's
events); the alert covers it.

**mTLS principal hint.** On the client port the verified certificate's CN is appended to the
connection's remote address (`127.0.0.1:52144 (tls:orders-svc)`), visible in consumer
instances and logs. Authorization still comes from the `Hello` token.

## Telemetry contract v1 (WebSocket `/ws`)

Defined in `fluxmq_proto::telemetry`. JSON text frames.

```jsonc
// server → client, first frame
{"type":"hello","contract_version":1,"broker_version":"0.2.0","node_id":"n1","server_time_ms":0}
// client → server
{"op":"subscribe","topics":["cluster.overview","channels.summary","alerts","events"]}
{"op":"unsubscribe","topics":["channel.orders"]}
{"op":"ping","nonce":1}
// server → client
{"type":"snapshot","topic":"cluster.overview","seq":1,"ts":0,"data":{ /* ClusterOverview */ }}
{"type":"delta","topic":"cluster.overview","seq":2,"ts":0,"data":{"ingress_rate":83172.5}}
{"type":"error","topic":"channel.nope","message":"unknown channel"}
```

Deltas are RFC 7386 JSON merge patches against the previous state of the topic (objects merge,
arrays/scalars replace, `null` deletes). On a `seq` gap the client re-subscribes to get a
fresh snapshot. Topics and cadences: `cluster.overview` 250 ms, `channels.summary` 500 ms,
`channel.{name}` 500 ms, `channel.{name}.consumers` 500 ms, `channel.{name}.tail` 250 ms,
`nodes` 1 s, `alerts` and `events` on change.

## Health rules (shared by telemetry + UI copy)

* Group `critical`: has lag and zero instances, or lag grew > 10× and > 10k within 5 min,
  or no acks for 30 s while lag > 0. `warning`: lag > 1k and growing for 60 s, lag ≥ 100k and
  not draining (not lower than 60 s ago), nack ratio > 5 %, or DLQ grew in the last 5 min.
* Instance `warning` when its ack rate < 50 % of the group median (with ≥ 3 instances) or its
  processing p99 > 3× the group's.
* One roll-up rule: instance → group → channel → cluster, each = worst of its own conditions
  and its children; the cluster also includes node health (disk > 90 % critical, > 80 % warning;
  fsync p99 > 50 ms warning). An open alert raises the object it is about (instance via
  `params.instance`, group, channel, cluster) to at least its severity, so a status never reads
  healthy next to an open alert.
* Alerts and events carry `params` (structured values behind the English title/detail) so the
  console can localise them; params are not carried in the cluster snapshot trailer.

## Monitoring (stage 9)

**Prometheus — `GET /metrics`** on every node, text format 0.0.4, **node-local** (scrape every
node; aggregate in PromQL). Rendered from the collector's latest local `BrokerStats` sample (no
extra broker sampling, no JSON). Labels are bounded: `channel`, `group`, `node` (+ `severity` on
`fluxmq_alerts_open`). Latency histograms: HDR (µs) → fixed buckets in seconds
`0.0001 … 60` + `+Inf`; `_sum` is approximated from the HDR mean.

| Metric | Type | Labels |
|---|---|---|
| `fluxmq_node_info` (=1; `cluster`, `version`) | gauge | node |
| `fluxmq_node_start_time_seconds`, `fluxmq_node_uptime_seconds`, `fluxmq_node_connections` | gauge | node |
| `fluxmq_node_cpu_percent`, `fluxmq_node_memory_bytes`, `fluxmq_node_memory_total_bytes` | gauge | node |
| `fluxmq_node_disk_used_bytes`, `fluxmq_node_disk_total_bytes`, `fluxmq_node_storage_bytes` | gauge | node |
| `fluxmq_node_fsync_p99_seconds` (rate window) / `fluxmq_node_fsync_latency_seconds` | gauge / histogram | node |
| `fluxmq_node_health`, `fluxmq_health` (0 healthy, 1 warning, 2 critical) | gauge | node |
| `fluxmq_alerts_open` | gauge | node, severity |
| `fluxmq_channel_{published_messages,published_bytes,delivered_messages,delivered_bytes}_total` | counter | channel |
| `fluxmq_channel_{acked,nacked,timed_out,retries,dead_lettered}_total` | counter | channel |
| `fluxmq_channel_{dlq_messages,storage_bytes,messages,partitions,lag_messages}` | gauge | channel |
| `fluxmq_channel_publish_latency_seconds`, `fluxmq_channel_delivery_latency_seconds` | histogram | channel |
| `fluxmq_group_{lag,in_flight,retry_pending}_messages`, `fluxmq_group_consumers` | gauge | channel, group |
| `fluxmq_group_{delivered_messages,acked,nacked,dead_lettered}_total` | counter | channel, group |
| `fluxmq_group_retention_skipped_messages_total` | counter | channel, group |
| `fluxmq_group_processing_latency_seconds` | histogram | channel, group |
| `fluxmq_router_routes` / `fluxmq_router_{evaluated,matched,forwarded,failed}_total` | gauge / counter | node |
| clustered only: `fluxmq_cluster_{is_controller,nodes,nodes_up,under_replicated_partitions,offline_partitions,raft_term,metadata_epoch}` | gauge | node |
| TLS configured only (v0.4): `fluxmq_tls_handshake_failures_total` / `fluxmq_tls_cert_expiry_seconds` (enabled surfaces; negative once expired) | counter / gauge | node, surface |

**Cluster aggregation (console).** On a clustered node `fluxmq-telemetry::ClusterSource` pulls
every peer's `NodeStatsSnapshot` from `GET /internal/v1/node-stats` once per second (timeout
900 ms, `x-fluxmq-cluster-secret` header) on its own task; the collector only merges the latest
decoded snapshots, so a slow peer never delays a tick. Snapshot = the node's cumulative
`BrokerStats` (counters + HDR histograms as V2 blobs, postcard) + node-only values (resource
sample, fsync p99, node health) + the partitions it leads. It is encoded at most once per
collector sample however many peers pull. Merge semantics:

* channel/group counters and latency histograms: summed (histograms merged) over the nodes
  that **lead at least one partition** of the channel; each partition is taken from its leader;
* group lag / in-flight / retry-pending and DLQ size: summed over the same nodes;
* consumer instances keep node attribution: id `"{node}/{client}#{conn}"`;
* connections and storage: summed; fsync stays per node (`nodes[*].disk_latency_p99_ms`);
* a peer without a successful pull for 3.5 s (or reported `down` by the controller) is
  **down**: excluded from the merge, listed in `nodes` with `status: "down"`,
  `health: "critical"`; `cluster.overview.partial = true`; `nodes_total` counts real members,
  `nodes_up` the members not `down` (status-based: the console's "N/M nodes up"; the pill's
  colour comes from overview `health`), `nodes_healthy` those up and not `critical`;
* any change of the merged set (peer down/up, restart, leadership move) restarts the rate
  window, so cumulative jumps never show as rate spikes (rates read 0 for one tick).

Cluster alerts (ids → UI link `/nodes`): `node_down:{node}` (critical),
`under_replicated_partitions:cluster` (warning), `offline_partitions:cluster` (critical); events
`controller_changed`, `node_down`, `node_up`. Overview health is at least `warning` while data is
partial or partitions are under-replicated, `critical` with offline partitions. A single node
runs none of this (no polling, no merge, identical documents).

## Engineering standards

* Rust 2024, `cargo clippy --workspace --all-targets -- -D warnings` clean, `cargo fmt`.
* No `unwrap()` on I/O or input paths; `expect()` only for invariants, with a reason.
* No blocking calls on async executors (file I/O in core runs on dedicated threads or
  `spawn_blocking`).
* Every crate has unit tests; `fluxmq-server/tests/` holds end-to-end tests over real TCP/HTTP.
* Public items documented. Keep `docs/` in sync with behaviour.

## Implementation notes (decisions made during v0.1 build)

Telemetry:
* Documents are sent with `null` fields omitted — the UI treats a missing field as `null`.
* `seq` is per session per topic; a (re)subscribe snapshot restarts at the next value.
* `ChannelSummary.lag` = max over groups; `ClusterOverview.total_lag` and history `lag` = sum of channel lags.
  `GroupState.dlq` = cumulative dead-lettered count of the group.
* Health evaluation details: "no acks for 30 s" only counts while the group has deliverable work
  (`lag > retry_pending`); "lag growing for 60 s" = lag(now) > lag(−30 s) > lag(−60 s); nack-ratio
  rule needs ≥ 20 events/min; slow-instance rule needs instance p99 ≥ 5 ms, ≥ 5 samples and
  group median ack rate ≥ 1/s. Instance warnings do not degrade group health. Channel publish
  p99 > 100 ms raises `high_publish_latency`.
* Alerts open after 3 consecutive failing 1 s evaluations and resolve after 10 clean ones.
* Events feed: a channel's `dead_lettered` events within 60 s merge into one entry (count and
  time updated, moved to the top) so a steady DLQ trickle can't push everything else out.
* Byte sizes in alert text use binary units with the UI's labels (1 GB = 2^30 B), matching the
  gauges. `cpu_percent` fields are `f64` rounded to 0.1.
* History rollups average finer tiers, except latency percentiles which take the max. Only the
  1 m / 30 m tiers persist across restarts.

API:
* `CoreError::ShuttingDown` → HTTP 503 with `error: "internal"`.
* `/ws` accepts the token as `?token=` (browsers can't set WS headers); HTTP trace spans log
  the path only, never the query string.
* `GET /api/health` is never token-protected; the UI checks a token against `GET /api/alerts`.
* Group offset reset only applies to existing groups (core can pre-create a group at a position,
  but over REST a typo would leave a phantom group with lag and no consumers).
* A failed handshake (version/token) gets an error `Response` to the Hello, then `Goodbye`.
* `/api/search` with empty `q` returns up to 50 results.
