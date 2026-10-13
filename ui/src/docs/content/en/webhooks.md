---
title: Webhooks
group: console
order: 6
summary: HTTP push — the broker POSTs a channel's messages to your endpoint, with signatures, a filter, retries and a DLQ.
console: /webhooks
---

A **webhook** is a subscription where the broker itself POSTs a channel's messages to your HTTP endpoint. The receiver needs no SDK and no long-lived connection — just an HTTP server that answers `2xx`.

## How it works {#how}

- Every webhook is a consumer group `$webhook:<name>` on its channel. Its lag, retries, delivery timeline and DLQ show in the console like any other group.
- In a cluster each node delivers the partitions it leads.
- The filter is the same [filter language](/docs/ref-filter) as routes. Non-matching messages are acked without a request and counted as "Skipped by filter".
- Disabling pauses delivery (the group and its position are kept). Deleting stops delivery and removes the group; dead letters stay in the DLQ.

## The list {#list}

| Column | Meaning |
|---|---|
| Name | the webhook name; next to it the format (envelope / raw body), batch size, paused |
| Channel → endpoint | the channel and the URL host |
| Delivered / Failed / Skipped | cluster-wide counters since the processes started |
| p99 | your server's response latency |
| Last status | HTTP code of the last attempt; only errors are coloured: 5xx, 429, 408 red, other 4xx amber |
| In flight | requests outstanding now |

A failing webhook is highlighted, with a link to its channel's dead letters; **paused** next to the status means the circuit breaker holds delivery (see [When the endpoint is down](#breaker)). **Send test** sends one synthetic delivery (`delivery_id` like `test/…`). The sidebar badge counts webhooks that are failing right now.

## The editor {#webhook-editor}

Requires the admin role.

| Field | What it sets |
|---|---|
| Name | `[a-z0-9._-]`, becomes the group `$webhook:<name>`; can't be renamed later |
| Channel | fixed after creation |
| URL | the endpoint |
| Payload format | **Envelope (JSON)** — metadata and payload; **Raw body** — only the message bytes, metadata in `X-FluxMQ-*` headers (batch size always 1) |
| Signing secret | optional; with it every request is signed with `X-FluxMQ-Signature` |
| Headers | your headers (e.g. `Authorization`); their values are secrets too |
| Filter | conditions, as in the route editor (rows or text) |
| Timeout | 100–60,000 ms, default 10,000; together with the batch linger it must stay below the channel's ack timeout |
| Max in flight | 1–64, default 4; **1 = strict order** within a partition |
| Batch size | 1–500 messages per request (a JSON array of envelopes) |
| Batch linger | 0–10,000 ms to wait for a batch to fill |
| 4xx responses | by default 4xx (except 408, 425, 429) is not retried; you can turn retries on |
| When the endpoint is down | pause delivery after N failed requests in a row (1–1,000); on by default for webhooks created in the console, 5 — see [below](#breaker) |
| Start from | new messages only / the oldest retained message / a point in time; creation only |

After saving, secrets show as `••••••••` with Replace / Keep stored; the server never returns them.

On the right, **Send test**: status, latency and the start of the response. A new webhook tests the draft; a saved one tests what is in the form now, unsaved edits included; nothing is saved. Errors are short (`HTTP 503`, `timeout`, `connection refused`, `TLS error`, `DNS error`, `blocked by policy`), the full text is in a tooltip. The panel also shows a sample request body and ready-to-copy signature checks in Node.js and Python.

## Activity, recent requests, rewind {#activity}

A saved webhook's page starts with:

- **Activity** — delivered and failed messages per bucket (10 s for 15 min and 1 h, 5 min for 24 h) and the response latency (average and maximum), summed over all nodes.
- **Recent requests** — the last 100 requests per node, newest first: time, message (`delivery_id` of the first one, `+N in batch`), attempt, result (Delivered, Will retry, To DLQ, and endpoint checks while paused), HTTP status or short error (full text in the tooltip), latency and, in a cluster, the node.
- **Rewind position…** (operator role) on the right — the consumer group reset: redeliver from the oldest retained message or a point in time, or skip to now. Works while the webhook is running.

Activity and recent requests live in each node's memory and start over after a restart; counters in the list behave the same way.

## When the endpoint is down {#breaker}

Without protection, every message keeps being retried while the endpoint is down and lands in the DLQ when its attempts run out — an outage of a few minutes can move a whole backlog to the DLQ. With **Pause delivery after N failed requests in a row** (the circuit breaker):

- Timeouts, connection errors, `5xx`, `408`, `425` and `429` count as failures; any other answer resets the count.
- After N in a row the webhook stops taking messages from the channel. They wait there **keeping their attempts**; nothing goes to the DLQ. The page shows a banner, the list shows **paused**, and the sidebar badge counts it as failing.
- The endpoint is checked with a test delivery (`delivery_id` like `probe/…`, header `flux-test: true`): after 2 s, then twice as rarely, up to every 60 s. Any answer that isn't a failure — including a `4xx` rejecting the test body — resumes delivery, from where it stopped.
- Before the pause the failed requests used up attempts of their messages (in a full outage a message gets at most about N). Keep N below the channel's max attempts so those messages don't reach the DLQ.
- Messages wait under the channel's retention: if the outage outlasts it, the oldest are deleted without delivery. Lag alerts warn about that.
- In a cluster each node has its own breaker for the partitions it leads.

## What your server receives {#request}

`POST <url>`, `Content-Type: application/json`, the body is an envelope:

```json
{
  "delivery_id": "orders/1/0",
  "subscription": "orders-crm",
  "channel": "orders",
  "partition": 1,
  "offset": 0,
  "key": "order-1001",
  "headers": {},
  "timestamp_ms": 1791446775012,
  "attempt": 1,
  "payload": {"encoding": "utf8", "data": "{\"id\":1001,\"country\":\"MD\"}"}
}
```

- `delivery_id` (`channel/partition/offset`) is the same for every retry of a message — **dedupe on it**.
- `payload.encoding` — `utf8` for text or `base64` for binary data.
- Request headers: `X-FluxMQ-Delivery-Id`, `X-FluxMQ-Attempt` (attempt number from 1), your headers, and `X-FluxMQ-Signature: t=<unix time>,v1=<hex HMAC-SHA256(secret, "<t>.<body>")>` when a secret is set. Verify against the **raw** body, compare in constant time and reject old timestamps.

## Retries and the DLQ {#retries}

| Server answer | What the broker does | DLQ reason |
|---|---|---|
| `2xx` | acks | — |
| `408`, `425`, `429`, `5xx` | retry under the channel's retry policy | `HTTP <code>` |
| timeout | retry | `timeout` |
| connection refused, DNS, TLS | retry | `connection refused`, `DNS error`, `TLS error` |
| address blocked by the SSRF policy | straight to the DLQ, no retries | `blocked by policy` |
| other `4xx` | straight to the DLQ (retried with "retry 4xx") | `HTTP <code>` |
| `3xx` | straight to the DLQ: redirects aren't followed | `HTTP <code>` |

`Retry-After` is honoured, capped at the channel's longest backoff. When attempts run out, the message goes to the channel's DLQ with group `$webhook:<name>`; retry it from there.

## Security (SSRF) {#ssrf}

While auth is enforced, a webhook can't target loopback, private, link-local, CGNAT or multicast addresses (`127.0.0.1`, `10.0.0.0/8`, `192.168.0.0/16`, `169.254.169.254`, `::1`…). The address is checked on save, on test and on every delivery (DNS-rebinding protection); redirects and proxies aren't used. In open mode (no tokens) private addresses are allowed so you can test on localhost. To set it explicitly use `--webhooks-allow-private` (see [Flags and environment](/docs/ref-cli)).

## Ordering and throughput {#order}

- **Max in flight = 1** — strict order within a partition: while a message waits for a retry, later ones don't overtake it (the whole subscription pauses). Strict order is guaranteed with batch size 1.
- **Above 1** — requests run in parallel, no ordering guarantee, more throughput. Ballpark on a local receiver: ~25k msg/s at 1, ~65k msg/s at 16–64, ~500k msg/s at 4 with batches of 100.
- Need order per key rather than per whole partition — use a `keyed` channel.
