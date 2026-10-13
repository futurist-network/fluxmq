---
title: Dead letters (DLQ)
group: console
order: 7
summary: The dead-letter queue — an error inbox grouped by reason; retry, discard and export messages.
console: /dlq
---

Messages land here when they exhaust every delivery attempt (nacked or timed out `max attempts` times), or when a consumer rejects them with `retry = false`. Each one is stored with its **last failure reason**, so the screen works like an inbox: reason first, then messages.

## Reasons {#reasons}

At the top: reason cards — the reason text (`PostgreSQL timeout`, `HTTP 503`, `Invalid schema`, `route target unavailable: …`), how many messages have it and when the last one arrived. "All reasons" shows everything. The number next to Dead Letters in the sidebar is the total in the DLQ.

> [!TIP]
> Use short and **stable** reasons in `nack`: "PostgreSQL timeout", not the exception text with a request id. Then identical failures collect into one card and can be retried with a single button.

## The message list {#entries}

| Column | Meaning |
|---|---|
| Message | type (`flux-type`) and the DLQ entry id |
| Channel | link to the channel |
| Group | the consumer group that failed; `router` — failed route forwards (`$router`), `$webhook:<name>` — a webhook |
| Reason | shown when "All reasons" is selected |
| Key | the message key |
| Attempts | how many times it was delivered |
| Dead | when it was dead-lettered |

Click a row to open a drawer: reason, payload, headers and delivery timeline, plus **Retry** and **Discard** for that one message.

## Actions {#actions}

| Button | What it does | Role |
|---|---|---|
| Retry all N | send every message of the selected reason (or all) back to its original group with a fresh attempt budget | operator |
| Retry selected | the same for the ticked rows | operator |
| Discard all / Discard selected | delete permanently (confirmed; recorded in the audit log) | operator |
| Export NDJSON | download the messages as NDJSON (one JSON object per line) | operator |

A retried message goes back to **the same group** that failed it — the channel's other groups don't receive it again.

> [!WARNING]
> Fix the cause first (bring the database back, fix the schema, recreate the deleted target channel), then press Retry all. Otherwise the messages run through all their attempts again and come back here.

## Over REST {#rest}

```bash
H=http://localhost:7421
curl -s $H/api/dlq/summary                                   # counts per reason
curl -s "$H/api/dlq?reason=PostgreSQL%20timeout&limit=50"     # entries
curl -s -X POST $H/api/dlq/retry -H 'content-type: application/json' \
  -d '{"reason":"PostgreSQL timeout"}'                        # retry one reason
curl -s "$H/api/dlq/export?channel=orders" > dlq.ndjson       # export (operator role)
```

## What's normal {#normal}

An empty DLQ is normal. A handful of "bad data" messages (`Invalid schema`) means: fix the producer and discard them. A growing DLQ opens a "DLQ growth" alert and colours the Errors card on the [Dashboard](/docs/dashboard). Step by step: [Handle the DLQ](/docs/howto-handle-dlq).
