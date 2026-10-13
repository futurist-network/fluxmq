---
title: Handle the dead-letter queue
group: howto
order: 5
summary: How to find out why messages were dead-lettered, fix the cause and safely retry or discard them.
---

## 1. Look at the reasons {#reasons}

Open [Dead Letters](/docs/dlq). The cards at the top group messages by their last failure reason. There are usually only a few:

| Kind of reason | Example | Meaning |
|---|---|---|
| A temporary dependency | `PostgreSQL timeout`, `HTTP 503`, `connection refused` | an external system was down for longer than all the attempts lasted |
| Bad data | `Invalid schema`, `missing field order_id` | the message will never be processed — the producer is at fault |
| Routing | `route target unavailable: orders.md` | a route's target channel was deleted or is unavailable (group `router`) |
| Webhook | `HTTP 404`, `timeout`, `blocked by policy` | the webhook endpoint answers with an error (group `$webhook:<name>`) |
| Ack timeout | (empty last reason or a timeout) | the handler took longer than the ack timeout or died without a nack |

## 2. Inspect a couple of messages {#inspect}

Click a reason, then a message: payload, headers and the **delivery timeline** show how many attempts there were, with which delays and how each ended. The channel and group tell you whose service it is.

## 3. Fix the cause {#fix}

- temporary dependency — make sure it works again;
- bad data — fix the producer (the messages themselves probably need discarding, or fixing and republishing);
- routing — recreate the target channel or fix the route;
- webhook — check the endpoint with **Send test** on [Webhooks](/docs/webhooks);
- timeout — raise the channel's ack timeout or speed up processing.

## 4. Retry or discard {#act}

- **Retry all N** with a reason selected sends all its messages back to their original group with a fresh attempt budget. Only the group that failed a message receives it again.
- **Retry selected** — to be safe, start with one or two messages, check they were processed (gone from the DLQ, the timeline ends with an ack), then retry the rest.
- **Discard** — for messages you don't need. If you might, **Export NDJSON** first.

Retry, discard and export require the operator role and are recorded in the audit log.

> [!WARNING]
> Don't retry before the cause is fixed: the messages go through every attempt again, load the dependency and land back in the DLQ.

## 5. Over REST and for automation {#rest}

```bash
H=http://localhost:7421; A="Authorization: Bearer $TOKEN"
curl -s -H "$A" $H/api/dlq/summary
curl -s -H "$A" -X POST $H/api/dlq/retry -H 'content-type: application/json' -d '{"reason":"PostgreSQL timeout"}'
curl -s -H "$A" -X POST $H/api/dlq/discard -H 'content-type: application/json' -d '{"reason":"Invalid schema","channel":"orders"}'
curl -s -H "$A" "$H/api/dlq/export?channel=orders" > dlq.ndjson
```

A `retry` / `discard` body sets exactly one selector: `ids` (`[channel, id]` pairs), `reason` (optionally with `channel`), `channel`, or `"all": true`. The answer is `{"affected": N}`.

## 6. So the DLQ never grows silently {#prevent}

- A [notification](/docs/howto-alerts) rule on `dlq_growth` and, if you like, the `dead_lettered` event.
- Short, stable `nack` reasons, so grouping works.
- Tell temporary (`nack(reason, true)`) from permanent (`nack(reason, false)`) failures — permanent ones then don't waste attempts.
