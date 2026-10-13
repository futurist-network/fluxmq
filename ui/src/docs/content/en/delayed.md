---
title: Delayed messages
group: console
order: 6.6
summary: "Deliver at 9:00" or "in 10 minutes": the broker keeps the message safely and publishes it to its channel at that time.
console: /scheduled
---

A **delayed message** is published to its channel at a given time instead of right away. Examples: a reminder in a day, a payment retry in an hour, "cancel the order if it is not paid by 18:00". No timer queue, cron or extra service of your own.

## How it works {#how}

- A delayed message is written to disk (and to replicas in a cluster) at once, in the system channel `$scheduled`. The publisher gets its confirmation after that write, as for any publish.
- When due, the broker publishes it to its channel. From then on it is an ordinary message: consumers, retries, DLQ, routes and webhooks work as usual.
- Precision is about 100 ms. If the broker was down at the due time, the message goes out as soon as it is back.
- The delay is at most **30 days**. A time in the past means "send now".
- The broker keeps only the due time and position of each waiting message in memory (about 100 bytes) and reads the payload from disk when sending, so a million delayed messages put no load on the system.
- Per-key order is the order of release: messages with the same key and due time go out in the order they were scheduled.

> [!NOTE]
> The guarantee is at least once. If a node fails exactly between publishing and marking the message sent, it may go out again after recovery. Both copies carry the same `flux-scheduled-id` header, so a consumer can recognize the repeat.

## How to send {#send}

**Console:** on a channel page, **Send message** → **When**: "Now", "In…" (seconds, minutes, hours, days) or "At…" (date and time). Needs the operator role.

**SDK (Rust):**

```rust
producer.send(message(body).delay(Duration::from_secs(600))).await?;   // in 10 minutes
producer.send(message(body).deliver_at(nine_am)).await?;                // at 9:00
```

**REST:** `delay_ms` (milliseconds from now, by the broker's clock) or `deliver_at_ms` (unix time, ms):

```bash
curl -X POST $FLUXMQ/api/channels/orders/publish -H 'content-type: application/json' \
  -d '{"messages":[{"key":"ord-8842","payload":{"encoding":"utf8","data":"{\"order_id\":8842}"},"delay_ms":3600000}]}'
```

**Any client:** the `flux-delay-ms` or `flux-deliver-at` header.

The id returned for a delayed message is its schedule id (`partition/offset` in `$scheduled`): cancel it or send it now with it. Once sent, the message gets an ordinary id in its channel and the headers `flux-scheduled-id` and `flux-scheduled-for`.

## The Delayed page {#page}

| Block | What it shows |
|---|---|
| Waiting | how many messages are scheduled, in how many channels |
| Next delivery | when the next one goes out |
| Delivered / Cancelled | counters since the nodes started |
| Channel filter | only the selected channel's messages |
| Coming up | the soonest messages over all nodes: when, channel, type and key, size, when scheduled |

Actions (operator role): **Send now** publishes the message immediately, **Cancel** means it will not be delivered.

## REST {#rest}

| Method and path | What it does | Role |
|---|---|---|
| `GET /api/scheduled?channel=&limit=` | the soonest delayed messages (default 100, max 1000), counts per channel | R |
| `DELETE /api/scheduled/{partition}/{offset}` | cancel | O |
| `POST /api/scheduled/{partition}/{offset}/release` | send now | O |

A message already sent or cancelled answers 404.
