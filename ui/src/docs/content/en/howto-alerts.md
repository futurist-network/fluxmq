---
title: Set up alerts and notifications
group: howto
order: 6
summary: Get critical alerts in Telegram in a couple of minutes, wire up PagerDuty or your own webhook, and write rules that don't spam.
---

The broker opens alerts by itself — you only configure **where** they go. Requires the admin role.

## Telegram in a couple of minutes {#telegram}

1. **Create a bot.** In Telegram open @BotFather, send `/newbot`, choose a name and a username (ending in `bot`). It replies with a token like `7123456789:AAH…` — treat it as a password.
2. **Add the bot to a chat.** Private chat — send the bot `/start`. Group — add the bot and send `/start@your_bot`. Channel — make the bot an admin allowed to post and publish a post.
3. **Find the chat id:**

   ```bash
   curl -s "https://api.telegram.org/bot<TOKEN>/getUpdates"
   ```

   Look for `"chat":{"id":…}`: positive for a private chat, negative for groups, like `-1001234567890` for supergroups and channels. In a forum group the topic number is `message_thread_id`. An empty `result` — send another message to the chat and retry.
4. **Add a notifier**: Settings → Notifications → **Add Telegram** (or Add notifier → Telegram) → bot token, chat id, optional topic id → **Send test**. The message should arrive in the chat.
5. **Create a rule**: **New rule** → name "Critical → on-call", send to that notifier, minimum severity Critical, group wait 30 s, repeat every 4 hours, recovery on.

Typical test errors: `HTTP 401` (wrong bot token), `HTTP 400 … chat not found` (wrong chat id or the bot isn't in the chat), `HTTP 403 … bot was blocked by the user`.

A message looks like this:

```text
🔴 CRITICAL · Email queue is growing rapidly
Lag increased from 1.2k → 18k in 6 min
Possible reason: Consumers processing slower than producers.
Channel: email · Group: workers · Open for: 6m
Open in console
```

The "Open in console" link appears when the broker runs with `--public-url`.

## PagerDuty {#pagerduty}

In the PagerDuty service: Integrations → Add integration → **Events API V2**, copy the Integration Key. In FluxMQ add a PagerDuty notifier with that key. An open alert triggers an incident and a resolved one resolves it; the dedup key is the alert id, so reminders don't multiply incidents. A test send triggers and immediately resolves an `info` incident.

## Your own HTTP endpoint {#webhook}

Type **Webhook**: URL, optional signing secret and headers. The broker sends JSON:

```json
{
  "id": "gfznwqk01b",
  "cluster": "local",
  "sent_at_ms": 1791447236768,
  "rule": "Channel deletions",
  "status": "event",
  "alerts": [],
  "events": [{"id": 4, "ts": 1791447231765, "kind": "channel_deleted", "title": "Channel deleted",
              "subject": "orders.tmp", "severity": "warning"}],
  "console_url": "http://127.0.0.1:7421"
}
```

`status`: `firing`, `resolved`, `event` or `test`; `id` is the same for retries of one send — dedupe on it. With a secret the request is signed with `X-FluxMQ-Signature`, the same scheme as [HTTP webhooks](/docs/webhooks#request).

## Rules without noise {#rules}

| Goal | Rule |
|---|---|
| On-call gets only what's worth waking up for | severity Critical, no events, group wait 30 s, repeat 4 h |
| The orders team gets everything about its channels | severity Warning, globs `orders` and `orders.*`, kinds `lag_growth`, `consumer_stalled`, `no_consumers`, `dlq_growth` |
| Audit dangerous actions | event `channel_deleted`, group wait 5–10 s, to an audit notifier |
| Cluster health | kinds `node_down`, `under_replicated_partitions`, `offline_partitions` **without channel globs** — otherwise channel-less alerts don't match |

- Events are off by default (there are many) — opt into the ones you need.
- **Group wait** folds a burst into one message and sends nothing if an alert opens and resolves inside the window.
- **Delivery history** shows what was sent, what failed (with the reason) and what a disabled notifier suppressed.

The same over REST: `POST /api/notify/notifiers`, `POST /api/notify/notifiers/{id}/test`, `POST /api/notify/rules` — see [REST API](/docs/ref-rest).
