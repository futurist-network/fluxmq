---
title: Notifications
group: console
order: 12
summary: Alerts and broker events to Telegram, Slack, Discord, email, PagerDuty or your webhook — notifiers, rules, delivery history and a pull feed for external systems.
console: /settings/notifications
---

FluxMQ reports problems itself: alerts (lag growing, no consumers, DLQ growing, a node down…) and, if you want, broker events (channel deleted, consumer disconnected…) go where your team looks. Viewing needs the viewer role; creating, changing, deleting and test sends need admin.

The model is simple: a **notifier** is *where* to send; a **rule** is *what* and *to whom*.

## Notifiers {#notifiers}

| Type | What to provide |
|---|---|
| Telegram | bot token, chat id, optional topic id (for forum groups); the form has a "How to get the chat id" hint |
| Slack | an incoming webhook URL (`https://hooks.slack.com/…`): Apps → Incoming Webhooks → Add to channel |
| Discord | a channel webhook URL: channel settings → Integrations → Webhooks → New webhook → Copy URL |
| Email (SMTP) | SMTP host, port, encryption (STARTTLS — 587, TLS — 465, none — local relay only), username and password (optional), From, recipients |
| PagerDuty | an Events API v2 integration key; an open alert triggers an incident, a resolved one resolves it (dedup key = alert id) |
| Webhook | URL, optional signing secret (`X-FluxMQ-Signature`, as for [webhooks](/docs/webhooks#request)) and headers |

The list shows type, name and destination, last delivery (sent/failed, time and a short reason), an Enabled switch and **Send test**. A disabled notifier sends nothing — such messages appear as "suppressed" in the history.

**Secrets** (bot token, Slack/Discord webhook URLs, SMTP password, PagerDuty key, signing secret, header values) are never returned by the server: the field shows `••••••••` with Replace. In the edit dialog **Send test** tests what's in the form now, unsaved edits included, and saves nothing. A notifier used by a rule can't be deleted (`409`) — remove it from the rules first.

## Rules {#rules}

| Field | What it sets |
|---|---|
| Name | how the rule shows in the list and history |
| Send to | one or more notifiers |
| Minimum severity | Warning (everything) or Critical — alerts only |
| Alert kinds | empty = any; codes in [Alerts](/docs/alerts#kinds) |
| Broker events | empty = **none** (events are frequent, opt in): `channel_deleted`, `dead_lettered`, `node_down`… |
| Channel globs | `orders.*` etc.; empty = every channel and cluster-wide alerts; a hint shows how many channels match today. With globs set, alerts without a channel (e.g. `node_down`) don't match |
| Group wait | none / 10 s / 30 s / 1 min / 5 min — everything that fires within the window arrives as one message ("3 issues need attention"); an alert that opens and resolves inside the window sends nothing |
| Repeat while open | never / 1 / 4 / 12 / 24 h — a reminder for a still-open alert |
| Recovery | whether to send "Resolved: …" when an alert closes |

Defaults: 30 s group wait, repeat every 4 hours, recovery messages on. The empty page suggests starting with "Critical → on-call".

## Delivery history {#history}

Recent sends (up to 1,000 on screen): time, status (**sent**, **failed**, **suppressed**), notifier and rule, text, error and attempts; a Failed filter. A failed send is retried up to 3 times (after 2 and 10 s). Errors are short — `connection refused`, `timeout`, `HTTP 503`, `TLS error`, `DNS error`, `SMTP 550`; the full text is in a tooltip.

History is kept for 7 days in the system channel `$system.notifications` and survives restarts and controller changes. In a cluster **only the controller** sends, so there are no duplicates.

> [!TIP]
> Start the broker with `--public-url https://mq.example.com` and every notification links to the right console screen.

## Feed for external systems (pull API) {#pull-feed}

The same content — every alert transition and every event — is appended to the system channel `$system.events`. An external system can pull it, no inbound endpoint needed:

```bash
# the latest 20 items (oldest → newest)
curl -s "http://localhost:7421/api/events/feed?tail=20" -H "Authorization: Bearer $TOKEN"
# then only new ones: pass next_cursor and wait up to 25 s (long-poll)
curl -s "http://localhost:7421/api/events/feed?after=<next_cursor>&wait_ms=25000" -H "Authorization: Bearer $TOKEN"
```

`after=latest` returns only what appears after the request; without `after` it starts at the oldest item. `gap: true` in a response means some items were already removed by retention. A viewer token with consume scope `$system.events` is enough for such a client. The bottom of the tab shows the feed live.

Step by step, with Telegram: [Set up alerts and notifications](/docs/howto-alerts).
