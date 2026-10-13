---
title: Console basics
group: start
order: 4
summary: The sidebar, the top bar, the command palette, pause mode, roles and banners — how the console is organised.
---

The broker serves the console itself at `http://<host>:7421` (or `https://` with TLS). In a cluster any node serves it, and the numbers on every screen are aggregated over the whole cluster.

## Sidebar {#sidebar}

The order follows how an operator hunts a problem: cluster → channels → consumers → errors.

| Item | What's there | Article |
|---|---|---|
| Dashboard | cluster health, key signals, charts, problem channels, events | [Dashboard](/docs/dashboard) |
| Channels | channel list, creation, a channel's page with its messages | [Channels](/docs/channels), [Channel page](/docs/channel-detail) |
| Consumers | every consumer group and its instances, lag | [Consumers](/docs/consumers) |
| Routing | rules that copy messages between channels | [Routing](/docs/routing) |
| Webhooks | delivering a channel to an HTTP endpoint | [Webhooks](/docs/webhooks) |
| Dead Letters | the dead-letter queue (DLQ), grouped by reason | [Dead letters (DLQ)](/docs/dlq) |
| Alerts | open alerts and the event history | [Alerts and events](/docs/alerts) |
| Nodes | nodes, partitions, rebalancing, decommissioning | [Nodes and cluster](/docs/nodes) |
| Settings | token, theme, language; access tokens, notifications, backups, security | [Settings](/docs/settings) |

Counters next to items: Alerts shows open alerts (red when one is critical), Dead Letters the number of messages in the DLQ, Webhooks how many webhooks are failing right now. The bottom of the sidebar shows who you are (token name and role) and the broker version with its round-trip time (RTT).

## Top bar {#topbar}

- **Cluster name and "N/M nodes up"** — nodes that are not `Down`. The dot colour and word show overall health: Healthy, Warning, Critical. In a cluster it adds the number of under-replicated or offline partitions, and "partial data" when telemetry is incomplete.
- **Search and commands** (<kbd>⌘</kbd> <kbd>K</kbd> on a Mac, <kbd>Ctrl</kbd> <kbd>K</kbd> elsewhere) — the palette: jump to a channel, group, route or webhook, open the DLQ, create a channel, route, token or webhook, add Telegram, run a backup now. Type a condition such as `total > 500` and it offers to search messages matching it.
- **Create channel** — opens the creation wizard (admin role).
- **Live / Paused** — freezes the view so you can study it calmly. Data keeps streaming in the background; Live resumes updates.
- **Language** — switch English / Russian; remembered in this browser.

## Live data {#live}

The console keeps one WebSocket per tab: a snapshot first, then only changes. The cluster overview updates 4 times a second, channels and groups twice, nodes once, alerts and events on change. When the connection drops, a banner with a countdown appears; the console reconnects and resubscribes by itself and shows the last known data meanwhile.

## Roles and disabled buttons {#roles}

The console asks the broker who you are (`GET /api/auth/me`). Buttons your role can't use are not hidden; they are disabled with a hint naming the role you need:

- **Viewer** — read only.
- **Operator** — plus DLQ retry and discard, DLQ export, message replay, offset resets, publishing over REST.
- **Admin** — everything: channels, routes, tokens, webhooks, notifications, backups and cluster operations.

See [Access tokens and roles](/docs/access-tokens).

## Banners {#banners}

| Banner | Meaning | What to do |
|---|---|---|
| "Auth disabled — anyone can administer this broker" | no token exists and `--admin-token` is not set | create an admin token, see [Secure the broker](/docs/howto-secure) |
| "Connection lost" | the broker is stopped or unreachable | wait for the reconnect or check the broker |
| "Access token required" | the broker enforces auth (401) | paste an `fmq_…` token or the bootstrap `--admin-token` |
| "Session token revoked or expired" | your token was revoked or expired; the WebSocket closed with code 4401 | sign in with a new token |

## Keyboard shortcuts {#hotkeys}

| Keys | Action |
|---|---|
| <kbd>⌘</kbd>/<kbd>Ctrl</kbd> + <kbd>K</kbd> | command palette |
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | move through the message list |
| <kbd>/</kbd> | search the docs (on this page) |
