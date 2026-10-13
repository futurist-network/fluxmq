---
title: Settings
group: console
order: 10
summary: The General tab — your token in this browser, theme and language, live-data cadence, versions and the metrics URL; plus a map of the other settings tabs.
console: /settings
---

Settings has five tabs: **General**, [Access tokens](/docs/access-tokens), [Notifications](/docs/notifications), [Backups](/docs/backups) and [Security](/docs/security). This article covers General: everything here applies to **this browser**, not to the broker.

## Your access token {#token}

The token the console uses to talk to the broker: an `fmq_…` access token or the bootstrap `--admin-token`. It is stored in the browser's `localStorage` and sent as `Authorization: Bearer …`, and as `?token=` for the WebSocket. Save checks it with the server ("Verified with the server" or "The server rejected this token"). Empty means no token (fine while auth is disabled).

Create and revoke tokens on the [Access tokens](/docs/access-tokens) tab.

## Appearance and language {#appearance}

- **Theme** — light, dark or "System" (the default: follows the operating system, switching live). The sun / moon button in the top bar switches in one click.
- **Language** — English or Russian; the same switch is in the top bar. The documentation follows the interface language.

## Live data {#live}

For reference: how often the broker pushes updates over the WebSocket.

| Topic | Cadence |
|---|---|
| `cluster.overview` | 250 ms |
| `channels.summary` | 500 ms |
| `channel.{name}` and `.consumers` | 500 ms |
| `channel.{name}.tail` | 250 ms (sampled) |
| `nodes` | 1 s |
| alerts, events | on change |

Below it: the connection state, round-trip time and the number of resyncs (when updates were missed, the console asks for a fresh snapshot).

## About {#about}

Console and telemetry contract versions, the broker version, node id, uptime, server status and this node's **Prometheus** `/metrics` URL. Metrics are per node — in a cluster scrape every node (see [Metrics](/docs/ref-metrics)).
