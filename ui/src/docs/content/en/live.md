---
title: Live subscriptions (WebSocket / SSE)
group: console
order: 6.5
summary: Browsers, apps and devices read channels in real time straight from the broker's HTTP port — no SDK, no backend of your own.
console: /live
---

A **live subscription** reads a channel in real time over WebSocket or SSE, straight from the broker's HTTP port (`7421`). It suits browsers, mobile apps and devices: no SDK, no separate backend, no client-protocol connection.

- A subscription **only reads**: no consumer group is created, nothing is acknowledged, other consumers notice nothing.
- It sees exactly what a regular consumer would: committed, durably written messages only.
- After a disconnect it resumes **by cursor**, with no loss and no duplicates.
- One shared read per channel: a thousand subscribers of one channel cost the broker one disk read.

Nothing needs to be turned on — subscriptions work out of the box. Only the limits are configurable (`--live-*` flags).

## How to connect {#connect}

1. **Token.** For a browser, create a token with the **Subscriber** role (*Settings → Access tokens*) and list its channels in "Live subscriptions to". Such a token can do nothing but subscribe to those channels. Issue short-lived tokens from your backend, one per user. Viewer and higher roles may subscribe to any channel.
2. **Code.** Ready-made code with the channel filled in is on a channel's **Live** tab and on the **Live subscriptions** page.
3. **Another site.** Pages served from the broker's own address work right away. Other sites are refused by default (`Origin` check); allow one with `--live-allowed-origins https://app.example.com`.

```js
// SSE: the simplest; the browser reconnects and resumes by itself
const es = new EventSource('/api/channels/orders/events?from=latest&key=user_42&access_token=' + token);
es.addEventListener('messages', (e) => {
  for (const m of JSON.parse(e.data).messages) console.log(m.offset, m.payload);
});

// WebSocket: several subscriptions per connection; the token is the second subprotocol
const ws = new WebSocket(`wss://${location.host}/api/channels/orders/live?from=earliest`,
                         ['fluxmq.live.v1', 'fluxmq.token.' + token]);
ws.onmessage = (e) => {
  const ev = JSON.parse(e.data);           // hello | messages | gap | reset | progress | error
  if (ev.cursor) localStorage.cursor = ev.cursor;   // to resume: ?cursor=...
};
```

Servers and scripts can simply send `Authorization: Bearer …` — it works on both endpoints.

## Parameters {#params}

| Parameter | What it does |
|---|---|
| `from` | where to start: `latest` (default, new messages only), `earliest`, `time:<unix ms>`, `offsets:0=10,1=20` |
| `cursor` | resume from the position in the last event — no loss, no duplicates; wins over `from` |
| `key` | only messages with these keys: `key=user_42` or `key=a,b` |
| `header.<name>` | header equality: `header.type=order.created` |
| `filter` | a [filter language](/docs/ref-filter) expression: `amount >= 100 AND country = "DE"` |
| `partitions` | only these partitions: `0,2` |
| `access_token` | SSE only: the token in the URL (`EventSource` cannot set headers) |

Filtering happens on the server; filtered-out messages still advance the cursor (`progress` event).

## Events {#events}

| Event | When |
|---|---|
| `hello` | the subscription is open; carries the start cursor and the limits |
| `messages` | a batch of messages: partition, offset, key, headers, time, payload |
| `gap` | some messages were removed by retention before they could be sent (`lost` = how many) |
| `reset` | the position was past the end of the log (node restored from a backup) — reading continues from the end |
| `progress` | the filter skipped messages, the cursor moved |
| `error` | a bad command, or the subscription was refused |
| `end` | SSE only: the server closes the stream (`slow`, `revoked`, `deleted`, `limit`, `shutdown`) |

Payloads above `--live-max-payload-bytes` (64 KiB) are cut and marked `truncated: true` — fetch the full message over REST.

## The channel tab {#live-tab}

A channel's **Live** tab shows:

- how many subscriptions the channel has right now (over all nodes) and the per-channel limit;
- **Try it** — a real subscription from the browser with the console's token: messages appear as the channel commits them. Read-only — consumers and their offsets are not touched;
- **Code** — ready-to-paste SSE, WebSocket and curl code for the chosen options (start from, keys, filter); the token in it is always the `TOKEN` placeholder.

## The Live subscriptions page {#page}

| Block | What it shows |
|---|---|
| Connections | open WebSocket and SSE connections |
| Subscriptions | following new messages, and catching up (still reading history) |
| Channels with subscribers | which channels are subscribed now and how much of the limit is used |
| Refused | how many connections were turned away and why: limits, foreign site, no permission, no token |
| Slow clients / gaps | clients closed for not keeping up, and subscribers that lost messages to retention |
| Limits | the `--live-*` values on every node |

Counters run from node start. The same data is in the `fluxmq_live_*` metrics (`/metrics`) and over REST: `GET /api/live/status`.

## Limits {#limits}

| Flag | Default | What it limits |
|---|---|---|
| `--live-max-connections` | 10000 | connections per node; over → 503 |
| `--live-max-connections-per-token` | 1000 | connections per token; over → 429 |
| `--live-max-connections-per-channel` | 10000 | subscriptions per channel on a node; over → 429 |
| `--live-max-subscriptions` | 16 | subscriptions per WebSocket |
| `--live-max-msgs-per-sec` | 10000 | messages per second per connection (throttles, never drops) |
| `--live-max-bytes-per-sec` | 16 MiB | bytes per second per connection |
| `--live-send-timeout` | 10 s | how long a slow client may stall before it is closed |
| `--live-allowed-origins` | — | which other sites may connect (`*` = any) |

The full list is in the [CLI reference](/docs/ref-cli).

> [!NOTE]
> The broker never loses messages because of a slow client: rate limits slow the sending down, and a client that stops reading entirely is closed. Reconnecting with its cursor, it gets everything it missed — as long as the messages are retained.
