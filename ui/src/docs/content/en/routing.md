---
title: Routing
group: console
order: 5
summary: Rules that copy a source channel's matching messages into target channels — the route list, the WHEN/THEN editor with a live preview, and counters.
console: /routing
---

A **route** is a rule: "messages of a source channel that match the conditions are copied into target channels". For example: orders from Moldova worth 100 or more go to `orders.md` and `audit`. The router is built into the broker.

## How it works {#how}

- The router reads the source as an ordinary consumer group with the system name **`$router`**. It never slows publishing, and its lag and health show on [Consumers](/docs/consumers).
- Every message is checked against all enabled routes of its source. Each target gets a copy **once**, even if several routes matched. Key, headers and payload are kept; the headers `flux-routed-from` (`channel/partition/offset`), `flux-route-id` and `flux-route-path` (the channels the message passed through) are added.
- **Loop protection**: no copy is sent to a channel already in `flux-route-path`; a chain is at most 8 hops. Routes A→B and B→A are safe.
- **Errors**: if a target is unavailable (deleted, say), the message is retried under the source's retry policy, then dead-lettered in the source's DLQ with group `$router` and reason `route target unavailable: <channel>`.
- A route processes only messages published **after** it was created. Move older messages with [replay](/docs/channel-detail#replay).
- A **disabled** route is paused: the `$router` group keeps its position and resumes where it stopped. When a source's last route is deleted, its `$router` group is removed automatically.

## The route list {#list}

| Column | Meaning |
|---|---|
| Route | name and the WHEN condition (or "every message") |
| Source → targets | where messages are copied from and to |
| Matched | messages that matched, share of evaluated, rate per second |
| Forwarded | copies sent (**per target**: a match with three targets adds 1 to Matched and 3 to Forwarded) |
| Failed | failed forwards (retried, then dead-lettered with group `$router`) |
| Last match | when the route last fired |
| Enabled | the pause switch |

Counters are since node start, per node; refreshed every 5 seconds.

## The route editor {#route-editor}

**New route** (or **Create route from filter** on a channel's Messages tab, or **New route from …** on its Routing tab) opens the editor. Requires the admin role.

- The route **name**.
- **FROM** — the source channel. It can't be changed later: to read another channel, create a new route.
- **WHEN** — conditions as "field · operator · value" rows. Fields are suggested from the source's last 100 messages: `key`, `partition`, `header.<name>`, JSON paths like `shipping.country`; the operators offered depend on the field type. Switch to **Text** to write the [filter language](/docs/ref-filter) directly; the rows also show the condition as text. No conditions means every message is routed.
- **THEN** — one or more target channels (the source can't be picked).
- **Enabled / Paused**.

On the right (below the editor on narrow screens) a live **Preview**: how many stored messages would have matched in 1 h / 24 h / 7 d ("Matched … of … scanned"), sample matches (click to open them in the inspector), and a note when the scan limit (200,000 messages) was hit. The preview forwards nothing.

Under a saved route's editor: its counters — evaluated, matched, forwarded (per target), failed and last match.

## Example {#example}

With demo data (`fluxmq-demo`): create a channel `orders.md`, then a route from `orders.events` with `type = "OrderCreated" AND country = "MD"` and target `orders.md`. The preview immediately shows hundreds of matches in 24 h, the counters grow within seconds, and messages in `orders.md` carry `flux-route-id`, `flux-route-path: orders.events,orders.md` and `flux-routed-from`.

> [!NOTE]
> Conditions have no `OR`: for "either" create several routes into the same target — the copy is still sent once.
