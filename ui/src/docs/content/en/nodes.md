---
title: Nodes and cluster
group: console
order: 9
summary: Nodes, partition leaders, replication, draining leadership, reassigning replicas, rebalancing, decommissioning and forgetting nodes.
console: /nodes
---

On a single broker the Nodes screen shows one card (CPU, memory, disk, fsync p99, connections) and a "Single-node mode" note. In a cluster it has everything you need to operate it: membership, replication and node operations. It refreshes every 2 seconds.

## How the cluster works {#model}

- **Metadata** (channels, partition assignments, tokens, routes, the node list) lives in Raft on every node. The Raft leader is the **controller**: it places partitions on nodes and fails them over.
- **Data** is replicated per partition, Kafka-style: a partition has a leader and replicas that fetch from it. A publish is acknowledged once every in-sync replica (**ISR**) has it. Consumers only see replicated data.
- Only an ISR member can become the new leader. With no live ISR member a partition goes **offline** instead of losing data.
- You can talk to any node: channel creation goes through the controller, publishes are forwarded to leaders, reads and group operations are gathered from leaders.

## Summary {#summary}

| Stat | Meaning | Healthy |
|---|---|---|
| Nodes up | nodes not `Down` out of all | all |
| Controller | which node leads Raft, the Raft term | present |
| Replication factor | replicas of new channels (`--replication-factor`) | usually 3 |
| Partitions | total partitions | — |
| Under-replicated | partitions whose ISR is smaller than their replica set | 0 (except while a node restarts) |
| Offline | partitions without a live leader | **always 0**, otherwise they can't take publishes |
| Raft log | whether the metadata log is committed, the epoch | committed |

## Node cards {#node-cards}

Status: **Up**, **Down**, **Joining** (catching up with leaders before joining the ISR), **Leaving** (after a drain), **Decommissioning**. Only non-Up is coloured. Badges: controller, voter or learner.

A card shows the client and HTTP addresses, leaderships and replicas, CPU, memory, disk (amber > 80 %, red > 90 %), fsync p99 (warning > 50 ms), connections, uptime and version. Actions (admin role):

- **Drain leadership** — moves the node's partition leaderships to other ISR members and marks it `Leaving`. It keeps replicating, so you can restart it with no partition unavailable; leaderships come back after the restart. A graceful stop (`SIGTERM`) drains automatically.
- **Decommission** — see [below](#decommission).
- **Forget** — for a dead node, see [below](#forget).

## Partition leaders matrix {#matrix}

Rows are channels, cells are partitions, the cell colour is the leader node. An **amber border** means under-replicated, **hatching** means offline, a separate mark means a replica is moving. "Only problems" filters to problem partitions. A cell's tooltip shows the leader, epoch, ISR and replica lag.

Click a cell to **Reassign**: pick the partition's replica set; the first replica is the preferred leader. New replicas catch up and join the ISR before old ones are removed. To move whole partitions, rebalancing (below) is easier than reassigning by hand.

## Rebalancing {#rebalancing}

The controller looks after replica placement:

- **Repair** — when a node has been down for longer than `replace_after_secs` (default 600 s), its replicas are recreated on live nodes. A replica is removed only when there is somewhere to move it — the copy count never drops.
- **Balance** — when `max − min` replicas per live node exceeds the threshold (default 1), the fewest possible replicas move from overloaded to free nodes, then preferred leaders are evened out. A new node gets its share about 10 s after it is Up, plus up to 30 s until the next check.

How a partition moves: new replicas are added and catch up (outside the ISR) → once all are in the ISR the new list is committed and removed replicas delete their copy → leadership moves to the preferred replica. Acknowledged messages are never lost; if new replicas don't reach the ISR within `move_timeout_secs`, the move rolls back. The plan is stored in metadata and resumes after a controller failover.

The **Rebalancing** panel on the Nodes screen:

- load per node (replicas, leaders, bytes) — now and projected after the plan, an imbalance badge, under-replicated partitions;
- the current or last plan — a table of moves: partition, replicas "from → to", reason, state (`pending` — waiting for a slot, `catching_up` — catching up with lag and progress, `finalizing`, `done`, `rolled_back` — old replicas restored, `cancelled`), time;
- **Rebalance now** — a plan preview first (changes nothing), then start; works even in `off` mode;
- **Pause / Resume / Cancel** — pause starts no new moves, running ones finish; cancel never leaves a partition with fewer replicas;
- **Settings** (stored in cluster metadata):

| Setting | Default | Meaning |
|---|---|---|
| Mode | `full` | `off` — manual only, `repair` — repair only, `full` — repair and balance |
| Max concurrent moves | 2 | for the whole cluster |
| Imbalance threshold | 1 | allowed `max − min` replicas per live node |
| Replace replicas after | 600 s | how long a node must be Down before its replicas are recreated; 0 — never |
| Catch-up throttle | 0 (unlimited) | bytes/s per catching-up replica; in-sync followers are never throttled |
| Move timeout | 1800 s | after this a move rolls back |

`--auto-rebalance` seeds the mode only when a cluster starts for the first time; afterwards these settings are the source of truth.

## Decommission a node {#decommission}

**Decommission** removes a live node for good in one action:

1. **Checks** (the preview shows the plan and how many replicas move). `409` if another rebalance or decommission is running, the remaining nodes can't hold the replicas (e.g. RF 3 on three nodes — add a node first), a partition has no live leader, or removal would break the Raft quorum.
2. **Moving replicas** — the node becomes Decommissioning, its leaderships move away, and a rebalance plan of repair moves runs (visible in the Rebalancing panel). The card shows "moved N of M". **Cancel** finishes moves in flight; the decommission ends as cancelled and the node is Up again.
3. **Leaving Raft membership** — once no replicas remain.
4. **Removed** — the node disappears from the list and the card says it is safe to stop the process. Restarting it with the same id joins it as a **new, empty node**.

One node at a time. Decommissioning the controller works too: it moves its replicas first, then removes itself from Raft, and the rest elect a new controller in 1–2 s.

## Forget a dead node {#forget}

**Forget** is for a machine that is gone for good, after its replicas were already restored elsewhere (automatically after `replace_after_secs` or by a manual rebalance). Only for a `Down` node holding no replicas, otherwise `409`. If the process unexpectedly comes back with that id, it is rejected; a restart joins it as a new node.

> [!WARNING]
> A voter can be removed only if at least one voter remains and a majority of both the remaining and the current voters is alive. With three voters and one down, you can't remove a second.

## What happens when a node fails {#failure}

| Time | What happens |
|---|---|
| 0 s | the node dies (`kill -9`, power, network) |
| up to 4 s | its lease expires — it stops leading partitions itself, even if alive but cut off |
| ~6 s | the controller marks it Down, elects new leaders from the ISR, removes it from ISRs; the SDK gets `NotLeader`/`MetadataChanged` and retries |
| the node returns | truncates a divergent tail, catches up, rejoins the ISR, turns Up ~10 s after start and takes back its preferred leaderships |

Publishes to partitions the failed node led pause for ~5–6 s (up to ~9 s if it was the controller). Adding and removing nodes step by step: [Add or remove a node](/docs/howto-nodes).
