---
title: Add or remove a node, rebalance
group: howto
order: 8
summary: Add a node to the cluster, restart one for maintenance, decommission it for good, forget a dead node and even out replicas.
---

All operations need the admin role and work on any node (the controller executes them). Watch the state on [Nodes](/docs/nodes).

## Add a node {#add}

1. Start a new process with a **new** `--node-id`, the same `--cluster-secret` (and the same cluster TLS flags) and `--peers` listing the existing nodes:

   ```bash
   ./target/release/fluxmq --node-id n4 --peers n1@10.0.0.1:7422,n2@10.0.0.2:7422,n3@10.0.0.3:7422 \
     --cluster-secret "$FLUXMQ_CLUSTER_SECRET" --advertise-host 10.0.0.4 --data-dir /var/lib/fluxmq
   ```

2. The controller adds it to Raft as a learner first, then as a voter (up to 5 voters). On Nodes it shows as Joining, then Up.
3. New channels use the node at once. In `full` mode existing replicas move onto it about 10 s after it is Up (plus up to 30 s until the next check) — the Rebalancing panel shows a plan with reason `node n4 joined`. In `off` and `repair` modes press **Rebalance now**.

## Restart a node for maintenance {#maintenance}

1. On the node card — **Drain leadership** (or just stop it with `SIGTERM`: a graceful stop does the same itself).
2. Stop it, do the work, start it with the same `--data-dir` and flags.
3. It catches up, rejoins the ISR, turns Up after ~10 s and takes back its preferred leaderships. A short downtime (below `replace_after_secs`, 10 minutes by default) doesn't move any replicas.

## Decommission a node for good {#decommission}

1. Make sure the remaining nodes can hold the replicas: with RF 3 on three nodes, add a fourth first.
2. On the card — **Decommission**. The preview shows how many replicas of which channels move and how the load changes.
3. Confirm. The card shows progress ("moved N of M"), the Rebalancing panel the moves. Cancel finishes moves in flight and returns the node to Up.
4. When the card says the node left the cluster, stop the process. You may delete its data directory. Restarting it with the same id joins it as a new, empty node.

Over REST:

```bash
curl -s -X POST -H "$AUTH" "$H/api/cluster/nodes/n3/decommission?dry_run=true"   # the plan, changes nothing
curl -s -X POST -H "$AUTH" $H/api/cluster/nodes/n3/decommission                  # start
curl -s -H "$AUTH" $H/api/cluster/nodes/n3/decommission                          # progress
```

## A node died for good {#dead}

1. The node is Down. After `replace_after_secs` (600 s by default) in `repair`/`full` mode its replicas are recreated on live nodes; don't want to wait — **Rebalance now**.
2. Once it holds no replicas, use **Forget** on its card (`DELETE /api/cluster/nodes/{id}`). The node is removed from Raft and the registry.

> [!WARNING]
> Raft quorum: a voter can be removed only if a majority of both the remaining and the current voters is alive. Lose the majority and metadata becomes read-only and failover impossible. Never wipe `data/raft/` on a majority of nodes.

## Even out replicas by hand {#rebalance}

The Rebalancing panel on Nodes → **Rebalance now** → preview (which partitions move where and the projected load) → **Start**. Moves are safe: new replicas catch up and join the ISR before old ones are removed; limit network load with the catch-up throttle. **Pause** starts no new moves, **Cancel** never leaves a partition with fewer replicas.

To move a single partition, click its matrix cell → **Reassign** (the first replica is the preferred leader).

## Limits {#limits}

- One node is decommissioned at a time, and not during another rebalance.
- The planner counts replicas, not bytes; no rack/AZ awareness.
- Publishes to partitions of a failed leader pause for ~5–6 s while the controller elects a new one.
