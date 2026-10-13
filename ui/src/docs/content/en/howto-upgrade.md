---
title: Upgrade
group: howto
order: 10
summary: Rolling upgrades of a cluster without downtime, what changes from 0.6 to 0.7, and why you can't downgrade afterwards.
---

## The general procedure {#rolling}

Upgrade nodes **one at a time**:

1. (Optional) **Drain leadership** on [Nodes](/docs/nodes) — or simply stop it with `SIGTERM`: a graceful stop hands over leadership and leaves the ISRs by itself.
2. Stop the process, replace the binary (or image), start it with **the same** `--data-dir` and flags.
3. Wait until the node is Up and **under-replicated partitions are 0** (the Nodes summary or `GET /api/cluster/status` → `under_replicated_partitions = 0`).
4. Move to the next node. Do the controller last — stopping it costs one Raft election (1–3 s).

A single broker: stop it, replace the binary, start it on the same data directory. A [backup](/docs/howto-backup-restore) before upgrading is a good idea.

If you don't want replicas moving during the upgrade, set rebalancing to `off` (Rebalancing panel → settings) and switch back to `full` afterwards.

## 0.6 → 0.7: record batches {#v07}

In 0.7 the broker stores messages as **record batches**: a producer's batch is written to the log as is, can be compressed (lz4/zstd), replicated and served whole. A 0.6 node can't read such batches, so the transition works like this:

- **A 0.6 data directory opens unchanged**: old records stay readable, new ones are written in the new format; one segment may hold both.
- **In a cluster** a partition leader starts writing batches only once **every** replica of the partition has announced support for the new format (i.e. runs 0.7). While any 0.6 node remains, publishes are stored in the old format — same offsets and acknowledgements.
- A leader never sends a batch to a replica that can't read it: such a replica gets only the old prefix and the broker logs a loud error — no data corruption.
- **While some leaders still run 0.6**, their partitions can show two 0.6 ISR bugs (fixed in 0.7): the ISR can flap, and a returned replica can stay out of the ISR in the metadata although it has caught up (the partition stays under-replicated). No data is lost — acks still need `--min-isr` replicas. If a partition led by a 0.6 node stays under-replicated for more than ~30 s while its ISR ≥ min ISR, carry on: upgrading (restarting) that leader clears it.
- Verified by a rolling upgrade of three processes under load with `kill -9` of leaders: 0 acknowledged messages lost.

> [!WARNING]
> **Downgrading to 0.6 after batches were written is not supported.** Once the last node runs 0.7 (or a single broker accepted its first publish on 0.7), the logs contain batches 0.6 can't read. If you might need to roll back, take a backup before upgrading the last node.

**New flags in 0.7** (the defaults suit most setups): `--partition-cache-bytes` (16 MiB of recent batches per partition in memory, so caught-up consumers read without touching disk) and `--delivery-threads` (2 threads of a separate delivery runtime, so consumers aren't starved when producers saturate the broker). See [Flags and environment](/docs/ref-cli).

## Clients {#clients}

- **SDK 0.7** works with brokers 0.4–0.7: it offers protocol v4 and switches straight to what an older broker supports. So 0.7 clients can be upgraded in any order relative to the brokers; batches, compression and cumulative acks pay off once the broker runs 0.7.
- **Older SDKs** (protocols v2/v3) keep working with a 0.7 broker unchanged.
- Historically (0.4 → 0.5) SDK 0.5 couldn't step its protocol down, hence the old rule "all brokers first, then clients". SDK 0.7 lifts that restriction.

## Earlier upgrades {#older}

- **0.3 → 0.4**: a 0.4+ node deletes local copies of partitions it no longer replicates; the first 0.4 controller writes the rebalancing settings from `--auto-rebalance` (default `full`). Start nodes with `--auto-rebalance off` if replicas shouldn't start moving right away.
- **0.4 → 0.5, 0.5 → 0.6**: no data format change, upgrade node by node. In 0.6 the default segment size became 64 MiB (was 128 MiB) — old segments stay readable.

## After the upgrade {#verify}

- [Nodes](/docs/nodes): all Up, 0 under-replicated and 0 offline partitions, every node on the new version.
- [Dashboard](/docs/dashboard): latency and lag back to their usual values.
- `curl -s http://<node>:7421/api/health` — the new `version`.
