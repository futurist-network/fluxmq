---
title: Backups
group: console
order: 13
summary: Consistent incremental backups to S3-compatible storage or disk — status, target and schedule, encryption, history and restore.
console: /settings/backups
---

FluxMQ takes **consistent, incremental backups without stopping the broker**: to S3-compatible storage (AWS S3, MinIO, Cloudflare R2, Backblaze B2…) or a directory on disk. Restore is a separate command, `fluxmq restore`, into an empty directory. Only admins can change settings and run backups; everyone else sees status and history.

## What a backup contains {#contents}

- **Messages** of every channel — per partition up to a consistent cut. Publishing is not blocked during a backup.
- **Channel configs**, **consumer group offsets**, the **DLQ** — taken right after the message cut (so after a restore some messages may be delivered again, but nothing is lost).
- **Metadata**: routes, access tokens (hashes only), notifiers, webhooks. The backup settings themselves (with S3 keys) are **not** included.

Backups are incremental: sealed segments are immutable and uploaded once, later backups reuse them. Each partition's active segment is uploaded again every run, so small channels gain little.

## Status {#status}

Whether backups are configured and the schedule is on; a running backup with progress (bytes, segments reused and uploaded); the last success (age, size, duration), the last failure, the next run. **Run now** starts a backup immediately (also available in the command palette).

## Target & schedule {#config}

| Field | What it sets |
|---|---|
| Target type | S3-compatible or filesystem |
| Presets | AWS S3, MinIO, Cloudflare R2 — fill in endpoint, region and addressing |
| Endpoint, region, bucket, prefix | for S3; empty endpoint = AWS; the prefix is a folder inside the bucket, one per cluster |
| Addressing | path-style (needed by MinIO and R2) or virtual-host |
| Access key id, Secret access key | credentials; the secret isn't shown again after saving |
| Path on the broker host | for filesystem targets (works well with NFS) |
| Every | 15 min, hourly, 6 h, daily (any interval from 300 s via the API) |
| Keep last / and at most | how many backups to keep and the maximum age in days (0 — no limit); the newest is never deleted |
| Include messages | off — metadata, offsets and DLQ only: fast and tiny |

**Test connection** writes, reads and deletes a probe object — nothing is saved.

## Encryption {#encryption}

With a key set, every object is encrypted on the broker (AES-256-GCM) before upload — the bucket owner sees only ciphertext. **Generate** creates a random key (32 bytes, base64) and shows it once with a copy button. You can't save until "I have stored this key" is ticked.

> [!WARNING]
> **Lose the key, lose the backup.** FluxMQ keeps the key nowhere but its own settings. Store it in a secrets manager, separately from the broker. Turning encryption off deletes the stored key, while older encrypted backups still need it.

## History {#history}

Each backup: id, trigger (scheduled / manual / CLI), status, start, duration, size and bytes uploaded, an "incremental savings" bar (share of reused segments). **Restore…** opens the details: where it was taken, which nodes uploaded partition data, contents (a channel table: partitions, messages up to the cut, size, segments, groups, DLQ), encryption, the steps and a **ready-made** `fluxmq restore` command for this backup. Deleting a backup removes its manifest; segments no other backup references are garbage-collected.

## In a cluster {#cluster}

The **controller** runs backups (a request to any node is proxied to it) and every partition's leader uploads that partition. If a partition has no leader during the backup, the run fails — start it again. A restore always produces a **single node**, from which you can grow a new cluster.

Step by step, including restore: [Back up and restore](/docs/howto-backup-restore).
