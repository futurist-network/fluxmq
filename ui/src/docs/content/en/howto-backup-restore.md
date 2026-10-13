---
title: Back up and restore
group: howto
order: 9
summary: Set up backups to S3 or disk, verify them, and restore a broker with fluxmq restore.
---

## 1. Configure the target {#configure}

**In the console**: Settings → Backups → Target & schedule: S3-compatible (AWS S3 / MinIO / Cloudflare R2 presets) or a path on disk → **Test connection** → interval and retention → optionally encryption (**Generate**, store the key in a secrets manager, tick "I have stored this key") → **Save settings**. Field details: [Backups](/docs/backups).

**Over REST** (admin role):

```bash
# A directory on disk
curl -X PUT http://localhost:7421/api/backups/config \
  -H 'Content-Type: application/json' -H "Authorization: Bearer $ADMIN" \
  -d '{"enabled": true,
       "target": {"type": "filesystem", "path": "/var/backups/fluxmq"},
       "interval_secs": 3600, "keep_last": 24, "keep_days": 7,
       "include_messages": true, "encryption_key": null}'

# Test the target, saving nothing
curl -X POST http://localhost:7421/api/backups/test-target \
  -H 'Content-Type: application/json' -H "Authorization: Bearer $ADMIN" \
  -d '{"type": "filesystem", "path": "/var/backups/fluxmq"}'
```

> [!WARNING]
> Never point two brokers (or a broker and `fluxmq backup run`) at the same storage prefix: one's garbage collection can delete the other's objects.

## 2. Run and verify {#run}

**Run now** in the console (or `POST /api/backups/run`). The first backup uploads everything, later ones only new segments. The first scheduled run happens a minute after the broker starts.

```bash
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:7421/api/backups/status   # progress
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:7421/api/backups          # history
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:7421/api/backups/latest   # contents of the newest
```

## 3. Restore {#restore}

Restore runs with the broker **stopped** and always into an **empty** (or not yet existing) directory. **Restore…** in the history shows the ready-made command for a specific backup.

1. See which backups exist:

   ```bash
   fluxmq backup list --fs-path /var/backups/fluxmq
   # S3 (keys come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY):
   fluxmq backup list --s3-bucket fluxmq-bk --s3-endpoint http://minio.local:9000 --s3-prefix prod --s3-path-style
   ```

2. Restore the one you need (`latest` is the newest):

   ```bash
   fluxmq restore --fs-path /var/backups/fluxmq --backup latest --data-dir /var/lib/fluxmq-restored
   ```

   For an encrypted backup add `--encryption-key <base64>` or `FLUXMQ_BACKUP_ENCRYPTION_KEY`; with the wrong key restore stops at once and prints both key fingerprints. Every segment is CRC-checked; on an error delete the directory and retry.

3. Start the broker on the restored directory:

   ```bash
   fluxmq --data-dir /var/lib/fluxmq-restored
   ```

Consumer groups continue from their saved offsets (the tail may be redelivered — at-least-once); the DLQ, routes, tokens, notifiers, webhooks and channel settings are back. The backup settings themselves are not restored — set them again.

| Target | Flags |
|---|---|
| Directory | `--fs-path /mnt/backup` |
| AWS S3 | `--s3-bucket my-fluxmq-backups --s3-region eu-central-1 --s3-prefix prod` |
| MinIO | `--s3-bucket fluxmq --s3-endpoint http://minio.local:9000 --s3-path-style` |
| Cloudflare R2 | `--s3-bucket fluxmq --s3-endpoint https://<account_id>.r2.cloudflarestorage.com --s3-region auto --s3-path-style` |

## Cluster {#cluster}

In a cluster the controller coordinates the backup and each partition's leader uploads it. **A restore always produces a single node** — start it, then add nodes as needed, see [Add or remove a node](/docs/howto-nodes#add).

## Backing up a stopped broker {#offline}

Before a move, without the console:

```bash
fluxmq backup run --data-dir /var/lib/fluxmq --fs-path /mnt/backup   # the broker must be stopped!
```

It also takes `--encryption-key` and `--no-messages`. No rotation happens. For a stopped broker the simplest option is copying the whole data directory.
