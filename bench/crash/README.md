# Crash durability: acknowledged-but-lost messages after kill -9 and power loss

This tool checks whether every message that a broker acknowledged can still be read after a
crash. It runs in Docker on one machine and covers FluxMQ (group commit, `fsync=os`) and
Apache Kafka (default flush, flush per message), on one node and on three nodes.

```sh
bench/crash/build.sh                                   # fluxmq-crash, kafka-crash, fluxmq-crash-client
bench/crash/crash.py --systems fluxmq-single --iterations 3
bench/crash/crash.py --systems all --iterations 5      # 8 systems x 2 modes (x 2 targets on clusters)
bench/crash/crash.py --systems fluxmq-cluster,kafka-cluster --modes powercut --targets all
```

Results go to `bench/results/crash-<timestamp>/`:

* `summary.json`: one row per run.
* `env.json`: the method text, the images and the git sha.
* `runs/<run>/` holds the evidence for each run:
  * `run.json`
  * `acked.jsonl`: every acked id, as `[producer, seq, partition, offset, ack_ms]`.
  * `write.json`
  * `verify1.json` (and `verify2.json`)
  * `writer.log`
  * the broker logs from 5 s before the crash.

## What one run does

1. **Set up.** The writer (`mq-crash write`, which runs in a client container in the same compose
   network) creates a fresh topic or channel. By default that is 4 partitions, with RF=3 and
   min ISR 2 on clusters.
2. **Prepare.** `crashctl prepare` runs on every node. It writes the *synced canary* file and
   calls `sync -f` on the data filesystem. The run therefore starts from a durable state, like a
   broker that has been running for a while. No sync of any kind happens after this point.
3. **Publish.** The writer publishes at a steady open-loop rate: 5000 msg/s total, 2 producers,
   1 KiB messages. It uses broker acks:
   * FluxMQ: `BatchingProducer`.
   * Kafka: `acks=all` with `enable.idempotence=true`.

   Every acknowledged message id is recorded together with its partition, offset and ack time.
   Failed or timed-out publishes (30 s) are counted but never recorded as acked.
4. **Crash.** After a random 8–12 s of publishing, the run crashes the victim(s):
   * On one node, the victim is that node.
   * On clusters, `--targets leader` crashes the node that leads partition 0, and
     `--targets all` crashes every node at once.

   Just before the crash, crashctl writes the *unsynced canary*: a new file that never gets
   fsynced.
5. **Restart.** The victims stay down for `--down` s (default 3) and then restart. The writer keeps
   publishing for `--after` s (default 5) after recovery, then stops and waits up to 45 s for the
   last acks.
6. **Verify.** `mq-crash verify` re-reads the whole topic from the earliest offset with a fresh
   consumer:
   * Kafka: explicit assignment, read up to the high watermarks taken at the start.
   * FluxMQ: a new consumer group, read until all acked ids are found or nothing has arrived for
     15 s.

   It reports **lost** = acked ids that are not in the log, split into acked before and after the
   end of the crash. If anything is lost, verification runs again 10 s later; the second pass is
   the result. Real loss persists across both passes.
7. **Clean up.** The topic is deleted so disk stays bounded. The stack is reused for the next run.
   After the last run the stack is removed with `down -v` once `crashctl teardown` has released
   the devices.

A FluxMQ group-commit run that loses anything stops the whole matrix (`--stop-on-loss`). The
stack is left running for inspection.

## Crash modes: what each one simulates

### `kill9`: process crash

`crashctl kill9` sends SIGKILL to the broker's whole process group inside its container. The
kernel and its page cache survive. Everything the broker had passed to `write(2)` is still there
after the restart, whether it was fsynced or not.

This simulates a crashing broker process: an OOM kill, a panic or `kill -9`. It does not
simulate a machine crash. Any system that acks after `write(2)` should lose nothing here. The
check: the unsynced canary must survive.

### `powercut`: power loss of the broker's machine

Each broker's data directory is an ext4 filesystem on a device-mapper device. That device is
`linear` over a loop device, and the loop device is backed by a sparse file in a Docker volume.
All of this lives inside the broker's own privileged container (`crashctl boot`). The broker
image, its settings, the CPU and memory limits and the healthcheck are unchanged. The cut runs
in this order:

1. **Cut.** The dm device is suspended with `--noflush --nolockfs`, which freezes in-flight I/O
   without flushing it, and the dm `error` target is loaded. From this point nothing reaches the
   "disk".
2. **Kill.** The broker gets SIGKILL before the device resumes. A fatal signal is handled before
   any system call returns to user space, so the broker never sees the I/O errors and cannot act
   on them. In particular it cannot ack after a failed fsync.
3. **Drop the page cache.** The device resumes with the error target and the filesystem is
   unmounted. Writing back the dirty pages (data written but not yet written back or fsynced)
   fails against the error target. The last close of the device drops its cache. A lazy unmount
   is refused, because the old superblock and its cache would survive and be reused by the
   remount; in that case the crash fails and the run is invalid. The canary caught exactly this
   bug during development, when a `docker exec` working directory kept the filesystem busy.
4. **Power back on.** The `linear` mapping is restored and ext4 is mounted again. This replays the
   journal; if mounting fails, e2fsck runs. The broker now sees exactly the bytes that had reached
   the device at the cut. The device must come back read-write, otherwise the run fails.
5. **Restart.** The broker starts and runs its own recovery.

A real power loss also loses data that the application never fsynced. That is precisely what this
method removes, and only that. The checks: the unsynced canary must be gone and the synced canary
must be there. Every run records both (`canary`, `method_valid`), and the summary flags a failure.

For multi-node power cuts (`--targets all`), every victim first gets SIGSTOP in parallel, then
every victim is cut in parallel. Nothing acks between those two phases, so the cuts are close to
simultaneous: within the time `docker exec` takes, about 100–200 ms.

### Limits of the method (stated with every result)

* **The device below the cut is trusted.** Data that reached the loop device's backing file before
  the cut counts as durable, because the Docker VM itself does not crash. A real disk with a
  volatile write cache that ignores flushes would be worse.
* **Kernel writeback makes the cut slightly favourable to page-cache systems.** Data the kernel had
  already written back before the cut survives. That writeback happens after 30 s of dirtiness
  (`dirty_expire_centisecs=3000`) or under cgroup and memory pressure (2 GiB containers). A real
  power cut would also lose whatever a disk cache had not yet persisted. The method therefore
  favours page-cache systems (Kafka defaults, FluxMQ `fsync=os`), never fsync-before-ack systems.
* **What is not covered.** The dm `error` target fails I/O outright; it does not emulate torn
  sectors inside one write. Torn writes are covered separately:
  * the storage engine's torn-tail recovery tests;
  * `crates/fluxmq-proto/tests/batch_props.rs`: every strict prefix of a batch is rejected, and
    random byte flips never panic.
* **Why not a VM-level power cut?** Killing the Docker Desktop VM is not used. It would take down
  every container of every project on the machine, it cannot be automated safely, and whether
  Docker Desktop honours guest flushes on the host (`F_FULLFSYNC`) is undocumented. The dm switch
  is local to one broker, repeatable, and verified by the canary on every crash.
* **Why not other techniques?**
  * `echo 3 > drop_caches` only drops clean pages. It cannot simulate losing dirty data.
  * `docker pause` freezes processes but keeps the page cache.
  * dm-flakey is not built into the Docker Desktop kernel: `dmsetup targets` lists only crypt,
    striped, linear and error.
  * LazyFS (FUSE) would also work, but it needs a C++ build in every broker image.
* **Environment.** One laptop, one SSD, three "nodes" on one VM.
* **Recovery time is approximate.** It is the time until the readiness probe passes. For Kafka,
  every probe call starts a JVM (`kafka-broker-api-versions.sh`), which adds a few seconds.

## Systems

| Name | Durability setting |
|---|---|
| `fluxmq-single`, `fluxmq-cluster` | group commit: ack after fsync, on every ISR member in a cluster (RF=3, min ISR 2) |
| `fluxmq-single-os`, `fluxmq-cluster-os` | `FLUXMQ_FSYNC=os`: ack after `write(2)` |
| `kafka-single`, `kafka-cluster` | Kafka defaults (`log.flush.interval.messages` = Long.MAX_VALUE: page cache), `acks=all`, RF=3, `min.insync.replicas=2` |
| `kafka-single-fsync`, `kafka-cluster-fsync` | `log.flush.interval.messages=1`: fsync of the partition log on every append before the answer |

Images: the broker images are the normal `fluxmq:dev` and `apache/kafka:3.7.0`, plus dmsetup,
e2fsprogs, util-linux and crashctl (`Dockerfile.broker`). The client is `Dockerfile.client`:
`mq-crash` with the FluxMQ SDK and librdkafka 2.12. The compose overlay is
`docker-compose.crash.yml`, used as project `fluxmq-crash`. It has no host ports, so it never
collides with the bench project.

Devices: loop and dm devices are global in the Docker VM kernel. They are released by
`crashctl teardown`, which runs before every `down`, and by a `gc` step. The gc step removes
leftover `fxcrash-*` dm devices and loop devices over `/crashdisk/disk.img`, but only while no
`fluxmq-crash` container exists.

## Options

`--rate 5000 --size 1024 --producers 2 --partitions 4 --crash-min 8 --crash-max 12 --down 3
--after 5 --iterations 3 --modes kill9,powercut --targets leader,all --disk-size 3G
--min-free-gb 10 --keep --no-stop-on-loss`.

`mq-crash` can also be used on its own: `mq-crash write --help` and `mq-crash verify --help`.
`--no-default-features` builds a FluxMQ-only binary natively (no cmake).
