# Running a FluxMQ cluster

This guide covers running FluxMQ as a cluster (roadmap stage 9). If you don't pass `--peers`,
the broker is a single node and behaves exactly like v0.1. Design: `docs/design/v0.2.md` §4.
Code: `crates/fluxmq-cluster` (metadata Raft, controller, reconciler) and
`crates/fluxmq-replication` (data replication).

## Model in one paragraph

Every node runs an openraft 0.9 member for **metadata**: channel configs, partition
assignments, the node registry, RBAC tokens and routes. The Raft leader is the
**controller**. It places partitions (replication factor = `min(--replication-factor,
live nodes)`, with leaders spread evenly), watches node liveness and fails partitions
over. **Data** is replicated per partition, Kafka-style: followers pull from the leader. The
leader tracks the in-sync replica set (ISR), and a publish is acknowledged once the whole ISR
has it (`acks=all`). Leaders are only ever elected from the ISR. With no live ISR member the
partition goes offline instead of losing acknowledged messages. Each leader change bumps the
partition `epoch`, which fences the previous leader.

## Flags

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--node-id` | `FLUXMQ_NODE_ID` | — (required with `--peers`) | Stable node id. It also overrides the id persisted in the data dir. |
| `--peers` | `FLUXMQ_PEERS` | — | Initial voters, as `id@raft_host:port,…`. Use the same list on every initial node. It may include the node itself. |
| `--raft-addr` | `FLUXMQ_RAFT_ADDR` | `0.0.0.0:7422` | Raft and cluster RPC listener (HTTP+JSON, internal only). |
| `--replication-addr` | `FLUXMQ_REPLICATION_ADDR` | `0.0.0.0:7423` | Partition replication listener (internal only). |
| `--advertise-host` | `FLUXMQ_ADVERTISE_HOST` | listen IP, or `127.0.0.1` when listening on `0.0.0.0` | Host other nodes and clients use to reach this node. **Set it on multi-host deployments.** |
| `--replication-factor` | `FLUXMQ_REPLICATION_FACTOR` | `3` | RF of new channels, capped at the number of live nodes at creation time. |
| `--min-isr` | `FLUXMQ_MIN_ISR` | `1` | A publish fails with `Overloaded` (not enough replicas) when the ISR is smaller. It is capped at the partition's RF. |
| `--cluster-secret` | `FLUXMQ_CLUSTER_SECRET` | — (required with `--peers`) | Shared secret for Raft RPC, replication and `/internal/*`. It is sent in the `x-fluxmq-cluster-secret` header and compared in constant time. |
| `--cluster-tls-cert` / `--cluster-tls-key` / `--cluster-tls-ca` | `FLUXMQ_CLUSTER_TLS_CERT` / `_KEY` / `_CA` | — (plaintext) | v0.4: mutual TLS for Raft RPC, replication and internal HTTP. All three or none; every node must agree. See *TLS between nodes*. |
| `--auto-rebalance` | `FLUXMQ_AUTO_REBALANCE` | `full` | Rebalancing mode (`off`, `repair`, `full`) written to `cluster/rebalance/config` when the cluster has none yet. Afterwards the API is the source of truth. See *Automatic rebalancing*. |

`--client-addr`, `--http-addr`, `--data-dir` and the other flags work as on a single node.
The advertised addresses `advertise_host:port` are what `GET /api/cluster/metadata` and the
wire `Metadata` response publish. SDKs connect to `client_addr`, and peers pull telemetry
from `http_addr`.

Use the same `--cluster-secret` on every node. Without cluster TLS, Raft, replication and
`/internal/*` traffic is authenticated but **not encrypted**, so keep those ports on a private
network — or enable cluster TLS (v0.4, *TLS between nodes* below).

## Bootstrap a 3-node cluster locally

From the repository root, after `cargo build --release -p fluxmq-server`, open one terminal
per node:

```sh
export FLUXMQ_CLUSTER_SECRET=dev-secret
PEERS=n1@127.0.0.1:17422,n2@127.0.0.1:17432,n3@127.0.0.1:17442

./target/release/fluxmq --node-id n1 --peers $PEERS --data-dir ./data/n1 \
  --client-addr 127.0.0.1:17420 --http-addr 127.0.0.1:17421 \
  --raft-addr 127.0.0.1:17422 --replication-addr 127.0.0.1:17423

./target/release/fluxmq --node-id n2 --peers $PEERS --data-dir ./data/n2 \
  --client-addr 127.0.0.1:17430 --http-addr 127.0.0.1:17431 \
  --raft-addr 127.0.0.1:17432 --replication-addr 127.0.0.1:17433

./target/release/fluxmq --node-id n3 --peers $PEERS --data-dir ./data/n3 \
  --client-addr 127.0.0.1:17440 --http-addr 127.0.0.1:17441 \
  --raft-addr 127.0.0.1:17442 --replication-addr 127.0.0.1:17443
```

(These ports are offset from the defaults 7420–7423, so the cluster doesn't clash with a dev
UI or a single node.)

Bootstrapping happens once. It is done by the lowest node id in `--peers` (`n1`), and only
when that node has no Raft state and no other peer reports an initialized cluster. Start the
nodes in any order. Metadata becomes writable as soon as a majority is up.

Check it:

```sh
curl -s 127.0.0.1:17421/api/cluster/metadata | jq '.controller, [.nodes[] | {id, status, is_voter}]'
curl -s -X POST 127.0.0.1:17431/api/channels -H 'content-type: application/json' \
  -d '{"name":"orders","mode":"stream","partitions":6}'
curl -s 127.0.0.1:17441/api/cluster/metadata | jq '.channels.orders[] | {partition, leader, replicas, isr}'
curl -s -X POST 127.0.0.1:17441/api/channels/orders/publish -H 'content-type: application/json' \
  -d '{"messages":[{"key":"a","payload":{"encoding":"utf8","data":"hello"}}]}'
curl -s 127.0.0.1:17421/api/cluster/status | jq '.raft, .under_replicated_partitions, .offline_partitions'
```

You can call any node. Channel create, update and delete go through the controller and return
once the channel exists on the node you called; the other nodes create it within milliseconds,
and a subscribe/publish that reaches a node before its local copy exists waits for it (≤ 3 s),
so "create, then use it through any node" works. A partition leader whose role is still being
applied (fresh channel, leadership that just moved) holds a publish for up to 1.5 s instead of
answering `NotLeader`. Publishes are routed to partition leaders. Reads (messages, DLQ, DLQ
export, replay) fan out to partition leaders and return 503 "changing leaders; retry" while a
partition's leadership moves.

## Adding a node

Start the new node with a fresh `--node-id`. Give it `--peers` set to the existing seeds (the
new id is *not* in the list) and the same secret. It heartbeats the seeds, which redirect it
to the controller. The controller then adds it as a Raft **learner**, and promotes it to
**voter** once it is caught up, up to 5 voters (more nodes stay learners). New channels use
the new node, and (mode `full`, the default) the rebalancer moves existing replicas onto it
once it has been up for `rebalance_stable` (`node_timeout + 4 s`); see *Automatic
rebalancing*.

## Timing (defaults, `node_timeout` = 6 s)

* Raft: heartbeat 150 ms, election timeout 1–2 s.
* Each node heartbeats the controller every 1 s. The controller confirms its own leadership
  with a quorum before it acks. An ack gives the node a **lease** of 4 s, counted from the
  time the heartbeat was *sent*.
* A node without a valid lease stops leading partitions (**self-fencing**). This applies, for
  example, to a node partitioned away from the majority.
* The controller marks a node `Down` 6 s after its last heartbeat, which is always after that
  node's lease expired. At that point it elects new leaders from the ISR for the partitions the
  node led (epoch + 1). It also removes the node from every ISR right away, so acks don't wait
  for the 10 s replica-lag eviction.
* A newly elected controller waits `lease + 0.5 s` before failing over nodes it hasn't heard
  from. This covers leases granted by its predecessor.
* When a node restarts (new process), the controller moves its leaderships away and removes it
  from the ISRs before the node gets a lease. The node rejoins as a follower and catches up
  (usually within a second); it is marked `Up` once every channel's demotion has been committed
  (measured 9–12 s after the process starts, while its leaders re-add it to the ISRs), and it
  gets its preferred leaderships back shortly after.
* **Graceful shutdown** (SIGINT/SIGTERM) first hands leadership over: the node drains itself,
  stops following and tells the controller it leaves; it is marked `Down` and removed from
  every ISR at once, so peers don't wait for `node_timeout`. Budget 4 s (falls back to normal
  Down detection). Stopping the controller still costs a Raft election (openraft 0.9 has no
  leadership transfer).

Measured on loopback (release build, 3 processes on one MacBook, demo traffic running):

| Scenario | Longest gap between publish acks (client view) |
|---|---|
| Non-controller leader `kill -9` | 4.5–5.7 s |
| Controller `kill -9` (election + new-controller grace) | 8.5–9.2 s (earlier v0.2 measurement, not re-run) |
| `kill -9` + restart within 1 s | ~1 s |
| Graceful stop (SIGTERM) of a non-controller leader | 0.9 s |
| Graceful stop of the controller | 1.3 s |
| Integration test: graceful stop of a leader (`node_timeout` 3 s) | ≈ 70 ms |

`cargo run -p fluxmq-client --example resilience` against the three seeds during these runs:
30 260 and 27 290 acknowledged publishes (`kill -9` + restart; rolling SIGTERM restart of all
three nodes), **0 lost**, 17 and 87 redeliveries.

## Failure scenarios

| Situation | What happens |
|---|---|
| A node crashes | It is marked `Down` after 6 s. Its partitions move to ISR members and its ISR entries are removed. Publishes resume. Clients get `MetadataChanged` and `NotLeader`, refresh metadata and retry. |
| The controller crashes | Raft elects a new controller (1–3 s). After the grace period it fails over the dead node's partitions. |
| A node is cut off from the majority | Its lease expires after 4 s and it stops leading. The majority fails its partitions over after 6 s. When the node reconnects, it truncates to the new leader's log and follows (epoch fencing). |
| A partition loses every ISR member | The partition goes offline (`leader: null`, counted in `offline_partitions`). There is no unclean election. The first ISR member to return becomes leader again. |
| A majority of voters is lost | Metadata is read-only (writes fail with `unavailable`: after ~8 s of retries on a follower, after 4.5 s on a leader that lost its quorum — openraft's `client_write` waits for the commit with no bound, so the local proposal is bounded; the outcome is then unknown and the write may still apply if the quorum returns). There is no failover. Partitions whose leaders still hold a lease keep serving until the lease expires, then stop. |
| A node restarts quickly (before being marked `Down`) | The restart is detected by the node's incarnation. Its leaderships move to other ISR members first, so metadata never names it leader of a partition it can't serve yet. |
| A node's data dir is wiped | It comes back empty. It is not in any ISR, so it is never elected, and it re-replicates as a follower. Don't wipe a majority of voters' Raft state (`data/raft/`). |

## Operations

* **Drain before maintenance**: `POST /api/cluster/nodes/{id}/drain` (admin; console: Nodes →
  node card → *Drain leadership*) moves leadership off the node and marks it `Leaving`. A plain
  SIGTERM does this automatically before stopping. A `Leaving` node is chosen as leader only as
  a last resort. Restarting the node sets it back to `Up`, and preferred-leader rebalancing
  then moves its leaderships back.
* **Reassign a partition** (console: Nodes → click a square in the partition matrix):
  `POST /api/cluster/partitions/reassign`
  `{"channel":"orders","partition":0,"replicas":["n3","n2","n4"]}` (admin; the first replica is
  the preferred leader). The current leader stays leader if it is still a replica. Otherwise
  the new leader must be a live ISR member among the new replicas. To move a partition fully,
  first add the new replicas (keeping the old leader), wait for them to join the ISR (see
  `GET /api/cluster/status`), then remove the old ones.
* **Health**: `GET /api/cluster/status` returns Raft term, leader, voters and learners,
  `under_replicated_partitions`, `offline_partitions`, and per-partition HW, LEO, ISR and
  follower lag (gathered from every leader). Prometheus: `fluxmq_cluster_*` on `/metrics`.
* **Remove a node permanently**: decommission it (live node) or forget it (dead node) — see
  *Node decommission and forget (v0.5)* below.
* The audit log (`GET /api/audit`, `<data-dir>/audit.log`) is **per node**: an action is
  recorded on the node that served the request.

## TLS between nodes (v0.4)

`--cluster-tls-cert/--cluster-tls-key/--cluster-tls-ca` switch every node-to-node connection to
**mutual TLS** (rustls, aws-lc-rs, TLS 1.2/1.3):

| Traffic | Server side | Client side |
|---|---|---|
| Raft + cluster RPC (`--raft-addr`) | HTTPS, client certificate required | `reqwest` with the node certificate as identity |
| Replication (`--replication-addr`) | TLS before the replication `Hello`, client certificate required | the follower's fetch connection presents the node certificate |
| Internal HTTP (`/internal/v1/*` on the HTTP port) | see below | same `reqwest` client, `https://` |

* **Trust = the cluster CA.** A peer is accepted when its certificate chains to
  `--cluster-tls-ca`; names (SANs) are not checked, because peers are dialled by whatever host
  was advertised (often an IP). **Node-id pinning:** the subject CN should equal the node id; a
  mismatch only logs a warning (`gen-dev` issues `CN=<node id>`).
* **The cluster secret stays required** on top (defense in depth): a valid certificate without
  the secret is still refused.
* **All nodes must agree.** A node without cluster TLS cannot talk to a TLS cluster and vice
  versa (handshakes fail and are counted in `fluxmq_tls_handshake_failures_total{surface="cluster"}`).
  Turning cluster TLS on or off therefore means restarting every node with the new flags
  (the data directories are unaffected); there is no mixed mode.
* **Internal HTTP on the HTTP port.** Peers call each other's `/internal/v1/*` on the public HTTP
  port, presenting their node certificate:
  * HTTP port with TLS (`--tls-cert` / `--http-tls-cert`): client certificates are *requested*
    (optional, verified against the cluster CA) so browsers connect without one and peers with
    theirs. The HTTP certificate must chain to the cluster CA or to the OS trust store (with
    `gen-dev`, `server.pem` and the node certificates share one CA).
  * HTTP port plaintext (`--http-tls off`, or no `--tls-cert`): a connection that starts with a
    TLS handshake is served as cluster mTLS (node certificate, client certificate required);
    anything else is plain HTTP for browsers and tools.
  * Either way `/internal/*` answers **401** unless the request arrived over a connection whose
    client certificate chains to the cluster CA.
* TLS on the HTTP port in a cluster requires cluster TLS (the broker refuses to start
  otherwise), because peers could not reach the internal API.
* Certificates are re-read on change (30 s check) and on SIGHUP; established connections are
  kept. The CA bundle is read at startup.

Local example with `gen-dev` certificates (same ports as above):

```sh
./target/release/fluxmq tls gen-dev --out ./certs --nodes n1,n2,n3
export FLUXMQ_CLUSTER_SECRET=dev-secret FLUXMQ_CLUSTER_TLS_CA=./certs/ca.pem
TLS_ARGS="--tls-cert ./certs/server.pem --tls-key ./certs/server-key.pem --tls-client-ca ./certs/ca.pem"

./target/release/fluxmq --node-id n1 --peers $PEERS --data-dir ./data/n1 \
  --client-addr 127.0.0.1:17420 --http-addr 127.0.0.1:17421 \
  --raft-addr 127.0.0.1:17422 --replication-addr 127.0.0.1:17423 \
  --cluster-tls-cert ./certs/node-n1.pem --cluster-tls-key ./certs/node-n1-key.pem $TLS_ARGS
# n2, n3 alike with node-n2.* / node-n3.*

curl -s --cacert ./certs/ca.pem https://127.0.0.1:17421/api/tls | jq '.cluster'
cargo run --release -p fluxmq-client --bin fluxmq-bench -- \
  --seeds tls://127.0.0.1:17420,tls://127.0.0.1:17430,tls://127.0.0.1:17440 \
  --tls-ca ./certs/ca.pem --tls-cert ./certs/client.pem --tls-key ./certs/client-key.pem
```

## Internal endpoints

All internal endpoints require `x-fluxmq-cluster-secret`; with cluster TLS also a peer
certificate from the cluster CA. They are not part of the public API.

* On `--raft-addr`: `POST /raft/{append,vote,snapshot}` (Raft), `POST /cluster/write`
  (metadata writes forwarded to the leader), `POST /cluster/heartbeat` (liveness + join),
  `POST /cluster/controller` (create/delete channel, reassign, drain, leave, rebalance
  status/config/request/pause/resume/cancel, v0.5 decommission / forget), and
  `GET /cluster/info` (bootstrap probe).
* On the HTTP port, `/internal/v1/*`: `publish` (forwarded publishes, postcard), `channels/{name}/messages[/{p}/{o}]`,
  `channels/{name}/read` (replay reads), `dlq`, `dlq/summary`, `dlq/{retry,discard}`,
  `channels/{name}/groups/{group}[/reset]` (`DELETE …?check=true` = deletion pre-check),
  `replication-status` and `node-stats` (telemetry). Each node answers only for partitions it
  currently leads. v0.3 adds `webhooks/stats` (per-node webhook counters, summed by
  `GET /api/webhooks/stats`), `notify/history` (v0.5: a node-local read of the
  `$system.notifications` history, proxied to its partition leader by
  `GET /api/notify/history`; `POST notify/history/append` on the controller appends records
  produced elsewhere), and `backup/partial` (cut + upload of the
  partitions this node leads), `backup/run` and `backup/status` (proxied to the controller).
  v0.4 adds `events/feed` (a node-local `$system.events` read, see below).
  On a single node these return 404.

## Node decommission and forget (v0.5)

Contract: the decommission block of `fluxmq_proto::cluster`. Code:
`crates/fluxmq-cluster/src/decommission.rs` (state machine on the controller), the planner rule
in `rebalance.rs`, the moves through the normal executor in `rebalancer.rs`.

**Decommission a live node** — `POST /api/cluster/nodes/{id}/decommission` (admin, any node;
it runs on the controller) → `DecommissionStatus`:

1. *Checks* (409 `conflict` with the reason, nothing changed): no rebalance plan is running or
   paused; no other node is being decommissioned; removing the node keeps a safe Raft quorum
   (see below); for every partition with a replica on the node, the other `Up` nodes can hold
   its replica count (e.g. RF 3 on 3 nodes is refused — add a node first) and the partition
   has a live leader to copy from. Unknown node → 404. Repeating the call while it runs returns
   the current status.
2. *`moving_replicas`*: the node becomes `Decommissioning` in the registry, its partition
   leaderships are drained, and a repair-only plan starts (trigger `decommission <id>`, visible
   in `GET /api/cluster/rebalance`, runs in every rebalance mode including `off`). Planner rule:
   a decommissioning node is never a target, and every replica it holds gets a `repair` move —
   in any plan. Moves follow the normal protocol (add, catch up, commit, leader), so
   durability never drops. If a plan ends with replicas left (rollbacks), the next one starts
   (at most 5); no possible move for 120 s ends in `failed`. **Cancel** = `POST
   /api/cluster/rebalance/cancel` while `moving_replicas`: in-flight moves finish, the
   decommission ends `failed` with error `cancelled`. After any failure the node stays in the
   cluster with status `Up` (replicas already moved stay moved) and `POST` retries.
3. *`leaving_membership`* (0 replicas): the quorum is re-checked (it waits, with the reason in
   `error`, while unsafe), then openraft `change_membership` removes the node: a voter with
   `RemoveVoters` (not kept as learner), a learner with `RemoveNodes`.
4. *`removed`*: `cluster/nodes/<id>` is deleted. The process may be stopped. Its heartbeats are
   refused; it stops its cluster roles (Raft, replication, heartbeats) and writes
   `<data-dir>/cluster-removed`.

`GET /api/cluster/nodes/{id}/decommission` (viewer) shows progress from the replicated record
(`cluster/decommission/<id>`) on any node: `state`, live `replicas_remaining`,
`initial_replicas` (the baseline for a progress bar), start/finish, `error`. 404 when there is
no record. Records stay readable for 24 h after they end; `GET /api/cluster/decommissions`
(viewer) lists the current and recent ones, newest first. The removed process itself keeps its
last view.

`POST …/decommission?dry_run=true` runs the same checks (same 409 / 404) and returns the
`RebalanceStatus` of the plan it would start (`moves` all `pending`, `projected_load`), writing
nothing.

Audit: the request is audited on the serving node and the start / end (`removed`, `failed`) on
the controller that observed them, all as `node.decommission`.

**Restarting a removed node** with the same id (and data dir) joins it as a **new, empty
node**: the marker makes it wipe its stale Raft state (a removed member's old log and term
would disturb the group when re-added) and never bootstrap; the controller accepts its newer
incarnation (the refused incarnation is kept in `cluster/removed/<id>` until then — it
outlives the 24 h record), registers it, adds it as a learner and promotes it like any joining
node. Automatic rebalancing (mode `full`) then gives it
replicas.

**Removing the controller.** openraft 0.9 has no leadership transfer (and an election
triggered on another voter can't win while the leader lease holds). The controller is
decommissioned like any node: its replicas and leaderships move away while it stays in
charge, then it removes *itself* from the membership. openraft keeps it leader until that
change is committed and applied, then steps it down; the remaining voters elect a new
controller within the election timeout (1–2 s), which finds the record in
`leaving_membership` with the node already out and finishes step 4. No partition is affected
(the node leads none) and node leases outlast the election; controller commands retry across
it.

**Forget a dead node** — `DELETE /api/cluster/nodes/{id}` (admin) → `DecommissionStatus`: only
for a node that is `Down` and holds no replicas (after the automatic repair, mode
`repair`/`full` after `replace_after_secs`, or a manual rebalance). Otherwise 409 (a live node
must be decommissioned; replicas must be moved first); unknown → 404. It runs steps 3–4 at
once. If the machine comes back with the same process, its heartbeats are refused; a restart
joins as a new node.

**Quorum safety** (both operations): removing a learner is always allowed. Removing a voter
needs at least one remaining voter and a live majority of both the remaining voters and the
current voters (joint consensus needs both); e.g. with 3 voters and one down, removing a
second one is refused. Removing a voter of a 2-voter cluster leaves 1 voter (allowed by the
contract; any learner is then promoted).

**Events**: the decommission's plan produces the usual `rebalance_started` (subject
`decommission <id>`), `partition_moved`, `rebalance_completed` / `rebalance_failed`; the
decommission itself produces `node_decommission_started` (warning; title "Decommissioning node
n3: 8 replicas to move"), then `node_decommissioned` (healthy) or `node_decommission_failed`
(critical, the error in the title, e.g. `cancelled`); a forget produces `node_forgotten`
(subject = node id for all four). Like the rebalance events, every node derives them from the
replicated decommission records (`Cluster::decommission_records`, polled with the telemetry
facts once per second; the first observation is a baseline), so the feed agrees cluster-wide;
a record is identified by node + start time, so a retried decommission is a new one. A
restarted node shows up as `node_up`.

Single node: all three routes answer 400 `unsupported_single_node`.

## v0.3 services in a cluster

* **Notifications** are dispatched by the controller only (exactly one message per signal);
  `$system.events` is created once by the controller (1 partition, replicated like any
  channel) and only the controller appends to it.
* **Notification history** (v0.5): every delivery record is stored in the replicated system
  channel `$system.notifications` (1 partition, retention 7 days with a 64 MiB backstop; the
  API returns at most the newest 10 000 records). Only the controller appends: dispatch
  records through a background writer, test sends before the API answers; a test send handled
  by another node is forwarded to the controller (`/internal/v1/notify/history/append`).
  `GET /api/notify/history?limit=` (default 1000, max 10 000) on any node is proxied to that
  channel's partition leader (`/internal/v1/notify/history`), like the events feed, and merged
  with that node's in-memory ring of recent records (deduplicated by id, newest first). The
  history therefore survives a controller failover and a full restart; before v0.5 it lived
  only in the controller's memory.
* **The events feed on any node** (v0.4 fix). With RF < N some nodes hold no copy of
  `$system.events`; previously `GET /api/events/feed` on such a node returned an empty page.
  Now, like message reads, a node that isn't the partition leader proxies the request (query
  unchanged, long-poll included) to the leader's `/internal/v1/events/feed`, which reads its
  local replica; cursors are offsets, identical on every replica, so a client can switch
  nodes between polls. Without a leader (failover) the node answers from its own copy
  (empty, cursor preserved, when it has none) instead of failing the poll loop. A graceful
  shutdown ends a proxied long-poll with an immediate re-ask to the leader.
* **Node-local alerts** (v0.4: `tls_cert_expiring`, `tls_reload_failed` — each node has its
  own certificates) ride along in the node-stats snapshot every node already pulls from its
  peers each second (an optional postcard trailer; older nodes ignore it). Every node adds
  the live peers' local alerts to its alert candidates (ids are node-qualified,
  `tls_cert_expiring:n2/cluster`), so the alert shows on every node and the controller
  notifies it exactly once. A down peer's alerts drop out (its `node_down` alert covers it).
* **HTTP push webhooks** run on every node for the partitions it leads (`$webhook:<name>`
  groups); stats are summed over nodes.
* **Backups** run on the controller: every leader uploads its partitions, the controller merges
  and writes the manifest; a partition without a leader during the run fails the backup
  (retry). Restore produces a single node — seed a new cluster from it. A node older than
  0.3.0 makes backups answer `400 unsupported_in_cluster`.
* **Graceful leave** (v0.3 fix): the drain retries channels whose assignment write lost a CAS
  race, and the node waits (bounded) until its reconciler has demoted the drained partitions
  before it stops replicating — previously a skipped or not-yet-demoted partition kept
  accepting forwarded publishes it could never acknowledge, stalling senders for the 5 s
  forward timeout.

## Automatic rebalancing (v0.4)

Contract: the v0.4 block of `fluxmq_proto::cluster`. Code: `crates/fluxmq-cluster/src/rebalance.rs`
(pure planner + move transitions) and `rebalancer.rs` (executor and triggers, on the controller).

### What it does

* **Repair.** A replica on a node that has been `Down` longer than `replace_after_secs`
  (default 600 s; `0` = never) is replaced by a replica on the least-loaded eligible node.
  A partition with fewer replicas than `--replication-factor` (RF capped at creation because
  fewer nodes were alive) grows to it. A replica is removed only when a replacement exists,
  so repair never lowers the number of copies.
* **Balance** (mode `full`). When `max - min` replicas per eligible node exceeds
  `imbalance_threshold` (default 1), one replica at a time moves from the most to the least
  loaded node until they differ by at most one (or no move is possible). Moves are sticky:
  a partition that moves anyway is preferred, then one where the source isn't the preferred
  leader. Then preferred leaders (replica 0) are balanced the same way by reordering replica
  lists (no data movement). Storage bytes (reported in heartbeats) only break ties.
* **Eligible targets** are alive, `Up`, not `Leaving`/draining, and (for automatic plans) up
  for at least `rebalance_stable`. `Down`/`Leaving` nodes and partitions without a live leader
  are never touched. A partition never gets two replicas on one node.

The planner is deterministic and pure; property tests (randomized assignments, liveness,
thresholds and modes) check the invariants, that repair restores RF wherever enough nodes
exist, and that planning again on the result of a plan gives no moves.

### Move protocol

| Step | Assignment write (CAS on the channel's key, retried on conflict) | `MoveState` |
|---|---|---|
| start | `replicas = from ∪ to` (old preferred first); leader, ISR and **epoch unchanged** — the leader adds the new followers at the same epoch, outside the ISR, so publish acks never wait for an empty replica | `pending → catching_up` |
| caught up | once every new replica is in the committed ISR: `replicas = to`, ISR restricted to it, epoch + 1 (a removed replica deletes its copy only for a newer epoch); the leader stays if it is still a replica, else the first live ISR member of `to` leads | `catching_up → finalizing` |
| leader | preferred-leader election to `to[0]` (epoch + 1); after 30 s without it — or at once when `to[0]` is down (v0.5) — the move counts as done and the regular tick finishes the election | `finalizing → done` |
| cancel | a move that hasn't started when the plan is cancelled is dropped (never touched the assignment) | `pending → cancelled` |
| timeout | new replicas not in the ISR after `move_timeout_secs`: `replicas = from` (epoch + 1). If no member of `from` is a live ISR member, the union stays and the rollback is retried — never below the old set | `rolled_back` |

A pure reorder (preferred-leader balancing) skips the first step. At most
`max_concurrent_moves` moves are in flight cluster-wide. While a partition catches up, every
node sets `throttle_bytes_per_sec` on it (`ReplicationManager::set_catch_up_throttle`; the
leader enforces it for followers outside the ISR). The executor runs every 500 ms on the
controller, serialized with the controller tick and operator commands.

**Persistence and failover.** The plan is the meta document `cluster/rebalance/plan`
(id, state, trigger, moves with states and timestamps, outcome); the config is
`cluster/rebalance/config`. The executor writes the assignment first and the plan second;
on every step it compares each in-flight move with the live assignment (`replicas == from`,
`== from ∪ to`, `== to`, or something else), so a new controller adopts writes whose plan
update was lost and resumes the same plan. A move whose partition was changed by someone
else (operator reassign) is abandoned (`rolled_back` with an error), nothing is overwritten.
Move timeouts count from `started_at_ms` (wall clock), so they survive failover.

### Triggers

Automatic plans (mode `repair` or `full`) are evaluated at most every 30 s, first one interval
after a node becomes controller, and never while a plan is active:

* a replica on a node down longer than `replace_after_secs` (down-since = last heartbeat seen
  by this controller; a new controller starts counting from its own election);
* a partition below the replication factor (capped RF growth) with a free eligible node;
* mode `full`: replica or preferred-leader imbalance above the threshold — e.g. a node that
  joined and has been up for `rebalance_stable`.

The trigger is recorded (`node n4 joined`, `node n2 down 605s`, `replication factor below
target`, `imbalance 3 > threshold 1`, `manual`). After a plan with rollbacks, automatic
evaluation pauses for 10 intervals so a failing move isn't retried in a tight loop; since v0.5
the pause applies to balance moves only — repair moves of a down node still start (a balance
plan that failed because its target crashed must not delay that node's repair). Mode
`off` never plans by itself (v0.3 behaviour; preferred-leader election stays on).

### Knobs (`PUT /api/cluster/rebalance/config`, whole object)

| Field | Default | Meaning |
|---|---|---|
| `mode` | `full` (or `--auto-rebalance`) | `off` / `repair` / `full` |
| `max_concurrent_moves` | 2 | moves in flight cluster-wide (1–1000) |
| `imbalance_threshold` | 1 | allowed `max - min` replicas per eligible node |
| `replace_after_secs` | 600 | replace replicas of a node down this long; 0 = never |
| `throttle_bytes_per_sec` | 0 | catch-up rate per moving replica; 0 = unlimited |
| `move_timeout_secs` | 1800 | roll a move back after this long (≥ 1) |

### REST

| Method | Path | Role | Body → response |
|---|---|---|---|
| GET | `/api/cluster/rebalance` | viewer | `RebalanceStatus` (config, state, load of every registered node with its status — `Down`/`Leaving` nodes still show their replicas —, imbalance over `Up` nodes, under-replicated, moves with live `lag` and `initial_lag`, trigger, projected load). Answered by the controller (live lag and storage), falling back to the local replica of the plan after 3 s. |
| PUT | `/api/cluster/rebalance/config` | admin | `RebalanceConfig` → status; 400 when invalid |
| POST | `/api/cluster/rebalance` | admin | `RebalanceRequest {dry_run, channels}` → status. A dry run returns the plan in `moves` (all `pending`) and `projected_load`, and writes nothing. Otherwise it starts a plan (trigger `manual`, threshold ignored, works in mode `off`); 409 `conflict` while a plan is running, paused or cancelling (a dry run is always allowed); 404 for an unknown channel (exact names) |
| POST | `/api/cluster/rebalance/pause` · `resume` · `cancel` | admin | → status. Pause stops starting moves (in-flight ones finish); cancel turns pending moves into `cancelled`, waits for in-flight moves to finish or roll back, then ends the plan; 400 when no plan is active |

Any node accepts them; mutations run on the controller through `POST /cluster/controller`
(like reassign and drain). Single node: `GET` returns `mode = off`, `state = idle`;
mutations return 400 `unsupported_single_node`. Mutations are audited on the serving node.

### Events

Every node derives them from the replicated plan (so the feed agrees cluster-wide); the
controller's copy goes to `$system.events` and notifications: `rebalance_started` (subject =
trigger), `replica_repair_started` (subject = the down node), `partition_moved` (subject
`channel/partition`, title `n1,n2 → n2,n4`), `rebalance_completed` (title "completed" or
"cancelled" with done / rolled-back counts) and `rebalance_failed` (some moves rolled back).

### Safety summary

* Durability never drops: replicas are added before any is removed, removal waits for the
  new replicas to be in the committed ISR, rollback restores the old list, and cancel only
  stops future moves.
* Leaders only come from the ISR (no unclean election); every leader change bumps the epoch.
* Each write is a CAS; concurrent ISR persistence, failover, operator commands and a new
  controller can't be overwritten.
* Mode `off` plans nothing by itself. Independent of the mode, v0.4 nodes write the config
  document once and delete local copies of partitions they are no longer a replica of (see
  *Replica moves* and *Upgrading from 0.3*).

## Replica moves (v0.4): bootstrap, snapshot, cleanup, throttle

Automatic rebalancing moves a partition with ordinary assignment changes: add the new
replicas (`replicas = old ∪ new`, same epoch), wait until each is in the ISR, commit the final
replica list (epoch + 1), and the removed replicas delete their copy. The storage side:

* **Bootstrap.** A new replica with no data, or with data entirely below the leader's log
  start (retention), restarts its log empty at the leader's `low_offset` and copies from
  there. Older messages are not copied (they are gone on the leader too). A partition log
  may start at any offset; recovery, reads, retention and `low_offset` handle it.
* **State snapshot.** The first fetch of every leadership returns a snapshot of the
  partition's consumer state: every group's committed offset and the whole DLQ of the
  partition. Later fetches stream changes. A replica joins the ISR only after it has applied
  the snapshot. So if it is elected later, consumers resume from the replicated offsets
  (at-least-once) and the DLQ is complete, even if the state history it would need is gone.
* **Cleanup.** A node removed from a partition's replica list stops following it and deletes
  everything it holds for it: records, epoch history, group positions on the partition and
  its DLQ entries. When no partition of a channel holds anything on the node, the channel
  directory goes too. The channel stays registered, so REST lists still work, and its
  directory comes back automatically if the node later gets a role for one of its
  partitions. A `None` role alone (leader unknown, partition offline, fenced) **never**
  deletes data. Deletion needs an explicit
  `ReplicationManager::remove_replica(channel, partition, epoch)` from the reconciler, issued
  when the committed assignment's `replicas` no longer lists the node. It is refused for a
  stale epoch (the node may have been re-added) and while the node still leads or follows
  the partition at that epoch. A role set during the deletion wins.
* **Throttle.** `throttle_bytes_per_sec` limits the fetch bytes a leader serves to followers
  **outside the ISR**, per follower (token bucket, 1 s burst). In-sync followers are never
  throttled. The executor sets it per moving partition with
  `ReplicationManager::set_catch_up_throttle` on every node (the leader enforces it).
  `follower_progress` reports each follower's lag, ISR membership and whether it is
  throttled (the source for `PartitionMove::lag`).

## Upgrading from 0.3

Rolling upgrade, one node at a time: stop it, replace the binary, start it with the same
`--data-dir` and flags, and wait until it is `Up` again and its replicas are back in the ISR
(`GET /api/cluster/status`: `under_replicated_partitions = 0`) before the next node. The
on-disk formats (segments, offsets, DLQ, Raft log and snapshots, meta documents) are
unchanged; v0.3 and v0.4 nodes replicate with each other (the replication wire protocol is
the same; the group/DLQ state snapshot uses the existing state-record stream).

Behaviour changes to know about, **in every mode including `--auto-rebalance off`**:

* **Cleanup of non-replica copies.** A v0.4 node deletes its local copy of every partition
  whose committed assignment doesn't list it (`ReplicationManager::remove_replica`, see
  *Replica moves*). v0.3 left such copies behind after a reassignment, so the first v0.4 start
  on an old data dir can free disk space and remove whole channel directories on nodes that
  hold none of a channel's partitions. Guards: it runs only after the node has caught up with
  the controller's metadata; only for partitions absent from `replicas` (never for a `None`
  role — unknown leader, offline, fenced); never with an assignment epoch older than the
  local copy's; and a role set meanwhile wins. Partitions the node still replicates are never
  touched. Copy `data/channels/<channel>/p<N>` away first if you want to keep stale copies.
* **Rebalancing config.** The first v0.4 controller writes `cluster/rebalance/config` from
  `--auto-rebalance` (default `full`). In a mixed cluster a v0.4 controller can start moves
  whose new replica is a v0.3 node; that works (the v0.3 node follows like after a
  reassignment) but the v0.3 node won't delete copies it loses until it is upgraded. To keep
  placement frozen during the upgrade, start the v0.4 nodes with `--auto-rebalance off`
  (or `PUT /api/cluster/rebalance/config` with `mode: off`) and switch to `full` afterwards.
* **Verified** (v0.4 integration): a 3-node v0.3.0 cluster (RF 2) with a 6-partition channel
  (3,400 messages) and two partitions reassigned the v0.3 way (each left a stale copy behind)
  was upgraded node by node to v0.4 with `--auto-rebalance off` while the `resilience`
  example kept publishing and consuming. Mixed 0.3/0.4 phases ran with a v0.4 controller and
  v0.3 followers, 0 under-replicated / offline partitions after each step. The two stale
  copies and the empty skeletons of partitions a node never held were emptied (an empty
  segment and the `replica.json` epoch fence remain while the channel has other partitions
  on the node); every partition a node still replicates kept its exact bytes. All 3,400
  messages read back with an identical content hash through every node, and
  92,770 acknowledged publishes during the upgrade: 0 lost (61 redeliveries).
* **Wire additions** are all optional fields (`Heartbeat.storage_bytes`, the node-stats
  alert trailer, new `ControllerOp` variants used only by the rebalancing API — a v0.3
  controller answers them with an error, so use the rebalancing API once the controller runs
  v0.4).

## Data on disk

`data/raft/` holds the metadata Raft state:

* `log.bin`: CRC-framed JSON entries. A torn tail is truncated on start.
* `vote.json`: fsync'd.
* `snapshot.json`: a snapshot every 1000 entries. The log is purged up to 200 entries behind it.
* `committed.json`: best effort.

Partition data lives in the usual `data/channels/` layout. Every node registers every channel
locally and serves only the partitions it has a role for. Since v0.4, a channel none of whose
partitions is held on the node has no directory there (see *Replica moves*). When a node has caught up, it moves
local channels that aren't in the cluster metadata to the broker's trash directory. Note
that this also applies to a former single node joining a cluster.
