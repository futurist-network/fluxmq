/**
 * Mock node decommission / forget (v0.5) — contract of the decommission block in
 * crates/fluxmq-proto/src/cluster.rs.
 *
 *   POST /api/cluster/nodes/{id}/decommission   marks the node `decommissioning`, drains its
 *        leadership and plans `repair` moves for every replica it holds, run by the v0.4 executor
 *        (mock/rebalance.ts, 4 at a time in the decommission scenario → ~30 s). Once it holds none:
 *        `leaving_membership` (~4 s), then it leaves the registry → `removed`.
 *        409 `conflict`: already decommissioning; a rebalance plan running / paused / cancelling;
 *        not enough other `up` nodes for some channel's RF; Raft quorum would break.
 *   POST …/decommission?dry_run=true   → `RebalanceStatus` with the planned moves (same checks,
 *        nothing changes). MOCK_NO_DECOMMISSION_DRY_RUN=1 answers 400 instead (UI fallback).
 *   GET  …/decommission     progress (readable 24 h after it ends); 404 when none is known.
 *   GET  /api/cluster/decommissions   current and recent (24 h) decommissions.
 *   Cancel = POST /api/cluster/rebalance/cancel while moving_replicas: in-flight moves finish, then
 *        `failed` with error "cancelled" and the node returns to `up`.
 *   DELETE /api/cluster/nodes/{id}   forget: only a `down` node with no replicas (409 otherwise).
 *
 *   MOCK_DECOMMISSION_FAIL=1   the third move stalls and rolls back after 25 s → `failed`, and the
 *                              node returns to `up` (what the real server does is not specified).
 */
import type { DecommissionStatus, PartitionMove, RebalanceStatus } from '../src/api/types';
import type { Cluster } from './cluster';
import type { Rebalancer } from './rebalance';
import { ApiErr, CLUSTERED, type Sim } from './sim';

const LEAVE_MS = 4_000;
const FAIL = process.env.MOCK_DECOMMISSION_FAIL === '1';
const NO_DRY_RUN = process.env.MOCK_NO_DECOMMISSION_DRY_RUN === '1';
const RETAIN_MS = 24 * 3_600_000;

interface Run extends DecommissionStatus {
  leaveAt: number | null;
  moves: PartitionMove[];
}

export class Decommissioner {
  private runs = new Map<string, Run>();

  constructor(
    private sim: Sim,
    private cluster: Cluster,
    private rebalancer: Rebalancer,
  ) {
    sim.onTick.push((now) => this.tick(now));
  }

  list(): DecommissionStatus[] {
    const now = Date.now();
    return [...this.runs.values()].filter((r) => r.finished_at_ms == null || now - r.finished_at_ms < RETAIN_MS).map((r) => this.view(r));
  }

  status(id: string): DecommissionStatus {
    const r = this.runs.get(id);
    if (!r || (r.finished_at_ms != null && Date.now() - r.finished_at_ms >= RETAIN_MS)) throw new ApiErr(404, 'not_found', `no decommission known for node '${id}'`);
    return this.view(r);
  }

  private view(r: Run): DecommissionStatus {
    return { node: r.node, state: r.state, replicas_remaining: r.replicas_remaining, initial_replicas: r.initial_replicas, started_at_ms: r.started_at_ms, finished_at_ms: r.finished_at_ms, error: r.error };
  }

  /** `?dry_run=true`: the plan without starting (same refusals). */
  dryRun(id: string): RebalanceStatus {
    if (NO_DRY_RUN) throw new ApiErr(400, 'bad_request', "unknown query parameter 'dry_run'");
    const targets = this.check(id);
    return this.rebalancer.preview(this.plan(id, targets), `decommission ${id}`);
  }

  start(id: string): DecommissionStatus {
    const targets = this.check(id);
    this.cluster.markStatus(id, 'decommissioning');
    const moved = this.cluster.drainForever(id);
    const moves = this.plan(id, targets);
    const now = Date.now();
    const initial = this.cluster.replicaCount(id);
    const run: Run = { node: id, state: 'moving_replicas', replicas_remaining: initial, initial_replicas: initial, started_at_ms: now, finished_at_ms: null, error: null, leaveAt: null, moves };
    this.runs.set(id, run);
    if (moves.length) this.rebalancer.runExternal(moves, `decommission ${id}`, { stallAt: FAIL && moves.length > 3 ? 2 : -1 });
    this.sim.addAudit('cluster.node.decommission', id, { replicas: run.replicas_remaining, moved_leaderships: moved });
    this.sim.pushEvent('node_decommission_started', `Decommissioning node ${id}: ${initial} replica${initial === 1 ? '' : 's'} to move`, id, 'warning');
    return this.view(run);
  }

  /** Validations shared by start and dry run; returns the nodes that can take replicas. */
  private check(id: string): string[] {
    if (!CLUSTERED) throw new ApiErr(400, 'unsupported_single_node', 'decommission needs a cluster: this broker runs as a single node');
    const n = this.cluster.node(id);
    if (!n) throw new ApiErr(404, 'not_found', `node '${id}' not found`);
    if (n.status === 'decommissioning') throw new ApiErr(409, 'conflict', `node '${id}' is already being decommissioned`);
    const busy = this.rebalancer.busyState();
    if (busy !== 'idle') throw new ApiErr(409, 'conflict', `a rebalance plan is ${busy}; wait for it to finish or cancel it before decommissioning '${id}'`);
    const nodes = this.cluster.nodeList();
    const targets = nodes.filter((x) => x.id !== id && x.status === 'up').map((x) => x.id);
    for (const [channel, parts] of this.cluster.placement()) {
      if (!parts.some((p) => p.replicas.includes(id))) continue;
      const rf = Math.max(...parts.map((p) => p.replicas.length));
      if (rf > targets.length) throw new ApiErr(409, 'conflict', `cannot hold replication factor ${rf} of channel '${channel}' without '${id}': only ${targets.length} other up node${targets.length === 1 ? '' : 's'} (${targets.join(', ') || 'none'})`);
    }
    if (n.is_voter) {
      const remaining = nodes.filter((x) => x.is_voter && x.id !== id);
      const live = remaining.filter((x) => x.status !== 'down').length;
      const quorum = Math.floor(remaining.length / 2) + 1;
      if (!remaining.length) throw new ApiErr(409, 'conflict', `'${id}' is the only Raft voter`);
      if (live < quorum) throw new ApiErr(409, 'conflict', `removing '${id}' would leave ${live} live of ${remaining.length} voters, below a quorum of ${quorum}`);
    }

    return targets;
  }

  /** Repair-reason moves: each replica on `id` goes to the least-loaded target not already in the set. */
  private plan(id: string, targets: string[]): PartitionMove[] {
    const count = new Map(targets.map((t) => [t, this.cluster.replicaCount(t)]));
    const out: PartitionMove[] = [];
    for (const [channel, parts] of this.cluster.placement()) {
      for (const p of parts) {
        const idx = p.replicas.indexOf(id);
        if (idx < 0) continue;
        const pick = targets.filter((t) => !p.replicas.includes(t)).sort((a, b) => count.get(a)! - count.get(b)! || a.localeCompare(b, undefined, { numeric: true }))[0];
        if (!pick) continue;
        count.set(pick, count.get(pick)! + 1);
        const to = [...p.replicas];
        to[idx] = pick;
        out.push({ channel, partition: p.partition, reason: 'repair', from_replicas: [...p.replicas], to_replicas: to, state: 'pending', lag: null, initial_lag: null, started_at_ms: null, finished_at_ms: null, error: null });
      }
    }
    return out;
  }

  forget(id: string): void {
    if (!CLUSTERED) throw new ApiErr(400, 'unsupported_single_node', 'this broker runs as a single node');
    const n = this.cluster.node(id);
    if (!n) throw new ApiErr(404, 'not_found', `node '${id}' not found`);
    if (n.status !== 'down') throw new ApiErr(409, 'conflict', `node '${id}' is ${n.status}, not down — decommission a running node instead of forgetting it`);
    const replicas = this.cluster.replicaCount(id);
    if (replicas > 0) throw new ApiErr(409, 'conflict', `node '${id}' still holds ${replicas} replica${replicas === 1 ? '' : 's'}; wait for repair or reassign them first`);
    this.cluster.removeNode(id);
    this.sim.addAudit('cluster.node.forget', id, {});
    this.sim.pushEvent('node_forgotten', `Node ${id} forgotten`, id, 'healthy');
  }

  private tick(now: number): void {
    for (const r of this.runs.values()) {
      if (r.state === 'moving_replicas') {
        r.replicas_remaining = this.cluster.replicaCount(r.node);
        const rolled = r.moves.find((m) => m.state === 'rolled_back');
        if (rolled) {
          r.state = 'failed';
          r.finished_at_ms = now;
          r.error = `move ${rolled.channel}/${rolled.partition} rolled back: ${rolled.error ?? 'catch-up timed out'}`;
          this.sim.pushEvent('node_decommission_failed', `Decommission of node ${r.node} failed: ${r.error}`, r.node, 'critical');
          this.cluster.undrain(r.node);
          this.cluster.markStatus(r.node, 'up');
          continue;
        }
        const open = r.moves.some((m) => m.state === 'pending' || m.state === 'catching_up' || m.state === 'finalizing');
        if (!open && r.moves.some((m) => m.state === 'cancelled')) {
          // Operator cancel (POST /api/cluster/rebalance/cancel): moved replicas stay moved.
          r.state = 'failed';
          r.finished_at_ms = now;
          r.error = 'cancelled';
          this.sim.pushEvent('node_decommission_failed', `Decommission of node ${r.node} failed: cancelled`, r.node, 'critical');
          this.cluster.undrain(r.node);
          this.cluster.markStatus(r.node, 'up');
          this.sim.addAudit('cluster.node.decommission.cancelled', r.node, { replicas_remaining: r.replicas_remaining });
          continue;
        }
        if (r.replicas_remaining === 0 && !open) {
          r.state = 'leaving_membership';
          r.leaveAt = now + LEAVE_MS;
        }
      } else if (r.state === 'leaving_membership' && r.leaveAt != null && now >= r.leaveAt) {
        this.cluster.removeNode(r.node);
        r.state = 'removed';
        r.finished_at_ms = now;
        this.sim.addAudit('cluster.node.removed', r.node, {});
        this.sim.pushEvent('node_decommissioned', `Node ${r.node} decommissioned and removed from the cluster`, r.node, 'healthy');
      }
    }
  }
}
