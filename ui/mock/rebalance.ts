/**
 * Mock automatic rebalancing (v0.4) — contract of the rebalancing block in
 * crates/fluxmq-proto/src/cluster.rs.
 *
 *   MOCK_SCENARIO=rebalance   4 nodes, n4 joined at boot with no replicas. 6 s later the controller
 *                             plans `node n4 joined` and drives every move pending → catching_up
 *                             (lag shrinking) → finalizing → done, at most `max_concurrent_moves`
 *                             at a time. The third move stalls and rolls back after 25 s (mock time
 *                             for `move_timeout_secs`). Full mode re-evaluates every 30 s.
 *   MOCK_REBALANCE_MODE=off|repair|full   initial mode (default full); `repair` leaves n4 empty
 *                             with the imbalance over the threshold — a good start for a dry run.
 *   default / cluster         3 nodes at RF 3: balanced; the last plan (preferred-leader moves) is
 *                             from two days ago.
 *   single / empty            GET = mode off, idle; every mutation → 400 unsupported_single_node.
 */
import type { MoveReason, PartitionMove, RebalanceConfig, RebalanceMode, RebalanceNodeLoad, RebalanceState, RebalanceStatus } from '../src/api/types';
import { REBALANCE_MODES } from '../src/api/types';
import type { Cluster, PState } from './cluster';
import { ApiErr, CLUSTERED, SCENARIO, type Sim } from './sim';
import { fnv } from './util';

const GIB = 1024 ** 3;
const AUTO_START_MS = 6_000;
const REEVALUATE_MS = 30_000;
const FINALIZE_MS = 1_800;
const STALL_TIMEOUT_MS = 25_000;
/** Messages per second a new replica catches up when unthrottled (mock speed, ~1 KiB messages). */
const CATCHUP_RATE = 9_000;
/** Lag at which the stalling move gets stuck. */
const STALL_LAG = 41_203;

const moveId = (m: PartitionMove) => `${m.channel}/${m.partition}`;

interface Plan {
  moves: PartitionMove[];
  trigger: string | null;
  started_at_ms: number | null;
  finished_at_ms: number | null;
  /** Index (in `moves`) of the move that stalls and rolls back; -1 = none. */
  stallAt: number;
  /** v0.5 decommission plans: smaller catch-up lags so the mock finishes in ~30 s. */
  fast?: boolean;
}

const isObj = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);

function partitionBytes(channel: string, p: number): number {
  return Math.round((0.35 + ((fnv(`${channel}/${p}`) % 1000) / 1000) * 3.4) * GIB);
}

export class Rebalancer {
  private config: RebalanceConfig;
  private state: RebalanceState = 'idle';
  private plan: Plan = { moves: [], trigger: null, started_at_ms: null, finished_at_ms: null, stallAt: -1 };
  private nextEvalAt: number;
  private finalizeAt = new Map<string, number>();

  constructor(
    private sim: Sim,
    private cluster: Cluster,
  ) {
    const envMode = process.env.MOCK_REBALANCE_MODE as RebalanceMode | undefined;
    this.config = {
      mode: CLUSTERED ? (envMode && REBALANCE_MODES.includes(envMode) ? envMode : 'full') : 'off',
      // decommission scenario: 4 at a time so draining a node takes ~30 s of mock time
      max_concurrent_moves: SCENARIO === 'decommission' ? 4 : 2,
      imbalance_threshold: 1,
      replace_after_secs: 600,
      throttle_bytes_per_sec: 0,
      move_timeout_secs: 1800,
    };
    this.nextEvalAt = sim.start + AUTO_START_MS;
    if (CLUSTERED && SCENARIO !== 'rebalance') this.seedHistory();
    sim.onTick.push((now, dt) => this.tick(now, dt));
  }

  // ───────────── REST ─────────────

  status(): RebalanceStatus {
    const load = this.load(this.cluster.placement());
    return {
      config: { ...this.config },
      state: this.state,
      load,
      imbalance: spread(load),
      under_replicated: this.underReplicated(),
      moves: this.plan.moves.map((m) => ({ ...m, from_replicas: [...m.from_replicas], to_replicas: [...m.to_replicas] })),
      trigger: this.plan.trigger,
      started_at_ms: this.plan.started_at_ms,
      finished_at_ms: this.plan.finished_at_ms,
      projected_load: this.projected(),
    };
  }

  putConfig(body: unknown): RebalanceStatus {
    this.single();
    if (!isObj(body)) throw new ApiErr(400, 'bad_request', 'body must be a RebalanceConfig object');
    const mode = body.mode as RebalanceMode;
    if (!REBALANCE_MODES.includes(mode)) throw new ApiErr(400, 'bad_request', "'mode' must be off, repair or full");
    const int = (k: string, min: number, max = Number.MAX_SAFE_INTEGER): number => {
      const v = body[k];
      if (typeof v !== 'number' || !Number.isInteger(v) || v < min || v > max) throw new ApiErr(400, 'bad_request', `'${k}' must be an integer in ${min}..${max === Number.MAX_SAFE_INTEGER ? '∞' : max}`);
      return v;
    };
    this.config = {
      mode,
      max_concurrent_moves: int('max_concurrent_moves', 1, 64),
      imbalance_threshold: int('imbalance_threshold', 0),
      replace_after_secs: int('replace_after_secs', 0),
      throttle_bytes_per_sec: int('throttle_bytes_per_sec', 0),
      move_timeout_secs: int('move_timeout_secs', 60),
    };
    this.sim.addAudit('cluster.rebalance.config', 'cluster', { ...this.config });
    this.nextEvalAt = Date.now() + 2_000;
    return this.status();
  }

  request(body: unknown): RebalanceStatus {
    this.single();
    const b = body === undefined ? {} : body;
    if (!isObj(b)) throw new ApiErr(400, 'bad_request', 'body must be a RebalanceRequest object');
    const dry = b.dry_run === true;
    const channels = b.channels == null ? [] : b.channels;
    if (!Array.isArray(channels) || !channels.every((c) => typeof c === 'string')) throw new ApiErr(400, 'bad_request', "'channels' must be an array of channel names");
    for (const c of channels as string[]) if (!this.sim.channels.has(c)) throw new ApiErr(404, 'not_found', `channel '${c}' not found`);
    if (!dry && this.state !== 'idle') throw new ApiErr(409, 'conflict', `a rebalance plan is already ${this.state}`);
    const moves = this.planMoves('manual', channels as string[]);
    if (dry) {
      const now = this.status();
      return { ...now, moves, trigger: 'manual', started_at_ms: null, finished_at_ms: null, projected_load: this.projected(moves) };
    }
    this.sim.addAudit('cluster.rebalance', 'cluster', { channels, moves: moves.length });
    if (moves.length) this.begin(moves, 'manual', false);
    return this.status();
  }

  pause(): void {
    this.single();
    if (this.state !== 'running') throw new ApiErr(409, 'conflict', `cannot pause: rebalance is ${this.state}`);
    this.state = 'paused';
    this.sim.addAudit('cluster.rebalance.pause', 'cluster', {});
  }

  resume(): void {
    this.single();
    if (this.state !== 'paused') throw new ApiErr(409, 'conflict', `cannot resume: rebalance is ${this.state}`);
    this.state = 'running';
    this.sim.addAudit('cluster.rebalance.resume', 'cluster', {});
  }

  cancel(): void {
    this.single();
    if (this.state !== 'running' && this.state !== 'paused') throw new ApiErr(409, 'conflict', `cannot cancel: rebalance is ${this.state}`);
    // Pending moves become `cancelled` (never touched the assignment); in-flight ones finish or roll back.
    for (const m of this.plan.moves) if (m.state === 'pending') m.state = 'cancelled';
    this.state = 'cancelling';
    this.sim.addAudit('cluster.rebalance.cancel', 'cluster', {});
  }

  // ───────────── v0.5 decommission hooks (mock/decommission.ts) ─────────────

  busyState(): RebalanceState {
    return this.state;
  }

  /** Run moves planned elsewhere (decommission) through the normal executor. */
  runExternal(moves: PartitionMove[], trigger: string, opts: { stallAt?: number } = {}): void {
    const now = Date.now();
    this.plan = { moves, trigger, started_at_ms: now, finished_at_ms: null, stallAt: opts.stallAt ?? -1, fast: true };
    this.state = 'running';
    this.sim.pushEvent('rebalance_started', 'Rebalance started (decommission)', trigger, 'healthy');
  }

  /** A dry-run answer for moves planned elsewhere (decommission `?dry_run=true`). */
  preview(moves: PartitionMove[], trigger: string): RebalanceStatus {
    return { ...this.status(), moves, trigger, started_at_ms: null, finished_at_ms: null, projected_load: this.projected(moves) };
  }

  /** The current plan's moves (live references). */
  currentMoves(): readonly PartitionMove[] {
    return this.plan.moves;
  }

  private single(): void {
    if (!CLUSTERED) throw new ApiErr(400, 'unsupported_single_node', 'rebalancing needs a cluster: this broker runs as a single node');
  }

  // ───────────── load ─────────────

  /** Every registered node (down / leaving ones keep their remaining replicas). */
  private load(placement: [string, PState[]][], override?: Map<string, string[]>): RebalanceNodeLoad[] {
    const out = new Map<string, RebalanceNodeLoad>(this.cluster.registeredNodes().map((n) => [n.id, { node: n.id, status: n.status, replicas: 0, leaders: 0, preferred_leaders: 0, storage_bytes: 0 }]));
    for (const [ch, parts] of placement) {
      for (const p of parts) {
        const reps = override?.get(`${ch}/${p.partition}`) ?? p.replicas;
        const bytes = partitionBytes(ch, p.partition);
        reps.forEach((r, i) => {
          const l = out.get(r);
          if (!l) return;
          l.replicas++;
          l.storage_bytes += bytes;
          if (i === 0) l.preferred_leaders++;
        });
        // Projected: leadership settles on replica 0. Current: the actual leader.
        const leader = override ? reps[0] : p.leader;
        const ll = leader ? out.get(leader) : undefined;
        if (ll && ll.status !== 'down') ll.leaders++;
      }
    }
    return [...out.values()];
  }

  private projected(moves: PartitionMove[] = this.plan.moves): RebalanceNodeLoad[] {
    const final = new Map<string, string[]>();
    for (const m of moves) if (m.state !== 'rolled_back' && m.state !== 'cancelled') final.set(`${m.channel}/${m.partition}`, m.to_replicas);
    return this.load(this.cluster.placement(), final);
  }

  private underReplicated(): number {
    const rf = this.cluster.replicationFactor();
    let n = 0;
    for (const [, parts] of this.cluster.placement()) for (const p of parts) if (p.replicas.filter((r) => this.cluster.isUp(r)).length < Math.min(rf, p.replicas.length || rf)) n++;
    return n;
  }

  // ───────────── planner (sticky, minimal; never two replicas on one node) ─────────────

  private planMoves(reason: MoveReason, channels: string[]): PartitionMove[] {
    const live = this.cluster.liveNodes();
    if (live.length < 2) return [];
    const placement = this.cluster.placement().filter(([ch]) => !channels.length || channels.includes(ch));
    const all = this.cluster.placement();
    const count = new Map(live.map((n) => [n, 0]));
    const pref = new Map(live.map((n) => [n, 0]));
    for (const [, parts] of all)
      for (const p of parts)
        p.replicas.forEach((r, i) => {
          if (count.has(r)) count.set(r, count.get(r)! + 1);
          if (i === 0 && pref.has(r)) pref.set(r, pref.get(r)! + 1);
        });
    const moved = new Set<string>();
    const moves: PartitionMove[] = [];
    const byId = (a: string, b: string) => a.localeCompare(b, undefined, { numeric: true });
    for (let guard = 0; guard < 500; guard++) {
      const sorted = [...live].sort((a, b) => count.get(b)! - count.get(a)! || byId(a, b));
      const max = sorted[0]!;
      const min = sorted[sorted.length - 1]!;
      if (count.get(max)! - count.get(min)! <= 1) break;
      const wantLeader = pref.get(max)! > pref.get(min)! + 1;
      let pick: { ch: string; p: PState; idx: number } | null = null;
      for (const [ch, parts] of placement) {
        for (const p of parts) {
          if (moved.has(`${ch}/${p.partition}`)) continue;
          const idx = p.replicas.indexOf(max);
          if (idx < 0 || p.replicas.includes(min)) continue;
          if (!pick || (wantLeader ? idx === 0 && pick.idx !== 0 : idx !== 0 && pick.idx === 0)) pick = { ch, p, idx };
          if (pick && (wantLeader ? pick.idx === 0 : pick.idx !== 0)) break;
        }
        if (pick && (wantLeader ? pick.idx === 0 : pick.idx !== 0)) break;
      }
      if (!pick) break;
      const to = [...pick.p.replicas];
      to[pick.idx] = min;
      moved.add(`${pick.ch}/${pick.p.partition}`);
      count.set(max, count.get(max)! - 1);
      count.set(min, count.get(min)! + 1);
      if (pick.idx === 0) {
        pref.set(max, pref.get(max)! - 1);
        pref.set(min, pref.get(min)! + 1);
      }
      moves.push({ channel: pick.ch, partition: pick.p.partition, reason, from_replicas: [...pick.p.replicas], to_replicas: to, state: 'pending', lag: null, initial_lag: null, started_at_ms: null, finished_at_ms: null, error: null });
    }
    return moves;
  }

  // ───────────── executor ─────────────

  private begin(moves: PartitionMove[], trigger: string, auto: boolean): void {
    const now = Date.now();
    this.plan = { moves, trigger, started_at_ms: now, finished_at_ms: null, stallAt: moves.length > 3 ? 2 : -1 };
    this.state = 'running';
    this.sim.pushEvent('rebalance_started', auto ? 'Rebalance started' : 'Rebalance started (manual)', trigger, 'healthy');
  }

  private tick(now: number, dt: number): void {
    if (!CLUSTERED) return;
    if (this.state === 'idle') {
      if (now >= this.nextEvalAt) {
        this.nextEvalAt = now + REEVALUATE_MS;
        this.autoTrigger();
      }
      return;
    }
    const throttleRate = this.config.throttle_bytes_per_sec > 0 ? Math.max(200, this.config.throttle_bytes_per_sec / 1024) : Infinity;
    const rate = Math.min(CATCHUP_RATE, throttleRate);
    const moves = this.plan.moves;
    for (let i = 0; i < moves.length; i++) {
      const m = moves[i]!;
      if (m.state === 'catching_up') {
        const stall = i === this.plan.stallAt;
        const floor = stall ? STALL_LAG : 0;
        const jitter = 0.7 + 0.6 * ((fnv(`${m.channel}${m.partition}${Math.floor(now / 1000)}`) % 100) / 100);
        m.lag = Math.max(floor, Math.round((m.lag ?? 0) - rate * dt * jitter));
        if (stall && now - (m.started_at_ms ?? now) > STALL_TIMEOUT_MS) {
          m.state = 'rolled_back';
          m.finished_at_ms = now;
          m.error = `catch-up timed out after ${this.config.move_timeout_secs} s: ${m.to_replicas.filter((r) => !m.from_replicas.includes(r)).join(', ')} still ${m.lag.toLocaleString('en-US')} messages behind`;
          m.lag = null;
          const p = this.find(m);
          if (p) this.cluster.writeAssignment(m.channel, m.partition, m.from_replicas, p.isr);
        } else if (m.lag <= 0) {
          m.lag = 0;
          m.state = 'finalizing';
          const p = this.find(m);
          if (p) this.cluster.writeAssignment(m.channel, m.partition, p.replicas, [...new Set([...p.isr, ...m.to_replicas])]);
          this.finalizeAt.set(moveId(m), now + FINALIZE_MS);
        }
      } else if (m.state === 'finalizing') {
        if (now >= (this.finalizeAt.get(moveId(m)) ?? 0)) {
          this.finalizeAt.delete(moveId(m));
          m.state = 'done';
          m.lag = null;
          m.finished_at_ms = now;
          const p = this.find(m);
          if (p) this.cluster.writeAssignment(m.channel, m.partition, m.to_replicas, p.isr);
          this.sim.pushEvent('partition_moved', `${m.from_replicas.join(',')} → ${m.to_replicas.join(',')}`, `${m.channel}/${m.partition}`, 'healthy');
        }
      }
    }
    // Start pending moves up to the concurrency limit (not while paused / cancelling).
    if (this.state === 'running') {
      let inFlight = moves.filter((m) => m.state === 'catching_up' || m.state === 'finalizing').length;
      for (const m of moves) {
        if (inFlight >= this.config.max_concurrent_moves) break;
        if (m.state !== 'pending') continue;
        const p = this.find(m);
        if (!p) continue;
        m.state = 'catching_up';
        m.started_at_ms = now;
        m.lag = moves.indexOf(m) === this.plan.stallAt ? 72_480 : this.plan.fast ? 4_000 + (fnv(`${m.channel}/${m.partition}`) % 9_000) : 14_000 + (fnv(`${m.channel}/${m.partition}`) % 46_000);
        m.initial_lag = m.lag;
        // Step 1: replicas = old ∪ new (old preferred first); new replicas are not in the ISR yet.
        this.cluster.writeAssignment(m.channel, m.partition, [...m.from_replicas, ...m.to_replicas.filter((r) => !m.from_replicas.includes(r))], p.isr);
        inFlight++;
      }
    }
    const open = moves.some((m) => m.state === 'pending' || m.state === 'catching_up' || m.state === 'finalizing');
    if (!open) {
      const rolled = moves.filter((m) => m.state === 'rolled_back').length;
      const cancelled = this.state === 'cancelling';
      this.state = 'idle';
      this.plan.finished_at_ms = now;
      this.nextEvalAt = now + REEVALUATE_MS;
      const done = moves.filter((m) => m.state === 'done').length;
      if (cancelled) this.sim.pushEvent('rebalance_completed', `Rebalance cancelled · ${done} moved${rolled ? `, ${rolled} rolled back` : ''}, ${moves.filter((m) => m.state === 'cancelled').length} cancelled`, this.plan.trigger ?? 'manual', 'warning');
      else if (rolled && !done) this.sim.pushEvent('rebalance_failed', `Rebalance failed · ${rolled} moves rolled back`, this.plan.trigger ?? 'manual', 'critical');
      else this.sim.pushEvent('rebalance_completed', rolled ? `Rebalance completed · ${rolled} move rolled back` : `Rebalance completed · ${done} partitions moved`, this.plan.trigger ?? 'manual', rolled ? 'warning' : 'healthy');
    }
  }

  private autoTrigger(): void {
    if (this.config.mode !== 'full') return;
    const load = this.load(this.cluster.placement());
    const imb = spread(load);
    if (imb <= this.config.imbalance_threshold) return;
    const moves = this.planMoves('balance', []);
    if (!moves.length) return;
    const joined = load.find((l) => l.replicas === 0 && l.status === 'up');
    this.begin(moves, joined ? `node ${joined.node} joined` : `imbalance ${imb} > threshold ${this.config.imbalance_threshold}`, true);
  }

  private find(m: PartitionMove): PState | undefined {
    return this.cluster.placement().find(([ch]) => ch === m.channel)?.[1][m.partition];
  }

  /** Default cluster: a finished plan from two days ago (preferred leaders evened out after n2 rejoined). */
  private seedHistory(): void {
    const placement = this.cluster.placement();
    const picks: PartitionMove[] = [];
    const t0 = this.sim.start - 2 * 86_400_000 - 3 * 3_600_000;
    for (const [ch, parts] of placement) {
      for (const p of parts) {
        if (picks.length >= 4 || p.replicas.length < 2 || ch.startsWith('$')) continue;
        if ((p.partition + ch.length) % 3 !== 0) continue;
        const to = [...p.replicas];
        const from = [to[1]!, to[0]!, ...to.slice(2)];
        const i = picks.length;
        picks.push({ channel: ch, partition: p.partition, reason: 'balance', from_replicas: from, to_replicas: to, state: 'done', lag: null, initial_lag: 18_000 + i * 7_000, started_at_ms: t0 + i * 41_000, finished_at_ms: t0 + i * 41_000 + 6_000 + i * 900, error: null });
      }
    }
    this.plan = { moves: picks, trigger: 'node n2 joined', started_at_ms: t0, finished_at_ms: t0 + picks.length * 41_000 + 9_000, stallAt: -1 };
  }
}

/** `max - min` replicas per `up` node. */
function spread(load: RebalanceNodeLoad[]): number {
  const up = load.filter((l) => l.status === 'up');
  if (!up.length) return 0;
  const v = up.map((l) => l.replicas);
  return Math.max(...v) - Math.min(...v);
}
