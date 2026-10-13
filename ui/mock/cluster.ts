/**
 * Mock cluster model — contract of crates/fluxmq-proto/src/cluster.rs.
 *
 * default           3 healthy nodes (n1 controller), RF 3
 * MOCK_SCENARIO=cluster   same, plus a failure cycle (240 s, starts 12 s after boot):
 *    +0 s   n3 stops answering → status down; partitions it led are offline
 *    +6 s   controller fails them over to ISR members (epoch++); ISRs shrink → under-replicated
 *    +90 s  n3 returns → joining, catching up (follower lag shrinks)
 *    +110 s n3 back in every ISR → up; leaders stay moved
 *    +135 s preferred-leader election moves leadership back
 * MOCK_SCENARIO=rebalance   4 nodes; n4 just joined, so every existing replica sits on n1–n3
 *    and the v0.4 rebalancer (mock/rebalance.ts) moves replicas onto n4
 * MOCK_SCENARIO=decommission (v0.5)   5 nodes, RF 3: replicas balanced over n1–n4; n5 is a dead
 *    machine (down, its replicas already repaired away) — forget it; decommission n4 (see
 *    mock/decommission.ts). Decommissioning a second node fails with 409: RF 3 can't be held.
 * single / empty    one node, RF 1
 */
import type { ClusterStatus, NodeInfo, NodeStatus, PartitionAssignment, PartitionReplicationStatus, ReassignRequest } from '../src/api/types';
import { ApiErr, NODE_IDS, SCENARIO, VERSION, type Sim } from './sim';

const CYCLE_MS = 240_000;
const FAIL_START_MS = 12_000;
const NODE_TIMEOUT_MS = 6_000;
const DOWN_FOR_MS = 90_000;
const CATCHUP_MS = 20_000;
const REBALANCE_AT_MS = 135_000;

export interface PState extends PartitionAssignment {
  /** follower → lag */
  lag: Record<string, number>;
}

export class Cluster {
  private nodes: NodeInfo[];
  private assign = new Map<string, PState[]>();
  private term = 7;
  private epoch = 412;
  private logIndex = 18_204;
  private phase: 'up' | 'down' | 'failed' | 'joining' | 'rejoined' = 'up';
  private pending = new Map<string, number>(); // `${channel}/${p}/${node}` → in-ISR at
  private drained = new Map<string, number>(); // node → until
  private readonly failing = SCENARIO === 'cluster';
  private readonly rf: number;

  constructor(private sim: Sim) {
    this.nodes = NODE_IDS.map((id, i) => ({
      id,
      client_addr: `10.12.0.${11 + i}:7420`,
      http_addr: `10.12.0.${11 + i}:7421`,
      status: 'up' as NodeStatus,
      is_controller: i === 0,
      is_voter: true,
      version: VERSION,
    }));
    this.rf = Math.min(3, SCENARIO === 'rebalance' ? this.nodes.length - 1 : this.nodes.length);
    if (SCENARIO === 'decommission') {
      const n5 = this.nodes.find((n) => n.id === 'n5');
      if (n5) {
        n5.status = 'down';
        n5.is_voter = false; // a learner: forgetting it can't touch the Raft quorum
      }
    }
    sim.nodeMembership = (id) => {
      const n = this.nodes.find((x) => x.id === id);
      return n ? { status: n.status, is_controller: n.is_controller } : null;
    };
    sim.clusterAlerts = () => this.alerts();
    sim.onTick.push((now) => this.tick(now));
  }

  private ids(): string[] {
    return this.nodes.map((n) => n.id);
  }

  private up(id: string | null): boolean {
    return id != null && this.nodes.find((n) => n.id === id)?.status !== 'down';
  }

  private ensure(name: string): PState[] {
    const ch = this.sim.channels.get(name);
    if (!ch) return [];
    let a = this.assign.get(name);
    if (!a || a.length !== ch.config.partitions) {
      // A node that "just joined" (rebalance scenario) holds nothing yet.
      const ids = SCENARIO === 'rebalance' ? this.ids().filter((id) => id !== 'n4') : SCENARIO === 'decommission' ? this.ids().filter((id) => id !== 'n5') : this.ids();
      const offset = [...name].reduce((s, c) => s + c.charCodeAt(0), 0);
      a = Array.from({ length: ch.config.partitions }, (_, p) => {
        const replicas = Array.from({ length: this.rf }, (_, k) => ids[(p + offset + k) % ids.length]!);
        return { partition: p, leader: replicas[0]!, replicas, isr: [...replicas], epoch: 3 + (p % 4), lag: {} };
      });
      this.assign.set(name, a);
    }
    return a;
  }

  private all(): [string, PState[]][] {
    for (const name of [...this.assign.keys()]) if (!this.sim.channels.has(name)) this.assign.delete(name);
    return [...this.sim.channels.keys()].map((n) => [n, this.ensure(n)]);
  }

  private setStatus(id: string, status: NodeStatus): void {
    const n = this.nodes.find((x) => x.id === id);
    if (!n || n.status === status) return;
    const prev = n.status;
    n.status = status;
    this.epoch++;
    this.logIndex += 3;
    if (status === 'down') this.sim.pushEvent('node_down', `Node ${id} is not responding`, id, 'critical');
    if (prev === 'down' && status !== 'down') this.sim.pushEvent('node_up', `Node ${id} is back`, id, 'healthy');
  }

  /** Move leadership of partitions led by `id` (or with a dead leader) to another live ISR member. */
  private failover(isDead: (leader: string | null) => boolean, avoid?: string): number {
    let moved = 0;
    for (const [, parts] of this.all()) {
      for (const p of parts) {
        if (!isDead(p.leader)) continue;
        const next = p.isr.find((r) => r !== p.leader && r !== avoid && this.up(r)) ?? p.replicas.find((r) => r !== p.leader && r !== avoid && this.up(r) && p.isr.includes(r));
        if (next) {
          p.leader = next;
          p.epoch++;
          moved++;
        } else if (!this.up(p.leader)) {
          p.leader = null; // offline: no live ISR member (no unclean election)
        }
      }
    }
    if (moved) {
      this.epoch++;
      this.logIndex += moved;
    }
    return moved;
  }

  private tick(now: number): void {
    this.logIndex += 1 + Math.floor(Math.random() * 3);
    if (this.failing) {
      const t = (now - this.sim.start - FAIL_START_MS) % CYCLE_MS;
      const victim = this.nodes[2]?.id;
      if (victim && now - this.sim.start >= FAIL_START_MS) {
        if (t < NODE_TIMEOUT_MS) {
          if (this.phase !== 'down') {
            this.phase = 'down';
            this.setStatus(victim, 'down');
          }
        } else if (t < DOWN_FOR_MS) {
          if (this.phase === 'down' || this.phase === 'up' || this.phase === 'rejoined') {
            this.phase = 'failed';
            this.setStatus(victim, 'down');
            for (const [, parts] of this.all()) for (const p of parts) p.isr = p.isr.filter((r) => r !== victim);
            this.failover((l) => l === victim);
            this.sim.pushEvent('alert_opened', `Partitions failed over from ${victim}`, victim, 'warning');
          }
        } else if (t < DOWN_FOR_MS + CATCHUP_MS) {
          if (this.phase === 'failed') {
            this.phase = 'joining';
            this.setStatus(victim, 'joining');
          }
        } else if (this.phase === 'joining') {
          this.phase = 'rejoined';
          this.setStatus(victim, 'up');
          for (const [, parts] of this.all()) for (const p of parts) if (p.replicas.includes(victim) && !p.isr.includes(victim)) p.isr.push(victim);
        }
        if (this.phase === 'rejoined' && t >= REBALANCE_AT_MS) {
          this.phase = 'up';
          this.preferredElection();
        }
      }
    }
    // Pending reassignment catch-up.
    for (const [k, at] of this.pending) {
      if (now < at) continue;
      this.pending.delete(k);
      const [name, p, node] = k.split('/');
      const ps = this.assign.get(name!)?.[Number(p)];
      if (ps && ps.replicas.includes(node!) && !ps.isr.includes(node!) && this.up(node!)) ps.isr.push(node!);
    }
    for (const [id, until] of this.drained) if (now > until) this.drained.delete(id);
    // Partitions whose leader is down and haven't failed over yet stay offline.
    for (const [, parts] of this.all()) for (const p of parts) if (p.leader && !this.up(p.leader) && this.phase !== 'down') this.failover((l) => l === p.leader);
  }

  private preferredElection(): void {
    let moved = 0;
    for (const [, parts] of this.all()) {
      for (const p of parts) {
        const pref = p.replicas[0];
        if (pref && p.leader !== pref && p.isr.includes(pref) && this.up(pref) && !this.drained.has(pref)) {
          p.leader = pref;
          p.epoch++;
          moved++;
        }
      }
    }
    if (moved) this.epoch++;
  }

  private alerts() {
    const out: { id: string; kind: 'node_down' | 'under_replicated_partitions' | 'offline_partitions'; severity: 'warning' | 'critical'; title: string; detail: string; link: string }[] = [];
    for (const n of this.nodes) if (n.status === 'down') out.push({ id: `node_down:${n.id}`, kind: 'node_down', severity: 'critical', title: `Node ${n.id} is down`, detail: `${n.client_addr} stopped answering heartbeats`, link: '/nodes' });
    const s = this.status();
    if (s.offline_partitions) out.push({ id: 'offline_partitions:cluster', kind: 'offline_partitions', severity: 'critical', title: `${s.offline_partitions} partitions offline`, detail: 'No in-sync replica is alive: publish and consume fail for them', link: '/nodes' });
    if (s.under_replicated_partitions) out.push({ id: 'under_replicated_partitions:cluster', kind: 'under_replicated_partitions', severity: 'warning', title: `${s.under_replicated_partitions} partitions under-replicated`, detail: 'ISR is smaller than the replica set — one more failure may lose availability', link: '/nodes' });
    return out;
  }

  status(): ClusterStatus {
    const channels: Record<string, PartitionAssignment[]> = {};
    const partitions: PartitionReplicationStatus[] = [];
    let under = 0;
    let offline = 0;
    const joining = this.nodes.find((n) => n.status === 'joining');
    const now = Date.now();
    for (const [name, parts] of this.all()) {
      const ch = this.sim.channels.get(name)!;
      channels[name] = parts.map(({ partition, leader, replicas, isr, epoch }) => ({ partition, leader, replicas: [...replicas], isr: [...isr], epoch }));
      for (const p of parts) {
        const isOffline = !p.leader || !this.up(p.leader);
        if (isOffline) offline++;
        else if (p.isr.length < p.replicas.length) under++;
        const hw = this.sim.partitionHigh(ch, p.partition);
        const lag: Record<string, number> = {};
        for (const r of p.replicas) {
          if (r === p.leader) continue;
          const n = this.nodes.find((x) => x.id === r);
          if (n?.status === 'down') lag[r] = Math.round(ch.rate * (ch.weights[p.partition] ?? 0) * 40);
          else if (n?.status === 'joining' && joining) lag[r] = Math.round(ch.rate * (ch.weights[p.partition] ?? 0) * 30 * Math.max(0.05, 1 - ((now - this.sim.start) % 20_000) / 20_000));
          else lag[r] = p.isr.includes(r) ? Math.floor(Math.random() * 40) : Math.round(ch.rate * 2);
        }
        partitions.push({ channel: name, partition: p.partition, leader: p.leader, epoch: p.epoch, high_watermark: hw, log_end_offset: hw + Math.floor(Math.random() * 30), follower_lag: lag, isr: [...p.isr] });
      }
    }
    const controller = this.nodes.find((n) => n.is_controller)?.id ?? null;
    return {
      metadata: { cluster_name: 'prod-eu-1', epoch: this.epoch, controller, nodes: this.nodes.map((n) => ({ ...n })), channels },
      replication_factor: this.rf,
      under_replicated_partitions: under,
      offline_partitions: offline,
      raft: {
        term: this.term,
        leader: controller,
        last_log_index: this.logIndex + (this.phase === 'down' ? 2 : 0),
        committed_index: this.logIndex,
        voters: this.nodes.filter((n) => n.is_voter).map((n) => n.id),
        learners: this.nodes.filter((n) => !n.is_voter).map((n) => n.id),
      },
      partitions,
    };
  }

  metadata() {
    return this.status().metadata;
  }

  // ── v0.4 rebalancer hooks (mock/rebalance.ts) ──

  replicationFactor(): number {
    return this.rf;
  }

  /** Live partition state (mutable references), channels in name order. */
  placement(): [string, PState[]][] {
    return this.all().sort((a, b) => a[0].localeCompare(b[0]));
  }

  /** Nodes that can host replicas (not down, not leaving). */
  liveNodes(): string[] {
    return this.nodes.filter((n) => n.status === 'up' || n.status === 'joining').map((n) => n.id);
  }

  /** Every registered node with its status (rebalance `NodeLoad.status`). */
  registeredNodes(): { id: string; status: NodeStatus }[] {
    return this.nodes.map((n) => ({ id: n.id, status: n.status }));
  }

  isUp(id: string): boolean {
    return this.up(id);
  }

  /** An assignment write (epoch bump), as the controller does for every move step. */
  writeAssignment(channel: string, partition: number, replicas: string[], isr: string[]): void {
    const p = this.assign.get(channel)?.[partition];
    if (!p) return;
    p.replicas = [...replicas];
    p.isr = isr.filter((r) => replicas.includes(r));
    const pref = p.replicas[0];
    if (pref && p.isr.includes(pref) && this.up(pref) && !this.drained.has(pref)) p.leader = pref;
    else if (!p.leader || !p.replicas.includes(p.leader)) p.leader = p.isr.find((r) => this.up(r)) ?? null;
    p.epoch++;
    this.epoch++;
    this.logIndex += 1;
  }

  // ── v0.5 decommission / forget hooks (mock/decommission.ts) ──

  node(id: string): NodeInfo | undefined {
    return this.nodes.find((n) => n.id === id);
  }

  nodeList(): NodeInfo[] {
    return this.nodes.map((n) => ({ ...n }));
  }

  markStatus(id: string, status: NodeStatus): void {
    this.setStatus(id, status);
  }

  /** Replicas currently assigned to a node (incl. moves in flight). */
  replicaCount(id: string): number {
    let n = 0;
    for (const [, parts] of this.all()) for (const p of parts) if (p.replicas.includes(id)) n++;
    return n;
  }

  /** Move leadership away for good (a decommissioning node never leads again). */
  drainForever(id: string): number {
    this.drained.set(id, Number.POSITIVE_INFINITY);
    return this.failover((l) => l === id, id);
  }

  undrain(id: string): void {
    this.drained.delete(id);
  }

  /** Raft membership change + registry delete. A removed controller hands leadership over first. */
  removeNode(id: string): void {
    const n = this.nodes.find((x) => x.id === id);
    if (!n) return;
    this.nodes = this.nodes.filter((x) => x.id !== id);
    this.drained.delete(id);
    if (n.is_controller) {
      const next = this.nodes.find((x) => x.status === 'up' && x.is_voter);
      if (next) next.is_controller = true;
      this.term++;
    }
    for (const [, parts] of this.all()) {
      for (const p of parts) {
        p.replicas = p.replicas.filter((r) => r !== id);
        p.isr = p.isr.filter((r) => r !== id);
        if (p.leader === id) p.leader = p.isr.find((r) => this.up(r)) ?? null;
      }
    }
    this.epoch++;
    this.logIndex += 2;
  }

  drain(id: string): number {
    const n = this.nodes.find((x) => x.id === id);
    if (!n) throw new ApiErr(404, 'not_found', `node '${id}' not found`);
    if (n.status === 'down') throw new ApiErr(409, 'conflict', `node '${id}' is down; its partitions already failed over`);
    if (n.status === 'decommissioning') throw new ApiErr(409, 'conflict', `node '${id}' is being decommissioned; its leadership is already drained`);
    this.drained.set(id, Date.now() + 120_000);
    const moved = this.failover((l) => l === id, id);
    this.sim.addAudit('cluster.drain', id, { moved_leaderships: moved });
    return moved;
  }

  reassign(req: ReassignRequest): void {
    const parts = this.ensure(req.channel);
    if (!parts.length) throw new ApiErr(404, 'not_found', `channel '${req.channel}' not found`);
    const p = parts[req.partition];
    if (!p) throw new ApiErr(400, 'bad_request', `partition ${req.partition} does not exist`);
    if (!Array.isArray(req.replicas) || !req.replicas.length) throw new ApiErr(400, 'bad_request', "'replicas' must be a non-empty list of node ids");
    for (const r of req.replicas) if (!this.ids().includes(r)) throw new ApiErr(400, 'bad_request', `unknown node '${r}'`);
    if (new Set(req.replicas).size !== req.replicas.length) throw new ApiErr(400, 'bad_request', 'duplicate replica');
    const now = Date.now();
    p.replicas = [...req.replicas];
    p.isr = p.isr.filter((r) => p.replicas.includes(r));
    for (const r of p.replicas) if (!p.isr.includes(r)) this.pending.set(`${req.channel}/${req.partition}/${r}`, now + 8_000);
    const pref = p.replicas[0]!;
    if (p.isr.includes(pref) && this.up(pref)) p.leader = pref;
    else if (!p.leader || !p.replicas.includes(p.leader)) p.leader = p.isr.find((r) => this.up(r)) ?? null;
    p.epoch++;
    this.epoch++;
    this.sim.addAudit('cluster.reassign', `${req.channel}/${req.partition}`, { replicas: req.replicas });
  }
}
