/**
 * The FluxMQ demo-scenario simulation behind the mock server.
 *
 * Everything time-dependent that can be a closed-form function of wall-clock time
 * (traffic rates, publish latency, "normal" lag) is, so `/api/history` and the live
 * WebSocket values agree. Stateful bits (email backlog, stalls, DLQ) are integrated per tick
 * and recorded at 1 s resolution for the last hour.
 */
import type {
  Alert,
  AlertKind,
  AlertsState,
  AuditEntry,
  ChannelConfig,
  ChannelDetail,
  ChannelMode,
  ChannelSummary,
  ChannelsSummary,
  ClusterOverview,
  ConsumersState,
  DeadLetterView,
  DeliveryAttempt,
  EventKind,
  EventsFeed,
  FluxEvent,
  GroupState,
  Health,
  HistoryRange,
  HistorySeries,
  InstanceState,
  LatencySummary,
  MessageDetail,
  MessagePage,
  MessageQuery,
  MessageView,
  NodesState,
  NodeStatus,
  RetentionPolicy,
  RetryPolicy,
  StartFrom,
  TailMessage,
  TailState,
} from '../src/api/types';
import { HEALTH_RANK, HISTORY_BUCKET_MS, HISTORY_RANGE_MS, METRICS } from '../src/api/types';
import { genKindFor, genMessage, keyOf, keyPartition, toTail, type GenKind } from './messages';
import { DAILY_AVG, clamp, crc32, daily, fmtCount, fnv, mix, noise, prng, unit } from './util';

export class ApiErr extends Error {
  constructor(
    public status: number,
    public code: string,
    message: string,
    /** Byte offset of a filter DSL syntax error (`ApiError.position`). */
    public position?: number,
  ) {
    super(message);
  }
}

const env = process.env;
export const FAST = env.MOCK_FAST === '1';
export const EMPTY = env.MOCK_SCENARIO === 'empty';
/** `cluster`: 3 nodes with a node failure cycle; `single`: demo data on one node; `rebalance`
 *  (v0.4): 4 nodes, n4 just joined with no replicas and the controller rebalances onto it;
 *  default: healthy 3-node cluster. */
export const SCENARIO = env.MOCK_SCENARIO ?? 'default';
export const CLUSTERED = !EMPTY && SCENARIO !== 'single';
export const NODE_IDS = !CLUSTERED ? ['n1'] : SCENARIO === 'rebalance' ? ['n1', 'n2', 'n3', 'n4'] : SCENARIO === 'decommission' ? ['n1', 'n2', 'n3', 'n4', 'n5'] : ['n1', 'n2', 'n3'];
/** Nodes that host consumer connections. v0.5 `decommission`: n4 is about to be removed and n5 is a
 *  dead machine, so clients sit on n1–n3. */
const HOST_NODE_IDS = SCENARIO === 'decommission' ? NODE_IDS.slice(0, 3) : NODE_IDS;
export const VERSION = '0.5.0';

/** Cluster instance ids are `"{node}/{id}"`; single-node ids are bare. */
function instId(spec: { client: string; conn: number }): string {
  const base = `${spec.client}#${spec.conn}`;
  return CLUSTERED ? `${HOST_NODE_IDS[spec.conn % HOST_NODE_IDS.length]}/${base}` : base;
}
const CLUSTER_NAME = 'prod-eu-1';
const DAY = 86_400_000;
const GIB = 1024 ** 3;
const TIB = 1024 ** 4;

const DEFAULT_RETRY: RetryPolicy = { max_attempts: 5, backoff_ms: [1_000, 10_000, 60_000, 300_000], ack_timeout_ms: 30_000 };

// ───────────────────────────── scenario spec ─────────────────────────────

/** `retired`: an idle group (no consumers, lag frozen) — the Delete group action shows for it. */
type Behaviour = 'normal' | 'email' | 'stall' | 'retired';

interface InstSpec {
  client: string;
  conn: number;
  weight?: number;
  procMul?: number;
  cpu?: number | null;
  memMb?: number | null;
  lastError?: string;
  lastErrorAgoMs?: number;
}

interface GroupSpec {
  name: string;
  baseLag: number;
  procP99: number;
  nackRatio: number;
  behaviour?: Behaviour;
  reason?: string;
  instances: InstSpec[];
}

interface ChannelSpec {
  name: string;
  mode: ChannelMode;
  partitions: number;
  baseRate: number;
  avgSize: number;
  retentionSecs: number | null;
  maxBytes?: number | null;
  orderingKey: string | null;
  createdAgoMs: number;
  latMul?: number;
  groups: GroupSpec[];
}

const SPECS: ChannelSpec[] = [
  {
    name: 'orders.events',
    mode: 'stream',
    partitions: 8,
    baseRate: 45_000,
    avgSize: 520,
    retentionSecs: 3 * 86_400,
    orderingKey: 'order_id',
    createdAgoMs: 41 * DAY,
    groups: [
      {
        name: 'billing',
        baseLag: 1200,
        procP99: 12,
        nackRatio: 0.004,
        reason: 'PostgreSQL timeout',
        instances: [
          { client: 'billing-svc', conn: 11, cpu: 38, memMb: 612 },
          { client: 'billing-svc', conn: 12, cpu: 41, memMb: 640 },
          { client: 'billing-svc', conn: 13, cpu: 35, memMb: 598 },
        ],
      },
      {
        name: 'analytics',
        baseLag: 300,
        procP99: 4,
        nackRatio: 0.001,
        reason: 'Invalid schema',
        instances: [
          { client: 'analytics-consumer', conn: 21, cpu: null, memMb: null },
          { client: 'analytics-consumer', conn: 22, cpu: null, memMb: null },
        ],
      },
      { name: 'shipping', baseLag: 60, procP99: 25, nackRatio: 0.002, instances: [{ client: 'shipping-svc', conn: 31, cpu: 22, memMb: 410 }] },
    ],
  },
  {
    name: 'payments.events',
    mode: 'keyed',
    partitions: 12,
    baseRate: 18_500,
    avgSize: 380,
    retentionSecs: 7 * 86_400,
    orderingKey: 'customer_id',
    createdAgoMs: 41 * DAY,
    latMul: 1.25,
    groups: [
      {
        name: 'ledger',
        baseLag: 320,
        procP99: 18,
        nackRatio: 0.012,
        reason: 'PostgreSQL timeout',
        instances: [
          { client: 'ledger-writer', conn: 41, cpu: 52, memMb: 980 },
          { client: 'ledger-writer', conn: 42, cpu: 57, memMb: 1010, lastError: 'PostgreSQL timeout', lastErrorAgoMs: 95_000 },
          { client: 'ledger-writer', conn: 43, cpu: 49, memMb: 955 },
        ],
      },
      {
        name: 'fraud-check',
        baseLag: 800,
        procP99: 45,
        nackRatio: 0.002,
        instances: [
          { client: 'fraud-model', conn: 51, cpu: 71, memMb: 2300 },
          { client: 'fraud-model', conn: 52, cpu: 68, memMb: 2250 },
        ],
      },
    ],
  },
  {
    name: 'email',
    mode: 'queue',
    partitions: 1,
    baseRate: 1_250,
    avgSize: 290,
    retentionSecs: 86_400,
    orderingKey: null,
    createdAgoMs: 33 * DAY,
    groups: [
      {
        name: 'workers',
        baseLag: 1200,
        procP99: 170,
        nackRatio: 0.03,
        behaviour: 'email',
        reason: 'HTTP 503',
        instances: [
          { client: 'worker-01', conn: 61, cpu: 44, memMb: 310 },
          { client: 'worker-02', conn: 62, cpu: 47, memMb: 322 },
          { client: 'worker-03', conn: 63, weight: 0.38, procMul: 4.4, cpu: 93, memMb: 488, lastError: 'SMTP 421 rate limited', lastErrorAgoMs: 4_000 },
          { client: 'worker-04', conn: 64, cpu: 42, memMb: 305 },
        ],
      },
    ],
  },
  {
    name: 'notifications',
    mode: 'stream',
    partitions: 4,
    baseRate: 6_300,
    avgSize: 240,
    retentionSecs: 86_400,
    orderingKey: 'user_id',
    createdAgoMs: 27 * DAY,
    groups: [
      {
        name: 'push-gateway',
        baseLag: 40,
        procP99: 30,
        nackRatio: 0.003,
        reason: 'HTTP 503',
        instances: [
          { client: 'push-gateway', conn: 71, cpu: 28, memMb: 260 },
          { client: 'push-gateway', conn: 72, cpu: 31, memMb: 274 },
        ],
      },
    ],
  },
  {
    name: 'orders.priority',
    mode: 'stream',
    partitions: 3,
    baseRate: 260,
    avgSize: 540,
    retentionSecs: 7 * 86_400,
    orderingKey: 'order_id',
    createdAgoMs: 12 * DAY,
    groups: [
      { name: 'vip-desk', baseLag: 4, procP99: 40, nackRatio: 0.001, instances: [{ client: 'vip-desk', conn: 95, cpu: 8, memMb: 220 }] },
      { name: 'legacy-export', baseLag: 0, procP99: 5, nackRatio: 0, behaviour: 'retired', instances: [] },
    ],
  },
  {
    name: 'inventory.updates',
    mode: 'stream',
    partitions: 6,
    baseRate: 8_400,
    avgSize: 210,
    retentionSecs: 7 * 86_400,
    orderingKey: 'sku',
    createdAgoMs: 19 * DAY,
    groups: [
      { name: 'sync', baseLag: 200, procP99: 9, nackRatio: 0.001, behaviour: 'stall', reason: 'ack timeout', instances: [{ client: 'inventory-sync', conn: 7, cpu: 12, memMb: 190 }] },
      { name: 'search-indexer', baseLag: 150, procP99: 6, nackRatio: 0.0005, instances: [{ client: 'search-indexer', conn: 81, cpu: null, memMb: null }] },
    ],
  },
];

const AUDIT_SPEC: ChannelSpec = {
  name: 'audit.logs',
  mode: 'stream',
  partitions: 3,
  baseRate: 420,
  avgSize: 180,
  retentionSecs: 30 * 86_400,
  maxBytes: 500 * GIB,
  orderingKey: null,
  createdAgoMs: 0,
  groups: [],
};

// ───────────────────────────── runtime state ─────────────────────────────

interface SimInstance {
  spec: InstSpec;
  id: string;
  remote_addr: string;
  connected_at_ms: number;
  assigned: number[];
  prefetch: number;
  retries_total: number;
  last_error: string | null;
  last_error_at_ms: number | null;
  live: InstanceState | null;
}

interface SimGroup {
  spec: GroupSpec;
  behaviour: Behaviour;
  lag: number;
  instances: SimInstance[];
  emailPhase: 'grow' | 'drain';
  lastAckMs: number;
  dlqTimes: number[];
  retryPending: number;
  live: GroupState | null;
}

export interface SimChannel {
  config: ChannelConfig;
  kind: GenKind;
  seed: number;
  baseRate: number;
  avgSize: number;
  latMul: number;
  weights: number[];
  acc: number[];
  prevAcc: number[];
  anchors: [number, number][][];
  groups: SimGroup[];
  rate: number;
  overrides: Map<string, MessageView>;
  rr: number;
  tailBuf: TailMessage[];
  tailBatch: TailMessage[];
  tailShownRate: number;
  pubLat: LatencySummary;
  delLat: LatencySummary;
}

interface Series {
  ts: number[];
  v: number[];
}

const RECORD_MAX = 3600;
/** inventory.updates/sync stall cycle: stalls for STALL_MS every STALL_CYCLE_MS. */
const STALL_CYCLE_MS = 170_000;
const STALL_MS = 65_000;

export class Sim {
  readonly start = Date.now();
  readonly growthStart = this.start - 120_000;
  private now = this.start;
  private lastTick = this.start;
  private lastRecord = 0;
  private rand = prng(42);
  readonly channels = new Map<string, SimChannel>();
  private dlq: DeadLetterView[] = [];
  private dlqId = 1;
  private events: FluxEvent[] = [];
  private eventId = 1;
  private audit: AuditEntry[] = [];
  private alertsOpen = new Map<string, number>();
  private alerts: AlertsState = {};
  private nodes: NodesState = {};
  private series = new Map<string, Series>();
  private retries24h = 3184;
  private auditAt = this.start + (FAST ? 8_000 : 45_000);
  private auditConsumerAt = 0;
  private nextDlqAt = this.start + 9_000;
  private nextChurnAt = this.start + 70_000;
  private reconnectAt = 0;
  private connSeq = 100;
  /** Channels whose tail is subscribed by at least one session (set by the server). */
  tailChannels = new Set<string>();
  /** Cluster membership per node (set by the cluster model). */
  /** null = not a member (decommissioned / forgotten): dropped from the `nodes` topic. */
  nodeMembership: (id: string) => { status: NodeStatus; is_controller: boolean } | null = (id) => ({ status: 'up', is_controller: id === 'n1' });
  /** Extra alerts (cluster model). */
  clusterAlerts: () => Omit<Alert, 'since_ms' | 'possible_reason' | 'channel' | 'group'>[] = () => [];
  /** v0.4 TLS alerts (mock/tls.ts). */
  tlsAlerts: () => Omit<Alert, 'since_ms' | 'channel' | 'group'>[] = () => [];
  /** Called every tick after channels advanced (routing / cluster models). */
  onTick: ((now: number, dt: number) => void)[] = [];

  constructor() {
    if (!EMPTY) {
      for (const spec of SPECS) this.addChannel(spec, this.start - spec.createdAgoMs, spec.groups);
      this.seedDlq();
      this.findGroup('payments.events', 'ledger')?.dlqTimes.push(this.start - 4 * 60_000);
      this.seedEvents();
      this.alertsOpen.set('dlq_growth:payments.events/ledger', this.start - 9 * 60_000);
      this.alertsOpen.set(`slow_instance:email/workers/${instId({ client: 'worker-03', conn: 63 })}`, this.start - 14 * 60_000);
      this.alertsOpen.set('lag_growth:email/workers', this.growthStart);
      const email = this.channels.get('email')?.groups[0];
      if (email) email.lag = 1200 + 44 * 120;
    } else {
      this.pushEvent('channel_created', 'Cluster bootstrapped', CLUSTER_NAME, 'healthy', this.start - 60_000);
    }
    this.tick(this.start);
  }

  // ───────────── channel lifecycle ─────────────

  private addChannel(spec: ChannelSpec, createdAt: number, groups: GroupSpec[]): SimChannel {
    const P = spec.partitions;
    const seed = fnv(spec.name);
    const raw = Array.from({ length: P }, (_, p) => 1 + 0.15 * Math.sin(p * 1.7 + seed));
    const sum = raw.reduce((a, b) => a + b, 0);
    const weights = raw.map((w) => w / sum);
    const ageSec = Math.max(0, (this.now - createdAt) / 1000);
    const acc = weights.map((w) => Math.round(spec.baseRate * DAILY_AVG * w * ageSec));
    const config: ChannelConfig = {
      name: spec.name,
      mode: spec.mode,
      partitions: P,
      retention: { max_age_secs: spec.retentionSecs, max_bytes: spec.maxBytes ?? null },
      retry: { ...DEFAULT_RETRY, backoff_ms: [...DEFAULT_RETRY.backoff_ms] },
      dlq_enabled: true,
      ordering_key: spec.orderingKey,
      created_at_ms: createdAt,
    };
    const ch: SimChannel = {
      config,
      kind: genKindFor(spec.name),
      seed,
      baseRate: spec.baseRate,
      avgSize: spec.avgSize,
      latMul: spec.latMul ?? 1,
      weights,
      acc,
      prevAcc: [...acc],
      anchors: acc.map((a) => [[this.now, a]]),
      groups: [],
      rate: 0,
      overrides: new Map(),
      rr: 0,
      tailBuf: [],
      tailBatch: [],
      tailShownRate: 0,
      pubLat: { p50: 0, p95: 0, p99: 0, p999: 0, max: 0 },
      delLat: { p50: 0, p95: 0, p99: 0, p999: 0, max: 0 },
    };
    this.channels.set(spec.name, ch);
    for (const g of groups) this.addGroup(ch, g);
    return ch;
  }

  private addGroup(ch: SimChannel, spec: GroupSpec): SimGroup {
    const g: SimGroup = {
      spec,
      behaviour: spec.behaviour ?? 'normal',
      lag: spec.baseLag,
      instances: [],
      emailPhase: 'grow',
      lastAckMs: this.now,
      dlqTimes: [],
      retryPending: 0,
      live: null,
    };
    ch.groups.push(g);
    for (const i of spec.instances) this.addInstance(ch, g, i, this.now - (2 * 3_600_000 + unit(mix(i.conn, 1)) * 9 * DAY));
    return g;
  }

  private addInstance(ch: SimChannel, g: SimGroup, spec: InstSpec, connectedAt: number): SimInstance {
    const inst: SimInstance = {
      spec,
      id: instId(spec),
      remote_addr: `10.12.${mix(spec.conn, 2) % 32}.${10 + (mix(spec.conn, 3) % 200)}:${40000 + (mix(spec.conn, 4) % 20000)}`,
      connected_at_ms: Math.round(connectedAt),
      assigned: [],
      prefetch: ch.config.mode === 'queue' ? 32 : 256,
      retries_total: Math.round(spec.conn * 37 + unit(mix(spec.conn, 5)) * 400),
      last_error: spec.lastError ?? null,
      last_error_at_ms: spec.lastError ? this.now - (spec.lastErrorAgoMs ?? 60_000) : null,
      live: null,
    };
    g.instances.push(inst);
    this.rebalance(ch, g);
    return inst;
  }

  private rebalance(ch: SimChannel, g: SimGroup): void {
    const P = ch.config.partitions;
    if (ch.config.mode === 'queue') {
      for (const i of g.instances) i.assigned = Array.from({ length: P }, (_, p) => p);
      return;
    }
    g.instances.forEach((i) => (i.assigned = []));
    for (let p = 0; p < P; p++) g.instances[p % Math.max(1, g.instances.length)]?.assigned.push(p);
  }

  // ───────────── closed-form signals ─────────────

  private rateAt(ch: SimChannel, t: number, scale = 20_000): number {
    if (t < ch.config.created_at_ms) return 0;
    const fine = scale <= 20_000 ? 0.025 * noise(ch.seed + 1, t / 3_000) : 0;
    return Math.max(0, ch.baseRate * daily(t) * (1 + 0.06 * noise(ch.seed, t / scale) + fine));
  }

  private latAt(ch: SimChannel | null, t: number, scale = 15_000): LatencySummary {
    const seed = ch ? ch.seed : 7;
    const mul = ch ? ch.latMul : 1;
    const base = 0.9 * mul * (1 + 0.12 * noise(seed + 2, t / scale));
    const minute = Math.floor(t / 60_000);
    const r = unit(mix(seed, minute, 99));
    const spike = r < 0.05 ? 1 + (1.5 + r * 60) * Math.sin(Math.PI * ((t % 60_000) / 60_000)) : 1;
    const p99 = base * 3.1 * spike;
    return { p50: base, p95: base * 2.2 * (1 + (spike - 1) * 0.5), p99, p999: p99 * 2.2, max: p99 * 2.2 * 1.7 };
  }

  private stallPhase(t: number): number {
    return (((t - this.start - 20_000) % STALL_CYCLE_MS) + STALL_CYCLE_MS) % STALL_CYCLE_MS;
  }

  private targetLag(ch: SimChannel, g: SimGroup, t: number): number {
    if (g.behaviour === 'stall') {
      const ph = this.stallPhase(t);
      const r = this.rateAt(ch, t);
      if (ph < STALL_MS) return 200 + (r * ph) / 1000;
      return Math.max(200, r * (STALL_MS / 1000) - r * 0.8 * ((ph - STALL_MS) / 1000));
    }
    if (g.behaviour === 'email' && t < this.start) {
      if (t < this.growthStart) return 1200 * (1 + 0.1 * noise(ch.seed + 5, t / 30_000));
      return 1200 + 44 * ((t - this.growthStart) / 1000);
    }
    return g.spec.baseLag * (1 + 0.3 * noise(ch.seed + fnv(g.spec.name), t / 10_000));
  }

  // ───────────── offsets ↔ time ─────────────

  private high(ch: SimChannel, p: number): number {
    return Math.floor(ch.acc[p] ?? 0);
  }

  private low(ch: SimChannel, p: number): number {
    const high = this.high(ch, p);
    let low = 0;
    const age = ch.config.retention.max_age_secs;
    if (age != null) low = Math.max(0, Math.ceil(this.offsetAt(ch, p, this.now - age * 1000)));
    const maxBytes = ch.config.retention.max_bytes;
    if (maxBytes != null) low = Math.max(low, high - Math.floor(maxBytes / ch.config.partitions / ch.avgSize));
    if (ch.config.mode === 'queue') {
      const maxLag = Math.max(0, ...ch.groups.map((g) => Math.round(g.lag)));
      if (ch.groups.length > 0) low = Math.max(low, high - maxLag - 50);
    }
    return clamp(low, 0, high);
  }

  private perMs(ch: SimChannel, p: number, avg: boolean): number {
    const w = ch.weights[p] ?? 0;
    return Math.max(1e-6, ((avg ? ch.baseRate * DAILY_AVG : Math.max(ch.rate, 0.001)) * w) / 1000);
  }

  tsAt(ch: SimChannel, p: number, o: number): number {
    const a = ch.anchors[p];
    if (!a || a.length === 0) return this.now;
    const first = a[0]!;
    if (o <= first[1]) return Math.floor(first[0] - (first[1] - o) / this.perMs(ch, p, true));
    let lo = 0;
    let hi = a.length - 1;
    while (lo < hi) {
      const mid = (lo + hi + 1) >> 1;
      if (a[mid]![1] <= o) lo = mid;
      else hi = mid - 1;
    }
    const cur = a[lo]!;
    const next = a[lo + 1];
    if (!next) return Math.floor(Math.min(this.now, cur[0] + (o - cur[1]) / this.perMs(ch, p, false)));
    const span = next[1] - cur[1];
    return Math.floor(span <= 0 ? cur[0] : cur[0] + ((o - cur[1]) / span) * (next[0] - cur[0]));
  }

  offsetAt(ch: SimChannel, p: number, t: number): number {
    const a = ch.anchors[p];
    if (!a || a.length === 0) return 0;
    const first = a[0]!;
    if (t <= first[0]) return Math.max(0, first[1] - (first[0] - t) * this.perMs(ch, p, true));
    let lo = 0;
    let hi = a.length - 1;
    while (lo < hi) {
      const mid = (lo + hi + 1) >> 1;
      if (a[mid]![0] <= t) lo = mid;
      else hi = mid - 1;
    }
    const cur = a[lo]!;
    const next = a[lo + 1];
    if (!next) return Math.min(ch.acc[p] ?? 0, cur[1] + (t - cur[0]) * this.perMs(ch, p, false));
    const span = next[0] - cur[0];
    return span <= 0 ? cur[1] : cur[1] + ((t - cur[0]) / span) * (next[1] - cur[1]);
  }

  // ───────────── tick ─────────────

  tick(now: number): void {
    this.now = now;
    const dt = Math.max(0.001, (now - this.lastTick) / 1000);
    this.lastTick = now;

    if (!EMPTY && !this.channels.has(AUDIT_SPEC.name) && now >= this.auditAt) {
      this.addChannel(AUDIT_SPEC, now, []);
      this.pushEvent('channel_created', 'Channel created', AUDIT_SPEC.name, 'healthy');
      this.auditConsumerAt = now + 6_000;
    }
    if (this.auditConsumerAt && now >= this.auditConsumerAt) {
      this.auditConsumerAt = 0;
      const ch = this.channels.get(AUDIT_SPEC.name);
      if (ch) {
        const spec: GroupSpec = { name: 'archiver', baseLag: 20, procP99: 3, nackRatio: 0, instances: [] };
        const g = this.addGroup(ch, spec);
        this.addInstance(ch, g, { client: 'archiver', conn: 91, cpu: 9, memMb: 140 }, now);
        this.pushEvent('consumer_connected', 'Consumer connected', 'archiver#91 · audit.logs', 'healthy');
      }
    }

    for (const ch of this.channels.values()) this.tickChannel(ch, now, dt);
    if (!EMPTY) {
      this.tickDlq(now);
      this.tickChurn(now);
    }
    this.tickNodes(now);
    for (const f of this.onTick) f(now, dt);
    this.computeAlerts(now);
    if (now - this.lastRecord >= 1000) {
      this.lastRecord = now;
      this.record(now);
    }
  }

  private tickChannel(ch: SimChannel, now: number, dt: number): void {
    const P = ch.config.partitions;
    ch.rate = this.rateAt(ch, now);
    ch.prevAcc = [...ch.acc];
    for (let p = 0; p < P; p++) ch.acc[p] = (ch.acc[p] ?? 0) + ch.rate * (ch.weights[p] ?? 0) * dt;
    if (now - (ch.anchors[0]?.at(-1)?.[0] ?? 0) >= 1000) {
      for (let p = 0; p < P; p++) {
        const a = ch.anchors[p]!;
        a.push([now, ch.acc[p] ?? 0]);
        if (a.length > RECORD_MAX) a.shift();
      }
    }
    ch.pubLat = this.latAt(ch, now);

    for (const g of ch.groups) this.tickGroup(ch, g, now, dt);

    const maxLag = Math.max(0, ...ch.groups.map((g) => g.lag));
    const consume = Math.max(1, ch.rate);
    const qd = (maxLag / consume) * 1000;
    const d50 = 1.4 + Math.min(qd * 0.5, 120_000);
    ch.delLat = { p50: d50, p95: d50 * 1.9 + 2, p99: d50 * 2.6 + 4, p999: d50 * 3.4 + 9, max: d50 * 4.5 + 20 };

    if (this.tailChannels.has(ch.config.name)) this.sampleTail(ch, dt);
  }

  private tickGroup(ch: SimChannel, g: SimGroup, now: number, dt: number): void {
    const rate = ch.rate;
    let consumption = rate;
    const n = g.instances.length;
    if (n === 0) {
      consumption = 0;
      if (g.behaviour !== 'retired') g.lag += rate * dt;
    } else if (g.behaviour === 'email') {
      if (g.emailPhase === 'grow' && g.lag > 48_000) {
        g.emailPhase = 'drain';
        const inst = this.addInstance(ch, g, { client: 'worker-05', conn: 65, cpu: 40, memMb: 300 }, now);
        this.pushEvent('consumer_connected', 'Consumer connected', `${inst.id} · email`, 'healthy');
      } else if (g.emailPhase === 'drain' && g.lag < 1500) {
        g.emailPhase = 'grow';
        const idx = g.instances.findIndex((i) => i.spec.client === 'worker-05');
        if (idx >= 0) {
          const [gone] = g.instances.splice(idx, 1);
          this.rebalance(ch, g);
          this.pushEvent('consumer_disconnected', 'Consumer disconnected', `${gone?.id ?? 'worker-05'} · email`, 'healthy');
        }
      }
      consumption = rate * (g.emailPhase === 'grow' ? 0.968 : 1.32);
      g.lag = Math.max(0, g.lag + (rate - consumption) * dt);
      if (consumption > 0) g.lastAckMs = now;
    } else if (g.behaviour === 'stall') {
      const stalled = this.stallPhase(now) < STALL_MS;
      if (stalled) {
        consumption = 0;
        g.lag += rate * dt;
      } else {
        const catchUp = Math.min(Math.max(0, g.lag - 200), rate * 0.8 * dt);
        g.lag = Math.max(200, g.lag - catchUp);
        consumption = rate + catchUp / dt;
        g.lastAckMs = now;
      }
    } else {
      const target = this.targetLag(ch, g, now);
      if (g.lag > target * 1.5 + 1000) {
        const catchUp = Math.min(g.lag - target, rate * 0.6 * dt + 50);
        g.lag -= catchUp;
        consumption = rate + catchUp / dt;
      } else {
        g.lag = target;
      }
      g.lastAckMs = now;
    }

    const nackRatio = g.spec.nackRatio;
    const ack = consumption;
    const nack = consumption * nackRatio;
    const egress = ack + nack;
    g.retryPending = Math.max(0, g.retryPending * 0.98 + nack * 4 * dt * 0.1);
    this.retries24h += nack * dt;

    const weights = g.instances.map((i) => (g.behaviour === 'stall' && consumption === 0 ? 0 : (i.spec.weight ?? 1)));
    const sumW = weights.reduce((a, b) => a + b, 0) || 1;
    const P = ch.config.partitions;
    const base = g.spec.procP99;
    const instP99s: number[] = [];
    const lives: InstanceState[] = g.instances.map((inst, idx) => {
      const share = (weights[idx] ?? 0) / sumW;
      const mul = inst.spec.procMul ?? 1;
      const p99 = base * mul * (1 + 0.12 * noise(inst.spec.conn, now / 4_000));
      instP99s.push(p99);
      const iEgress = egress * share;
      const iNackRatio = nackRatio * (inst.spec.procMul ? 3 : 1);
      const iAck = iEgress / (1 + iNackRatio);
      const iNack = iEgress - iAck;
      inst.retries_total += iNack * dt;
      if (inst.spec.lastError && inst.spec.procMul && unit(mix(now, inst.spec.conn)) < 0.06) inst.last_error_at_ms = now;
      const lag =
        ch.config.mode === 'queue' ? g.lag / Math.max(1, n) : g.lag * (inst.assigned.length / Math.max(1, P));
      const stalled = g.behaviour === 'stall' && consumption === 0;
      const inFlight = stalled ? inst.prefetch : Math.min(inst.prefetch, Math.round(iEgress * (p99 / 1000) * 0.6));
      const cpu = inst.spec.cpu == null ? null : clamp(inst.spec.cpu * (1 + 0.12 * noise(inst.spec.conn + 9, now / 5_000)) * (stalled ? 0.15 : 1), 0, 100);
      const mem = inst.spec.memMb == null ? null : Math.round(inst.spec.memMb * 1024 * 1024 * (1 + 0.03 * noise(inst.spec.conn + 3, now / 30_000)));
      return {
        id: inst.id,
        client_name: inst.spec.client,
        remote_addr: inst.remote_addr,
        connected_at_ms: inst.connected_at_ms,
        health: 'healthy' as Health,
        egress_rate: iEgress,
        ack_rate: iAck,
        nack_rate: iNack,
        in_flight: inFlight,
        prefetch: inst.prefetch,
        lag: Math.round(lag),
        assigned_partitions: [...inst.assigned],
        processing_p99_ms: p99,
        retries_total: Math.round(inst.retries_total),
        last_error: inst.last_error,
        last_error_at_ms: inst.last_error_at_ms,
        cpu_percent: cpu == null ? null : Math.round(cpu * 10) / 10,
        memory_bytes: mem,
      };
    });
    const sorted = [...instP99s].sort((a, b) => a - b);
    const medianP99 = sorted[Math.floor(sorted.length / 2)] ?? base;
    const acks = lives.map((l) => l.ack_rate).sort((a, b) => a - b);
    const medianAck = acks[Math.floor(acks.length / 2)] ?? 0;
    for (const l of lives) {
      const slow = (lives.length >= 3 && l.ack_rate < 0.5 * medianAck) || l.processing_p99_ms > 3 * medianP99;
      const stalled = g.behaviour === 'stall' && consumption === 0 && now - g.lastAckMs > 5_000;
      l.health = slow || stalled ? 'warning' : 'healthy';
    }

    g.dlqTimes = g.dlqTimes.filter((t) => now - t < 300_000);
    let health: Health = 'healthy';
    if (g.lag > 0 && n === 0 && g.lag > 100) health = 'critical';
    else if (g.behaviour === 'stall' && now - g.lastAckMs >= 30_000 && g.lag > 0) health = 'critical';
    else if (g.behaviour === 'email' && g.lag >= 12_000 && g.emailPhase === 'grow') health = 'critical';
    else if (g.behaviour === 'email' && g.lag > 1000 && g.emailPhase === 'grow') health = 'warning';
    else if (nackRatio > 0.05 || g.dlqTimes.length > 0) health = 'warning';

    const p99 = g.behaviour === 'email' ? medianP99 * 1.9 : medianP99 * 1.05;
    const dlqCount = this.dlqCount(ch.config.name, g.spec.name);
    g.live = {
      group: g.spec.name,
      health,
      lag: Math.round(g.lag),
      egress_rate: egress,
      ack_rate: ack,
      nack_rate: nack,
      in_flight: lives.reduce((s, l) => s + l.in_flight, 0),
      retry_pending: Math.round(g.retryPending),
      dlq: dlqCount,
      processing_latency: { p50: p99 * 0.32, p95: p99 * 0.74, p99, p999: p99 * 2.1, max: p99 * 3.8 },
      instances: Object.fromEntries(lives.map((l) => [l.id, l])),
    };
  }

  private sampleTail(ch: SimChannel, dt: number): void {
    const P = ch.config.partitions;
    const produced = ch.acc.reduce((s, a, p) => s + Math.floor(a) - Math.floor(ch.prevAcc[p] ?? a), 0);
    const want = Math.min(50, produced);
    const seen = new Set<string>();
    const batch: TailMessage[] = [];
    for (let i = 0; i < want * 2 && batch.length < want; i++) {
      let r = this.rand();
      let p = 0;
      for (; p < P - 1; p++) {
        r -= ch.weights[p] ?? 0;
        if (r <= 0) break;
      }
      const lo = Math.floor(ch.prevAcc[p] ?? 0);
      const hi = Math.floor(ch.acc[p] ?? 0);
      if (hi <= lo) continue;
      const o = lo + Math.floor(this.rand() * (hi - lo));
      const k = `${p}:${o}`;
      if (seen.has(k)) continue;
      seen.add(k);
      const m = this.getMessageUnchecked(ch, p, o);
      batch.push(toTail(m));
    }
    batch.sort((a, b) => a.timestamp_ms - b.timestamp_ms);
    ch.tailBatch = batch;
    ch.tailShownRate = batch.length / dt;
    ch.tailBuf.push(...batch);
    if (ch.tailBuf.length > 200) ch.tailBuf.splice(0, ch.tailBuf.length - 200);
  }

  /** Fill the tail buffer from the last ~2 s so a fresh subscriber isn't staring at nothing. */
  private primeTail(ch: SimChannel): void {
    const P = ch.config.partitions;
    const msgs: TailMessage[] = [];
    const per = Math.max(1, Math.ceil(100 / P));
    for (let p = 0; p < P; p++) {
      const high = this.high(ch, p);
      const span = Math.max(1, Math.floor(ch.rate * (ch.weights[p] ?? 0) * 2));
      const low = Math.max(this.low(ch, p), high - span);
      if (high <= low) continue;
      for (let i = 0; i < per; i++) {
        const o = low + Math.floor(((i + 0.5) / per) * (high - low));
        msgs.push(toTail(this.getMessageUnchecked(ch, p, o)));
      }
    }
    msgs.sort((a, b) => a.timestamp_ms - b.timestamp_ms);
    ch.tailBuf = msgs.slice(-200);
    if (ch.tailShownRate === 0) ch.tailShownRate = Math.min(200, ch.rate);
  }

  private tickDlq(now: number): void {
    if (now < this.nextDlqAt) return;
    this.nextDlqAt = now + 15_000 + this.rand() * 15_000;
    const n = 1 + Math.floor(this.rand() * 6);
    this.addDeadLetters('payments.events', 'ledger', 'PostgreSQL timeout', n, now);
    this.pushEvent('dead_lettered', 'Messages dead-lettered', `payments.events (${n} messages)`, 'warning');
    const ledger = this.findGroup('payments.events', 'ledger');
    const writer = ledger?.instances[1];
    if (writer) {
      writer.last_error = 'PostgreSQL timeout';
      writer.last_error_at_ms = now;
    }
    if (this.rand() < 0.15) {
      const m = 1 + Math.floor(this.rand() * 3);
      this.addDeadLetters('email', 'workers', 'HTTP 503', m, now);
      this.pushEvent('dead_lettered', 'Messages dead-lettered', `email (${m} messages)`, 'warning');
    }
  }

  private tickChurn(now: number): void {
    const g = this.findGroup('notifications', 'push-gateway');
    const ch = this.channels.get('notifications');
    if (!g || !ch) return;
    if (this.reconnectAt && now >= this.reconnectAt) {
      this.reconnectAt = 0;
      const inst = this.addInstance(ch, g, { client: 'push-gateway', conn: ++this.connSeq, cpu: 30, memMb: 268 }, now);
      this.pushEvent('consumer_connected', 'Consumer connected', `${inst.id} · notifications`, 'healthy');
    } else if (!this.reconnectAt && now >= this.nextChurnAt && g.instances.length > 1) {
      this.nextChurnAt = now + 80_000 + this.rand() * 40_000;
      this.reconnectAt = now + 5_000;
      const gone = g.instances.pop();
      this.rebalance(ch, g);
      if (gone) this.pushEvent('consumer_disconnected', 'Consumer disconnected', `${gone.id} · notifications`, 'healthy');
    }
  }

  private tickNodes(now: number): void {
    const conns = [...this.channels.values()].reduce((s, c) => s + c.groups.reduce((a, g) => a + g.instances.length, 0), 0);
    const specs = EMPTY
      ? [{ id: 'n1', cpu: 6, mem: 11, disk: 0.04, fsync: 1.2 }]
      : !CLUSTERED
        ? [{ id: 'n1', cpu: 58, mem: 47, disk: 0.61, fsync: 2.1 }]
        : [
            { id: 'n1', cpu: 46, mem: 41, disk: 0.52, fsync: 1.9 },
            { id: 'n2', cpu: 39, mem: 38, disk: 0.47, fsync: 1.7 },
            { id: 'n3', cpu: 43, mem: 44, disk: 0.78, fsync: 3.6 },
            ...(NODE_IDS.includes('n4') ? [SCENARIO === 'decommission' ? { id: 'n4', cpu: 31, mem: 36, disk: 0.44, fsync: 1.6 } : { id: 'n4', cpu: 12, mem: 9, disk: 0.04, fsync: 1.2 }] : []),
            ...(NODE_IDS.includes('n5') ? [{ id: 'n5', cpu: 22, mem: 28, disk: 0.12, fsync: 1.4 }] : []),
          ];
    const out: NodesState = {};
    specs.forEach((s, i) => {
      const m = this.nodeMembership(s.id);
      if (!m) return; // left the cluster
      const prev = this.nodes[s.id];
      if (m.status === 'down' && prev) {
        // Unreachable peer: kept with its last known stats, marked down / critical.
        out[s.id] = { ...prev, status: 'down', health: 'critical', is_controller: m.is_controller, connections: 0 };
        return;
      }
      const seed = fnv(s.id);
      const diskTotal = 8 * TIB;
      const diskUsed = Math.round(diskTotal * (s.disk + ((now - this.start) / 3_600_000) * 0.002));
      const fsync = s.fsync * (1 + 0.2 * noise(seed, now / 3_000));
      let health: Health = 'healthy';
      if (diskUsed / diskTotal > 0.9) health = 'critical';
      else if (diskUsed / diskTotal > 0.8 || fsync > 50) health = 'warning';
      out[s.id] = {
        id: s.id,
        address: `10.12.0.${11 + i}:7420`,
        health,
        version: VERSION,
        uptime_secs: Math.floor((now - this.start) / 1000) + (s.id === 'n4' && SCENARIO === 'rebalance' ? 95 : 1_209_600 - i * 3_600),
        cpu_percent: Math.round(s.cpu * (1 + 0.15 * noise(seed + 1, now / 4_000)) * 10) / 10,
        memory_bytes: Math.round(s.mem * GIB * (1 + 0.02 * noise(seed + 2, now / 20_000))),
        memory_total_bytes: 64 * GIB,
        disk_used_bytes: diskUsed,
        disk_total_bytes: diskTotal,
        disk_latency_p99_ms: Math.round(fsync * 100) / 100,
        connections: m.status === 'joining' ? 3 : Math.round(conns / specs.length + 14 + (i === 0 ? 3 : 0)),
        status: m.status,
        is_controller: m.is_controller,
      };
      if (m.status === 'joining' && out[s.id]!.health === 'healthy') out[s.id]!.health = 'warning';
    });
    this.nodes = out;
  }

  // ───────────── alerts & events ─────────────

  private computeAlerts(now: number): void {
    const next: AlertsState = {};
    const open = (
      id: string,
      kind: AlertKind,
      severity: Health,
      title: string,
      detail: string,
      possible_reason: string | null,
      link: string | null,
      channel: string | null,
      group: string | null,
    ) => {
      if (!this.alertsOpen.has(id)) {
        this.alertsOpen.set(id, now);
      }
      const since = this.alertsOpen.get(id) ?? now;
      next[id] = { id, severity, kind, title, detail, possible_reason, link, channel, group, since_ms: since };
    };

    for (const ch of this.channels.values()) {
      const name = ch.config.name;
      for (const g of ch.groups) {
        const gl = g.live;
        if (!gl) continue;
        const gname = g.spec.name;
        if (g.behaviour === 'email' && g.emailPhase === 'grow' && g.lag >= 5_000) {
          const ago = this.seriesAt(`channel.${name}.lag`, now - 360_000) ?? this.targetLag(ch, g, now - 360_000);
          open(
            `lag_growth:${name}/${gname}`,
            'lag_growth',
            gl.health === 'critical' ? 'critical' : 'warning',
            'Email queue is growing rapidly',
            `Lag increased from ${fmtCount(ago)} → ${fmtCount(g.lag)} in 6 min`,
            'Consumers processing slower than producers.',
            `/channels/${name}/consumers`,
            name,
            gname,
          );
        }
        if (g.instances.length === 0 && g.lag > 100) {
          open(`no_consumers:${name}/${gname}`, 'no_consumers', 'critical', `No consumers on ${name}`, `Group ${gname} has ${fmtCount(g.lag)} messages waiting and no instances`, 'All instances disconnected.', `/channels/${name}/consumers`, name, gname);
        }
        if (g.behaviour === 'stall' && gl.health === 'critical') {
          const secs = Math.round((now - g.lastAckMs) / 1000);
          open(
            `consumer_stalled:${name}/${gname}`,
            'consumer_stalled',
            'critical',
            `${name} consumer stalled`,
            `No acks from group ${gname} for ${secs} s while lag is ${fmtCount(g.lag)}`,
            `The only instance (${g.instances[0]?.id ?? 'n/a'}) stopped acknowledging — possibly blocked on a downstream lock or a long GC pause.`,
            `/channels/${name}/consumers`,
            name,
            gname,
          );
        }
        for (const inst of Object.values(gl.instances)) {
          if (inst.health !== 'warning' || g.behaviour === 'stall') continue;
          const ratio = inst.processing_p99_ms / Math.max(0.001, g.spec.procP99);
          open(
            `slow_instance:${name}/${gname}/${inst.id}`,
            'slow_instance',
            'warning',
            `${inst.client_name} is processing ${ratio.toFixed(1)}× slower than peers`,
            `Processing p99 ${Math.round(inst.processing_p99_ms)} ms vs ${Math.round(g.spec.procP99)} ms for the rest of ${gname}`,
            inst.last_error ? `Last error: ${inst.last_error}. The downstream provider may be throttling this instance.` : 'Instance is slower than its peers.',
            `/channels/${name}/consumers`,
            name,
            gname,
          );
        }
        if (g.dlqTimes.length > 0) {
          open(
            `dlq_growth:${name}/${gname}`,
            'dlq_growth',
            'warning',
            `Dead letters growing on ${name}`,
            `+${g.dlqTimes.length} dead-letter batch${g.dlqTimes.length === 1 ? '' : 'es'} in the last 5 min (${g.spec.reason ?? 'unknown'})`,
            g.spec.reason === 'PostgreSQL timeout'
              ? 'Consumer cannot reach PostgreSQL in time — check the connection pool and slow queries.'
              : 'Downstream dependency is failing; messages exhaust their retry budget.',
            '/dlq',
            name,
            gname,
          );
        }
      }
    }
    for (const a of this.clusterAlerts()) open(a.id, a.kind, a.severity, a.title, a.detail, a.kind === 'node_down' ? 'The node stopped answering heartbeats: crashed, partitioned from the network, or overloaded.' : null, a.link, null, null);
    for (const a of this.tlsAlerts()) open(a.id, a.kind, a.severity, a.title, a.detail, a.possible_reason, a.link, null, null);
    for (const node of Object.values(this.nodes)) {
      if (node.status === 'down') continue;
      const pct = node.disk_used_bytes / node.disk_total_bytes;
      if (pct > 0.8) {
        open(`storage_nearly_full:${node.id}`, 'storage_nearly_full', pct > 0.9 ? 'critical' : 'warning', `Disk ${Math.round(pct * 100)}% full on ${node.id}`, `${node.address}`, 'Retention is too long for the current ingress.', '/nodes', null, null);
      }
    }

    for (const [id, a] of Object.entries(this.alerts)) {
      if (!next[id]) {
        this.alertsOpen.delete(id);
        this.pushEvent('alert_resolved', `Resolved: ${a.title}`, subjectOf(a), 'healthy');
      }
    }
    for (const [id, a] of Object.entries(next)) {
      if (!this.alerts[id] && (this.alertsOpen.get(id) ?? 0) >= this.start) {
        this.pushEvent('alert_opened', a.title, subjectOf(a), a.severity);
      }
    }
    // Drop `since` bookkeeping for preset ids that never opened.
    for (const id of [...this.alertsOpen.keys()]) if (!next[id]) this.alertsOpen.delete(id);
    this.alerts = next;
  }

  pushEvent(kind: EventKind, title: string, subject: string, severity: Health, ts = this.now): void {
    this.events.unshift({ id: this.eventId++, ts: Math.round(ts), kind, title, subject, severity });
    this.events.sort((a, b) => b.ts - a.ts);
    if (this.events.length > 100) this.events.length = 100;
  }

  private seedEvents(): void {
    const s = this.start;
    const seed: [number, EventKind, string, string, Health][] = [
      [s - 6 * 3_600_000, 'channel_updated', 'Retention changed', 'orders.events', 'healthy'],
      [s - 3 * 3_600_000, 'dlq_retried', 'Dead letters retried', 'orders.events (42 messages)', 'healthy'],
      [s - 2 * 3_600_000, 'consumer_connected', 'Consumer connected', 'fraud-model#52 · payments.events', 'healthy'],
      [s - 95 * 60_000, 'alert_opened', 'High publish latency', 'payments.events', 'warning'],
      [s - 88 * 60_000, 'alert_resolved', 'Resolved: High publish latency', 'payments.events', 'healthy'],
      [s - 61 * 60_000, 'messages_replayed', 'Messages replayed', 'orders.events (120 messages)', 'healthy'],
      [s - 47 * 60_000, 'consumer_disconnected', 'Consumer disconnected', 'billing-svc#10 · orders.events', 'healthy'],
      [s - 46 * 60_000, 'consumer_connected', 'Consumer connected', 'billing-svc#13 · orders.events', 'healthy'],
      [s - 31 * 60_000, 'dead_lettered', 'Messages dead-lettered', 'orders.events (18 messages)', 'warning'],
      [s - 14 * 60_000, 'alert_opened', 'worker-03 is processing 4.4× slower than peers', 'email / workers', 'warning'],
      [s - 9 * 60_000, 'alert_opened', 'Dead letters growing on payments.events', 'payments.events / ledger', 'warning'],
      [s - 4 * 60_000, 'dead_lettered', 'Messages dead-lettered', 'payments.events (24 messages)', 'warning'],
      [s - 2 * 60_000, 'alert_opened', 'Email queue is growing rapidly', 'email / workers', 'warning'],
    ];
    for (const [ts, kind, title, subject, sev] of seed) this.pushEvent(kind, title, subject, sev, ts);
  }

  // ───────────── dead letters ─────────────

  dlqCount(channel: string, group?: string): number {
    let n = 0;
    for (const d of this.dlq) if (d.channel === channel && (group === undefined || d.group === group)) n++;
    return n;
  }

  private findGroup(channel: string, group: string): SimGroup | undefined {
    return this.channels.get(channel)?.groups.find((g) => g.spec.name === group);
  }

  private addDeadLetters(channel: string, group: string, reason: string, n: number, deadAt: number, track = true): void {
    const ch = this.channels.get(channel);
    if (!ch) return;
    const g = this.findGroup(channel, group);
    for (let i = 0; i < n; i++) {
      const at = Math.round(deadAt - i * 37);
      const retry = ch.config.retry;
      const totalBackoff = retry.backoff_ms.slice(0, retry.max_attempts - 1).reduce((a, b) => a + b, 0) + retry.max_attempts * 40;
      const publishedAt = at - totalBackoff - (reason === 'ack timeout' ? retry.ack_timeout_ms * retry.max_attempts : 0);
      const p = Math.floor(this.rand() * ch.config.partitions);
      const o = Math.max(0, Math.floor(this.offsetAt(ch, p, publishedAt)));
      const msg = this.getMessageUnchecked(ch, p, o);
      const consumers = g?.instances.map((x) => x.id) ?? [`${group}#0`];
      const attempts = buildFailedAttempts(msg.timestamp_ms, retry, reason, consumers, mix(p, o));
      const last = attempts.at(-1);
      this.dlq.push({
        id: this.dlqId++,
        channel,
        group,
        reason,
        dead_at_ms: Math.max(at, last?.finished_at_ms ?? at),
        message: msg,
        attempts,
      });
    }
    this.dlq.sort((a, b) => b.id - a.id);
    if (track && g) g.dlqTimes.push(deadAt);
  }

  private seedDlq(): void {
    const s = this.start;
    const plan: [string, string, string, number, number][] = [
      ['orders.events', 'analytics', 'Invalid schema', 31, 6 * 3_600_000],
      ['inventory.updates', 'sync', 'ack timeout', 19, 5 * 3_600_000],
      ['notifications', 'push-gateway', 'HTTP 503', 22, 4 * 3_600_000],
      ['orders.events', 'billing', 'PostgreSQL timeout', 18, 3 * 3_600_000],
      ['email', 'workers', 'HTTP 503', 12, 2 * 3_600_000],
      ['payments.events', 'ledger', 'PostgreSQL timeout', 38, 3_600_000],
    ];
    // Generate interleaved by time so ids increase with dead_at.
    const items: [string, string, string, number][] = [];
    for (const [ch, g, reason, n, span] of plan) {
      for (let i = 0; i < n; i++) items.push([ch, g, reason, s - 60_000 - unit(mix(fnv(ch + g), i)) * span]);
    }
    items.sort((a, b) => a[3] - b[3]);
    for (const [ch, g, reason, at] of items) this.addDeadLetters(ch, g, reason, 1, at, false);
  }

  dlqSummary(): { total: number; groups: { reason: string; count: number; channels: Record<string, number>; first_ms: number; last_ms: number }[] } {
    const by = new Map<string, { reason: string; count: number; channels: Record<string, number>; first_ms: number; last_ms: number }>();
    for (const d of this.dlq) {
      let g = by.get(d.reason);
      if (!g) {
        g = { reason: d.reason, count: 0, channels: {}, first_ms: d.dead_at_ms, last_ms: d.dead_at_ms };
        by.set(d.reason, g);
      }
      g.count++;
      g.channels[d.channel] = (g.channels[d.channel] ?? 0) + 1;
      g.first_ms = Math.min(g.first_ms, d.dead_at_ms);
      g.last_ms = Math.max(g.last_ms, d.dead_at_ms);
    }
    return { total: this.dlq.length, groups: [...by.values()].sort((a, b) => b.last_ms - a.last_ms) };
  }

  dlqList(q: { channel?: string; reason?: string; limit?: number; before?: number }, forExport = false): { entries: DeadLetterView[]; total: number } {
    const filtered = this.dlq.filter((d) => (q.channel == null || d.channel === q.channel) && (q.reason == null || d.reason === q.reason));
    const limit = forExport ? (q.limit ?? Number.MAX_SAFE_INTEGER) : clamp(q.limit ?? 50, 1, 1000);
    const page = filtered.filter((d) => q.before == null || d.id < q.before).slice(0, limit);
    return { entries: page, total: filtered.length };
  }

  dlqAction(kind: 'retry' | 'discard', req: { ids?: [string, number][] | null; reason?: string | null; channel?: string | null; all?: boolean | null }): number {
    const selectors = [req.ids != null, req.reason != null, req.all === true].filter(Boolean).length;
    if (selectors !== 1) throw new ApiErr(400, 'bad_request', 'exactly one of `ids`, `reason`, `all` must be set (`channel` optionally narrows `reason`/`all`)');
    let match: (d: DeadLetterView) => boolean;
    if (req.ids != null) {
      if (!Array.isArray(req.ids) || !req.ids.every((x) => Array.isArray(x) && typeof x[0] === 'string' && typeof x[1] === 'number')) {
        throw new ApiErr(400, 'bad_request', '`ids` must be an array of [channel, id] pairs');
      }
      const set = new Set(req.ids.map(([c, i]) => `${c}\u0000${i}`));
      match = (d) => set.has(`${d.channel}\u0000${d.id}`);
    } else {
      match = (d) => (req.reason == null || d.reason === req.reason) && (req.channel == null || d.channel === req.channel);
    }
    const hit = this.dlq.filter(match);
    if (hit.length === 0) return 0;
    this.dlq = this.dlq.filter((d) => !match(d));
    const byChannel = new Map<string, number>();
    for (const d of hit) {
      byChannel.set(d.channel, (byChannel.get(d.channel) ?? 0) + 1);
      if (kind === 'retry') {
        const g = this.findGroup(d.channel, d.group);
        if (g) g.retryPending += 1;
      }
    }
    for (const [channel, n] of byChannel) {
      this.pushEvent(
        kind === 'retry' ? 'dlq_retried' : 'dlq_discarded',
        kind === 'retry' ? 'Dead letters retried' : 'Dead letters discarded',
        `${channel} (${n} message${n === 1 ? '' : 's'})`,
        'healthy',
      );
    }
    this.addAudit(`dlq.${kind}`, req.reason ?? (req.all ? 'all' : 'ids'), { affected: hit.length, ...req });
    return hit.length;
  }

  // ───────────── messages ─────────────

  channel(name: string): SimChannel {
    const ch = this.channels.get(name);
    if (!ch) throw new ApiErr(404, 'not_found', `channel '${name}' not found`);
    return ch;
  }

  private getMessageUnchecked(ch: SimChannel, p: number, o: number): MessageView {
    const ov = ch.overrides.get(`${p}:${o}`);
    if (ov) return ov;
    return genMessage(ch.kind, ch.config.name, ch.config.partitions, p, o, this.tsAt(ch, p, o));
  }

  private keyAt(ch: SimChannel, p: number, o: number): string | null {
    const ov = ch.overrides.get(`${p}:${o}`);
    if (ov) return ov.key;
    return keyOf(ch.kind, ch.config.name, ch.config.partitions, p, o);
  }

  /** Log end offset of a partition (cluster model / backtest). */
  partitionHigh(ch: SimChannel, p: number): number {
    return this.high(ch, p);
  }

  partitionLow(ch: SimChannel, p: number): number {
    return this.low(ch, p);
  }

  /** Materialize a stored message without range checks (offset must be in range). */
  messageAt(ch: SimChannel, p: number, o: number): MessageView {
    return this.getMessageUnchecked(ch, p, o);
  }

  /** Add (or remove) the system `$router` group on a channel. */
  setRouterGroup(name: string, on: boolean): void {
    const ch = this.channels.get(name);
    if (!ch) return;
    const has = ch.groups.find((g) => g.spec.name === '$router');
    if (on && !has) {
      const g = this.addGroup(ch, { name: '$router', baseLag: 30, procP99: 2, nackRatio: 0.0002, reason: 'target unavailable', instances: [] });
      HOST_NODE_IDS.forEach((_, i) => this.addInstance(ch, g, { client: 'fluxmq-router', conn: 900 + i, cpu: null, memMb: null }, this.now - 3_600_000));
    } else if (!on && has) {
      ch.groups = ch.groups.filter((g) => g !== has);
    }
  }

  /** Add (or remove) a system group such as `$webhook:<name>` (one broker-side instance per node). */
  setSystemGroup(channel: string, group: string, on: boolean, opts: { client: string; nackRatio?: number; reason?: string; procP99?: number; baseLag?: number }): void {
    const ch = this.channels.get(channel);
    if (!ch) return;
    const has = ch.groups.find((g) => g.spec.name === group);
    if (on && !has) {
      const g = this.addGroup(ch, { name: group, baseLag: opts.baseLag ?? 20, procP99: opts.procP99 ?? 120, nackRatio: opts.nackRatio ?? 0.0005, reason: opts.reason, instances: [] });
      const seed = fnv(group) % 500;
      HOST_NODE_IDS.forEach((_, i) => this.addInstance(ch, g, { client: opts.client, conn: 1000 + seed + i, cpu: null, memMb: null }, this.now - 3_600_000));
    } else if (!on && has) {
      ch.groups = ch.groups.filter((g) => g !== has);
    }
  }

  /** Dead-letter `n` messages now (live, counted in the group's DLQ growth). */
  deadLetter(channel: string, group: string, reason: string, n = 1): void {
    this.addDeadLetters(channel, group, reason, n, Date.now());
  }

  /** A broker-owned `$…` channel (e.g. `$system.events`): 1 partition, 7 days retention. */
  addSystemChannel(name: string, createdAgoMs: number, ratePerSec: number): void {
    if (this.channels.has(name)) return;
    this.addChannel({ name, mode: 'stream', partitions: 1, baseRate: ratePerSec, avgSize: 420, retentionSecs: 7 * 86_400, orderingKey: null, createdAgoMs, groups: [] }, this.now - createdAgoMs, []);
  }

  /** Record a broker event (feeds the events topic). */
  emitEvent(kind: FluxEvent['kind'], title: string, subject: string, severity: Health): void {
    this.pushEvent(kind, title, subject, severity);
  }

  getMessage(name: string, p: number, o: number): MessageView {
    const ch = this.channel(name);
    if (!Number.isInteger(p) || p < 0 || p >= ch.config.partitions) throw new ApiErr(404, 'not_found', `partition ${p} does not exist`);
    if (!Number.isInteger(o) || o < this.low(ch, p) || o >= this.high(ch, p)) throw new ApiErr(404, 'not_found', `offset ${o} is out of range for partition ${p}`);
    return this.getMessageUnchecked(ch, p, o);
  }

  queryMessages(name: string, q: MessageQuery, pred?: (m: MessageView) => boolean): MessagePage {
    const ch = this.channel(name);
    const P = ch.config.partitions;
    const limit = clamp(q.limit ?? 100, 1, 1000);
    if (q.partition != null && (q.partition < 0 || q.partition >= P)) throw new ApiErr(400, 'bad_request', `partition ${q.partition} does not exist (channel has ${P})`);
    let parts = q.partition != null ? [q.partition] : Array.from({ length: P }, (_, p) => p);
    if (q.key != null && q.offset == null) {
      const kp = keyPartition(ch.kind, q.key, P);
      if (kp != null) parts = parts.filter((p) => p === kp);
    }
    const SCAN_CAP = pred ? 50_000 : q.key != null ? 250_000 : 20_000;
    const matches = (p: number, o: number) => (q.key == null || this.keyAt(ch, p, o) === q.key) && (!pred || pred(this.getMessageUnchecked(ch, p, o)));
    const out: MessageView[] = [];
    let scanned = 0;
    let hasMore = false;

    if (q.offset != null) {
      const p = q.partition ?? 0;
      const high = this.high(ch, p);
      let o = Math.max(q.offset, this.low(ch, p));
      for (; o < high; o++) {
        if (out.length >= limit || scanned >= SCAN_CAP) {
          hasMore = true;
          break;
        }
        scanned++;
        if (q.until_ms != null && this.tsAt(ch, p, o) > q.until_ms) break;
        if (matches(p, o)) out.push(this.getMessageUnchecked(ch, p, o));
      }
      return { messages: out, scanned, has_more: hasMore };
    }

    if (q.since_ms != null && !q.newest_first) {
      const cur = parts.map((p) => ({ p, o: Math.max(this.low(ch, p), Math.ceil(this.offsetAt(ch, p, q.since_ms!))), high: this.high(ch, p) }));
      for (;;) {
        let best: (typeof cur)[number] | null = null;
        let bestTs = Infinity;
        for (const c of cur) {
          if (c.o >= c.high) continue;
          const ts = this.tsAt(ch, c.p, c.o);
          if (q.until_ms != null && ts > q.until_ms) continue;
          if (ts < bestTs) {
            bestTs = ts;
            best = c;
          }
        }
        if (!best) break;
        if (out.length >= limit || scanned >= SCAN_CAP) {
          hasMore = true;
          break;
        }
        scanned++;
        if (matches(best.p, best.o)) out.push(this.getMessageUnchecked(ch, best.p, best.o));
        best.o++;
      }
      return { messages: out, scanned, has_more: hasMore };
    }

    // newest `limit` (bounded by until_ms, and by since_ms with newest_first), returned ascending
    const cur = parts.map((p) => {
      const low = this.low(ch, p);
      let o = this.high(ch, p) - 1;
      if (q.until_ms != null) o = Math.min(o, Math.floor(this.offsetAt(ch, p, q.until_ms)));
      return { p, o, low };
    });
    for (;;) {
      let best: (typeof cur)[number] | null = null;
      let bestTs = -Infinity;
      for (const c of cur) {
        if (c.o < c.low) continue;
        const ts = this.tsAt(ch, c.p, c.o);
        if (q.since_ms != null && ts < q.since_ms) continue;
        if (ts > bestTs) {
          bestTs = ts;
          best = c;
        }
      }
      if (!best) break;
      if (out.length >= limit || scanned >= SCAN_CAP) {
        hasMore = true;
        break;
      }
      scanned++;
      if (matches(best.p, best.o)) out.push(this.getMessageUnchecked(ch, best.p, best.o));
      best.o--;
    }
    out.reverse();
    return { messages: out, scanned, has_more: hasMore };
  }

  messageDetail(name: string, p: number, o: number): MessageDetail {
    const ch = this.channel(name);
    const message = this.getMessage(name, p, o);
    const deliveries: Record<string, DeliveryAttempt[]> = {};
    for (const g of ch.groups) {
      const consumers = g.instances.map((i) => i.id);
      if (consumers.length === 0) {
        deliveries[g.spec.name] = [];
        continue;
      }
      // Not yet delivered: inside the group's backlog.
      const backlogMs = (g.lag / Math.max(1, ch.rate)) * 1000;
      if (this.now - message.timestamp_ms < backlogMs) {
        deliveries[g.spec.name] = [];
        continue;
      }
      const reason = g.spec.reason ?? 'PostgreSQL timeout';
      deliveries[g.spec.name] = buildAttempts(message.timestamp_ms + backlogMs, ch.config.retry, reason, consumers, mix(fnv(g.spec.name), p, o), g.spec.procP99, this.now);
    }
    return { message, deliveries };
  }

  publish(name: string, items: { key: string | null; headers: Record<string, string>; data: Buffer; payload: MessageView['payload'] }[]): { partition: number; offset: number }[] {
    const ch = this.channel(name);
    const P = ch.config.partitions;
    const ids: { partition: number; offset: number }[] = [];
    for (const it of items) {
      const p = it.key != null ? crc32Mod(it.key, P) : ch.rr++ % P;
      const o = Math.floor(ch.acc[p] ?? 0);
      ch.acc[p] = (ch.acc[p] ?? 0) + 1;
      const m: MessageView = { partition: p, offset: o, key: it.key, headers: { ...it.headers }, timestamp_ms: this.now, size: it.data.length, payload: it.payload };
      ch.overrides.set(`${p}:${o}`, m);
      if (ch.overrides.size > 5000) {
        const first = ch.overrides.keys().next().value;
        if (first !== undefined) ch.overrides.delete(first);
      }
      ids.push({ partition: p, offset: o });
    }
    return ids;
  }

  // ───────────── management ─────────────

  createChannel(req: {
    name: string;
    mode: ChannelMode;
    partitions: number | null;
    retention: RetentionPolicy | null;
    retry: RetryPolicy | null;
    dlq_enabled: boolean | null;
    ordering_key: string | null;
  }): ChannelConfig {
    if (this.channels.has(req.name)) throw new ApiErr(409, 'conflict', `channel '${req.name}' already exists`);
    const spec: ChannelSpec = {
      name: req.name,
      mode: req.mode,
      partitions: req.partitions ?? (req.mode === 'queue' ? 1 : 8),
      baseRate: 5 + unit(fnv(req.name)) * 15,
      avgSize: 200,
      retentionSecs: req.retention ? req.retention.max_age_secs : 7 * 86_400,
      maxBytes: req.retention ? req.retention.max_bytes : null,
      orderingKey: req.ordering_key,
      createdAgoMs: 0,
      groups: [],
    };
    const ch = this.addChannel(spec, this.now, []);
    if (req.retry) ch.config.retry = req.retry;
    if (req.dlq_enabled != null) ch.config.dlq_enabled = req.dlq_enabled;
    this.pushEvent('channel_created', 'Channel created', req.name, 'healthy');
    this.addAudit('channel.create', req.name, req);
    return ch.config;
  }

  updateChannel(name: string, req: { retention?: RetentionPolicy | null; retry?: RetryPolicy | null; dlq_enabled?: boolean | null }): ChannelConfig {
    const ch = this.channel(name);
    const config = { ...ch.config };
    if (req.retention != null) config.retention = req.retention;
    if (req.retry != null) config.retry = req.retry;
    if (req.dlq_enabled != null) config.dlq_enabled = req.dlq_enabled;
    ch.config = config;
    this.pushEvent('channel_updated', 'Channel updated', name, 'healthy');
    this.addAudit('channel.update', name, req);
    return config;
  }

  deleteChannel(name: string): void {
    this.channel(name);
    if (name.startsWith('$')) throw new ApiErr(400, 'bad_request', `'${name}' is a system channel and can't be deleted`);
    this.channels.delete(name);
    this.pushEvent('channel_deleted', 'Channel deleted', name, 'healthy');
    this.addAudit('channel.delete', name, {});
  }

  replay(name: string, ids: { partition: number; offset: number }[], target: string | null): { partition: number; offset: number }[] {
    const tgtName = target ?? name;
    this.channel(tgtName);
    const msgs = ids.map((id) => this.getMessage(name, id.partition, id.offset));
    const out = this.publish(
      tgtName,
      msgs.map((m) => ({
        key: m.key,
        headers: { ...m.headers, 'flux-replayed-from': `${name}/${m.partition}/${m.offset}` },
        data: m.payload.encoding === 'utf8' ? Buffer.from(m.payload.data) : Buffer.from(m.payload.data, 'base64'),
        payload: m.payload,
      })),
    );
    this.pushEvent('messages_replayed', 'Messages replayed', `${tgtName} (${out.length} message${out.length === 1 ? '' : 's'})`, 'healthy');
    this.addAudit('messages.replay', name, { ids, target_channel: target });
    return out;
  }

  /** Remove a consumer group (the caller checked the 409 rules). */
  deleteGroup(name: string, group: string): void {
    const ch = this.channel(name);
    ch.groups = ch.groups.filter((g) => g.spec.name !== group);
    this.alertsOpen.delete(`no_consumers:${name}/${group}`);
    this.addAudit('group.delete', `${name}/${group}`, {});
  }

  resetOffsets(name: string, group: string, to: StartFrom): void {
    const ch = this.channel(name);
    const g = ch.groups.find((x) => x.spec.name === group);
    if (!g) throw new ApiErr(404, 'not_found', `group '${group}' not found on channel '${name}'`);
    const P = ch.config.partitions;
    let lag = 0;
    for (let p = 0; p < P; p++) {
      const high = this.high(ch, p);
      const low = this.low(ch, p);
      if (to === 'earliest') lag += high - low;
      else if (to === 'latest') lag += 0;
      else lag += Math.max(0, high - Math.max(low, Math.floor(this.offsetAt(ch, p, to.timestamp_ms))));
    }
    g.lag = lag;
    this.addAudit('group.reset', `${name}/${group}`, { to });
  }

  addAudit(action: string, target: string, detail: unknown): void {
    this.audit.unshift({ ts: this.now, actor: process.env.MOCK_ADMIN_TOKEN ? 'admin' : 'anonymous', action, target, detail });
    if (this.audit.length > 1000) this.audit.length = 1000;
  }

  auditList(limit: number): AuditEntry[] {
    return this.audit.slice(0, clamp(limit, 1, 1000));
  }

  search(q: string): { kind: string; label: string; link: string }[] {
    const needle = q.trim().toLowerCase();
    const scored: { score: number; r: { kind: string; label: string; link: string } }[] = [];
    const score = (s: string) => {
      const h = s.toLowerCase();
      if (!needle) return 1;
      if (h === needle) return 100;
      if (h.startsWith(needle)) return 50;
      if (h.includes(needle)) return 25;
      let i = 0;
      for (const c of h) if (c === needle[i]) i++;
      return i === needle.length ? 5 : 0;
    };
    for (const ch of this.channels.values()) {
      const n = ch.config.name;
      const s = score(n);
      if (s > 0) scored.push({ score: s + 1, r: { kind: 'channel', label: n, link: `/channels/${encodeURIComponent(n)}` } });
      for (const g of ch.groups) {
        const gs = score(g.spec.name);
        if (gs > 0) scored.push({ score: gs, r: { kind: 'group', label: `${g.spec.name} · ${n}`, link: `/channels/${encodeURIComponent(n)}/consumers` } });
      }
    }
    return scored.sort((a, b) => b.score - a.score || a.r.label.localeCompare(b.r.label)).slice(0, 20).map((x) => x.r);
  }

  // ───────────── telemetry topic payloads ─────────────

  private summaryOf(ch: SimChannel): ChannelSummary {
    const P = ch.config.partitions;
    let messages = 0;
    for (let p = 0; p < P; p++) messages += this.high(ch, p) - this.low(ch, p);
    let health: Health = 'healthy';
    for (const g of ch.groups) if (g.live && HEALTH_RANK[g.live.health] > HEALTH_RANK[health]) health = g.live.health;
    return {
      name: ch.config.name,
      mode: ch.config.mode,
      health,
      partitions: P,
      ingress_rate: ch.rate,
      egress_rate: ch.groups.reduce((s, g) => s + (g.live?.egress_rate ?? 0), 0),
      ack_rate: ch.groups.reduce((s, g) => s + (g.live?.ack_rate ?? 0), 0),
      lag: Math.round(Math.max(0, ...ch.groups.map((g) => g.lag))),
      publish_p99_ms: ch.pubLat.p99,
      consumer_groups: ch.groups.length,
      consumers: ch.groups.reduce((s, g) => s + g.instances.length, 0),
      storage_bytes: messages * ch.avgSize,
      messages,
      dlq: this.dlqCount(ch.config.name),
    };
  }

  channelsSummary(): ChannelsSummary {
    const out: ChannelsSummary = {};
    for (const ch of this.channels.values()) out[ch.config.name] = this.summaryOf(ch);
    return out;
  }

  channelDetail(name: string): ChannelDetail | undefined {
    const ch = this.channels.get(name);
    if (!ch) return undefined;
    const P = ch.config.partitions;
    const partitions = Array.from({ length: P }, (_, p) => {
      const low = this.low(ch, p);
      const high = this.high(ch, p);
      const bytes = (high - low) * ch.avgSize;
      return { partition: p, low_offset: low, high_offset: high, storage_bytes: bytes, segments: Math.max(1, Math.ceil(bytes / GIB)), ingress_rate: ch.rate * (ch.weights[p] ?? 0) };
    });
    const oldest = partitions.some((x) => x.high_offset > x.low_offset)
      ? Math.min(...partitions.filter((x) => x.high_offset > x.low_offset).map((x) => this.tsAt(ch, x.partition, x.low_offset)))
      : null;
    return {
      summary: this.summaryOf(ch),
      config: ch.config,
      publish_latency: ch.pubLat,
      delivery_latency: ch.delLat,
      retries_rate: ch.groups.reduce((s, g) => s + (g.live?.nack_rate ?? 0), 0),
      dlq_rate: this.dlqRate(ch.config.name),
      partitions,
      oldest_timestamp_ms: oldest,
    };
  }

  private dlqRate(channel: string): number {
    let n = 0;
    for (const d of this.dlq) {
      if (d.dead_at_ms < this.now - 60_000) break;
      if (d.channel === channel) n++;
    }
    return n / 60;
  }

  consumers(name: string): ConsumersState | undefined {
    const ch = this.channels.get(name);
    if (!ch) return undefined;
    const out: ConsumersState = {};
    for (const g of ch.groups) if (g.live) out[g.spec.name] = g.live;
    return out;
  }

  tail(name: string, snapshot: boolean): TailState | undefined {
    const ch = this.channels.get(name);
    if (!ch) return undefined;
    if (snapshot && ch.tailBuf.length === 0) this.primeTail(ch);
    const shown = ch.tailShownRate;
    return {
      sample_ratio: ch.rate > 0 ? Math.min(1, shown / ch.rate) : 0,
      shown_rate: shown,
      total_rate: ch.rate,
      messages: snapshot ? [...ch.tailBuf] : ch.tailBatch,
    };
  }

  private clusterLatency(sel: (c: SimChannel) => LatencySummary): LatencySummary {
    const chs = [...this.channels.values()];
    const w = chs.reduce((s, c) => s + c.rate, 0);
    if (w <= 0) return { p50: 0, p95: 0, p99: 0, p999: 0, max: 0 };
    const acc: LatencySummary = { p50: 0, p95: 0, p99: 0, p999: 0, max: 0 };
    for (const c of chs) {
      const l = sel(c);
      const k = c.rate / w;
      acc.p50 += l.p50 * k;
      acc.p95 += l.p95 * k;
      acc.p99 += l.p99 * k;
      acc.p999 += l.p999 * k;
      acc.max = Math.max(acc.max, l.max);
    }
    return acc;
  }

  clusterOverview(): ClusterOverview {
    const sums = [...this.channels.values()].map((c) => this.summaryOf(c));
    const nodes = Object.values(this.nodes);
    let health: Health = 'healthy';
    for (const h of [...sums.map((s) => s.health), ...nodes.map((n) => n.health)]) if (HEALTH_RANK[h] > HEALTH_RANK[health]) health = h;
    const ingress = sums.reduce((s, c) => s + c.ingress_rate, 0);
    const egress = sums.reduce((s, c) => s + c.egress_rate, 0);
    const bytesIn = [...this.channels.values()].reduce((s, c) => s + c.rate * c.avgSize, 0);
    return {
      cluster_name: CLUSTER_NAME,
      health,
      version: VERSION,
      uptime_secs: Math.floor((this.now - this.start) / 1000) + 1_209_600,
      nodes_total: nodes.length,
      // Status-based (reachable members), like the broker; colour comes from `health`.
      nodes_up: nodes.filter((n) => n.status !== 'down').length,
      // Up and not critical.
      nodes_healthy: nodes.filter((n) => n.status !== 'down' && n.health !== 'critical').length,
      channels: sums.length,
      consumer_groups: sums.reduce((s, c) => s + c.consumer_groups, 0),
      consumers: sums.reduce((s, c) => s + c.consumers, 0),
      ingress_rate: ingress,
      egress_rate: egress,
      ack_rate: sums.reduce((s, c) => s + c.ack_rate, 0),
      ingress_bytes_rate: bytesIn,
      egress_bytes_rate: ingress > 0 ? (bytesIn * egress) / ingress : 0,
      publish_latency: this.clusterLatency((c) => c.pubLat),
      delivery_latency: this.clusterLatency((c) => c.delLat),
      total_lag: sums.reduce((s, c) => s + c.lag, 0),
      retries_rate: [...this.channels.values()].reduce((s, c) => s + c.groups.reduce((a, g) => a + (g.live?.nack_rate ?? 0), 0), 0),
      retries_24h: EMPTY ? 0 : Math.round(this.retries24h),
      dlq_total: this.dlq.length,
      storage_bytes: sums.reduce((s, c) => s + c.storage_bytes, 0),
      storage_capacity_bytes: nodes.reduce((s, n) => s + n.disk_total_bytes, 0),
      messages_total: sums.reduce((s, c) => s + c.messages, 0),
      open_alerts: Object.keys(this.alerts).length,
      partial: nodes.some((n) => n.status === 'down'),
    };
  }

  nodesState(): NodesState {
    return this.nodes;
  }

  alertsState(): AlertsState {
    return this.alerts;
  }

  eventsFeed(): EventsFeed {
    return { events: [...this.events] };
  }

  // ───────────── history ─────────────

  private record(now: number): void {
    const put = (key: string, v: number) => {
      let s = this.series.get(key);
      if (!s) {
        s = { ts: [], v: [] };
        this.series.set(key, s);
      }
      s.ts.push(now);
      s.v.push(v);
      if (s.ts.length > RECORD_MAX) {
        s.ts.shift();
        s.v.shift();
      }
    };
    for (const ch of this.channels.values()) {
      for (const [m, v] of Object.entries(this.channelMetrics(ch))) put(`channel.${ch.config.name}.${m}`, v);
    }
    for (const [m, v] of Object.entries(this.clusterMetrics())) put(m, v);
  }

  private channelMetrics(ch: SimChannel): Record<string, number> {
    const s = this.summaryOf(ch);
    return {
      ingress_rate: s.ingress_rate,
      egress_rate: s.egress_rate,
      ack_rate: s.ack_rate,
      publish_p50: ch.pubLat.p50,
      publish_p95: ch.pubLat.p95,
      publish_p99: ch.pubLat.p99,
      lag: s.lag,
      retries_rate: ch.groups.reduce((a, g) => a + (g.live?.nack_rate ?? 0), 0),
      dlq_rate: this.dlqRate(ch.config.name),
      consumers: s.consumers,
      storage_bytes: s.storage_bytes,
    };
  }

  private clusterMetrics(): Record<string, number> {
    const o = this.clusterOverview();
    return {
      ingress_rate: o.ingress_rate,
      egress_rate: o.egress_rate,
      ack_rate: o.ack_rate,
      publish_p50: o.publish_latency.p50,
      publish_p95: o.publish_latency.p95,
      publish_p99: o.publish_latency.p99,
      lag: o.total_lag,
      retries_rate: o.retries_rate,
      dlq_rate: [...this.channels.values()].reduce((s, c) => s + this.dlqRate(c.config.name), 0),
      consumers: o.consumers,
      storage_bytes: o.storage_bytes,
    };
  }

  private seriesAt(key: string, t: number): number | null {
    const s = this.series.get(key);
    if (!s || s.ts.length === 0 || t < (s.ts[0] ?? Infinity)) return null;
    let lo = 0;
    let hi = s.ts.length - 1;
    while (lo < hi) {
      const mid = (lo + hi + 1) >> 1;
      if ((s.ts[mid] ?? 0) <= t) lo = mid;
      else hi = mid - 1;
    }
    return s.v[lo] ?? null;
  }

  private synthChannel(ch: SimChannel, metric: string, t: number, scale: number): number | null {
    if (t < ch.config.created_at_ms) return null;
    const rate = this.rateAt(ch, t, scale);
    const lat = this.latAt(ch, t, Math.max(15_000, scale));
    switch (metric) {
      case 'ingress_rate':
        return rate;
      case 'egress_rate':
        return ch.groups.reduce((s, g) => s + rate * (1 + g.spec.nackRatio), 0);
      case 'ack_rate':
        return ch.groups.length * rate;
      case 'publish_p50':
        return lat.p50;
      case 'publish_p95':
        return lat.p95;
      case 'publish_p99':
        return lat.p99;
      case 'lag':
        return Math.max(0, ...ch.groups.map((g) => this.targetLag(ch, g, t)));
      case 'retries_rate':
        return ch.groups.reduce((s, g) => s + rate * g.spec.nackRatio, 0);
      case 'dlq_rate':
        return Math.max(0, (ch.config.name === 'payments.events' ? 0.12 : 0.008) * (1 + noise(ch.seed + 11, t / 120_000)));
      case 'consumers':
        return ch.groups.reduce((s, g) => s + g.spec.instances.length, 0);
      case 'storage_bytes': {
        const cur = this.summaryOf(ch).storage_bytes;
        return cur * (1 - ((this.now - t) / (7 * DAY)) * 0.03);
      }
      default:
        return null;
    }
  }

  private synth(metric: string, ch: SimChannel | null, t: number, scale: number): number | null {
    if (ch) return this.synthChannel(ch, metric, t, scale);
    const chs = [...this.channels.values()];
    if (metric.startsWith('publish_')) {
      let w = 0;
      let v = 0;
      for (const c of chs) {
        const r = this.rateAt(c, t, scale);
        const x = this.synthChannel(c, metric, t, scale);
        if (x == null) continue;
        w += r;
        v += x * r;
      }
      return w > 0 ? v / w : chs.length === 0 ? 0 : null;
    }
    let sum = 0;
    for (const c of chs) sum += this.synthChannel(c, metric, t, scale) ?? 0;
    return sum;
  }

  history(metricParam: string, range: string): HistorySeries {
    if (!(range in HISTORY_RANGE_MS)) throw new ApiErr(400, 'bad_request', `unknown range '${range}' (expected 5m, 15m, 1h, 24h, 7d)`);
    const r = range as HistoryRange;
    let ch: SimChannel | null = null;
    let metric: string | null = null;
    if ((METRICS as readonly string[]).includes(metricParam)) metric = metricParam;
    else if (metricParam.startsWith('channel.')) {
      const m = [...METRICS].sort((a, b) => b.length - a.length).find((x) => metricParam.endsWith(`.${x}`));
      if (m) {
        const name = metricParam.slice('channel.'.length, -(m.length + 1));
        ch = this.channels.get(name) ?? null;
        if (!ch) throw new ApiErr(404, 'not_found', `channel '${name}' not found`);
        metric = m;
      }
    }
    if (!metric) throw new ApiErr(400, 'bad_request', `unknown metric '${metricParam}'`);
    const bucket = HISTORY_BUCKET_MS[r];
    const dur = HISTORY_RANGE_MS[r];
    const end = this.now;
    const startT = Math.floor((end - dur) / bucket) * bucket;
    const key = ch ? `channel.${ch.config.name}.${metric}` : metric;
    const rec = this.series.get(key);
    const recStart = rec?.ts[0] ?? Infinity;
    const points: [number, number][] = [];
    const scale = Math.max(20_000, bucket * 4);
    let ri = 0;
    for (let t = startT; t <= end; t += bucket) {
      let v: number | null = null;
      if (rec && t + bucket > recStart) {
        while (ri < rec.ts.length && (rec.ts[ri] ?? 0) < t) ri++;
        let n = 0;
        let sum = 0;
        let j = ri;
        while (j < rec.ts.length && (rec.ts[j] ?? 0) < t + bucket) {
          sum += rec.v[j] ?? 0;
          n++;
          j++;
        }
        if (n > 0) v = sum / n;
      }
      if (v == null && t < (Number.isFinite(recStart) ? recStart : end + 1)) v = this.synth(metric, ch, t, scale);
      if (v != null && Number.isFinite(v)) points.push([t, Math.round(v * 1000) / 1000]);
    }
    return { metric: metricParam, range: r, bucket_ms: bucket, points };
  }
}

function subjectOf(a: Alert): string {
  if (a.channel && a.group) return `${a.channel} / ${a.group}`;
  return a.channel ?? a.title;
}

function crc32Mod(key: string, P: number): number {
  return crc32(key) % P;
}

/** A failed delivery history ending in dead-lettering. */
function buildFailedAttempts(publishedAt: number, retry: RetryPolicy, reason: string, consumers: string[], seed: number): DeliveryAttempt[] {
  const out: DeliveryAttempt[] = [];
  let t = publishedAt + 3 + (Math.abs(seed) % 20);
  for (let a = 1; a <= retry.max_attempts; a++) {
    const consumer = consumers[(seed + a) % consumers.length] ?? 'unknown';
    const timedOut = reason === 'ack timeout';
    const finished = timedOut ? t + retry.ack_timeout_ms : t + 20 + ((seed >>> a) % 400);
    out.push({
      attempt: a,
      consumer,
      delivered_at_ms: Math.round(t),
      finished_at_ms: Math.round(finished),
      outcome: timedOut ? { kind: 'timed_out' } : { kind: 'nacked', reason },
    });
    const backoff = retry.backoff_ms[Math.min(a - 1, retry.backoff_ms.length - 1)] ?? 1000;
    t = finished + backoff;
  }
  return out;
}

/** A believable delivery history for one group; clipped at `now`. */
function buildAttempts(
  firstDeliveryAt: number,
  retry: RetryPolicy,
  reason: string,
  consumers: string[],
  seed: number,
  procP99: number,
  now: number,
): DeliveryAttempt[] {
  const r = unit(seed);
  let failures: number;
  let timeout = false;
  let dead = false;
  if (r < 0.8) failures = 0;
  else if (r < 0.92) failures = 1;
  else if (r < 0.96) failures = 2;
  else if (r < 0.98) {
    failures = 1;
    timeout = true;
  } else {
    failures = retry.max_attempts;
    dead = true;
  }
  const rand = prng(seed);
  const out: DeliveryAttempt[] = [];
  let t = firstDeliveryAt + 2 + rand() * 30;
  const total = dead ? retry.max_attempts : failures + 1;
  for (let a = 1; a <= total; a++) {
    if (t > now) break;
    const consumer = consumers[Math.floor(rand() * consumers.length)] ?? consumers[0] ?? 'unknown';
    const failed = a <= failures;
    const isTimeout = failed && timeout && a === 1;
    const finished = isTimeout ? t + retry.ack_timeout_ms : t + procP99 * (0.2 + rand() * 0.8);
    if (finished > now) {
      out.push({ attempt: a, consumer, delivered_at_ms: Math.round(t), finished_at_ms: null, outcome: { kind: 'in_flight' } });
      break;
    }
    out.push({
      attempt: a,
      consumer,
      delivered_at_ms: Math.round(t),
      finished_at_ms: Math.round(finished),
      outcome: !failed ? { kind: 'acked' } : isTimeout ? { kind: 'timed_out' } : { kind: 'nacked', reason },
    });
    const backoff = retry.backoff_ms[Math.min(a - 1, retry.backoff_ms.length - 1)] ?? 1000;
    t = finished + backoff;
  }
  return out;
}
