/**
 * Client-side time series (sparklines, live chart tails). Lives outside React: charts
 * subscribe to a key and redraw imperatively via uPlot, so a new sample never causes a
 * React render. Samples are bucketed to 1 s (latest value in the bucket wins).
 */
import type { ChannelsSummary, ClusterOverview } from '../api/types';
import { TOPICS } from '../api/types';
import type { FrameCommit } from '../telemetry/client';

export interface Series {
  xs: number[]; // seconds (uPlot convention)
  ys: number[];
  version: number;
}

const CAP = 900; // 15 min @ 1 Hz

type Listener = () => void;

export class SeriesStore {
  private readonly map = new Map<string, Series>();
  private readonly listeners = new Map<string, Set<Listener>>();
  private readonly dirty = new Set<string>();

  get(key: string): Series {
    let s = this.map.get(key);
    if (!s) {
      s = { xs: [], ys: [], version: 0 };
      this.map.set(key, s);
    }
    return s;
  }

  has(key: string): boolean {
    return (this.map.get(key)?.xs.length ?? 0) > 0;
  }

  append(key: string, tsMs: number, value: number): void {
    if (!Number.isFinite(value)) return;
    const s = this.get(key);
    const sec = Math.floor(tsMs / 1000);
    const n = s.xs.length;
    if (n > 0 && s.xs[n - 1] === sec) {
      s.ys[n - 1] = value;
    } else if (n > 0 && (s.xs[n - 1] ?? 0) > sec) {
      return; // out of order
    } else {
      s.xs.push(sec);
      s.ys.push(value);
      if (s.xs.length > CAP) {
        s.xs.splice(0, s.xs.length - CAP);
        s.ys.splice(0, s.ys.length - CAP);
      }
    }
    s.version++;
    this.dirty.add(key);
  }

  /** Prefill from history (`[ms, value]`); keeps live samples newer than the history. */
  seed(key: string, points: [number, number][]): void {
    const s = this.get(key);
    const liveFrom = s.xs.length ? s.xs[0]! : Infinity;
    const xs: number[] = [];
    const ys: number[] = [];
    for (const [ms, v] of points) {
      const sec = Math.floor(ms / 1000);
      if (sec >= liveFrom) break;
      if (xs.length && xs[xs.length - 1] === sec) continue;
      xs.push(sec);
      ys.push(v);
    }
    s.xs = xs.concat(s.xs).slice(-CAP);
    s.ys = ys.concat(s.ys).slice(-CAP);
    s.version++;
    this.dirty.add(key);
    this.notify();
  }

  subscribe(key: string, l: Listener): () => void {
    let set = this.listeners.get(key);
    if (!set) {
      set = new Set();
      this.listeners.set(key, set);
    }
    set.add(l);
    return () => {
      set.delete(l);
    };
  }

  notify(): void {
    if (!this.dirty.size) return;
    const keys = [...this.dirty];
    this.dirty.clear();
    for (const k of keys) this.listeners.get(k)?.forEach((l) => l());
  }

  clear(): void {
    this.map.clear();
    this.dirty.clear();
  }
}

export const series = new SeriesStore();

/** Well-known series keys. */
export const SK = {
  ingress: 'cluster.ingress',
  egress: 'cluster.egress',
  acked: 'cluster.acked',
  p50: 'cluster.p50',
  p95: 'cluster.p95',
  p99: 'cluster.p99',
  lag: 'cluster.lag',
  errors: 'cluster.errors',
  chIngress: (name: string) => `ch.${name}.ingress`,
  chEgress: (name: string) => `ch.${name}.egress`,
  chAcked: (name: string) => `ch.${name}.acked`,
  chLag: (name: string) => `ch.${name}.lag`,
  chP99: (name: string) => `ch.${name}.p99`,
} as const;

let lastDlqTotal: number | null = null;
let lastDlqAt = 0;

/** Feed derived series from a telemetry frame. Called for every frame, paused or not. */
export function recordFrame(c: FrameCommit, store: SeriesStore = series): void {
  if (c.changed.has(TOPICS.CLUSTER)) {
    const o = c.view[TOPICS.CLUSTER] as ClusterOverview | undefined;
    if (o) {
      store.append(SK.ingress, c.now, o.ingress_rate);
      store.append(SK.egress, c.now, o.egress_rate);
      store.append(SK.acked, c.now, o.ack_rate);
      store.append(SK.p50, c.now, o.publish_latency.p50);
      store.append(SK.p95, c.now, o.publish_latency.p95);
      store.append(SK.p99, c.now, o.publish_latency.p99);
      store.append(SK.lag, c.now, o.total_lag);
      // Errors = retries/s + newly dead-lettered/s (derived from the DLQ total).
      let dlqRate = 0;
      if (lastDlqTotal != null && c.now > lastDlqAt) {
        dlqRate = Math.max(0, ((o.dlq_total - lastDlqTotal) * 1000) / (c.now - lastDlqAt));
      }
      if (c.now - lastDlqAt >= 1000 || lastDlqTotal == null) {
        lastDlqTotal = o.dlq_total;
        lastDlqAt = c.now;
      }
      store.append(SK.errors, c.now, o.retries_rate + dlqRate);
    }
  }
  if (c.changed.has(TOPICS.CHANNELS)) {
    const chs = c.view[TOPICS.CHANNELS] as ChannelsSummary | undefined;
    if (chs) {
      for (const ch of Object.values(chs)) {
        store.append(SK.chIngress(ch.name), c.now, ch.ingress_rate);
        store.append(SK.chEgress(ch.name), c.now, ch.egress_rate);
        store.append(SK.chAcked(ch.name), c.now, ch.ack_rate);
        store.append(SK.chLag(ch.name), c.now, ch.lag);
        store.append(SK.chP99(ch.name), c.now, ch.publish_p99_ms);
      }
    }
  }
  store.notify();
}
