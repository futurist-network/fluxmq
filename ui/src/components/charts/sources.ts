import { useEffect, useMemo, useState } from 'react';
import type uPlot from 'uplot';
import { api } from '../../api/client';
import { HISTORY_BUCKET_MS, type HistoryRange, type HistorySeries } from '../../api/types';
import { series as seriesStore } from '../../store/series';
import type { ChartSource } from './UChart';

/** Chart source over one or more live SeriesStore keys sharing the first key's x axis. */
export function liveSource(keys: string[], windowSec = 120): ChartSource {
  return {
    get() {
      const base = seriesStore.get(keys[0]!);
      const n = base.xs.length;
      const from = Math.max(0, n - windowSec);
      const xs = base.xs.slice(from);
      const cols: (number | null)[][] = keys.map((k, i) => {
        if (i === 0) return base.ys.slice(from);
        const s = seriesStore.get(k);
        const map = new Map<number, number>();
        for (let j = Math.max(0, s.xs.length - windowSec - 5); j < s.xs.length; j++) map.set(s.xs[j]!, s.ys[j]!);
        return xs.map((x) => map.get(x) ?? null);
      });
      return [xs, ...cols] as uPlot.AlignedData;
    },
    subscribe(cb) {
      const unsubs = keys.map((k) => seriesStore.subscribe(k, cb));
      return () => unsubs.forEach((u) => u());
    },
  };
}

export function useLiveSource(keys: string[], windowSec = 120): ChartSource {
  const k = keys.join('|');
  return useMemo(() => liveSource(k.split('|'), windowSec), [k, windowSec]);
}

const seeded = new Set<string>();

/** Prefill a live series key from `/api/history?range=5m` once per session (sparklines). */
export function useSeedSeries(key: string, metrics: string[]): void {
  const m = metrics.join('|');
  useEffect(() => {
    if (seeded.has(key)) return;
    seeded.add(key);
    const ms = m.split('|');
    Promise.all(ms.map((metric) => api.history(metric, '5m')))
      .then((res) => seriesStore.seed(key, sumSeries(res)))
      .catch(() => seeded.delete(key));
  }, [key, m]);
}

/** Sum several history series by timestamp (Errors = retries + DLQ). */
export function sumSeries(list: HistorySeries[]): [number, number][] {
  if (list.length === 1) return list[0]!.points;
  const acc = new Map<number, number>();
  for (const s of list) for (const [t, v] of s.points) acc.set(t, (acc.get(t) ?? 0) + v);
  return [...acc.entries()].sort((a, b) => a[0] - b[0]);
}

export interface HistoryState {
  source: ChartSource;
  loading: boolean;
  error: unknown;
  empty: boolean;
  hasData: boolean;
}

/**
 * History-backed chart source: loads `/api/history` for each metric and the range, then
 * appends live samples from `liveKeys` (only for ranges with ≤ 1 s buckets, where a live
 * point is the same resolution). Longer ranges refetch periodically instead.
 */
export function useHistorySource(metrics: string[], range: HistoryRange, liveKeys: string[] | null): HistoryState {
  const mk = metrics.join('|');
  const lk = liveKeys?.join('|') ?? '';
  const [state, setState] = useState<{ data: uPlot.AlignedData | null; loading: boolean; error: unknown; key: string }>({
    data: null,
    loading: true,
    error: undefined,
    key: '',
  });
  const reqKey = `${mk}@${range}`;

  useEffect(() => {
    const ctrl = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const load = () => {
      Promise.all(mk.split('|').map((m) => api.history(m, range, ctrl.signal)))
        .then((res) => {
          if (ctrl.signal.aborted) return;
          setState({ data: alignHistory(res), loading: false, error: undefined, key: reqKey });
          const refresh = Math.max(15_000, HISTORY_BUCKET_MS[range] * 6);
          timer = setTimeout(load, refresh);
        })
        .catch((error: unknown) => {
          if (ctrl.signal.aborted) return;
          setState((s) => ({ data: s.key === reqKey ? s.data : null, loading: false, error, key: reqKey }));
          timer = setTimeout(load, 10_000);
        });
    };
    load();
    return () => {
      ctrl.abort();
      clearTimeout(timer);
    };
  }, [mk, range, reqKey]);

  const live = HISTORY_BUCKET_MS[range] <= 1000 && lk ? lk.split('|') : null;
  const data = state.key === reqKey ? state.data : null;

  const source = useMemo<ChartSource>(() => {
    const winSec = { '5m': 300, '15m': 900, '1h': 3600, '24h': 86_400, '7d': 604_800 }[range];
    return {
      get() {
        const hist = data ?? ([[], ...mk.split('|').map(() => [])] as unknown as uPlot.AlignedData);
        if (!live) return hist;
        const xs = (hist[0] as number[]).slice();
        const cols = hist.slice(1).map((c) => (c as (number | null)[]).slice());
        const lastHist = xs.length ? xs[xs.length - 1]! : 0;
        const base = seriesStore.get(live[0]!);
        const others = live.slice(1).map((k) => {
          const s = seriesStore.get(k);
          return new Map(s.xs.map((x, i) => [x, s.ys[i]!] as const));
        });
        for (let i = 0; i < base.xs.length; i++) {
          const x = base.xs[i]!;
          if (x <= lastHist) continue;
          xs.push(x);
          cols[0]?.push(base.ys[i]!);
          others.forEach((m, j) => cols[j + 1]?.push(m.get(x) ?? null));
        }
        // Trim to the window.
        const cutoff = (xs[xs.length - 1] ?? 0) - winSec;
        let start = 0;
        while (start < xs.length && xs[start]! < cutoff) start++;
        return [xs.slice(start), ...cols.map((c) => c.slice(start))] as uPlot.AlignedData;
      },
      subscribe(cb) {
        if (!live) return () => {};
        const unsubs = live.map((k) => seriesStore.subscribe(k, cb));
        return () => unsubs.forEach((u) => u());
      },
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, lk, range, mk, live?.length]);

  const err = state.key === reqKey ? state.error : undefined;
  return { source, loading: !data && !err, error: err, empty: !!data && (data[0] as number[]).length === 0, hasData: !!data };
}

/** Merge several `[ms, v]` series onto one x axis (seconds). */
export function alignHistory(list: HistorySeries[]): uPlot.AlignedData {
  const xsSet = new Set<number>();
  for (const s of list) for (const [t] of s.points) xsSet.add(Math.floor(t / 1000));
  const xs = [...xsSet].sort((a, b) => a - b);
  const index = new Map(xs.map((x, i) => [x, i]));
  const cols = list.map((s) => {
    const col: (number | null)[] = new Array<number | null>(xs.length).fill(null);
    for (const [t, v] of s.points) col[index.get(Math.floor(t / 1000))!] = v;
    return col;
  });
  return [xs, ...cols] as uPlot.AlignedData;
}
