/**
 * Routes (`GET /api/routes`) and their counters (`GET /api/routes/stats`), shared by the
 * Routing page, the channel Routing tab, the consumers `$router` link and ⌘K.
 * Stats are polled while a screen asks for them; rates are derived from consecutive polls.
 */
import { useEffect } from 'react';
import { create } from 'zustand';
import { api, ApiError, errorMessage } from '../api/client';
import type { Route, RouteStats } from '../api/types';

interface RoutesStore {
  routes: Route[] | null;
  stats: Record<string, RouteStats>;
  /** matched/s per route id, from the last two stats polls. */
  rates: Record<string, number>;
  statsAt: number;
  error: string | null;
  unsupported: boolean;
}

export const useRoutes = create<RoutesStore>()(() => ({ routes: null, stats: {}, rates: {}, statsAt: 0, error: null, unsupported: false }));

export async function loadRoutes(): Promise<void> {
  try {
    const routes = await api.listRoutes();
    useRoutes.setState({ routes: [...routes].sort((a, b) => a.name.localeCompare(b.name)), error: null, unsupported: false });
  } catch (e) {
    if (e instanceof ApiError && e.isNotFound) useRoutes.setState({ routes: [], unsupported: true, error: null });
    else useRoutes.setState({ error: errorMessage(e) });
  }
}

export async function loadRouteStats(): Promise<void> {
  try {
    const list = await api.routeStats();
    const now = Date.now();
    const { stats: prev, statsAt } = useRoutes.getState();
    // Polls closer than 1 s (StrictMode double effects, several screens mounting) give noisy rates.
    if (statsAt && now - statsAt < 1000) return;
    const stats: Record<string, RouteStats> = {};
    const rates: Record<string, number> = {};
    for (const s of list) {
      stats[s.id] = s;
      const p = prev[s.id];
      if (p && statsAt && now > statsAt) rates[s.id] = Math.max(0, ((s.matched - p.matched) * 1000) / (now - statsAt));
    }
    useRoutes.setState({ stats, rates, statsAt: now });
  } catch {
    /* stats are decoration; the list still renders */
  }
}

/** Patch one route locally after a mutation (no refetch flash). */
export function upsertRoute(r: Route): void {
  const cur = useRoutes.getState().routes ?? [];
  const next = cur.some((x) => x.id === r.id) ? cur.map((x) => (x.id === r.id ? r : x)) : [...cur, r];
  useRoutes.setState({ routes: next.sort((a, b) => a.name.localeCompare(b.name)) });
}

export function removeRoute(id: string): void {
  useRoutes.setState({ routes: (useRoutes.getState().routes ?? []).filter((r) => r.id !== id) });
}

/** Load routes on mount and poll stats every `ms` while mounted. */
export function useRoutesPolling(ms = 5000): void {
  useEffect(() => {
    void loadRoutes();
    void loadRouteStats();
    const id = setInterval(() => {
      if (!document.hidden) void loadRouteStats();
    }, ms);
    return () => clearInterval(id);
  }, [ms]);
}
