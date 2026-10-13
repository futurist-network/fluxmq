/**
 * `GET /api/cluster/status`, polled while anything on screen needs it. Consumers register the
 * cadence they want (Nodes page 2 s, top bar 10 s); the poller runs at the fastest one.
 * A server without the endpoint (v0.1) yields `unsupported` and polling stops.
 */
import { useEffect } from 'react';
import { create } from 'zustand';
import { api, ApiError } from '../api/client';
import type { ClusterStatus } from '../api/types';

interface ClusterStore {
  status: ClusterStatus | null;
  /** True once the first answer (or error) arrived. */
  loaded: boolean;
  unsupported: boolean;
  error: string | null;
  fetchedAt: number;
}

export const useCluster = create<ClusterStore>()(() => ({ status: null, loaded: false, unsupported: false, error: null, fetchedAt: 0 }));

const wants = new Map<number, number>();
let wantId = 0;
let timer: ReturnType<typeof setTimeout> | null = null;
let inflight = false;

function interval(): number | null {
  return wants.size ? Math.min(...wants.values()) : null;
}

export async function refreshCluster(): Promise<void> {
  if (inflight) return;
  inflight = true;
  try {
    const status = await api.clusterStatus();
    useCluster.setState({ status, loaded: true, unsupported: false, error: null, fetchedAt: Date.now() });
  } catch (e) {
    if (e instanceof ApiError && e.isNotFound) useCluster.setState({ status: null, loaded: true, unsupported: true, error: null });
    else useCluster.setState({ loaded: true, error: e instanceof Error ? e.message : String(e) });
  } finally {
    inflight = false;
  }
}

function schedule(): void {
  if (timer) clearTimeout(timer);
  timer = null;
  const ms = interval();
  if (ms == null || useCluster.getState().unsupported) return;
  const elapsed = Date.now() - useCluster.getState().fetchedAt;
  timer = setTimeout(
    () => {
      timer = null;
      const run = document.hidden ? Promise.resolve() : refreshCluster();
      void run.then(schedule);
    },
    Math.max(0, ms - elapsed),
  );
}

/** Keep cluster status fresh at (at least) `ms` while mounted. */
export function useClusterPolling(ms: number): void {
  useEffect(() => {
    const id = ++wantId;
    wants.set(id, ms);
    schedule();
    return () => {
      wants.delete(id);
      schedule();
    };
  }, [ms]);
}
