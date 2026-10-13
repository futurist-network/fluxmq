/**
 * `GET /api/cluster/rebalance` (v0.4), polled while the Rebalancing panel is mounted: every 2 s
 * while a plan is running or cancelling, every 10 s otherwise (`pollIntervalMs`). Rebalance
 * events from telemetry trigger an immediate refresh (see the panel). The highest lag seen per
 * catching-up move is kept here so progress bars have a baseline (the contract has none).
 */
import { useEffect } from 'react';
import { create } from 'zustand';
import { api, ApiError, errorMessage } from '../api/client';
import type { RebalanceStatus } from '../api/types';
import { pollIntervalMs, trackPeakLag } from '../lib/rebalance';

interface RebalanceStore {
  status: RebalanceStatus | null;
  loaded: boolean;
  /** Server without the endpoint (pre-v0.4): 404. */
  unsupported: boolean;
  error: string | null;
  /** `channel/partition@started_at` → highest lag seen while catching up. */
  peaks: Record<string, number>;
  fetchedAt: number;
}

export const useRebalance = create<RebalanceStore>()(() => ({ status: null, loaded: false, unsupported: false, error: null, peaks: {}, fetchedAt: 0 }));

/** Store a status answered by a (non-dry-run) mutation or a GET. */
export function setRebalanceStatus(status: RebalanceStatus): void {
  useRebalance.setState((s) => ({ status, loaded: true, unsupported: false, error: null, peaks: trackPeakLag(s.peaks, status.moves), fetchedAt: Date.now() }));
}

let inflight: Promise<void> | null = null;

export function refreshRebalance(): Promise<void> {
  if (inflight) return inflight;
  inflight = (async () => {
    try {
      setRebalanceStatus(await api.rebalanceStatus());
    } catch (e) {
      if (e instanceof ApiError && e.isNotFound) useRebalance.setState({ loaded: true, unsupported: true, error: null });
      else useRebalance.setState({ loaded: true, error: errorMessage(e) });
    } finally {
      inflight = null;
    }
  })();
  return inflight;
}

/** Poll while mounted (and `enabled`), re-reading the cadence from the latest state after every fetch. */
export function useRebalancePolling(enabled: boolean): void {
  useEffect(() => {
    if (!enabled) return;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let stopped = false;
    const arm = (ms: number) => {
      if (timer) clearTimeout(timer);
      timer = stopped ? null : setTimeout(() => void loop(), ms);
    };
    const loop = async () => {
      timer = null;
      if (!document.hidden) await refreshRebalance();
      if (stopped || useRebalance.getState().unsupported) return;
      arm(pollIntervalMs(useRebalance.getState().status?.state));
    };
    void loop();
    // A state change from elsewhere (a mutation's answer) re-arms the timer at the new cadence.
    const unsub = useRebalance.subscribe((s, prev) => {
      if (s.status?.state !== prev.status?.state && timer) arm(pollIntervalMs(s.status?.state));
    });
    return () => {
      stopped = true;
      unsub();
      if (timer) clearTimeout(timer);
    };
  }, [enabled]);
}
