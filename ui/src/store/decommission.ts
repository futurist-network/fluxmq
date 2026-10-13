/**
 * Node decommissions (v0.5) from `GET /api/cluster/decommissions` (current + the last 24 h), polled
 * every 2 s while one runs and every 10 s otherwise. Any browser sees removed / failed cards.
 * A server without the list endpoint (404) falls back to `GET /api/cluster/nodes/{id}/decommission`
 * for decommissioning nodes and the ones this tab started. Dismissing a card is a per-viewer
 * convenience (sessionStorage).
 */
import { useEffect } from 'react';
import { create } from 'zustand';
import { api, ApiError, errorMessage } from '../api/client';
import type { DecommissionStatus, NodeStatus } from '../api/types';
import { decomKey, decommissionPollMs, isTerminal, latestByNode } from '../lib/decommission';

const DISMISS_KEY = 'fluxmq.dismissedDecommissions';

function loadDismissed(): string[] {
  try {
    const v = JSON.parse(window.sessionStorage.getItem(DISMISS_KEY) ?? '[]') as unknown;
    return Array.isArray(v) ? v.filter((x): x is string => typeof x === 'string') : [];
  } catch {
    return [];
  }
}

interface DecommissionStore {
  byNode: Record<string, DecommissionStatus>;
  dismissed: string[];
  error: string | null;
  /** Server without `GET /api/cluster/decommissions`. */
  listUnsupported: boolean;
}

export const useDecommission = create<DecommissionStore>()(() => ({ byNode: {}, dismissed: typeof window === 'undefined' ? [] : loadDismissed(), error: null, listUnsupported: false }));

/** Store a status (POST answer or GET). */
export function setDecommissionStatus(s: DecommissionStatus): void {
  useDecommission.setState((st) => ({ byNode: { ...st.byNode, [s.node]: s } }));
}

export function dismissDecommission(s: DecommissionStatus): void {
  const dismissed = [...new Set([...useDecommission.getState().dismissed, decomKey(s)])].slice(-50);
  useDecommission.setState({ dismissed });
  try {
    window.sessionStorage.setItem(DISMISS_KEY, JSON.stringify(dismissed));
  } catch {
    /* storage unavailable: dismissal lasts for this page only */
  }
}

let inflight: Promise<void> | null = null;

/** Refresh from the list (or, on an older server, per node). */
export function refreshDecommissions(decommissioning: readonly string[] = []): Promise<void> {
  if (inflight) return inflight;
  inflight = (async () => {
    const st = useDecommission.getState();
    try {
      if (!st.listUnsupported) {
        try {
          useDecommission.setState({ byNode: latestByNode(await api.listDecommissions()), error: null });
          return;
        } catch (e) {
          if (!(e instanceof ApiError && e.isNotFound)) throw e;
          useDecommission.setState({ listUnsupported: true });
        }
      }
      const ids = new Set([...decommissioning, ...Object.values(st.byNode).filter((s) => !isTerminal(s.state)).map((s) => s.node)]);
      for (const id of ids) {
        try {
          setDecommissionStatus(await api.decommissionStatus(id));
        } catch (e) {
          if (!(e instanceof ApiError && e.isNotFound)) throw e;
        }
      }
      useDecommission.setState({ error: null });
    } catch (e) {
      useDecommission.setState({ error: errorMessage(e) });
    } finally {
      inflight = null;
    }
  })();
  return inflight;
}

/** Poll while mounted; the cadence follows the latest state. */
export function useDecommissionPolling(nodes: readonly { id: string; status: NodeStatus }[]): void {
  const decommissioning = nodes.filter((n) => n.status === 'decommissioning').map((n) => n.id).join(',');
  const anyDecom = decommissioning.length > 0;
  useEffect(() => {
    let timer: ReturnType<typeof setTimeout> | null = null;
    let stopped = false;
    const ids = decommissioning ? decommissioning.split(',') : [];
    const loop = async () => {
      if (!document.hidden) await refreshDecommissions(ids);
      if (stopped) return;
      timer = setTimeout(() => void loop(), decommissionPollMs(useDecommission.getState().byNode, anyDecom ? [{ status: 'decommissioning' }] : []));
    };
    void loop();
    return () => {
      stopped = true;
      if (timer) clearTimeout(timer);
    };
  }, [decommissioning, anyDecom]);
}
