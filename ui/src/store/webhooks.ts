/**
 * HTTP push webhooks (`GET /api/webhooks`) and their cluster-wide stats (`GET /api/webhooks/stats`),
 * shared by the Webhooks pages, the sidebar badge, consumers (`$webhook:*` links) and ⌘K.
 */
import { useEffect } from 'react';
import { create } from 'zustand';
import { api, ApiError, errorMessage } from '../api/client';
import type { WebhookStats, WebhookSubscription } from '../api/types';

interface WebhooksStore {
  list: WebhookSubscription[] | null;
  stats: Record<string, WebhookStats>;
  statsAt: number;
  error: string | null;
  /** The broker has no webhooks API (pre-v0.3). */
  unsupported: boolean;
  /** Enabled webhooks whose last delivery failed. */
  failing: number;
}

export const useWebhooks = create<WebhooksStore>()(() => ({ list: null, stats: {}, statsAt: 0, error: null, unsupported: false, failing: 0 }));

function failingCount(list: WebhookSubscription[] | null, stats: Record<string, WebhookStats>): number {
  return (list ?? []).filter((w) => {
    const s = stats[w.id];
    return w.enabled && s && (s.breaker != null || s.last_error != null || (s.last_status != null && (s.last_status < 200 || s.last_status >= 300)));
  }).length;
}

export async function loadWebhooks(): Promise<void> {
  try {
    const list = [...(await api.listWebhooks())].sort((a, b) => a.name.localeCompare(b.name));
    useWebhooks.setState((s) => ({ list, error: null, unsupported: false, failing: failingCount(list, s.stats) }));
  } catch (e) {
    if (e instanceof ApiError && e.isNotFound) useWebhooks.setState({ list: [], unsupported: true, error: null });
    else useWebhooks.setState({ error: errorMessage(e) });
  }
}

export async function loadWebhookStats(): Promise<void> {
  if (useWebhooks.getState().unsupported) return;
  try {
    const now = Date.now();
    if (now - useWebhooks.getState().statsAt < 800) return;
    const stats: Record<string, WebhookStats> = {};
    for (const s of await api.webhookStats()) stats[s.id] = s;
    useWebhooks.setState((st) => ({ stats, statsAt: now, failing: failingCount(st.list, stats) }));
  } catch {
    /* stats are decoration */
  }
}

export function upsertWebhook(w: WebhookSubscription): void {
  const cur = useWebhooks.getState().list ?? [];
  const next = cur.some((x) => x.id === w.id) ? cur.map((x) => (x.id === w.id ? w : x)) : [...cur, w];
  useWebhooks.setState({ list: next.sort((a, b) => a.name.localeCompare(b.name)) });
}

export function removeWebhook(id: string): void {
  useWebhooks.setState({ list: (useWebhooks.getState().list ?? []).filter((w) => w.id !== id) });
}

/** Load the list on mount and poll stats every `ms` while mounted. */
export function useWebhooksPolling(ms = 5000): void {
  useEffect(() => {
    void loadWebhooks().then(loadWebhookStats);
    const id = setInterval(() => {
      if (!document.hidden) void loadWebhookStats();
    }, ms);
    return () => clearInterval(id);
  }, [ms]);
}
