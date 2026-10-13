/**
 * Conditions → canonical DSL text through `POST /api/filter/render` (the server is the only
 * renderer). Results are cached by the conditions' JSON so the routes list, ⌘K search and the
 * editor's builder → text switch share them; concurrent requests for the same key are merged.
 */
import { useEffect } from 'react';
import { create } from 'zustand';
import { api, ApiError } from '../api/client';
import type { Condition } from '../api/types';
import { t, useT } from '../i18n';

export type RenderResult =
  | { status: 'ok'; rendered: string }
  | { status: 'error'; message: string }
  /** The server can't render (role, or a server without the endpoint). */
  | { status: 'unknown' };

export type RenderState = RenderResult | { status: 'idle' } | { status: 'checking' };

const MAX_ENTRIES = 500;

export const useRenderCache = create<{ results: Record<string, RenderResult> }>()(() => ({ results: {} }));

export const conditionsKey = (conditions: readonly Condition[]) => JSON.stringify(conditions);

const inflight = new Map<string, Promise<RenderResult>>();

function store(key: string, r: RenderResult): void {
  const results = { ...useRenderCache.getState().results, [key]: r };
  const keys = Object.keys(results);
  for (const k of keys.slice(0, Math.max(0, keys.length - MAX_ENTRIES))) delete results[k];
  useRenderCache.setState({ results });
}

/** Render (or return the cached rendering of) `conditions`. Never throws. */
export function renderConditions(conditions: readonly Condition[]): Promise<RenderResult> {
  // An empty list renders to the empty filter — no round trip needed.
  if (!conditions.length) return Promise.resolve({ status: 'ok', rendered: '' });
  const key = conditionsKey(conditions);
  const cached = useRenderCache.getState().results[key];
  if (cached && cached.status !== 'error') return Promise.resolve(cached);
  let p = inflight.get(key);
  if (!p) {
    let transient = false;
    p = api
      .renderFilter([...conditions])
      .then((r): RenderResult => ({ status: 'ok', rendered: r.rendered }))
      .catch((e: unknown): RenderResult => {
        if (e instanceof ApiError && (e.isForbidden || e.isNotFound)) return { status: 'unknown' };
        // Network failures are transient: don't pin them in the cache.
        transient = !(e instanceof ApiError) || e.code === 'network';
        return { status: 'error', message: e instanceof Error ? e.message : String(e) };
      })
      .then((r) => {
        inflight.delete(key);
        if (!transient) store(key, r);
        return r;
      });
    inflight.set(key, p);
  }
  return p;
}

/**
 * Debounced rendering of `conditions` (null = nothing to render, e.g. incomplete builder rows).
 * `delay = 0` renders immediately (list summaries).
 */
export function useFilterRender(conditions: readonly Condition[] | null, delay = 300): RenderState {
  const key = conditions ? conditionsKey(conditions) : null;
  const cached = useRenderCache((s) => (key ? s.results[key] : undefined));
  useEffect(() => {
    if (!key || !conditions || !conditions.length || useRenderCache.getState().results[key]) return;
    const t = setTimeout(() => void renderConditions(conditions), delay);
    return () => clearTimeout(t);
    // conditions are captured through `key`
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, delay]);
  if (!conditions) return { status: 'idle' };
  if (!conditions.length) return { status: 'ok', rendered: '' };
  return cached ?? { status: 'checking' };
}

/** One-line WHEN summary of a route for lists: canonical text once rendered. */
export function useConditionSummary(conditions: readonly Condition[]): string {
  const r = useFilterRender(conditions, 0);
  useT(); // re-render on language change
  if (!conditions.length) return t('routing.everyMessage');
  if (r.status === 'ok') return r.rendered;
  return t('routing.conditionsCount', { count: conditions.length });
}
