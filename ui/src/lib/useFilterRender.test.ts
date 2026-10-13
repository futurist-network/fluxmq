import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Condition } from '../api/types';
import { conditionsKey, renderConditions, useRenderCache } from './useFilterRender';

const c = (name: string, value: number): Condition => ({ field: { kind: 'payload', name }, op: 'gte', value });

describe('builder → text via POST /api/filter/render', () => {
  beforeEach(() => useRenderCache.setState({ results: {} }));
  afterEach(() => vi.unstubAllGlobals());

  it('posts the conditions, caches the result and merges concurrent requests', async () => {
    const fetch = vi.fn(async (_url: string, init?: RequestInit) => {
      const { conditions } = JSON.parse(String(init?.body)) as { conditions: Condition[] };
      return new Response(JSON.stringify({ rendered: `${conditions[0]!.field.kind === 'payload' ? (conditions[0]!.field as { name: string }).name : '?'} >= ${String(conditions[0]!.value)}` }), { status: 200 });
    });
    vi.stubGlobal('fetch', fetch);
    const conds = [c('total', 250)];
    const [a, b] = await Promise.all([renderConditions(conds), renderConditions(conds)]);
    expect(a).toEqual({ status: 'ok', rendered: 'total >= 250' });
    expect(b).toEqual(a);
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch.mock.calls[0]![0]).toBe('/api/filter/render');
    expect(useRenderCache.getState().results[conditionsKey(conds)]).toEqual(a);
    await renderConditions(conds);
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it('renders the empty list locally', async () => {
    const fetch = vi.fn();
    vi.stubGlobal('fetch', fetch);
    expect(await renderConditions([])).toEqual({ status: 'ok', rendered: '' });
    expect(fetch).not.toHaveBeenCalled();
  });

  it('reports invalid conditions (400) and servers without the endpoint', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ error: 'bad_request', message: 'condition 1: value is required' }), { status: 400 })));
    expect(await renderConditions([c('a', 1)])).toEqual({ status: 'error', message: 'condition 1: value is required' });
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ error: 'not_found', message: 'no route' }), { status: 404 })));
    expect(await renderConditions([c('b', 1)])).toEqual({ status: 'unknown' });
  });

  it('does not cache network failures', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => Promise.reject(new TypeError('Failed to fetch'))));
    const r = await renderConditions([c('x', 1)]);
    expect(r.status).toBe('error');
    expect(useRenderCache.getState().results[conditionsKey([c('x', 1)])]).toBeUndefined();
  });
});
