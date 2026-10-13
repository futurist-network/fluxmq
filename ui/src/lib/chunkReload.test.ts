import { afterEach, describe, expect, it, vi } from 'vitest';
import { isChunkLoadError, reloadForNewBuild } from './chunkReload';

describe('chunk reload', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('recognises failed dynamic imports only', () => {
    expect(isChunkLoadError(new TypeError('Failed to fetch dynamically imported module: http://x/assets/A-1.js'))).toBe(true);
    expect(isChunkLoadError(new TypeError('error loading dynamically imported module'))).toBe(true);
    expect(isChunkLoadError(new Error('Importing a module script failed.'))).toBe(true);
    expect(isChunkLoadError(new Error('Cannot read properties of undefined'))).toBe(false);
    expect(isChunkLoadError(null)).toBe(false);
  });

  it('reloads at most once per 10 s', () => {
    const reload = vi.fn();
    vi.stubGlobal('window', { location: { reload } });
    const store = new Map<string, string>();
    const storage = { getItem: (k: string) => store.get(k) ?? null, setItem: (k: string, v: string) => void store.set(k, v) };
    expect(reloadForNewBuild(100_000, storage)).toBe(true);
    expect(reloadForNewBuild(105_000, storage)).toBe(false);
    expect(reloadForNewBuild(111_000, storage)).toBe(true);
    expect(reload).toHaveBeenCalledTimes(2);
  });
});
