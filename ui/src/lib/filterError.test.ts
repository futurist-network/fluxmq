import { afterEach, describe, expect, it, vi } from 'vitest';
import { api, ApiError } from '../api/client';
import { byteToIndex, errorSpan, filterErrorInfo } from './filterError';

const bytes = (s: string) => new TextEncoder().encode(s).length;

describe('byte offset → string index', () => {
  it('is the identity for ASCII', () => {
    expect(byteToIndex('a = 1 OR b', 6)).toBe(6);
    expect(byteToIndex('abc', 99)).toBe(3);
    expect(byteToIndex('', 0)).toBe(0);
  });

  it('handles 2-, 3- and 4-byte characters', () => {
    for (const text of ['naïve = 1 AND x >= OR', 'städte = "€" AND OR', 'emoji = "🚀🚀" AND OR', '名前 = 1 OR']) {
      const at = text.lastIndexOf('OR');
      expect(byteToIndex(text, bytes(text.slice(0, at)))).toBe(at);
    }
  });

  it('maps an offset inside a multi-byte character to its start', () => {
    // '€' is 3 bytes at index 1 (byte 1..3)
    expect(byteToIndex('a€b', 2)).toBe(1);
    expect(byteToIndex('a€b', 4)).toBe(2);
    // '🚀' is 2 UTF-16 units / 4 bytes
    expect(byteToIndex('🚀x', 4)).toBe(2);
  });
});

describe('filter errors from the server', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('reads ApiError.position from the body (not from the message text)', async () => {
    const text = 'naïve = 1 AND total >= OR';
    const position = bytes('naïve = 1 AND total >= ');
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => new Response(JSON.stringify({ error: 'bad_request', message: `filter syntax error at ${position}: expected a value, found 'OR'`, position }), { status: 400 })),
    );
    const err = await api.parseFilter(text).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).position).toBe(position);
    const info = filterErrorInfo(text, err);
    expect(info.detail).toBe("expected a value, found 'OR'");
    expect(text.slice(info.index!)).toBe('OR');
    expect(errorSpan(text, info.index!)).toEqual([text.length - 2, text.length]);
  });

  it('has no position when the body has none, even if the message mentions one', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ error: 'bad_request', message: 'limit at 5 is too low' }), { status: 400 })));
    const err = await api.parseFilter('x').catch((e: unknown) => e);
    expect((err as ApiError).position).toBeNull();
    expect(filterErrorInfo('x', err)).toEqual({ message: 'limit at 5 is too low', detail: 'limit at 5 is too low', index: null });
  });
});
