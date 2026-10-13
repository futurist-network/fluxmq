import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { detectLang, getStoredLang, getTheme, setStoredLang, setTheme } from './settings';

class MemoryStorage {
  m = new Map<string, string>();
  getItem(k: string) {
    return this.m.has(k) ? this.m.get(k)! : null;
  }
  setItem(k: string, v: string) {
    this.m.set(k, v);
  }
  removeItem(k: string) {
    this.m.delete(k);
  }
}

const g = globalThis as unknown as { window?: unknown; document?: unknown };

describe('language setting', () => {
  let storage: MemoryStorage;
  beforeEach(() => {
    storage = new MemoryStorage();
    g.window = { localStorage: storage };
    g.document = { documentElement: { dataset: {} as Record<string, string>, lang: '' } };
  });
  afterEach(() => {
    delete g.window;
    delete g.document;
  });

  it('defaults from the browser language: ru* → ru, anything else → en', () => {
    expect(detectLang(['ru-RU', 'en'])).toBe('ru');
    expect(detectLang(['ru'])).toBe('ru');
    expect(detectLang(['RU-ua'])).toBe('ru');
    expect(detectLang(['en-US', 'ru'])).toBe('en');
    expect(detectLang(['de-DE'])).toBe('en');
    expect(detectLang(['rus'])).toBe('en');
    expect(detectLang([])).toBe('en');
  });

  it('persists the chosen language next to the theme', () => {
    setStoredLang('ru');
    expect(storage.getItem('fluxmq.lang')).toBe('ru');
    expect(getStoredLang()).toBe('ru');
    setStoredLang('en');
    expect(getStoredLang()).toBe('en');
    setTheme('system');
    expect(storage.getItem('fluxmq.theme')).toBeNull();
    setTheme('light');
    expect(storage.getItem('fluxmq.theme')).toBe('light');
    expect(getTheme()).toBe('light');
  });

  it('ignores garbage in storage', () => {
    storage.setItem('fluxmq.lang', 'klingon');
    expect(['en', 'ru']).toContain(getStoredLang());
  });

  it('survives storage that throws (private mode)', () => {
    g.window = {
      get localStorage(): Storage {
        throw new Error('SecurityError');
      },
    };
    expect(() => setStoredLang('ru')).not.toThrow();
    expect(['en', 'ru']).toContain(getStoredLang());
  });

  it('without a window (SSR/tests) the language is English', () => {
    delete g.window;
    expect(getStoredLang()).toBe('en');
  });
});
