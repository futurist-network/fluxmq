import { afterEach, describe, expect, it } from 'vitest';
import { isValidElement, type ReactElement } from 'react';
import { en, enNamespaces } from './en';
import { ru, ruNamespaces } from './ru';
import { getLang, LOCALE, rich, setLang, t, template, translate, type TKey } from './index';

type AnyEntry = string | Record<string, string>;
const enD = en as unknown as Record<string, AnyEntry>;
const ruD = ru as unknown as Record<string, AnyEntry>;

const params = (s: string) => [...s.matchAll(/\{(\w+)\}/g)].map((m) => m[1]).sort();
const texts = (e: AnyEntry) => (typeof e === 'string' ? [e] : Object.values(e));

afterEach(() => setLang('en'));

describe('dictionary parity', () => {
  it('ru has every en key and vice versa', () => {
    const enKeys = Object.keys(en).sort();
    const ruKeys = Object.keys(ru).sort();
    expect(ruKeys.filter((k) => !(k in en))).toEqual([]);
    expect(enKeys.filter((k) => !(k in ru))).toEqual([]);
  });

  it('namespaces are disjoint and keys carry their namespace prefix', () => {
    const seen = new Map<string, string>();
    for (const [ns, dict] of Object.entries(enNamespaces)) {
      for (const k of Object.keys(dict)) {
        expect(seen.get(k), `${k} defined in ${seen.get(k)} and ${ns}`).toBeUndefined();
        seen.set(k, ns);
        expect(k.startsWith(`${ns}.`), `${k} should start with "${ns}."`).toBe(true);
      }
    }
    expect(Object.keys(ruNamespaces).sort()).toEqual(Object.keys(enNamespaces).sort());
    for (const [ns, dict] of Object.entries(ruNamespaces)) {
      expect(Object.keys(dict).sort(), ns).toEqual(Object.keys(enNamespaces[ns as keyof typeof enNamespaces]).sort());
    }
  });

  it('no empty strings', () => {
    for (const [k, v] of [...Object.entries(enD), ...Object.entries(ruD)]) {
      for (const s of texts(v)) expect(s.trim(), k).not.toBe('');
    }
  });

  it('entry shapes match: strings stay strings, plurals have the right forms', () => {
    for (const k of Object.keys(enD)) {
      const e = enD[k]!;
      const r = ruD[k]!;
      expect(typeof r, k).toBe(typeof e);
      if (typeof e !== 'string') {
        expect(Object.keys(e).sort(), k).toEqual(['one', 'other']);
        expect(Object.keys(r).sort(), k).toEqual(['few', 'many', 'one', 'other']);
      }
    }
  });

  it('interpolation params match between languages (and across plural forms)', () => {
    for (const k of Object.keys(enD)) {
      const all = [...texts(enD[k]!), ...texts(ruD[k]!)].map((s) => params(s).filter((p) => p !== 'count'));
      for (const p of all) expect(p, k).toEqual(all[0]);
    }
  });

  it('Russian text is actually Russian (no untranslated English sentences slipped in)', () => {
    // Technical terms that are the same in both languages by design (AWS/S3 field names).
    const SAME_BY_DESIGN = new Set(['backups.config.accessKeyId', 'backups.config.secretAccessKey']);
    const suspicious = Object.entries(ruD)
      .filter(([k, v]) => !SAME_BY_DESIGN.has(k) && typeof v === 'string' && v === enD[k] && /[a-z]{3,} [a-z]{3,}/i.test(v))
      .map(([k]) => k);
    expect(suspicious).toEqual([]);
  });
});

describe('t()', () => {
  it('interpolates params and leaves unknown ones visible', () => {
    expect(translate('en', 'shell.pal.route', { name: 'hv' })).toBe('Route: hv');
    expect(translate('ru', 'shell.pal.route', { name: 'hv' })).toBe('Маршрут: hv');
    expect(translate('en', 'shell.pal.route')).toBe('Route: {name}');
  });

  it('follows the active language', () => {
    setLang('ru');
    expect(getLang()).toBe('ru');
    expect(t('common.save')).toBe('Сохранить');
    setLang('en');
    expect(t('common.save')).toBe('Save');
  });

  it('rich() substitutes React nodes', () => {
    const node = rich('shell.notFound.body', { keys: 'K' }) as ReactElement<{ children: unknown[] }>;
    expect(isValidElement(node)).toBe(true);
    expect(node.props.children).toEqual(['Press ', 'K', ' to jump to any channel, consumer group or page.']);
  });
});

describe('plural rules', () => {
  const key = 'common.messages' as TKey;
  it.each([
    [0, '0 messages', '0 сообщений'],
    [1, '1 message', '1 сообщение'],
    [2, '2 messages', '2 сообщения'],
    [4, '4 messages', '4 сообщения'],
    [5, '5 messages', '5 сообщений'],
    [11, '11 messages', '11 сообщений'],
    [12, '12 messages', '12 сообщений'],
    [21, '21 messages', '21 сообщение'],
    [22, '22 messages', '22 сообщения'],
    [25, '25 messages', '25 сообщений'],
    [101, '101 messages', '101 сообщение'],
    [111, '111 messages', '111 сообщений'],
  ])('%i', (n, enText, ruText) => {
    expect(translate('en', key, { count: n })).toBe(enText);
    expect(translate('ru', key, { count: n })).toBe(ruText);
  });

  it('fractions use "other" and counts get locale grouping', () => {
    expect(translate('ru', key, { count: 1.5 })).toBe('1,5 сообщения');
    expect(translate('en', key, { count: 12345 })).toBe('12,345 messages');
    expect(translate('ru', key, { count: 12345 })).toBe('12 345 сообщений');
  });

  it('template() picks the CLDR category', () => {
    expect(template('ru', key, 3)).toBe('{count} сообщения');
    expect(template('ru', key, 7)).toBe('{count} сообщений');
    expect(LOCALE.ru).toBe('ru-RU');
  });
});
