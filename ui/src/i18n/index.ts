/**
 * In-house i18n: typed flat dictionaries (`en.ts` is the source of truth, `ru.ts` is typed
 * against it), a zustand store for the active language, `t()` with `{param}` interpolation and
 * CLDR plural forms via Intl.PluralRules. No dependency beyond what the console already ships.
 *
 *   const t = useT();                      // in components: re-renders on language change
 *   t('nav.channels')                      // plain string
 *   t('common.items', { count: 3 })        // plural entry, picks one/few/many/other
 *   t.rich('x.deleteConfirm', { name: <b>{n}</b> })   // ReactNode params
 *
 * Outside React (toasts, validators, lib code) call the plain `t()`: it reads the current language.
 */
import { createElement, Fragment, type ReactNode } from 'react';
import { create } from 'zustand';
import { en, type Dict, type TKey } from './en';
import { ru } from './ru';
import { getStoredLang, setStoredLang } from '../lib/settings';
import type { Entry, Lang } from './types';

export type { Lang, TKey };
export { LANGS } from './types';

const DICTS: Record<Lang, Record<string, Entry | Record<string, string>>> = { en, ru };

/** BCP 47 tag used for Intl formatting. */
export const LOCALE: Record<Lang, string> = { en: 'en-US', ru: 'ru-RU' };

/** Native language names for the switcher (never translated). */
export const LANG_NAME: Record<Lang, string> = { en: 'English', ru: 'Русский' };

interface LangStore {
  lang: Lang;
  setLang: (l: Lang) => void;
}

function applyHtmlLang(lang: Lang) {
  if (typeof document !== 'undefined') document.documentElement.lang = lang;
}

export const useLang = create<LangStore>()((set) => ({
  lang: getStoredLang(),
  setLang: (lang) => {
    setStoredLang(lang);
    applyHtmlLang(lang);
    set({ lang });
  },
}));

applyHtmlLang(useLang.getState().lang);

export const getLang = (): Lang => useLang.getState().lang;
export const setLang = (l: Lang): void => useLang.getState().setLang(l);
export const getLocale = (): string => LOCALE[getLang()];

export type Params = Record<string, string | number | null | undefined>;

const pluralRules = new Map<string, Intl.PluralRules>();
function pluralCategory(locale: string, n: number): Intl.LDMLPluralRule {
  let r = pluralRules.get(locale);
  if (!r) {
    r = new Intl.PluralRules(locale);
    pluralRules.set(locale, r);
  }
  return r.select(n);
}

const intFmt = new Map<string, Intl.NumberFormat>();
/** `{count}` is rendered with locale grouping (1,284 / 1 284); other numbers verbatim (ports, ids). */
function fmtCountParam(locale: string, n: number): string {
  let f = intFmt.get(locale);
  if (!f) {
    f = new Intl.NumberFormat(locale, { maximumFractionDigits: 2 });
    intFmt.set(locale, f);
  }
  return f.format(n);
}

/** Resolve the raw template (plural form chosen) for a key in a language; falls back to English. */
export function template(lang: Lang, key: TKey, count?: number): string {
  const entry = (DICTS[lang][key] ?? DICTS.en[key]) as Entry | Record<string, string> | undefined;
  if (entry == null) return key;
  if (typeof entry === 'string') return entry;
  const forms = entry as Record<string, string>;
  const cat = count == null ? 'other' : pluralCategory(LOCALE[lang], count);
  return forms[cat] ?? forms.other ?? key;
}

const PARAM = /\{(\w+)\}/g;

function interpolate(tpl: string, params: Params | undefined, locale: string): string {
  if (!params) return tpl;
  return tpl.replace(PARAM, (m, name: string) => {
    const v = params[name];
    if (v == null) return m;
    if (name === 'count' && typeof v === 'number') return fmtCountParam(locale, v);
    return String(v);
  });
}

/** Translate in an explicit language (tests, previews of the other language). */
export function translate(lang: Lang, key: TKey, params?: Params): string {
  const count = typeof params?.count === 'number' ? params.count : undefined;
  return interpolate(template(lang, key, count), params, LOCALE[lang]);
}

/** Translate in the current language. Not reactive by itself — use `useT()` in components. */
export function t(key: TKey, params?: Params): string {
  return translate(getLang(), key, params);
}

export type RichParams = Record<string, ReactNode>;

/** Like `t` but params may be React nodes (bold names, links, <kbd>). */
export function rich(key: TKey, params: RichParams): ReactNode {
  return richIn(getLang(), key, params);
}

function richIn(lang: Lang, key: TKey, params: RichParams): ReactNode {
  const count = typeof params.count === 'number' ? params.count : undefined;
  const tpl = template(lang, key, count);
  const parts: ReactNode[] = [];
  let last = 0;
  for (const m of tpl.matchAll(PARAM)) {
    const name = m[1]!;
    if (!(name in params)) continue;
    if (m.index > last) parts.push(tpl.slice(last, m.index));
    const v = params[name];
    parts.push(name === 'count' && typeof v === 'number' ? fmtCountParam(LOCALE[lang], v) : v);
    last = m.index + m[0].length;
  }
  if (last < tpl.length) parts.push(tpl.slice(last));
  return createElement(Fragment, null, ...parts);
}

export interface TFn {
  (key: TKey, params?: Params): string;
  rich: (key: TKey, params: RichParams) => ReactNode;
  lang: Lang;
  locale: string;
}

const tCache = new Map<Lang, TFn>();
function tFor(lang: Lang): TFn {
  let fn = tCache.get(lang);
  if (!fn) {
    const f = ((key: TKey, params?: Params) => translate(lang, key, params)) as TFn;
    f.rich = (key, params) => richIn(lang, key, params);
    f.lang = lang;
    f.locale = LOCALE[lang];
    fn = f;
    tCache.set(lang, fn);
  }
  return fn;
}

/** Translation hook: subscribes the component to language changes (stable per language). */
export function useT(): TFn {
  const lang = useLang((s) => s.lang);
  return tFor(lang);
}

/** Is this a key of the dictionary (for mapping server codes to messages)? */
export function hasKey(key: string): key is TKey {
  return Object.prototype.hasOwnProperty.call(en, key);
}

export type { Dict };
