/** Persistent per-browser settings (admin token, theme, language). All storage access is guarded. */

const TOKEN_KEY = 'fluxmq.adminToken';
const THEME_KEY = 'fluxmq.theme';
const HINTS_KEY = 'fluxmq.hints';
const LANG_KEY = 'fluxmq.lang';

export type Theme = 'dark' | 'light';
/** What the user chose: a theme, or follow the operating system (the default). */
export type ThemePref = Theme | 'system';
export type StoredLang = 'en' | 'ru';

function read(key: string): string | null {
  try {
    return window.localStorage.getItem(key);
  } catch {
    return null;
  }
}

function write(key: string, value: string | null): void {
  try {
    if (value == null || value === '') window.localStorage.removeItem(key);
    else window.localStorage.setItem(key, value);
  } catch {
    /* storage unavailable (private mode) — settings are session-only */
  }
}

let tokenCache: string | null | undefined;
const tokenListeners = new Set<(t: string | null) => void>();

export function getToken(): string | null {
  if (tokenCache === undefined) tokenCache = typeof window === 'undefined' ? null : read(TOKEN_KEY);
  return tokenCache;
}

export function setToken(token: string | null): void {
  const t = token?.trim() || null;
  tokenCache = t;
  write(TOKEN_KEY, t);
  tokenListeners.forEach((l) => l(t));
}

export function onTokenChange(l: (t: string | null) => void): () => void {
  tokenListeners.add(l);
  return () => tokenListeners.delete(l);
}

export function getThemePref(): ThemePref {
  const v = read(THEME_KEY);
  return v === 'light' || v === 'dark' ? v : 'system';
}

/** The operating system's preference (dark when unknown). */
export function systemTheme(): Theme {
  return typeof window !== 'undefined' && window.matchMedia?.('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
}

export function resolveTheme(pref: ThemePref): Theme {
  return pref === 'system' ? systemTheme() : pref;
}

/** The theme in effect now. */
export function getTheme(): Theme {
  return resolveTheme(getThemePref());
}

/** Stores the choice (`system` clears it) and applies the resulting theme. */
export function setTheme(pref: ThemePref): void {
  write(THEME_KEY, pref === 'system' ? null : pref);
  document.documentElement.dataset.theme = resolveTheme(pref);
}

/** Browser default: any `ru*` tag → Russian, everything else → English. */
export function detectLang(languages?: readonly string[]): StoredLang {
  const list = languages ?? (typeof navigator !== 'undefined' && typeof window !== 'undefined' ? (navigator.languages?.length ? navigator.languages : [navigator.language]) : []);
  const first = list.find((l) => !!l);
  return first && /^ru\b/i.test(first) ? 'ru' : 'en';
}

/** Stored language, or the browser's preference when none was chosen yet. */
export function getStoredLang(): StoredLang {
  if (typeof window === 'undefined') return 'en';
  const v = read(LANG_KEY);
  return v === 'ru' || v === 'en' ? v : detectLang();
}

export function setStoredLang(lang: StoredLang): void {
  write(LANG_KEY, lang);
}

/** Page descriptions and explanatory blocks (`.explain`), hidden by default; form field hints always stay. */
export function getHints(): boolean {
  return read(HINTS_KEY) === 'on';
}

export function setHints(on: boolean): void {
  write(HINTS_KEY, on ? 'on' : null);
  document.documentElement.dataset.hints = on ? 'on' : 'off';
}
