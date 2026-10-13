/**
 * Number / time formatting used everywhere in the console. One place so that
 * "1.4M msg/s", "2.8 ms", "4.8 TB" and "3m ago" look identical on every screen.
 *
 * Locale-aware: every function formats for the active console language (src/i18n). (No `lang`
 * parameter on purpose: these are passed straight to `.map()` and chart callbacks.) English keeps the compact operator style ("1.4M", "3h 12m",
 * "4m ago"); Russian uses Russian decimal commas, grouping, unit abbreviations ("1,4 млн",
 * "3 ч 12 мин") and full relative phrases ("5 минут назад") via Intl.
 */
import { getLang, LOCALE, type Lang } from '../i18n';

const DASH = '—';
const NBSP = ' ';

interface Units {
  count: [string, string, string, string];
  msgRate: string;
  ms: string;
  s: string;
  min: string;
  h: string;
  d: string;
  bytes: string[];
  perSec: string;
  justNow: string;
  forever: string;
  days: Partial<Record<Intl.LDMLPluralRule, string>>;
  hours: Partial<Record<Intl.LDMLPluralRule, string>>;
}

const UNITS: Record<Lang, Units> = {
  en: {
    count: ['K', 'M', 'B', 'T'],
    msgRate: 'msg/s',
    ms: 'ms',
    s: 's',
    min: 'min',
    h: 'h',
    d: 'd',
    bytes: ['B', 'KB', 'MB', 'GB', 'TB', 'PB'],
    perSec: '/s',
    justNow: 'just now',
    forever: 'Forever',
    days: { one: 'day', other: 'days' },
    hours: { one: 'hour', other: 'hours' },
  },
  ru: {
    count: [`${NBSP}тыс.`, `${NBSP}млн`, `${NBSP}млрд`, `${NBSP}трлн`],
    msgRate: 'сообщ./с',
    ms: 'мс',
    s: 'с',
    min: 'мин',
    h: 'ч',
    d: 'д',
    bytes: ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ', 'ПБ'],
    perSec: '/с',
    justNow: 'только что',
    forever: 'Бессрочно',
    days: { one: 'день', few: 'дня', many: 'дней', other: 'дня' },
    hours: { one: 'час', few: 'часа', many: 'часов', other: 'часа' },
  },
};

const cur = (): Lang => getLang();

function trimZeros(s: string): string {
  return s.includes('.') ? s.replace(/\.?0+$/, '') : s;
}

/** Decimal separator per language ("1.5" / "1,5"); the digits come from toFixed + trimZeros. */
function dec(s: string, lang: Lang): string {
  return lang === 'ru' ? s.replace('.', ',') : s;
}

const nfCache = new Map<string, Intl.NumberFormat>();
function nf(locale: string, opts: Intl.NumberFormatOptions): Intl.NumberFormat {
  const k = `${locale}|${JSON.stringify(opts)}`;
  let f = nfCache.get(k);
  if (!f) {
    f = new Intl.NumberFormat(locale, opts);
    nfCache.set(k, f);
  }
  return f;
}

/** Compact count: 0, 999, 1.2K, 18K, 1.4M, 2.1B (ru: 1,2 тыс., 1,4 млн). */
export function fmtCount(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return DASH;
  const l = cur();
  const a = Math.abs(n);
  const sign = n < 0 ? '-' : '';
  if (a < 1000) return sign + dec(Number.isInteger(a) ? String(a) : trimZeros(a.toFixed(a < 10 ? 1 : 0)), l);
  const suffix = UNITS[l].count;
  const steps = [1e3, 1e6, 1e9, 1e12];
  let i = steps.findIndex((_u, idx) => idx === steps.length - 1 || a < steps[idx + 1]!);
  const v = a / steps[i]!;
  // 1.2K, 18K, 184K — keep 2–3 significant digits.
  let str = v >= 100 ? v.toFixed(0) : trimZeros(v.toFixed(1));
  // Rounding can reach the next unit (999.96K → "1000K"): promote to "1M".
  if (str === '1000' && i < steps.length - 1) {
    i++;
    str = '1';
  }
  return sign + dec(str, l) + suffix[i]!;
}

/** Full integer with locale grouping: 1,284,402 (ru: 1 284 402). */
export function fmtInt(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return DASH;
  return nf(LOCALE[cur()], { maximumFractionDigits: 0 }).format(Math.round(n));
}

/** Plain number in the active locale (decimal comma in Russian), up to `digits` fraction digits. */
export function fmtNumber(n: number | null | undefined, digits = 2): string {
  if (n == null || !Number.isFinite(n)) return DASH;
  return nf(LOCALE[cur()], { maximumFractionDigits: digits }).format(n);
}

/** Message rate: "42.4K msg/s". Pass `unit=false` for the bare number. */
export function fmtRate(n: number | null | undefined, unit = true): string {
  if (n == null || !Number.isFinite(n)) return DASH;
  const l = cur();
  const v = n > 0 && n < 1 ? dec(trimZeros(n.toFixed(2)), l) : fmtCount(n);
  return unit ? `${v} ${UNITS[l].msgRate}` : v;
}

/** Latency in ms: "0.42 ms", "2.8 ms", "38 ms", "1.2 s", "3.4 min". */
export function fmtMs(ms: number | null | undefined, unit = true): string {
  if (ms == null || !Number.isFinite(ms)) return DASH;
  const l = cur();
  const U = UNITS[l];
  let v: string;
  let u: string;
  if (ms < 1) {
    v = trimZeros(ms.toFixed(2));
    u = U.ms;
  } else if (ms < 10) {
    v = trimZeros(ms.toFixed(1));
    u = U.ms;
  } else if (ms < 1000) {
    v = ms.toFixed(0);
    u = U.ms;
  } else if (ms < 60_000) {
    v = trimZeros((ms / 1000).toFixed(1));
    u = U.s;
  } else {
    v = trimZeros((ms / 60_000).toFixed(1));
    u = U.min;
  }
  v = dec(v, l);
  return unit ? `${v} ${u}` : v;
}

/** Split value and unit (for big metric numbers with a smaller unit label). */
export function splitUnit(s: string): [string, string] {
  const i = s.indexOf(' ');
  return i === -1 ? [s, ''] : [s.slice(0, i), s.slice(i + 1)];
}

/** Bytes, binary-ish but labelled the way operators expect: "512 B", "4.2 KB", "4.8 TB" (ru: "4,2 КБ"). */
export function fmtBytes(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return DASH;
  const l = cur();
  const units = UNITS[l].bytes;
  let v = Math.abs(n);
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  const s = i === 0 ? v.toFixed(0) : v >= 100 ? v.toFixed(0) : trimZeros(v.toFixed(1));
  return `${n < 0 ? '-' : ''}${dec(s, l)} ${units[i]}`;
}

export function fmtBytesRate(n: number | null | undefined): string {
  return n == null ? DASH : `${fmtBytes(n)}${UNITS[cur()].perSec}`;
}

export function fmtPercent(n: number | null | undefined, digits = 0): string {
  if (n == null || !Number.isFinite(n)) return DASH;
  const l = cur();
  // Russian typography puts a (non-breaking) space before the percent sign.
  return l === 'ru' ? `${dec(n.toFixed(digits), l)}${NBSP}%` : `${n.toFixed(digits)}%`;
}

/** Duration in seconds: "45s", "12m", "3h 12m", "14d 6h" (ru: "45 с", "3 ч 12 мин", "14 д 6 ч"). */
export function fmtDuration(secs: number | null | undefined): string {
  if (secs == null || !Number.isFinite(secs) || secs < 0) return DASH;
  const l = cur();
  const U = UNITS[l];
  // English: "3h" glued; Russian: "3 ч" with a non-breaking space.
  const p = (v: number, u: string) => (l === 'ru' ? `${v}${NBSP}${u}` : `${v}${u === U.min ? 'm' : u}`);
  const s = Math.floor(secs);
  if (s < 60) return p(s, U.s);
  const m = Math.floor(s / 60);
  if (m < 60) return p(m, U.min);
  const h = Math.floor(m / 60);
  if (h < 24) return m % 60 ? `${p(h, U.h)} ${p(m % 60, U.min)}` : p(h, U.h);
  const d = Math.floor(h / 24);
  return h % 24 ? `${p(d, U.d)} ${p(h % 24, U.h)}` : p(d, U.d);
}

const rtfCache = new Map<string, Intl.RelativeTimeFormat>();
function rtf(lang: Lang): Intl.RelativeTimeFormat {
  let f = rtfCache.get(lang);
  if (!f) {
    // English keeps the compact operator style ("4m ago"); Russian reads naturally ("4 минуты назад").
    f = new Intl.RelativeTimeFormat(LOCALE[lang], { style: lang === 'en' ? 'narrow' : 'long', numeric: 'always' });
    rtfCache.set(lang, f);
  }
  return f;
}

/** Relative time: "just now", "12s ago", "4m ago", "3h ago", "2d ago", or "in 5s" (ru: "5 минут назад"). */
export function fmtRelative(ts: number | null | undefined, now: number = Date.now()): string {
  if (ts == null || !Number.isFinite(ts)) return DASH;
  const l = cur();
  const diff = Math.round((now - ts) / 1000);
  const a = Math.abs(diff);
  if (a < 5) return UNITS[l].justNow;
  const sign = diff < 0 ? 1 : -1;
  if (a < 60) return rtf(l).format(sign * a, 'second');
  if (a < 3600) return rtf(l).format(sign * Math.floor(a / 60), 'minute');
  if (a < 86_400) return rtf(l).format(sign * Math.floor(a / 3600), 'hour');
  return rtf(l).format(sign * Math.floor(a / 86_400), 'day');
}

const pad = (n: number, w = 2) => String(n).padStart(w, '0');

const dtfCache = new Map<string, Intl.DateTimeFormat>();
function dtf(lang: Lang, opts: Intl.DateTimeFormatOptions): Intl.DateTimeFormat {
  const k = `${lang}|${JSON.stringify(opts)}`;
  let f = dtfCache.get(k);
  if (!f) {
    f = new Intl.DateTimeFormat(LOCALE[lang], opts);
    dtfCache.set(k, f);
  }
  return f;
}

/** "14:03:22.481" — local time with milliseconds (DevTools style, same in every locale). */
export function fmtTimeMs(ts: number): string {
  const d = new Date(ts);
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
}

/** "Oct 7, 14:03:22" (ru: "7 окт., 14:03:22"). */
export function fmtDateTime(ts: number | null | undefined): string {
  if (ts == null) return DASH;
  return dtf(cur(), { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' }).format(new Date(ts));
}

/** "Jul 26, 2027" (ru: "26 июл. 2027 г.") — calendar date with the year. */
export function fmtDate(ts: number | null | undefined): string {
  if (ts == null) return DASH;
  return dtf(cur(), { month: 'short', day: 'numeric', year: 'numeric' }).format(new Date(ts));
}

/** Full ISO-ish with ms for metadata panels: "2026-10-07 14:03:22.481" (locale-neutral on purpose). */
export function fmtTimestampFull(ts: number): string {
  const d = new Date(ts);
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${fmtTimeMs(ts)}`;
}

/** Axis label for chart time ticks (24-hour clock in both languages). */
export function fmtClock(ts: number, withSeconds = false): string {
  const d = new Date(ts);
  return withSeconds ? `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}` : `${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

/** "1 s", "10 s", "1 min", "5 min" — for backoff lists (ru: "10 с", "5 мин"). */
export function fmtBackoff(ms: number): string {
  const l = cur();
  const U = UNITS[l];
  if (ms < 1000) return `${ms} ${U.ms}`;
  if (ms < 60_000) return `${dec(trimZeros((ms / 1000).toFixed(1)), l)} ${U.s}`;
  if (ms < 3_600_000) return `${dec(trimZeros((ms / 60_000).toFixed(1)), l)} ${U.min}`;
  return `${dec(trimZeros((ms / 3_600_000).toFixed(1)), l)} ${U.h}`;
}

const prCache = new Map<Lang, Intl.PluralRules>();
function pluralForm(lang: Lang, n: number, forms: Partial<Record<Intl.LDMLPluralRule, string>>): string {
  let r = prCache.get(lang);
  if (!r) {
    r = new Intl.PluralRules(LOCALE[lang]);
    prCache.set(lang, r);
  }
  return forms[r.select(n)] ?? forms.other ?? '';
}

/** Retention age: "7 days", "24 hours", "Forever" (ru: "7 дней", "24 часа", "Бессрочно"). */
export function fmtRetention(secs: number | null): string {
  const l = cur();
  if (secs == null) return UNITS[l].forever;
  if (secs % 86_400 === 0) {
    const d = secs / 86_400;
    return `${fmtInt(d)} ${pluralForm(l, d, UNITS[l].days)}`;
  }
  if (secs % 3600 === 0) {
    const h = secs / 3600;
    return `${fmtInt(h)} ${pluralForm(l, h, UNITS[l].hours)}`;
  }
  return fmtDuration(secs);
}

/**
 * English-only noun pluralisation, kept for technical strings. User-visible counts use a plural
 * dictionary entry instead: `t('ns.key', { count })` (Russian needs one/few/many).
 */
export function plural(n: number, one: string, many = `${one}s`): string {
  return `${n.toLocaleString('en-US')} ${n === 1 ? one : many}`;
}
