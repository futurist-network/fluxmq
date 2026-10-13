/**
 * Alert and event texts built on the client from `kind + params` (i18n keys), so Russian gets a
 * real sentence instead of the broker's English. Brokers before v0.9.1 send no `params`: the
 * values are then parsed from the English detail they always sent (format pinned by
 * crates/fluxmq-telemetry/src/alerts.rs). Unknown kinds or unparsable texts fall back to the
 * server strings.
 */
import type { Alert, AlertKind, FluxEvent } from '../api/types';
import { getLang, t, type TKey } from '../i18n';
import { ALERT_KIND_LABEL, EVENT_KIND_LABEL } from './labels';
import { fmtBytes, fmtCount, fmtDuration, fmtMs, fmtNumber, fmtPercent, fmtRate } from './format';

export type Params = Record<string, number | string | boolean>;

export interface AlertText {
  title: string;
  detail: string;
  reason: string | null;
  /** Compact "why" for status tooltips: `worker-03#7: p99 39× the group`. */
  short: string;
}

// ---------------------------------------------------------------------------------------------
// Parsing the legacy English detail (brokers without `params`)
// ---------------------------------------------------------------------------------------------

const SUFFIX: Record<string, number> = { k: 1e3, K: 1e3, M: 1e6, B: 1e9 };

/** `950` / `1.2k` / `18k` / `2.5M` → number. */
export function parseCount(s: string | undefined): number | undefined {
  if (!s) return undefined;
  const m = /^([\d.]+)([kKMB]?)$/.exec(s.trim());
  if (!m) return undefined;
  const v = Number(m[1]) * (m[2] ? SUFFIX[m[2]]! : 1);
  return Number.isFinite(v) ? v : undefined;
}

/** `45 s` / `6 min` / `2 h` → ms. */
export function parseDurationMs(s: string | undefined): number | undefined {
  const m = s ? /^(\d+) (s|min|h)$/.exec(s.trim()) : null;
  if (!m) return undefined;
  return Number(m[1]) * (m[2] === 's' ? 1000 : m[2] === 'min' ? 60_000 : 3_600_000);
}

/** `20 ms` / `1.4 s` → ms. */
export function parseMs(s: string | undefined): number | undefined {
  const m = s ? /^([\d.]+) (ms|s)$/.exec(s.trim()) : null;
  if (!m) return undefined;
  return Number(m[1]) * (m[2] === 's' ? 1000 : 1);
}

const BYTE_UNITS = ['B', 'KB', 'MB', 'GB', 'TB'];
/** `34 GB` → bytes. */
function parseBytes(s: string | undefined): number | undefined {
  const m = s ? /^([\d.]+) (B|KB|MB|GB|TB)$/.exec(s.trim()) : null;
  if (!m) return undefined;
  return Number(m[1]) * 1024 ** BYTE_UNITS.indexOf(m[2]!);
}

const set = (p: Params, k: string, v: number | string | undefined) => {
  if (v != null && !(typeof v === 'number' && !Number.isFinite(v))) p[k] = v;
};

/** Id tail after `kind:` (subject of node/cluster alerts). */
const idSubject = (a: Alert) => a.id.slice(a.id.indexOf(':') + 1);

function legacyParams(a: Alert): Params | null {
  const p: Params = {};
  const d = a.detail ?? '';
  let m: RegExpExecArray | null;
  switch (a.kind) {
    case 'lag_growth':
      if ((m = /Lag increased from (\S+) → (\S+) in (\d+ \w+)/.exec(d))) {
        p.variant = a.severity === 'critical' ? 'spike' : 'growing';
        set(p, 'from', parseCount(m[1]));
        set(p, 'to', parseCount(m[2]));
        set(p, 'over_ms', parseDurationMs(m[3]));
      } else if ((m = /Lag is (\S+) messages/.exec(d))) {
        p.variant = 'high';
        set(p, 'lag', parseCount(m[1]));
      } else return null;
      return p;
    case 'no_consumers':
      if (!(m = /has (\S+) pending messages/.exec(d))) return null;
      set(p, 'lag', parseCount(m[1]));
      return p;
    case 'consumer_stalled':
      if (!(m = /for (\d+ \w+) while (\S+) messages are pending \((\d+) instances?\)/.exec(d))) return null;
      set(p, 'for_ms', parseDurationMs(m[1]));
      set(p, 'lag', parseCount(m[2]));
      set(p, 'instances', Number(m[3]));
      return p;
    case 'high_retry_rate':
      if (!(m = /^([\d.]+)% of messages .* \((\S+) of (\S+)\)/.exec(d))) return null;
      set(p, 'ratio', Number(m[1]) / 100);
      set(p, 'nacked', parseCount(m[2]));
      set(p, 'total', parseCount(m[3]));
      return p;
    case 'dlq_growth':
      if (!(m = /^(\S+) messages? moved to the DLQ in the last (\d+ \w+)/.exec(d))) return null;
      set(p, 'added', parseCount(m[1]));
      set(p, 'over_ms', parseDurationMs(m[2]));
      return p;
    case 'slow_instance': {
      const prefix = `slow_instance:${a.channel ?? ''}/${a.group ?? ''}/`;
      if (!a.id.startsWith(prefix)) return null;
      p.instance = a.id.slice(prefix.length);
      if ((m = /Ack rate ([\d.]+[kKMB]?)\/s vs group median ([\d.]+[kKMB]?)\/s/.exec(d))) {
        set(p, 'rate', parseCount(m[1]));
        set(p, 'median_rate', parseCount(m[2]));
      }
      if ((m = /Processing p99 ([\d.]+ m?s) vs group ([\d.]+ m?s)/.exec(d))) {
        set(p, 'p99_ms', parseMs(m[1]));
        set(p, 'group_p99_ms', parseMs(m[2]));
      }
      return p.rate != null || p.p99_ms != null ? p : null;
    }
    case 'storage_nearly_full':
      if (!(m = /\((.+) of (.+)\) on (.+)$/.exec(d))) return null;
      set(p, 'used', parseBytes(m[1]));
      set(p, 'total', parseBytes(m[2]));
      p.node = m[3]!;
      return p.used != null && p.total != null ? p : null;
    case 'high_publish_latency':
      if (a.id.startsWith('high_publish_latency:node/')) {
        if (!(m = /fsync p99 is ([\d.]+ m?s) on (.+)$/.exec(d))) return null;
        set(p, 'fsync_p99_ms', parseMs(m[1]));
        p.node = m[2]!;
        return p;
      }
      if (!(m = /Publish p99 is ([\d.]+ m?s) at ([\d.]+[kKMB]?) msg\/s/.exec(d))) return null;
      set(p, 'p99_ms', parseMs(m[1]));
      set(p, 'rate', parseCount(m[2]));
      return p;
    case 'node_down':
      p.node = idSubject(a);
      if ((m = /for (\d+)s/.exec(d))) set(p, 'since_secs', Number(m[1]));
      if ((m = /\((.+)\)$/.exec(d))) p.error = m[1]!;
      return p;
    case 'under_replicated_partitions':
      if (!(m = /^(\d+)/.exec(a.title))) return null;
      set(p, 'count', Number(m[1]));
      if ((m = /\((\d+)\)$/.exec(d))) set(p, 'replication_factor', Number(m[1]));
      return p;
    case 'offline_partitions':
      if (!(m = /^(\d+)/.exec(a.title))) return null;
      set(p, 'count', Number(m[1]));
      return p;
    case 'tls_cert_expiring':
    case 'tls_reload_failed': {
      const s = idSubject(a);
      const i = s.lastIndexOf('/');
      if (i <= 0) return null;
      p.node = s.slice(0, i);
      p.surface = s.slice(i + 1);
      if (a.kind === 'tls_cert_expiring') {
        if (/has expired/.test(a.title)) p.expired = true;
        else if ((m = /expires in (\d+) days?/.exec(a.title))) set(p, 'days_left', Number(m[1]));
        else if (/less than a day/.test(a.title)) p.days_left = 0;
      }
      return p;
    }
    default:
      return null;
  }
}

/** Structured params: from the broker (v0.9.1+) or parsed from the legacy English detail. */
export function alertParams(a: Alert): Params | null {
  if (a.params && Object.keys(a.params).length) return a.params;
  return legacyParams(a);
}

// ---------------------------------------------------------------------------------------------
// Building the texts
// ---------------------------------------------------------------------------------------------

const num = (p: Params, k: string): number | undefined => (typeof p[k] === 'number' ? (p[k] as number) : undefined);
const str = (p: Params, k: string): string | undefined => (typeof p[k] === 'string' ? (p[k] as string) : undefined);
const dur = (ms: number | undefined) => (ms == null ? '—' : fmtDuration(ms / 1000));

const KNOWN: ReadonlySet<AlertKind> = new Set<AlertKind>([
  'lag_growth',
  'consumer_stalled',
  'no_consumers',
  'dlq_growth',
  'high_retry_rate',
  'slow_instance',
  'storage_nearly_full',
  'high_publish_latency',
  'node_down',
  'under_replicated_partitions',
  'offline_partitions',
  'tls_cert_expiring',
  'tls_reload_failed',
]);

/** Ratio p99 / group p99 (slow processing), or null. */
export function slowRatio(p: Params): number | null {
  const a = num(p, 'p99_ms');
  const b = num(p, 'group_p99_ms');
  return a != null && b ? a / b : null;
}

function build(a: Alert, p: Params): AlertText | null {
  const name = a.channel ?? '';
  const group = a.group ?? '';
  const k = (key: string, params?: Record<string, string | number | undefined>) => t(`alerts.k.${key}` as TKey, params);
  switch (a.kind) {
    case 'lag_growth': {
      const v = str(p, 'variant');
      if (v === 'high') {
        const lag = fmtCount(num(p, 'lag'));
        return { title: k('lagHigh.title', { name }), detail: k('lagHigh.detail', { lag, group }), reason: k('lagHigh.reason'), short: t('alerts.short.lagHigh', { lag }) };
      }
      const from = fmtCount(num(p, 'from'));
      const to = fmtCount(num(p, 'to'));
      const over = dur(num(p, 'over_ms'));
      return {
        title: k(v === 'spike' ? 'lagSpike.title' : 'lagGrowing.title', { name }),
        detail: k('lag.detail', { from, to, over, group }),
        reason: k('lag.reason'),
        short: t('alerts.short.lag', { from, to, over }),
      };
    }
    case 'no_consumers': {
      const lag = fmtCount(num(p, 'lag'));
      return { title: k('noConsumers.title', { name }), detail: k('noConsumers.detail', { group, lag }), reason: k('noConsumers.reason'), short: t('alerts.short.noConsumers', { lag }) };
    }
    case 'consumer_stalled': {
      const f = dur(num(p, 'for_ms'));
      return {
        title: k('stalled.title', { name }),
        detail: k('stalled.detail', { group, for: f, lag: fmtCount(num(p, 'lag')), count: num(p, 'instances') ?? 0 }),
        reason: k('stalled.reason'),
        short: t('alerts.short.stalled', { for: f }),
      };
    }
    case 'high_retry_rate': {
      const pct = fmtPercent((num(p, 'ratio') ?? 0) * 100, 1);
      return {
        title: k('retry.title', { name }),
        detail: k('retry.detail', { pct, group, nacked: fmtCount(num(p, 'nacked')), total: fmtCount(num(p, 'total')) }),
        reason: k('retry.reason'),
        short: t('alerts.short.retry', { pct }),
      };
    }
    case 'dlq_growth': {
      const added = num(p, 'added') ?? 0;
      const over = dur(num(p, 'over_ms'));
      return {
        title: k('dlq.title', { name }),
        detail: k('dlq.detail', { count: added, over, group }),
        reason: k('dlq.reason'),
        short: t('alerts.short.dlq', { added: fmtCount(added), over }),
      };
    }
    case 'slow_instance': {
      const instance = str(p, 'instance') ?? '';
      const parts: string[] = [];
      const rate = num(p, 'rate');
      const median = num(p, 'median_rate');
      const ratio = slowRatio(p);
      if (rate != null && median != null) parts.push(k('slow.ack', { rate: fmtRate(rate, false), median: fmtRate(median, false) }));
      if (ratio != null) parts.push(k('slow.processing', { p99: fmtMs(num(p, 'p99_ms')), groupP99: fmtMs(num(p, 'group_p99_ms')) }));
      if (!parts.length) return null;
      const short =
        ratio != null
          ? t('alerts.short.slowProcessing', { instance, ratio: fmtNumber(ratio, ratio >= 10 ? 0 : 1) })
          : t('alerts.short.slowAck', { instance, percent: fmtPercent(median ? ((rate ?? 0) / median) * 100 : 0) });
      return {
        title: k('slow.title', { instance }),
        detail: `${parts.join('; ')} ${k('slow.where', { group, channel: name })}`,
        reason: ratio != null ? k('slow.reasonProcessing') : k('slow.reasonAck'),
        short,
      };
    }
    case 'storage_nearly_full': {
      const used = num(p, 'used') ?? 0;
      const total = num(p, 'total') ?? 0;
      const title = k('storage.title');
      return {
        title,
        detail: k('storage.detail', { pct: fmtPercent(total ? (used * 100) / total : 0), used: fmtBytes(used), total: fmtBytes(total), node: str(p, 'node') ?? '' }),
        reason: k('storage.reason'),
        short: title,
      };
    }
    case 'high_publish_latency': {
      if (str(p, 'node') != null && num(p, 'fsync_p99_ms') != null) {
        const title = k('fsync.title');
        return { title, detail: k('fsync.detail', { p99: fmtMs(num(p, 'fsync_p99_ms')), node: str(p, 'node') }), reason: k('fsync.reason'), short: title };
      }
      const p99 = fmtMs(num(p, 'p99_ms'));
      return { title: k('publish.title', { name }), detail: k('publish.detail', { p99, rate: fmtRate(num(p, 'rate')) }), reason: k('publish.reason'), short: t('alerts.short.publish', { p99 }) };
    }
    case 'node_down': {
      const node = str(p, 'node') ?? idSubject(a);
      const secs = num(p, 'since_secs');
      const err = str(p, 'error');
      const base = secs != null ? k('nodeDown.detailFor', { node, for: fmtDuration(secs) }) : k('nodeDown.detailNever', { node });
      const title = k('nodeDown.title', { node });
      return { title, detail: err ? `${base} (${err})` : base, reason: k('nodeDown.reason'), short: title };
    }
    case 'under_replicated_partitions': {
      const count = num(p, 'count') ?? 0;
      const title = k('underReplicated.title', { count });
      return { title, detail: k('underReplicated.detail', { count, rf: num(p, 'replication_factor') ?? '—' }), reason: k('underReplicated.reason'), short: title };
    }
    case 'offline_partitions': {
      const count = num(p, 'count') ?? 0;
      const title = k('offline.title', { count });
      return { title, detail: k('offline.detail', { count }), reason: k('offline.reason'), short: title };
    }
    case 'tls_cert_expiring': {
      const node = str(p, 'node') ?? '';
      const surface = str(p, 'surface') ?? '';
      const days = num(p, 'days_left');
      const title = p.expired === true ? k('tlsExpired.title', { node, surface }) : days === 0 || days == null ? k('tlsExpiringToday.title', { node, surface }) : k('tlsExpiring.title', { node, surface, count: days });
      const m = /^(.*) \(issuer (.*)\) is valid until (.*)$/.exec(a.detail ?? '');
      return { title, detail: m ? k('tlsExpiring.detail', { subject: m[1], issuer: m[2], until: m[3] }) : a.detail, reason: k('tlsExpiring.reason'), short: title };
    }
    case 'tls_reload_failed': {
      const title = k('tlsReload.title', { node: str(p, 'node') ?? '', surface: str(p, 'surface') ?? '' });
      return { title, detail: k('tlsReload.detail', { error: a.detail }), reason: k('tlsReload.reason'), short: title };
    }
    default:
      return null;
  }
}

/** Localised title / detail / possible reason (server text as the fallback). */
export function alertText(a: Alert): AlertText {
  const p = KNOWN.has(a.kind) ? alertParams(a) : null;
  const built = p ? build(a, p) : null;
  if (built) return built;
  // Unknown kind or unparsable text: English keeps the server sentence; other languages get the
  // translated kind as the title and the server detail below it.
  const title = KNOWN.has(a.kind) ? (ALERT_KIND_LABEL[a.kind] ?? a.title) : a.title;
  const en = getLang() === 'en';
  return { title: en ? a.title : title, detail: a.detail, reason: a.possible_reason, short: en ? a.title : title };
}

/** The instance an alert is about (slow_instance), if any. */
export function alertInstance(a: Alert): string | null {
  if (a.kind !== 'slow_instance') return null;
  const p = alertParams(a);
  return (p && str(p, 'instance')) ?? null;
}

// ---------------------------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------------------------

/** `payments.events (7 messages)` → channel + count (brokers without event params). */
function legacyEventParams(e: FluxEvent): Params | null {
  if (e.kind === 'dead_lettered' || e.kind === 'messages_replayed') {
    const m = /^(.*) \((\d+) messages?\)$/.exec(e.subject);
    return m ? { channel: m[1]!, count: Number(m[2]) } : null;
  }
  if (e.kind === 'dlq_retried' || e.kind === 'dlq_discarded') {
    const m = /^(\d+) messages?$/.exec(e.subject);
    return m ? { count: Number(m[1]) } : null;
  }
  return null;
}

export function eventParams(e: FluxEvent): Params | null {
  if (e.params && Object.keys(e.params).length) return e.params;
  return legacyEventParams(e);
}

/** Localised event title + subject line. */
export function eventText(e: FluxEvent): { title: string; subject: string } {
  const p = eventParams(e);
  const label = EVENT_KIND_LABEL[e.kind] ?? e.title;
  const en = getLang() === 'en';
  if (e.kind === 'alert_opened' || e.kind === 'alert_resolved') {
    const kind = p && str(p, 'alert_kind');
    if (kind && KNOWN.has(kind as AlertKind)) {
      const inst = p && str(p, 'instance');
      return { title: t(e.kind === 'alert_opened' ? 'alerts.ev.opened' : 'alerts.ev.resolved', { kind: ALERT_KIND_LABEL[kind as AlertKind] }), subject: inst ? `${e.subject} · ${inst}` : e.subject };
    }
    return { title: en ? e.title : label, subject: e.subject };
  }
  if (p && num(p, 'count') != null) {
    const count = num(p, 'count')!;
    const channel = str(p, 'channel');
    return { title: en ? e.title : label, subject: channel ? t('alerts.ev.channelCount', { channel, count }) : t('alerts.ev.count', { count }) };
  }
  return { title: en ? e.title : label, subject: e.subject };
}

/** Window in which repeated dead-letter entries of one channel collapse into one feed row. */
export const EVENT_COLLAPSE_MS = 5 * 60_000;

export interface FeedRow {
  event: FluxEvent;
  /** Number of feed entries collapsed into this row (1 = not collapsed). */
  times: number;
  /** Summed message count of the collapsed entries, when known. */
  count: number | null;
}

/**
 * Collapse consecutive `dead_lettered` entries of the same channel within 5 min into one row
 * ("×N in 5 min"); newest first in, newest first out.
 */
export function collapseEvents(events: FluxEvent[]): FeedRow[] {
  const out: FeedRow[] = [];
  for (const e of events) {
    const prev = out[out.length - 1];
    if (prev && e.kind === 'dead_lettered' && prev.event.kind === 'dead_lettered') {
      const a = eventParams(prev.event);
      const b = eventParams(e);
      const chA = a && str(a, 'channel');
      const chB = b && str(b, 'channel');
      // Window measured from the newest entry of the run.
      if (chA && chA === chB && prev.event.ts - e.ts <= EVENT_COLLAPSE_MS) {
        prev.times++;
        const n = b && num(b, 'count');
        prev.count = prev.count != null && n != null ? prev.count + n : null;
        continue;
      }
    }
    const p = e.kind === 'dead_lettered' ? eventParams(e) : null;
    out.push({ event: e, times: 1, count: p ? (num(p, 'count') ?? null) : null });
  }
  return out;
}
