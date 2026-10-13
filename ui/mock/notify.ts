/**
 * Mock notifications — contract of crates/fluxmq-proto/src/notify.rs: notifiers + rules CRUD
 * (secrets redacted on read, uniform update rule), test sends, delivery history driven by the
 * simulated alerts, and the `$system.events` pull feed (long-poll).
 */
import type {
  Alert,
  AlertKind,
  AlertsState,
  CreateNotifierRequest,
  CreateRuleRequest,
  EventKind,
  FeedItem,
  FeedPage,
  Health,
  MessageDetail,
  MessagePage,
  MessageQuery,
  MessageView,
  NotificationRecord,
  NotificationRule,
  Notifier,
  NotifierKind,
  RuleMatch,
  TestResult,
  UpdateNotifierRequest,
  UpdateRuleRequest,
} from '../src/api/types';
import { HEALTH_RANK, REDACTED, SYSTEM_EVENTS_CHANNEL } from '../src/api/types';
import { ApiErr, EMPTY, type Sim } from './sim';

const H = 3_600_000;
const bad = (m: string): never => {
  throw new ApiErr(400, 'bad_request', m);
};
const isObj = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);
const str = (v: unknown, name: string, required = true): string => {
  if (typeof v === 'string' && (v.trim() || !required)) return v;
  return bad(`'${name}' is required`);
};

/** Uniform secret rule: REDACTED keeps, "" removes (optional), else replace. */
function mergeSecret(next: string | null | undefined, prev: string | null | undefined, optional: boolean, name: string): string | null {
  if (next === REDACTED) {
    if (prev == null || prev === '') return bad(`'${name}': nothing stored to keep`);
    return prev;
  }
  if (next == null || next === '') {
    if (!optional) return bad(`'${name}' is required`);
    return null;
  }
  return next;
}

function mergeHeaders(next: Record<string, string>, prev: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(next)) {
    if (typeof v !== 'string') bad(`header '${k}' must be a string`);
    if (v === REDACTED) {
      const p = prev[k];
      if (p == null) bad(`header '${k}': nothing stored to keep`);
      out[k] = p!;
    } else if (v !== '') out[k] = v;
  }
  return out;
}

export const redactHeaders = (h: Record<string, string>) => Object.fromEntries(Object.keys(h).map((k) => [k, REDACTED]));
export { mergeSecret, mergeHeaders };

function redact(k: NotifierKind): NotifierKind {
  switch (k.type) {
    case 'telegram':
      return { ...k, bot_token: REDACTED };
    case 'slack':
    case 'discord':
      return { ...k, webhook_url: REDACTED };
    case 'email':
      return { ...k, password: k.password ? REDACTED : null };
    case 'pagerduty':
      return { ...k, routing_key: REDACTED };
    case 'webhook':
      return { ...k, secret: k.secret ? REDACTED : null, headers: redactHeaders(k.headers) };
  }
}

/** Parse + validate a kind, resolving secrets against `prev` (same type) when given. */
/** Short error class of a long error text (mirrors `fluxmq_proto::api::short_error`). */
export function shortOf(detail: string): string {
  const http = /^(?:Telegram |HTTP )(\d{3})/.exec(detail);
  if (http) return `HTTP ${http[1]}`;
  const c = detail.toLowerCase();
  if (c.includes('timed out') || c.includes('timeout')) return 'timeout';
  if (c.includes('dns error') || c.includes('failed to lookup')) return 'DNS error';
  if (c.includes('certificate') || c.includes('tls')) return 'TLS error';
  if (c.includes('connection refused')) return 'connection refused';
  return detail.split(':')[0]!.trim();
}

function parseKind(v: unknown, prev: NotifierKind | null): NotifierKind {
  if (!isObj(v)) return bad("'kind' must be an object with a 'type'");
  const p = prev && prev.type === v.type ? prev : null;
  switch (v.type) {
    case 'telegram': {
      const chat = str(v.chat_id, 'kind.chat_id');
      if (!/^(-?\d+|@\w{5,})$/.test(chat.trim())) bad("'kind.chat_id' must be a number or @channel");
      if (v.thread_id != null && (typeof v.thread_id !== 'number' || !Number.isInteger(v.thread_id))) bad("'kind.thread_id' must be an integer or null");
      return { type: 'telegram', bot_token: mergeSecret(v.bot_token as string, p?.type === 'telegram' ? p.bot_token : null, false, 'kind.bot_token')!, chat_id: chat.trim(), thread_id: (v.thread_id as number | null | undefined) ?? null };
    }
    case 'slack':
    case 'discord': {
      const prevUrl = p && (p.type === 'slack' || p.type === 'discord') ? p.webhook_url : null;
      return { type: v.type, webhook_url: mergeSecret(v.webhook_url as string, prevUrl, false, 'kind.webhook_url')! };
    }
    case 'email': {
      const pe = p?.type === 'email' ? p : null;
      if (typeof v.smtp_port !== 'number' || v.smtp_port < 1 || v.smtp_port > 65535) bad("'kind.smtp_port' must be 1-65535");
      if (!['starttls', 'tls', 'none'].includes(v.tls as string)) bad("'kind.tls' must be starttls, tls or none");
      if (!Array.isArray(v.to) || !v.to.length) bad("'kind.to' needs at least one address");
      return {
        type: 'email',
        smtp_host: str(v.smtp_host, 'kind.smtp_host'),
        smtp_port: v.smtp_port as number,
        username: (v.username as string | null) || null,
        password: mergeSecret(v.password as string | null, pe?.password, true, 'kind.password'),
        tls: v.tls as string,
        from: str(v.from, 'kind.from'),
        to: v.to as string[],
      };
    }
    case 'pagerduty':
      return { type: 'pagerduty', routing_key: mergeSecret(v.routing_key as string, p?.type === 'pagerduty' ? p.routing_key : null, false, 'kind.routing_key')! };
    case 'webhook': {
      const pw = p?.type === 'webhook' ? p : null;
      const url = str(v.url, 'kind.url');
      if (!/^https?:\/\//.test(url)) bad("'kind.url' must be an http(s) URL");
      return { type: 'webhook', url, secret: mergeSecret(v.secret as string | null, pw?.secret, true, 'kind.secret'), headers: mergeHeaders(isObj(v.headers) ? (v.headers as Record<string, string>) : {}, pw?.headers ?? {}) };
    }
  }
  return bad("'kind.type' must be telegram, slack, discord, email, pagerduty or webhook");
}

function parseMatch(v: unknown): RuleMatch {
  if (!isObj(v)) return bad("'match' must be an object");
  if (!['warning', 'critical', 'healthy'].includes(v.min_severity as string)) bad("'match.min_severity' must be warning or critical");
  const arr = (x: unknown, n: string): string[] => {
    if (x == null) return [];
    if (!Array.isArray(x) || x.some((s) => typeof s !== 'string')) bad(`'match.${n}' must be an array of strings`);
    return x as string[];
  };
  return { min_severity: v.min_severity as Health, alert_kinds: arr(v.alert_kinds, 'alert_kinds') as AlertKind[], event_kinds: arr(v.event_kinds, 'event_kinds') as EventKind[], channels: arr(v.channels, 'channels') };
}

const globRe = (g: string) => new RegExp(`^${g.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*').replace(/\?/g, '.')}$`);

export class Notify {
  private notifiers = new Map<string, Notifier>();
  private rules = new Map<string, NotificationRule>();
  private history: NotificationRecord[] = [];
  private feed: FeedItem[] = [];
  private seenAlerts = new Map<string, Alert>();
  private lastEventId = 0;
  private seq = 1;
  private recSeq = 1;
  private waiters = new Set<() => void>();

  constructor(private sim: Sim) {
    if (!EMPTY) this.seed();
    sim.addSystemChannel(SYSTEM_EVENTS_CHANNEL, 6 * 86_400_000, 0.05);
    this.seedFeed();
    sim.onTick.push((now) => this.tick(now));
  }

  private seed(): void {
    const now = Date.now();
    const n = (id: string, name: string, kind: NotifierKind, enabled = true, ago = 20) =>
      this.notifiers.set(id, { id, name, enabled, kind, created_at_ms: now - ago * 24 * H, updated_at_ms: now - 2 * 24 * H });
    n('nt_tg01', 'On-call Telegram', { type: 'telegram', bot_token: '7123456789:AAE-mock-token-abcdefghijklmnop', chat_id: '-1001987654321', thread_id: 12 });
    n('nt_sl02', 'Slack #payments-alerts', { type: 'slack', webhook_url: 'https://hooks.slack.com/services/T000/B000/XXXX' }, true, 12);
    n('nt_em03', 'Email: platform team', { type: 'email', smtp_host: 'smtp.eu.mailgun.org', smtp_port: 587, username: 'postmaster@mg.acme.io', password: 'hunter22', tls: 'starttls', from: 'fluxmq@acme.io', to: ['platform@acme.io', 'sre@acme.io'] }, false, 6);
    const r = (id: string, name: string, notifier_ids: string[], match: RuleMatch, extra: Partial<NotificationRule> = {}) =>
      this.rules.set(id, { id, name, enabled: true, notifier_ids, match, group_wait_secs: 30, repeat_interval_secs: 14_400, send_resolved: true, created_at_ms: now - 15 * 24 * H, updated_at_ms: now - 3 * 24 * H, ...extra });
    r('nr_crit', 'Critical → on-call', ['nt_tg01'], { min_severity: 'critical', alert_kinds: [], event_kinds: [], channels: [] });
    r('nr_pay', 'Payments team', ['nt_sl02'], { min_severity: 'warning', alert_kinds: ['dlq_growth', 'high_retry_rate', 'lag_growth'], event_kinds: ['dead_lettered'], channels: ['payments.*'] }, { group_wait_secs: 60, repeat_interval_secs: 3_600 });
    r('nr_cl', 'Cluster membership', ['nt_tg01', 'nt_em03'], { min_severity: 'warning', alert_kinds: ['node_down', 'under_replicated_partitions', 'offline_partitions'], event_kinds: ['node_down', 'node_up', 'controller_changed'], channels: [] }, { group_wait_secs: 10, repeat_interval_secs: 0 });

    const plan: [number, string | null, string, NotificationRecord['status'], string, number, string | null][] = [
      [22 * H, 'nr_crit', 'nt_tg01', 'sent', '🔴 orders.events/billing: consumer stalled', 1, null],
      [21.6 * H, 'nr_crit', 'nt_tg01', 'sent', '✅ Resolved: orders.events/billing consumer stalled', 1, null],
      [19 * H, 'nr_pay', 'nt_sl02', 'failed', 'payments.events/ledger: DLQ growth (+38 in 10 min)', 3, 'HTTP 404 no_service — the Slack webhook was removed or the app uninstalled'],
      [18.9 * H, 'nr_pay', 'nt_sl02', 'sent', 'payments.events/ledger: DLQ growth (+38 in 10 min)', 1, null],
      [14 * H, 'nr_cl', 'nt_tg01', 'sent', 'Node n3 down · 18 partitions under-replicated', 1, null],
      [14 * H, 'nr_cl', 'nt_em03', 'suppressed', 'Node n3 down · 18 partitions under-replicated', 0, 'notifier disabled'],
      [13.8 * H, 'nr_cl', 'nt_tg01', 'sent', 'Node n3 up · replication caught up', 1, null],
      [9 * H, 'nr_crit', 'nt_tg01', 'failed', '🔴 inventory.updates/sync: consumer stalled', 3, 'Telegram 429: Too Many Requests: retry after 31'],
      [8.9 * H, 'nr_crit', 'nt_tg01', 'sent', '🔴 inventory.updates/sync: consumer stalled', 2, null],
      [5 * H, 'nr_pay', 'nt_sl02', 'sent', '3 issues need attention on payments.*', 1, null],
      [4 * H, 'nr_pay', 'nt_sl02', 'suppressed', 'payments.events/ledger: high retry rate (repeat)', 0, 'repeat interval (1 h) not elapsed'],
      [1.2 * H, null, 'nt_tg01', 'sent', 'Test message from FluxMQ', 1, null],
      [9 * 60_000, 'nr_pay', 'nt_sl02', 'sent', 'payments.events/ledger: DLQ growth', 1, null],
    ];
    for (const [ago, rule, nid, status, summary, attempts, error] of plan) {
      this.history.push({ id: `nh_${this.recSeq++}`, ts: now - ago, rule_id: rule, notifier_id: nid, status, summary, attempts, error: error && status === 'failed' ? shortOf(error) : error, error_detail: status === 'failed' ? error : null });
    }
    this.history.sort((a, b) => b.ts - a.ts);
  }

  private pushFeed(item: Omit<FeedItem, 'cursor' | 'node'>): void {
    this.feed.push({ ...item, cursor: String(this.feed.length), node: 'n1' });
    if (this.feed.length > 5000) this.feed.splice(0, 1000);
    for (const w of this.waiters) w();
  }

  private seedFeed(): void {
    const events = [...this.sim.eventsFeed().events].reverse();
    for (const e of events) {
      this.pushFeed({ ts: e.ts, kind: 'event', alert: null, event: e });
      this.lastEventId = Math.max(this.lastEventId, e.id);
    }
    for (const a of Object.values(this.sim.alertsState())) {
      this.seenAlerts.set(a.id, a);
      this.pushFeed({ ts: a.since_ms, kind: 'alert_opened', alert: a, event: null });
    }
    this.feed.sort((a, b) => a.ts - b.ts).forEach((f, i) => (f.cursor = String(i)));
  }

  private tick(now: number): void {
    const alerts: AlertsState = this.sim.alertsState();
    for (const a of Object.values(alerts)) {
      if (this.seenAlerts.has(a.id)) continue;
      this.seenAlerts.set(a.id, a);
      this.pushFeed({ ts: now, kind: 'alert_opened', alert: a, event: null });
      this.dispatch(a, 'firing', now);
    }
    for (const [id, a] of this.seenAlerts) {
      if (alerts[id]) continue;
      this.seenAlerts.delete(id);
      this.pushFeed({ ts: now, kind: 'alert_resolved', alert: a, event: null });
      this.dispatch(a, 'resolved', now);
    }
    for (const e of [...this.sim.eventsFeed().events].reverse()) {
      if (e.id <= this.lastEventId) continue;
      this.lastEventId = e.id;
      if (e.kind === 'alert_opened' || e.kind === 'alert_resolved') continue; // carried as alert items
      this.pushFeed({ ts: e.ts, kind: 'event', alert: null, event: e });
    }
  }

  private dispatch(a: Alert, status: 'firing' | 'resolved', now: number): void {
    for (const r of this.rules.values()) {
      if (!r.enabled) continue;
      const m = r.match;
      if (HEALTH_RANK[a.severity] < HEALTH_RANK[m.min_severity]) continue;
      if (m.alert_kinds.length && !m.alert_kinds.includes(a.kind)) continue;
      if (m.channels.length && !(a.channel && m.channels.some((g) => globRe(g).test(a.channel!)))) continue;
      if (status === 'resolved' && !r.send_resolved) continue;
      for (const nid of r.notifier_ids) {
        const n = this.notifiers.get(nid);
        if (!n) continue;
        const summary = status === 'firing' ? `${a.severity === 'critical' ? '🔴' : '🟠'} ${a.title}` : `✅ Resolved: ${a.title}`;
        const fail = n.kind.type === 'slack' && Math.random() < 0.15;
        this.history.unshift({
          id: `nh_${this.recSeq++}`,
          ts: now + r.group_wait_secs * 1000 * 0.1,
          rule_id: r.id,
          notifier_id: nid,
          status: !n.enabled ? 'suppressed' : fail ? 'failed' : 'sent',
          summary,
          attempts: !n.enabled ? 0 : fail ? 3 : 1,
          error: !n.enabled ? 'notifier disabled' : fail ? 'HTTP 503' : null,
          error_detail: fail && n.enabled ? 'HTTP 503 from hooks.slack.com: service unavailable (retried 3×)' : null,
        });
      }
    }
    if (this.history.length > 1000) this.history.length = 1000;
  }

  // ── notifiers ──

  listNotifiers(): Notifier[] {
    return [...this.notifiers.values()].map((n) => ({ ...n, kind: redact(n.kind) })).sort((a, b) => a.created_at_ms - b.created_at_ms);
  }

  getNotifier(id: string): Notifier {
    const n = this.notifiers.get(id);
    if (!n) throw new ApiErr(404, 'not_found', `notifier '${id}' not found`);
    return { ...n, kind: redact(n.kind) };
  }

  private validateCreate(body: unknown): CreateNotifierRequest {
    if (!isObj(body)) return bad('body must be a CreateNotifierRequest object');
    const name = str(body.name, 'name').trim();
    if (name.length > 64) bad("'name' must be at most 64 characters");
    return { name, enabled: body.enabled !== false, kind: parseKind(body.kind, null) };
  }

  createNotifier(body: unknown): Notifier {
    const req = this.validateCreate(body);
    if ([...this.notifiers.values()].some((n) => n.name === req.name)) throw new ApiErr(409, 'conflict', `a notifier named '${req.name}' already exists`);
    const now = Date.now();
    const n: Notifier = { id: `nt_${(this.seq++).toString(36)}${Math.random().toString(36).slice(2, 6)}`, name: req.name, enabled: req.enabled ?? true, kind: req.kind, created_at_ms: now, updated_at_ms: now };
    this.notifiers.set(n.id, n);
    return this.getNotifier(n.id);
  }

  updateNotifier(id: string, body: unknown): Notifier {
    const n = this.notifiers.get(id);
    if (!n) throw new ApiErr(404, 'not_found', `notifier '${id}' not found`);
    if (!isObj(body)) return bad('body must be an UpdateNotifierRequest object');
    const req = body as UpdateNotifierRequest;
    const next = { ...n, updated_at_ms: Date.now() };
    if (req.name != null) next.name = str(req.name, 'name').trim();
    if (req.enabled != null) next.enabled = !!req.enabled;
    if (req.kind != null) next.kind = parseKind(req.kind, n.kind);
    this.notifiers.set(id, next);
    return this.getNotifier(id);
  }

  deleteNotifier(id: string): void {
    if (!this.notifiers.delete(id)) throw new ApiErr(404, 'not_found', `notifier '${id}' not found`);
    for (const r of this.rules.values()) r.notifier_ids = r.notifier_ids.filter((x) => x !== id);
  }

  private async send(kind: NotifierKind): Promise<TestResult> {
    const latency = Math.round(140 + Math.random() * 380);
    await new Promise((r) => setTimeout(r, Math.min(latency, 700)));
    // Short error + full detail, like the server (`fluxmq_proto::api::short_error`).
    const fail = (detail: string): TestResult => ({ ok: false, latency_ms: latency, error: shortOf(detail), error_detail: detail });
    switch (kind.type) {
      case 'telegram':
        if (kind.chat_id === '0' || kind.chat_id === '-1') return fail('Telegram 400: Bad Request: chat not found — add the bot to the chat first');
        if (kind.bot_token.includes('bad')) return fail('Telegram 401: Unauthorized — check the bot token');
        break;
      case 'slack':
        if (!kind.webhook_url.startsWith('https://hooks.slack.com/')) return fail('HTTP 404 no_service');
        break;
      case 'discord':
        if (!/discord(app)?\.com\/api\/webhooks\//.test(kind.webhook_url)) return fail('HTTP 401: Invalid Webhook Token');
        break;
      case 'email':
        if (kind.smtp_host.endsWith('.local')) return fail(`connection refused: ${kind.smtp_host}:${kind.smtp_port}`);
        break;
      case 'webhook':
        if (/fail|localhost|127\.0\.0\.1/.test(kind.url)) return fail('HTTP 503 Service Unavailable');
        break;
      case 'pagerduty':
        break;
    }
    return { ok: true, latency_ms: latency, error: null };
  }

  /** Optional draft (update shape, REDACTED = stored secret) merged over the stored notifier, not saved. */
  async testNotifier(id: string, draft?: unknown): Promise<TestResult> {
    const n = this.notifiers.get(id);
    if (!n) throw new ApiErr(404, 'not_found', `notifier '${id}' not found`);
    const d = (draft ?? {}) as { kind?: unknown };
    const kind = d.kind != null ? parseKind(d.kind, n.kind) : n.kind;
    const r = await this.send(kind);
    this.history.unshift({ id: `nh_${this.recSeq++}`, ts: Date.now(), rule_id: null, notifier_id: id, status: r.ok ? 'sent' : 'failed', summary: 'Test message from FluxMQ', attempts: 1, error: r.error, error_detail: r.error_detail ?? null });
    return r;
  }

  async testDraft(body: unknown): Promise<TestResult> {
    const req = this.validateCreate(body);
    return this.send(req.kind);
  }

  // ── rules ──

  listRules(): NotificationRule[] {
    return [...this.rules.values()].sort((a, b) => a.created_at_ms - b.created_at_ms);
  }

  private checkNotifiers(ids: unknown): string[] {
    if (!Array.isArray(ids) || !ids.length) return bad("'notifier_ids' needs at least one notifier");
    for (const id of ids) if (!this.notifiers.has(id as string)) throw new ApiErr(400, 'bad_request', `unknown notifier '${String(id)}'`);
    return ids as string[];
  }

  createRule(body: unknown): NotificationRule {
    if (!isObj(body)) return bad('body must be a CreateRuleRequest object');
    const req = body as unknown as CreateRuleRequest;
    const now = Date.now();
    const r: NotificationRule = {
      id: `nr_${(this.seq++).toString(36)}${Math.random().toString(36).slice(2, 6)}`,
      name: str(req.name, 'name').trim(),
      enabled: req.enabled !== false,
      notifier_ids: this.checkNotifiers(req.notifier_ids),
      match: parseMatch(body.match),
      group_wait_secs: req.group_wait_secs ?? 30,
      repeat_interval_secs: req.repeat_interval_secs ?? 14_400,
      send_resolved: req.send_resolved ?? true,
      created_at_ms: now,
      updated_at_ms: now,
    };
    this.rules.set(r.id, r);
    return r;
  }

  updateRule(id: string, body: unknown): NotificationRule {
    const r = this.rules.get(id);
    if (!r) throw new ApiErr(404, 'not_found', `rule '${id}' not found`);
    if (!isObj(body)) return bad('body must be an UpdateRuleRequest object');
    const req = body as unknown as UpdateRuleRequest;
    const next: NotificationRule = { ...r, updated_at_ms: Date.now() };
    if (req.name != null) next.name = str(req.name, 'name').trim();
    if (req.enabled != null) next.enabled = !!req.enabled;
    if (req.notifier_ids != null) next.notifier_ids = this.checkNotifiers(req.notifier_ids);
    if (body.match != null) next.match = parseMatch(body.match);
    if (req.group_wait_secs != null) next.group_wait_secs = req.group_wait_secs;
    if (req.repeat_interval_secs != null) next.repeat_interval_secs = req.repeat_interval_secs;
    if (req.send_resolved != null) next.send_resolved = req.send_resolved;
    this.rules.set(id, next);
    return next;
  }

  deleteRule(id: string): void {
    if (!this.rules.delete(id)) throw new ApiErr(404, 'not_found', `rule '${id}' not found`);
  }

  historyList(): NotificationRecord[] {
    return this.history.slice(0, 1000);
  }

  // ── pull feed ──

  /** `$system.events` messages = the feed items as JSON payloads (offset = cursor). */
  private toMessage(f: FeedItem): MessageView {
    const data = JSON.stringify(f);
    return { partition: 0, offset: Number(f.cursor), key: f.alert?.id ?? null, headers: { 'content-type': 'application/json', 'flux-type': f.kind, 'flux-producer': `fluxmq/${f.node}` }, timestamp_ms: f.ts, size: Buffer.byteLength(data), payload: { encoding: 'utf8', data } };
  }

  systemMessages(q: MessageQuery): MessagePage {
    const limit = Math.min(1000, Math.max(1, q.limit ?? 100));
    let list = this.feed;
    if (q.partition != null && q.partition !== 0) throw new ApiErr(400, 'bad_request', `partition ${q.partition} does not exist (channel has 1)`);
    if (q.since_ms != null) list = list.filter((f) => f.ts >= q.since_ms!);
    if (q.until_ms != null) list = list.filter((f) => f.ts <= q.until_ms!);
    if (q.key != null) list = list.filter((f) => f.alert?.id === q.key);
    let out: FeedItem[];
    let hasMore: boolean;
    if (q.offset != null) {
      const from = list.findIndex((f) => Number(f.cursor) >= q.offset!);
      out = from < 0 ? [] : list.slice(from, from + limit);
      hasMore = from >= 0 && from + limit < list.length;
    } else if (q.newest_first) {
      out = list.slice(-limit);
      hasMore = list.length > limit;
    } else {
      out = list.slice(0, limit);
      hasMore = list.length > limit;
    }
    return { messages: out.map((f) => this.toMessage(f)), scanned: out.length, has_more: hasMore };
  }

  systemMessage(offset: number): MessageDetail {
    const f = this.feed.find((x) => Number(x.cursor) === offset);
    if (!f) throw new ApiErr(404, 'not_found', `offset ${offset} is out of range for partition 0`);
    return { message: this.toMessage(f), deliveries: {} };
  }


  private page(after: string | null, limit: number): FeedPage {
    if (after === 'latest') return { items: [], next_cursor: this.feed.length ? this.feed[this.feed.length - 1]!.cursor : null, gap: false };
    const first = Number(this.feed[0]?.cursor ?? 0);
    const from = after == null ? 0 : this.feed.findIndex((f) => Number(f.cursor) > Number(after));
    const items = from < 0 ? [] : this.feed.slice(from, from + limit);
    return { items, next_cursor: items.length ? items[items.length - 1]!.cursor : after, gap: after != null && Number(after) + 1 < first };
  }

  async feedPage(after: string | null, limit: number, waitMs: number, tail: number | null = null): Promise<FeedPage> {
    if (tail != null) {
      if (after != null) bad("'tail' and 'after' are mutually exclusive");
      if (!Number.isInteger(tail) || tail < 1 || tail > 1000) bad("'tail' must be 1..1000");
      if (!this.feed.length && waitMs) return this.feedPage('latest', limit, waitMs);
      const items = this.feed.slice(-tail);
      return { items, next_cursor: items.length ? items[items.length - 1]!.cursor : null, gap: false };
    }
    if (after != null && after !== 'latest' && !/^\d+$/.test(after)) bad("'after' must be a cursor or 'latest'");
    if (!Number.isInteger(limit) || limit < 1 || limit > 1000) bad("'limit' must be 1..1000");
    if (waitMs < 0 || waitMs > 30_000) bad("'wait_ms' must be 0..30000");
    let p = this.page(after, limit);
    if (p.items.length || !waitMs) return p;
    const cursor = p.next_cursor;
    await new Promise<void>((resolve) => {
      const done = () => {
        clearTimeout(t);
        this.waiters.delete(done);
        resolve();
      };
      const t = setTimeout(done, waitMs);
      this.waiters.add(done);
    });
    p = this.page(after === 'latest' ? (cursor ?? '-1') : after, limit);
    return p;
  }
}
