/**
 * Mock HTTP push webhooks — contract of crates/fluxmq-proto/src/webhooks.rs: CRUD with
 * validation (name rules, ranges, raw ⇒ batch 1, timeout < ack timeout), secret + header
 * redaction, `$webhook:<name>` system groups, cluster-wide stats, test sends, and one failing
 * endpoint (503) whose dead letters keep growing.
 */
import type { Condition, CreateWebhookRequest, PayloadFormat, StartFrom, UpdateWebhookRequest, WebhookStats, WebhookSubscription, WebhookTestResult } from '../src/api/types';
import { REDACTED, WEBHOOK_LIMITS, webhookGroup } from '../src/api/types';
import { parse } from './dsl';
import { matches } from './filter';
import { mergeHeaders, mergeSecret, redactHeaders } from './notify';
import { validateConditions } from './routing';
import { ApiErr, EMPTY, type Sim } from './sim';

const DAY = 86_400_000;
const bad = (m: string): never => {
  throw new ApiErr(400, 'bad_request', m);
};
const isObj = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);

interface Live {
  delivered: number;
  failed: number;
  skipped: number;
  sel: number;
  failRate: number;
  status: number;
  p50: number;
  p99: number;
  lastMs: number | null;
  dlqAcc: number;
}

function range(v: unknown, name: keyof typeof WEBHOOK_LIMITS, def: number): number {
  if (v == null) return def;
  const [lo, hi] = WEBHOOK_LIMITS[name] as readonly [number, number];
  if (typeof v !== 'number' || !Number.isInteger(v) || v < lo || v > hi) bad(`'${name}' must be an integer in ${lo}..${hi}`);
  return v as number;
}

function parseStart(v: unknown): StartFrom {
  if (v == null) return 'latest';
  if (v === 'earliest' || v === 'latest') return v;
  if (isObj(v) && typeof v.timestamp_ms === 'number') return { timestamp_ms: v.timestamp_ms };
  return bad("'start' must be \"earliest\", \"latest\" or {\"timestamp_ms\": n}");
}

export class Webhooks {
  private subs = new Map<string, WebhookSubscription>();
  private live = new Map<string, Live>();
  private seq = 1;

  constructor(private sim: Sim) {
    if (!EMPTY) this.seed();
    sim.onTick.push((now, dt) => this.tick(now, dt));
  }

  private seed(): void {
    const now = Date.now();
    const mk = (id: string, name: string, channel: string, url: string, filter: string, extra: Partial<WebhookSubscription>, live: Partial<Live>, agoDays: number) => {
      const w: WebhookSubscription = {
        id,
        name,
        channel,
        url,
        enabled: true,
        headers: {},
        secret: null,
        filter: filter ? parse(filter) : [],
        format: 'envelope',
        timeout_ms: 10_000,
        max_in_flight: 4,
        batch_max: 1,
        batch_linger_ms: 0,
        retry_on_4xx: false,
        start: 'latest',
        created_at_ms: now - agoDays * DAY,
        updated_at_ms: now - Math.min(agoDays, 2) * DAY,
        ...extra,
      };
      this.subs.set(id, w);
      const ch = this.sim.channels.get(channel);
      const sel = live.sel ?? 1;
      const total = (ch?.baseRate ?? 0) * 0.002 * Math.min(agoDays, 2) * 86_400;
      this.live.set(id, { delivered: total * sel * (1 - (live.failRate ?? 0)), failed: total * sel * (live.failRate ?? 0), skipped: total * (1 - sel), sel, failRate: 0, status: 200, p50: 40, p99: 180, lastMs: now - 800, dlqAcc: 0, ...live });
      this.sim.setSystemGroup(channel, webhookGroup(name), w.enabled, { client: 'fluxmq-webhooks', nackRatio: live.failRate ? 0.4 : 0.0005, reason: live.status && live.status >= 500 ? `HTTP ${live.status}` : undefined, procP99: live.p99 ?? 180 });
    };
    mk('wh_crm01', 'crm-sync', 'orders.events', 'https://crm.acme.io/hooks/fluxmq', 'header.flux-type = "order.created"', { headers: { Authorization: 'Bearer crm_live_9f8e7d', 'X-Tenant': 'acme-eu' }, secret: 'whsec_crm_31415', max_in_flight: 8, batch_max: 20, batch_linger_ms: 200 }, { sel: 0.34, p50: 38, p99: 142 }, 18);
    mk('wh_wms02', 'warehouse-raw', 'orders.priority', 'https://wms.example.net/api/inbound', '', { format: 'raw' as PayloadFormat, max_in_flight: 1, secret: 'whsec_wms_2718' }, { sel: 1, p50: 61, p99: 310 }, 7);
    mk('wh_erp03', 'legacy-erp', 'inventory.updates', 'https://erp.internal.example.com/fluxmq/push', 'reason in ["restock", "return"]', { timeout_ms: 5_000, retry_on_4xx: true, headers: { 'X-Api-Key': 'erp-legacy-key' } }, { sel: 0.42, failRate: 0.93, status: 503, p50: 4_100, p99: 5_000 }, 30);
    // Its dead letters so far (the DLQ inbox groups them under "HTTP 503").
    this.sim.deadLetter('inventory.updates', webhookGroup('legacy-erp'), 'HTTP 503', 26);
  }

  private redact(w: WebhookSubscription): WebhookSubscription {
    return { ...w, secret: w.secret ? REDACTED : null, headers: redactHeaders(w.headers) };
  }

  private selectivity(channel: string, filter: Condition[]): number {
    if (!filter.length) return 1;
    try {
      const page = this.sim.queryMessages(channel, { limit: 300, newest_first: true });
      if (!page.messages.length) return 0.5;
      return page.messages.filter((m) => matches(filter, m)).length / page.messages.length;
    } catch {
      return 0.5;
    }
  }

  private tick(now: number, dt: number): void {
    for (const w of this.subs.values()) {
      const l = this.live.get(w.id);
      const ch = this.sim.channels.get(w.channel);
      if (!l || !ch || !w.enabled) continue;
      // Webhook endpoints see a small slice of the channel (demo scale).
      const n = ch.rate * 0.002 * dt;
      l.skipped += n * (1 - l.sel);
      const hit = n * l.sel;
      l.failed += hit * l.failRate;
      l.delivered += hit * (1 - l.failRate);
      if (hit > 0) l.lastMs = now;
      if (l.failRate > 0.5) {
        l.dlqAcc += hit * 0.08 + dt * 0.12;
        if (l.dlqAcc >= 1) {
          const k = Math.floor(l.dlqAcc);
          l.dlqAcc -= k;
          this.sim.deadLetter(w.channel, webhookGroup(w.name), `HTTP ${l.status}`, k);
        }
      }
    }
  }

  list(): WebhookSubscription[] {
    return [...this.subs.values()].map((w) => this.redact(w)).sort((a, b) => a.name.localeCompare(b.name));
  }

  get(id: string): WebhookSubscription {
    const w = this.subs.get(id);
    if (!w) throw new ApiErr(404, 'not_found', `webhook '${id}' not found`);
    return this.redact(w);
  }

  private validate(body: unknown, existing: WebhookSubscription | null): WebhookSubscription {
    if (!isObj(body)) return bad('body must be an object');
    const now = Date.now();
    const prev = existing;
    if (!prev) {
      if (typeof body.name !== 'string' || !WEBHOOK_LIMITS.name.test(body.name)) bad("'name' must be 1-64 characters [a-z0-9._-]");
      if (typeof body.channel !== 'string' || !this.sim.channels.has(body.channel)) bad(`channel '${String(body.channel)}' does not exist`);
    } else {
      for (const k of ['name', 'channel', 'start'] as const) if (body[k] != null && body[k] !== prev[k]) bad(`'${k}' is immutable — create a new webhook instead`);
    }
    const url = body.url ?? prev?.url;
    if (typeof url !== 'string' || !/^https?:\/\/[^/\s]+/.test(url)) bad("'url' must be an http(s) URL");
    const format = (body.format ?? prev?.format ?? 'envelope') as PayloadFormat;
    if (format !== 'envelope' && format !== 'raw') bad("'format' must be envelope or raw");
    const batch = range(body.batch_max, 'batch_max', prev?.batch_max ?? 1);
    if (format === 'raw' && batch !== 1) bad('format raw can not be combined with batching (batch_max must be 1)');
    const channel = (prev?.channel ?? body.channel) as string;
    const timeout = range(body.timeout_ms, 'timeout_ms', prev?.timeout_ms ?? 10_000);
    const ack = this.sim.channels.get(channel)?.config.retry.ack_timeout_ms ?? 30_000;
    if (timeout >= ack) bad(`'timeout_ms' (${timeout}) must be below the channel's ack_timeout_ms (${ack})`);
    return {
      id: prev?.id ?? `wh_${(this.seq++).toString(36)}${Math.random().toString(36).slice(2, 6)}`,
      name: (prev?.name ?? body.name) as string,
      channel,
      url: url as string,
      enabled: body.enabled == null ? (prev?.enabled ?? true) : !!body.enabled,
      headers: body.headers == null ? (prev?.headers ?? {}) : isObj(body.headers) ? mergeHeaders(body.headers as Record<string, string>, prev?.headers ?? {}) : bad("'headers' must be an object"),
      secret: body.secret === undefined && prev ? prev.secret : mergeSecret(body.secret as string | null, prev?.secret, true, 'secret'),
      filter: body.filter == null ? (prev?.filter ?? []) : validateConditions(body.filter),
      format,
      timeout_ms: timeout,
      max_in_flight: range(body.max_in_flight, 'max_in_flight', prev?.max_in_flight ?? 4),
      batch_max: batch,
      batch_linger_ms: range(body.batch_linger_ms, 'batch_linger_ms', prev?.batch_linger_ms ?? 0),
      retry_on_4xx: body.retry_on_4xx == null ? (prev?.retry_on_4xx ?? false) : !!body.retry_on_4xx,
      start: prev ? prev.start : parseStart(body.start),
      created_at_ms: prev?.created_at_ms ?? now,
      updated_at_ms: now,
    };
  }

  create(body: unknown): WebhookSubscription {
    const w = this.validate(body as CreateWebhookRequest, null);
    if ([...this.subs.values()].some((x) => x.name === w.name)) throw new ApiErr(409, 'conflict', `a webhook named '${w.name}' already exists`);
    this.subs.set(w.id, w);
    const broken = /fail|503/.test(w.url);
    this.live.set(w.id, { delivered: 0, failed: 0, skipped: 0, sel: this.selectivity(w.channel, w.filter), failRate: broken ? 0.95 : 0, status: broken ? 503 : 200, p50: 45, p99: 160, lastMs: null, dlqAcc: 0 });
    this.sim.setSystemGroup(w.channel, webhookGroup(w.name), w.enabled, { client: 'fluxmq-webhooks', nackRatio: broken ? 0.4 : 0.0005, reason: broken ? 'HTTP 503' : undefined });
    this.sim.addAudit('webhook.create', w.name, { channel: w.channel, url: w.url });
    return this.redact(w);
  }

  update(id: string, body: unknown): WebhookSubscription {
    const prev = this.subs.get(id);
    if (!prev) throw new ApiErr(404, 'not_found', `webhook '${id}' not found`);
    const w = this.validate(body as UpdateWebhookRequest, prev);
    this.subs.set(id, w);
    const l = this.live.get(id);
    if (l) {
      l.sel = this.selectivity(w.channel, w.filter);
      if (w.url !== prev.url) {
        const broken = /fail|503/.test(w.url);
        l.failRate = broken ? 0.95 : 0;
        l.status = broken ? 503 : 200;
        l.p50 = broken ? 4000 : 45;
        l.p99 = broken ? 5000 : 160;
      }
    }
    this.sim.setSystemGroup(w.channel, webhookGroup(w.name), w.enabled, { client: 'fluxmq-webhooks' });
    this.sim.addAudit('webhook.update', w.name, body);
    return this.redact(w);
  }

  delete(id: string): void {
    const w = this.subs.get(id);
    if (!w) throw new ApiErr(404, 'not_found', `webhook '${id}' not found`);
    this.subs.delete(id);
    this.live.delete(id);
    this.sim.setSystemGroup(w.channel, webhookGroup(w.name), false, { client: 'fluxmq-webhooks' });
    this.sim.addAudit('webhook.delete', w.name, {});
  }

  stats(): WebhookStats[] {
    return [...this.subs.values()].map((w) => {
      const l = this.live.get(w.id)!;
      const broken = l.failRate > 0.5;
      return {
        id: w.id,
        delivered: Math.round(l.delivered),
        failed: Math.round(l.failed),
        skipped_by_filter: Math.round(l.skipped),
        in_flight: w.enabled ? (broken ? w.max_in_flight : Math.min(w.max_in_flight, Math.round(Math.random() * w.max_in_flight))) : 0,
        last_status: l.lastMs ? l.status : null,
        last_error: broken ? `HTTP ${l.status}` : null,
        last_error_detail: broken ? `HTTP ${l.status}: <html><head><title>503 Service Unavailable</title></head><body>upstream connect error</body></html>` : null,
        last_delivery_ms: l.lastMs,
        latency_p50_ms: Math.round(l.p50 * (0.9 + Math.random() * 0.2)),
        latency_p99_ms: Math.round(l.p99 * (0.9 + Math.random() * 0.2)),
      };
    });
  }

  private async send(url: string): Promise<WebhookTestResult> {
    const broken = /fail|503|erp\.internal/.test(url);
    const refused = /localhost|127\.0\.0\.1|:1\b/.test(url);
    const latency = refused ? 2 : broken ? 5000 : Math.round(60 + Math.random() * 240);
    await new Promise((r) => setTimeout(r, Math.min(latency, 900)));
    if (refused)
      return { ok: false, status: null, latency_ms: latency, response_snippet: null, error: 'connection refused', error_detail: 'error sending request: client error (Connect): tcp connect error: Connection refused (os error 61)' };
    if (broken) return { ok: false, status: 503, latency_ms: latency, response_snippet: '<html><head><title>503 Service Unavailable</title></head><body>upstream connect error</body></html>', error: 'HTTP 503', error_detail: 'HTTP 503: upstream connect error' };
    if (/\/404|notfound/.test(url)) return { ok: false, status: 404, latency_ms: latency, response_snippet: '{"error":"no such route"}', error: 'HTTP 404', error_detail: 'HTTP 404 (not retried): {"error":"no such route"}' };
    return { ok: true, status: 200, latency_ms: latency, response_snippet: '{"received":true}', error: null };
  }

  /** Optional draft (update shape, REDACTED = stored value) merged over the stored one, not saved. */
  async test(id: string, draft?: unknown): Promise<WebhookTestResult> {
    const w = this.subs.get(id);
    if (!w) throw new ApiErr(404, 'not_found', `webhook '${id}' not found`);
    const merged = draft === undefined ? w : this.validate(draft as UpdateWebhookRequest, w);
    return this.send(merged.url);
  }

  async testDraft(body: unknown): Promise<WebhookTestResult> {
    const w = this.validate(body, null);
    return this.send(w.url);
  }
}
