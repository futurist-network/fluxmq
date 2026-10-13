/** Webhook editor: draft ⇄ subscription / requests, validation (ranges from webhooks.rs docs). */
import { WEBHOOK_LIMITS, type Condition, type CreateWebhookRequest, type PayloadFormat, type StartFrom, type UpdateWebhookRequest, type WebhookSubscription } from '../api/types';
import { t } from '../i18n';
import { headersFromRead, headersToRequest, newSecret, secretFromRead, secretToRequest, validateHeaders, type HeaderRow, type SecretField } from './secrets';

export type StartMode = 'latest' | 'earliest' | 'timestamp';

export interface WebhookDraft {
  name: string;
  channel: string;
  url: string;
  enabled: boolean;
  headers: HeaderRow[];
  secret: SecretField;
  format: PayloadFormat;
  /** Numbers as typed (strings), validated on save. */
  timeout_ms: string;
  max_in_flight: string;
  batch_max: string;
  batch_linger_ms: string;
  retry_on_4xx: boolean;
  /** Circuit breaker on, and after how many consecutive failures. */
  pause_enabled: boolean;
  pause_after: string;
  start: StartMode;
  /** `datetime-local` value when `start = timestamp`. */
  start_at: string;
}

/** The console turns the circuit breaker on for new webhooks (the API default is off). */
export const WEBHOOK_DEFAULTS = { timeout_ms: 10_000, max_in_flight: 4, batch_max: 1, batch_linger_ms: 0, pause_after_failures: 5 } as const;

export function emptyWebhookDraft(channel = ''): WebhookDraft {
  return {
    name: '',
    channel,
    url: '',
    enabled: true,
    headers: [],
    secret: newSecret(),
    format: 'envelope',
    timeout_ms: String(WEBHOOK_DEFAULTS.timeout_ms),
    max_in_flight: String(WEBHOOK_DEFAULTS.max_in_flight),
    batch_max: String(WEBHOOK_DEFAULTS.batch_max),
    batch_linger_ms: String(WEBHOOK_DEFAULTS.batch_linger_ms),
    retry_on_4xx: false,
    pause_enabled: true,
    pause_after: String(WEBHOOK_DEFAULTS.pause_after_failures),
    start: 'latest',
    start_at: '',
  };
}

function startToDraft(s: StartFrom): Pick<WebhookDraft, 'start' | 'start_at'> {
  if (s === 'earliest' || s === 'latest') return { start: s, start_at: '' };
  const d = new Date(s.timestamp_ms);
  const pad = (n: number) => String(n).padStart(2, '0');
  return { start: 'timestamp', start_at: `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}` };
}

export function webhookToDraft(w: WebhookSubscription): WebhookDraft {
  return {
    name: w.name,
    channel: w.channel,
    url: w.url,
    enabled: w.enabled,
    headers: headersFromRead(w.headers),
    secret: secretFromRead(w.secret),
    format: w.format,
    timeout_ms: String(w.timeout_ms),
    max_in_flight: String(w.max_in_flight),
    batch_max: String(w.batch_max),
    batch_linger_ms: String(w.batch_linger_ms),
    retry_on_4xx: w.retry_on_4xx,
    pause_enabled: (w.pause_after_failures ?? 0) > 0,
    pause_after: String(w.pause_after_failures || WEBHOOK_DEFAULTS.pause_after_failures),
    ...startToDraft(w.start),
  };
}

/** Switching to raw forces a batch of 1 (raw bodies can't be batched). */
export function setFormat(d: WebhookDraft, format: PayloadFormat): WebhookDraft {
  return format === 'raw' ? { ...d, format, batch_max: '1', batch_linger_ms: '0' } : { ...d, format };
}

export type WebhookErrors = Partial<Record<'name' | 'channel' | 'url' | 'header' | 'timeout_ms' | 'max_in_flight' | 'batch_max' | 'batch_linger_ms' | 'pause_after' | 'start_at', string>>;

function intIn(s: string, [lo, hi]: readonly [number, number]): number | null {
  if (!/^\d+$/.test(s.trim())) return null;
  const n = Number(s.trim());
  return n >= lo && n <= hi ? n : null;
}

export function validateWebhookDraft(d: WebhookDraft, opts: { isNew: boolean; existingNames?: string[]; ackTimeoutMs?: number | null } = { isNew: true }): WebhookErrors {
  const e: WebhookErrors = {};
  if (opts.isNew) {
    if (!d.name) e.name = t('webhooks.err.nameRequired');
    else if (!WEBHOOK_LIMITS.name.test(d.name)) e.name = t('webhooks.err.nameRule');
    else if (opts.existingNames?.includes(d.name)) e.name = t('webhooks.err.nameExists', { name: d.name });
    if (!d.channel) e.channel = t('webhooks.err.channel');
  }
  try {
    const u = new URL(d.url.trim());
    if (u.protocol !== 'https:' && u.protocol !== 'http:') e.url = t('webhooks.err.scheme');
  } catch {
    e.url = d.url.trim() ? t('webhooks.err.urlInvalid') : t('webhooks.err.urlRequired');
  }
  const he = Object.values(validateHeaders(d.headers))[0];
  if (he) e.header = he;
  const [tlo, thi] = WEBHOOK_LIMITS.timeout_ms;
  const timeout = intIn(d.timeout_ms, WEBHOOK_LIMITS.timeout_ms);
  if (timeout == null) e.timeout_ms = `${tlo}–${thi} ${t('webhooks.ms')}`;
  else if (opts.ackTimeoutMs != null && timeout >= opts.ackTimeoutMs) e.timeout_ms = t('webhooks.err.belowAck', { ms: opts.ackTimeoutMs });
  if (intIn(d.max_in_flight, WEBHOOK_LIMITS.max_in_flight) == null) e.max_in_flight = `${WEBHOOK_LIMITS.max_in_flight[0]}–${WEBHOOK_LIMITS.max_in_flight[1]}`;
  const batch = intIn(d.batch_max, WEBHOOK_LIMITS.batch_max);
  if (batch == null) e.batch_max = `${WEBHOOK_LIMITS.batch_max[0]}–${WEBHOOK_LIMITS.batch_max[1]}`;
  else if (d.format === 'raw' && batch !== 1) e.batch_max = t('webhooks.err.rawBatch');
  if (intIn(d.batch_linger_ms, WEBHOOK_LIMITS.batch_linger_ms) == null) e.batch_linger_ms = `${WEBHOOK_LIMITS.batch_linger_ms[0]}–${WEBHOOK_LIMITS.batch_linger_ms[1]} ${t('webhooks.ms')}`;
  if (d.pause_enabled && intIn(d.pause_after, [1, WEBHOOK_LIMITS.pause_after_failures[1]]) == null) e.pause_after = `1–${WEBHOOK_LIMITS.pause_after_failures[1]}`;
  if (opts.isNew && d.start === 'timestamp' && !(d.start_at && Number.isFinite(new Date(d.start_at).getTime()))) e.start_at = t('webhooks.err.startAt');
  return e;
}

function pauseAfter(d: WebhookDraft): number {
  return d.pause_enabled ? Number(d.pause_after) : 0;
}

function startFrom(d: WebhookDraft): StartFrom {
  if (d.start === 'timestamp') return { timestamp_ms: new Date(d.start_at).getTime() };
  return d.start;
}

export function draftToCreateWebhook(d: WebhookDraft, filter: Condition[]): CreateWebhookRequest {
  return {
    name: d.name,
    channel: d.channel,
    url: d.url.trim(),
    enabled: d.enabled,
    headers: headersToRequest(d.headers),
    secret: secretToRequest(d.secret, true),
    filter,
    format: d.format,
    timeout_ms: Number(d.timeout_ms),
    max_in_flight: Number(d.max_in_flight),
    batch_max: Number(d.batch_max),
    batch_linger_ms: Number(d.batch_linger_ms),
    retry_on_4xx: d.retry_on_4xx,
    pause_after_failures: pauseAfter(d),
    start: startFrom(d),
  };
}

/** Full PATCH of the mutable fields (name/channel/start are immutable). */
export function draftToUpdateWebhook(d: WebhookDraft, filter: Condition[]): UpdateWebhookRequest {
  return {
    url: d.url.trim(),
    enabled: d.enabled,
    headers: headersToRequest(d.headers),
    secret: secretToRequest(d.secret, true),
    filter,
    format: d.format,
    timeout_ms: Number(d.timeout_ms),
    max_in_flight: Number(d.max_in_flight),
    batch_max: Number(d.batch_max),
    batch_linger_ms: Number(d.batch_linger_ms),
    retry_on_4xx: d.retry_on_4xx,
    pause_after_failures: pauseAfter(d),
  };
}

export function urlHost(url: string): string {
  try {
    return new URL(url).host;
  } catch {
    return url;
  }
}

/** Status code colour: only errors get colour (2xx stays neutral). */
export function statusTone(status: number | null): 'warning' | 'critical' | null {
  if (status == null || (status >= 200 && status < 300)) return null;
  if (status === 408 || status === 425 || status === 429 || status >= 500) return 'critical';
  return 'warning';
}

/** Example body for the preview panel (matches `WebhookEnvelope`). */
export function exampleEnvelope(name: string, channel: string, batched = false): string {
  const env = {
    delivery_id: `${channel || 'orders.events'}/3/1842211`,
    subscription: name || 'my-webhook',
    channel: channel || 'orders.events',
    partition: 3,
    offset: 1842211,
    key: 'ord_8842',
    headers: { 'flux-type': 'order.created', 'content-type': 'application/json' },
    timestamp_ms: 1767225600000,
    attempt: 1,
    payload: { encoding: 'utf8', data: '{"order_id":8842,"total":129.9,"country":"MD"}' },
  };
  return JSON.stringify(batched ? [env, { ...env, delivery_id: `${env.channel}/3/1842212`, offset: 1842212, key: 'ord_8843' }] : env, null, 2);
}

export const VERIFY_NODE = `import crypto from 'node:crypto';

// Express: app.post('/hook', express.raw({ type: '*/*' }), handler)
export function verifyFluxMQ(rawBody, header, secret, toleranceSec = 300) {
  const parts = Object.fromEntries(header.split(',').map((p) => p.split('=')));
  const t = Number(parts.t);
  if (!t || Math.abs(Date.now() / 1000 - t) > toleranceSec) return false; // replay guard
  const expected = crypto.createHmac('sha256', secret).update(\`\${t}.\${rawBody}\`).digest('hex');
  const got = Buffer.from(parts.v1 ?? '', 'hex');
  return got.length === 32 && crypto.timingSafeEqual(got, Buffer.from(expected, 'hex'));
}
// verifyFluxMQ(req.body, req.get('X-FluxMQ-Signature'), process.env.FLUXMQ_WEBHOOK_SECRET)
// Dedupe on the X-FluxMQ-Delivery-Id header (stable across retries).`;

export const VERIFY_PYTHON = `import hashlib, hmac, time

def verify_fluxmq(raw_body: bytes, header: str, secret: str, tolerance: int = 300) -> bool:
    parts = dict(p.split("=", 1) for p in header.split(","))
    t = int(parts.get("t", "0"))
    if abs(time.time() - t) > tolerance:  # replay guard
        return False
    msg = f"{t}.".encode() + raw_body
    expected = hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, parts.get("v1", ""))

# Flask: verify_fluxmq(request.get_data(), request.headers["X-FluxMQ-Signature"], SECRET)
# Dedupe on the X-FluxMQ-Delivery-Id header (stable across retries).`;

/** "connection refused: error sending request …" → "connection refused" (full text in a tooltip). */
/** Detail line for a short error: the server's `*_detail` when it adds something, else the long
 *  legacy text when the short form truncated it. `null` when there is nothing more to show. */
export function errorDetail(error: string | null | undefined, detail?: string | null): string | null {
  if (detail && detail.trim() && detail.trim() !== (error ?? '').trim()) return detail.trim();
  if (error && shortError(error) !== error) return error;
  return null;
}

export function shortError(e: string): string {
  const head = e.split(':')[0]!.trim();
  return head.length > 32 ? `${head.slice(0, 30)}…` : head;
}
