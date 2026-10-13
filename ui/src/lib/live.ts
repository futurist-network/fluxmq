/**
 * Live subscriptions (WebSocket / SSE, v0.10 gateway) in the console: URLs, ready-to-paste client
 * code, event parsing and close-code explanations. Pure functions, tested without a DOM.
 * Protocol: docs/design/v0.10-live-gateway.md.
 */
import type { LiveChannelCount, LiveEnvelope, LiveLimits, LiveNodeStatus, LiveRejection } from '../api/types';
import { t, type TKey } from '../i18n';

/** WebSocket subprotocol of the gateway; the token rides as a second one (`fluxmq.token.<token>`). */
export const LIVE_PROTOCOL = 'fluxmq.live.v1';
export const TOKEN_PROTOCOL_PREFIX = 'fluxmq.token.';

export type LiveFrom = 'latest' | 'earliest';

export interface LiveOptions {
  from: LiveFrom;
  /** Exact keys, comma-separated (empty = any). */
  key: string;
  /** Filter language expression (empty = none). */
  filter: string;
}

export const DEFAULT_LIVE_OPTIONS: LiveOptions = { from: 'latest', key: '', filter: '' };

/** Query string (with `?`, or empty) for the given options; `latest` is the server default. */
export function liveQuery(o: LiveOptions, extra: Record<string, string> = {}): string {
  const q = new URLSearchParams();
  if (o.from !== 'latest') q.set('from', o.from);
  const keys = o.key
    .split(',')
    .map((k) => k.trim())
    .filter(Boolean);
  if (keys.length) q.set('key', keys.join(','));
  if (o.filter.trim()) q.set('filter', o.filter.trim());
  for (const [k, v] of Object.entries(extra)) q.set(k, v);
  const s = q.toString();
  return s ? `?${s}` : '';
}

export const livePath = (channel: string) => `/api/channels/${encodeURIComponent(channel)}/live`;
export const ssePath = (channel: string) => `/api/channels/${encodeURIComponent(channel)}/events`;

/** `ws(s)://host` for an `http(s)://host` origin. */
export function wsOrigin(origin: string): string {
  return origin.replace(/^http/, 'ws');
}

export function liveSubprotocols(token: string | null): string[] {
  return token ? [LIVE_PROTOCOL, TOKEN_PROTOCOL_PREFIX + token] : [LIVE_PROTOCOL];
}

export type SnippetKind = 'sse' | 'ws' | 'curl';
export const SNIPPET_KINDS: readonly SnippetKind[] = ['sse', 'ws', 'curl'];

/** Placeholder shown instead of a real secret: the console never pastes its own token into code. */
export const TOKEN_PLACEHOLDER = 'TOKEN';

/**
 * Client code for one subscription. `origin` is where the browser reaches the broker
 * (`https://mq.example.com`). Tokens are always the placeholder.
 */
export function liveSnippet(kind: SnippetKind, origin: string, channel: string, o: LiveOptions): string {
  const q = liveQuery(o);
  if (kind === 'sse') {
    const url = `${origin}${ssePath(channel)}${q ? `${q}&` : '?'}access_token=`;
    return [
      `// ${t('live.snippet.sseComment')}`,
      `const es = new EventSource('${url}' + ${TOKEN_PLACEHOLDER});`,
      `es.addEventListener('messages', (e) => {`,
      `  for (const m of JSON.parse(e.data).messages) {`,
      `    console.log(m.partition, m.offset, m.key, m.payload);`,
      `  }`,
      `});`,
      `es.addEventListener('end', (e) => console.warn('closed by server:', e.data));`,
    ].join('\n');
  }
  if (kind === 'ws') {
    return [
      `// ${t('live.snippet.wsComment')}`,
      `let cursor = localStorage.getItem('fluxmq.cursor');`,
      `const url = '${wsOrigin(origin)}${livePath(channel)}${q}';`,
      `const ws = new WebSocket(cursor ? url + '${q ? '&' : '?'}cursor=' + encodeURIComponent(cursor) : url,`,
      `                         ['${LIVE_PROTOCOL}', '${TOKEN_PROTOCOL_PREFIX}' + ${TOKEN_PLACEHOLDER}]);`,
      `ws.onmessage = (e) => {`,
      `  const ev = JSON.parse(e.data); // hello | messages | gap | reset | progress | error`,
      `  if (ev.type === 'messages') {`,
      `    for (const m of ev.messages) console.log(m.partition, m.offset, m.key, m.payload);`,
      `  }`,
      `  if (ev.cursor) localStorage.setItem('fluxmq.cursor', ev.cursor); // ${t('live.snippet.cursorComment')}`,
      `};`,
    ].join('\n');
  }
  return [`curl -N -H "Authorization: Bearer $${TOKEN_PLACEHOLDER}" \\`, `  '${origin}${ssePath(channel)}${q}'`].join('\n');
}

/** A `messages` / other event from the gateway (only the fields the console uses). */
export interface LiveEvent {
  type: string;
  sub?: string;
  cursor?: string;
  messages?: LiveEnvelope[];
  code?: string;
  message?: string;
  reason?: string;
  partition?: number;
  lost?: number;
}

export function parseLiveEvent(data: string): LiveEvent | null {
  try {
    const v = JSON.parse(data) as unknown;
    return v && typeof v === 'object' && typeof (v as LiveEvent).type === 'string' ? (v as LiveEvent) : null;
  } catch {
    return null;
  }
}

/** Short one-line payload preview. */
export function payloadPreview(m: LiveEnvelope, max = 160): string {
  if (m.encoding === 'base64') return t('live.try.binary', { size: m.size });
  const s = m.payload.replace(/\s+/g, ' ');
  return s.length > max ? `${s.slice(0, max)}…` : s;
}

/** Why the server closed a live WebSocket (design doc §1.4), or null for an unexplained code. */
export function closeReason(code: number): string | null {
  switch (code) {
    case 4008:
      return t('live.close.slow');
    case 4401:
      return t('live.close.revoked');
    case 4404:
      return t('live.close.deleted');
    case 4429:
      return t('live.close.limit');
    case 1001:
      return t('live.close.shutdown');
    case 1008:
      return t('live.close.idle');
    default:
      return null;
  }
}

/** Refusal reasons of `fluxmq_live_rejections_total` / `LiveRejection.reason`. */
export function rejectionLabel(reason: string): string {
  switch (reason) {
    case 'node_limit':
      return t('live.reject.node_limit');
    case 'token_limit':
      return t('live.reject.token_limit');
    case 'channel_limit':
      return t('live.reject.channel_limit');
    case 'subscription_limit':
      return t('live.reject.subscription_limit');
    case 'origin':
      return t('live.reject.origin');
    case 'forbidden':
      return t('live.reject.forbidden');
    case 'unauthorized':
      return t('live.reject.unauthorized');
    default:
      return reason;
  }
}

/** The gateway summed over the nodes that answered. */
export interface LiveTotals {
  nodes: number;
  connectionsWs: number;
  connectionsSse: number;
  subscriptionsLive: number;
  subscriptionsCatchup: number;
  messagesSent: number;
  bytesSent: number;
  slowDisconnects: number;
  gaps: number;
  rejections: LiveRejection[];
  channels: LiveChannelCount[];
}

export function liveTotals(nodes: readonly LiveNodeStatus[]): LiveTotals {
  const rej = new Map<string, number>();
  const ch = new Map<string, number>();
  const tot: LiveTotals = { nodes: nodes.length, connectionsWs: 0, connectionsSse: 0, subscriptionsLive: 0, subscriptionsCatchup: 0, messagesSent: 0, bytesSent: 0, slowDisconnects: 0, gaps: 0, rejections: [], channels: [] };
  for (const n of nodes) {
    tot.connectionsWs += n.connections_ws;
    tot.connectionsSse += n.connections_sse;
    tot.subscriptionsLive += n.subscriptions_live;
    tot.subscriptionsCatchup += n.subscriptions_catchup;
    tot.messagesSent += n.messages_sent;
    tot.bytesSent += n.bytes_sent;
    tot.slowDisconnects += n.slow_disconnects;
    tot.gaps += n.gaps;
    for (const r of n.rejections) rej.set(r.reason, (rej.get(r.reason) ?? 0) + r.count);
    for (const c of n.channels) ch.set(c.channel, (ch.get(c.channel) ?? 0) + c.subscriptions);
  }
  tot.rejections = [...rej].map(([reason, count]) => ({ reason, count })).sort((a, b) => b.count - a.count);
  tot.channels = [...ch].map(([channel, subscriptions]) => ({ channel, subscriptions })).sort((a, b) => b.subscriptions - a.subscriptions || a.channel.localeCompare(b.channel));
  return tot;
}

/** Subscriptions on one channel over all nodes. */
export function channelSubscriptions(nodes: readonly LiveNodeStatus[], channel: string): number {
  return nodes.reduce((sum, n) => sum + (n.channels.find((c) => c.channel === channel)?.subscriptions ?? 0), 0);
}

/** The `--live-*` flags shown on the status page, in display order. `unlimited0`: 0 means no limit. */
export const LIMIT_ROWS: { key: keyof Omit<LiveLimits, 'allowed_origins'>; flag: string; label: TKey; unit?: 'bytes' | 'bytesPerSec' | 'secs' | 'perSec'; unlimited0?: boolean }[] = [
  { key: 'max_connections', flag: '--live-max-connections', label: 'live.limit.maxConnections' },
  { key: 'max_connections_per_token', flag: '--live-max-connections-per-token', label: 'live.limit.perToken' },
  { key: 'max_connections_per_channel', flag: '--live-max-connections-per-channel', label: 'live.limit.perChannel' },
  { key: 'max_subscriptions', flag: '--live-max-subscriptions', label: 'live.limit.subscriptions' },
  { key: 'max_msgs_per_sec', flag: '--live-max-msgs-per-sec', label: 'live.limit.msgsPerSec', unit: 'perSec', unlimited0: true },
  { key: 'max_bytes_per_sec', flag: '--live-max-bytes-per-sec', label: 'live.limit.bytesPerSec', unit: 'bytesPerSec', unlimited0: true },
  { key: 'max_payload_bytes', flag: '--live-max-payload-bytes', label: 'live.limit.payload', unit: 'bytes' },
  { key: 'max_batch', flag: '--live-max-batch', label: 'live.limit.batch' },
  { key: 'send_buffer', flag: '--live-send-buffer', label: 'live.limit.sendBuffer' },
  { key: 'send_timeout_secs', flag: '--live-send-timeout', label: 'live.limit.sendTimeout', unit: 'secs' },
  { key: 'heartbeat_secs', flag: '--live-heartbeat', label: 'live.limit.heartbeat', unit: 'secs' },
  { key: 'idle_timeout_secs', flag: '--live-idle-timeout', label: 'live.limit.idleTimeout', unit: 'secs' },
  { key: 'fanout_buffer', flag: '--live-fanout-buffer', label: 'live.limit.fanoutBuffer' },
];
