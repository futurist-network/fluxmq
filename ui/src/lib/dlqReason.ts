/**
 * Dead-letter reasons in human words. The reason is a raw string: either a fixed code from the
 * broker (`ack timeout`, `consumer disconnected`, `unknown` — crates/fluxmq-core/src/group.rs),
 * the router (`route target unavailable: <channel>`), webhook deliveries (`HTTP <status>` or a
 * `short_error` class — crates/fluxmq-proto/src/api.rs), or whatever text the consumer passed
 * to nack(). Known codes get a translated title and a "what to do" hint; consumer text is
 * shown as is (it is data, not UI), with a hint saying where it came from.
 */
import { t } from '../i18n';

export type ReasonKind =
  | 'ack_timeout'
  | 'disconnected'
  | 'no_reason'
  | 'route_target'
  | 'http_rate_limit'
  | 'http_unavailable'
  | 'http_rejected'
  | 'http_redirect'
  | 'timeout'
  | 'conn_refused'
  | 'conn_failed'
  | 'tls'
  | 'dns'
  | 'blocked'
  | 'request_failed'
  | 'consumer';

export interface ReasonText {
  kind: ReasonKind;
  /** Translated title for known codes; the consumer's own text otherwise. */
  title: string;
  /** What the reason means and what to do about it. */
  hint: string;
  /** The title isn't the raw reason: show the raw one too (it's the grouping key). */
  coded: boolean;
  /** Retrying won't help until the producer, consumer or config changes. */
  permanent: boolean;
}

/**
 * Is a consumer's reason text a permanent (non-transient) error? Schema, validation, parsing and
 * 4xx-style errors don't go away by retrying: the producer or the consumer has to change first.
 * Heuristic; unknown reasons count as transient.
 */
const NON_TRANSIENT = /schema|valid|malformed|pars(e|ing)|deserial|decod|unmarshal|invalid|unsupported|unknown (field|type|event)|missing (field|required)|bad request|unprocessable|\b(400|404|409|410|422)\b|constraint|duplicate key|not allowed|forbidden/i;

/** Fixed codes → kind (exact match: the broker and webhooks never decorate them). */
const CODES: Record<string, ReasonKind> = {
  'ack timeout': 'ack_timeout',
  'consumer disconnected': 'disconnected',
  unknown: 'no_reason',
  timeout: 'timeout',
  'connection refused': 'conn_refused',
  'connection failed': 'conn_failed',
  'TLS error': 'tls',
  'DNS error': 'dns',
  'blocked by policy': 'blocked',
  'request failed': 'request_failed',
};

const ROUTE_PREFIX = 'route target unavailable: ';

/** HTTP status → kind. 408/425/429 and 5xx are retried by webhooks; other 4xx and 3xx aren't. */
function httpKind(status: number): ReasonKind {
  if (status === 429) return 'http_rate_limit';
  if (status === 408 || status === 425 || status >= 500) return 'http_unavailable';
  if (status >= 400) return 'http_rejected';
  return 'http_redirect';
}

const PERMANENT_KINDS = new Set<ReasonKind>(['http_rejected', 'http_redirect', 'blocked']);

export function classifyReason(reason: string): { kind: ReasonKind; status?: number; target?: string } {
  const code = CODES[reason.trim()];
  if (code) return { kind: code };
  if (reason.startsWith(ROUTE_PREFIX)) return { kind: 'route_target', target: reason.slice(ROUTE_PREFIX.length).trim() };
  const m = /^HTTP (\d{3})$/.exec(reason.trim());
  if (m) {
    const status = Number(m[1]);
    return { kind: httpKind(status), status };
  }
  return { kind: 'consumer' };
}

export function isNonTransient(reason: string): boolean {
  const { kind } = classifyReason(reason);
  return kind === 'consumer' ? NON_TRANSIENT.test(reason) : PERMANENT_KINDS.has(kind);
}

/** Title + hint for a reason, in the current language. */
export function reasonText(reason: string): ReasonText {
  const c = classifyReason(reason);
  const permanent = isNonTransient(reason);
  const p = { status: c.status ?? '', target: c.target ?? '' };
  switch (c.kind) {
    case 'consumer':
      return { kind: c.kind, title: reason, hint: t(permanent ? 'dlq.why.consumerPermanent' : 'dlq.why.consumer'), coded: false, permanent };
    case 'ack_timeout':
      return coded(c.kind, 'dlq.why.ackTimeout', 'dlq.why.ackTimeoutHint', p, permanent);
    case 'disconnected':
      return coded(c.kind, 'dlq.why.disconnected', 'dlq.why.disconnectedHint', p, permanent);
    case 'no_reason':
      return coded(c.kind, 'dlq.why.noReason', 'dlq.why.noReasonHint', p, permanent);
    case 'route_target':
      return coded(c.kind, 'dlq.why.routeTarget', 'dlq.why.routeTargetHint', p, permanent);
    case 'http_rate_limit':
      return coded(c.kind, 'dlq.why.httpRateLimit', 'dlq.why.httpRateLimitHint', p, permanent);
    case 'http_unavailable':
      return coded(c.kind, 'dlq.why.httpUnavailable', 'dlq.why.httpUnavailableHint', p, permanent);
    case 'http_rejected':
      return coded(c.kind, 'dlq.why.httpRejected', 'dlq.why.httpRejectedHint', p, permanent);
    case 'http_redirect':
      return coded(c.kind, 'dlq.why.httpRedirect', 'dlq.why.httpRedirectHint', p, permanent);
    case 'timeout':
      return coded(c.kind, 'dlq.why.timeout', 'dlq.why.timeoutHint', p, permanent);
    case 'conn_refused':
      return coded(c.kind, 'dlq.why.connRefused', 'dlq.why.connRefusedHint', p, permanent);
    case 'conn_failed':
      return coded(c.kind, 'dlq.why.connFailed', 'dlq.why.connFailedHint', p, permanent);
    case 'tls':
      return coded(c.kind, 'dlq.why.tls', 'dlq.why.tlsHint', p, permanent);
    case 'dns':
      return coded(c.kind, 'dlq.why.dns', 'dlq.why.dnsHint', p, permanent);
    case 'blocked':
      return coded(c.kind, 'dlq.why.blocked', 'dlq.why.blockedHint', p, permanent);
    case 'request_failed':
      return coded(c.kind, 'dlq.why.requestFailed', 'dlq.why.requestFailedHint', p, permanent);
  }
}

type WhyKey = Parameters<typeof t>[0] & `dlq.why.${string}`;

function coded(kind: ReasonKind, title: WhyKey, hint: WhyKey, p: { status: number | string; target: string }, permanent: boolean): ReasonText {
  return { kind, title: t(title, p), hint: t(hint, p), coded: true, permanent };
}
