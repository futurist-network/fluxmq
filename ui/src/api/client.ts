/**
 * Typed REST client — one function per route in docs/ARCHITECTURE.md.
 * Every call sends `Authorization: Bearer <token>` when an admin token is configured and
 * throws `ApiError` on non-2xx (the server's `{error, message}` body when present).
 */
import { getToken } from '../lib/settings';
import { getLang, hasKey, t } from '../i18n';
import type {
  ApiErrorBody,
  ApiErrorCode,
  AlertsState,
  AuditEntry,
  ChannelConfig,
  ClusterMetadata,
  ClusterStatus,
  Condition,
  CreateChannelRequest,
  CreateRouteRequest,
  CreateTokenRequest,
  CreateTokenResponse,
  CertBinding,
  CertBindingList,
  CreateCertBindingRequest,
  DecommissionStatus,
  DrainResponse,
  ParseFilterResponse,
  RenderFilterResponse,
  ReassignRequest,
  RebalanceConfig,
  RebalanceRequest,
  RebalanceStatus,
  TlsStatus,
  Route,
  RouteStats,
  RouteTestRequest,
  RouteTestResponse,
  TokenInfo,
  UpdateRouteRequest,
  WhoAmI,
  DlqActionRequest,
  DlqActionResponse,
  DlqPage,
  DlqQuery,
  DlqSummary,
  HealthResponse,
  HistoryRange,
  HistorySeries,
  MessageDetail,
  MessagePage,
  MessageQuery,
  PublishRequest,
  PublishResponse,
  ReplayRequest,
  SearchResult,
  StartFrom,
  UpdateChannelRequest,
  BackupConfig,
  BackupInfo,
  BackupManifest,
  BackupOverview,
  BackupTarget,
  CreateNotifierRequest,
  CreateRuleRequest,
  CreateWebhookRequest,
  FeedPage,
  NotificationRecord,
  NotificationRule,
  Notifier,
  TestResult,
  UpdateNotifierRequest,
  UpdateRuleRequest,
  UpdateWebhookRequest,
  WebhookStats,
  WebhookAttempt,
  WebhookHistory,
  WebhookHistoryRange,
  LiveStatus,
  ScheduledList,
  MessageId,
  WebhookSubscription,
  WebhookTestResult,
} from './types';

export class ApiError extends Error {
  readonly status: number;
  /** `bad_request` | `unauthorized` | `forbidden` | `not_found` | `conflict` | `internal` | `network` | ... */
  readonly code: string;
  /**
   * Byte offset (UTF-8) of a filter DSL syntax error — the server's `ApiError.position`
   * (message search `filter=`, `POST /api/filter/parse`). Null for every other error.
   * Convert with `byteToIndex` (lib/filterError.ts) before indexing a JS string.
   */
  readonly position: number | null;

  constructor(status: number, code: string, message: string, position: number | null = null) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
    this.position = position;
  }

  get isUnauthorized(): boolean {
    return this.status === 401;
  }
  get isNotFound(): boolean {
    return this.status === 404;
  }
  get isForbidden(): boolean {
    return this.status === 403;
  }
  get isConflict(): boolean {
    return this.status === 409;
  }
}

type Listener = (err: ApiError) => void;
const unauthorizedListeners = new Set<Listener>();

/** Notified on any 401 so the shell can prompt for a token. */
export function onUnauthorized(l: Listener): () => void {
  unauthorizedListeners.add(l);
  return () => unauthorizedListeners.delete(l);
}

const forbiddenListeners = new Set<Listener>();

/** Notified on any 403 (valid token, insufficient role) so the shell can explain it once. */
export function onForbidden(l: Listener): () => void {
  forbiddenListeners.add(l);
  return () => forbiddenListeners.delete(l);
}

type Query = Record<string, string | number | boolean | null | undefined>;

export function buildQuery(q: Query | undefined): string {
  if (!q) return '';
  const p = new URLSearchParams();
  for (const [k, v] of Object.entries(q)) {
    if (v === undefined || v === null || v === '') continue;
    p.set(k, String(v));
  }
  const s = p.toString();
  return s ? `?${s}` : '';
}

const enc = encodeURIComponent;

export function authHeaders(): Record<string, string> {
  const token = getToken();
  return token ? { Authorization: `Bearer ${token}` } : {};
}

interface RequestOpts {
  query?: Query;
  body?: unknown;
  raw?: boolean;
  signal?: AbortSignal;
  /** Don't broadcast 403 to the shell (the caller renders it inline). */
  quiet403?: boolean;
  /** Use this token instead of the stored one, and don't broadcast 401 (verifying a candidate token). */
  token?: string;
}

async function request<T>(method: string, path: string, opts: RequestOpts = {}): Promise<T> {
  const headers: Record<string, string> = { Accept: 'application/json', ...(opts.token !== undefined ? { Authorization: `Bearer ${opts.token}` } : authHeaders()) };
  let body: string | undefined;
  if (opts.body !== undefined) {
    headers['Content-Type'] = 'application/json';
    body = JSON.stringify(opts.body);
  }
  let res: Response;
  try {
    res = await fetch(`${path}${buildQuery(opts.query)}`, { method, headers, body, signal: opts.signal });
  } catch (e) {
    if (e instanceof DOMException && e.name === 'AbortError') throw e;
    throw new ApiError(0, 'network', t('errors.network'));
  }
  if (!res.ok) {
    let code = res.status === 401 ? 'unauthorized' : res.status === 403 ? 'forbidden' : res.status === 404 ? 'not_found' : 'http_error';
    let message = res.statusText || `HTTP ${res.status}`;
    let position: number | null = null;
    try {
      const b = (await res.json()) as Partial<ApiErrorBody>;
      if (typeof b.error === 'string') code = b.error;
      if (typeof b.message === 'string') message = b.message;
      if (typeof b.position === 'number' && Number.isInteger(b.position) && b.position >= 0) position = b.position;
    } catch {
      /* non-JSON error body */
    }
    const err = new ApiError(res.status, code, message, position);
    if (res.status === 401 && opts.token === undefined) unauthorizedListeners.forEach((l) => l(err));
    if (res.status === 403 && !opts.quiet403) forbiddenListeners.forEach((l) => l(err));
    throw err;
  }
  if (opts.raw) return (await res.text()) as T;
  if (res.status === 204) return undefined as T;
  const text = await res.text();
  return (text ? JSON.parse(text) : undefined) as T;
}

export const api = {
  /** Liveness; never requires the admin token (load balancers probe it). */
  health: () => request<HealthResponse>('GET', '/api/health'),
  /** CPU profile of the node serving the console (`profiling` builds only). Blocks for `seconds`. */
  cpuProfile: (seconds: number, format: 'svg' | 'folded', signal?: AbortSignal) =>
    request<string>('GET', '/api/debug/pprof/profile', { query: { seconds, hz: 99, format }, raw: true, signal }),
  /** Cheapest token-protected call: resolves iff the current token (or dev mode) is accepted. */
  checkAuth: () => request<unknown>('GET', '/api/alerts'),
  /** Same check with a candidate token, before storing it (a 401 is not broadcast to the shell). */
  checkToken: (token: string) => request<unknown>('GET', '/api/alerts', { token }),

  listChannels: () => request<ChannelConfig[]>('GET', '/api/channels'),
  getChannel: (name: string) => request<ChannelConfig>('GET', `/api/channels/${enc(name)}`),
  createChannel: (req: CreateChannelRequest) => request<ChannelConfig>('POST', '/api/channels', { body: req }),
  updateChannel: (name: string, req: UpdateChannelRequest) => request<ChannelConfig>('PATCH', `/api/channels/${enc(name)}`, { body: req }),
  deleteChannel: (name: string) => request<void>('DELETE', `/api/channels/${enc(name)}`),

  publish: (name: string, req: PublishRequest) => request<PublishResponse>('POST', `/api/channels/${enc(name)}/publish`, { body: req }),
  messages: (name: string, q: MessageQuery, signal?: AbortSignal) =>
    request<MessagePage>('GET', `/api/channels/${enc(name)}/messages`, { query: { ...q }, signal }),
  message: (name: string, partition: number, offset: number, signal?: AbortSignal) =>
    request<MessageDetail>('GET', `/api/channels/${enc(name)}/messages/${partition}/${offset}`, { signal }),
  replay: (name: string, req: ReplayRequest) => request<PublishResponse>('POST', `/api/channels/${enc(name)}/replay`, { body: req }),
  resetOffsets: (name: string, group: string, to: StartFrom) =>
    request<void>('POST', `/api/channels/${enc(name)}/groups/${enc(group)}/reset`, { body: { to } }),
  /** Admin; 409 while the group has subscribers or is a `$…` system group still in use. */
  deleteGroup: (name: string, group: string) => request<void>('DELETE', `/api/channels/${enc(name)}/groups/${enc(group)}`),

  dlqSummary: () => request<DlqSummary>('GET', '/api/dlq/summary'),
  dlq: (q: DlqQuery, signal?: AbortSignal) => request<DlqPage>('GET', '/api/dlq', { query: { ...q }, signal }),
  dlqRetry: (req: DlqActionRequest) => request<DlqActionResponse>('POST', '/api/dlq/retry', { body: req }),
  dlqDiscard: (req: DlqActionRequest) => request<DlqActionResponse>('POST', '/api/dlq/discard', { body: req }),
  dlqExport: (q: DlqQuery) => request<string>('GET', '/api/dlq/export', { query: { ...q }, raw: true }),

  history: (metric: string, range: HistoryRange, signal?: AbortSignal) =>
    request<HistorySeries>('GET', '/api/history', { query: { metric, range }, signal }),
  alerts: () => request<AlertsState>('GET', '/api/alerts'),
  search: (q: string, signal?: AbortSignal) => request<SearchResult[]>('GET', '/api/search', { query: { q }, signal }),
  audit: (limit = 50) => request<AuditEntry[]>('GET', '/api/audit', { query: { limit } }),

  // ── RBAC (v0.2) ──
  whoami: (signal?: AbortSignal) => request<WhoAmI>('GET', '/api/auth/me', { signal }),
  listTokens: (signal?: AbortSignal) => request<TokenInfo[]>('GET', '/api/auth/tokens', { signal, quiet403: true }),
  createToken: (req: CreateTokenRequest) => request<CreateTokenResponse>('POST', '/api/auth/tokens', { body: req }),
  /** v0.5: closes live client / WS sessions using the token within 1 s. Revoking the last active
   *  admin while other tokens exist (no bootstrap token) → 409 `last_admin`. */
  revokeToken: (id: string) => request<void>('DELETE', `/api/auth/tokens/${enc(id)}`),

  // ── Certificate bindings (v0.5, admin) — mTLS client CN → principal on the client port ──
  listCertBindings: (signal?: AbortSignal) => request<CertBindingList>('GET', '/api/auth/cert-bindings', { signal, quiet403: true }),
  createCertBinding: (req: CreateCertBindingRequest) => request<CertBinding>('POST', '/api/auth/cert-bindings', { body: req }),
  deleteCertBinding: (cn: string) => request<void>('DELETE', `/api/auth/cert-bindings/${enc(cn)}`),

  // ── Routing (v0.2) ──
  listRoutes: (signal?: AbortSignal) => request<Route[]>('GET', '/api/routes', { signal }),
  getRoute: (id: string, signal?: AbortSignal) => request<Route>('GET', `/api/routes/${enc(id)}`, { signal }),
  createRoute: (req: CreateRouteRequest) => request<Route>('POST', '/api/routes', { body: req }),
  updateRoute: (id: string, req: UpdateRouteRequest) => request<Route>('PATCH', `/api/routes/${enc(id)}`, { body: req }),
  deleteRoute: (id: string) => request<void>('DELETE', `/api/routes/${enc(id)}`),
  testRoute: (req: RouteTestRequest, signal?: AbortSignal) => request<RouteTestResponse>('POST', '/api/routes/test', { body: req, signal, quiet403: true }),
  routeStats: (signal?: AbortSignal) => request<RouteStats[]>('GET', '/api/routes/stats', { signal }),
  parseFilter: (filter: string, signal?: AbortSignal) => request<ParseFilterResponse>('POST', '/api/filter/parse', { body: { filter }, signal, quiet403: true }),
  renderFilter: (conditions: Condition[], signal?: AbortSignal) => request<RenderFilterResponse>('POST', '/api/filter/render', { body: { conditions }, signal, quiet403: true }),

  // ── Cluster (v0.2) ──
  clusterStatus: (signal?: AbortSignal) => request<ClusterStatus>('GET', '/api/cluster/status', { signal }),
  clusterMetadata: (signal?: AbortSignal) => request<ClusterMetadata>('GET', '/api/cluster/metadata', { signal }),
  drainNode: (id: string) => request<DrainResponse>('POST', `/api/cluster/nodes/${enc(id)}/drain`),
  reassignPartition: (req: ReassignRequest) => request<void>('POST', '/api/cluster/partitions/reassign', { body: req }),

  // ── Node decommission / forget (v0.5) ──
  /** Admin. Not enough other `up` nodes for some channel's RF, a running rebalance plan, or a
   *  Raft quorum that would break → 409 `conflict` (the message says which). */
  decommissionNode: (id: string) => request<DecommissionStatus>('POST', `/api/cluster/nodes/${enc(id)}/decommission`),
  /** Optional server feature: the planned moves without starting (`RebalanceStatus`). Servers
   *  without it answer 400 / 404 — callers fall back to their own projection. */
  decommissionDryRun: (id: string, signal?: AbortSignal) => request<RebalanceStatus>('POST', `/api/cluster/nodes/${enc(id)}/decommission`, { query: { dry_run: true }, signal, quiet403: true }),
  /** Viewer. Current and recent (24 h) decommissions. */
  listDecommissions: (signal?: AbortSignal) => request<DecommissionStatus[]>('GET', '/api/cluster/decommissions', { signal }),
  /** Viewer. Progress of a decommission. */
  decommissionStatus: (id: string, signal?: AbortSignal) => request<DecommissionStatus>('GET', `/api/cluster/nodes/${enc(id)}/decommission`, { signal }),
  /** Admin. Only a `down` node holding no replicas; otherwise 409 `conflict`. */
  forgetNode: (id: string) => request<void>('DELETE', `/api/cluster/nodes/${enc(id)}`),

  // ── Automatic rebalancing (v0.4) — GET = viewer, mutations = admin; on a single node the
  //    mutations answer 400 `unsupported_single_node` ──
  rebalanceStatus: (signal?: AbortSignal) => request<RebalanceStatus>('GET', '/api/cluster/rebalance', { signal }),
  /** Every mutation answers with the resulting `RebalanceStatus`. */
  putRebalanceConfig: (cfg: RebalanceConfig) => request<RebalanceStatus>('PUT', '/api/cluster/rebalance/config', { body: cfg }),
  /** `dry_run: true` returns the proposed plan without executing it (always allowed); an
   *  executing request while a plan is running / paused / cancelling → 409 `conflict`. */
  rebalance: (req: RebalanceRequest) => request<RebalanceStatus>('POST', '/api/cluster/rebalance', { body: req }),
  pauseRebalance: () => request<RebalanceStatus>('POST', '/api/cluster/rebalance/pause'),
  resumeRebalance: () => request<RebalanceStatus>('POST', '/api/cluster/rebalance/resume'),
  /** Pending moves become `cancelled`; in-flight ones finish (or roll back). */
  cancelRebalance: () => request<RebalanceStatus>('POST', '/api/cluster/rebalance/cancel'),

  // ── TLS (v0.4) ──
  tlsStatus: (signal?: AbortSignal) => request<TlsStatus>('GET', '/api/tls', { signal }),

  // ── Notifications (v0.3) — GET = read (secrets redacted), mutations + test = admin ──
  listNotifiers: (signal?: AbortSignal) => request<Notifier[]>('GET', '/api/notify/notifiers', { signal }),
  getNotifier: (id: string, signal?: AbortSignal) => request<Notifier>('GET', `/api/notify/notifiers/${enc(id)}`, { signal }),
  createNotifier: (req: CreateNotifierRequest) => request<Notifier>('POST', '/api/notify/notifiers', { body: req }),
  updateNotifier: (id: string, req: UpdateNotifierRequest) => request<Notifier>('PATCH', `/api/notify/notifiers/${enc(id)}`, { body: req }),
  deleteNotifier: (id: string) => request<void>('DELETE', `/api/notify/notifiers/${enc(id)}`),
  /** Sends a test message with the stored notifier; `draft` (update shape, REDACTED = stored
   *  secret) is merged over it without being persisted. */
  testNotifier: (id: string, draft?: UpdateNotifierRequest) =>
    request<TestResult>('POST', `/api/notify/notifiers/${enc(id)}/test`, draft ? { body: draft } : {}),
  /** Test before saving (nothing is persisted). */
  testNotifierDraft: (req: CreateNotifierRequest) => request<TestResult>('POST', '/api/notify/notifiers/test', { body: req }),
  listRules: (signal?: AbortSignal) => request<NotificationRule[]>('GET', '/api/notify/rules', { signal }),
  createRule: (req: CreateRuleRequest) => request<NotificationRule>('POST', '/api/notify/rules', { body: req }),
  updateRule: (id: string, req: UpdateRuleRequest) => request<NotificationRule>('PATCH', `/api/notify/rules/${enc(id)}`, { body: req }),
  deleteRule: (id: string) => request<void>('DELETE', `/api/notify/rules/${enc(id)}`),
  notifyHistory: (signal?: AbortSignal) => request<NotificationRecord[]>('GET', '/api/notify/history', { signal }),
  /** Pull feed (`$system.events`); `wait_ms` long-polls (0..30000). `tail` (≤ 1000) = newest N,
   *  mutually exclusive with `after`. */
  eventsFeed: (q: { after?: string; tail?: number; limit?: number; wait_ms?: number }, signal?: AbortSignal) => request<FeedPage>('GET', '/api/events/feed', { query: { ...q }, signal }),

  // ── HTTP push webhooks (v0.3) ──
  listWebhooks: (signal?: AbortSignal) => request<WebhookSubscription[]>('GET', '/api/webhooks', { signal }),
  getWebhook: (id: string, signal?: AbortSignal) => request<WebhookSubscription>('GET', `/api/webhooks/${enc(id)}`, { signal }),
  createWebhook: (req: CreateWebhookRequest) => request<WebhookSubscription>('POST', '/api/webhooks', { body: req }),
  updateWebhook: (id: string, req: UpdateWebhookRequest) => request<WebhookSubscription>('PATCH', `/api/webhooks/${enc(id)}`, { body: req }),
  deleteWebhook: (id: string) => request<void>('DELETE', `/api/webhooks/${enc(id)}`),
  /** One synthetic envelope; `draft` (update shape, REDACTED = stored value) is merged over the
   *  stored subscription without being persisted. */
  testWebhook: (id: string, draft?: UpdateWebhookRequest) =>
    request<WebhookTestResult>('POST', `/api/webhooks/${enc(id)}/test`, draft ? { body: draft } : {}),
  testWebhookDraft: (req: CreateWebhookRequest) => request<WebhookTestResult>('POST', '/api/webhooks/test', { body: req }),
  webhookStats: (signal?: AbortSignal) => request<WebhookStats[]>('GET', '/api/webhooks/stats', { signal }),
  webhookAttempts: (id: string, signal?: AbortSignal) => request<WebhookAttempt[]>('GET', `/api/webhooks/${enc(id)}/attempts`, { signal }),
  webhookHistory: (id: string, range: WebhookHistoryRange, signal?: AbortSignal) => request<WebhookHistory>('GET', `/api/webhooks/${enc(id)}/history`, { query: { range }, signal }),

  // ── Live subscriptions (v0.11 status; the gateway itself is WebSocket/SSE) ──
  liveStatus: (signal?: AbortSignal) => request<LiveStatus>('GET', '/api/live/status', { signal }),

  // ── Delayed messages (v0.12) ──
  scheduledList: (q: { channel?: string; limit?: number }, signal?: AbortSignal) => request<ScheduledList>('GET', '/api/scheduled', { query: q, signal }),
  /** Operator. 404 when it was already released or cancelled. */
  cancelScheduled: (id: MessageId) => request<void>('DELETE', `/api/scheduled/${id.partition}/${id.offset}`),
  /** Operator: publish it now. */
  releaseScheduled: (id: MessageId) => request<void>('POST', `/api/scheduled/${id.partition}/${id.offset}/release`),

  // ── Backups (v0.3) ──
  /** null when no backup is configured. */
  backupConfig: (signal?: AbortSignal) => request<BackupConfig | null>('GET', '/api/backups/config', { signal }),
  putBackupConfig: (cfg: BackupConfig) => request<BackupConfig>('PUT', '/api/backups/config', { body: cfg }),
  testBackupTarget: (target: BackupTarget) => request<TestResult>('POST', '/api/backups/test-target', { body: target }),
  backupStatus: (signal?: AbortSignal) => request<BackupOverview>('GET', '/api/backups/status', { signal }),
  runBackup: () => request<BackupInfo | undefined>('POST', '/api/backups/run'),
  listBackups: (signal?: AbortSignal) => request<BackupInfo[]>('GET', '/api/backups', { signal }),
  /** Manifest summary (`info`, encryption fingerprint, nodes, per-channel summary); `latest` = newest. */
  getBackup: (id: string, signal?: AbortSignal) => request<BackupManifest>('GET', `/api/backups/${enc(id)}`, { signal }),
  deleteBackup: (id: string) => request<void>('DELETE', `/api/backups/${enc(id)}`),
};

/** True when `e` is an API error with this machine-readable code (`ApiError.error`). */
export function isApiErrorCode(e: unknown, code: ApiErrorCode): boolean {
  return e instanceof ApiError && e.code === code;
}

/** Error codes whose message the console words itself (the server's text is English and generic). */
const LOCALIZED_CODES = new Set(['network', 'unauthorized', 'forbidden', 'unavailable', 'internal', 'unsupported_in_cluster', 'unsupported_single_node', 'last_admin']);

/**
 * User-facing text for an error. English shows the server's message (it carries specifics);
 * other languages map known `ApiError.code`s to a translated explanation and otherwise fall back
 * to the server's message (validation details such as a channel name stay verbatim).
 */
export function errorMessage(e: unknown): string {
  if (e instanceof ApiError) {
    if (e.code === 'network') return t('errors.network');
    const key = `errors.${e.code}`;
    if (getLang() !== 'en' && LOCALIZED_CODES.has(e.code) && hasKey(key)) return t(key);
    return e.message;
  }
  if (e instanceof Error) return e.message;
  return String(e);
}
