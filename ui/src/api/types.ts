/**
 * TypeScript mirror of the FluxMQ contract.
 *
 * Source of truth (Rust, serde JSON):
 *   crates/fluxmq-proto/src/model.rs      — domain types
 *   crates/fluxmq-proto/src/api.rs        — REST bodies
 *   crates/fluxmq-proto/src/telemetry.rs  — WebSocket telemetry contract v1
 *   crates/fluxmq-proto/src/auth.rs       — RBAC (v0.2): roles, tokens, whoami
 *   crates/fluxmq-proto/src/routing.rs    — routes + filter DSL (v0.2)
 *   crates/fluxmq-proto/src/cluster.rs    — cluster metadata / status (v0.2)
 *   crates/fluxmq-proto/src/notify.rs     — notifiers, rules, history, pull feed (v0.3)
 *   crates/fluxmq-proto/src/webhooks.rs   — HTTP push subscriptions (v0.3)
 *   crates/fluxmq-proto/src/backup.rs     — backups (v0.3)
 *
 * Hand-written on purpose: keep field names / casing identical to serde output.
 * `Option<T>` → `T | null` (serde serializes None as null; missing on input is accepted).
 * `u64` values are JSON numbers; they stay < 2^53 in practice.
 */

// ───────────────────────────── model.rs ─────────────────────────────

/** `priority` (v0.12): a queue whose partitions are priority levels (2–10), highest first. */
export type ChannelMode = 'stream' | 'queue' | 'keyed' | 'priority';
export const CHANNEL_MODES: readonly ChannelMode[] = ['stream', 'queue', 'keyed', 'priority'];
/** Priority levels of a `priority` channel (its partition count). */
export const PRIORITY_LEVELS = { min: 2, max: 10 } as const;

export interface RetentionPolicy {
  /** null = keep forever */
  max_age_secs: number | null;
  /** null = unbounded */
  max_bytes: number | null;
}

export interface RetryPolicy {
  max_attempts: number;
  backoff_ms: number[];
  ack_timeout_ms: number;
}

export interface ChannelConfig {
  name: string;
  mode: ChannelMode;
  partitions: number;
  retention: RetentionPolicy;
  retry: RetryPolicy;
  dlq_enabled: boolean;
  ordering_key: string | null;
  created_at_ms: number;
}

/** Well-known header names (`model::headers`). */
export const HEADERS = {
  PRODUCER: 'flux-producer',
  TRACE_ID: 'flux-trace-id',
  CONTENT_TYPE: 'content-type',
  EVENT_TYPE: 'flux-type',
} as const;

/** Externally tagged: `"earliest"`, `"latest"`, `{"timestamp_ms": n}`. */
export type StartFrom = 'earliest' | 'latest' | { timestamp_ms: number };

export type AttemptOutcome =
  | { kind: 'in_flight' }
  | { kind: 'acked' }
  | { kind: 'nacked'; reason: string }
  | { kind: 'timed_out' };

export interface DeliveryAttempt {
  attempt: number;
  consumer: string;
  delivered_at_ms: number;
  finished_at_ms: number | null;
  outcome: AttemptOutcome;
}

export interface MessageId {
  partition: number;
  offset: number;
}

/** Channel name rules — mirrors `validate_channel_name`. Returns an error string or null. */
export function validateChannelName(name: string): string | null {
  if (name.length === 0 || name.length > 128) return 'channel name must be 1-128 characters';
  if (!/^[a-zA-Z0-9._-]+$/.test(name)) return 'channel name may only contain [a-zA-Z0-9._-]';
  if (name.endsWith('.consumers') || name.endsWith('.tail') || name.startsWith('.')) {
    return "channel name may not start with '.' or end with '.consumers'/'.tail'";
  }
  return null;
}

// ───────────────────────────── api.rs ─────────────────────────────

/** Machine-readable `ApiError.error` codes (api.rs). Branch on these, never on `message`. */
export type ApiErrorCode =
  | 'bad_request'
  | 'unauthorized'
  | 'forbidden'
  | 'not_found'
  | 'conflict'
  | 'unavailable'
  | 'internal'
  /** v0.3: refused because the node is part of a cluster (HTTP 400). */
  | 'unsupported_in_cluster'
  /** v0.4: cluster-only operation (rebalancing) on a single node (HTTP 400). */
  | 'unsupported_single_node'
  /** v0.5: the operation would remove the last admin (HTTP 409, auth.rs `ERROR_LAST_ADMIN`). */
  | 'last_admin';

/** `ApiError.error` of an operation refused because the node is clustered (`ERROR_UNSUPPORTED_IN_CLUSTER`). */
export const ERROR_UNSUPPORTED_IN_CLUSTER: ApiErrorCode = 'unsupported_in_cluster';
/** `ApiError.error` of a cluster-only operation on a single node (`ERROR_UNSUPPORTED_SINGLE_NODE`). */
export const ERROR_UNSUPPORTED_SINGLE_NODE: ApiErrorCode = 'unsupported_single_node';
/** v0.5 (auth.rs `ERROR_LAST_ADMIN`): revoking the last active admin token while other tokens exist
 *  and no bootstrap `--admin-token` is configured → HTTP 409 with this code. */
export const ERROR_LAST_ADMIN: ApiErrorCode = 'last_admin';

export interface ApiErrorBody {
  /** One of {@link ApiErrorCode} (kept as `string` for forward compatibility). */
  error: ApiErrorCode | (string & {});
  message: string;
  /** Byte offset (UTF-8) of a filter DSL syntax error in the submitted text (`filter=` on
   *  message search, `POST /api/filter/parse`). Absent for every other error. */
  position?: number;
}

export type JsonPayload = { encoding: 'utf8'; data: string } | { encoding: 'base64'; data: string };

export interface CreateChannelRequest {
  name: string;
  mode: ChannelMode;
  partitions: number | null;
  retention: RetentionPolicy | null;
  retry: RetryPolicy | null;
  dlq_enabled: boolean | null;
  ordering_key: string | null;
}

export interface UpdateChannelRequest {
  retention?: RetentionPolicy | null;
  retry?: RetryPolicy | null;
  dlq_enabled?: boolean | null;
}

export interface PublishItem {
  key: string | null;
  headers: Record<string, string>;
  payload: JsonPayload;
  /** v0.12: deliver at this unix time (ms) instead of now. */
  deliver_at_ms?: number;
  /** v0.12: deliver this many ms from now (broker clock). Exclusive with `deliver_at_ms`. */
  delay_ms?: number;
}

export interface PublishRequest {
  messages: PublishItem[];
}

export interface PublishResponse {
  ids: MessageId[];
}

/** Query string of `GET /api/channels/{name}/messages`. */
export interface MessageQuery {
  partition?: number;
  offset?: number;
  key?: string;
  since_ms?: number;
  until_ms?: number;
  /** default 100, max 1000 */
  limit?: number;
  /** Without `offset`: return the newest `limit` matches in [since_ms, until_ms] (still sorted
   *  ascending by the server; `has_more` = older matches exist). */
  newest_first?: boolean;
  /** Filter DSL (see routing.rs), e.g. `order_id = 8842 AND country = "MD"`. Scan-based. */
  filter?: string;
}

export interface MessageView {
  partition: number;
  offset: number;
  key: string | null;
  headers: Record<string, string>;
  timestamp_ms: number;
  size: number;
  payload: JsonPayload;
}

export interface MessagePage {
  messages: MessageView[];
  scanned: number;
  has_more: boolean;
}

export interface MessageDetail {
  message: MessageView;
  /** group → attempts */
  deliveries: Record<string, DeliveryAttempt[]>;
}

export interface ReplayRequest {
  ids: MessageId[];
  target_channel: string | null;
}

export interface DeadLetterView {
  id: number;
  channel: string;
  group: string;
  reason: string;
  dead_at_ms: number;
  message: MessageView;
  attempts: DeliveryAttempt[];
}

export interface DlqReasonGroup {
  reason: string;
  count: number;
  /** channel → count */
  channels: Record<string, number>;
  first_ms: number;
  last_ms: number;
}

export interface DlqSummary {
  total: number;
  groups: DlqReasonGroup[];
}

export interface DlqQuery {
  channel?: string;
  reason?: string;
  limit?: number;
  before?: number;
}

export interface DlqPage {
  entries: DeadLetterView[];
  total: number;
}

/** Exactly one selector must be set. `ids` = `[[channel, id], ...]`. */
export interface DlqActionRequest {
  ids?: [string, number][] | null;
  reason?: string | null;
  channel?: string | null;
  all?: boolean | null;
}

export interface DlqActionResponse {
  affected: number;
}

/** Body of `POST /api/channels/{name}/groups/{group}/reset` (group comes from the path). */
export interface ResetOffsetsRequest {
  to: StartFrom;
}

export interface SearchResult {
  kind: string;
  label: string;
  link: string;
}

export interface AuditEntry {
  ts: number;
  actor: string;
  action: string;
  target: string;
  detail: unknown;
}

export interface HealthResponse {
  status: string;
  version: string;
  node_id: string;
  uptime_secs: number;
  /** Optional build features of the node serving the console, e.g. `profiling`. Absent before v0.11. */
  features?: string[];
}

// ───────────────────────────── telemetry.rs ─────────────────────────────

export const CONTRACT_VERSION = 1;

export type ClientMsg =
  | { op: 'subscribe'; topics: string[] }
  | { op: 'unsubscribe'; topics: string[] }
  | { op: 'ping'; nonce: number | null };

export type ServerMsg =
  | { type: 'hello'; contract_version: number; broker_version: string; node_id: string; server_time_ms: number }
  | { type: 'snapshot'; topic: string; seq: number; ts: number; data: unknown }
  | { type: 'delta'; topic: string; seq: number; ts: number; data: unknown }
  | { type: 'pong'; nonce: number | null; server_time_ms: number }
  | { type: 'error'; topic: string | null; message: string };

export const TOPICS = {
  CLUSTER: 'cluster.overview',
  CHANNELS: 'channels.summary',
  ALERTS: 'alerts',
  EVENTS: 'events',
  NODES: 'nodes',
  channel: (name: string) => `channel.${name}`,
  consumers: (name: string) => `channel.${name}.consumers`,
  tail: (name: string) => `channel.${name}.tail`,
} as const;

export type Topic =
  | { kind: 'cluster' }
  | { kind: 'channels' }
  | { kind: 'alerts' }
  | { kind: 'events' }
  | { kind: 'nodes' }
  | { kind: 'channel'; name: string }
  | { kind: 'consumers'; name: string }
  | { kind: 'tail'; name: string };

/** Mirrors `topics::parse`. */
export function parseTopic(topic: string): Topic | null {
  switch (topic) {
    case TOPICS.CLUSTER:
      return { kind: 'cluster' };
    case TOPICS.CHANNELS:
      return { kind: 'channels' };
    case TOPICS.ALERTS:
      return { kind: 'alerts' };
    case TOPICS.EVENTS:
      return { kind: 'events' };
    case TOPICS.NODES:
      return { kind: 'nodes' };
  }
  if (!topic.startsWith('channel.')) return null;
  const rest = topic.slice('channel.'.length);
  if (rest.endsWith('.consumers')) return { kind: 'consumers', name: rest.slice(0, -'.consumers'.length) };
  if (rest.endsWith('.tail')) return { kind: 'tail', name: rest.slice(0, -'.tail'.length) };
  return { kind: 'channel', name: rest };
}

export type Health = 'healthy' | 'warning' | 'critical';

export interface LatencySummary {
  p50: number;
  p95: number;
  p99: number;
  p999: number;
  max: number;
}

export interface ClusterOverview {
  cluster_name: string;
  health: Health;
  version: string;
  uptime_secs: number;
  nodes_total: number;
  /** Nodes whose cluster status is up/leaving/joining (reachable, whatever their health).
   *  "N/M nodes up" uses this; the colour comes from `health`. */
  nodes_up: number;
  /** Nodes that are up and not `critical` health. */
  nodes_healthy: number;
  channels: number;
  consumer_groups: number;
  consumers: number;
  ingress_rate: number;
  /** deliveries/s summed over all groups (incl. redeliveries) */
  egress_rate: number;
  /** acks/s summed over all groups ("consumed") */
  ack_rate: number;
  ingress_bytes_rate: number;
  egress_bytes_rate: number;
  publish_latency: LatencySummary;
  delivery_latency: LatencySummary;
  total_lag: number;
  retries_rate: number;
  retries_24h: number;
  dlq_total: number;
  storage_bytes: number;
  storage_capacity_bytes: number;
  messages_total: number;
  open_alerts: number;
  /** Clustered only: some peers' stats are missing/stale, figures cover reachable nodes only. */
  partial?: boolean;
}

export interface ChannelSummary {
  name: string;
  mode: ChannelMode;
  health: Health;
  partitions: number;
  ingress_rate: number;
  /** deliveries/s summed over all groups (incl. redeliveries) */
  egress_rate: number;
  /** acks/s summed over all groups ("consumed") */
  ack_rate: number;
  lag: number;
  publish_p99_ms: number;
  consumer_groups: number;
  consumers: number;
  storage_bytes: number;
  messages: number;
  dlq: number;
}

export type ChannelsSummary = Record<string, ChannelSummary>;

export interface PartitionInfo {
  partition: number;
  low_offset: number;
  high_offset: number;
  storage_bytes: number;
  segments: number;
  ingress_rate: number;
}

export interface ChannelDetail {
  summary: ChannelSummary;
  config: ChannelConfig;
  publish_latency: LatencySummary;
  delivery_latency: LatencySummary;
  retries_rate: number;
  dlq_rate: number;
  partitions: PartitionInfo[];
  oldest_timestamp_ms: number | null;
}

export interface InstanceState {
  id: string;
  client_name: string;
  remote_addr: string;
  connected_at_ms: number;
  health: Health;
  egress_rate: number;
  ack_rate: number;
  nack_rate: number;
  in_flight: number;
  prefetch: number;
  lag: number;
  assigned_partitions: number[];
  processing_p99_ms: number;
  retries_total: number;
  last_error: string | null;
  last_error_at_ms: number | null;
  cpu_percent: number | null;
  memory_bytes: number | null;
}

export interface GroupState {
  group: string;
  health: Health;
  lag: number;
  egress_rate: number;
  ack_rate: number;
  nack_rate: number;
  in_flight: number;
  retry_pending: number;
  dlq: number;
  processing_latency: LatencySummary;
  instances: Record<string, InstanceState>;
}

export type ConsumersState = Record<string, GroupState>;

export interface TailMessage {
  partition: number;
  offset: number;
  key: string | null;
  timestamp_ms: number;
  headers: Record<string, string>;
  size: number;
  payload_preview: string;
  truncated: boolean;
}

export interface TailState {
  sample_ratio: number;
  shown_rate: number;
  total_rate: number;
  /** Newest last. Snapshot = recent buffer; deltas replace the array with the new batch. */
  messages: TailMessage[];
}

export type AlertKind =
  | 'lag_growth'
  | 'consumer_stalled'
  | 'no_consumers'
  | 'dlq_growth'
  | 'high_retry_rate'
  | 'slow_instance'
  | 'storage_nearly_full'
  | 'high_publish_latency'
  | 'node_down'
  | 'under_replicated_partitions'
  | 'offline_partitions'
  /** v0.4: a served certificate expires within 14 days (critical within 3 / expired). Id `tls_cert_expiring:{node}/{surface}`. */
  | 'tls_cert_expiring'
  /** v0.4: the last certificate reload failed (previous one still served). Id `tls_reload_failed:{node}/{surface}`. */
  | 'tls_reload_failed';

export interface Alert {
  id: string;
  severity: Health;
  kind: AlertKind;
  title: string;
  detail: string;
  possible_reason: string | null;
  link: string | null;
  channel: string | null;
  group: string | null;
  since_ms: number;
  /**
   * v0.9.1: structured values the console localises from (`lib/alertText.ts`); absent on older
   * brokers (then the console parses the English detail, falling back to the server text).
   * Keys per kind: lag_growth {variant: spike|growing|high, from, to, over_ms, lag, mode};
   * no_consumers {lag}; consumer_stalled {lag, for_ms, instances}; high_retry_rate {ratio, nacked, total};
   * dlq_growth {added, over_ms}; slow_instance {instance, rate?, median_rate?, p99_ms?, group_p99_ms?};
   * storage_nearly_full {node, used, total}; high_publish_latency {node, fsync_p99_ms} | {p99_ms, rate};
   * node_down {node, since_secs?, error?}; under_replicated_partitions {count, replication_factor};
   * offline_partitions {count}; tls_* {node, surface, days_left?, expired?}.
   */
  params?: Record<string, number | string | boolean>;
}

/** open alerts only; resolved ones are deleted (merge-patch `null`). */
export type AlertsState = Record<string, Alert>;

export type EventKind =
  | 'channel_created'
  | 'channel_deleted'
  | 'channel_updated'
  | 'consumer_connected'
  | 'consumer_disconnected'
  | 'alert_opened'
  | 'alert_resolved'
  | 'dead_lettered'
  | 'dlq_retried'
  | 'dlq_discarded'
  | 'messages_replayed'
  | 'controller_changed'
  | 'node_down'
  | 'node_up'
  /** v0.4 rebalancing; subject = trigger (`node n4 joined`, `manual`, …). */
  | 'rebalance_started'
  | 'rebalance_completed'
  | 'rebalance_failed'
  /** subject = `channel/partition`. */
  | 'partition_moved'
  /** subject = down node id whose replicas are being replaced. */
  | 'replica_repair_started'
  /** subject = `client|http|cluster`. */
  | 'tls_reload_failed'
  /** v0.5 decommission lifecycle; subject = node id (failed: the title carries the error). */
  | 'node_decommission_started'
  | 'node_decommissioned'
  | 'node_decommission_failed'
  /** A `down` node with no replicas was forgotten; subject = node id. */
  | 'node_forgotten';

export interface FluxEvent {
  id: number;
  ts: number;
  kind: EventKind;
  title: string;
  subject: string;
  severity: Health;
  /**
   * v0.9.1: dead_lettered / messages_replayed {channel, count}; dlq_retried / dlq_discarded {count};
   * alert_opened / alert_resolved {alert_kind, alert_id, channel?, group?, instance?}.
   */
  params?: Record<string, number | string | boolean>;
}

export interface EventsFeed {
  /** newest first, at most 100 */
  events: FluxEvent[];
}

export interface NodeState {
  id: string;
  address: string;
  health: Health;
  version: string;
  uptime_secs: number;
  cpu_percent: number;
  memory_bytes: number;
  memory_total_bytes: number;
  disk_used_bytes: number;
  disk_total_bytes: number;
  disk_latency_p99_ms: number;
  connections: number;
  /** Membership status (`up` on a single node; unreachable peers are kept as `down`). v0.2 */
  status?: NodeStatus;
  /** Raft leader / controller (`true` on a single node). v0.2 */
  is_controller?: boolean;
}

/** `nodes`: one entry on a single node; every cluster member when clustered. */
export type NodesState = Record<string, NodeState>;

export const METRICS = [
  'ingress_rate',
  'egress_rate',
  'ack_rate',
  'publish_p50',
  'publish_p95',
  'publish_p99',
  'lag',
  'retries_rate',
  'dlq_rate',
  'consumers',
  'storage_bytes',
] as const;
export type MetricName = (typeof METRICS)[number];

export type HistoryRange = '5m' | '15m' | '1h' | '24h' | '7d';

export const HISTORY_RANGE_MS: Record<HistoryRange, number> = {
  '5m': 5 * 60_000,
  '15m': 15 * 60_000,
  '1h': 3_600_000,
  '24h': 86_400_000,
  '7d': 7 * 86_400_000,
};

export const HISTORY_BUCKET_MS: Record<HistoryRange, number> = {
  '5m': 1_000,
  '15m': 1_000,
  '1h': 5_000,
  '24h': 60_000,
  '7d': 30 * 60_000,
};

export interface HistorySeries {
  metric: string;
  range: HistoryRange;
  bucket_ms: number;
  /** `[timestamp_ms, value]`, oldest first; missing buckets omitted. */
  points: [number, number][];
}

export const HEALTH_RANK: Record<Health, number> = { healthy: 0, warning: 1, critical: 2 };

// ───────────────────────────── auth.rs (v0.2) ─────────────────────────────

/** Cumulative: viewer ⊂ operator ⊂ admin. `subscriber` (v0.10) is below viewer: live
 *  subscriptions (WebSocket/SSE) on its `scopes.subscribe` channels only, no console access. */
export type Role = 'subscriber' | 'viewer' | 'operator' | 'admin';
export const ROLES: readonly Role[] = ['subscriber', 'viewer', 'operator', 'admin'];

export interface ClientScopes {
  /** Channel globs this token may publish to. */
  publish: string[];
  /** Channel globs this token may consume from. */
  consume: string[];
  /** v0.10: channel globs this token may open live subscriptions (WS/SSE) on. Omitted by the
   *  server when empty. */
  subscribe?: string[];
}

/** Stored token metadata (never contains the secret). */
export interface TokenInfo {
  id: string;
  name: string;
  role: Role;
  scopes: ClientScopes;
  created_at_ms: number;
  created_by: string;
  expires_at_ms: number | null;
  last_used_ms: number | null;
}

/** `POST /api/auth/tokens` (admin). */
export interface CreateTokenRequest {
  name: string;
  role: Role;
  scopes: ClientScopes;
  /** null = no expiry */
  ttl_secs: number | null;
}

/** The secret is shown exactly once. Format: `fmq_<id>_<secret>`. */
export interface CreateTokenResponse {
  token: string;
  info: TokenInfo;
  /** True when this token switched the broker from open to enforced mode (first token): the
   *  caller must adopt it, since anonymous access stopped working. */
  adopt: boolean;
}

export type AuthMode = 'open' | 'enforced';

/** `GET /api/auth/me`. */
export interface WhoAmI {
  mode: AuthMode;
  /** Token name, `"admin"` for the bootstrap token, `"anonymous"` in open mode. */
  name: string;
  token_id: string | null;
  role: Role;
  scopes: ClientScopes;
}

// ───────────────────────────── auth.rs (v0.5): revocation, certificate bindings ─────────────────────────────
//
// Revoking (or expiry of) a token closes every client connection (`Goodbye { reason: "token
// revoked" | "token expired" }`) and WS session (close code `WS_CLOSE_TOKEN_REVOKED`) using it within
// 1 s, on every node. Certificate bindings map a verified client-port mTLS certificate subject CN to
// a principal (actor `cert:<cn>`); without client-port mTLS (`--tls-client-ca`) they are inert.
// REST (admin): `GET/POST /api/auth/cert-bindings`, `DELETE /api/auth/cert-bindings/{cn}`.

/** WebSocket close code when the session's token was revoked or expired. Not a named proto constant:
 *  the number is stated in the auth.rs v0.5 doc and docs/design/v0.5.md §2. */
export const WS_CLOSE_TOKEN_REVOKED = 4401;

export interface CertBinding {
  /** Exact certificate subject CN. */
  cn: string;
  role: Role;
  /** `#[serde(default)]` on the Rust side; always serialized. */
  scopes: ClientScopes;
  created_at_ms: number;
  created_by: string;
  last_used_ms: number | null;
}

/** `POST /api/auth/cert-bindings` (admin). */
export interface CreateCertBindingRequest {
  cn: string;
  role: Role;
  scopes: ClientScopes;
}

/** `GET /api/auth/cert-bindings` (admin). */
export interface CertBindingList {
  bindings: CertBinding[];
  /** False when the client port has no mTLS (`--tls-client-ca`): bindings are inert. */
  client_mtls_enabled: boolean;
}

// ───────────────────────────── routing.rs (v0.2) ─────────────────────────────

/** Adjacently tagged: `{"kind":"key"}`, `{"kind":"partition"}`, `{"kind":"header","name":"x"}`,
 *  `{"kind":"payload","name":"order.items.0.sku"}`. */
export type FieldRef = { kind: 'key' } | { kind: 'partition' } | { kind: 'header'; name: string } | { kind: 'payload'; name: string };

export type Op = 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'in' | 'not_in' | 'prefix' | 'contains' | 'exists' | 'not_exists';
export const OPS: readonly Op[] = ['eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'in', 'not_in', 'prefix', 'contains', 'exists', 'not_exists'];

export type JsonValue = null | boolean | number | string | JsonValue[] | { [k: string]: JsonValue };

/** One predicate. `value` is required for all ops except `exists`/`not_exists`. */
export interface Condition {
  field: FieldRef;
  op: Op;
  value?: JsonValue | null;
}

export interface Route {
  id: string;
  name: string;
  source: string;
  enabled: boolean;
  /** All must match (AND). Empty = match everything. */
  conditions: Condition[];
  targets: string[];
  created_at_ms: number;
  updated_at_ms: number;
}

/** `POST /api/routes` (admin). */
export interface CreateRouteRequest {
  name: string;
  source: string;
  /** default true */
  enabled?: boolean;
  conditions: Condition[];
  targets: string[];
}

/** `PATCH /api/routes/{id}` (admin). Source is immutable. */
export interface UpdateRouteRequest {
  name?: string | null;
  enabled?: boolean | null;
  conditions?: Condition[] | null;
  targets?: string[] | null;
}

/** `POST /api/routes/test` — backtest against stored messages. */
export interface RouteTestRequest {
  source: string;
  conditions: Condition[];
  /** default 24 h */
  window_ms: number | null;
  /** default 20, max 100 */
  sample_limit: number | null;
}

export interface RouteTestResponse {
  scanned: number;
  matched: number;
  /** True when the scan cap (200k messages) was hit before covering the window. */
  truncated: boolean;
  samples: MessageView[];
}

/** `GET /api/routes/stats` — per route counters since broker start (node-local in a cluster).
 *  `evaluated`/`matched` count messages; `forwarded`/`failed` count **per target** — a message
 *  matching a route with 3 targets adds 1 to `matched` and 3 to `forwarded`. */
export interface RouteStats {
  id: string;
  evaluated: number;
  matched: number;
  forwarded: number;
  failed: number;
  last_matched_ms: number | null;
}

/** `POST /api/filter/parse`. */
export interface ParseFilterRequest {
  filter: string;
}

export interface ParseFilterResponse {
  conditions: Condition[];
  /** Canonical DSL text. */
  rendered: string;
}

/** `POST /api/filter/render` (viewer) — conditions → canonical DSL text (builder → text).
 *  Invalid conditions (e.g. a missing value) are a 400. */
export interface RenderFilterRequest {
  conditions: Condition[];
}

export interface RenderFilterResponse {
  rendered: string;
}

/** Router system consumer group; groups starting with `$` are reserved. */
export const ROUTER_GROUP = '$router';
/** Headers stamped on routed copies. */
export const ROUTE_HEADERS = {
  ROUTED_FROM: 'flux-routed-from',
  ROUTE_ID: 'flux-route-id',
  ROUTE_PATH: 'flux-route-path',
} as const;

// ───────────────────────────── cluster.rs (v0.2) ─────────────────────────────

export type NodeStatus =
  | 'up'
  | 'down'
  | 'joining'
  | 'leaving'
  /** v0.5: being removed permanently — every replica moves away, then it leaves Raft membership
   *  and the registry. Never a rebalance target. */
  | 'decommissioning';

export interface NodeInfo {
  id: string;
  /** Binary protocol address. */
  client_addr: string;
  /** REST / WS / UI address. */
  http_addr: string;
  status: NodeStatus;
  /** Currently the Raft leader / controller. */
  is_controller: boolean;
  /** Raft voter (false = learner). */
  is_voter: boolean;
  version: string;
}

export interface PartitionAssignment {
  partition: number;
  leader: string | null;
  /** All replicas incl. the leader, preferred leader first. */
  replicas: string[];
  isr: string[];
  epoch: number;
}

/** Wire `Metadata` and `GET /api/cluster/metadata`. */
export interface ClusterMetadata {
  cluster_name: string;
  epoch: number;
  controller: string | null;
  nodes: NodeInfo[];
  /** channel → assignment per partition (index = partition). */
  channels: Record<string, PartitionAssignment[]>;
}

export interface RaftStatus {
  term: number;
  leader: string | null;
  last_log_index: number;
  committed_index: number;
  voters: string[];
  learners: string[];
}

export interface PartitionReplicationStatus {
  channel: string;
  partition: number;
  leader: string | null;
  epoch: number;
  high_watermark: number;
  log_end_offset: number;
  /** follower node → messages behind the leader */
  follower_lag: Record<string, number>;
  isr: string[];
}

/** `GET /api/cluster/status`. */
export interface ClusterStatus {
  metadata: ClusterMetadata;
  replication_factor: number;
  under_replicated_partitions: number;
  offline_partitions: number;
  raft: RaftStatus;
  partitions: PartitionReplicationStatus[];
}

/** `POST /api/cluster/partitions/reassign` (admin). */
export interface ReassignRequest {
  channel: string;
  partition: number;
  replicas: string[];
}

/** `POST /api/cluster/nodes/{id}/drain`. */
export interface DrainResponse {
  moved_leaderships: number;
}

// ───────────────────────────── cluster.rs — automatic rebalancing (v0.4) ─────────────────────────────

/** `off`: no automatic moves (manual POST still works; preferred-leader election stays on).
 *  `repair`: restore RF only. `full`: repair + balance replicas and leaders. */
export type RebalanceMode = 'off' | 'repair' | 'full';
export const REBALANCE_MODES: readonly RebalanceMode[] = ['off', 'repair', 'full'];

/** `PUT /api/cluster/rebalance/config` (admin); part of `RebalanceStatus`. */
export interface RebalanceConfig {
  mode: RebalanceMode;
  /** Default 2. */
  max_concurrent_moves: number;
  /** Max allowed `max - min` replicas per live node before balancing. Default 1. */
  imbalance_threshold: number;
  /** Replace replicas on a node down for this long. Default 600; 0 = never. */
  replace_after_secs: number;
  /** Catch-up throttle per moving replica. Default 0 (unlimited). */
  throttle_bytes_per_sec: number;
  /** Default 1800. */
  move_timeout_secs: number;
}

/** `POST /api/cluster/rebalance` (admin). */
export interface RebalanceRequest {
  /** Compute and return the plan only. */
  dry_run?: boolean;
  /** Limit to these channels (empty = all). */
  channels?: string[];
}

/** `cancelling`: cancel requested; waiting for in-flight moves. */
export type RebalanceState = 'idle' | 'running' | 'paused' | 'cancelling';

/** `repair`: restore RF · `balance`: even out replicas / leaders · `manual`: operator POST. */
export type MoveReason = 'repair' | 'balance' | 'manual';

/** `catching_up`: new replicas added, catching up · `finalizing`: final list being committed /
 *  leader moving · `rolled_back`: timed out or failed, old replica list restored ·
 *  `cancelled`: dropped by cancel before it started (never touched the assignment). */
export type MoveState = 'pending' | 'catching_up' | 'finalizing' | 'done' | 'rolled_back' | 'cancelled';

export interface PartitionMove {
  channel: string;
  partition: number;
  reason: MoveReason;
  from_replicas: string[];
  /** Target order, preferred leader first. */
  to_replicas: string[];
  state: MoveState;
  /** Messages the slowest new replica is behind the leader (`catching_up`). */
  lag: number | null;
  /** `lag` when catch-up started — progress = 1 − lag / initial_lag. */
  initial_lag: number | null;
  started_at_ms: number | null;
  finished_at_ms: number | null;
  error: string | null;
}

/** Per-live-node load (`NodeLoad` in cluster.rs; renamed here — `lib/cluster` has its own `NodeLoad`). */
export interface RebalanceNodeLoad {
  node: string;
  /** Every registered node is listed; `down` / `leaving` nodes still show the replicas assigned
   *  to them (repair moves them away). Imbalance counts only `up` nodes. */
  status: NodeStatus;
  replicas: number;
  leaders: number;
  /** Preferred leaderships (replica 0) — where leaders settle after elections. */
  preferred_leaders: number;
  storage_bytes: number;
}

/** `GET /api/cluster/rebalance` (viewer); also the answer of `POST /api/cluster/rebalance`. */
export interface RebalanceStatus {
  config: RebalanceConfig;
  state: RebalanceState;
  /** Current load of every registered node (see `RebalanceNodeLoad.status`). */
  load: RebalanceNodeLoad[];
  /** `max - min` replicas per `up` node. */
  imbalance: number;
  /** Partitions with fewer live replicas than their RF. */
  under_replicated: number;
  /** Current (or last) plan; for a dry run, the proposed plan. */
  moves: PartitionMove[];
  /** Why the current plan exists (`node n4 joined`, `node n2 down 600s`, `manual`). */
  trigger: string | null;
  started_at_ms: number | null;
  finished_at_ms: number | null;
  /** Load expected after the plan completes. */
  projected_load: RebalanceNodeLoad[];
}

// ───────────────────────────── cluster.rs — node decommission (v0.5) ─────────────────────────────
//
// `POST /api/cluster/nodes/{id}/decommission` (admin) → `DecommissionStatus`: marks the node
// `decommissioning`, plans `repair` moves for all its replicas (not enough other `up` nodes for a
// channel's RF → 409 `conflict` with the reason; a running rebalance plan → 409), then removes it from
// Raft membership and the registry. `GET …/decommission` (viewer) → progress.
// `DELETE /api/cluster/nodes/{id}` (admin) forgets a node that is `down` and holds no replicas (409
// otherwise). Removing below 1 voter or below a Raft quorum of the remaining voters → 409.
// Status stays readable for 24 h after it ends; `GET /api/cluster/decommissions` (viewer) lists all
// current and recent ones. Cancel = `POST /api/cluster/rebalance/cancel` while `moving_replicas`:
// in-flight moves finish, the decommission ends `failed` ("cancelled"). Any `failed` decommission
// returns the node to `up`. Optional (may be absent → 400/404): `POST …/decommission?dry_run=true`
// → `RebalanceStatus` with the planned moves.

export type DecommissionState =
  /** Moving replicas away (see the rebalance plan). */
  | 'moving_replicas'
  /** Replicas gone; leaving Raft membership. */
  | 'leaving_membership'
  /** Removed from the cluster; safe to stop the process. */
  | 'removed'
  | 'failed';

export const DECOMMISSION_STATES: readonly DecommissionState[] = ['moving_replicas', 'leaving_membership', 'removed', 'failed'];

export interface DecommissionStatus {
  node: string;
  state: DecommissionState;
  /** Replicas still on the node. */
  replicas_remaining: number;
  /** Replicas on the node when the decommission started (progress baseline). */
  initial_replicas: number;
  started_at_ms: number;
  finished_at_ms: number | null;
  error: string | null;
}

// ───────────────────────────── tls.rs (v0.4) ─────────────────────────────

export type TlsSurface = 'client' | 'http' | 'cluster';
export const TLS_SURFACES: readonly TlsSurface[] = ['client', 'http', 'cluster'];

/** `GET /api/tls` (viewer) — what is enabled and when certificates expire. */
export interface TlsStatus {
  /** The node that answered — TLS state is node-local (each node has its own certificates). */
  node_id: string;
  client: TlsEndpointStatus;
  http: TlsEndpointStatus;
  cluster: TlsEndpointStatus;
}

export interface TlsEndpointStatus {
  enabled: boolean;
  /** Client certificates required (always true for `cluster` when enabled). */
  mutual: boolean;
  /** Leaf certificate currently served; null when disabled. */
  certificate: CertificateInfo | null;
  /** Last successful (re)load, ms since epoch. */
  loaded_at_ms: number | null;
  /** Last reload failure (short error), cleared on the next success. */
  last_error: string | null;
  /** Failed handshakes since start (bad cert, plaintext on a TLS port, protocol mismatch). */
  handshake_failures_total: number;
}

export interface CertificateInfo {
  subject: string;
  issuer: string;
  /** DNS / IP subject alternative names. */
  sans: string[];
  not_before_ms: number;
  not_after_ms: number;
  /** SHA-256 of the DER leaf, lowercase hex. */
  sha256_fingerprint: string;
}

/** Days before `not_after` at which the `tls_cert_expiring` alert fires (warning; critical at 3 days / expired). */
export const CERT_EXPIRY_WARNING_DAYS = 14;
/** Not a proto constant: the critical threshold stated in the tls.rs doc and docs/design/v0.4.md §1. */
export const CERT_EXPIRY_CRITICAL_DAYS = 3;

// ───────────────────────────── notify.rs (v0.3) ─────────────────────────────

/** Returned instead of secrets. Uniform update rule for every secret field (notifiers, webhooks,
 *  backup target): `REDACTED` keeps the stored value, `""` removes it, anything else replaces.
 *  Every custom header value is a secret. */
export const REDACTED = '********';

/** Internally tagged (`{"type":"telegram", ...}`). */
export type NotifierKind =
  | { type: 'telegram'; bot_token: string; chat_id: string; /** forum topic */ thread_id: number | null }
  | { type: 'slack'; webhook_url: string }
  | { type: 'discord'; webhook_url: string }
  | {
      type: 'email';
      smtp_host: string;
      smtp_port: number;
      username: string | null;
      password: string | null;
      /** "starttls" | "tls" | "none" */
      tls: string;
      from: string;
      to: string[];
    }
  | { type: 'pagerduty'; routing_key: string }
  | { type: 'webhook'; url: string; secret: string | null; headers: Record<string, string> };

export type NotifierType = NotifierKind['type'];
export const NOTIFIER_TYPES: readonly NotifierType[] = ['telegram', 'slack', 'discord', 'email', 'pagerduty', 'webhook'];

export interface Notifier {
  id: string;
  name: string;
  enabled: boolean;
  kind: NotifierKind;
  created_at_ms: number;
  updated_at_ms: number;
}

/** `POST /api/notify/notifiers` and `POST /api/notify/notifiers/test` (admin). */
export interface CreateNotifierRequest {
  name: string;
  /** default true */
  enabled?: boolean;
  kind: NotifierKind;
}

/** `PATCH /api/notify/notifiers/{id}` (admin). Secret fields may be `REDACTED` (keep). */
export interface UpdateNotifierRequest {
  name?: string | null;
  enabled?: boolean | null;
  kind?: NotifierKind | null;
}

/** What a rule matches. Empty lists = any. */
export interface RuleMatch {
  /** Alerts with severity ≥ this (`warning` or `critical`). Alerts only. */
  min_severity: Health;
  alert_kinds: AlertKind[];
  /** Broker events to forward (none by default — events are chatty). */
  event_kinds: EventKind[];
  /** Channel globs. Alerts/events without a channel match only when empty. */
  channels: string[];
}

export interface NotificationRule {
  id: string;
  name: string;
  enabled: boolean;
  notifier_ids: string[];
  match: RuleMatch;
  /** default 30 */
  group_wait_secs: number;
  /** default 14400; 0 = never */
  repeat_interval_secs: number;
  send_resolved: boolean;
  created_at_ms: number;
  updated_at_ms: number;
}

export interface CreateRuleRequest {
  name: string;
  enabled?: boolean;
  notifier_ids: string[];
  match: RuleMatch;
  group_wait_secs: number | null;
  repeat_interval_secs: number | null;
  send_resolved: boolean | null;
}

export interface UpdateRuleRequest {
  name?: string | null;
  enabled?: boolean | null;
  notifier_ids?: string[] | null;
  match?: RuleMatch | null;
  group_wait_secs?: number | null;
  repeat_interval_secs?: number | null;
  send_resolved?: boolean | null;
}

/** Body of generic webhook notifications. */
export interface NotificationPayload {
  id: string;
  cluster: string;
  sent_at_ms: number;
  rule: string;
  /** "firing" | "resolved" | "event" | "test" */
  status: string;
  alerts: Alert[];
  events: FluxEvent[];
  console_url: string | null;
}

export type DeliveryStatus = 'sent' | 'failed' | 'suppressed';

/** `GET /api/notify/history` (newest first, last 1000). */
export interface NotificationRecord {
  id: string;
  ts: number;
  rule_id: string | null;
  notifier_id: string;
  status: DeliveryStatus;
  summary: string;
  attempts: number;
  /** Short error (`connection refused`, `timeout`, `HTTP 503`, `TLS error`, `DNS error`, …). */
  error: string | null;
  /** Full cause chain / response snippet behind `error` (v0.3, additive). */
  error_detail?: string | null;
}

/** Notifier test (`/test`) and backup target test (`/api/backups/test-target`). */
export interface TestResult {
  ok: boolean;
  latency_ms: number;
  /** Short error (see NotificationRecord.error). */
  error: string | null;
  /** Full cause chain behind `error` (additive). */
  error_detail?: string | null;
}

/** System channel with every alert transition and broker event (pull feed). */
export const SYSTEM_EVENTS_CHANNEL = '$system.events';

/** One pull-feed item (also the JSON payload stored in `$system.events`). */
export interface FeedItem {
  /** Offset in the system channel, decimal string; pass as `after` to continue. */
  cursor: string;
  ts: number;
  /** "alert_opened" | "alert_resolved" | "event" */
  kind: string;
  alert: Alert | null;
  event: FluxEvent | null;
  node: string;
}

/** `GET /api/events/feed?after=&limit=&wait_ms=` or `?tail=N` (newest N, ≤ 1000; exclusive with
 *  `after`). Feed responses always carry the real `cursor`; the payload stored in the channel has
 *  `cursor: ""` (SDK consumers use the message offset). */
export interface FeedPage {
  items: FeedItem[];
  next_cursor: string | null;
  /** Older items were already removed by retention (a gap since `after`). */
  gap: boolean;
}

/** System channels (`$…`) are read-only in the console and can't be deleted. */
export const isSystemChannel = (name: string) => name.startsWith('$');

// ───────────────────────────── webhooks.rs (v0.3) ─────────────────────────────

export type PayloadFormat = 'envelope' | 'raw';

/** Webhook subscriptions consume their channel as this system group. */
export const WEBHOOK_GROUP_PREFIX = '$webhook:';
export const webhookGroup = (name: string) => `${WEBHOOK_GROUP_PREFIX}${name}`;

/** Validation ranges (webhooks.rs module docs). */
export const WEBHOOK_LIMITS = {
  name: /^[a-z0-9._-]{1,64}$/,
  timeout_ms: [100, 60_000],
  max_in_flight: [1, 64],
  batch_max: [1, 500],
  batch_linger_ms: [0, 10_000],
  /** 0 = circuit breaker off. */
  pause_after_failures: [0, 1_000],
} as const;

export interface WebhookSubscription {
  id: string;
  /** Unique; immutable. The consumer group is `$webhook:<name>`. */
  name: string;
  /** Immutable. */
  channel: string;
  url: string;
  enabled: boolean;
  /** Values read back as `REDACTED`. */
  headers: Record<string, string>;
  /** Read back as `REDACTED`. */
  secret: string | null;
  filter: Condition[];
  format: PayloadFormat;
  timeout_ms: number;
  max_in_flight: number;
  batch_max: number;
  batch_linger_ms: number;
  retry_on_4xx: boolean;
  /** Circuit breaker: pause after this many consecutive retryable failures (0 = off). Absent before v0.11. */
  pause_after_failures?: number;
  /** Where the new group started (immutable). */
  start: StartFrom;
  created_at_ms: number;
  updated_at_ms: number;
}

/** `POST /api/webhooks` and `POST /api/webhooks/test` (admin). Defaults: timeout 10000,
 *  in-flight 4, batch 1, linger 0, retry_on_4xx false, start latest. */
export interface CreateWebhookRequest {
  name: string;
  channel: string;
  url: string;
  enabled?: boolean;
  headers?: Record<string, string>;
  secret: string | null;
  filter?: Condition[];
  format?: PayloadFormat;
  timeout_ms: number | null;
  max_in_flight: number | null;
  batch_max: number | null;
  batch_linger_ms: number | null;
  retry_on_4xx: boolean | null;
  /** Default 0 (off). */
  pause_after_failures?: number | null;
  start: StartFrom | null;
}

/** `PATCH /api/webhooks/{id}` (admin). `name`/`channel`/`start` are immutable. */
export interface UpdateWebhookRequest {
  url?: string | null;
  enabled?: boolean | null;
  headers?: Record<string, string> | null;
  /** `REDACTED` keeps, `""` removes. */
  secret?: string | null;
  filter?: Condition[] | null;
  format?: PayloadFormat | null;
  timeout_ms?: number | null;
  max_in_flight?: number | null;
  batch_max?: number | null;
  batch_linger_ms?: number | null;
  retry_on_4xx?: boolean | null;
  pause_after_failures?: number | null;
}

/** JSON body per message (`format = envelope`). */
export interface WebhookEnvelope {
  /** `{channel}/{partition}/{offset}` — stable across retries (dedupe on it). */
  delivery_id: string;
  subscription: string;
  channel: string;
  partition: number;
  offset: number;
  key: string | null;
  headers: Record<string, string>;
  timestamp_ms: number;
  attempt: number;
  payload: JsonPayload;
}

/** `GET /api/webhooks/stats` (cluster-wide). */
export interface WebhookStats {
  id: string;
  delivered: number;
  failed: number;
  skipped_by_filter: number;
  in_flight: number;
  last_status: number | null;
  /** Short error of the last failed delivery (`HTTP 503`, `timeout`, `blocked by policy`, …). */
  last_error: string | null;
  /** Full cause / response snippet behind `last_error` (additive). */
  last_error_detail?: string | null;
  last_delivery_ms: number | null;
  latency_p50_ms: number;
  latency_p99_ms: number;
  /** Set while the circuit breaker holds delivery on at least one node (v0.11). */
  breaker?: WebhookBreaker | null;
}

/** An open circuit breaker (merged over nodes: earliest times). */
export interface WebhookBreaker {
  since_ms: number;
  /** Consecutive failed requests that opened it, plus failed probes since. */
  failures: number;
  next_probe_ms: number;
  /** Nodes where it is open. */
  nodes: number;
}

export type WebhookAttemptOutcome = 'delivered' | 'retry' | 'dead' | 'probe_ok' | 'probe_failed';

/** `GET /api/webhooks/{id}/attempts`: last requests, newest first (up to 100; node memory). */
export interface WebhookAttempt {
  at_ms: number;
  node: string;
  /** First message's `{channel}/{partition}/{offset}`; `probe/<ts>` for breaker probes. */
  delivery_id: string;
  /** Messages in the request (0 for probes). */
  messages: number;
  /** Attempt of the first message (0 for probes). */
  attempt: number;
  outcome: WebhookAttemptOutcome;
  status: number | null;
  latency_ms: number;
  error: string | null;
  error_detail: string | null;
}

export type WebhookHistoryRange = '15m' | '1h' | '24h';

/** `GET /api/webhooks/{id}/history?range=…`: per bucket, summed over nodes. */
export interface WebhookHistory {
  bucket_ms: number;
  points: { ts_ms: number; delivered: number; failed: number; latency_avg_ms: number; latency_max_ms: number }[];
}

/** `POST /api/webhooks/{id}/test` and `POST /api/webhooks/test`. */
export interface WebhookTestResult {
  ok: boolean;
  status: number | null;
  latency_ms: number;
  response_snippet: string | null;
  /** Short error (same classes as WebhookStats.last_error). */
  error: string | null;
  /** Full cause chain behind `error` (additive). */
  error_detail?: string | null;
}

// ───────────────────────────── backup.rs (v0.3) ─────────────────────────────

export type BackupTarget =
  | {
      type: 's3';
      /** Custom endpoint for MinIO/R2/B2; null = AWS. */
      endpoint: string | null;
      region: string;
      bucket: string;
      prefix: string;
      access_key_id: string;
      /** Secret (REDACTED on read). */
      secret_access_key: string;
      /** Path-style addressing (MinIO). */
      path_style: boolean;
    }
  | { type: 'filesystem'; path: string };

/** `GET /api/backups/config` (null when unset) / `PUT /api/backups/config` (admin). */
export interface BackupConfig {
  enabled: boolean;
  target: BackupTarget;
  /** ≥ 300, default 3600 */
  interval_secs: number;
  /** ≥ 1, default 24 */
  keep_last: number;
  /** 0 = no age limit, default 7 */
  keep_days: number;
  /** false = metadata/offsets/DLQ only */
  include_messages: boolean;
  /** base64 32-byte AES-256-GCM key (REDACTED on read). */
  encryption_key: string | null;
}

export type BackupStatus = 'running' | 'succeeded' | 'failed';

export interface BackupInfo {
  /** Sortable: `<yyyymmddThhmmssZ>-<short random>`. */
  id: string;
  status: BackupStatus;
  started_at_ms: number;
  finished_at_ms: number | null;
  /** "scheduled" | "manual" | "cli" */
  trigger: string;
  channels: number;
  partitions: number;
  messages: number;
  bytes_uploaded: number;
  segments_uploaded: number;
  /** Segments reused from earlier backups (incremental savings). */
  segments_reused: number;
  total_bytes: number;
  /** While running: bytes processed so far. Progress = progress_bytes / total_bytes. */
  progress_bytes: number;
  error: string | null;
}

/** `GET /api/backups/status`. */
export interface BackupOverview {
  configured: boolean;
  enabled: boolean;
  running: BackupInfo | null;
  last_success: BackupInfo | null;
  last_failure: BackupInfo | null;
  next_run_ms: number | null;
}

/** `GET /api/backups/{id}` (`latest` = newest) — summary of the backup manifest (backup.rs `BackupManifest`). */
export interface BackupManifest {
  info: BackupInfo;
  /** Node that wrote the manifest (the controller in a cluster). */
  node_id: string;
  cluster_name: string;
  fluxmq_version: string;
  /** false = metadata, offsets and DLQ only. */
  include_messages: boolean;
  /** Fingerprint of the encryption key; null = not encrypted. */
  encryption: string | null;
  /** Nodes that uploaded partition data (cluster: the leaders at backup time). */
  nodes: string[];
  /** Per channel, ordered by name. */
  channels: BackupChannelSummary[];
}

export interface BackupChannelSummary {
  name: string;
  mode: ChannelMode;
  partitions: number;
  /** Messages held up to the cut (0 for metadata-only backups). */
  messages: number;
  /** Logical bytes of the channel's segments (incl. reused ones). */
  bytes: number;
  segments: number;
  /** Consumer groups whose committed offsets are included. */
  groups: number;
  dlq_entries: number;
}

// ───────────────────────────── live.rs (v0.11): live subscription gateway status ─────────────────────────────
//
// The gateway (v0.10): WebSocket `GET /api/channels/{name}/live` (token in the second subprotocol
// `fluxmq.token.<token>`) and SSE `GET /api/channels/{name}/events` (`?access_token=`). Read-only,
// only committed messages (≤ high watermark), resumable by `cursor`. `GET /api/live/status` (read)
// reports each node's limits and usage; unreachable nodes are left out.

/** `--live-*` limits of one node (they may differ between nodes). */
export interface LiveLimits {
  max_connections: number;
  max_connections_per_token: number;
  max_connections_per_channel: number;
  max_subscriptions: number;
  /** 0 = unlimited */
  max_msgs_per_sec: number;
  /** 0 = unlimited */
  max_bytes_per_sec: number;
  max_payload_bytes: number;
  max_batch: number;
  send_buffer: number;
  send_timeout_secs: number;
  heartbeat_secs: number;
  idle_timeout_secs: number;
  fanout_buffer: number;
  /** Browser origins allowed besides same-origin (`*` = any). */
  allowed_origins: string[];
}

export interface LiveChannelCount {
  channel: string;
  subscriptions: number;
}

export type LiveRejectReason = 'node_limit' | 'token_limit' | 'channel_limit' | 'subscription_limit' | 'origin' | 'forbidden' | 'unauthorized';

export interface LiveRejection {
  reason: LiveRejectReason | string;
  count: number;
}

/** One node's gateway; counters are cumulative since the node started. */
export interface LiveNodeStatus {
  node: string;
  limits: LiveLimits;
  connections_ws: number;
  connections_sse: number;
  subscriptions_catchup: number;
  subscriptions_live: number;
  /** Channels with a shared read running (one per channel, whatever the subscriber count). */
  feeds: number;
  messages_sent: number;
  bytes_sent: number;
  slow_disconnects: number;
  /** Subscribers that fell behind retention and skipped messages. */
  gaps: number;
  /** Only non-zero reasons. */
  rejections: LiveRejection[];
  /** Most subscribed first. */
  channels: LiveChannelCount[];
}

/** `GET /api/live/status`: this node first, then the peers that answered. */
export interface LiveStatus {
  nodes: LiveNodeStatus[];
}

/** One message of a live `messages` event (design doc §1.2). */
export interface LiveEnvelope {
  partition: number;
  offset: number;
  key: string | null;
  headers: Record<string, string>;
  timestamp: number;
  encoding: 'utf8' | 'base64';
  payload: string;
  size: number;
  truncated?: boolean;
}

// ───────────────────────────── scheduled.rs (v0.12): delayed messages ─────────────────────────────
//
// A publish item with `deliver_at_ms` / `delay_ms` (or the headers `flux-deliver-at` /
// `flux-delay-ms`) is kept in the system channel `$scheduled` and published to its channel when
// due (at most 30 days ahead). Its returned id is a schedule id (cancel / release with it).
// Released copies carry `flux-scheduled-id` and `flux-scheduled-for`.

export const SCHEDULED_CHANNEL = '$scheduled';
export const MAX_DELAY_MS = 30 * 86_400_000;
export const SCHEDULED_HEADERS = { ID: 'flux-scheduled-id', FOR: 'flux-scheduled-for' } as const;

export interface ScheduledMessage {
  /** Schedule id: the entry's position in `$scheduled`. */
  id: MessageId;
  channel: string;
  key: string | null;
  due_ms: number;
  scheduled_at_ms: number;
  size: number;
  event_type: string | null;
  /** Node holding it (the partition's leader). */
  node: string;
}

export interface ScheduledChannel {
  channel: string;
  count: number;
  next_due_ms: number;
}

/** `GET /api/scheduled?channel=&limit=`. */
export interface ScheduledList {
  /** Soonest first. */
  items: ScheduledMessage[];
  total: number;
  channels: ScheduledChannel[];
  /** Since the nodes started. */
  released: number;
  cancelled: number;
}
