/**
 * FluxMQ mock server — same HTTP routes and WebSocket telemetry contract v1 as `fluxmq-api`
 * (see docs/ARCHITECTURE.md and crates/fluxmq-proto), backed by a believable simulation.
 *
 *   pnpm mock                      # http://localhost:7421
 *
 * Env: PORT, MOCK_ADMIN_TOKEN (bootstrap admin token → enforced auth), MOCK_ROLE=viewer|operator|admin
 * (enforced auth with seeded tokens; unauthenticated requests act as that role),
 * MOCK_SCENARIO=empty|single|cluster (default: healthy 3-node cluster; `cluster` adds a node
 * failure + failover cycle), MOCK_FAST=1 (audit.logs appears after 8 s instead of 45 s),
 * MOCK_DROP_SEQ=1 (occasionally skip a delta seq), MOCK_OLD_NODE=1 (backup runs refused with
 * `unsupported_in_cluster`, as when a cluster node predates the internal backup API).
 * v0.4: MOCK_SCENARIO=rebalance (4 nodes, n4 joined → a rebalance plan runs; see mock/rebalance.ts),
 * MOCK_REBALANCE_MODE=off|repair|full, MOCK_TLS=off (plaintext deployment; see mock/tls.ts).
 * v0.5: MOCK_SCENARIO=decommission (5 nodes: decommission n4, forget the dead n5; see
 * mock/decommission.ts), MOCK_DECOMMISSION_FAIL=1, MOCK_NO_DECOMMISSION_DRY_RUN=1, MOCK_CLIENT_MTLS=off (certificate bindings inert),
 * MOCK_WS_REVOKE_AFTER=<secs> (each WS session is closed with 4401 "token revoked" that many seconds
 * after it opens, and the token it presented is refused from then on — once per token value, so a
 * new token works). Revoking / expiry of a stored token closes its WS sessions with 4401 too.
 */
import http from 'node:http';
import { WebSocketServer, type WebSocket } from 'ws';
import type {
  ChannelMode,
  ClientMsg,
  CreateChannelRequest,
  DlqActionRequest,
  MessageQuery,
  PublishItem,
  RetentionPolicy,
  RetryPolicy,
  ServerMsg,
  StartFrom,
  UpdateChannelRequest,
} from '../src/api/types';
import { CHANNEL_MODES, CONTRACT_VERSION, SYSTEM_EVENTS_CHANNEL, parseTopic, validateChannelName } from '../src/api/types';
import { diffMergePatch } from '../src/api/mergePatch';
import { ApiErr, NODE_IDS, Sim, VERSION } from './sim';
import { Auth, needFor, type Principal } from './auth';
import { parseFilterOrThrow, Routing } from './routing';
import { Cluster } from './cluster';
import { matches } from './filter';
import { Notify } from './notify';
import { Webhooks } from './webhooks';
import { Backups } from './backups';
import { Rebalancer } from './rebalance';
import { Tls } from './tls';
import { Decommissioner } from './decommission';

const PORT = Number(process.env.PORT ?? 7421);
const TOKEN = process.env.MOCK_ADMIN_TOKEN || null;
const DROP_SEQ = process.env.MOCK_DROP_SEQ === '1';
const WS_REVOKE_AFTER_MS = Number(process.env.MOCK_WS_REVOKE_AFTER ?? 0) * 1000;
/** WebSocket close code for a revoked / expired session token (auth.rs v0.5). */
const WS_CLOSE_TOKEN_REVOKED = 4401;
const TICK_MS = 250;

const sim = new Sim();
const auth = new Auth(TOKEN, process.env.MOCK_ROLE);
const routing = new Routing(sim);
const cluster = new Cluster(sim);
const notify = new Notify(sim);
const webhooks = new Webhooks(sim);
const backups = new Backups(sim);
const rebalancer = new Rebalancer(sim, cluster);
const tls = new Tls(sim);
const decommissioner = new Decommissioner(sim, cluster, rebalancer);

// ───────────────────────────── HTTP helpers ─────────────────────────────

function send(res: http.ServerResponse, status: number, body?: unknown): void {
  if (body === undefined) {
    res.writeHead(status);
    res.end();
    return;
  }
  const text = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' });
  res.end(text);
}

function readBody(req: http.IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    req.on('data', (c: Buffer) => {
      size += c.length;
      if (size > 16 * 1024 * 1024) {
        reject(new ApiErr(413, 'bad_request', 'body too large'));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8');
      if (raw.trim() === '') return resolve(undefined);
      try {
        resolve(JSON.parse(raw));
      } catch {
        reject(new ApiErr(400, 'bad_request', 'request body is not valid JSON'));
      }
    });
    req.on('error', reject);
  });
}

function isObj(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function bad(message: string): never {
  throw new ApiErr(400, 'bad_request', message);
}

function optInt(q: URLSearchParams, name: string): number | undefined {
  const v = q.get(name);
  if (v == null || v === '') return undefined;
  const n = Number(v);
  if (!Number.isInteger(n) || n < 0) bad(`query parameter '${name}' must be a non-negative integer`);
  return n;
}

function optNumOrNull(v: unknown, name: string): number | null {
  if (v == null) return null;
  if (typeof v !== 'number' || !Number.isFinite(v) || v < 0) bad(`'${name}' must be a non-negative number or null`);
  return v;
}

function parseRetention(v: unknown): RetentionPolicy | null {
  if (v == null) return null;
  if (!isObj(v)) bad("'retention' must be an object");
  return { max_age_secs: optNumOrNull(v.max_age_secs, 'retention.max_age_secs'), max_bytes: optNumOrNull(v.max_bytes, 'retention.max_bytes') };
}

function parseRetry(v: unknown): RetryPolicy | null {
  if (v == null) return null;
  if (!isObj(v)) bad("'retry' must be an object");
  const { max_attempts, backoff_ms, ack_timeout_ms } = v;
  if (typeof max_attempts !== 'number' || !Number.isInteger(max_attempts) || max_attempts < 1) bad("'retry.max_attempts' must be an integer >= 1");
  if (!Array.isArray(backoff_ms) || !backoff_ms.every((x) => typeof x === 'number' && x >= 0)) bad("'retry.backoff_ms' must be an array of non-negative numbers");
  if (typeof ack_timeout_ms !== 'number' || ack_timeout_ms < 0) bad("'retry.ack_timeout_ms' must be a non-negative number");
  return { max_attempts, backoff_ms: backoff_ms as number[], ack_timeout_ms };
}

function parseStartFrom(v: unknown): StartFrom {
  if (v === 'earliest' || v === 'latest') return v;
  if (isObj(v) && typeof v.timestamp_ms === 'number' && v.timestamp_ms >= 0) return { timestamp_ms: v.timestamp_ms };
  bad("'to' must be \"earliest\", \"latest\" or {\"timestamp_ms\": n}");
}

function decodePayload(v: unknown, where: string): { payload: PublishItem['payload']; data: Buffer } {
  if (!isObj(v) || typeof v.data !== 'string') bad(`${where}.payload must be {"encoding":"utf8"|"base64","data":"..."}`);
  if (v.encoding === 'utf8') return { payload: { encoding: 'utf8', data: v.data }, data: Buffer.from(v.data, 'utf8') };
  if (v.encoding === 'base64') {
    if (!/^[A-Za-z0-9+/]*={0,2}$/.test(v.data)) bad(`${where}.payload.data is not valid base64`);
    return { payload: { encoding: 'base64', data: v.data }, data: Buffer.from(v.data, 'base64') };
  }
  bad(`${where}.payload.encoding must be "utf8" or "base64"`);
}

function principal(req: http.IncomingMessage, url: URL, allowQuery: boolean): Principal | null {
  const h = req.headers.authorization;
  const bearer = h?.startsWith('Bearer ') ? h.slice(7).trim() : null;
  return auth.authenticate(bearer ?? (allowQuery ? url.searchParams.get('token') : null));
}

// ───────────────────────────── routes ─────────────────────────────

async function route(req: http.IncomingMessage, res: http.ServerResponse, url: URL): Promise<void> {
  const method = req.method ?? 'GET';
  const seg = url.pathname.split('/').filter(Boolean).map(decodeURIComponent);
  if (seg[0] === 'metrics' && seg.length === 1) {
    const o = sim.clusterOverview();
    res.writeHead(200, { 'content-type': 'text/plain; version=0.0.4' });
    res.end(`# HELP fluxmq_ingress_messages_per_second Publish rate\n# TYPE fluxmq_ingress_messages_per_second gauge\nfluxmq_ingress_messages_per_second ${o.ingress_rate.toFixed(1)}\n# TYPE fluxmq_consumer_lag_messages gauge\nfluxmq_consumer_lag_messages ${o.total_lag}\n# TYPE fluxmq_dlq_messages gauge\nfluxmq_dlq_messages ${o.dlq_total}\n`);
    return;
  }
  if (seg[0] !== 'api') {
    res.writeHead(404, { 'content-type': 'text/plain' });
    res.end('FluxMQ mock serves /api and /ws only — run the UI with `pnpm dev`.\n');
    return;
  }
  const q = url.searchParams;
  const [, a, b, c, d, e] = seg;
  const n = seg.length;

  // Like the real server: health is open (liveness probes), everything else needs the token.
  if (a === 'health' && n === 2 && method === 'GET') {
    return send(res, 200, { status: 'ok', version: VERSION, node_id: 'n1', uptime_secs: Math.floor((Date.now() - sim.start) / 1000) + 1_209_600 });
  }
  const who = principal(req, url, false);
  if (!who) throw new ApiErr(401, 'unauthorized', 'invalid or missing auth token');
  auth.authorize(who, needFor(method, seg));

  // ── RBAC ──
  if (a === 'auth') {
    if (b === 'me' && n === 3 && method === 'GET') return send(res, 200, auth.whoami(who));
    if (b === 'tokens' && n === 3 && method === 'GET') return send(res, 200, auth.list());
    if (b === 'tokens' && n === 3 && method === 'POST') {
      const body = await readBody(req);
      if (!isObj(body)) bad('body must be a CreateTokenRequest object');
      const r = auth.create(body as never, who);
      sim.addAudit('auth.token.create', r.info.name, { role: r.info.role });
      return send(res, 201, r);
    }
    if (b === 'tokens' && n === 4 && c && method === 'DELETE') {
      auth.revoke(c);
      sim.addAudit('auth.token.revoke', c, {});
      closeRevokedSessions();
      return send(res, 204);
    }
    // v0.5 certificate bindings
    if (b === 'cert-bindings' && n === 3 && method === 'GET') return send(res, 200, auth.listBindings());
    if (b === 'cert-bindings' && n === 3 && method === 'POST') {
      const body = await readBody(req);
      if (!isObj(body)) bad('body must be a CreateCertBindingRequest object');
      const r = auth.createBinding(body as never, who);
      sim.addAudit('auth.cert_binding.create', r.cn, { role: r.role });
      return send(res, 201, r);
    }
    if (b === 'cert-bindings' && n === 4 && c && method === 'DELETE') {
      auth.deleteBinding(c);
      sim.addAudit('auth.cert_binding.delete', c, {});
      return send(res, 204);
    }
  }

  // ── Routing ──
  if (a === 'filter' && b === 'parse' && n === 3 && method === 'POST') {
    const body = await readBody(req);
    return send(res, 200, routing.parse(isObj(body) ? body.filter : undefined));
  }
  if (a === 'filter' && b === 'render' && n === 3 && method === 'POST') {
    const body = await readBody(req);
    if (!isObj(body)) bad('body must be a RenderFilterRequest object');
    return send(res, 200, routing.render(body.conditions));
  }
  if (a === 'routes') {
    if (n === 2 && method === 'GET') return send(res, 200, routing.list());
    if (n === 2 && method === 'POST') {
      const body = await readBody(req);
      if (!isObj(body)) bad('body must be a CreateRouteRequest object');
      const r = routing.create(body as never);
      sim.addAudit('route.create', r.name, { source: r.source, targets: r.targets });
      return send(res, 201, r);
    }
    if (b === 'stats' && n === 3 && method === 'GET') return send(res, 200, routing.stats());
    if (b === 'test' && n === 3 && method === 'POST') {
      const body = await readBody(req);
      if (!isObj(body)) bad('body must be a RouteTestRequest object');
      return send(res, 200, routing.test(body as never));
    }
    if (b && n === 3) {
      if (method === 'GET') return send(res, 200, routing.get(b));
      if (method === 'PATCH') {
        const body = await readBody(req);
        if (!isObj(body)) bad('body must be an UpdateRouteRequest object');
        const r = routing.update(b, body as never);
        sim.addAudit('route.update', r.name, body);
        return send(res, 200, r);
      }
      if (method === 'DELETE') {
        routing.delete(b);
        sim.addAudit('route.delete', b, {});
        return send(res, 204);
      }
    }
  }

  // ── Notifications (v0.3) ──
  if (a === 'notify') {
    if (b === 'notifiers') {
      if (n === 3 && method === 'GET') return send(res, 200, notify.listNotifiers());
      if (n === 3 && method === 'POST') {
        const r = notify.createNotifier(await readBody(req));
        sim.addAudit('notify.notifier.create', r.name, { type: r.kind.type });
        return send(res, 201, r);
      }
      if (c === 'test' && n === 4 && method === 'POST') return send(res, 200, await notify.testDraft(await readBody(req)));
      if (c && n === 4 && method === 'GET') return send(res, 200, notify.getNotifier(c));
      if (c && n === 4 && method === 'PATCH') return send(res, 200, notify.updateNotifier(c, await readBody(req)));
      if (c && n === 4 && method === 'DELETE') {
        notify.deleteNotifier(c);
        return send(res, 204);
      }
      if (c && d === 'test' && n === 5 && method === 'POST') return send(res, 200, await notify.testNotifier(c, await readBody(req)));
    }
    if (b === 'rules') {
      if (n === 3 && method === 'GET') return send(res, 200, notify.listRules());
      if (n === 3 && method === 'POST') return send(res, 201, notify.createRule(await readBody(req)));
      if (c && n === 4 && method === 'GET') {
        const r = notify.listRules().find((x) => x.id === c);
        if (!r) throw new ApiErr(404, 'not_found', `rule '${c}' not found`);
        return send(res, 200, r);
      }
      if (c && n === 4 && method === 'PATCH') return send(res, 200, notify.updateRule(c, await readBody(req)));
      if (c && n === 4 && method === 'DELETE') {
        notify.deleteRule(c);
        return send(res, 204);
      }
    }
    if (b === 'history' && n === 3 && method === 'GET') return send(res, 200, notify.historyList());
  }
  if (a === 'events' && b === 'feed' && n === 3 && method === 'GET') {
    return send(res, 200, await notify.feedPage(q.get('after'), optInt(q, 'limit') ?? 100, optInt(q, 'wait_ms') ?? 0, optInt(q, 'tail') ?? null));
  }

  // ── Webhooks (v0.3) ──
  if (a === 'webhooks') {
    if (n === 2 && method === 'GET') return send(res, 200, webhooks.list());
    if (n === 2 && method === 'POST') return send(res, 201, webhooks.create(await readBody(req)));
    if (b === 'stats' && n === 3 && method === 'GET') return send(res, 200, webhooks.stats());
    if (b === 'test' && n === 3 && method === 'POST') return send(res, 200, await webhooks.testDraft(await readBody(req)));
    if (b && n === 3 && method === 'GET') return send(res, 200, webhooks.get(b));
    if (b && n === 3 && method === 'PATCH') return send(res, 200, webhooks.update(b, await readBody(req)));
    if (b && n === 3 && method === 'DELETE') {
      webhooks.delete(b);
      return send(res, 204);
    }
    if (b && c === 'test' && n === 4 && method === 'POST') return send(res, 200, await webhooks.test(b, await readBody(req)));
  }

  // ── Backups (v0.3) ──
  if (a === 'backups') {
    if (b === 'config' && n === 3 && method === 'GET') return send(res, 200, backups.getConfig());
    if (b === 'config' && n === 3 && method === 'PUT') return send(res, 200, backups.putConfig(await readBody(req)));
    if (b === 'test-target' && n === 3 && method === 'POST') return send(res, 200, await backups.testTarget(await readBody(req)));
    if (b === 'status' && n === 3 && method === 'GET') return send(res, 200, backups.status());
    if (b === 'run' && n === 3 && method === 'POST') return send(res, 202, backups.run());
    if (n === 2 && method === 'GET') return send(res, 200, backups.list());
    if (b && n === 3 && method === 'GET') return send(res, 200, backups.get(b));
    if (b && n === 3 && method === 'DELETE') {
      backups.delete(b);
      return send(res, 204);
    }
  }

  // ── Cluster ──
  if (a === 'cluster') {
    if (b === 'status' && n === 3 && method === 'GET') return send(res, 200, cluster.status());
    if (b === 'metadata' && n === 3 && method === 'GET') return send(res, 200, cluster.metadata());
    if (b === 'nodes' && d === 'drain' && n === 5 && c && method === 'POST') return send(res, 200, { moved_leaderships: cluster.drain(c) });
    // v0.5 decommission / forget
    if (b === 'nodes' && d === 'decommission' && n === 5 && c && method === 'POST') return send(res, 200, q.get('dry_run') === 'true' ? decommissioner.dryRun(c) : decommissioner.start(c));
    if (b === 'decommissions' && n === 3 && method === 'GET') return send(res, 200, decommissioner.list());
    if (b === 'nodes' && d === 'decommission' && n === 5 && c && method === 'GET') return send(res, 200, decommissioner.status(c));
    if (b === 'nodes' && n === 4 && c && method === 'DELETE') {
      decommissioner.forget(c);
      return send(res, 204);
    }
    // v0.4 automatic rebalancing
    if (b === 'rebalance' && n === 3 && method === 'GET') return send(res, 200, rebalancer.status());
    if (b === 'rebalance' && n === 3 && method === 'POST') return send(res, 200, rebalancer.request(await readBody(req)));
    if (b === 'rebalance' && c === 'config' && n === 4 && method === 'PUT') return send(res, 200, rebalancer.putConfig(await readBody(req)));
    if (b === 'rebalance' && n === 4 && method === 'POST' && (c === 'pause' || c === 'resume' || c === 'cancel')) {
      rebalancer[c]();
      return send(res, 200, rebalancer.status());
    }
    if (b === 'partitions' && c === 'reassign' && n === 4 && method === 'POST') {
      const body = await readBody(req);
      if (!isObj(body)) bad('body must be a ReassignRequest object');
      cluster.reassign(body as never);
      return send(res, 202);
    }
  }

  if (a === 'channels') {
    if (n === 2 && method === 'GET') {
      return send(res, 200, [...sim.channels.values()].map((x) => x.config).sort((x, y) => x.name.localeCompare(y.name)));
    }
    if (n === 2 && method === 'POST') {
      const body = await readBody(req);
      if (!isObj(body)) bad('body must be a CreateChannelRequest object');
      if (typeof body.name !== 'string') bad("'name' is required");
      const err = validateChannelName(body.name);
      if (err) bad(err);
      if (!CHANNEL_MODES.includes(body.mode as ChannelMode)) bad("'mode' must be one of stream, queue, keyed");
      const partitions = body.partitions == null ? null : Number(body.partitions);
      if (partitions != null && (!Number.isInteger(partitions) || partitions < 1 || partitions > 1024)) bad("'partitions' must be an integer in 1..1024");
      if (body.ordering_key != null && typeof body.ordering_key !== 'string') bad("'ordering_key' must be a string or null");
      if (body.dlq_enabled != null && typeof body.dlq_enabled !== 'boolean') bad("'dlq_enabled' must be a boolean or null");
      const reqBody: CreateChannelRequest = {
        name: body.name,
        mode: body.mode as ChannelMode,
        partitions,
        retention: parseRetention(body.retention),
        retry: parseRetry(body.retry),
        dlq_enabled: (body.dlq_enabled as boolean | null | undefined) ?? null,
        ordering_key: (body.ordering_key as string | null | undefined) ?? null,
      };
      return send(res, 201, sim.createChannel(reqBody));
    }
    if (!b) throw new ApiErr(405, 'bad_request', `method ${method} not allowed`);
    if (n === 3) {
      if (method === 'GET') return send(res, 200, sim.channel(b).config);
      if (method === 'PATCH') {
        if (b.startsWith('$')) bad(`'${b}' is a system channel and is read-only`);
        const body = await readBody(req);
        if (!isObj(body)) bad('body must be an UpdateChannelRequest object');
        if (body.dlq_enabled != null && typeof body.dlq_enabled !== 'boolean') bad("'dlq_enabled' must be a boolean or null");
        const upd: UpdateChannelRequest = {
          retention: parseRetention(body.retention),
          retry: parseRetry(body.retry),
          dlq_enabled: (body.dlq_enabled as boolean | null | undefined) ?? null,
        };
        return send(res, 200, sim.updateChannel(b, upd));
      }
      if (method === 'DELETE') {
        sim.deleteChannel(b);
        return send(res, 204);
      }
    }
    if (c === 'publish' && n === 4 && method === 'POST') {
      sim.channel(b);
      const body = await readBody(req);
      if (!isObj(body) || !Array.isArray(body.messages) || body.messages.length === 0) bad("'messages' must be a non-empty array");
      if (body.messages.length > 10_000) bad('at most 10000 messages per request');
      const items = body.messages.map((m, i) => {
        if (!isObj(m)) bad(`messages[${i}] must be an object`);
        if (m.key != null && typeof m.key !== 'string') bad(`messages[${i}].key must be a string or null`);
        const headers: Record<string, string> = {};
        if (m.headers != null) {
          if (!isObj(m.headers)) bad(`messages[${i}].headers must be an object`);
          for (const [k, v] of Object.entries(m.headers)) {
            if (typeof v !== 'string') bad(`messages[${i}].headers.${k} must be a string`);
            headers[k] = v;
          }
        }
        const { payload, data } = decodePayload(m.payload, `messages[${i}]`);
        return { key: (m.key as string | null | undefined) ?? null, headers, payload, data };
      });
      const ids = sim.publish(b, items);
      sim.addAudit('messages.publish', b, { count: ids.length });
      return send(res, 200, { ids });
    }
    if (c === 'messages' && method === 'GET' && b === SYSTEM_EVENTS_CHANNEL) {
      if (n === 4) return send(res, 200, notify.systemMessages({ limit: optInt(q, 'limit'), offset: optInt(q, 'offset'), since_ms: optInt(q, 'since_ms'), until_ms: optInt(q, 'until_ms'), newest_first: q.get('newest_first') === 'true', key: q.get('key') || undefined, partition: optInt(q, 'partition') }));
      if (n === 6) return send(res, 200, notify.systemMessage(Number(e)));
    }
    if (c === 'messages' && method === 'GET') {
      if (n === 4) {
        const mq: MessageQuery = {};
        const partition = optInt(q, 'partition');
        const offset = optInt(q, 'offset');
        const since = optInt(q, 'since_ms');
        const until = optInt(q, 'until_ms');
        const limit = optInt(q, 'limit');
        if (partition !== undefined) mq.partition = partition;
        if (offset !== undefined) mq.offset = offset;
        if (since !== undefined) mq.since_ms = since;
        if (until !== undefined) mq.until_ms = until;
        if (limit !== undefined) mq.limit = limit;
        const key = q.get('key');
        if (key != null && key !== '') mq.key = key;
        const nf = q.get('newest_first');
        if (nf === 'true') mq.newest_first = true;
        else if (nf != null && nf !== 'false') bad('newest_first must be true or false');
        if (since !== undefined && until !== undefined && since > until) bad('since_ms must be <= until_ms');
        const filter = q.get('filter');
        if (filter != null && filter.trim() !== '') {
          const conds = parseFilterOrThrow(filter);
          return send(res, 200, sim.queryMessages(b, mq, (m) => matches(conds, m)));
        }
        return send(res, 200, sim.queryMessages(b, mq));
      }
      if (n === 6 && d !== undefined && e !== undefined) {
        const p = Number(d);
        const o = Number(e);
        if (!Number.isInteger(p) || !Number.isInteger(o)) bad('partition and offset must be integers');
        return send(res, 200, sim.messageDetail(b, p, o));
      }
    }
    if (c === 'replay' && n === 4 && method === 'POST') {
      const body = await readBody(req);
      if (!isObj(body) || !Array.isArray(body.ids) || body.ids.length === 0) bad("'ids' must be a non-empty array of {partition, offset}");
      const ids = body.ids.map((x, i) => {
        if (!isObj(x) || typeof x.partition !== 'number' || typeof x.offset !== 'number') bad(`ids[${i}] must be {partition, offset}`);
        return { partition: x.partition, offset: x.offset };
      });
      if (body.target_channel != null && typeof body.target_channel !== 'string') bad("'target_channel' must be a string or null");
      return send(res, 200, { ids: sim.replay(b, ids, (body.target_channel as string | null | undefined) ?? null) });
    }
    if (c === 'groups' && n === 5 && d !== undefined && method === 'DELETE') {
      routing.deleteGroup(b, d);
      return send(res, 204);
    }
    if (c === 'groups' && e === 'reset' && n === 6 && d !== undefined && method === 'POST') {
      const body = await readBody(req);
      if (!isObj(body)) bad('body must be {"to": StartFrom}');
      sim.resetOffsets(b, d, parseStartFrom(body.to));
      return send(res, 204);
    }
  }

  if (a === 'dlq') {
    const dq = () => {
      const out: { channel?: string; reason?: string; limit?: number; before?: number } = {};
      const ch = q.get('channel');
      const reason = q.get('reason');
      if (ch) out.channel = ch;
      if (reason) out.reason = reason;
      const limit = optInt(q, 'limit');
      const before = optInt(q, 'before');
      if (limit !== undefined) out.limit = limit;
      if (before !== undefined) out.before = before;
      return out;
    };
    if (b === 'summary' && n === 3 && method === 'GET') return send(res, 200, sim.dlqSummary());
    if (n === 2 && method === 'GET') return send(res, 200, sim.dlqList(dq()));
    if (b === 'export' && n === 3 && method === 'GET') {
      const { entries } = sim.dlqList(dq(), true);
      res.writeHead(200, {
        'content-type': 'application/x-ndjson',
        'content-disposition': `attachment; filename="fluxmq-dlq-${Date.now()}.ndjson"`,
      });
      res.end(entries.map((x) => JSON.stringify(x)).join('\n') + (entries.length ? '\n' : ''));
      return;
    }
    if ((b === 'retry' || b === 'discard') && n === 3 && method === 'POST') {
      const body = await readBody(req);
      if (!isObj(body)) bad('body must be a DlqActionRequest object');
      const reqBody: DlqActionRequest = {
        ids: body.ids as DlqActionRequest['ids'],
        reason: body.reason as string | null | undefined,
        channel: body.channel as string | null | undefined,
        all: body.all as boolean | null | undefined,
      };
      return send(res, 200, { affected: sim.dlqAction(b, reqBody) });
    }
  }

  if (a === 'history' && n === 2 && method === 'GET') {
    const metric = q.get('metric');
    const range = q.get('range') ?? '5m';
    if (!metric) bad("query parameter 'metric' is required");
    return send(res, 200, sim.history(metric, range));
  }
  if (a === 'tls' && n === 2 && method === 'GET') return send(res, 200, tls.status());
  if (a === 'alerts' && n === 2 && method === 'GET') return send(res, 200, sim.alertsState());
  if (a === 'search' && n === 2 && method === 'GET') return send(res, 200, sim.search(q.get('q') ?? ''));
  if (a === 'audit' && n === 2 && method === 'GET') return send(res, 200, sim.auditList(optInt(q, 'limit') ?? 100));

  throw new ApiErr(404, 'not_found', `no route for ${method} ${url.pathname}`);
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url ?? '/', 'http://localhost');
  route(req, res, url).catch((err: unknown) => {
    if (res.headersSent) return;
    if (err instanceof ApiErr) return send(res, err.status, err.position == null ? { error: err.code, message: err.message } : { error: err.code, message: err.message, position: err.position });
    console.error(err);
    send(res, 500, { error: 'internal', message: err instanceof Error ? err.message : String(err) });
  });
});

// ───────────────────────────── WebSocket telemetry ─────────────────────────────

interface Sub {
  seq: number;
  last: unknown;
}

interface Session {
  ws: WebSocket;
  subs: Map<string, Sub>;
  /** Stored token the session authenticated with (null: bootstrap / open mode / forced role). */
  tokenId: string | null;
  /** Token string presented on upgrade ('' = none). */
  presented: string;
  openedAt: number;
}

/** MOCK_WS_REVOKE_AFTER: token values whose sessions were already revoked once. */
const revokedOnce = new Set<string>();

function closeSession(s: Session, reason: string): void {
  sessions.delete(s);
  try {
    s.ws.close(WS_CLOSE_TOKEN_REVOKED, reason);
  } catch {
    /* already closing */
  }
}

/** Revocation / expiry: close every WS session whose token is gone (the broker does this within 1 s). */
function closeRevokedSessions(): void {
  for (const s of sessions) {
    const why = auth.sessionEnd(s.tokenId);
    if (why) closeSession(s, why);
  }
}

const sessions = new Set<Session>();
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url ?? '/', 'http://localhost');
  if (url.pathname !== '/ws') {
    socket.write('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');
    socket.destroy();
    return;
  }
  const who = principal(req, url, true);
  if (!who) {
    const body = JSON.stringify({ error: 'unauthorized', message: 'invalid or missing auth token' });
    socket.write(`HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: ${Buffer.byteLength(body)}\r\nConnection: close\r\n\r\n${body}`);
    socket.destroy();
    return;
  }
  const presented = (req.headers.authorization?.startsWith('Bearer ') ? req.headers.authorization.slice(7).trim() : null) ?? url.searchParams.get('token') ?? '';
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req, who.token_id, presented));
});

function sendMsg(s: Session, msg: ServerMsg): void {
  if (s.ws.readyState === s.ws.OPEN) s.ws.send(JSON.stringify(msg));
}

/** Current state of a topic; `snapshot` matters only for the tail (buffer vs. latest batch). */
function topicData(topic: string, snapshot: boolean): { data: unknown } | { error: string } {
  const t = parseTopic(topic);
  if (!t) return { error: `unknown topic '${topic}'` };
  switch (t.kind) {
    case 'cluster':
      return { data: sim.clusterOverview() };
    case 'channels':
      return { data: sim.channelsSummary() };
    case 'alerts':
      return { data: sim.alertsState() };
    case 'events':
      return { data: sim.eventsFeed() };
    case 'nodes':
      return { data: sim.nodesState() };
    case 'channel': {
      const d = sim.channelDetail(t.name);
      return d ? { data: d } : { error: `unknown channel '${t.name}'` };
    }
    case 'consumers': {
      const d = sim.consumers(t.name);
      return d ? { data: d } : { error: `unknown channel '${t.name}'` };
    }
    case 'tail': {
      const d = sim.tail(t.name, snapshot);
      return d ? { data: d } : { error: `unknown channel '${t.name}'` };
    }
  }
}

function refreshTailChannels(): void {
  const set = new Set<string>();
  for (const s of sessions) {
    for (const topic of s.subs.keys()) {
      const t = parseTopic(topic);
      if (t?.kind === 'tail') set.add(t.name);
    }
  }
  sim.tailChannels = set;
}

function subscribe(s: Session, topic: string): void {
  const r = topicData(topic, true);
  if ('error' in r) {
    sendMsg(s, { type: 'error', topic, message: r.error });
    return;
  }
  const prev = s.subs.get(topic);
  const seq = prev ? prev.seq + 1 : 1;
  s.subs.set(topic, { seq, last: r.data });
  sendMsg(s, { type: 'snapshot', topic, seq, ts: Date.now(), data: r.data });
}

wss.on('connection', (ws: WebSocket, _req: http.IncomingMessage, tokenId: string | null, presented: string) => {
  const s: Session = { ws, subs: new Map(), tokenId, presented, openedAt: Date.now() };
  sessions.add(s);
  sendMsg(s, { type: 'hello', contract_version: CONTRACT_VERSION, broker_version: VERSION, node_id: 'n1', server_time_ms: Date.now() });
  ws.on('message', (raw) => {
    let msg: ClientMsg;
    try {
      msg = JSON.parse(String(raw)) as ClientMsg;
    } catch {
      sendMsg(s, { type: 'error', topic: null, message: 'invalid JSON' });
      return;
    }
    switch (msg.op) {
      case 'subscribe':
        if (!Array.isArray(msg.topics)) break;
        for (const t of msg.topics) if (typeof t === 'string') subscribe(s, t);
        refreshTailChannels();
        return;
      case 'unsubscribe':
        if (!Array.isArray(msg.topics)) break;
        for (const t of msg.topics) s.subs.delete(t);
        refreshTailChannels();
        return;
      case 'ping':
        sendMsg(s, { type: 'pong', nonce: msg.nonce ?? null, server_time_ms: Date.now() });
        return;
    }
    sendMsg(s, { type: 'error', topic: null, message: 'unknown op (expected subscribe | unsubscribe | ping)' });
  });
  ws.on('close', () => {
    sessions.delete(s);
    refreshTailChannels();
  });
  ws.on('error', () => {
    sessions.delete(s);
  });
});

/** Which topics are due on this tick (cadences from the contract). */
function due(topic: string, tick: number): boolean {
  const t = parseTopic(topic);
  if (!t) return false;
  switch (t.kind) {
    case 'cluster':
    case 'tail':
      return true; // 250 ms
    case 'channels':
    case 'channel':
    case 'consumers':
      return tick % 2 === 0; // 500 ms
    case 'nodes':
    case 'alerts':
    case 'events':
      return tick % 4 === 0; // 1 s (alerts/events: only when changed)
  }
}

let tickNo = 0;
setInterval(() => {
  const now = Date.now();
  sim.tick(now);
  tickNo++;
  if (tickNo % 4 === 0) closeRevokedSessions();
  if (WS_REVOKE_AFTER_MS > 0) {
    for (const s of sessions) {
      if (revokedOnce.has(s.presented) || now - s.openedAt < WS_REVOKE_AFTER_MS) continue;
      revokedOnce.add(s.presented);
      if (s.presented) auth.revokeSecret(s.presented);
      closeSession(s, 'token revoked');
    }
  }
  const cache = new Map<string, { data: unknown } | { error: string }>();
  for (const s of sessions) {
    for (const [topic, sub] of s.subs) {
      if (!due(topic, tickNo)) continue;
      let r = cache.get(topic);
      if (!r) {
        r = topicData(topic, false);
        cache.set(topic, r);
      }
      if ('error' in r) {
        s.subs.delete(topic);
        sendMsg(s, { type: 'error', topic, message: r.error });
        continue;
      }
      const patch = diffMergePatch(sub.last, r.data);
      if (patch === undefined) continue;
      sub.seq++;
      sub.last = r.data;
      if (DROP_SEQ && Math.random() < 0.005) continue; // simulate a lost frame → client sees a gap
      sendMsg(s, { type: 'delta', topic, seq: sub.seq, ts: now, data: patch });
    }
  }
}, TICK_MS);

server.listen(PORT, () => {
  console.log(`FluxMQ mock listening on http://localhost:${PORT} · ${NODE_IDS.length} node(s) · auth ${auth.mode()}${auth.forcedRole ? ` as ${auth.forcedRole}` : ''}${TOKEN ? ' (MOCK_ADMIN_TOKEN)' : ''}`);
});

