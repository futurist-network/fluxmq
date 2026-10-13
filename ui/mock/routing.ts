/** Mock router: routes CRUD, live counters, backtest — contract of crates/fluxmq-proto/src/routing.rs. */
import type { Condition, CreateRouteRequest, MessageView, ParseFilterResponse, RenderFilterResponse, Route, RouteStats, RouteTestRequest, RouteTestResponse, UpdateRouteRequest } from '../src/api/types';
import { OPS, ROUTER_GROUP } from '../src/api/types';
import { FilterSyntaxError, parse, render } from './dsl';
import { matches } from './filter';
import { ApiErr, EMPTY, type Sim, type SimChannel } from './sim';

const DAY = 86_400_000;
const SCAN_CAP = 200_000;

interface Live {
  evaluated: number;
  matched: number;
  forwarded: number;
  failed: number;
  last_matched_ms: number | null;
}

export function parseFilterOrThrow(text: string): Condition[] {
  try {
    return parse(text);
  } catch (e) {
    if (e instanceof FilterSyntaxError) throw new ApiErr(400, 'bad_request', e.message, e.position);
    throw e;
  }
}

/** Like `fluxmq_router::validate_conditions` (400 names the offending condition). */
export function validateConditions(v: unknown): Condition[] {
  if (!Array.isArray(v)) throw new ApiErr(400, 'bad_request', "'conditions' must be an array");
  if (v.length > 32) throw new ApiErr(400, 'bad_request', 'at most 32 conditions are allowed');
  return v.map((c, i) => {
    const where = `condition ${i + 1}`;
    if (!c || typeof c !== 'object') throw new ApiErr(400, 'bad_request', `${where} must be an object`);
    const { field, op, value } = c as Condition;
    const kinds = ['key', 'partition', 'header', 'payload'];
    if (!field || !kinds.includes(field.kind)) throw new ApiErr(400, 'bad_request', `${where}.field.kind must be one of ${kinds.join(', ')}`);
    if ((field.kind === 'header' || field.kind === 'payload') && (typeof field.name !== 'string' || !field.name)) throw new ApiErr(400, 'bad_request', `${where}.field.name is required`);
    if (!OPS.includes(op)) throw new ApiErr(400, 'bad_request', `${where}.op is not a known operator`);
    const needs = op !== 'exists' && op !== 'not_exists';
    if (needs && !('value' in c)) throw new ApiErr(400, 'bad_request', `${where}: value is required`);
    if (needs && (value === undefined || value === null) && op !== 'eq' && op !== 'ne') throw new ApiErr(400, 'bad_request', `${where}.value is required for ${op}`);
    if (Array.isArray(value) && op !== 'in' && op !== 'not_in') throw new ApiErr(400, 'bad_request', `${where}: lists are only allowed with in / not_in`);
    return needs ? { field, op, value: value ?? null } : { field, op, value: null };
  });
}

export class Routing {
  private routes = new Map<string, Route>();
  private live = new Map<string, Live>();
  private sel = new Map<string, number>();
  private failRate = new Map<string, number>();
  private seq = 1;

  constructor(private sim: Sim) {
    if (!EMPTY) this.seed();
    sim.onTick.push((now, dt) => this.tick(now, dt));
    this.syncRouterGroups();
  }

  private seed(): void {
    const now = Date.now();
    const mk = (id: string, name: string, source: string, filter: string, targets: string[], agoDays: number, enabled = true, fail = 0) => {
      const r: Route = { id, name, source, enabled, conditions: parse(filter), targets, created_at_ms: now - agoDays * DAY, updated_at_ms: now - Math.min(agoDays, 1.5) * DAY };
      this.routes.set(id, r);
      this.failRate.set(id, fail);
      const sel = this.selectivity(r);
      const ch = this.sim.channels.get(source);
      const secs = Math.min(14 * DAY, now - r.created_at_ms) / 1000;
      const evaluated = enabled ? (ch?.baseRate ?? 0) * 0.9 * secs : (ch?.baseRate ?? 0) * 0.9 * secs * 0.3;
      const matched = evaluated * sel;
      this.live.set(id, { evaluated, matched, forwarded: matched * targets.length * (1 - fail), failed: matched * targets.length * fail, last_matched_ms: matched > 0 ? (enabled ? now - 1000 : now - 2 * DAY) : null });
    };
    mk('rt_hv01', 'High-value orders', 'orders.events', 'header.flux-type = "order.created" AND total >= 250', ['orders.priority', 'notifications'], 9);
    mk('rt_pf02', 'Failed payments → customer email', 'payments.events', 'header.flux-type = "payment.failed"', ['email'], 20);
    mk('rt_de03', 'DE express shipping', 'orders.events', 'shipping.country = "DE" AND shipping.method = "express"', ['orders.priority'], 2, true, 0.004);
    mk('rt_rs04', 'Restocks & returns → push', 'inventory.updates', 'reason in ["restock", "return"]', ['notifications'], 30, false);
  }

  private syncRouterGroups(): void {
    const sources = new Set([...this.routes.values()].filter((r) => r.enabled).map((r) => r.source));
    for (const name of this.sim.channels.keys()) this.sim.setRouterGroup(name, sources.has(name));
  }

  /** Evenly spaced sample of stored messages of the newest `windowMs`. */
  private sample(ch: SimChannel, n: number, windowMs: number): MessageView[] {
    const P = ch.config.partitions;
    const now = Date.now();
    const out: MessageView[] = [];
    const k = Math.max(2, Math.ceil(n / P));
    for (let p = 0; p < P; p++) {
      const high = this.sim.partitionHigh(ch, p);
      const low = Math.max(this.sim.partitionLow(ch, p), Math.ceil(this.sim.offsetAt(ch, p, now - windowMs)));
      if (high <= low) continue;
      const span = high - low;
      const step = Math.max(1, Math.floor(span / k));
      for (let o = high - 1; o >= low && out.length < (p + 1) * k; o -= step) out.push(this.sim.messageAt(ch, p, o));
    }
    return out;
  }

  private selectivity(r: Pick<Route, 'source' | 'conditions'>, windowMs = DAY): number {
    if (!r.conditions.length) return 1;
    const ch = this.sim.channels.get(r.source);
    if (!ch) return 0;
    const s = this.sample(ch, 1200, windowMs);
    if (!s.length) return 0;
    return s.filter((m) => matches(r.conditions, m)).length / s.length;
  }

  private tick(now: number, dt: number): void {
    for (const r of this.routes.values()) {
      if (!r.enabled) continue;
      const ch = this.sim.channels.get(r.source);
      const l = this.live.get(r.id);
      if (!ch || !l) continue;
      let sel = this.sel.get(r.id);
      if (sel == null) this.sel.set(r.id, (sel = this.selectivity(r)));
      const fail = this.failRate.get(r.id) ?? 0;
      const dEval = ch.rate * dt;
      const dMatch = dEval * sel;
      l.evaluated += dEval;
      l.matched += dMatch;
      l.forwarded += dMatch * r.targets.length * (1 - fail);
      l.failed += dMatch * r.targets.length * fail;
      if (dMatch > 0 && Math.random() < 1 - Math.exp(-dMatch)) l.last_matched_ms = now;
    }
  }

  list(): Route[] {
    return [...this.routes.values()];
  }

  get(id: string): Route {
    const r = this.routes.get(id);
    if (!r) throw new ApiErr(404, 'not_found', `route '${id}' not found`);
    return r;
  }

  private checkTargets(source: string, targets: unknown): string[] {
    if (!Array.isArray(targets) || !targets.length || !targets.every((t) => typeof t === 'string')) throw new ApiErr(400, 'bad_request', "'targets' must be a non-empty array of channel names");
    for (const t of targets) {
      if (t === source) throw new ApiErr(400, 'bad_request', 'a route cannot target its own source channel');
      if (!this.sim.channels.has(t)) throw new ApiErr(400, 'bad_request', `target channel '${t}' does not exist`);
    }
    return [...new Set(targets)];
  }

  create(req: CreateRouteRequest): Route {
    const name = typeof req.name === 'string' ? req.name.trim() : '';
    if (!name) throw new ApiErr(400, 'bad_request', "'name' is required");
    if (typeof req.source !== 'string' || !this.sim.channels.has(req.source)) throw new ApiErr(400, 'bad_request', `source channel '${String(req.source)}' does not exist`);
    if (req.source.startsWith('$')) throw new ApiErr(400, 'bad_request', 'invalid source');
    const now = Date.now();
    const r: Route = {
      id: `rt_${Math.random().toString(36).slice(2, 8)}${this.seq++}`,
      name,
      source: req.source,
      enabled: req.enabled ?? true,
      conditions: validateConditions(req.conditions),
      targets: this.checkTargets(req.source, req.targets),
      created_at_ms: now,
      updated_at_ms: now,
    };
    this.routes.set(r.id, r);
    this.live.set(r.id, { evaluated: 0, matched: 0, forwarded: 0, failed: 0, last_matched_ms: null });
    this.syncRouterGroups();
    return r;
  }

  update(id: string, req: UpdateRouteRequest): Route {
    const r = this.get(id);
    const next: Route = { ...r, updated_at_ms: Date.now() };
    if (req.name != null) {
      if (!req.name.trim()) throw new ApiErr(400, 'bad_request', "'name' must not be empty");
      next.name = req.name.trim();
    }
    if (req.enabled != null) next.enabled = !!req.enabled;
    if (req.conditions != null) {
      next.conditions = validateConditions(req.conditions);
      this.sel.delete(id);
    }
    if (req.targets != null) next.targets = this.checkTargets(r.source, req.targets);
    this.routes.set(id, next);
    this.syncRouterGroups();
    return next;
  }

  delete(id: string): void {
    this.get(id);
    this.routes.delete(id);
    this.live.delete(id);
    this.syncRouterGroups();
  }

  stats(): RouteStats[] {
    return [...this.routes.values()].map((r) => {
      const l = this.live.get(r.id) ?? { evaluated: 0, matched: 0, forwarded: 0, failed: 0, last_matched_ms: null };
      return { id: r.id, evaluated: Math.round(l.evaluated), matched: Math.round(l.matched), forwarded: Math.round(l.forwarded), failed: Math.round(l.failed), last_matched_ms: l.last_matched_ms };
    });
  }

  parse(filter: unknown): ParseFilterResponse {
    if (typeof filter !== 'string') throw new ApiErr(400, 'bad_request', "'filter' must be a string");
    const conditions = parseFilterOrThrow(filter);
    return { conditions, rendered: render(conditions) };
  }

  render(conditions: unknown): RenderFilterResponse {
    return { rendered: render(validateConditions(conditions)) };
  }

  /**
   * `DELETE /api/channels/{name}/groups/{group}`: 404 unknown group, 409 while it has
   * subscribers, 409 for a `$…` system group still used by a route (`$router` of a source).
   */
  deleteGroup(channel: string, group: string): void {
    const ch = this.sim.channel(channel);
    const g = ch.groups.find((x) => x.spec.name === group);
    if (!g) throw new ApiErr(404, 'not_found', `group '${group}' not found on channel '${channel}'`);
    if (group.startsWith('$')) {
      const used = [...this.routes.values()].filter((r) => r.source === channel);
      if (group === ROUTER_GROUP && used.length) {
        throw new ApiErr(409, 'conflict', `group '${group}' is used by ${used.length} route${used.length === 1 ? '' : 's'} with source '${channel}' — delete those routes first`);
      }
    }
    if (g.instances.length) throw new ApiErr(409, 'conflict', `group '${group}' has ${g.instances.length} active subscriber${g.instances.length === 1 ? '' : 's'} — stop them first`);
    this.sim.deleteGroup(channel, group);
  }

  test(req: RouteTestRequest): RouteTestResponse {
    const ch = this.sim.channels.get(req.source);
    if (!ch) throw new ApiErr(404, 'not_found', `channel '${String(req.source)}' not found`);
    const conditions = validateConditions(req.conditions);
    const windowMs = req.window_ms ?? DAY;
    const limit = Math.min(100, Math.max(1, req.sample_limit ?? 20));
    const now = Date.now();
    let total = 0;
    for (let p = 0; p < ch.config.partitions; p++) {
      const high = this.sim.partitionHigh(ch, p);
      const low = Math.max(this.sim.partitionLow(ch, p), Math.ceil(this.sim.offsetAt(ch, p, now - windowMs)));
      total += Math.max(0, high - low);
    }
    const scanned = Math.min(total, SCAN_CAP);
    const truncated = total > SCAN_CAP;
    // The scan covers the newest `scanned` messages: sample that span.
    const spanMs = truncated ? Math.max(1000, (scanned / Math.max(1, ch.rate)) * 1000) : windowMs;
    let matched: number;
    if (!conditions.length) matched = scanned;
    else {
      const s = this.sample(ch, 1500, spanMs);
      matched = s.length ? Math.round((s.filter((m) => matches(conditions, m)).length / s.length) * scanned) : 0;
    }
    const samples = matched > 0 ? this.sim.queryMessages(req.source, { newest_first: true, limit, since_ms: now - windowMs }, conditions.length ? (m) => matches(conditions, m) : undefined).messages : [];
    if (samples.length > matched) matched = samples.length;
    return { scanned, matched, truncated, samples };
  }
}
