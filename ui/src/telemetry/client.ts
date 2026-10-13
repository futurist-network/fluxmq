/**
 * Telemetry client — contract v1 (docs/ARCHITECTURE.md, crates/fluxmq-proto/src/telemetry.rs).
 *
 * * One WebSocket per tab. Topics are ref-counted: the first `subscribe(topic)` sends
 *   `{"op":"subscribe"}`, the last release sends `{"op":"unsubscribe"}` (after a short grace
 *   period so route changes / StrictMode double-mounts don't churn the server).
 * * `snapshot` replaces a topic's state; `delta` is an RFC 7386 merge patch applied on top.
 *   `seq` must increase by exactly 1 per topic — on a gap we re-subscribe and drop deltas
 *   until the fresh snapshot arrives.
 * * Frames are decoded in the socket callback (never in React render), queued, and applied
 *   at most once per animation frame. The sink receives one immutable view per frame.
 * * Reconnect with exponential backoff + jitter, re-subscribing every active topic.
 *   Ping keepalive; a silent socket is closed and reconnected.
 * * v0.5: close code 4401 means the session's token was revoked or expired. Reconnecting with the
 *   same token can't succeed, so the client stops and reports `conn.revoked` until `restart()`
 *   (called when the token changes).
 *
 * Environment-dependent pieces (WebSocket, rAF, timers, clock) are injectable for tests.
 */
import { applyMergePatch } from '../api/mergePatch';
import { CONTRACT_VERSION, WS_CLOSE_TOKEN_REVOKED, parseTopic, type ClientMsg, type ServerMsg, type TailMessage, type TailState } from '../api/types';

export interface MinimalSocket {
  readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  onopen: ((ev: unknown) => void) | null;
  onclose: ((ev: { code?: number; reason?: string }) => void) | null;
  onerror: ((ev: unknown) => void) | null;
  onmessage: ((ev: { data: unknown }) => void) | null;
}

export type SocketFactory = (url: string) => MinimalSocket;

export type ConnStatus = 'idle' | 'connecting' | 'open' | 'reconnecting' | 'stopped';

export interface ConnState {
  status: ConnStatus;
  /** Consecutive failed attempts since the last successful open. */
  attempt: number;
  /** When the next reconnect attempt fires (ms epoch), for the countdown banner. */
  retryAt: number | null;
  /** Ever connected during this page's lifetime (distinguishes "connecting" vs "lost"). */
  everOpened: boolean;
  brokerVersion: string | null;
  nodeId: string | null;
  /** Server contract differs from ours. */
  contractMismatch: number | null;
  /** Round-trip time of the last ping (ms). */
  rttMs: number | null;
  /** Seq gaps detected (diagnostics). */
  gaps: number;
  /** The server closed the session because its token was revoked or expired (WS close 4401);
   *  the client stopped reconnecting. Cleared by `restart()`. */
  revoked: { code: number; reason: string } | null;
}

export interface FrameCommit {
  /** Full view (topic → state). Untouched topics keep object identity. */
  view: Readonly<Record<string, unknown>>;
  /** Topics whose state changed in this frame. */
  changed: ReadonlySet<string>;
  /** Per-topic subscription errors (`{"type":"error"}`). */
  errors: Readonly<Record<string, string>>;
  /** Wall-clock time of the commit. */
  now: number;
}

export interface TelemetryOptions {
  url: () => string;
  socket?: SocketFactory;
  raf?: (cb: () => void) => unknown;
  now?: () => number;
  random?: () => number;
  isHidden?: () => boolean;
  /** Called once per frame with the new state. */
  onCommit: (c: FrameCommit) => void;
  onConn?: (c: ConnState) => void;
  /** Called when a connection attempt fails before ever opening (e.g. to probe for 401). */
  onConnectFailed?: (attempt: number) => void;
  /** Called once when the server closes the session with 4401 (token revoked / expired). */
  onRevoked?: (info: { code: number; reason: string }) => void;
  unsubscribeGraceMs?: number;
  backoffBaseMs?: number;
  backoffMaxMs?: number;
  pingIntervalMs?: number;
  /** Close the socket if nothing was received for this long. */
  idleTimeoutMs?: number;
  /** Live tail: number of sampled messages kept client-side per channel. */
  tailBufferSize?: number;
}

const OPEN = 1;

export class TelemetryClient {
  private readonly opts: Required<Omit<TelemetryOptions, 'onConn' | 'onConnectFailed' | 'onRevoked'>> &
    Pick<TelemetryOptions, 'onConn' | 'onConnectFailed' | 'onRevoked'>;
  private ws: MinimalSocket | null = null;
  private stopped = true;

  private readonly refs = new Map<string, number>();
  /** Topics the server should be sending. */
  private readonly active = new Set<string>();
  private readonly unsubTimers = new Map<string, ReturnType<typeof setTimeout>>();
  private readonly pendingSub = new Set<string>();
  private readonly pendingUnsub = new Set<string>();
  private sendScheduled = false;

  private readonly seq = new Map<string, number>();
  /** Topics that must receive a snapshot before deltas are accepted. */
  private readonly awaiting = new Set<string>();
  /** Raw protocol state per topic (what merge patches apply to). */
  private readonly raw = new Map<string, unknown>();
  private view: Record<string, unknown> = {};
  private errors: Record<string, string> = {};

  private queue: ServerMsg[] = [];
  private frameScheduled = false;
  private readonly changed = new Set<string>();
  private dirty = false;

  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private lastRecvAt = 0;
  private pingNonce = 0;
  private readonly pingSentAt = new Map<number, number>();
  private openedThisSocket = false;

  conn: ConnState = {
    status: 'idle',
    attempt: 0,
    retryAt: null,
    everOpened: false,
    brokerVersion: null,
    nodeId: null,
    contractMismatch: null,
    rttMs: null,
    gaps: 0,
    revoked: null,
  };

  constructor(opts: TelemetryOptions) {
    this.opts = {
      socket: (url) => new WebSocket(url) as unknown as MinimalSocket,
      raf: (cb) => requestAnimationFrame(cb),
      now: () => Date.now(),
      random: Math.random,
      isHidden: () => typeof document !== 'undefined' && document.hidden,
      unsubscribeGraceMs: 1500,
      backoffBaseMs: 500,
      backoffMaxMs: 30_000,
      pingIntervalMs: 15_000,
      idleTimeoutMs: 45_000,
      tailBufferSize: 2000,
      ...opts,
    };
  }

  // ─────────────────────────── lifecycle ───────────────────────────

  start(): void {
    if (!this.stopped) return;
    this.stopped = false;
    this.connect();
  }

  stop(): void {
    this.stopped = true;
    this.clearReconnect();
    this.stopPing();
    const ws = this.ws;
    this.ws = null;
    if (ws) {
      ws.onclose = ws.onmessage = ws.onerror = ws.onopen = null;
      ws.close(1000, 'client stop');
    }
    this.setConn({ status: 'stopped', retryAt: null });
  }

  /** Drop the current socket and connect again right away (e.g. token changed). */
  restart(): void {
    this.stop();
    this.conn.attempt = 0;
    this.conn.revoked = null;
    this.start();
  }

  /** Skip the backoff countdown. */
  retryNow(): void {
    if (this.stopped || this.conn.status === 'open' || this.conn.status === 'connecting') return;
    this.clearReconnect();
    this.connect();
  }

  private connect(): void {
    this.clearReconnect();
    this.openedThisSocket = false;
    this.setConn({ status: this.conn.everOpened ? 'reconnecting' : 'connecting', retryAt: null });
    let ws: MinimalSocket;
    try {
      ws = this.opts.socket(this.opts.url());
    } catch {
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;
    ws.onopen = () => this.handleOpen(ws);
    ws.onmessage = (ev) => this.handleRaw(ws, ev.data);
    ws.onerror = () => {
      /* onclose follows */
    };
    ws.onclose = (ev) => this.handleClose(ws, ev);
  }

  private handleOpen(ws: MinimalSocket): void {
    if (ws !== this.ws) return;
    this.openedThisSocket = true;
    this.lastRecvAt = this.opts.now();
    this.seq.clear();
    this.awaiting.clear();
    for (const t of this.active) this.awaiting.add(t);
    this.pendingSub.clear();
    this.pendingUnsub.clear();
    if (this.active.size > 0) this.sendNow({ op: 'subscribe', topics: [...this.active] });
    this.startPing();
    this.setConn({ status: 'open', attempt: 0, retryAt: null, everOpened: true });
  }

  private handleClose(ws: MinimalSocket, ev?: { code?: number; reason?: string }): void {
    if (ws !== this.ws) return;
    this.ws = null;
    this.stopPing();
    if (this.stopped) return;
    if (ev?.code === WS_CLOSE_TOKEN_REVOKED) {
      // Token revoked / expired: the same token will never be accepted again. Stop instead of
      // reconnecting forever; a new token restarts the client.
      this.stopped = true;
      this.clearReconnect();
      const revoked = { code: ev.code, reason: ev.reason ?? '' };
      this.setConn({ status: 'stopped', retryAt: null, revoked });
      this.opts.onRevoked?.(revoked);
      return;
    }
    const failedBeforeOpen = !this.openedThisSocket;
    this.scheduleReconnect();
    if (failedBeforeOpen) this.opts.onConnectFailed?.(this.conn.attempt);
  }

  private scheduleReconnect(): void {
    const attempt = this.conn.attempt;
    const base = Math.min(this.opts.backoffMaxMs, this.opts.backoffBaseMs * 2 ** attempt);
    const delay = Math.round(base * (0.8 + 0.4 * this.opts.random()));
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (!this.stopped) this.connect();
    }, delay);
    this.setConn({ status: 'reconnecting', attempt: attempt + 1, retryAt: this.opts.now() + delay });
  }

  private clearReconnect(): void {
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
  }

  private startPing(): void {
    this.stopPing();
    this.pingTimer = setInterval(() => {
      const ws = this.ws;
      if (!ws || ws.readyState !== OPEN) return;
      if (this.opts.now() - this.lastRecvAt > this.opts.idleTimeoutMs) {
        // Half-open connection: force a reconnect.
        ws.close(4000, 'idle timeout');
        this.handleClose(ws);
        return;
      }
      const nonce = ++this.pingNonce;
      this.pingSentAt.set(nonce, this.opts.now());
      this.sendNow({ op: 'ping', nonce });
    }, this.opts.pingIntervalMs);
  }

  private stopPing(): void {
    if (this.pingTimer) clearInterval(this.pingTimer);
    this.pingTimer = null;
    this.pingSentAt.clear();
  }

  private setConn(patch: Partial<ConnState>): void {
    this.conn = { ...this.conn, ...patch };
    this.opts.onConn?.(this.conn);
  }

  // ─────────────────────────── subscriptions ───────────────────────────

  /** Ref-counted subscription. Returns the release function. */
  subscribe(topic: string): () => void {
    const n = (this.refs.get(topic) ?? 0) + 1;
    this.refs.set(topic, n);
    if (n === 1) {
      const t = this.unsubTimers.get(topic);
      if (t) {
        // Re-acquired during the grace period: the server subscription is still live.
        clearTimeout(t);
        this.unsubTimers.delete(topic);
      } else if (!this.active.has(topic)) {
        this.active.add(topic);
        this.awaiting.add(topic);
        this.pendingUnsub.delete(topic);
        this.pendingSub.add(topic);
        this.scheduleSend();
      }
    }
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.release(topic);
    };
  }

  private release(topic: string): void {
    const n = (this.refs.get(topic) ?? 0) - 1;
    if (n > 0) {
      this.refs.set(topic, n);
      return;
    }
    this.refs.delete(topic);
    const drop = () => {
      this.unsubTimers.delete(topic);
      if (this.refs.has(topic)) return;
      this.active.delete(topic);
      this.awaiting.delete(topic);
      this.seq.delete(topic);
      this.raw.delete(topic);
      this.pendingSub.delete(topic);
      this.pendingUnsub.add(topic);
      if (topic in this.view || topic in this.errors) {
        const { [topic]: _v, ...view } = this.view;
        const { [topic]: _e, ...errors } = this.errors;
        this.view = view;
        this.errors = errors;
        this.markChanged(topic);
      }
      this.scheduleSend();
    };
    if (this.opts.unsubscribeGraceMs <= 0) drop();
    else this.unsubTimers.set(topic, setTimeout(drop, this.opts.unsubscribeGraceMs));
  }

  /** Topics with a live server subscription. */
  /**
   * Ask for a fresh snapshot of an active topic now (after a REST mutation whose effect should
   * show without waiting for the next delta). No-op for topics nobody is subscribed to.
   */
  refresh(topic: string): void {
    if (!this.active.has(topic)) return;
    this.awaiting.add(topic);
    this.seq.delete(topic);
    this.sendNow({ op: 'unsubscribe', topics: [topic] });
    this.sendNow({ op: 'subscribe', topics: [topic] });
  }

  activeTopics(): string[] {
    return [...this.active];
  }

  refCount(topic: string): number {
    return this.refs.get(topic) ?? 0;
  }

  private scheduleSend(): void {
    if (this.sendScheduled) return;
    this.sendScheduled = true;
    queueMicrotask(() => this.flushSends());
  }

  /** Coalesces subscribe/unsubscribe calls made in the same tick into single frames. */
  flushSends(): void {
    this.sendScheduled = false;
    if (!this.ws || this.ws.readyState !== OPEN) {
      // Will be covered by the subscribe-all on (re)open.
      this.pendingSub.clear();
      this.pendingUnsub.clear();
      return;
    }
    if (this.pendingUnsub.size) this.sendNow({ op: 'unsubscribe', topics: [...this.pendingUnsub] });
    if (this.pendingSub.size) this.sendNow({ op: 'subscribe', topics: [...this.pendingSub] });
    this.pendingSub.clear();
    this.pendingUnsub.clear();
  }

  private sendNow(msg: ClientMsg): void {
    const ws = this.ws;
    if (!ws || ws.readyState !== OPEN) return;
    try {
      ws.send(JSON.stringify(msg));
    } catch {
      /* socket closing; reconnect logic handles it */
    }
  }

  private resync(topic: string): void {
    this.awaiting.add(topic);
    this.seq.delete(topic);
    this.setConn({ gaps: this.conn.gaps + 1 });
    this.sendNow({ op: 'unsubscribe', topics: [topic] });
    this.sendNow({ op: 'subscribe', topics: [topic] });
  }

  // ─────────────────────────── inbound ───────────────────────────

  private handleRaw(ws: MinimalSocket, data: unknown): void {
    if (ws !== this.ws) return;
    this.lastRecvAt = this.opts.now();
    if (typeof data !== 'string') return;
    let msg: ServerMsg;
    try {
      msg = JSON.parse(data) as ServerMsg;
    } catch {
      return;
    }
    if (!msg || typeof msg !== 'object' || typeof (msg as { type?: unknown }).type !== 'string') return;
    this.queue.push(msg);
    this.scheduleFrame();
  }

  private scheduleFrame(): void {
    if (this.frameScheduled) return;
    this.frameScheduled = true;
    // rAF doesn't fire in background tabs; fall back to a slow timer so state stays fresh
    // and the queue can't grow without bound.
    if (this.opts.isHidden()) setTimeout(() => this.flushFrame(), 250);
    else this.opts.raf(() => this.flushFrame());
  }

  private markChanged(topic: string): void {
    this.changed.add(topic);
    this.dirty = true;
    this.scheduleFrame();
  }

  /** Apply everything received since the last frame and commit once. Public for tests. */
  flushFrame(): void {
    this.frameScheduled = false;
    const queue = this.queue;
    this.queue = [];
    for (const msg of queue) this.apply(msg);
    if (!this.dirty) return;
    this.dirty = false;
    const changed = new Set(this.changed);
    this.changed.clear();
    this.opts.onCommit({ view: this.view, changed, errors: this.errors, now: this.opts.now() });
  }

  private apply(msg: ServerMsg): void {
    switch (msg.type) {
      case 'hello':
        this.setConn({
          brokerVersion: msg.broker_version,
          nodeId: msg.node_id,
          contractMismatch: msg.contract_version === CONTRACT_VERSION ? null : msg.contract_version,
        });
        return;
      case 'pong': {
        if (msg.nonce != null) {
          const sent = this.pingSentAt.get(msg.nonce);
          if (sent != null) {
            this.pingSentAt.delete(msg.nonce);
            this.setConn({ rttMs: this.opts.now() - sent });
          }
        }
        return;
      }
      case 'error': {
        if (msg.topic) {
          this.errors = { ...this.errors, [msg.topic]: msg.message };
          this.awaiting.delete(msg.topic);
          this.markChanged(msg.topic);
        }
        return;
      }
      case 'snapshot': {
        const { topic } = msg;
        if (!this.active.has(topic)) return;
        this.awaiting.delete(topic);
        this.seq.set(topic, msg.seq);
        this.raw.set(topic, msg.data);
        if (topic in this.errors) {
          const { [topic]: _e, ...rest } = this.errors;
          this.errors = rest;
        }
        this.setView(topic, this.derive(topic, msg.data, true));
        return;
      }
      case 'delta': {
        const { topic } = msg;
        if (!this.active.has(topic) || this.awaiting.has(topic)) return;
        const last = this.seq.get(topic);
        if (last == null) return;
        if (msg.seq <= last) return; // duplicate / stale
        if (msg.seq !== last + 1) {
          this.resync(topic);
          return;
        }
        this.seq.set(topic, msg.seq);
        const next = applyMergePatch(this.raw.get(topic), msg.data);
        this.raw.set(topic, next);
        this.setView(topic, this.derive(topic, next, false, msg.data));
        return;
      }
    }
  }

  private setView(topic: string, value: unknown): void {
    if (this.view[topic] === value) return;
    this.view = { ...this.view, [topic]: value };
    this.markChanged(topic);
  }

  /**
   * Raw protocol state → view state. Only the live tail differs: its deltas *replace*
   * `messages` with the newest batch, so the view accumulates a bounded buffer.
   */
  private derive(topic: string, raw: unknown, snapshot: boolean, patch?: unknown): unknown {
    const parsed = parseTopic(topic);
    if (parsed?.kind !== 'tail' || !raw || typeof raw !== 'object') return raw;
    const state = raw as TailState;
    const batch: TailMessage[] = Array.isArray(state.messages) ? state.messages : [];
    if (snapshot) return { ...state, messages: batch.slice(-this.opts.tailBufferSize) };
    const prev = this.view[topic] as TailState | undefined;
    const prevMsgs = prev?.messages ?? [];
    // Only a delta that carries `messages` brings a new batch; otherwise keep the buffer.
    const hasBatch = !!patch && typeof patch === 'object' && 'messages' in patch;
    if (!hasBatch) return { ...state, messages: prevMsgs };
    const merged = prevMsgs.length + batch.length > this.opts.tailBufferSize
      ? prevMsgs.concat(batch).slice(-this.opts.tailBufferSize)
      : prevMsgs.concat(batch);
    return { ...state, messages: merged };
  }

  /** Snapshot received for this topic since (re)subscribing. */
  hasData(topic: string): boolean {
    return topic in this.view;
  }
}
