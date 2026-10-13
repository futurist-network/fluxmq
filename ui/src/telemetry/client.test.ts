import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { TelemetryClient, type FrameCommit, type MinimalSocket } from './client';

class FakeSocket implements MinimalSocket {
  readyState = 0;
  sent: unknown[] = [];
  closed = false;
  onopen: MinimalSocket['onopen'] = null;
  onclose: MinimalSocket['onclose'] = null;
  onerror: MinimalSocket['onerror'] = null;
  onmessage: MinimalSocket['onmessage'] = null;
  constructor(public url: string) {}
  send(data: string) {
    this.sent.push(JSON.parse(data));
  }
  close() {
    this.closed = true;
    this.readyState = 3;
  }
  // test helpers
  open() {
    this.readyState = 1;
    this.onopen?.({});
  }
  drop() {
    this.readyState = 3;
    this.onclose?.({ code: 1006 });
  }
  recv(msg: unknown) {
    this.onmessage?.({ data: JSON.stringify(msg) });
  }
  ops(op: string) {
    return this.sent.filter((m) => (m as { op: string }).op === op) as { op: string; topics: string[] }[];
  }
}

function setup(opts: Partial<ConstructorParameters<typeof TelemetryClient>[0]> = {}) {
  const sockets: FakeSocket[] = [];
  const rafQueue: (() => void)[] = [];
  const commits: FrameCommit[] = [];
  const client = new TelemetryClient({
    url: () => 'ws://test/ws',
    socket: (url) => {
      const s = new FakeSocket(url);
      sockets.push(s);
      return s;
    },
    raf: (cb) => rafQueue.push(cb),
    isHidden: () => false,
    random: () => 0.5,
    unsubscribeGraceMs: 0,
    onCommit: (c) => commits.push(c),
    ...opts,
  });
  const frame = () => {
    const q = rafQueue.splice(0);
    q.forEach((cb) => cb());
  };
  return { client, sockets, rafQueue, commits, frame, last: () => sockets[sockets.length - 1]! };
}

beforeEach(() => {
  vi.useFakeTimers();
});
afterEach(() => {
  vi.useRealTimers();
});

describe('TelemetryClient subscriptions', () => {
  it('ref-counts topics and coalesces subscribe frames', async () => {
    const { client, last } = setup();
    client.start();
    last().open();
    const r1 = client.subscribe('cluster.overview');
    const r2 = client.subscribe('cluster.overview');
    client.subscribe('alerts');
    await Promise.resolve();
    expect(last().ops('subscribe')).toEqual([{ op: 'subscribe', topics: ['cluster.overview', 'alerts'] }]);
    r1();
    await Promise.resolve();
    expect(last().ops('unsubscribe')).toHaveLength(0);
    r2();
    await Promise.resolve();
    expect(last().ops('unsubscribe')).toEqual([{ op: 'unsubscribe', topics: ['cluster.overview'] }]);
    expect(client.activeTopics()).toEqual(['alerts']);
  });

  it('keeps the server subscription during the grace period', async () => {
    const { client, last } = setup({ unsubscribeGraceMs: 1000 });
    client.start();
    last().open();
    const r = client.subscribe('nodes');
    await Promise.resolve();
    r();
    client.subscribe('nodes'); // e.g. StrictMode remount
    vi.advanceTimersByTime(2000);
    await Promise.resolve();
    expect(last().ops('unsubscribe')).toHaveLength(0);
    expect(last().ops('subscribe')).toHaveLength(1);
  });

  it('subscribes all active topics on open (topics requested before connect)', async () => {
    const { client, last } = setup();
    client.subscribe('a');
    client.subscribe('b');
    client.start();
    await Promise.resolve();
    last().open();
    expect(last().ops('subscribe')).toEqual([{ op: 'subscribe', topics: ['a', 'b'] }]);
  });
});

describe('TelemetryClient snapshot/delta', () => {
  it('applies snapshot then merge-patch deltas, batched once per frame', async () => {
    const { client, last, commits, frame } = setup();
    client.start();
    last().open();
    client.subscribe('cluster.overview');
    await Promise.resolve();
    const s = last();
    s.recv({ type: 'snapshot', topic: 'cluster.overview', seq: 1, ts: 0, data: { ingress_rate: 1, dlq_total: 0, nested: { a: 1 } } });
    s.recv({ type: 'delta', topic: 'cluster.overview', seq: 2, ts: 0, data: { ingress_rate: 2 } });
    s.recv({ type: 'delta', topic: 'cluster.overview', seq: 3, ts: 0, data: { dlq_total: 5, nested: { b: 2 } } });
    expect(commits).toHaveLength(0); // nothing until the frame
    frame();
    expect(commits).toHaveLength(1);
    expect(commits[0]!.view['cluster.overview']).toEqual({ ingress_rate: 2, dlq_total: 5, nested: { a: 1, b: 2 } });
    expect([...commits[0]!.changed]).toEqual(['cluster.overview']);
  });

  it('commits at most once per animation frame regardless of message count', async () => {
    const { client, last, commits, frame, rafQueue } = setup();
    client.start();
    last().open();
    client.subscribe('t');
    await Promise.resolve();
    last().recv({ type: 'snapshot', topic: 't', seq: 1, ts: 0, data: { n: 0 } });
    for (let i = 2; i < 100; i++) last().recv({ type: 'delta', topic: 't', seq: i, ts: 0, data: { n: i } });
    expect(rafQueue).toHaveLength(1);
    frame();
    expect(commits).toHaveLength(1);
    expect((commits[0]!.view.t as { n: number }).n).toBe(99);
  });

  it('resubscribes on a seq gap and ignores deltas until the next snapshot', async () => {
    const { client, last, commits, frame } = setup();
    client.start();
    last().open();
    client.subscribe('t');
    await Promise.resolve();
    const s = last();
    s.recv({ type: 'snapshot', topic: 't', seq: 1, ts: 0, data: { n: 1 } });
    s.recv({ type: 'delta', topic: 't', seq: 3, ts: 0, data: { n: 3 } }); // gap: 2 missing
    s.recv({ type: 'delta', topic: 't', seq: 4, ts: 0, data: { n: 4 } }); // dropped while awaiting
    frame();
    expect(commits.at(-1)!.view.t).toEqual({ n: 1 });
    expect(s.ops('unsubscribe').at(-1)).toEqual({ op: 'unsubscribe', topics: ['t'] });
    expect(s.ops('subscribe').at(-1)).toEqual({ op: 'subscribe', topics: ['t'] });
    expect(client.conn.gaps).toBe(1);
    s.recv({ type: 'snapshot', topic: 't', seq: 10, ts: 0, data: { n: 10 } });
    s.recv({ type: 'delta', topic: 't', seq: 11, ts: 0, data: { m: 1 } });
    frame();
    expect(commits.at(-1)!.view.t).toEqual({ n: 10, m: 1 });
  });

  it('ignores duplicate/stale deltas', async () => {
    const { client, last, commits, frame } = setup();
    client.start();
    last().open();
    client.subscribe('t');
    await Promise.resolve();
    last().recv({ type: 'snapshot', topic: 't', seq: 5, ts: 0, data: { n: 5 } });
    last().recv({ type: 'delta', topic: 't', seq: 5, ts: 0, data: { n: 99 } });
    frame();
    expect(commits.at(-1)!.view.t).toEqual({ n: 5 });
    expect(client.conn.gaps).toBe(0);
  });

  it('records subscription errors per topic', async () => {
    const { client, last, commits, frame } = setup();
    client.start();
    last().open();
    client.subscribe('channel.nope');
    await Promise.resolve();
    last().recv({ type: 'error', topic: 'channel.nope', message: 'unknown channel' });
    frame();
    expect(commits.at(-1)!.errors['channel.nope']).toBe('unknown channel');
  });

  it('accumulates live tail batches into a bounded buffer', async () => {
    const { client, last, commits, frame } = setup({ tailBufferSize: 3 });
    client.start();
    last().open();
    client.subscribe('channel.x.tail');
    await Promise.resolve();
    const m = (o: number) => ({ partition: 0, offset: o, key: null, timestamp_ms: o, headers: {}, size: 1, payload_preview: '', truncated: false });
    last().recv({ type: 'snapshot', topic: 'channel.x.tail', seq: 1, ts: 0, data: { sample_ratio: 1, shown_rate: 1, total_rate: 1, messages: [m(1)] } });
    last().recv({ type: 'delta', topic: 'channel.x.tail', seq: 2, ts: 0, data: { messages: [m(2), m(3)] } });
    last().recv({ type: 'delta', topic: 'channel.x.tail', seq: 3, ts: 0, data: { total_rate: 5 } }); // no batch
    last().recv({ type: 'delta', topic: 'channel.x.tail', seq: 4, ts: 0, data: { messages: [m(4)] } });
    frame();
    const v = commits.at(-1)!.view['channel.x.tail'] as { messages: { offset: number }[]; total_rate: number };
    expect(v.messages.map((x) => x.offset)).toEqual([2, 3, 4]);
    expect(v.total_rate).toBe(5);
  });
});

describe('TelemetryClient reconnect', () => {
  it('reconnects with exponential backoff and resubscribes active topics', async () => {
    const { client, sockets, last } = setup({ backoffBaseMs: 100, backoffMaxMs: 1000 });
    client.start();
    last().open();
    client.subscribe('cluster.overview');
    client.subscribe('alerts');
    await Promise.resolve();
    last().drop();
    expect(client.conn.status).toBe('reconnecting');
    expect(sockets).toHaveLength(1);
    vi.advanceTimersByTime(99);
    expect(sockets).toHaveLength(1);
    vi.advanceTimersByTime(1); // 100 ms * 2^0 (jitter factor 1.0 with random=0.5)
    expect(sockets).toHaveLength(2);
    last().drop(); // fails before open
    vi.advanceTimersByTime(199);
    expect(sockets).toHaveLength(2);
    vi.advanceTimersByTime(1); // 200 ms
    expect(sockets).toHaveLength(3);
    last().open();
    expect(client.conn.status).toBe('open');
    expect(client.conn.attempt).toBe(0);
    expect(last().ops('subscribe')).toEqual([{ op: 'subscribe', topics: ['cluster.overview', 'alerts'] }]);
  });

  it('caps backoff at the maximum', () => {
    const { client, last } = setup({ backoffBaseMs: 100, backoffMaxMs: 400 });
    client.start();
    for (let i = 0; i < 6; i++) {
      last().drop();
      vi.advanceTimersByTime(400);
    }
    const retryIn = (client.conn.retryAt ?? 0) - Date.now();
    expect(retryIn).toBeLessThanOrEqual(400);
  });

  it('pings and closes an idle socket', () => {
    const { client, sockets, last } = setup({ pingIntervalMs: 1000, idleTimeoutMs: 2500 });
    client.start();
    last().open();
    vi.advanceTimersByTime(1000);
    expect(last().ops('ping')).toHaveLength(1);
    vi.advanceTimersByTime(2000); // no messages for 3 s
    expect(sockets[0]!.closed).toBe(true);
    expect(client.conn.status).toBe('reconnecting');
  });

  it('measures RTT from pong', () => {
    const { client, last, frame } = setup({ pingIntervalMs: 1000 });
    client.start();
    last().open();
    vi.advanceTimersByTime(1000);
    vi.advanceTimersByTime(42);
    last().recv({ type: 'pong', nonce: 1, server_time_ms: 0 });
    frame();
    expect(client.conn.rttMs).toBe(42);
  });

  it('flags a contract version mismatch from hello', () => {
    const { client, last, frame } = setup();
    client.start();
    last().open();
    last().recv({ type: 'hello', contract_version: 2, broker_version: '9', node_id: 'n1', server_time_ms: 0 });
    frame();
    expect(client.conn.contractMismatch).toBe(2);
    expect(client.conn.brokerVersion).toBe('9');
  });
});

describe('TelemetryClient revoked session (v0.5, close 4401)', () => {
  it('stops reconnecting and reports the revocation until restart', () => {
    const revoked: unknown[] = [];
    const { client, sockets, last } = setup({ onRevoked: (r) => revoked.push(r) });
    client.start();
    last().open();
    last().readyState = 3;
    last().onclose?.({ code: 4401, reason: 'token revoked' });
    expect(client.conn.status).toBe('stopped');
    expect(client.conn.revoked).toEqual({ code: 4401, reason: 'token revoked' });
    expect(revoked).toEqual([{ code: 4401, reason: 'token revoked' }]);
    vi.advanceTimersByTime(120_000);
    expect(sockets).toHaveLength(1); // no reconnect attempts
    client.retryNow();
    expect(sockets).toHaveLength(1);
    client.restart(); // a new token
    expect(client.conn.revoked).toBeNull();
    expect(sockets).toHaveLength(2);
  });

  it('other close codes keep reconnecting', () => {
    const { client, sockets, last } = setup();
    client.start();
    last().open();
    last().drop();
    expect(client.conn.status).toBe('reconnecting');
    expect(client.conn.revoked).toBeNull();
    vi.advanceTimersByTime(1000);
    expect(sockets).toHaveLength(2);
  });
});
