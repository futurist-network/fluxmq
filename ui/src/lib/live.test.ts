import { describe, expect, it } from 'vitest';
import type { LiveNodeStatus } from '../api/types';
import { channelSubscriptions, closeReason, liveQuery, liveSnippet, liveSubprotocols, liveTotals, parseLiveEvent, payloadPreview, TOKEN_PLACEHOLDER, wsOrigin } from './live';
import { routeSection } from './motion';

const node = (id: string, over: Partial<LiveNodeStatus> = {}): LiveNodeStatus => ({
  node: id,
  limits: { max_connections: 10_000, max_connections_per_token: 1000, max_connections_per_channel: 10_000, max_subscriptions: 16, max_msgs_per_sec: 10_000, max_bytes_per_sec: 16 << 20, max_payload_bytes: 65_536, max_batch: 256, send_buffer: 64, send_timeout_secs: 10, heartbeat_secs: 15, idle_timeout_secs: 60, fanout_buffer: 1024, allowed_origins: [] },
  connections_ws: 0,
  connections_sse: 0,
  subscriptions_catchup: 0,
  subscriptions_live: 0,
  feeds: 0,
  messages_sent: 0,
  bytes_sent: 0,
  slow_disconnects: 0,
  gaps: 0,
  rejections: [],
  channels: [],
  ...over,
});

describe('live subscription URLs and code', () => {
  it('builds the query: latest is the default, keys are trimmed, filter is encoded', () => {
    expect(liveQuery({ from: 'latest', key: '', filter: '' })).toBe('');
    expect(liveQuery({ from: 'earliest', key: ' a , b ,', filter: ' amount >= 100 ' })).toBe('?from=earliest&key=a%2Cb&filter=amount+%3E%3D+100');
  });

  it('passes the token as the second WebSocket subprotocol, never in the URL', () => {
    expect(liveSubprotocols(null)).toEqual(['fluxmq.live.v1']);
    expect(liveSubprotocols('fmq_ab_cd')).toEqual(['fluxmq.live.v1', 'fluxmq.token.fmq_ab_cd']);
    expect(wsOrigin('https://mq.example.com')).toBe('wss://mq.example.com');
    expect(wsOrigin('http://127.0.0.1:7421')).toBe('ws://127.0.0.1:7421');
  });

  it('snippets carry the channel and options, and only a token placeholder', () => {
    const o = { from: 'earliest' as const, key: 'user_42', filter: '' };
    const sse = liveSnippet('sse', 'https://mq.example.com', 'orders', o);
    expect(sse).toContain("new EventSource('https://mq.example.com/api/channels/orders/events?from=earliest&key=user_42&access_token=' + TOKEN)");
    const ws = liveSnippet('ws', 'https://mq.example.com', 'orders', o);
    expect(ws).toContain("'wss://mq.example.com/api/channels/orders/live?from=earliest&key=user_42'");
    expect(ws).toContain("url + '&cursor='");
    expect(ws).toContain(`'fluxmq.token.' + ${TOKEN_PLACEHOLDER}`);
    expect(liveSnippet('ws', 'http://h', 'a b', { from: 'latest', key: '', filter: '' })).toContain("'ws://h/api/channels/a%20b/live'");
    expect(liveSnippet('curl', 'http://h', 'orders', o)).toContain('Authorization: Bearer $TOKEN');
  });
});

describe('live events', () => {
  it('parses events and ignores junk', () => {
    expect(parseLiveEvent('{"type":"hello","sub":"0"}')?.type).toBe('hello');
    expect(parseLiveEvent('nope')).toBeNull();
    expect(parseLiveEvent('{"a":1}')).toBeNull();
  });

  it('previews text payloads on one line and describes binary ones', () => {
    const m = { partition: 0, offset: 1, key: null, headers: {}, timestamp: 0, encoding: 'utf8' as const, payload: '{\n  "a": 1\n}', size: 12 };
    expect(payloadPreview(m)).toBe('{ "a": 1 }');
    expect(payloadPreview({ ...m, payload: 'x'.repeat(200) }, 10)).toBe(`${'x'.repeat(10)}…`);
    expect(payloadPreview({ ...m, encoding: 'base64', payload: 'AAE=', size: 2 })).toMatch(/2/);
  });

  it('explains the gateway close codes', () => {
    expect(closeReason(4401)).toMatch(/revoked/);
    expect(closeReason(4008)).toMatch(/slow/);
    expect(closeReason(1006)).toBeNull();
  });
});

describe('live status totals', () => {
  it('sums nodes and merges channels and refusals', () => {
    const tot = liveTotals([
      node('n1', { connections_ws: 3, connections_sse: 1, messages_sent: 10, channels: [{ channel: 'a', subscriptions: 2 }, { channel: 'b', subscriptions: 1 }], rejections: [{ reason: 'origin', count: 1 }] }),
      node('n2', { connections_ws: 2, messages_sent: 5, channels: [{ channel: 'b', subscriptions: 4 }], rejections: [{ reason: 'origin', count: 2 }, { reason: 'token_limit', count: 1 }] }),
    ]);
    expect([tot.connectionsWs, tot.connectionsSse, tot.messagesSent]).toEqual([5, 1, 15]);
    expect(tot.channels).toEqual([
      { channel: 'b', subscriptions: 5 },
      { channel: 'a', subscriptions: 2 },
    ]);
    expect(tot.rejections).toEqual([
      { reason: 'origin', count: 3 },
      { reason: 'token_limit', count: 1 },
    ]);
    expect(channelSubscriptions([node('n1', { channels: [{ channel: 'b', subscriptions: 1 }] }), node('n2', { channels: [{ channel: 'b', subscriptions: 4 }] })], 'b')).toBe(5);
  });
});

describe('route sections (tab switches are not screen changes)', () => {
  it('groups tabs of one screen', () => {
    expect(routeSection('/channels/orders')).toBe('/channels/orders');
    expect(routeSection('/channels/orders/messages')).toBe('/channels/orders');
    expect(routeSection('/channels/billing/live')).toBe('/channels/billing');
    expect(routeSection('/settings')).toBe('/settings');
    expect(routeSection('/settings/tokens')).toBe('/settings');
    expect(routeSection('/channels')).toBe('/channels');
    expect(routeSection('/webhooks/abc')).toBe('/webhooks/abc');
  });
});
