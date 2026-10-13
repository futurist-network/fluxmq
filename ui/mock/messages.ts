/** Deterministic message generation: the same (channel, partition, offset) always yields the same message. */
import type { JsonPayload, MessageView, TailMessage } from '../src/api/types';
import { fnv, hex, mix, prng, unit } from './util';

export type GenKind = 'orders' | 'payments' | 'email' | 'notifications' | 'inventory' | 'audit' | 'generic';

export function genKindFor(name: string): GenKind {
  switch (name) {
    case 'orders.events':
      return 'orders';
    case 'payments.events':
      return 'payments';
    case 'email':
      return 'email';
    case 'notifications':
      return 'notifications';
    case 'inventory.updates':
      return 'inventory';
    case 'audit.logs':
      return 'audit';
    default:
      return 'generic';
  }
}

const ORDER_BLOCK = 2500;
const PAY_BLOCK = 1500;

function orderNum(P: number, p: number, o: number): { num: number; stage: number } {
  const i = o % (ORDER_BLOCK * 3);
  return { num: 8000 + (i % ORDER_BLOCK) * P + p, stage: Math.floor(i / ORDER_BLOCK) };
}

function customerNum(P: number, p: number, o: number): { num: number; stage: number } {
  const i = o % (PAY_BLOCK * 2);
  return { num: 1000 + (i % PAY_BLOCK) * P + p, stage: Math.floor(i / PAY_BLOCK) };
}

/** Cheap key computation (used for scanning by key without materializing payloads). */
export function keyOf(kind: GenKind, name: string, P: number, p: number, o: number): string | null {
  switch (kind) {
    case 'orders':
      return `ord_${orderNum(P, p, o).num}`;
    case 'payments':
      return `cus_${customerNum(P, p, o).num}`;
    case 'email':
      return null;
    case 'notifications':
      return `usr_${10000 + (mix(p, o, 3) % 90000)}`;
    case 'inventory':
      return `SKU-${1000 + ((o * 7 + p * 131) % 4000)}`;
    case 'audit':
      return `svc-${['billing', 'auth', 'admin', 'search'][mix(o, p) % 4]}`;
    case 'generic':
      return o % 2 === 0 ? null : `k_${mix(fnv(name), p, o) % 1000}`;
  }
}

/** If the generator puts a key on a known partition, return it (lets key search skip partitions). */
export function keyPartition(kind: GenKind, key: string, P: number): number | null {
  const m = /^(ord|cus)_(\d+)$/.exec(key);
  if (!m) return null;
  const n = Number(m[2]);
  if (kind === 'orders' && m[1] === 'ord' && n >= 8000) return (n - 8000) % P;
  if (kind === 'payments' && m[1] === 'cus' && n >= 1000) return (n - 1000) % P;
  return null;
}

const CURRENCIES = ['EUR', 'USD', 'GBP', 'PLN'];
const SKUS = ['SKU-1042', 'SKU-2210', 'SKU-3381', 'SKU-0907', 'SKU-5150', 'SKU-7733'];
const TEMPLATES = ['order_confirmation', 'shipping_update', 'password_reset', 'invoice_ready'];
const COUNTRIES = ['DE', 'NL', 'FR', 'PL', 'ES', 'SE'];

function money(r: () => number, lo: number, hi: number): number {
  return Math.round((lo + r() * (hi - lo)) * 100) / 100;
}

interface Built {
  type: string;
  producer: string;
  body: unknown;
}

function build(kind: GenKind, name: string, P: number, p: number, o: number, ts: number, r: () => number): Built {
  switch (kind) {
    case 'orders': {
      const { num, stage } = orderNum(P, p, o);
      const cancelled = stage === 2 && mix(num, 17) % 17 === 0;
      const type = ['order.created', 'order.paid', cancelled ? 'order.cancelled' : 'order.shipped'][stage] ?? 'order.created';
      const items = Array.from({ length: 1 + (mix(num, 5) % 3) }, (_, i) => ({
        sku: SKUS[(mix(num, i) % SKUS.length)] ?? 'SKU-1042',
        qty: 1 + (mix(num, i, 9) % 3),
        price: Math.round(((mix(num, i, 11) % 9000) / 100 + 5) * 100) / 100,
      }));
      const total = Math.round(items.reduce((s, it) => s + it.qty * it.price, 0) * 100) / 100;
      const body: Record<string, unknown> = {
        order_id: num,
        customer_id: `cus_${1000 + (mix(num, 2) % 18000)}`,
        status: type.split('.')[1],
        total,
        currency: CURRENCIES[mix(num, 3) % CURRENCIES.length],
        items,
        shipping: { country: COUNTRIES[mix(num, 4) % COUNTRIES.length], method: mix(num, 6) % 2 ? 'express' : 'standard' },
        occurred_at: new Date(ts).toISOString(),
      };
      if (stage === 1) body.payment = { provider: 'stripe', intent: `pi_${hex(r, 16)}` };
      if (stage === 2 && !cancelled) body.tracking = { carrier: 'DHL', number: `JD0146${mix(num, 8) % 1e8}` };
      if (cancelled) body.reason = 'customer_request';
      return { type, producer: stage === 0 ? 'checkout-api@v2.14.1' : 'order-service@v3.2.0', body };
    }
    case 'payments': {
      const { num, stage } = customerNum(P, p, o);
      const failed = stage === 1 && mix(num, o, 23) % 23 === 0;
      const type = stage === 0 ? 'payment.authorized' : failed ? 'payment.failed' : 'payment.captured';
      const body: Record<string, unknown> = {
        payment_id: `pay_${hex(r, 14)}`,
        customer_id: `cus_${num}`,
        order_id: 8000 + (mix(num, Math.floor(o / 16)) % 20000),
        amount: money(r, 9, 640),
        currency: CURRENCIES[mix(num, 3) % CURRENCIES.length],
        method: ['card', 'sepa_debit', 'paypal', 'apple_pay'][mix(num, 1) % 4],
        risk_score: Math.round(r() * 100) / 100,
      };
      if (failed) body.decline_code = ['insufficient_funds', 'do_not_honor', 'expired_card'][mix(num, 2) % 3];
      return { type, producer: 'payments-gw@v1.9.4', body };
    }
    case 'email':
      return {
        type: 'email.send',
        producer: 'notification-svc@v4.0.2',
        body: {
          job_id: `job_${hex(r, 12)}`,
          template: TEMPLATES[mix(o, 1) % TEMPLATES.length],
          to: `customer${mix(o, 2) % 50000}@example.com`,
          locale: ['en-GB', 'de-DE', 'nl-NL', 'fr-FR'][mix(o, 3) % 4],
          order_id: 8000 + (mix(o, 4) % 20000),
          priority: mix(o, 5) % 10 === 0 ? 'high' : 'normal',
        },
      };
    case 'notifications':
      return {
        type: 'push.sent',
        producer: 'push-router@v2.1.0',
        body: {
          user_id: keyOf(kind, name, P, p, o),
          platform: mix(o, p) % 3 === 0 ? 'android' : 'ios',
          title: ['Your order has shipped', 'Payment received', 'Back in stock'][mix(o, 7) % 3],
          campaign: null,
          delivered: r() > 0.03,
        },
      };
    case 'inventory':
      return {
        type: 'stock.adjusted',
        producer: 'wms-sync@v0.9.7',
        body: {
          sku: keyOf(kind, name, P, p, o),
          warehouse: ['AMS-1', 'FRA-2', 'WAW-1'][mix(o, 2) % 3],
          delta: (mix(o, 3) % 20) - 10,
          on_hand: mix(o, 4) % 900,
          reason: ['sale', 'restock', 'return', 'audit'][mix(o, 5) % 4],
        },
      };
    case 'audit':
      return {
        type: 'audit.record',
        producer: 'audit-shipper@v1.0.0',
        body: {
          actor: keyOf(kind, name, P, p, o),
          action: ['login', 'config.update', 'token.rotate', 'export'][mix(o, 1) % 4],
          ip: `10.0.${mix(o, 2) % 255}.${mix(o, 3) % 255}`,
          ok: r() > 0.02,
        },
      };
    case 'generic':
      return {
        type: `${name}.event`,
        producer: 'producer@v1.0.0',
        body: { seq: o, partition: p, value: Math.round(r() * 1000) / 10 },
      };
  }
}

/** Generate the stored message at (partition, offset). */
export function genMessage(kind: GenKind, name: string, P: number, p: number, o: number, ts: number): MessageView {
  const seed = mix(fnv(name), p, o);
  const r = prng(seed);
  const key = keyOf(kind, name, P, p, o);
  const binary = unit(mix(seed, 7)) < 0.012;
  const built = build(kind, name, P, p, o, ts, r);
  const headers: Record<string, string> = {
    'content-type': binary ? 'application/x-protobuf' : 'application/json',
    'flux-producer': built.producer,
    'flux-trace-id': hex(r, 32),
    'flux-type': binary ? `${built.type}.v1` : built.type,
  };
  let payload: JsonPayload;
  let size: number;
  if (binary) {
    const len = 48 + Math.floor(r() * 160);
    const buf = Buffer.alloc(len);
    for (let i = 0; i < len; i++) buf[i] = Math.floor(r() * 256);
    // a few printable fragments so the hex view looks like a real protobuf
    buf.write(String(key ?? built.type), 2, 'utf8');
    payload = { encoding: 'base64', data: buf.toString('base64') };
    size = len;
  } else {
    const text = JSON.stringify(built.body);
    payload = { encoding: 'utf8', data: text };
    size = Buffer.byteLength(text, 'utf8');
  }
  return { partition: p, offset: o, key, headers, timestamp_ms: ts, size, payload };
}

export function payloadBytes(payload: JsonPayload): Buffer {
  return payload.encoding === 'utf8' ? Buffer.from(payload.data, 'utf8') : Buffer.from(payload.data, 'base64');
}

export function toTail(m: MessageView): TailMessage {
  const text = m.payload.encoding === 'utf8' ? m.payload.data : payloadBytes(m.payload).toString('utf8');
  const truncated = text.length > 4096;
  return {
    partition: m.partition,
    offset: m.offset,
    key: m.key,
    timestamp_ms: m.timestamp_ms,
    headers: m.headers,
    size: m.size,
    payload_preview: truncated ? text.slice(0, 4096) : text,
    truncated,
  };
}
