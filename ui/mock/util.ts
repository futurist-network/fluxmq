/** Deterministic hashing / noise helpers for the mock simulation. */

const U32 = 4294967296;

function u32(n: number): number {
  return ((Math.floor(n) % U32) + U32) % U32;
}

/** FNV-1a 32-bit. */
export function fnv(str: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

/** Mix several numbers (may exceed 2^32) into a 32-bit hash. */
export function mix(...ns: number[]): number {
  let h = 0x9e3779b9;
  for (const n of ns) {
    const lo = u32(n);
    const hi = u32(Math.floor(Math.abs(n) / U32));
    h = Math.imul(h ^ lo, 0x85ebca6b);
    h ^= h >>> 13;
    h = Math.imul(h ^ hi, 0xc2b2ae35);
    h ^= h >>> 16;
  }
  return h >>> 0;
}

/** Hash → [0, 1). */
export function unit(h: number): number {
  return (h >>> 0) / U32;
}

/** Seeded PRNG (mulberry32). */
export function prng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / U32;
  };
}

/** Smooth value noise in [-1, 1]. */
export function noise(seed: number, x: number): number {
  const i = Math.floor(x);
  const f = x - i;
  const a = unit(mix(seed, i)) * 2 - 1;
  const b = unit(mix(seed, i + 1)) * 2 - 1;
  const t = (1 - Math.cos(f * Math.PI)) / 2;
  return a * (1 - t) + b * t;
}

/** Daily traffic shape: ~0.8 at 03:00 UTC, 1.0 at 15:00 UTC. */
export function daily(t: number): number {
  const hour = (t / 3_600_000) % 24;
  return 0.8 + 0.2 * (0.5 - 0.5 * Math.cos((2 * Math.PI * (hour - 3)) / 24));
}

/** Average of `daily` over a day (for back-extrapolating offsets). */
export const DAILY_AVG = 0.9;

const CRC_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t[n] = c >>> 0;
  }
  return t;
})();

export function crc32(str: string): number {
  const bytes = Buffer.from(str, 'utf8');
  let c = 0xffffffff;
  for (const b of bytes) c = (CRC_TABLE[(c ^ b) & 0xff] ?? 0) ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

export function hex(rand: () => number, len: number): string {
  let s = '';
  for (let i = 0; i < len; i++) s += Math.floor(rand() * 16).toString(16);
  return s;
}

export function fmtCount(n: number): string {
  if (n >= 1e6) return `${(n / 1e6).toFixed(1).replace(/\.0$/, '')}M`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1).replace(/\.0$/, '')}k`;
  return String(Math.round(n));
}

export function clamp(v: number, lo: number, hi: number): number {
  return Math.min(hi, Math.max(lo, v));
}
