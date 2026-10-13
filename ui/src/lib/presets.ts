import { t } from '../i18n';
import { fmtBytes } from './format';
import { channelNameError } from './channelName';

/** Preset labels are getters: they follow the console language at render time. */
export interface RetentionPreset {
  readonly label: string;
  secs: number | null;
}

const retention = (key: Parameters<typeof t>[0], secs: number | null): RetentionPreset => ({
  get label() {
    return t(key);
  },
  secs,
});

export const RETENTION_PRESETS: RetentionPreset[] = [
  retention('channels.preset.1h', 3600),
  retention('channels.preset.24h', 86_400),
  retention('channels.preset.3d', 3 * 86_400),
  retention('channels.preset.7d', 7 * 86_400),
  retention('channels.preset.30d', 30 * 86_400),
  retention('channels.preset.forever', null),
];

export interface BytesPreset {
  readonly label: string;
  bytes: number | null;
}

const bytes = (b: number | null): BytesPreset => ({
  get label() {
    return b == null ? t('channels.preset.unlimited') : fmtBytes(b);
  },
  bytes: b,
});

export const MAX_BYTES_PRESETS: BytesPreset[] = [bytes(null), bytes(1024 ** 3), bytes(10 * 1024 ** 3), bytes(100 * 1024 ** 3), bytes(1024 ** 4)];

/** Defaults from `RetryPolicy::default()` in model.rs. */
export const DEFAULT_RETRY = { max_attempts: 5, backoff_ms: [1_000, 10_000, 60_000, 300_000], ack_timeout_ms: 30_000 };

/** Parse "1s, 10s, 1m, 5m" / "500ms" / "1000" into milliseconds. Returns null on error. */
export function parseBackoffList(input: string): number[] | null {
  const parts = input
    .split(/[,\s→>]+/)
    .map((s) => s.trim())
    .filter(Boolean);
  if (!parts.length) return null;
  const out: number[] = [];
  for (const p of parts) {
    const m = /^(\d+(?:\.\d+)?)(ms|s|m|min|h)?$/i.exec(p);
    if (!m) return null;
    const v = parseFloat(m[1]!);
    const unit = (m[2] ?? 'ms').toLowerCase();
    const mult = unit === 'ms' ? 1 : unit === 's' ? 1000 : unit === 'h' ? 3_600_000 : 60_000;
    out.push(Math.round(v * mult));
  }
  return out;
}

export function formatBackoffList(ms: number[]): string {
  return ms
    .map((v) => (v % 3_600_000 === 0 ? `${v / 3_600_000}h` : v % 60_000 === 0 ? `${v / 60_000}m` : v % 1000 === 0 ? `${v / 1000}s` : `${v}ms`))
    .join(', ');
}

/** The create-wizard's editable fields, as the inputs hold them (strings for text inputs). */
export interface CreateForm {
  name: string;
  partitions: string;
  attempts: string;
  backoff: string;
  /** Seconds — the API takes `ack_timeout_ms`. */
  ackTimeout: string;
}

export interface CreateFormErrors {
  name: string | null;
  partitions: boolean;
  /** First invalid advanced field, if any. */
  advanced: 'backoff' | 'attempts' | 'ack' | null;
  backoffMs: number[] | null;
}

/** Client-side validation of the create wizard (the broker re-validates; 409 = name taken). */
export function validateCreateForm(f: CreateForm): CreateFormErrors {
  const partitionsN = Number(f.partitions);
  const backoffMs = parseBackoffList(f.backoff);
  const attemptsN = Number(f.attempts);
  const ackN = Number(f.ackTimeout);
  return {
    name: channelNameError(f.name),
    partitions: !Number.isInteger(partitionsN) || partitionsN < 1 || partitionsN > 1024,
    advanced: !backoffMs ? 'backoff' : !Number.isInteger(attemptsN) || attemptsN < 1 ? 'attempts' : !(ackN > 0) ? 'ack' : null,
    backoffMs,
  };
}
