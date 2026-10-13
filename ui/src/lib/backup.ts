/** Backups screen: config draft ⇄ `BackupConfig`, presets, savings/progress maths, restore command. */
import type { BackupConfig, BackupInfo, BackupTarget } from '../api/types';
import { t } from '../i18n';
import { fmtBackoff } from './format';
import { keepsStored, newSecret, secretFromRead, secretMissing, secretToRequest, type SecretField } from './secrets';

export type S3Preset = 'aws' | 'minio' | 'r2';

/** `hint` is a getter: read in the current console language. */
export const S3_PRESETS: Record<S3Preset, { label: string; endpoint: string; region: string; path_style: boolean; readonly hint: string }> = {
  aws: {
    label: 'AWS S3',
    endpoint: '',
    region: 'eu-central-1',
    path_style: false,
    get hint() {
      return t('backups.preset.awsHint');
    },
  },
  minio: {
    label: 'MinIO',
    endpoint: 'http://minio:9000',
    region: 'us-east-1',
    path_style: true,
    get hint() {
      return t('backups.preset.minioHint');
    },
  },
  r2: {
    label: 'Cloudflare R2',
    endpoint: 'https://<account_id>.r2.cloudflarestorage.com',
    region: 'auto',
    path_style: false,
    get hint() {
      return t('backups.preset.r2Hint');
    },
  },
};

export interface BackupDraft {
  enabled: boolean;
  kind: 's3' | 'filesystem';
  endpoint: string;
  region: string;
  bucket: string;
  prefix: string;
  access_key_id: string;
  secret_access_key: SecretField;
  path_style: boolean;
  path: string;
  interval_secs: number;
  keep_last: string;
  keep_days: string;
  include_messages: boolean;
  encrypt: boolean;
  encryption_key: SecretField;
}

const preset = (value: number, label: () => string) => ({
  value,
  get label() {
    return label();
  },
});

export const INTERVAL_PRESETS: { value: number; label: string }[] = [
  preset(900, () => fmtBackoff(900_000)),
  preset(3_600, () => t('backups.interval.hourly')),
  preset(21_600, () => fmtBackoff(21_600_000)),
  preset(86_400, () => t('backups.interval.daily')),
];

export function emptyBackupDraft(): BackupDraft {
  return {
    enabled: true,
    kind: 's3',
    endpoint: '',
    region: S3_PRESETS.aws.region,
    bucket: '',
    prefix: 'fluxmq/',
    access_key_id: '',
    secret_access_key: newSecret(),
    path_style: false,
    path: '/var/backups/fluxmq',
    interval_secs: 3_600,
    keep_last: '24',
    keep_days: '7',
    include_messages: true,
    encrypt: false,
    encryption_key: newSecret(),
  };
}

export function configToDraft(c: BackupConfig | null): BackupDraft {
  const d = emptyBackupDraft();
  if (!c) return d;
  const base = { ...d, enabled: c.enabled, interval_secs: c.interval_secs, keep_last: String(c.keep_last), keep_days: String(c.keep_days), include_messages: c.include_messages, encrypt: c.encryption_key != null && c.encryption_key !== '', encryption_key: secretFromRead(c.encryption_key) };
  const tg = c.target;
  if (tg.type === 'filesystem') return { ...base, kind: 'filesystem', path: tg.path };
  return { ...base, kind: 's3', endpoint: tg.endpoint ?? '', region: tg.region, bucket: tg.bucket, prefix: tg.prefix, access_key_id: tg.access_key_id, secret_access_key: secretFromRead(tg.secret_access_key), path_style: tg.path_style };
}

export function draftToTarget(d: BackupDraft): BackupTarget {
  if (d.kind === 'filesystem') return { type: 'filesystem', path: d.path.trim() };
  return {
    type: 's3',
    endpoint: d.endpoint.trim() || null,
    region: d.region.trim(),
    bucket: d.bucket.trim(),
    prefix: d.prefix.trim(),
    access_key_id: d.access_key_id.trim(),
    secret_access_key: secretToRequest(d.secret_access_key),
    path_style: d.path_style,
  };
}

export function draftToConfig(d: BackupDraft): BackupConfig {
  return {
    enabled: d.enabled,
    target: draftToTarget(d),
    interval_secs: d.interval_secs,
    keep_last: Number(d.keep_last),
    keep_days: Number(d.keep_days),
    include_messages: d.include_messages,
    // Encryption off → "" removes a stored key (uniform secret rule); never configured → null.
    encryption_key: d.encrypt ? secretToRequest(d.encryption_key) : d.encryption_key.stored ? '' : null,
  };
}

export type BackupErrors = Partial<Record<'endpoint' | 'region' | 'bucket' | 'access_key_id' | 'secret_access_key' | 'path' | 'interval_secs' | 'keep_last' | 'keep_days' | 'encryption_key', string>>;

/** base64 of exactly 32 bytes. */
export function validKey(k: string): boolean {
  if (!/^[A-Za-z0-9+/]{43}=$/.test(k.trim())) return false;
  try {
    return atob(k.trim()).length === 32;
  } catch {
    return false;
  }
}

export function validateBackupDraft(d: BackupDraft): BackupErrors {
  const e: BackupErrors = {};
  if (d.kind === 's3') {
    if (d.endpoint.trim()) {
      if (d.endpoint.includes('<')) e.endpoint = t('backups.err.placeholder');
      else {
        try {
          const u = new URL(d.endpoint.trim());
          if (u.protocol !== 'https:' && u.protocol !== 'http:') e.endpoint = t('backups.err.httpUrl');
        } catch {
          e.endpoint = t('backups.err.httpUrl');
        }
      }
    }
    if (!d.region.trim()) e.region = t('backups.err.region');
    if (!d.bucket.trim()) e.bucket = t('backups.err.bucketRequired');
    else if (!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(d.bucket.trim())) e.bucket = t('backups.err.bucketFormat');
    if (!d.access_key_id.trim()) e.access_key_id = t('backups.err.accessKeyId');
    if (secretMissing(d.secret_access_key)) e.secret_access_key = t('backups.err.secretAccessKey');
  } else if (!d.path.trim().startsWith('/')) e.path = t('backups.err.path');
  if (!Number.isInteger(d.interval_secs) || d.interval_secs < 300) e.interval_secs = t('backups.err.interval');
  if (!/^\d+$/.test(d.keep_last) || Number(d.keep_last) < 1) e.keep_last = t('backups.err.keepLast');
  if (!/^\d+$/.test(d.keep_days)) e.keep_days = t('backups.err.keepDays');
  if (d.encrypt && !keepsStored(d.encryption_key)) {
    if (!d.encryption_key.value.trim()) e.encryption_key = t('backups.err.keyMissing');
    else if (!validKey(d.encryption_key.value)) e.encryption_key = t('backups.err.keyFormat');
  }
  return e;
}

/** Random AES-256 key, base64 (32 bytes). */
export function generateKey(): string {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  let s = '';
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s);
}

export interface Savings {
  /** Share of segments reused from earlier backups (0..1). */
  reusedShare: number;
  /** Bytes not uploaded thanks to reuse. */
  bytesSaved: number;
  /** Share of bytes that had to be uploaded (0..1). */
  uploadedShare: number;
}

export function savings(b: Pick<BackupInfo, 'segments_uploaded' | 'segments_reused' | 'bytes_uploaded' | 'total_bytes'>): Savings {
  const segs = b.segments_uploaded + b.segments_reused;
  const reusedShare = segs > 0 ? b.segments_reused / segs : 0;
  const bytesSaved = Math.max(0, b.total_bytes - b.bytes_uploaded);
  const uploadedShare = b.total_bytes > 0 ? Math.min(1, b.bytes_uploaded / b.total_bytes) : segs > 0 ? 1 - reusedShare : 0;
  return { reusedShare, bytesSaved, uploadedShare };
}

/** Running progress 0..1, or null when unknown (total not computed yet). */
export function progress(b: Pick<BackupInfo, 'status' | 'progress_bytes' | 'total_bytes'>): number | null {
  if (b.status !== 'running') return b.status === 'succeeded' ? 1 : null;
  if (!b.total_bytes) return null;
  return Math.max(0, Math.min(1, b.progress_bytes / b.total_bytes));
}

export function backupDuration(b: Pick<BackupInfo, 'started_at_ms' | 'finished_at_ms'>, now: number): number {
  return Math.max(0, (b.finished_at_ms ?? now) - b.started_at_ms);
}

const sh = (s: string) => (/^[A-Za-z0-9_./:@%+=,-]+$/.test(s) ? s : `'${s.replace(/'/g, `'\\''`)}'`);

/**
 * The exact `fluxmq restore` invocation for a backup (design §3 / §6). Credentials and the
 * encryption key come from the environment so they stay out of shell history and process lists.
 */
export function restoreCommand(cfg: BackupConfig, backupId: string, dataDir = '/var/lib/fluxmq-restored'): string {
  const lines: string[] = [];
  const t = cfg.target;
  if (t.type === 's3') {
    lines.push(`export AWS_ACCESS_KEY_ID=${sh(t.access_key_id || '<access-key-id>')}`);
    lines.push(`export AWS_SECRET_ACCESS_KEY='<secret-access-key>'`);
  }
  const encrypted = cfg.encryption_key != null && cfg.encryption_key !== '';
  if (encrypted) lines.push(`export FLUXMQ_BACKUP_ENCRYPTION_KEY='<your encryption key>'   # or --encryption-key`);
  const args: string[] = ['fluxmq restore'];
  if (t.type === 's3') {
    if (t.endpoint) args.push(`--s3-endpoint ${sh(t.endpoint)}`);
    args.push(`--s3-region ${sh(t.region)}`, `--s3-bucket ${sh(t.bucket)}`);
    if (t.prefix) args.push(`--s3-prefix ${sh(t.prefix)}`);
    if (t.path_style) args.push('--s3-path-style');
  } else {
    args.push(`--fs-path ${sh(t.path)}`);
  }
  args.push(`--backup ${sh(backupId)}`, `--data-dir ${sh(dataDir)}`);
  lines.push(args.join(' \\\n  '));
  return lines.join('\n');
}

/** Same target flags for `fluxmq backup list`. */
export function listCommand(cfg: BackupConfig): string {
  return restoreCommand(cfg, 'x')
    .replace('fluxmq restore', 'fluxmq backup list')
    .replace(/ \\\n {2}--backup x \\\n {2}--data-dir \S+$/, '')
    .split('\n')
    .filter((l) => !l.includes('ENCRYPTION_KEY'))
    .join('\n');
}
