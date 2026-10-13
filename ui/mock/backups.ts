/**
 * Mock backups — contract of crates/fluxmq-proto/src/backup.rs: config (null when unset,
 * secrets redacted, uniform update rule), target test, scheduler, a running backup that
 * progresses, manual runs, history with incremental segment reuse, retention, delete.
 */
import type { BackupConfig, BackupInfo, BackupManifest, BackupOverview, BackupTarget, TestResult } from '../src/api/types';
import { REDACTED } from '../src/api/types';
import { mergeSecret } from './notify';
import { ApiErr, EMPTY, type Sim } from './sim';

/** MOCK_OLD_NODE=1: one cluster node predates the internal backup API → runs are refused. */
const OLD_NODE = process.env.MOCK_OLD_NODE === '1';

const H = 3_600_000;
const GB = 1024 ** 3;
const bad = (m: string): never => {
  throw new ApiErr(400, 'bad_request', m);
};
const isObj = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);

function backupId(ts: number): string {
  const d = new Date(ts).toISOString().replace(/[-:]/g, '').replace(/\.\d+Z$/, 'Z');
  return `${d}-${Math.random().toString(36).slice(2, 6)}`;
}

interface Run {
  info: BackupInfo;
  durationMs: number;
  /** Segments this backup covers (reused + uploaded at the end). */
  segs: number;
}

export class Backups {
  private config: BackupConfig | null = null;
  private history: BackupInfo[] = [];
  private running: Run | null = null;
  private nextRun: number | null = null;

  constructor(private sim: Sim) {
    if (!EMPTY) this.seed();
    sim.onTick.push((now) => this.tick(now));
  }

  private seed(): void {
    const now = Date.now();
    this.config = {
      enabled: true,
      target: { type: 's3', endpoint: 'https://4f1c2a9be0d34.r2.cloudflarestorage.com', region: 'auto', bucket: 'fluxmq-backups', prefix: 'prod-eu-1/', access_key_id: 'b1f9e0c4a7d2', secret_access_key: 'r2-secret-mock', path_style: false },
      interval_secs: 3_600,
      keep_last: 24,
      keep_days: 7,
      include_messages: true,
      encryption_key: 'q3J0b0p5Y2Z4dW1rZ2V5LWRlbW8tMzItYnl0ZXMhIUE=',
    };
    const total0 = 41.8 * GB;
    for (let i = 14; i >= 1; i--) {
      const started = now - i * H - 4 * 60_000;
      const full = i === 14;
      const failed = i === 5;
      const segs = 1180 + (14 - i) * 9;
      const up = full ? segs : 6 + ((i * 7) % 11);
      const total = total0 + (14 - i) * 0.21 * GB;
      const bytesUp = full ? total : up * 96 * 1024 * 1024 + 0.4 * GB;
      this.history.push({
        id: backupId(started),
        status: failed ? 'failed' : 'succeeded',
        started_at_ms: started,
        finished_at_ms: started + (full ? 41 * 60_000 : failed ? 95_000 : 140_000 + ((i * 13) % 60) * 1000),
        trigger: i === 9 ? 'manual' : 'scheduled',
        channels: this.sim.channels.size,
        partitions: [...this.sim.channels.values()].reduce((a, c) => a + c.config.partitions, 0),
        messages: Math.round(total / 470),
        bytes_uploaded: failed ? 1.1 * GB : Math.round(bytesUp),
        segments_uploaded: failed ? 4 : up,
        segments_reused: full || failed ? (failed ? segs - 40 : 0) : segs - up,
        total_bytes: Math.round(total),
        progress_bytes: failed ? Math.round(0.31 * total) : Math.round(total),
        error: failed ? 'S3 PutObject segments/orders.events/3/18874112-9c1f2e04.log: 503 SlowDown — rate limited by the bucket (retried 5×)' : null,
      });
    }
    this.history.sort((a, b) => b.started_at_ms - a.started_at_ms);
    // A scheduled backup started 40 s ago; it progresses for ~20 minutes (a big hourly run).
    this.start('scheduled', now - 40_000, 20 * 60_000);
  }

  private start(trigger: string, at: number, durationMs: number): BackupInfo {
    const last = this.history.find((b) => b.status === 'succeeded');
    const total = Math.round((last?.total_bytes ?? 2 * GB) + 0.12 * GB);
    const segs = (last ? last.segments_uploaded + last.segments_reused : 0) + 5;
    const info: BackupInfo = {
      id: backupId(at),
      status: 'running',
      started_at_ms: at,
      finished_at_ms: null,
      trigger,
      channels: this.sim.channels.size,
      partitions: [...this.sim.channels.values()].reduce((s, c) => s + c.config.partitions, 0),
      messages: 0,
      bytes_uploaded: 0,
      segments_uploaded: 0,
      segments_reused: 0,
      total_bytes: total,
      progress_bytes: 0,
      error: null,
    };
    this.running = { info, durationMs, segs };
    return info;
  }

  private tick(now: number): void {
    const r = this.running;
    if (r) {
      const f = Math.min(1, (now - r.info.started_at_ms) / r.durationMs);
      const segs = r.segs;
      const eased = f < 0.15 ? f * 0.5 : 0.075 + ((f - 0.15) / 0.85) * 0.925;
      r.info.progress_bytes = Math.round(r.info.total_bytes * eased);
      r.info.segments_reused = Math.round((segs - 8) * eased);
      r.info.segments_uploaded = Math.round(8 * eased);
      r.info.bytes_uploaded = Math.round(r.info.segments_uploaded * 96 * 1024 * 1024 + 0.3 * GB * eased);
      r.info.messages = Math.round((r.info.total_bytes / 470) * eased);
      if (f >= 1) {
        r.info.status = 'succeeded';
        r.info.finished_at_ms = now;
        this.history.unshift({ ...r.info });
        this.running = null;
        this.retention();
        if (this.config?.enabled) this.nextRun = now + this.config.interval_secs * 1000;
      }
    }
    if (!this.running && this.config?.enabled && this.nextRun && now >= this.nextRun) {
      this.start('scheduled', now, 3 * 60_000);
      this.nextRun = null;
    }
    if (this.config?.enabled && !this.running && this.nextRun == null) this.nextRun = now + this.config.interval_secs * 1000;
  }

  private retention(): void {
    if (!this.config) return;
    const cutoff = this.config.keep_days ? Date.now() - this.config.keep_days * 24 * H : 0;
    this.history = this.history.filter((b, i) => i < this.config!.keep_last && b.started_at_ms >= cutoff);
  }

  private redacted(c: BackupConfig): BackupConfig {
    return {
      ...c,
      target: c.target.type === 's3' ? { ...c.target, secret_access_key: REDACTED } : c.target,
      encryption_key: c.encryption_key ? REDACTED : null,
    };
  }

  getConfig(): BackupConfig | null {
    return this.config ? this.redacted(this.config) : null;
  }

  private parseTarget(v: unknown, prev: BackupTarget | null): BackupTarget {
    if (!isObj(v)) return bad("'target' must be an object with a 'type'");
    if (v.type === 'filesystem') {
      if (typeof v.path !== 'string' || !v.path.startsWith('/')) bad("'target.path' must be an absolute path");
      return { type: 'filesystem', path: v.path as string };
    }
    if (v.type === 's3') {
      for (const k of ['region', 'bucket', 'access_key_id'] as const) if (typeof v[k] !== 'string' || !(v[k] as string).trim()) bad(`'target.${k}' is required`);
      if (v.endpoint != null && (typeof v.endpoint !== 'string' || !/^https?:\/\//.test(v.endpoint))) bad("'target.endpoint' must be an http(s) URL or null");
      const ps = prev?.type === 's3' ? prev.secret_access_key : null;
      return {
        type: 's3',
        endpoint: (v.endpoint as string | null) ?? null,
        region: v.region as string,
        bucket: v.bucket as string,
        prefix: typeof v.prefix === 'string' ? v.prefix : '',
        access_key_id: v.access_key_id as string,
        secret_access_key: mergeSecret(v.secret_access_key as string, ps, false, 'target.secret_access_key')!,
        path_style: !!v.path_style,
      };
    }
    return bad("'target.type' must be s3 or filesystem");
  }

  putConfig(body: unknown): BackupConfig {
    if (!isObj(body)) return bad('body must be a BackupConfig object');
    const interval = body.interval_secs;
    if (typeof interval !== 'number' || interval < 300) bad("'interval_secs' must be >= 300");
    if (typeof body.keep_last !== 'number' || body.keep_last < 1) bad("'keep_last' must be >= 1");
    if (typeof body.keep_days !== 'number' || body.keep_days < 0) bad("'keep_days' must be >= 0");
    const key = mergeSecret(body.encryption_key as string | null, this.config?.encryption_key, true, 'encryption_key');
    if (key != null && !(key.length === 44 && Buffer.from(key, 'base64').length === 32)) bad("'encryption_key' must be base64 of 32 bytes");
    this.config = {
      enabled: !!body.enabled,
      target: this.parseTarget(body.target, this.config?.target ?? null),
      interval_secs: interval as number,
      keep_last: body.keep_last as number,
      keep_days: body.keep_days as number,
      include_messages: body.include_messages !== false,
      encryption_key: key,
    };
    this.nextRun = this.config.enabled ? Date.now() + this.config.interval_secs * 1000 : null;
    this.sim.addAudit('backup.config', this.config.target.type, { enabled: this.config.enabled });
    return this.getConfig()!;
  }

  async testTarget(body: unknown): Promise<TestResult> {
    const t = this.parseTarget(body, this.config?.target ?? null);
    const latency = Math.round(t.type === 'filesystem' ? 3 : 90 + Math.random() * 200);
    await new Promise((r) => setTimeout(r, Math.min(latency, 600)));
    const fail = (error: string, error_detail: string): TestResult => ({ ok: false, latency_ms: latency, error, error_detail });
    if (t.type === 'filesystem') return t.path.startsWith('/readonly') ? fail('permission denied', `permission denied (os error 13): ${t.path}`) : { ok: true, latency_ms: latency, error: null };
    if (t.endpoint?.includes('<')) return fail('DNS error', `dns error: failed to lookup address information: ${t.endpoint}`);
    if (t.bucket.includes('missing')) return fail('HTTP 404', `NoSuchBucket: the bucket '${t.bucket}' does not exist`);
    if (t.secret_access_key.includes('bad')) return fail('HTTP 403', 'SignatureDoesNotMatch: check the secret access key');
    return { ok: true, latency_ms: latency, error: null };
  }

  status(): BackupOverview {
    return {
      configured: this.config != null,
      enabled: this.config?.enabled ?? false,
      running: this.running ? { ...this.running.info } : null,
      last_success: this.history.find((b) => b.status === 'succeeded') ?? null,
      last_failure: this.history.find((b) => b.status === 'failed') ?? null,
      next_run_ms: this.config?.enabled ? this.nextRun : null,
    };
  }

  run(): BackupInfo {
    if (OLD_NODE) throw new ApiErr(400, 'unsupported_in_cluster', "node 'n3' does not support cluster backups (FluxMQ 0.2.1); upgrade every node to 0.3");
    if (!this.config) throw new ApiErr(409, 'conflict', 'backups are not configured');
    if (this.running) throw new ApiErr(409, 'conflict', `backup ${this.running.info.id} is already running`);
    const info = this.start('manual', Date.now(), 35_000);
    this.sim.addAudit('backup.run', info.id, {});
    return { ...info };
  }

  list(): BackupInfo[] {
    return [...(this.running ? [{ ...this.running.info }] : []), ...this.history];
  }

  get(id: string): BackupManifest {
    const all = this.list();
    const b = id === 'latest' ? all.find((x) => x.status === 'succeeded') : all.find((x) => x.id === id);
    if (!b) throw new ApiErr(404, 'not_found', `backup '${id}' not found`);
    const withMessages = this.config?.include_messages ?? true;
    const channels = [...this.sim.channels.values()]
      .map((c) => {
        let messages = 0;
        let segments = 0;
        for (let p = 0; p < c.config.partitions; p++) {
          const end = Math.max(0, Math.floor(this.sim.partitionHigh(c, p)));
          const low = Math.max(0, Math.min(end, Math.floor(this.sim.partitionLow(c, p))));
          messages += end - low;
          segments += Math.max(1, Math.round((end - low) / 2_000_000));
        }
        return {
          name: c.config.name,
          mode: c.config.mode,
          partitions: c.config.partitions,
          messages: withMessages ? messages : 0,
          bytes: withMessages ? messages * 420 : 0,
          segments: withMessages ? segments : 0,
          groups: c.groups.length,
          dlq_entries: this.sim.dlqCount(c.config.name),
        };
      })
      .sort((x, y) => x.name.localeCompare(y.name));
    const nodes = Object.keys(this.sim.nodesState()).sort();
    return {
      info: b,
      node_id: nodes[0] ?? 'n1',
      cluster_name: 'prod-eu-1',
      fluxmq_version: '0.5.0',
      include_messages: withMessages,
      encryption: this.config?.encryption_key ? '9f3c1a7e' : null,
      nodes: nodes.length ? nodes : ['n1'],
      channels,
    };
  }

  delete(id: string): void {
    if (this.running?.info.id === id) throw new ApiErr(409, 'conflict', 'this backup is still running');
    const before = this.history.length;
    this.history = this.history.filter((b) => b.id !== id);
    if (this.history.length === before) throw new ApiErr(404, 'not_found', `backup '${id}' not found`);
    this.sim.addAudit('backup.delete', id, {});
  }
}
