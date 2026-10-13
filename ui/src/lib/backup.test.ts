import { describe, expect, it } from 'vitest';
import { REDACTED, type BackupConfig } from '../api/types';
import { configToDraft, draftToConfig, emptyBackupDraft, generateKey, progress, restoreCommand, savings, validKey, validateBackupDraft } from './backup';

const s3: BackupConfig = {
  enabled: true,
  target: { type: 's3', endpoint: 'https://acc.r2.cloudflarestorage.com', region: 'auto', bucket: 'fluxmq-backups', prefix: 'prod/', access_key_id: 'AKIA1', secret_access_key: REDACTED, path_style: false },
  interval_secs: 3600,
  keep_last: 24,
  keep_days: 7,
  include_messages: true,
  encryption_key: REDACTED,
};

describe('backup savings', () => {
  it('computes incremental reuse', () => {
    const s = savings({ segments_uploaded: 3, segments_reused: 117, bytes_uploaded: 200, total_bytes: 8000 });
    expect(s.reusedShare).toBeCloseTo(117 / 120);
    expect(s.bytesSaved).toBe(7800);
    expect(s.uploadedShare).toBeCloseTo(0.025);
  });
  it('handles the first (full) backup and empty backups', () => {
    expect(savings({ segments_uploaded: 10, segments_reused: 0, bytes_uploaded: 500, total_bytes: 500 })).toEqual({ reusedShare: 0, bytesSaved: 0, uploadedShare: 1 });
    expect(savings({ segments_uploaded: 0, segments_reused: 0, bytes_uploaded: 0, total_bytes: 0 }).reusedShare).toBe(0);
  });
  it('progress = progress_bytes / total_bytes while running', () => {
    expect(progress({ status: 'running', progress_bytes: 25, total_bytes: 100 })).toBe(0.25);
    expect(progress({ status: 'running', progress_bytes: 0, total_bytes: 0 })).toBeNull();
    expect(progress({ status: 'succeeded', progress_bytes: 0, total_bytes: 0 })).toBe(1);
  });
});

describe('backup config form', () => {
  it('round-trips with secrets kept', () => {
    const d = configToDraft(s3);
    expect(validateBackupDraft(d)).toEqual({});
    expect(draftToConfig(d)).toEqual(s3);
  });
  it('turning encryption off removes the stored key ("")', () => {
    expect(draftToConfig({ ...configToDraft(s3), encrypt: false }).encryption_key).toBe('');
    expect(draftToConfig(emptyBackupDraft()).encryption_key).toBeNull();
  });
  it('generates valid 32-byte keys and validates them', () => {
    const k = generateKey();
    expect(validKey(k)).toBe(true);
    expect(validKey('short')).toBe(false);
    const d = { ...emptyBackupDraft(), bucket: 'b-1x', access_key_id: 'a', secret_access_key: { stored: false, replacing: true, value: 's' }, encrypt: true };
    expect(validateBackupDraft(d).encryption_key).toBeTruthy();
    expect(validateBackupDraft({ ...d, encryption_key: { stored: false, replacing: true, value: k } })).toEqual({});
    expect(validateBackupDraft({ ...d, interval_secs: 60 }).interval_secs).toBeTruthy();
  });
  it('builds the exact restore command incl. the encryption key env', () => {
    expect(restoreCommand(s3, '20261008T120000Z-ab12')).toBe(
      [
        'export AWS_ACCESS_KEY_ID=AKIA1',
        "export AWS_SECRET_ACCESS_KEY='<secret-access-key>'",
        "export FLUXMQ_BACKUP_ENCRYPTION_KEY='<your encryption key>'   # or --encryption-key",
        'fluxmq restore \\\n  --s3-endpoint https://acc.r2.cloudflarestorage.com \\\n  --s3-region auto \\\n  --s3-bucket fluxmq-backups \\\n  --s3-prefix prod/ \\\n  --backup 20261008T120000Z-ab12 \\\n  --data-dir /var/lib/fluxmq-restored',
      ].join('\n'),
    );
    const fs: BackupConfig = { ...s3, target: { type: 'filesystem', path: '/mnt/backups' }, encryption_key: null };
    expect(restoreCommand(fs, 'latest', '/data/new')).toBe('fluxmq restore \\\n  --fs-path /mnt/backups \\\n  --backup latest \\\n  --data-dir /data/new');
  });
});

describe('cluster refusal', () => {
  it('recognises ApiError.error = "unsupported_in_cluster" (never the message text)', async () => {
    const { isClusterRefusal } = await import('./backupActions');
    const { ApiError } = await import('../api/client');
    expect(isClusterRefusal(new ApiError(400, 'unsupported_in_cluster', 'node n3 is too old'))).toBe(true);
    expect(isClusterRefusal(new ApiError(400, 'bad_request', 'backups of a cluster are not supported in this version'))).toBe(false);
    expect(isClusterRefusal(new ApiError(400, 'bad_request', 'keep_last must be >= 1'))).toBe(false);
    expect(isClusterRefusal(new ApiError(409, 'conflict', 'a backup is already running'))).toBe(false);
  });
});
