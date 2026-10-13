import { describe, expect, it } from 'vitest';
import { ApiError, isApiErrorCode } from '../api/client';
import { ERROR_UNSUPPORTED_IN_CLUSTER, REDACTED, type Notifier } from '../api/types';
import { isClusterRefusal } from './backupActions';
import { draftFromNotifier, draftToUpdate } from './notifierForm';
import { errorDetail } from './webhookForm';

describe('machine-readable error codes', () => {
  it('cluster refusal branches on ApiError.error only', () => {
    expect(isClusterRefusal(new ApiError(400, ERROR_UNSUPPORTED_IN_CLUSTER, 'anything'))).toBe(true);
    // The old message heuristic no longer counts — codes, not text.
    expect(isClusterRefusal(new ApiError(400, 'bad_request', 'backups of a cluster are not supported in this version'))).toBe(false);
    expect(isClusterRefusal(new Error('unsupported_in_cluster'))).toBe(false);
    expect(isApiErrorCode(new ApiError(503, 'unavailable', 'x'), 'unavailable')).toBe(true);
  });
});

describe('short errors with detail', () => {
  it('shows the server detail when it adds information', () => {
    expect(errorDetail('HTTP 503', 'HTTP 503: upstream connect error')).toBe('HTTP 503: upstream connect error');
    expect(errorDetail('timeout', 'timeout')).toBeNull();
    expect(errorDetail('HTTP 503', null)).toBeNull();
  });
  it('falls back to a long legacy error text', () => {
    expect(errorDetail('connection refused: tcp connect error', undefined)).toBe('connection refused: tcp connect error');
    expect(errorDetail(null, null)).toBeNull();
  });
});

describe('testing an edited notifier', () => {
  const stored: Notifier = {
    id: 'nt1',
    name: 'ops',
    enabled: true,
    kind: { type: 'telegram', bot_token: REDACTED, chat_id: '-100123', thread_id: null },
    created_at_ms: 1,
    updated_at_ms: 1,
  };
  it('draft → update request keeps untouched secrets as REDACTED (= stored value)', () => {
    const d = { ...draftFromNotifier(stored), chat_id: '-100999' };
    const u = draftToUpdate(d);
    expect(u.kind).toEqual({ type: 'telegram', bot_token: REDACTED, chat_id: '-100999', thread_id: null });
    expect(u.name).toBe('ops');
  });
});
