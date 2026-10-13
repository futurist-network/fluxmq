import type { DeliveryAttempt } from '../api/types';

/** Final state of one group's delivery history (retry timeline). */
export type TimelineEnd = 'acked' | 'in_flight' | 'dead' | 'retrying' | 'none';

export function timelineEnd(attempts: DeliveryAttempt[], maxAttempts: number | null, deadLettered?: boolean): TimelineEnd {
  const last = attempts[attempts.length - 1];
  if (!last) return 'none';
  if (last.outcome.kind === 'acked') return 'acked';
  if (last.outcome.kind === 'in_flight') return 'in_flight';
  if (deadLettered || (maxAttempts != null && attempts.length >= maxAttempts)) return 'dead';
  return 'retrying';
}

