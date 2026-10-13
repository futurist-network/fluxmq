import type { GroupState, InstanceState } from '../../api/types';
import { medianProcessingP99 } from '../../store/selectors';
import { t } from '../../i18n';
import { fmtNumber } from '../../lib/format';

export interface InstanceReason {
  /** `processing` = slow processing (the p99 cell is highlighted). */
  kind: 'processing' | 'ack' | 'stalled' | 'error';
  text: string;
}

/** Why an instance is flagged (mirrors the health rules in ARCHITECTURE.md). Text follows the console language. */
export function instanceReason(i: InstanceState, g: GroupState): InstanceReason | null {
  if (i.health === 'healthy') return null;
  const insts = Object.values(g.instances);
  const median = medianProcessingP99(g);
  if (median && i.processing_p99_ms > 3 * median) return { kind: 'processing', text: t('consumers.reason.processing', { ratio: fmtNumber(i.processing_p99_ms / median, 1) }) };
  if (insts.length >= 3) {
    const rates = insts.map((x) => x.ack_rate).sort((a, b) => a - b);
    const m = rates[Math.floor(rates.length / 2)] ?? 0;
    if (m > 0 && i.ack_rate < 0.5 * m) return { kind: 'ack', text: t('consumers.reason.ackRate', { percent: Math.round((i.ack_rate / m) * 100) }) };
  }
  if (i.ack_rate === 0 && i.in_flight > 0) return { kind: 'stalled', text: t('consumers.reason.stalled') };
  if (i.last_error) return { kind: 'error', text: i.last_error };
  return null;
}
