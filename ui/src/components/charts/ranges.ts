import type { HistoryRange } from '../../api/types';
import type { TFn, TKey } from '../../i18n';

export const DEFAULT_RANGES: readonly HistoryRange[] = ['5m', '15m', '1h', '24h'];

/** Range picker options in the current language ("5m" / "5 мин"). */
export function rangeOptions(t: TFn, ranges: readonly HistoryRange[] = DEFAULT_RANGES): { value: HistoryRange; label: string }[] {
  return ranges.map((value) => ({ value, label: t(`dashboard.rangeShort.${value}` as TKey) }));
}
