import type { HistoryRange } from '../../api/types';
import { ApiError, errorMessage } from '../../api/client';
import { Skeleton } from '../ui';
import { UChart, type ChartSeries } from './UChart';
import { useHistorySource } from './sources';
import { useT } from '../../i18n';

interface TimeChartProps {
  metrics: string[];
  liveKeys: string[] | null;
  range: HistoryRange;
  series: ChartSeries[];
  height: number;
  yFormat: (v: number) => string;
  unitLabel?: string;
}

/** History (`/api/history`) + live-appended time-series chart with legend. */
export function TimeChart({ metrics, liveKeys, range, series, height, yFormat, unitLabel }: TimeChartProps) {
  const t = useT();
  const h = useHistorySource(metrics, range, liveKeys);
  const short = range === '5m';
  return (
    <div className="timechart">
      <div className="legend" style={{ marginBottom: 6 }}>
        {series.map((s) => (
          <span key={s.label} className="legend-item">
            <i style={{ background: `var(${s.color})` }} />
            {s.label}
          </span>
        ))}
        {unitLabel && <span className="faint" style={{ marginLeft: 'auto' }}>{unitLabel}</span>}
      </div>
      {h.loading ? (
        <Skeleton h={height} r={8} />
      ) : h.error && !h.hasData ? (
        <div className="chart-empty" style={{ height }}>
          {h.error instanceof ApiError && (h.error.status === 0 || h.error.status >= 502) ? t('dashboard.chart.historyUnreachable') : t('dashboard.chart.historyError', { error: errorMessage(h.error) })}
        </div>
      ) : (
        <UChart source={h.source} series={series} height={height} yFormat={yFormat} seconds={short} ariaLabel={t('dashboard.chart.aria', { series: series.map((s) => s.label).join(', '), range: t(`dashboard.range.${range}`) })} />
      )}
    </div>
  );
}
