import { UChart } from './UChart';
import { useLiveSource, useSeedSeries } from './sources';

interface SparklineProps {
  seriesKey: string;
  /** History metric(s) to prefill from (summed). */
  seedMetrics?: string[];
  color?: string;
  height?: number;
  windowSec?: number;
  label: string;
}

/** Canvas sparkline bound to a live series key. Never re-renders on new samples. */
export function Sparkline({ seriesKey, seedMetrics, color = '--series-1', height = 32, windowSec = 120, label }: SparklineProps) {
  useSeedSeries(seriesKey, seedMetrics ?? []);
  const source = useLiveSource([seriesKey], windowSec);
  return <UChart spark source={source} series={[{ label, color, fill: true }]} height={height} ariaLabel={label} />;
}

/** Variant without history prefill (table rows: fills from live samples). */
export function LiveSparkline({ seriesKey, color = '--series-1', height = 24, windowSec = 90, label }: Omit<SparklineProps, 'seedMetrics'>) {
  const source = useLiveSource([seriesKey], windowSec);
  return <UChart spark source={source} series={[{ label, color, fill: true, width: 1.25 }]} height={height} ariaLabel={label} />;
}
