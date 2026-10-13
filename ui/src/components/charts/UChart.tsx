/**
 * Thin uPlot (canvas) wrapper. Data flows in imperatively through a `ChartSource`
 * (get + subscribe) so live samples redraw the canvas without a React render.
 * Re-created only when the series config or theme changes.
 */
import { useEffect, useRef } from 'react';
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';
import { useTelemetry } from '../../store/telemetry';
import { useUi } from '../../store/ui';
import { fmtClock } from '../../lib/format';
import { getLocale, useLang } from '../../i18n';
import { cssVar } from './theme';
import './charts.css';

export interface ChartSource {
  get(): uPlot.AlignedData;
  subscribe(cb: () => void): () => void;
}

export interface ChartSeries {
  label: string;
  /** CSS variable name, e.g. `--series-1`. */
  color: string;
  fill?: boolean;
  dash?: number[];
  width?: number;
}

export interface UChartProps {
  source: ChartSource;
  series: ChartSeries[];
  height: number;
  yFormat?: (v: number) => string;
  /** Sparkline mode: no axes, no cursor, no tooltip. */
  spark?: boolean;
  /** Show seconds on the x axis / tooltip (short ranges). */
  seconds?: boolean;
  /** Fixed y minimum (default 0). `null` = auto. */
  yMin?: number | null;
  ariaLabel: string;
  className?: string;
}

function withAlpha(hex: string, alpha: number): string {
  const h = hex.replace('#', '');
  if (h.length !== 6) return hex;
  const n = parseInt(h, 16);
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${alpha})`;
}

function gradientFill(color: string, strength: number): uPlot.Series.Fill {
  return (u: uPlot) => {
    const { top, height } = u.bbox;
    if (!Number.isFinite(top) || height <= 0) return withAlpha(color, strength / 2);
    const g = u.ctx.createLinearGradient(0, top, 0, top + height);
    g.addColorStop(0, withAlpha(color, strength));
    g.addColorStop(1, withAlpha(color, 0));
    return g;
  };
}

function tooltipPlugin(series: ChartSeries[], yFormat: (v: number) => string, seconds: boolean): uPlot.Plugin {
  let tip: HTMLDivElement;
  return {
    hooks: {
      init: (u) => {
        tip = document.createElement('div');
        tip.className = 'uc-tip';
        tip.style.display = 'none';
        u.over.appendChild(tip);
        u.over.addEventListener('mouseleave', () => (tip.style.display = 'none'));
      },
      setCursor: (u) => {
        const idx = u.cursor.idx;
        if (idx == null || u.cursor.left == null || u.cursor.left < 0) {
          tip.style.display = 'none';
          return;
        }
        const x = u.data[0][idx];
        if (x == null) return;
        const d = new Date(x * 1000);
        const rows = series
          .map((s, i) => {
            const v = u.data[i + 1]?.[idx];
            return `<div class="uc-tip-row"><i style="background:${cssVar(s.color)}"></i><span>${s.label}</span><b>${v == null ? '—' : yFormat(v)}</b></div>`;
          })
          .join('');
        tip.innerHTML = `<div class="uc-tip-time">${d.toLocaleDateString(getLocale(), { month: 'short', day: 'numeric' })} · ${fmtClock(x * 1000, seconds)}</div>${rows}`;
        tip.style.display = 'block';
        const w = tip.offsetWidth;
        const overW = u.over.clientWidth;
        const left = u.cursor.left + 14 + w > overW ? u.cursor.left - w - 14 : u.cursor.left + 14;
        tip.style.transform = `translate(${Math.max(0, left)}px, 8px)`;
      },
    },
  };
}

export function UChart({ source, series, height, yFormat = String, spark = false, seconds = false, yMin = 0, ariaLabel, className }: UChartProps) {
  const ref = useRef<HTMLDivElement>(null);
  const theme = useUi((s) => s.theme);
  // Axis numbers use the active locale (decimal comma in Russian): rebuild on language change.
  const lang = useLang((s) => s.lang);
  const seriesKey = JSON.stringify(series);
  // Keep latest props in refs so a parent re-render doesn't rebuild the chart.
  const fmtRef = useRef(yFormat);
  const uRef = useRef<uPlot | null>(null);
  useEffect(() => {
    fmtRef.current = yFormat;
  });

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const cfg = JSON.parse(seriesKey) as ChartSeries[];
    const axisColor = cssVar('--chart-axis');
    const gridColor = cssVar('--chart-grid');
    const fmt = (v: number) => fmtRef.current(v);
    const font = '11px "Inter Variable", ui-sans-serif, system-ui';

    const opts: uPlot.Options = {
      width: Math.max(10, el.clientWidth),
      height,
      pxAlign: 0,
      legend: { show: false },
      padding: spark ? [2, 0, 2, 0] : [12, 8, 0, 0],
      cursor: spark
        ? { show: false }
        : {
            points: { size: 7, width: 2, stroke: (_u, i) => cssVar(cfg[i - 1]?.color ?? '--series-1'), fill: () => cssVar('--card') },
            drag: { x: false, y: false },
            y: false,
          },
      select: { show: false, left: 0, top: 0, width: 0, height: 0 },
      scales: {
        x: { time: true },
        y: {
          range: (_u, min, max) => {
            const lo = yMin == null ? min : Math.min(yMin, min);
            let hi = max;
            if (!Number.isFinite(hi) || hi <= lo) hi = lo + 1;
            return [lo, hi + (hi - lo) * 0.12];
          },
        },
      },
      axes: spark
        ? [{ show: false }, { show: false }]
        : [
            {
              stroke: axisColor,
              font,
              size: 28,
              gap: 6,
              grid: { show: false },
              ticks: { show: false },
              values: (_u, splits) => splits.map((s) => fmtClock(s * 1000, seconds)),
              space: 70,
            },
            {
              stroke: axisColor,
              font,
              size: 52,
              gap: 8,
              grid: { stroke: gridColor, width: 1 },
              ticks: { show: false },
              values: (_u, splits) => splits.map((s) => fmt(s)),
              space: 36,
            },
          ],
      series: [
        {},
        ...cfg.map((s) => {
          const c = cssVar(s.color);
          return {
            label: s.label,
            stroke: c,
            width: s.width ?? (spark ? 1.5 : 2),
            dash: s.dash,
            fill: s.fill ? gradientFill(c, spark ? 0.22 : 0.18) : undefined,
            points: { show: false },
            spanGaps: false,
          } satisfies uPlot.Series;
        }),
      ],
      plugins: spark ? [] : [tooltipPlugin(cfg, fmt, seconds)],
    };

    const u = new uPlot(opts, [[], ...cfg.map(() => [])], el);
    uRef.current = u;
    const ro = new ResizeObserver(() => {
      const w = el.clientWidth;
      if (w > 0 && Math.abs(w - u.width) > 1) u.setSize({ width: w, height });
    });
    ro.observe(el);
    return () => {
      ro.disconnect();
      u.destroy();
      uRef.current = null;
    };
  }, [seriesKey, height, spark, seconds, yMin, theme, lang]);

  // Data binding: (re)subscribe whenever the source or the chart instance changes.
  useEffect(() => {
    const u = uRef.current;
    if (!u) return;
    u.setData(source.get());
    let raf = 0;
    const redraw = () => {
      if (raf) return;
      raf = requestAnimationFrame(() => {
        raf = 0;
        u.setData(source.get());
      });
    };
    const unsub = source.subscribe(() => {
      if (!useTelemetry.getState().paused) redraw();
    });
    const unsubPause = useTelemetry.subscribe((st, prev) => {
      if (prev.paused && !st.paused) redraw();
    });
    return () => {
      unsub();
      unsubPause();
      cancelAnimationFrame(raf);
    };
  }, [source, seriesKey, height, spark, seconds, yMin, theme, lang]);

  return <div ref={ref} className={`uchart ${spark ? 'spark' : ''} ${className ?? ''}`} style={{ height }} role="img" aria-label={ariaLabel} />;
}
