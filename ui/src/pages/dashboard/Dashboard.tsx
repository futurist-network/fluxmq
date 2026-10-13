import { useMemo, useState } from 'react';
import { Link } from 'react-router';
import { Activity, AlertTriangle, ArrowRight, Gauge, Layers, Plus, ShieldCheck, Timer, Waypoints } from 'lucide-react';
import { TOPICS, type AlertsState, type ChannelsSummary, type ClusterOverview, type Health, type HistoryRange } from '../../api/types';
import { useTopicSelect } from '../../telemetry';
import { alertCounts, alertSeverityByKind, effectiveClusterHealth, openAlerts, sortedChannels, worstHealth } from '../../store/selectors';
import { healthWhy, useAlerts } from '../../components/HealthWhy';
import { SK, series } from '../../store/series';
import { fmtBytes, fmtCount, fmtDuration, fmtMs, fmtRate, splitUnit } from '../../lib/format';
import { HEALTH_LABEL } from '../../lib/labels';
import { useNow } from '../../lib/hooks';
import { useUi } from '../../store/ui';
import { EmptyState, HealthDot, Segmented, Skeleton } from '../../components/ui';
import { Sparkline } from '../../components/charts/Sparkline';
import { TimeChart } from '../../components/charts/TimeChart';
import { ChannelsTable } from '../../components/ChannelsTable';
import { EventsFeedList } from '../../components/EventsFeedList';
import { AlertRow } from '../../components/AlertCard';
import './dashboard.css';
import { GateButton } from '../../components/auth/Gate';
import { useT } from '../../i18n';
import { rangeOptions } from '../../components/charts/ranges';

/** Lag KPI turns amber when lag grows this fast (% per minute) above LAG_GROWTH_MIN messages. */
const LAG_GROWTH_WARN_PCT = 50;
const LAG_GROWTH_MIN = 1000;

/** Demo traffic generator (a shell command — not translated). */
const DEMO_CMD = 'cargo run -p fluxmq-client --bin fluxmq-demo';

type HeaderData = {
  name?: string;
  health?: Health;
  version?: string;
  uptime?: number;
  nodes?: string;
  channels?: number;
  consumers?: number;
  groups?: number;
  messages?: number;
  storage?: number;
  capacity?: number;
};

function HealthHeader() {
  const d = useTopicSelect<ClusterOverview, HeaderData>(TOPICS.CLUSTER, (o) =>
    o
      ? {
          name: o.cluster_name,
          health: o.health,
          version: o.version,
          uptime: o.uptime_secs,
          nodes: `${o.nodes_up ?? o.nodes_healthy}/${o.nodes_total}`,
          channels: o.channels,
          consumers: o.consumers,
          groups: o.consumer_groups,
          messages: o.messages_total,
          storage: o.storage_bytes,
          capacity: o.storage_capacity_bytes,
        }
      : {},
  );
  const t = useT();
  const counts = useTopicSelect<AlertsState, ReturnType<typeof alertCounts>>(TOPICS.ALERTS, alertCounts);
  const alerts = useAlerts();
  const loading = d.name == null;
  // Same roll-up as everywhere: open alerts make the cluster at least "warning".
  const health = effectiveClusterHealth(d.health ?? 'healthy', alerts);
  const why = healthWhy(health, alerts, {});
  const statusLine =
    counts.total > 0
      ? t('dashboard.issues', { count: counts.total })
      : health === 'healthy'
        ? t('dashboard.allOperational')
        : t('dashboard.clusterHealth', { health: HEALTH_LABEL[health].toLowerCase() });
  const stats: [string, string | undefined, string?, string?][] = [
    [t('common.nodes'), d.nodes, t('dashboard.stat.nodesUp')],
    [t('common.channels'), d.channels != null ? fmtCount(d.channels) : undefined],
    [t('common.consumers'), d.consumers != null ? fmtCount(d.consumers) : undefined, d.groups != null ? t('common.groupsCount', { count: d.groups }) : undefined],
    // Rates and latency live in the KPI cards below; the header shows inventory only.
    [t('dashboard.stat.messages'), d.messages != null ? fmtCount(d.messages) : undefined, t('dashboard.stat.stored')],
    [t('dashboard.stat.storage'), d.storage != null ? fmtBytes(d.storage) : undefined, d.capacity ? t('dashboard.stat.ofCapacity', { capacity: fmtBytes(d.capacity) }) : undefined],
  ];
  return (
    <section className={`card health-header tone-${counts.critical ? 'critical' : health}`} aria-label={t('dashboard.aria.clusterHealth')}>
      <div className="hh-top">
        <div className={`hh-icon ${loading ? 'unknown' : health}`} aria-hidden>
          {loading ? <Activity size={20} /> : health === 'healthy' && !counts.total ? <ShieldCheck size={20} /> : <AlertTriangle size={20} />}
        </div>
        <div className="hh-title">
          {loading ? (
            <>
              <Skeleton w={180} h={24} />
              <Skeleton w={260} h={14} style={{ marginTop: 8 }} />
            </>
          ) : (
            <>
              <h1>
                {d.name}
                <span className={`hh-status ${health} ${why ? 'has-why' : ''}`} title={why}>
                  <HealthDot health={health} /> {HEALTH_LABEL[health]}
                  {why && <span className="sr-only">. {why}</span>}
                </span>
              </h1>
              {counts.total === 0 && <p className="muted">{statusLine}</p>}
            </>
          )}
        </div>
        {!loading && (
          <div className="hh-meta faint">
            {t('dashboard.versionUptime', { version: d.version ?? '', uptime: fmtDuration(d.uptime) })}
          </div>
        )}
      </div>
      <dl className="hh-stats">
        {stats.map(([label, value, sub, hint]) => (
          <div key={label} className="hh-stat" title={hint}>
            <dt className={hint ? 'has-hint' : undefined}>{label}</dt>
            <dd>
              {value == null ? (
                <Skeleton w={64} h={20} />
              ) : (
                <>
                  <span className="num">{splitUnit(value)[0]}</span>
                  {splitUnit(value)[1] && <span className="unit">{splitUnit(value)[1]}</span>}
                  {sub && <span className="sub">{sub}</span>}
                </>
              )}
            </dd>
          </div>
        ))}
      </dl>
      <AlertStrip />
    </section>
  );
}

function AlertStrip() {
  const t = useT();
  const alerts = useTopicSelect<AlertsState, ReturnType<typeof openAlerts>>(TOPICS.ALERTS, openAlerts);
  const now = useNow();
  if (!alerts.length) return null;
  const critical = alerts.some((a) => a.severity === 'critical');
  return (
    <section className={`alert-strip ${critical ? 'critical' : 'warning'}`} aria-label={t('dashboard.alertCenter')}>
      <div className="as-head">
        <AlertTriangle size={16} className={critical ? 'text-critical' : 'text-warning'} />
        <h2>{t('dashboard.issues', { count: alerts.length })}</h2>
        <span className="spacer" />
        <Link to="/alerts" className="btn-link">
          {t('dashboard.alertCenter')} <ArrowRight size={13} style={{ verticalAlign: -2 }} />
        </Link>
      </div>
      <ul className="as-list">
        {alerts.slice(0, 3).map((a) => (
          <AlertRow key={a.id} alert={a} now={now} />
        ))}
        {alerts.length > 3 && (
          <li className="as-more">
            <Link to="/alerts" className="btn-link">
              {t('dashboard.moreCount', { count: alerts.length - 3 })}
            </Link>
          </li>
        )}
      </ul>
    </section>
  );
}

interface KpiProps {
  label: string;
  icon: React.ReactNode;
  value: string | undefined;
  sub?: string;
  seriesKey: string;
  seed: string[];
  color: string;
  /** Problem tone computed by the caller (colour only what is actually wrong). */
  tone?: Health;
  trend?: string;
  to?: string;
  /** Tooltip on the sub line (e.g. "sum over all groups"). */
  subHint?: string;
}

function Kpi({ label, icon, value, sub, seriesKey, seed, color, tone = 'healthy', trend, to, subHint }: KpiProps) {
  const t = useT();
  const [num, unit] = value ? splitUnit(value) : ['', ''];
  const body = (
    <>
      <div className="kpi-head">
        <span className="kpi-icon" aria-hidden>
          {icon}
        </span>
        <span className="kpi-label">{label}</span>
        {trend && <span className={`kpi-trend ${tone !== 'healthy' ? tone : ''}`}>{trend}</span>}
      </div>
      <div className="kpi-value">
        {value == null ? (
          <Skeleton w={120} h={34} />
        ) : (
          <>
            <span className={`num ${tone === 'critical' ? 'text-critical' : tone === 'warning' ? 'text-warning' : ''}`}>{num}</span>
            {unit && <span className="kpi-unit">{unit}</span>}
          </>
        )}
      </div>
      <div className="kpi-sub faint" title={subHint ?? sub}>
        {sub ?? ' '}
      </div>
      <div className="kpi-spark">
        <Sparkline seriesKey={seriesKey} seedMetrics={seed} color={tone === 'critical' ? '--critical' : tone === 'warning' ? '--warning' : color} height={40} windowSec={300} label={t('dashboard.kpi.sparkAria', { label })} />
      </div>
    </>
  );
  return to ? (
    <Link to={to} className={`card kpi tone-${tone}`} aria-label={`${label}: ${value ?? t('common.loading')}`}>
      {body}
    </Link>
  ) : (
    <div className={`card kpi tone-${tone}`}>{body}</div>
  );
}

/** Change vs. ~60 s ago from the live series. */
function trendOf(key: string): { pct: number | null } {
  const s = series.get(key);
  const n = s.ys.length;
  if (n < 30) return { pct: null };
  const cur = s.ys[n - 1]!;
  const prev = s.ys[Math.max(0, n - 61)]!;
  if (!prev) return { pct: null };
  return { pct: ((cur - prev) / prev) * 100 };
}

function fmtTrend(pct: number | null, t: ReturnType<typeof useT>): string | undefined {
  if (pct == null || !Number.isFinite(pct)) return undefined;
  if (Math.abs(pct) < 1) return t('dashboard.kpi.trendFlat');
  return t('dashboard.kpi.trend', { arrow: pct > 0 ? '↑' : '↓', pct: Math.abs(pct).toFixed(0) });
}

function KpiRow() {
  const d = useTopicSelect<ClusterOverview, { ingress?: number; egress?: number; p99?: number; p50?: number; lag?: number; retries?: number; dlq?: number; retries24?: number }>(
    TOPICS.CLUSTER,
    (o) =>
      o
        ? {
            ingress: o.ingress_rate,
            egress: o.egress_rate,
            p99: o.publish_latency.p99,
            p50: o.publish_latency.p50,
            lag: o.total_lag,
            retries: o.retries_rate,
            dlq: o.dlq_total,
            retries24: o.retries_24h,
          }
        : {},
  );
  const t = useT();
  const lagTrend = trendOf(SK.lag).pct;
  const p99Trend = trendOf(SK.p99).pct;
  // Colour follows open alerts (the broker's health rules), not ad-hoc UI thresholds.
  const sev = useTopicSelect<AlertsState, ReturnType<typeof alertSeverityByKind>>(TOPICS.ALERTS, alertSeverityByKind);
  const worst = (...hs: (Health | undefined)[]) => worstHealth(hs.filter((h): h is Health => !!h));
  // Lag colour: open lag alerts, or fast growth over the last minute even before an alert opens
  // (same thresholds as the broker's "growing" rule: > 1k behind; here ≥ +50 % in 1 min).
  const lagGrowingFast = lagTrend != null && lagTrend >= LAG_GROWTH_WARN_PCT && (d.lag ?? 0) > LAG_GROWTH_MIN;
  const lagTone = worst(sev.lag_growth, sev.consumer_stalled, sev.no_consumers, lagGrowingFast ? 'warning' : undefined);
  const p99Tone = worst(sev.high_publish_latency);
  const errTone = worst(sev.dlq_growth, sev.high_retry_rate);
  return (
    <div className="kpi-row">
      <Kpi
        label={t('dashboard.kpi.traffic')}
        icon={<Activity size={15} />}
        value={d.ingress != null ? fmtRate(d.ingress) : undefined}
        sub={d.egress != null ? t('dashboard.kpi.trafficSub', { egress: fmtRate(d.egress) }) : t('dashboard.kpi.trafficSubNoEgress')}
        subHint={t('dashboard.sumAllGroups')}
        seriesKey={SK.ingress}
        seed={['ingress_rate']}
        color="--series-1"
        trend={fmtTrend(trendOf(SK.ingress).pct, t)}
      />
      <Kpi
        label={t('dashboard.kpi.latency')}
        icon={<Timer size={15} />}
        value={d.p99 != null ? fmtMs(d.p99) : undefined}
        sub={d.p50 != null ? t('dashboard.kpi.latencySub', { p50: fmtMs(d.p50) }) : undefined}
        seriesKey={SK.p99}
        seed={['publish_p99']}
        color="--series-1"
        tone={p99Tone}
        trend={fmtTrend(p99Trend, t)}
      />
      <Kpi
        label={t('common.lag')}
        icon={<Layers size={15} />}
        value={d.lag != null ? `${fmtCount(d.lag)} ${t('dashboard.kpi.msgsUnit')}` : undefined}
        sub={t('dashboard.kpi.lagSub')}
        seriesKey={SK.lag}
        seed={['lag']}
        color="--series-1"
        tone={lagTone}
        trend={fmtTrend(lagTrend, t)}
        to="/consumers"
      />
      <Kpi
        label={t('dashboard.kpi.errors')}
        icon={<Gauge size={15} />}
        value={d.retries != null ? `${fmtCount(d.retries)} ${t('dashboard.kpi.retriesUnit')}` : undefined}
        sub={d.dlq != null ? t('dashboard.kpi.errorsSub', { dlq: fmtCount(d.dlq), retries: fmtCount(d.retries24 ?? 0) }) : undefined}
        seriesKey={SK.errors}
        seed={['retries_rate', 'dlq_rate']}
        color="--series-1"
        tone={errTone}
        to="/dlq"
      />
    </div>
  );
}



function TrafficCard() {
  const t = useT();
  const [range, setRange] = useState<HistoryRange>('15m');
  return (
    <section className="card chart-card" aria-label={t('dashboard.traffic.aria')}>
      <div className="card-head">
        <h2>{t('dashboard.traffic.title')}</h2>
        <span className="spacer" />
        <Segmented value={range} options={rangeOptions(t)} onChange={setRange} label={t('dashboard.timeRange')} />
      </div>
      <div className="card-body">
        <TimeChart
          metrics={['ingress_rate', 'ack_rate']}
          liveKeys={[SK.ingress, SK.acked]}
          range={range}
          series={[
            { label: t('dashboard.stat.published'), color: '--series-1', fill: true },
            { label: t('dashboard.traffic.consumedAcked'), color: '--series-2' },
          ]}
          height={248}
          yFormat={(v) => fmtRate(v, false)}
          unitLabel={t('dashboard.unit.msgRate')}
        />
      </div>
    </section>
  );
}

function LatencyCard() {
  const t = useT();
  const [range, setRange] = useState<HistoryRange>('15m');
  return (
    <section className="card chart-card" aria-label={t('dashboard.latency.title')}>
      <div className="card-head">
        <h2>{t('dashboard.latency.title')}</h2>
        <span className="spacer" />
        <Segmented value={range} options={rangeOptions(t)} onChange={setRange} label={t('dashboard.timeRange')} />
      </div>
      <div className="card-body">
        <TimeChart
          metrics={['publish_p50', 'publish_p95', 'publish_p99']}
          liveKeys={[SK.p50, SK.p95, SK.p99]}
          range={range}
          series={[
            { label: 'p50', color: '--series-1', width: 1.5 },
            { label: 'p95', color: '--series-2', width: 1.5 },
            { label: 'p99', color: '--series-3', width: 2.25 },
          ]}
          height={248}
          yFormat={(v) => fmtMs(v)}
          unitLabel={t('dashboard.unit.ms')}
        />
      </div>
    </section>
  );
}

function ChannelsCard() {
  const rows = useTopicSelect<ChannelsSummary, string>(TOPICS.CHANNELS, (c) => (c ? sortedChannels(c).map((x) => x.name).join('\n') : '\u0000'));
  const t = useT();
  const setCreateOpen = useUi((s) => s.setCreateOpen);
  const loading = rows === '\u0000';
  const names = useMemo(() => (loading || !rows ? [] : rows.split('\n')), [rows, loading]);
  return (
    <section className="card" aria-label={t('common.channels')}>
      <div className="card-head">
        <h2>{t('common.channels')}</h2>
        {!loading && <span className="card-sub">{names.length}</span>}
        <span className="spacer" />
        <Link to="/channels" className="btn-link">
          {t('dashboard.viewAll')} <ArrowRight size={13} style={{ verticalAlign: -2 }} />
        </Link>
      </div>
      <div className="card-body flush">
        {!loading && names.length === 0 ? (
          <EmptyState
            icon={<Waypoints size={20} />}
            title={t('dashboard.empty.title')}
            actions={
              <GateButton need="admin" className="btn btn-primary" onClick={() => setCreateOpen(true)}>
                <Plus size={15} /> {t('dashboard.createChannel')}
              </GateButton>
            }
          >
            {t.rich('dashboard.empty.body', { cmd: <code>{DEMO_CMD}</code> })}
          </EmptyState>
        ) : (
          <ChannelsTable names={names} loading={loading} limit={8} compact />
        )}
      </div>
    </section>
  );
}

function EventsCard() {
  const t = useT();
  return (
    <section className="card" aria-label={t('dashboard.events.title')}>
      <div className="card-head">
        <h2>{t('dashboard.events.title')}</h2>
      </div>
      <div className="card-body flush">
        <EventsFeedList limit={8} />
      </div>
    </section>
  );
}

function Onboarding() {
  const t = useT();
  const setCreateOpen = useUi((s) => s.setCreateOpen);
  return (
    <section className="card onboarding">
      <EmptyState
        icon={<Waypoints size={20} />}
        title={t('dashboard.onboarding.title')}
        actions={
          <>
            <GateButton need="admin" className="btn btn-primary" onClick={() => setCreateOpen(true)}>
              <Plus size={15} /> {t('dashboard.onboarding.createFirst')}
            </GateButton>
            <Link to="/settings" className="btn">
              {t('common.settings')}
            </Link>
          </>
        }
      >
        {t.rich('dashboard.onboarding.body', {
          channel: <b>{t('dashboard.onboarding.channelWord')}</b>,
          groups: <b>{t('dashboard.onboarding.groupsWord')}</b>,
          cmd: <code>{DEMO_CMD}</code>,
        })}
      </EmptyState>
    </section>
  );
}

export default function Dashboard() {
  const empty = useTopicSelect<ClusterOverview, boolean>(TOPICS.CLUSTER, (o) => !!o && o.channels === 0);
  if (empty) {
    return (
      <div className="page dashboard">
        <HealthHeader />
        <Onboarding />
      </div>
    );
  }
  return (
    <div className="page dashboard">
      <HealthHeader />
      <KpiRow />
      <div className="grid-2-1">
        <TrafficCard />
        <LatencyCard />
      </div>
      <div className="grid-2-1">
        <ChannelsCard />
        <EventsCard />
      </div>
    </div>
  );
}

