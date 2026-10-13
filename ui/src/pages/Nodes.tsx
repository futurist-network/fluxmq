import { useMemo, useState } from 'react';
import { Link } from 'react-router';
import { Activity, AlertTriangle, Crown, Eraser, LogOut, PowerOff, Server, ServerOff, Shuffle } from 'lucide-react';
import { api, ApiError, errorMessage } from '../api/client';
import { TOPICS, type ClusterStatus, type DecommissionStatus, type NodeInfo, type NodeState, type NodeStatus, type NodesState } from '../api/types';
import { useTopicData } from '../telemetry';
import { fmtBytes, fmtCount, fmtDuration, fmtInt, fmtMs, fmtPercent } from '../lib/format';
import { channelPath } from '../lib/labels';
import { NODE_STATUS_HINT, NODE_STATUS_LABEL, nodeLoad, statusTone, nodeSlots, nodesUp, partitionMatrix, sortNodes, totalPartitions, type MatrixCell, type MatrixRow, type NodeLoad } from '../lib/cluster';
import { refreshCluster, useCluster, useClusterPolling } from '../store/cluster';
import { toast } from '../store/ui';
import { ConfirmDialog, EmptyState, HealthText, InlineError, Modal, Skeleton } from '../components/ui';
import { GateButton } from '../components/auth/Gate';
import { useGate } from '../components/auth/useGate';
import { RebalancePanel } from './rebalance/RebalancePanel';
import { cardStatus, departedNodes, forgetGate } from '../lib/decommission';
import { useDecommission, useDecommissionPolling } from '../store/decommission';
import { CpuProfileDialog } from './nodes/CpuProfile';
import { useServingNode } from '../lib/profile';
import { DecommissionDialog, DecommissionProgress, ForgetDialog, RemovedNodeCard } from './nodes/Decommission';
import { t as tr, useT, type TKey } from '../i18n';
import './nodes.css';

// Technical identifiers shown verbatim in every language.
const CLUSTER_DOC = 'docs/CLUSTER.md';
const FSYNC_P99 = 'fsync p99';

function Gauge({ label, pct, value, warnAt, critAt }: { label: string; pct: number; value: string; warnAt: number; critAt: number }) {
  const tone = pct >= critAt ? 'critical' : pct >= warnAt ? 'warning' : '';
  return (
    <div className="gauge">
      <div className="gauge-top">
        <span className="stat-label">{label}</span>
        <span className={`num gauge-val ${tone ? `text-${tone}` : ''}`}>{value}</span>
      </div>
      <div className={`meter ${tone}`} role="meter" aria-label={label} aria-valuenow={Math.round(pct)} aria-valuemin={0} aria-valuemax={100}>
        <span style={{ width: `${Math.min(100, Math.max(0, pct))}%` }} />
      </div>
    </div>
  );
}

/** Status shown only when it is not "Up" (colour = problem). */
function StatusChip({ status }: { status: NodeStatus }) {
  const t = useT();
  if (status === 'up') return <span className="node-status">{t('nodes.status.up')}</span>;
  return (
    <span className={`node-status ${statusTone(status)}`} title={NODE_STATUS_HINT[status]}>
      {status === 'down' ? <ServerOff size={12} /> : status === 'decommissioning' ? <PowerOff size={12} /> : null}
      {NODE_STATUS_LABEL[status]}
    </span>
  );
}

interface MergedNode {
  id: string;
  info: NodeInfo | null;
  stats: NodeState | null;
  status: NodeStatus;
  isController: boolean;
}

interface NodeActions {
  onDrain: (n: MergedNode) => void;
  onDecommission: (n: MergedNode) => void;
  onForget: (n: MergedNode) => void;
}

function NodeCard({ n, load, slot, clustered, actions, decom, decomError, onProfile }: { n: MergedNode; load?: NodeLoad; slot?: number; clustered: boolean; actions?: NodeActions; decom?: DecommissionStatus; decomError?: string | null; onProfile?: () => void }) {
  const t = useT();
  const s = n.stats;
  const down = n.status === 'down';
  const decommissioning = n.status === 'decommissioning';
  // Show progress while the registry says so, and keep a finished / failed status visible until dismissed.
  const showDecom = clustered && (decommissioning || decom != null);
  const decomBusy = decommissioning || decom?.state === 'moving_replicas' || decom?.state === 'leaving_membership';
  const forget = forgetGate(n.status, load ? load.replicas : null);
  const memPct = s?.memory_total_bytes ? (s.memory_bytes / s.memory_total_bytes) * 100 : 0;
  const diskPct = s?.disk_total_bytes ? (s.disk_used_bytes / s.disk_total_bytes) * 100 : 0;
  const tone = down ? 'critical' : decommissioning ? 'primary' : n.status !== 'up' ? 'warning' : s && s.health !== 'healthy' ? s.health : null;
  return (
    <article className={`card node-card ${tone ? `tone-${tone}` : ''} ${down ? 'is-down' : ''}`} aria-label={t('nodes.card.aria', { id: n.id })}>
      <div className="node-head">
        <span className="node-icon" aria-hidden>
          {down ? <ServerOff size={16} /> : <Server size={16} />}
          {clustered && slot != null && <span className={`node-swatch slot-${slot}`} />}
        </span>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div className="node-title">
            <h3 className="mono">{n.id}</h3>
            {clustered && n.isController && (
              <span className="badge badge-primary node-ctl" title={t('nodes.card.controllerHint')}>
                <Crown size={11} /> {t('nodes.card.controller')}
              </span>
            )}
            {clustered && n.info && <span className="node-role faint">{n.info.is_voter ? t('nodes.card.voter') : t('nodes.card.learner')}</span>}
          </div>
          <div className="faint mono node-addr">
            {n.info ? (
              <>
                <span title={t('nodes.card.clientAddr')}>{n.info.client_addr}</span> · <span title={t('nodes.card.httpAddr')}>{n.info.http_addr}</span>
              </>
            ) : (
              s?.address
            )}
          </div>
        </div>
        {clustered ? <StatusChip status={n.status} /> : s ? <HealthText health={s.health} /> : null}
      </div>

      {down && (
        <div className="node-down-note">
          <AlertTriangle size={14} />
          <span>
            {t('nodes.card.downNote')}
            {s ? ` ${t('nodes.card.lastKnown')}` : ''}
          </span>
        </div>
      )}
      {n.status === 'joining' && <div className="node-note faint">{t('nodes.card.joiningNote')}</div>}
      {n.status === 'leaving' && <div className="node-note faint">{t('nodes.card.leavingNote')}</div>}
      {showDecom && actions && <DecommissionProgress id={n.id} s={decom} error={decomError} onRetry={() => actions.onDecommission(n)} />}

      {s ? (
        <div className={`node-gauges ${down ? 'stale' : ''}`}>
          <Gauge label="CPU" pct={s.cpu_percent} value={fmtPercent(s.cpu_percent)} warnAt={80} critAt={95} />
          <Gauge label={t('nodes.card.memory')} pct={memPct} value={`${fmtBytes(s.memory_bytes)} / ${fmtBytes(s.memory_total_bytes)}`} warnAt={85} critAt={95} />
          <Gauge label={t('nodes.card.disk')} pct={diskPct} value={`${fmtBytes(s.disk_used_bytes)} / ${fmtBytes(s.disk_total_bytes)}`} warnAt={80} critAt={90} />
        </div>
      ) : (
        <div className="node-gauges">
          <span className="faint" style={{ fontSize: 12.5 }}>
            {t('nodes.card.noMetrics')}
          </span>
        </div>
      )}
      <dl className={`node-stats ${clustered ? 'six' : ''} ${down ? 'stale' : ''}`}>
        <div>
          <dt>{FSYNC_P99}</dt>
          <dd className={`num ${s && s.disk_latency_p99_ms > 50 ? 'text-warning' : ''}`}>{s ? fmtMs(s.disk_latency_p99_ms) : '—'}</dd>
        </div>
        <div>
          <dt>{t('nodes.card.connections')}</dt>
          <dd className="num">{s ? fmtInt(s.connections) : '—'}</dd>
        </div>
        {clustered && (
          <>
            <div>
              <dt>{t('nodes.card.leaders')}</dt>
              <dd className="num">{load ? fmtInt(load.leaders) : '—'}</dd>
            </div>
            <div>
              <dt>{t('common.replicas')}</dt>
              <dd className="num">{load ? fmtInt(load.replicas) : '—'}</dd>
            </div>
          </>
        )}
        <div>
          <dt>{t('common.uptime')}</dt>
          <dd className="num">{s && !down ? fmtDuration(s.uptime_secs) : '—'}</dd>
        </div>
        <div>
          <dt>{t('common.version')}</dt>
          <dd className="num">{n.info?.version ?? s?.version ?? '—'}</dd>
        </div>
      </dl>
      {onProfile && !down && (
        <div className="node-actions">
          <GateButton need="admin" className="btn btn-sm" onClick={onProfile} title={t('nodes.card.profileHint')}>
            <Activity size={13} /> {t('nodes.card.profile')}
          </GateButton>
        </div>
      )}
      {clustered && actions && !down && !decomBusy && (
        <div className="node-actions">
          <GateButton need="admin" className="btn btn-sm" onClick={() => actions.onDrain(n)} disabled={!load?.leaders} title={!load?.leaders ? t('nodes.card.leadsNone') : t('nodes.card.drainHint')}>
            <LogOut size={13} /> {t('nodes.card.drain')}
          </GateButton>
          {decom?.state !== 'failed' && (
            <GateButton need="admin" className="btn btn-sm" onClick={() => actions.onDecommission(n)} title={t('nodes.card.decomHint')}>
              <PowerOff size={13} /> {t('nodes.card.decommission')}
            </GateButton>
          )}
        </div>
      )}
      {clustered && actions && forget.visible && (
        <div className="node-actions">
          <GateButton need="admin" className="btn btn-sm" onClick={() => actions.onForget(n)} disabled={!forget.enabled} title={forget.reason ?? t('nodes.card.forgetHint')}>
            <Eraser size={13} /> {t('nodes.card.forget')}
          </GateButton>
        </div>
      )}
    </article>
  );
}

function ClusterSummary({ status }: { status: ClusterStatus }) {
  const t = useT();
  const { up, total } = nodesUp(status);
  const r = status.raft;
  const items: { k: TKey; v: string; sub?: string; tone?: 'warning' | 'critical' }[] = [
    { k: 'nodes.summary.nodesUp', v: `${up}/${total}`, tone: up < total ? (up * 2 <= total ? 'critical' : 'warning') : undefined, sub: up < total ? t('nodes.summary.notUp', { count: total - up }) : t('nodes.summary.allUp') },
    { k: 'nodes.summary.controller', v: r.leader ?? t('nodes.summary.none'), sub: t('nodes.summary.term', { term: fmtInt(r.term) }), tone: r.leader ? undefined : 'critical' },
    {
      k: 'nodes.summary.rf',
      v: String(status.replication_factor),
      sub: `${t('nodes.summary.voters', { count: r.voters.length })}${r.learners.length ? ` · ${t('nodes.summary.learners', { count: r.learners.length })}` : ''}`,
    },
    { k: 'nodes.summary.partitions', v: fmtInt(totalPartitions(status)), sub: t('common.channelsCount', { count: Object.keys(status.metadata.channels).length }) },
    { k: 'nodes.summary.under', v: fmtInt(status.under_replicated_partitions), tone: status.under_replicated_partitions ? 'warning' : undefined, sub: status.under_replicated_partitions ? t('nodes.summary.isrLess') : t('nodes.summary.none') },
    { k: 'nodes.summary.offline', v: fmtInt(status.offline_partitions), tone: status.offline_partitions ? 'critical' : undefined, sub: status.offline_partitions ? t('nodes.summary.noLeader') : t('nodes.summary.none') },
    {
      k: 'nodes.summary.raftLog',
      v: fmtCount(r.committed_index),
      sub: r.last_log_index > r.committed_index ? t('nodes.summary.uncommitted', { n: fmtInt(r.last_log_index - r.committed_index) }) : t('nodes.summary.committed', { epoch: fmtInt(status.metadata.epoch) }),
    },
  ];
  return (
    <section className="card cluster-summary" aria-label={t('nodes.summary.aria')}>
      {items.map((i) => (
        <div key={i.k} className="cs-item">
          <span className="hm-label" title={t(i.k)}>
            {t(i.k)}
          </span>
          <span className={`cs-value num ${i.tone ? `text-${i.tone}` : ''}`}>{i.v}</span>
          <span className="hm-sub" title={i.sub}>
            {i.sub}
          </span>
        </div>
      ))}
    </section>
  );
}

function cellTitle(c: MatrixCell, row: MatrixRow): string {
  const lag = c.maxLag ? ` · ${tr('nodes.cell.maxLag', { n: fmtInt(c.maxLag) })}` : '';
  const state = c.offline ? tr('nodes.cell.offline') : c.underReplicated ? tr('nodes.cell.under', { isr: c.isr.length, replicas: c.replicas.length }) : tr('nodes.cell.inSync');
  const leader = `${tr('nodes.cell.leader', { leader: c.leader ?? tr('nodes.summary.none') })}${c.notPreferred ? ` ${tr('nodes.cell.preferred', { node: c.replicas[0] ?? '' })}` : ''}`;
  return `${row.channel} p${c.partition}: ${state}\n${leader} · ${tr('nodes.cell.epoch', { epoch: c.epoch })}\n${tr('nodes.cell.replicas', { list: c.replicas.join(', ') })} · ISR ${c.isr.join(', ') || '—'}${lag}`;
}

function PartitionMatrix({ status, slots, onReassign }: { status: ClusterStatus; slots: Record<string, number>; onReassign: (row: MatrixRow, c: MatrixCell) => void }) {
  const rows = useMemo(() => partitionMatrix(status), [status]);
  const decomIds = useMemo(() => new Set(status.metadata.nodes.filter((n) => n.status === 'decommissioning').map((n) => n.id)), [status]);
  const onDecom = (c: MatrixCell) => decomIds.size > 0 && c.replicas.some((r) => decomIds.has(r));
  const [problemsOnly, setProblemsOnly] = useState(false);
  const gate = useGate('admin');
  const t = useT();
  const shown = problemsOnly ? rows.filter((r) => r.offline || r.underReplicated) : rows;
  const problems = rows.filter((r) => r.offline || r.underReplicated).length;
  return (
    <section className="card">
      <div className="card-head matrix-head">
        <h2>{t('nodes.matrix.title')}</h2>
        <span className="card-sub">
          {t('nodes.matrix.sub')}
          {gate.allowed ? ` · ${t('nodes.matrix.clickHint')}` : ''}
        </span>
        <span className="spacer" />
        <div className="matrix-legend" aria-label={t('nodes.matrix.legend')}>
          {sortNodes(status.metadata.nodes).map((n) => (
            <span key={n.id} className={`lg-item ${n.status === 'decommissioning' ? 'is-decom' : ''}`} title={n.status !== 'up' ? NODE_STATUS_HINT[n.status] : undefined}>
              <span className={`lg-sw slot-${slots[n.id] ?? 0}`} />
              <span className="mono">{n.id}</span>
              {n.status === 'decommissioning' && <span className="faint">({t('nodes.status.decommissioning').toLowerCase()})</span>}
            </span>
          ))}
          {decomIds.size > 0 && (
            <span className="lg-item">
              <span className="lg-sw decom" />
              {t('nodes.matrix.movingOff')}
            </span>
          )}
          <span className="lg-item">
            <span className="lg-sw under" />
            {t('nodes.matrix.under')}
          </span>
          <span className="lg-item">
            <span className="lg-sw offline" />
            {t('nodes.matrix.offline')}
          </span>
        </div>
        {(problems > 0 || problemsOnly) && (
          <label className="matrix-filter">
            <input type="checkbox" className="checkbox" checked={problemsOnly} onChange={(e) => setProblemsOnly(e.target.checked)} />
            {t('nodes.matrix.onlyProblems', { n: problems })}
          </label>
        )}
      </div>
      {shown.length === 0 ? (
        <EmptyState compact icon={<Server size={18} />} title={rows.length ? t('nodes.matrix.allReplicated') : t('nodes.matrix.noChannels')} />
      ) : (
        <div className="matrix">
          {shown.map((row) => (
            <div key={row.channel} className="matrix-row">
              <Link to={channelPath(row.channel, 'storage')} className="mx-name mono truncate" title={row.channel}>
                {row.channel}
              </Link>
              <div className="mx-cells" role="list" aria-label={t('nodes.matrix.rowAria', { channel: row.channel })}>
                {row.cells.map((c) => (
                  <button
                    key={c.partition}
                    type="button"
                    role="listitem"
                    className={`mx-cell ${c.offline ? 'offline' : `slot-${slots[c.leader ?? ''] ?? 0}`} ${c.underReplicated ? 'under' : ''} ${c.notPreferred ? 'moved' : ''} ${onDecom(c) ? 'decom' : ''}`}
                    title={cellTitle(c, row) + (onDecom(c) ? `\n${t('nodes.cell.decom', { nodes: c.replicas.filter((r) => decomIds.has(r)).join(', ') })}` : '')}
                    aria-label={cellTitle(c, row)}
                    disabled={!gate.allowed}
                    onClick={() => onReassign(row, c)}
                  >
                    {c.partition}
                  </button>
                ))}
              </div>
              <span className={`mx-sum ${row.offline ? 'text-critical' : row.underReplicated ? 'text-warning' : 'faint'}`}>
                {row.offline ? t('nodes.matrix.rowOffline', { n: row.offline }) : row.underReplicated ? t('nodes.matrix.rowUnder', { n: row.underReplicated }) : t('nodes.matrix.rowOk', { rf: row.rf })}
              </span>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

function ReassignDialog({ target, nodes, onClose }: { target: { row: MatrixRow; cell: MatrixCell } | null; nodes: NodeInfo[]; onClose: () => void }) {
  const [replicas, setReplicas] = useState<string[]>(() => target?.cell.replicas ?? []);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const t = useT();
  if (!target) return null;
  const { row, cell } = target;
  const toggle = (id: string) => setReplicas(replicas.includes(id) ? replicas.filter((r) => r !== id) : [...replicas, id]);
  const makePreferred = (id: string) => setReplicas([id, ...replicas.filter((r) => r !== id)]);
  const changed = JSON.stringify(replicas) !== JSON.stringify(cell.replicas);
  const downChosen = replicas.filter((r) => nodes.find((n) => n.id === r)?.status === 'down');
  const submit = async () => {
    setBusy(true);
    setError(null);
    try {
      await api.reassignPartition({ channel: row.channel, partition: cell.partition, replicas });
      toast('success', tr('nodes.reassign.toast', { channel: row.channel, partition: cell.partition, replicas: replicas.join(', ') }));
      void refreshCluster();
      onClose();
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) setError(errorMessage(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <Modal
      open
      onClose={onClose}
      title={t('nodes.reassign.title', { channel: row.channel, partition: cell.partition })}
      description={t('nodes.reassign.body', { leader: cell.leader ?? t('nodes.summary.none'), epoch: cell.epoch, isr: cell.isr.join(', ') || '—' })}
      footer={
        <>
          <button className="btn" onClick={onClose}>
            {t('common.cancel')}
          </button>
          <button className="btn btn-primary" onClick={submit} disabled={busy || !changed || replicas.length === 0}>
            <Shuffle size={14} /> {busy ? t('nodes.reassign.busy') : t('nodes.reassign.confirm')}
          </button>
        </>
      }
    >
      <div className="reassign-list" role="group" aria-label={t('common.replicas')}>
        {[...cell.replicas.map((id) => nodes.find((n) => n.id === id)).filter((n): n is NodeInfo => !!n), ...sortNodes(nodes.filter((n) => !cell.replicas.includes(n.id)))].map((n) => {
          const on = replicas.includes(n.id);
          const pref = replicas[0] === n.id;
          return (
            <div key={n.id} className={`ra-row ${on ? 'on' : ''}`}>
              <label className="ra-check">
                <input type="checkbox" className="checkbox" checked={on} onChange={() => toggle(n.id)} />
                <span className="mono">{n.id}</span>
              </label>
              <StatusChip status={n.status} />
              <span className="faint ra-info">{cell.isr.includes(n.id) ? t('nodes.reassign.inIsr') : cell.replicas.includes(n.id) ? t('nodes.reassign.outOfSync') : t('nodes.reassign.notReplica')}</span>
              <span className="spacer" />
              {on &&
                (pref ? (
                  <span className="badge badge-primary">{t('nodes.reassign.preferred')}</span>
                ) : (
                  <button type="button" className="btn-link" onClick={() => makePreferred(n.id)}>
                    {t('nodes.reassign.makePreferred')}
                  </button>
                ))}
            </div>
          );
        })}
      </div>
      {replicas.length > 0 && replicas.length < row.rf && <p className="field-hint text-warning">{t('nodes.reassign.fewer', { n: replicas.length, rf: row.rf })}</p>}
      {downChosen.length > 0 && <p className="field-hint text-warning">{t('nodes.reassign.downChosen', { count: downChosen.length, nodes: downChosen.join(', ') })}</p>}
      {error && <InlineError>{error}</InlineError>}
    </Modal>
  );
}

export default function NodesPage() {
  useClusterPolling(2000);
  const status = useCluster((s) => s.status);
  const loaded = useCluster((s) => s.loaded);
  const clusterError = useCluster((s) => s.error);
  const tel = useTopicData<NodesState>(TOPICS.NODES);
  const [drain, setDrain] = useState<MergedNode | null>(null);
  const [decommission, setDecommission] = useState<string | null>(null);
  const [forgetId, setForgetId] = useState<string | null>(null);
  const byNode = useDecommission((s) => s.byNode);
  const dismissedList = useDecommission((s) => s.dismissed);
  const decomError = useDecommission((s) => s.error);
  const dismissed = useMemo(() => new Set(dismissedList), [dismissedList]);
  const [draining, setDraining] = useState(false);
  const [reassign, setReassign] = useState<{ row: MatrixRow; cell: MatrixCell } | null>(null);
  const [profiling, setProfiling] = useState<string | null>(null);
  const serving = useServingNode();
  const t = useT();

  const merged: MergedNode[] | null = useMemo(() => {
    if (!status && !tel) return null;
    const ids = new Set<string>([...(status?.metadata.nodes.map((n) => n.id) ?? []), ...Object.keys(tel ?? {})]);
    const list = [...ids].map((id) => {
      const info = status?.metadata.nodes.find((n) => n.id === id) ?? null;
      const stats = tel?.[id] ?? null;
      return { id, info, stats, status: info?.status ?? stats?.status ?? 'up', isController: info?.is_controller ?? stats?.is_controller ?? false };
    });
    return sortNodes(list.map((n) => ({ ...n, is_controller: n.isController })));
  }, [status, tel]);

  const clustered = (merged?.length ?? 0) > 1;
  const slots = useMemo(() => nodeSlots(merged ?? []), [merged]);
  const load = useMemo(() => (status ? nodeLoad(status) : {}), [status]);
  const channelNames = useMemo(() => Object.keys(status?.metadata.channels ?? {}).sort(), [status]);
  const registry = useMemo(() => status?.metadata.nodes ?? [], [status]);
  useDecommissionPolling(clustered ? registry : []);
  // Only once the registry is known: before that every listed node would look departed.
  const departed = status ? departedNodes(registry, byNode, dismissed) : [];
  const actions: NodeActions = { onDrain: setDrain, onDecommission: (n) => setDecommission(n.id), onForget: (n) => setForgetId(n.id) };
  // The profile endpoint samples the process that answers it, i.e. the node serving the console.
  const profileFor = (id: string) => (serving?.node_id === id && serving.features?.includes('profiling') ? () => setProfiling(id) : undefined);

  const doDrain = async () => {
    if (!drain) return;
    setDraining(true);
    try {
      const r = await api.drainNode(drain.id);
      toast('success', tr('nodes.drain.toast', { count: r.moved_leaderships, id: drain.id }));
      setDrain(null);
      void refreshCluster();
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) toast('error', errorMessage(e));
    } finally {
      setDraining(false);
    }
  };

  return (
    <div className="page">
      <div className="page-head">
        <div className="titles">
          <h1>{t('nodes.title')}</h1>
          <p>{clustered ? t('nodes.subtitle.cluster') : t('nodes.subtitle.single')}</p>
        </div>
      </div>

      {!merged ? (
        <div className="node-grid">
          {[0, 1, 2].map((i) => (
            <div key={i} className="card node-card">
              <Skeleton h={20} w="50%" />
              <Skeleton h={80} style={{ marginTop: 16 }} />
            </div>
          ))}
        </div>
      ) : merged.length === 0 ? (
        <section className="card">
          <EmptyState icon={<Server size={20} />} title={t('nodes.empty')} />
        </section>
      ) : !clustered ? (
        <>
          <div className="single-note">
            <Server size={15} />
            <span>{t.rich('nodes.singleNote', { doc: <span className="mono">{CLUSTER_DOC}</span> })}</span>
          </div>
          <div className="node-grid single">
            {merged.map((n) => (
              <NodeCard key={n.id} n={n} clustered={false} onProfile={profileFor(n.id)} />
            ))}
          </div>
          <RebalancePanel singleNode slots={slots} channels={[]} />
        </>
      ) : (
        <>
          {status ? <ClusterSummary status={status} /> : loaded && clusterError ? <InlineError>{t('nodes.statusUnavailable', { error: clusterError })}</InlineError> : null}
          <div className="node-grid">
            {merged.map((n) => (
              <NodeCard key={n.id} n={n} clustered load={load[n.id]} slot={slots[n.id]} actions={actions} decom={cardStatus(byNode[n.id], dismissed)} decomError={decomError} onProfile={profileFor(n.id)} />
            ))}
            {departed.map((d) => (
              <RemovedNodeCard key={`removed-${d.node}@${d.started_at_ms}`} s={d} />
            ))}
          </div>
          <RebalancePanel singleNode={false} slots={slots} channels={channelNames} />
          {status && <PartitionMatrix status={status} slots={slots} onReassign={(row, cell) => setReassign({ row, cell })} />}
        </>
      )}

      <ConfirmDialog
        open={drain != null}
        title={t('nodes.drain.title', { id: drain?.id ?? '' })}
        description={t('nodes.drain.body', { count: load[drain?.id ?? '']?.leaders ?? 0, id: drain?.id ?? '' })}
        confirmLabel={t('nodes.drain.confirm')}
        busy={draining}
        onConfirm={doDrain}
        onClose={() => setDrain(null)}
      />
      {decommission && status && <DecommissionDialog key={decommission} id={decommission} status={status} slots={slots} onClose={() => setDecommission(null)} />}
      {profiling && <CpuProfileDialog node={profiling} onClose={() => setProfiling(null)} />}
      {forgetId && <ForgetDialog key={forgetId} id={forgetId} onClose={() => setForgetId(null)} />}
      {reassign && status && <ReassignDialog key={`${reassign.row.channel}/${reassign.cell.partition}`} target={reassign} nodes={status.metadata.nodes} onClose={() => setReassign(null)} />}
    </div>
  );
}
