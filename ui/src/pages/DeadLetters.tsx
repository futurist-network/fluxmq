import { useEffect, useMemo, useRef, useState } from 'react';
import { Link, useSearchParams } from 'react-router';
import { Download, Inbox, Info, RotateCcw, ShieldAlert, Trash2, X } from 'lucide-react';
import { api, errorMessage } from '../api/client';
import { HEADERS, TOPICS, type ClusterOverview, type DeadLetterView, type DlqActionRequest, type DlqPage, type DlqReasonGroup, ROUTER_GROUP } from '../api/types';
import { useAsync, useNow } from '../lib/hooks';
import { fmtClock, fmtCount, fmtDateTime, fmtInt, fmtRelative } from '../lib/format';
import { reasonText } from '../lib/dlqReason';
import { OverflowMenu } from '../components/OverflowMenu';
import { channelPath } from '../lib/labels';
import { toast } from '../store/ui';
import { useTopicSelect } from '../telemetry';
import { ConfirmDialog, Drawer, EmptyState, InlineError, SkeletonRows } from '../components/ui';
import { MessageInspector } from '../components/messages/MessageInspector';
import './dlq.css';
import { GateButton } from '../components/auth/Gate';
import { t as tr, useT } from '../i18n';

const PAGE = 100;
/** Short technical name of the system router group (not translated). */
const ROUTER_SHORT = 'router';
const keyOf = (e: DeadLetterView) => `${e.channel}#${e.id}`;

/**
 * A pending action, captured when the dialog opens: how many entries the scope had at that
 * moment (`at`), and for bulk deletes the name the operator must type.
 */
type Pending = { kind: 'retry' | 'discard'; req: DlqActionRequest; label: string; count: number; at: number; confirmName?: string } | null;

function EntriesPanel({
  reason,
  channel,
  group,
  activeGroup,
  dlqTotal,
  bump,
  autoRetry,
  onAutoRetryDone,
  onMutated,
}: {
  reason: string | null;
  channel: string | null;
  /** Client-side filter by consumer group (the API has no group selector). */
  group: string | null;
  activeGroup: DlqReasonGroup | undefined;
  dlqTotal: number | undefined;
  bump: number;
  /** `?action=retry` deep link: open the retry confirmation once entries are loaded. */
  autoRetry: boolean;
  onAutoRetryDone: () => void;
  onMutated: () => void;
}) {
  const now = useNow();
  const t = useT();
  const [entries, setEntries] = useState<DlqPage | null>(null);
  const [entriesError, setEntriesError] = useState<string | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [inspect, setInspect] = useState<DeadLetterView | null>(null);
  const [pending, setPending] = useState<Pending>(null);
  const [busy, setBusy] = useState(false);

  // `?action=retry`: open the retry confirmation (never run it) once the first page arrives.
  const autoRetryRef = useRef(autoRetry);
  useEffect(() => {
    const ctrl = new AbortController();
    api
      .dlq({ reason: reason ?? undefined, channel: channel ?? undefined, limit: PAGE }, ctrl.signal)
      .then((p) => {
        setEntriesError(null);
        setEntries(p);
        if (autoRetryRef.current) {
          autoRetryRef.current = false;
          onAutoRetryDone();
          const mine = p.entries.filter((e) => !group || e.group === group);
          const count = group ? mine.length : p.total;
          if (reason && count > 0) {
            setPending({
              kind: 'retry',
              req: group ? { ids: mine.map((e) => [e.channel, e.id] as [string, number]) } : { reason, channel },
              label: [tr('dlq.scope.reason', { reason: reasonText(reason).title }), channel ? tr('dlq.scope.channel', { channel }) : tr('dlq.scope.all'), group ? tr('dlq.scope.group', { group }) : null].filter(Boolean).join(' '),
              count,
              at: Date.now(),
            });
          }
        }
        setSelected((s) => {
          const keep = new Set(p.entries.map(keyOf));
          return new Set([...s].filter((k) => keep.has(k)));
        });
      })
      .catch((e: unknown) => {
        if (!ctrl.signal.aborted) setEntriesError(errorMessage(e));
      });
    return () => ctrl.abort();
    // `group` is fixed per mount (the panel is keyed by it); onAutoRetryDone runs at most once.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [reason, channel, bump, dlqTotal]);

  const loadMore = async () => {
    if (!entries?.entries.length) return;
    setLoadingMore(true);
    try {
      const last = entries.entries[entries.entries.length - 1]!;
      const p = await api.dlq({ reason: reason ?? undefined, channel: channel ?? undefined, limit: PAGE, before: last.id });
      const seen = new Set(entries.entries.map(keyOf));
      setEntries({ total: p.total, entries: [...entries.entries, ...p.entries.filter((e) => !seen.has(keyOf(e)))] });
    } catch (e) {
      toast('error', errorMessage(e));
    } finally {
      setLoadingMore(false);
    }
  };

  const list = useMemo(() => (entries?.entries ?? []).filter((e) => !group || e.group === group), [entries, group]);
  // With a group filter the API total covers every group: count what is loaded instead.
  const total = group ? list.length : (entries?.total ?? 0);
  const allSelected = list.length > 0 && list.every((e) => selected.has(keyOf(e)));
  const selIds = useMemo(() => list.filter((e) => selected.has(keyOf(e))).map((e) => [e.channel, e.id] as [string, number]), [list, selected]);

  // A group filter is client-side, so bulk actions then target the listed entries by id.
  const scopeReq: DlqActionRequest = group ? { ids: list.map((e) => [e.channel, e.id] as [string, number]) } : reason ? { reason, channel } : channel ? { all: true, channel } : { all: true };
  const why = reason != null ? reasonText(reason) : null;
  const scopeLabel = [why ? t('dlq.scope.reason', { reason: why.title }) : null, channel ? t('dlq.scope.channel', { channel }) : t('dlq.scope.all'), group ? t('dlq.scope.group', { group }) : null].filter(Boolean).join(' ');
  const selectedLabel = t('dlq.scope.selected', { count: selIds.length });
  // Name to type before a bulk delete: the channel when the scope has exactly one.
  const scopeChannels = channel ? [channel] : activeGroup ? Object.keys(activeGroup.channels) : [...new Set(list.map((e) => e.channel))];
  const confirmName = scopeChannels.length === 1 ? scopeChannels[0]! : t('dlq.confirm.allWord');
  const permanent = why?.permanent ?? false;
  const openRetryAll = () => setPending({ kind: 'retry', req: scopeReq, label: scopeLabel, count: total, at: Date.now() });



  const run = async () => {
    if (!pending) return;
    setBusy(true);
    try {
      const res = pending.kind === 'retry' ? await api.dlqRetry(pending.req) : await api.dlqDiscard(pending.req);
      toast('success', tr(pending.kind === 'retry' ? 'dlq.toast.retried' : 'dlq.toast.discarded', { count: res.affected }));
      setSelected(new Set());
      setPending(null);
      setInspect(null);
      onMutated();
    } catch (e) {
      toast('error', errorMessage(e));
    } finally {
      setBusy(false);
    }
  };

  const exportNdjson = async () => {
    try {
      const text = await api.dlqExport({ reason: reason ?? undefined, channel: channel ?? undefined });
      const blob = new Blob([text], { type: 'application/x-ndjson' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `fluxmq-dlq${reason ? `-${reason.replace(/[^a-z0-9]+/gi, '-').toLowerCase()}` : ''}-${new Date().toISOString().slice(0, 19).replace(/:/g, '')}.ndjson`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      toast('error', tr('dlq.toast.exportFailed', { error: errorMessage(e) }));
    }
  };

  return (
    <>
      <section className="card">
        <div className="card-head dlq-head">
          <h2 className="dlq-title">{why ? <span className={why.coded ? undefined : 'mono'}>{why.title}</span> : t('dlq.entries.all')}</h2>
          <span className="card-sub">
            {fmtInt(total)} {channel ? t('dlq.scope.channel', { channel }) : ''}
            {activeGroup ? ` · ${t('dlq.entries.first', { when: fmtRelative(activeGroup.first_ms, now) })}` : ''}
          </span>
          <span className="spacer" />
          <div className="toolbar">
            {selIds.length > 0 ? (
              <>
                <span className="muted" style={{ fontSize: 13 }}>
                  {t('common.selectedCount', { count: selIds.length })}
                </span>
                <GateButton need="operate" className="btn btn-sm btn-primary" onClick={() => setPending({ kind: 'retry', req: { ids: selIds }, label: selectedLabel, count: selIds.length, at: Date.now() })}>
                  <RotateCcw size={13} /> {t('dlq.action.retrySelected')}
                </GateButton>
                {/* Discard is destructive: behind the menu, like "Discard all", and confirmed. */}
                <OverflowMenu
                  label={t('dlq.action.more')}
                  items={[
                    {
                      key: 'discard-selected',
                      label: t('dlq.action.discardSelectedMenu', { n: fmtInt(selIds.length) }),
                      icon: <Trash2 size={14} />,
                      danger: true,
                      need: 'operate',
                      onSelect: () => setPending({ kind: 'discard', req: { ids: selIds }, label: selectedLabel, count: selIds.length, at: Date.now() }),
                    },
                  ]}
                />
              </>
            ) : (
              <>
                <GateButton need="operate" className="btn btn-sm" onClick={exportNdjson} disabled={!total}>
                  <Download size={13} /> {t('dlq.action.export')}
                </GateButton>
                {reason ? (
                  // Retry is scoped to the selected reason; a permanent error makes it a secondary action.
                  <GateButton need="operate" className={`btn btn-sm ${permanent ? '' : 'btn-primary'}`} onClick={openRetryAll} disabled={!total}>
                    <RotateCcw size={13} /> {t('dlq.action.retryReason', { n: fmtInt(total) })}
                  </GateButton>
                ) : (
                  <span className="gate-wrap" title={t('dlq.action.pickReason')}>
                    <button className="btn btn-sm" disabled>
                      <RotateCcw size={13} /> {t('dlq.action.retryReason', { n: fmtInt(total) })}
                    </button>
                  </span>
                )}
                <OverflowMenu
                  label={t('dlq.action.more')}
                  items={[
                    {
                      key: 'discard',
                      label: t('dlq.action.discardAllMenu', { n: fmtInt(total) }),
                      icon: <Trash2 size={14} />,
                      danger: true,
                      need: 'operate',
                      disabled: !total,
                      onSelect: () => setPending({ kind: 'discard', req: scopeReq, label: scopeLabel, count: total, at: Date.now(), confirmName }),
                    },
                  ]}
                />
              </>
            )}
          </div>
        </div>
        {why && total > 0 && (
          // What the reason means and what to fix; permanent errors get the danger tone.
          <div className={`dlq-why ${permanent ? 'permanent' : ''}`} role="note">
            {permanent ? <ShieldAlert size={15} /> : <Info size={15} />}
            <span>
              {why.hint}
              {why.coded && <span className="dlq-why-code mono"> {t('dlq.why.raw', { reason: reason ?? '' })}</span>}
            </span>
          </div>
        )}
        <div className="card-body flush">
          {entriesError ? (
            <div style={{ padding: 16 }}>
              <InlineError onRetry={() => onMutated()}>{entriesError}</InlineError>
            </div>
          ) : !entries ? (
            <SkeletonRows rows={8} />
          ) : list.length === 0 ? (
            <EmptyState compact icon={<Inbox size={18} />} title={t('dlq.entries.emptyTitle')}>
              {t('dlq.entries.emptyBody')}
            </EmptyState>
          ) : (
            <div className="table-wrap">
              <table className="table dlq-table">
                <thead>
                  <tr>
                    <th style={{ width: 36 }}>
                      <input
                        type="checkbox"
                        className="checkbox"
                        aria-label={t('dlq.col.selectAll')}
                        checked={allSelected}
                        ref={(el) => {
                          if (el) el.indeterminate = !allSelected && selected.size > 0;
                        }}
                        onChange={() => setSelected(allSelected ? new Set() : new Set(list.map(keyOf)))}
                      />
                    </th>
                    <th>{t('dlq.col.message')}</th>
                    <th>{t('common.channel')}</th>
                    <th>{t('common.group')}</th>
                    {!reason && <th>{t('common.reason')}</th>}
                    <th>{t('common.key')}</th>
                    <th className="r">{t('common.attempts')}</th>
                    <th className="r">{t('dlq.col.dead')}</th>
                  </tr>
                </thead>
                <tbody>
                  {list.map((e) => {
                    const k = keyOf(e);
                    const sel = selected.has(k);
                    return (
                      <tr key={k} className={`clickable ${sel ? 'selected-row' : ''}`} onClick={() => setInspect(e)}>
                        <td onClick={(ev) => ev.stopPropagation()}>
                          <input
                            type="checkbox"
                            className="checkbox"
                            aria-label={t('dlq.col.selectOne', { id: e.id })}
                            checked={sel}
                            onChange={() => {
                              const next = new Set(selected);
                              if (sel) next.delete(k);
                              else next.add(k);
                              setSelected(next);
                            }}
                          />
                        </td>
                        <td>
                          <span className="dlq-type">{e.message.headers[HEADERS.EVENT_TYPE] ?? t('dlq.col.messageFallback')}</span>
                          <span className="faint mono" style={{ fontSize: 12, marginLeft: 8 }}>
                            #{e.id}
                          </span>
                        </td>
                        <td>
                          <Link to={channelPath(e.channel)} className="ch-name" onClick={(ev) => ev.stopPropagation()}>
                            {e.channel}
                          </Link>
                        </td>
                        <td className="mono muted">{e.group === ROUTER_GROUP ? <span title={t('dlq.routerHint')}>{ROUTER_SHORT}</span> : e.group}</td>
                        {!reason && (
                          <td className="truncate" style={{ maxWidth: 220 }} title={e.reason}>
                            <span className="text-critical">{reasonText(e.reason).title}</span>
                          </td>
                        )}
                        <td className="mono muted truncate" style={{ maxWidth: 160 }}>
                          {e.message.key ?? '—'}
                        </td>
                        <td className="r num">{e.attempts.length}</td>
                        <td className="r num muted" title={fmtDateTime(e.dead_at_ms)}>
                          {fmtRelative(e.dead_at_ms, now)}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
              {list.length < total && (
                <div className="list-foot" style={{ position: 'static' }}>
                  <button className="btn btn-sm" onClick={loadMore} disabled={loadingMore}>
                    {loadingMore ? t('common.loadingEllipsis') : t('dlq.entries.loadMore', { n: fmtInt(total - list.length) })}
                  </button>
                </div>
              )}
            </div>
          )}
        </div>
      </section>

      <Drawer
        open={!!inspect}
        onClose={() => setInspect(null)}
        title={inspect ? `${inspect.message.headers[HEADERS.EVENT_TYPE] ?? t('dlq.drawer.fallback')} #${inspect.id}` : ''}
        subtitle={inspect ? `${inspect.channel} / ${inspect.group} · ${t('dlq.drawer.died', { when: fmtRelative(inspect.dead_at_ms, now) })}` : undefined}
      >
        {inspect && (
          <div className="dlq-inspector">
            <DrawerReason reason={inspect.reason} />
            <MessageInspector
              channel={inspect.channel}
              message={inspect.message}
              deliveries={{ [inspect.group]: inspect.attempts }}
              maxAttempts={inspect.attempts.length}
              deadGroup={inspect.group}
              extraActions={
                <>
                  <GateButton need="operate" className="btn btn-sm btn-primary" onClick={() => setPending({ kind: 'retry', req: { ids: [[inspect.channel, inspect.id]] }, label: `#${inspect.id}`, count: 1, at: Date.now() })}>
                    <RotateCcw size={13} /> {t('dlq.action.retry')}
                  </GateButton>
                  {/* Destructive action kept apart from Retry (right edge, danger colour). */}
                  <span className="spacer" />
                  <span className="dlq-danger-sep" aria-hidden />
                  <GateButton need="operate" className="btn btn-sm btn-ghost text-critical" onClick={() => setPending({ kind: 'discard', req: { ids: [[inspect.channel, inspect.id]] }, label: `#${inspect.id}`, count: 1, at: Date.now() })}>
                    <Trash2 size={13} /> {t('dlq.action.discard')}
                  </GateButton>
                </>
              }
              replay={false}
            />
          </div>
        )}
      </Drawer>

      <ConfirmDialog
        open={!!pending}
        title={pending ? t(pending.kind === 'retry' ? 'dlq.confirm.retryTitle' : 'dlq.confirm.discardTitle', { count: pending.count }) : ''}
        description={pending?.kind === 'retry' ? t('dlq.confirm.retryBody', { scope: pending.label }) : t('dlq.confirm.discardBody', { scope: pending?.label ?? '' })}
        confirmLabel={pending?.kind === 'retry' ? t('dlq.action.retry') : t('dlq.action.discardPermanently')}
        danger={pending?.kind === 'discard'}
        busy={busy}
        onConfirm={run}
        onClose={() => setPending(null)}
        typeToConfirm={pending?.kind === 'discard' && pending.confirmName ? pending.confirmName : undefined}
      >
        {pending && (
          <>
            <p className={pending.kind === 'discard' ? 'confirm-snapshot' : 'confirm-snapshot neutral'}>
              {t(pending.kind === 'discard' ? 'dlq.confirm.snapshotDiscard' : 'dlq.confirm.snapshotRetry', { count: pending.count, time: fmtClock(pending.at, true) })}
            </p>
            {!pending.req.ids && total !== pending.count && <p className="confirm-snapshot">{t('dlq.confirm.changed', { count: total })}</p>}
            {pending.kind === 'retry' && why && permanent && <p className="confirm-snapshot">{why.hint}</p>}
          </>
        )}
      </ConfirmDialog>
    </>
  );
}

/** Reason block in the inspector: human title, the raw code when it differs, and the hint. */
function DrawerReason({ reason }: { reason: string }) {
  const t = useT();
  const w = reasonText(reason);
  return (
    <div className="inline-error dlq-drawer-reason">
      <span>
        <b>{t('dlq.drawer.reason')}</b> <span className={w.coded ? undefined : 'mono'}>{w.title}</span>
        {w.coded && <span className="mono faint"> · {reason}</span>}
        <span className="dlq-drawer-hint">{w.hint}</span>
      </span>
    </div>
  );
}

/** Newest entry per reason (shows which group fails and on what message). At most 8 reasons. */
function useReasonSamples(groups: DlqReasonGroup[], channel: string | null): Record<string, DeadLetterView | undefined> {
  const [out, setOut] = useState<Record<string, DeadLetterView | undefined>>({});
  const key = groups
    .slice(0, 8)
    .map((g) => `${g.reason}@${g.last_ms}`)
    .join('|');
  useEffect(() => {
    const ctrl = new AbortController();
    for (const g of groups.slice(0, 8)) {
      api
        .dlq({ reason: g.reason, channel: channel ?? undefined, limit: 1 }, ctrl.signal)
        .then((p) => setOut((o) => ({ ...o, [g.reason]: p.entries[0] })))
        .catch(() => undefined);
    }
    return () => ctrl.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps -- refetch when reasons or their newest entry change
  }, [key, channel]);
  return out;
}

export default function DeadLettersPage() {
  const [params, setParams] = useSearchParams();
  const reason = params.get('reason');
  const channel = params.get('channel');
  const group = params.get('group');
  const autoRetry = params.get('action') === 'retry';
  const now = useNow();
  const t = useT();
  // Refetch when the cluster-wide DLQ total moves (cheap signal from telemetry).
  const dlqTotal = useTopicSelect<ClusterOverview, number | undefined>(TOPICS.CLUSTER, (o) => o?.dlq_total);
  const [bump, setBump] = useState(0);
  const summary = useAsync(() => api.dlqSummary(), [dlqTotal, bump]);

  const groups = summary.data?.groups ?? [];
  // A single reason needs no "All reasons" card: it is selected implicitly.
  const effReason = reason ?? (groups.length === 1 ? groups[0]!.reason : null);
  const activeGroup = groups.find((g) => g.reason === effReason);
  const samples = useReasonSamples(groups, channel);

  const setParam = (k: string, v: string | null) => {
    const next = new URLSearchParams(params);
    if (v) next.set(k, v);
    else next.delete(k);
    setParams(next, { replace: true });
  };
  const setReason = (r: string | null) => setParam('reason', r);

  return (
    <div className="page">
      <div className="page-head">
        <div className="titles">
          <h1>{t('dlq.title')}</h1>
          <p>{t('dlq.subtitle')}</p>
        </div>
      </div>

      {summary.error && !summary.data ? (
        <InlineError onRetry={summary.reload}>{t('dlq.summaryError', { error: errorMessage(summary.error) })}</InlineError>
      ) : !summary.data ? (
        <div className="reason-grid">
          {[0, 1, 2].map((i) => (
            <div key={i} className="card reason-card">
              <SkeletonRows rows={2} h={30} />
            </div>
          ))}
        </div>
      ) : summary.data.total === 0 ? (
        <section className="card">
          <EmptyState icon={<Inbox size={20} />} title={t('dlq.empty.title')}>
            {t.rich('dlq.empty.body', { maxAttempts: <i>{t('dlq.empty.maxAttempts')}</i> })}
          </EmptyState>
        </section>
      ) : (
        <>
          {(group || channel) && (
            <div className="dlq-filters">
              {channel && (
                <button className="chip" onClick={() => setParam('channel', null)} aria-label={t('dlq.filter.remove', { what: channel })}>
                  {t('dlq.filter.channel', { channel })} <X size={12} />
                </button>
              )}
              {group && (
                <button className="chip" onClick={() => setParam('group', null)} aria-label={t('dlq.filter.remove', { what: group })}>
                  {t('dlq.filter.group', { group })} <X size={12} />
                </button>
              )}
            </div>
          )}
          <div className="reason-grid" role="list" aria-label={t('dlq.reasons.aria')}>
            {groups.length > 1 && (
              <button role="listitem" className={`card reason-card ${effReason == null ? 'selected' : ''}`} onClick={() => setReason(null)} aria-pressed={effReason == null}>
                <span className="rc-count num">{fmtCount(summary.data.total)}</span>
                <span className="rc-reason">{t('dlq.reasons.all')}</span>
                <span className="rc-meta faint">{t('dlq.reasons.distinct', { count: groups.length })}</span>
              </button>
            )}
            {groups.map((g) => {
              const sample = samples[g.reason];
              const w = reasonText(g.reason);
              return (
                <button role="listitem" key={g.reason} className={`card reason-card ${g.reason === effReason ? 'selected' : ''}`} onClick={() => setReason(g.reason)} aria-pressed={g.reason === effReason}>
                  <span className="rc-count num">{fmtCount(g.count)}</span>
                  <span className={`rc-reason ${w.coded ? '' : 'mono'}`} title={w.hint}>
                    {w.title}
                  </span>
                  {w.coded && <span className="rc-code mono faint">{g.reason}</span>}
                  <span className="rc-channels">
                    {Object.entries(g.channels)
                      .sort((a, b) => b[1] - a[1])
                      .slice(0, 3)
                      .map(([c, n]) => (
                        <span key={c} className="rc-chip">
                          {c} <b className="num">{fmtCount(n)}</b>
                        </span>
                      ))}
                  </span>
                  {sample && (
                    <span className="rc-sample">
                      <span className="faint">{t('dlq.reasons.group')}</span> <span className="mono">{sample.group === ROUTER_GROUP ? ROUTER_SHORT : sample.group}</span>
                      <span className="faint"> · {t('dlq.reasons.sample')}</span> {sample.message.headers[HEADERS.EVENT_TYPE] ?? t('dlq.col.messageFallback')}{' '}
                      <span className="faint mono">#{sample.id}</span>
                    </span>
                  )}
                  {w.permanent && <span className="rc-permanent">{t('dlq.reasons.permanent')}</span>}
                  <span className="rc-meta faint">{t('dlq.reasons.last', { when: fmtRelative(g.last_ms, now) })}</span>
                </button>
              );
            })}
          </div>

          <EntriesPanel
            key={`${effReason ?? ''}|${channel ?? ''}|${group ?? ''}`}
            reason={effReason}
            channel={channel}
            group={group}
            activeGroup={activeGroup}
            dlqTotal={dlqTotal}
            bump={bump}
            autoRetry={autoRetry}
            onAutoRetryDone={() => setParam('action', null)}
            onMutated={() => setBump((b) => b + 1)}
          />
        </>
      )}

    </div>
  );
}
