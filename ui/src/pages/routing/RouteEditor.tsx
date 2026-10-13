import { useEffect, useState } from 'react';
import { Link, useNavigate, useParams, useSearchParams } from 'react-router';
import { AlertTriangle, ArrowRight, ChevronRight, ListFilter, Loader2, Lock, Route as RouteIcon, Trash2, X } from 'lucide-react';
import { api, ApiError, errorMessage } from '../../api/client';
import { HEADERS, TOPICS, type ChannelsSummary, type Condition, type MessageView, type Route, type RouteTestResponse } from '../../api/types';
import { FilterBuilder } from '../../components/routing/FilterBuilder';
import { useFilterBuilder } from '../../components/routing/useFilterBuilder';
import { useAsync, useNow } from '../../lib/hooks';
import { fmtCount, fmtInt, fmtNumber, fmtRelative, fmtTimeMs } from '../../lib/format';
import { channelPath } from '../../lib/labels';
import { channelNames } from '../../store/selectors';
import { useTopicSelect } from '../../telemetry';
import { removeRoute, upsertRoute, useRoutes, useRoutesPolling } from '../../store/routes';
import { toast } from '../../store/ui';
import { ConfirmDialog, EmptyState, InlineError, Segmented, Skeleton, Switch } from '../../components/ui';
import { GateButton } from '../../components/auth/Gate';
import { useGate } from '../../components/auth/useGate';
import { useT, type TKey } from '../../i18n';
import './routing-page.css';

/** Rule keywords of the DSL, shown verbatim in both languages. */
const DSL_KW = { from: 'FROM', when: 'WHEN', then: 'THEN' } as const;


// ───────────────────────────── targets ─────────────────────────────

function TargetsPicker({ targets, onChange, channels, source, readOnly, invalid }: { targets: string[]; onChange: (t: string[]) => void; channels: string[]; source: string; readOnly: boolean; invalid: boolean }) {
  const t = useT();
  const options = channels.filter((c) => c !== source && !targets.includes(c));
  return (
    <div className={`targets ${invalid ? 'invalid' : ''}`}>
      {targets.map((tg) => (
        <span key={tg} className="target-step">
          <ArrowRight size={14} className="faint" aria-hidden />
          <span className={`chip target-chip ${channels.length && !channels.includes(tg) ? 'missing' : ''}`} title={channels.length && !channels.includes(tg) ? t('routing.targetMissing') : undefined}>
            {tg}
            {!readOnly && (
              <button type="button" aria-label={t('routing.removeTarget', { name: tg })} onClick={() => onChange(targets.filter((x) => x !== tg))}>
                <X size={12} />
              </button>
            )}
          </span>
        </span>
      ))}
      {!readOnly && (
        <span className="target-step">
          <ArrowRight size={14} className="faint" aria-hidden />
          <select
            className="select target-add"
            value=""
            aria-label={t('routing.addTargetAria')}
            disabled={!options.length}
            onChange={(e) => {
              if (e.target.value) onChange([...targets, e.target.value]);
            }}
          >
            <option value="">{options.length ? t('routing.addTarget') : source ? t('routing.noOtherChannels') : t('routing.pickSourceFirst')}</option>
            {options.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
        </span>
      )}
    </div>
  );
}

// ───────────────────────────── backtest preview ─────────────────────────────

type Win = '1h' | '24h' | '7d';
const WIN_MS: Record<Win, number> = { '1h': 3_600_000, '24h': 86_400_000, '7d': 7 * 86_400_000 };
const WIN_LABEL: Record<Win, TKey> = { '1h': 'routing.win.1h', '24h': 'routing.win.24h', '7d': 'routing.win.7d' };

function sampleType(m: MessageView, fallback: string): string {
  return m.headers[HEADERS.EVENT_TYPE] ?? fallback;
}

function Preview({ source, conditions, pendingReason }: { source: string; conditions: Condition[] | null; pendingReason: string | null }) {
  const t = useT();
  const [win, setWin] = useState<Win>('24h');
  const key = conditions && source ? JSON.stringify([source, conditions, win]) : null;
  const [res, setRes] = useState<{ key: string; data: RouteTestResponse | null; error: string | null; forbidden?: boolean } | null>(null);
  const navigate = useNavigate();

  useEffect(() => {
    if (!key || !conditions) return;
    const ctrl = new AbortController();
    const timer = setTimeout(() => {
      api
        .testRoute({ source, conditions, window_ms: WIN_MS[win], sample_limit: 20 }, ctrl.signal)
        .then((data) => setRes({ key, data, error: null }))
        .catch((e: unknown) => {
          if (ctrl.signal.aborted) return;
          setRes({ key, data: null, error: errorMessage(e), forbidden: e instanceof ApiError && e.isForbidden });
        });
    }, 450);
    return () => {
      clearTimeout(timer);
      ctrl.abort();
    };
    // conditions are captured through `key`
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);

  const stale = res != null && res.key !== key;
  const data = res?.data ?? null;
  const pct = data && data.scanned ? (data.matched / data.scanned) * 100 : 0;

  return (
    <section className="card preview-card" aria-label={t('routing.previewAria')}>
      <div className="card-head">
        <h2>{t('routing.preview')}</h2>
        <span className="spacer" />
        <Segmented value={win} onChange={setWin} label={t('routing.window')} options={[{ value: '1h', label: t('routing.winShort.1h') }, { value: '24h', label: t('routing.winShort.24h') }, { value: '7d', label: t('routing.winShort.7d') }]} />
      </div>
      <div className="card-body">
        {!source ? (
          <p className="muted">{t('routing.pickSourceToBacktest')}</p>
        ) : !conditions ? (
          <p className="muted">{pendingReason ?? t('routing.finishConditions')}</p>
        ) : res?.forbidden ? (
          <p className="muted">{t('routing.backtestForbidden')}</p>
        ) : res?.error && !stale ? (
          <InlineError>{res.error}</InlineError>
        ) : !data ? (
          <>
            <Skeleton w={140} h={12} />
            <Skeleton w={200} h={34} style={{ marginTop: 10 }} />
            <Skeleton h={6} style={{ marginTop: 14 }} />
          </>
        ) : (
          <div className={stale ? 'preview-stale' : ''}>
            <div className="pv-label">
              {t('routing.matchedWindow', { window: t(WIN_LABEL[win]) })} {stale && <Loader2 size={12} className="spin" aria-label={t('routing.updating')} />}
            </div>
            <div className="pv-big">
              <span className="num">{fmtInt(data.matched)}</span>
              <span className="pv-of">
                {t('routing.ofScanned', { count: fmtCount(data.scanned) })}
                {data.scanned > 0 && <> · {pct < 0.01 && pct > 0 ? `<${fmtNumber(0.01)}` : fmtNumber(pct, pct < 1 ? 2 : 1)}%</>}
              </span>
            </div>
            <div className="pv-bar" role="meter" aria-valuenow={Math.round(pct)} aria-valuemin={0} aria-valuemax={100} aria-label={t('routing.meterAria')}>
              <span style={{ width: `${Math.max(pct > 0 ? 0.8 : 0, Math.min(100, pct))}%` }} />
            </div>
            {data.truncated && (
              <p className="pv-note">
                <AlertTriangle size={12} /> {t('routing.scanCap', { count: fmtCount(data.scanned), window: t(WIN_LABEL[win]) })}
              </p>
            )}
            {conditions.length === 0 && <p className="pv-note">{t('routing.noConditionsAll')}</p>}
          </div>
        )}
      </div>
      <p className="pv-history-note faint">
        {t('routing.historyNote')}
      </p>
      {data && data.samples.length > 0 && (
        <div className="pv-samples">
          <div className="pv-samples-head">
            {t('routing.sampleMatches')} <span className="faint">{t('routing.sampleMatchesHint')}</span>
          </div>
          <ul>
            {[...data.samples].reverse().map((m) => (
              <li key={`${m.partition}:${m.offset}`}>
                <button type="button" onClick={() => navigate(`${channelPath(source, 'messages')}?p=${m.partition}&o=${m.offset}&sel=${m.partition}:${m.offset}`)}>
                  <span className="pv-line1">
                    <span className="pv-type truncate">{sampleType(m, t('routing.sampleTypeFallback'))}</span>
                    <span className="mono truncate pv-key">{m.key ?? <span className="faint">{t('routing.noKey')}</span>}</span>
                  </span>
                  <span className="pv-line2 mono faint">
                    {fmtTimeMs(m.timestamp_ms)} · p{m.partition}:{fmtInt(m.offset)}
                  </span>
                  <ChevronRight size={13} className="faint pv-chev" />
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}
      {data && data.samples.length === 0 && data.matched === 0 && !stale && (
        <div className="pv-samples">
          <EmptyState compact icon={<ListFilter size={18} />} title={t('routing.nothingMatchedTitle')}>
            {t('routing.nothingMatchedBody', { window: t(WIN_LABEL[win]) })}
          </EmptyState>
        </div>
      )}
    </section>
  );
}

function RouteStatsCard({ id }: { id: string }) {
  const t = useT();
  const s = useRoutes((st) => st.stats[id]);
  const rate = useRoutes((st) => st.rates[id]);
  const now = useNow();
  if (!s) return null;
  const items: [string, string, string?, boolean?][] = [
    [t('routing.stat.evaluated'), fmtCount(s.evaluated)],
    [t('routing.col.matched'), fmtCount(s.matched), rate != null ? `${fmtCount(Math.round(rate))}${t.lang === 'ru' ? '/с' : '/s'}` : undefined],
    [t('routing.col.forwarded'), fmtCount(s.forwarded), t('routing.stat.perTarget')],
    [t('routing.col.failed'), fmtCount(s.failed), undefined, true],
  ];
  return (
    <section className="card route-stats">
      <dl>
        {items.map(([k, v, sub, isFailed]) => (
          <div key={k}>
            <dt>{k}</dt>
            <dd className={`num ${isFailed && s.failed > 0 ? 'text-critical' : ''}`}>
              {v}
              {sub && <small className="faint"> {sub}</small>}
            </dd>
          </div>
        ))}
        <div>
          <dt>{t('routing.col.lastMatch')}</dt>
          <dd>{s.last_matched_ms ? fmtRelative(s.last_matched_ms, now) : '—'}</dd>
        </div>
      </dl>
      <p className="faint">
        {t('routing.statsNote')}
      </p>
    </section>
  );
}

// ───────────────────────────── editor ─────────────────────────────

interface Baseline {
  name: string;
  source: string;
  enabled: boolean;
  conditions: Condition[];
  targets: string[];
}

function Editor({ initial, presetSource, presetFilter }: { initial: Route | null; presetSource: string | null; presetFilter: string | null }) {
  const t = useT();
  const navigate = useNavigate();
  const gate = useGate('admin');
  const readOnly = !gate.allowed;
  const channels = useTopicSelect<ChannelsSummary, string[]>(TOPICS.CHANNELS, channelNames);
  const isNew = initial == null;

  const [baseline, setBaseline] = useState<Baseline>(() => ({
    name: initial?.name ?? '',
    source: initial?.source ?? presetSource ?? '',
    enabled: initial?.enabled ?? true,
    conditions: initial?.conditions ?? [],
    targets: initial?.targets ?? [],
  }));
  const [name, setName] = useState(baseline.name);
  const [source, setSource] = useState(baseline.source);
  const [enabled, setEnabled] = useState(baseline.enabled);
  const [targets, setTargets] = useState<string[]>(baseline.targets);
  const [showErrors, setShowErrors] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const fb = useFilterBuilder({ source, initial: initial?.conditions ?? null, presetFilter });
  const { conditions, pendingReason, mode } = fb;

  const current: Baseline = { name: name.trim(), source, enabled, conditions: conditions ?? [], targets };
  const dirty = JSON.stringify(current) !== JSON.stringify(baseline) || fb.hasDraft;

  const problems: string[] = [];
  if (!name.trim()) problems.push(t('routing.problem.name'));
  if (!source) problems.push(t('routing.problem.source'));
  if (!conditions) problems.push(mode === 'text' ? t('routing.problem.validFilter') : t('routing.problem.conditions'));
  if (!targets.length) problems.push(t('routing.problem.target'));

  const save = async () => {
    setShowErrors(true);
    if (problems.length || !conditions) return;
    setSaving(true);
    setSaveError(null);
    try {
      if (isNew) {
        const r = await api.createRoute({ name: name.trim(), source, enabled, conditions, targets });
        upsertRoute(r);
        toast('success', t('routing.toastCreated', { name: r.name }));
        navigate(`/routing/${encodeURIComponent(r.id)}`, { replace: true });
      } else {
        const r = await api.updateRoute(initial.id, { name: name.trim(), enabled, conditions, targets });
        upsertRoute(r);
        setBaseline({ name: r.name, source: r.source, enabled: r.enabled, conditions: r.conditions, targets: r.targets });
        toast('success', t('routing.toastSaved'));
      }
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) setSaveError(errorMessage(e));
    } finally {
      setSaving(false);
    }
  };

  const doDelete = async () => {
    if (!initial) return;
    setDeleting(true);
    try {
      await api.deleteRoute(initial.id);
      removeRoute(initial.id);
      toast('success', t('routing.toastDeleted', { name: initial.name }));
      navigate('/routing');
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) toast('error', errorMessage(e));
      setDeleting(false);
    }
  };

  return (
    <div className="page route-editor">
      <div>
        <nav className="breadcrumbs" aria-label={t('routing.breadcrumb')}>
          <Link to="/routing">{t('routing.title')}</Link>
          <ChevronRight size={12} />
          <span aria-current="page">{isNew ? t('routing.newRouteCrumb') : initial.name}</span>
        </nav>
        <div className="page-head re-head">
          <div className="titles re-titles">
            <input className="re-name" value={name} onChange={(e) => setName(e.target.value)} placeholder={t('routing.namePlaceholder')} aria-label={t('routing.nameAria')} readOnly={readOnly} autoFocus={isNew && !readOnly} />
            {showErrors && !name.trim() && <span className="field-error">{t('routing.nameRequired')}</span>}
          </div>
          <div className="page-actions">
            <span className="re-enabled">
              <Switch checked={enabled} onChange={setEnabled} label={t('routing.enabled')} disabled={readOnly} title={readOnly ? (gate.reason ?? undefined) : undefined} />
              <span>{enabled ? t('routing.enabled') : t('routing.pausedLabel')}</span>
            </span>
            {!isNew && (
              <GateButton need="admin" className="btn btn-ghost" onClick={() => setDeleteOpen(true)}>
                <Trash2 size={14} /> {t('common.delete')}
              </GateButton>
            )}
            <Link to="/routing" className="btn">
              {dirty && !readOnly ? t('common.cancel') : t('common.back')}
            </Link>
            <GateButton need="admin" className="btn btn-primary" onClick={save} disabled={saving || (!dirty && !isNew)}>
              {saving ? t('common.saving') : isNew ? t('routing.createRoute') : t('routing.saveChanges')}
            </GateButton>
          </div>
        </div>
        {readOnly && (
          <div className="re-readonly">
            <Lock size={13} /> <span title={gate.reason ?? undefined}>{t('routing.readOnly')}</span>
          </div>
        )}
      </div>

      <div className="re-grid">
        <div className="re-main">
          <section className="card rule-card">
            <div className="rule-section">
              <div className="rule-k">{DSL_KW.from}</div>
              <div className="rule-v">
                {isNew ? (
                  <select className={`select re-source ${showErrors && !source ? 'invalid' : ''}`} value={source} onChange={(e) => {
                    setSource(e.target.value);
                    setTargets(targets.filter((x) => x !== e.target.value));
                  }} aria-label={t('routing.sourceAria')} disabled={readOnly}>
                    <option value="">{t('routing.selectSource')}</option>
                    {channels.map((c) => (
                      <option key={c} value={c}>
                        {c}
                      </option>
                    ))}
                  </select>
                ) : (
                  <span className="re-source-fixed">
                    <Link to={channelPath(source, 'routing')} className="chip static">
                      {source}
                    </Link>
                    <span className="faint">{t('routing.sourceFixed')}</span>
                  </span>
                )}
                {fb.sampleCount != null && <span className="faint re-src-hint">{t('routing.fieldSuggestions', { count: fb.sampleCount })}</span>}
              </div>
            </div>

            <div className="rule-section">
              <div className="rule-k">{DSL_KW.when}</div>
              <div className="rule-v">
                <FilterBuilder fb={fb} readOnly={readOnly} showErrors={showErrors} emptyText={t('routing.whenEmpty')} />
              </div>
            </div>

            <div className="rule-section">
              <div className="rule-k">{DSL_KW.then}</div>
              <div className="rule-v">
                <TargetsPicker targets={targets} onChange={setTargets} channels={channels} source={source} readOnly={readOnly} invalid={showErrors && !targets.length} />
                <span className="faint re-src-hint">{t('routing.thenHint')}</span>
                {showErrors && !targets.length && <span className="field-error">{t('routing.targetRequired')}</span>}
              </div>
            </div>
          </section>
          {showErrors && problems.length > 0 && <InlineError>{t('routing.toSave', { list: problems.join(', ') })}</InlineError>}
          {saveError && <InlineError>{saveError}</InlineError>}
          {!isNew && <RouteStatsCard id={initial.id} />}
        </div>
        <aside className="re-side">
          <Preview source={source} conditions={conditions} pendingReason={pendingReason} />
        </aside>
      </div>

      <ConfirmDialog
        open={deleteOpen}
        title={t('routing.deleteTitle', { name: initial?.name ?? '' })}
        description={t('routing.deleteBody')}
        confirmLabel={t('routing.deleteRoute')}
        danger
        busy={deleting}
        onConfirm={doDelete}
        onClose={() => setDeleteOpen(false)}
      />
    </div>
  );
}

export default function RouteEditorPage() {
  const t = useT();
  const { id } = useParams();
  const [params] = useSearchParams();
  useRoutesPolling(5000);
  const route = useAsync((signal) => (id ? api.getRoute(id, signal) : Promise.resolve(null)), [id]);
  if (id && route.error) {
    return (
      <div className="page">
        <EmptyState
          icon={<RouteIcon size={20} />}
          title={route.error instanceof ApiError && route.error.isNotFound ? t('routing.notFound') : t('routing.loadFailed')}
          actions={
            <Link to="/routing" className="btn">
              {t('routing.backToRouting')}
            </Link>
          }
        >
          {errorMessage(route.error)}
        </EmptyState>
      </div>
    );
  }
  if (id && (route.loading || !route.data)) {
    return (
      <div className="page">
        <Skeleton w={240} h={28} />
        <Skeleton h={320} style={{ marginTop: 20 }} />
      </div>
    );
  }
  return <Editor key={route.data?.id ?? 'new'} initial={route.data ?? null} presetSource={params.get('source')} presetFilter={params.get('filter')} />;
}
