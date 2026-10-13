import { useState } from 'react';
import { useShallow } from 'zustand/react/shallow';
import { Link, useNavigate, useParams, useSearchParams } from 'react-router';
import { ChevronRight, CircleCheck, CircleX, Inbox, Loader2, Lock, RotateCcw, Send, Trash2, Users, Webhook } from 'lucide-react';
import { api, ApiError, errorMessage } from '../../api/client';
import { TOPICS, WEBHOOK_LIMITS, isSystemChannel, webhookGroup, type ChannelsSummary, type WebhookSubscription, type WebhookTestResult } from '../../api/types';
import { useAsync, useNow } from '../../lib/hooks';
import { fmtCount, fmtDateTime, fmtMs, fmtRelative } from '../../lib/format';
import { channelPath } from '../../lib/labels';
import {
  draftToCreateWebhook,
  draftToUpdateWebhook,
  emptyWebhookDraft,
  exampleEnvelope,
  setFormat,
  errorDetail,
  shortError,
  urlHost,
  validateWebhookDraft,
  VERIFY_NODE,
  VERIFY_PYTHON,
  webhookToDraft,
  type WebhookDraft,
} from '../../lib/webhookForm';
import { channelNames } from '../../store/selectors';
import { useTopicSelect } from '../../telemetry';
import { removeWebhook, upsertWebhook, useWebhooks, useWebhooksPolling } from '../../store/webhooks';
import { toast } from '../../store/ui';
import { ConfirmDialog, EmptyState, InlineError, Segmented, Skeleton, Switch } from '../../components/ui';
import { CodeBlock, Field, HeadersEditor, SecretInput } from '../../components/forms';
import { FilterBuilder } from '../../components/routing/FilterBuilder';
import { useFilterBuilder } from '../../components/routing/useFilterBuilder';
import { GateButton } from '../../components/auth/Gate';
import { useGate } from '../../components/auth/useGate';
import { StatusCode } from './WebhooksPage';
import { ActivityCard, AttemptsCard, BreakerBanner } from './WebhookActivity';
import { ResetOffsetsDialog } from '../../components/consumers/ResetOffsetsDialog';
import '../routing/routing-page.css';
import './webhooks.css';
import { useT } from '../../i18n';

/** Technical identifiers shown verbatim in both languages. */
const TEST_ID = 'delivery_id = test/…';
const H_DELIVERY_ID = 'X-FluxMQ-Delivery-Id';
const H_ATTEMPT = 'X-FluxMQ-Attempt';
const F_DELIVERY_ID = 'delivery_id';
/** DSL keyword. */
const DSL_FILTER = 'FILTER';
const CODE_LANGS = [{ value: 'node' as const, label: 'Node' }, { value: 'python' as const, label: 'Python' }];

function TestCard({ result, testing, onTest, editing }: { result: WebhookTestResult | null; testing: boolean; onTest: () => void; editing: boolean }) {
  const t = useT();
  return (
    <section className="card wh-test">
      <div className="card-head">
        <h2>{t('webhooks.sendTest')}</h2>
        <span className="spacer" />
        <GateButton need="admin" className="btn btn-sm" onClick={onTest} disabled={testing}>
          {testing ? <Loader2 size={13} className="spin" /> : <Send size={13} />} {t('webhooks.sendTest')}
        </GateButton>
      </div>
      <div className="card-body">
        {!result ? (
          <p className="muted wh-test-hint">
            {t.rich('webhooks.testHint', { id: <span className="mono">{TEST_ID}</span> })}
            {editing ? t('webhooks.testHintEditing') : ''}
          </p>
        ) : (
          <div className="wh-test-result">
            <div className={`test-result ${result.ok ? 'ok' : 'fail'}`}>
              {result.ok ? <CircleCheck size={15} /> : <CircleX size={15} />}
              {result.status != null ? <StatusCode status={result.status} /> : null}
              <span title={result.error_detail ?? undefined}>{result.ok ? t('webhooks.delivered') : result.error && result.error !== `HTTP ${result.status}` ? shortError(result.error) : result.status != null ? t('webhooks.endpointError') : t('webhooks.failed')}</span>
              <span className="faint">· {fmtMs(result.latency_ms)}</span>
            </div>
            {!result.ok && errorDetail(result.error, result.error_detail) && <p className="faint wh-test-detail">{errorDetail(result.error, result.error_detail)}</p>}
            {result.response_snippet && (
              <pre className="mono wh-snippet" aria-label={t('webhooks.responseBody')}>
                {result.response_snippet}
              </pre>
            )}
          </div>
        )}
      </div>
    </section>
  );
}

function StatsCard({ w }: { w: WebhookSubscription }) {
  const t = useT();
  const s = useWebhooks((st) => st.stats[w.id]);
  const now = useNow();
  if (!s) return null;
  const items: [string, React.ReactNode, string?][] = [
    [t('webhooks.col.delivered'), fmtCount(s.delivered)],
    [t('webhooks.col.failed'), <span className={s.failed ? 'text-critical' : ''}>{fmtCount(s.failed)}</span>],
    [t('webhooks.stat.skipped'), fmtCount(s.skipped_by_filter)],
    [t('webhooks.stat.latency'), s.latency_p99_ms > 0 || s.delivered > 0 ? `${fmtMs(s.latency_p50_ms)} / ${fmtMs(s.latency_p99_ms)}` : '—'],
    [t('webhooks.col.inFlight'), `${s.in_flight} / ${w.max_in_flight}`],
    [t('webhooks.col.lastStatus'), <StatusCode status={s.last_status} error={s.last_error} detail={s.last_error_detail} />],
  ];
  return (
    <section className="card route-stats wh-stats">
      <dl>
        {items.map(([k, v]) => (
          <div key={k}>
            <dt>{k}</dt>
            <dd className="num">{v}</dd>
          </div>
        ))}
      </dl>
      <p className="faint">
        {t('webhooks.statsNote')}
        {s.last_delivery_ms ? t('webhooks.lastDelivery', { when: fmtRelative(s.last_delivery_ms, now) }) : ''}.
        {s.last_error ? <span title={s.last_error_detail ?? s.last_error}>{t('webhooks.lastError', { error: shortError(s.last_error) })}</span> : ''}
      </p>
    </section>
  );
}

function Editor({ initial, presetChannel }: { initial: WebhookSubscription | null; presetChannel: string | null }) {
  const t = useT();
  const navigate = useNavigate();
  const gate = useGate('admin');
  const readOnly = !gate.allowed;
  const isNew = initial == null;
  const channels = useTopicSelect<ChannelsSummary, string[]>(TOPICS.CHANNELS, channelNames);
  const existingNames = useWebhooks(useShallow((s) => (s.list ?? []).map((w) => w.name)));
  const [baseline, setBaseline] = useState<WebhookSubscription | null>(initial);
  const [d, setD] = useState<WebhookDraft>(() => (initial ? webhookToDraft(initial) : emptyWebhookDraft(presetChannel ?? '')));
  const fb = useFilterBuilder({ source: d.channel, initial: initial?.filter ?? null, startEmpty: true });
  const [showErrors, setShowErrors] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [testing, setTesting] = useState(false);
  const [test, setTest] = useState<WebhookTestResult | null>(null);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [lang, setLang] = useState<'node' | 'python'>('node');
  const [rewindOpen, setRewindOpen] = useState(false);
  const breaker = useWebhooks((s) => (initial ? s.stats[initial.id]?.breaker : null));
  const chConfig = useAsync(() => (d.channel ? api.getChannel(d.channel) : Promise.resolve(null)), [d.channel]);
  const ackTimeout = chConfig.data?.retry.ack_timeout_ms ?? null;

  const errs = validateWebhookDraft(d, { isNew, existingNames, ackTimeoutMs: ackTimeout });
  const shown = showErrors ? errs : {};
  const set = (p: Partial<WebhookDraft>) => setD({ ...d, ...p });
  const filter = fb.conditions;
  const dirty = isNew || fb.hasDraft || (baseline != null && JSON.stringify(draftToUpdateWebhook(d, filter ?? [])) !== JSON.stringify(draftToUpdateWebhook(webhookToDraft(baseline), baseline.filter)));
  const problems = Object.keys(errs).length + (filter ? 0 : 1);

  const save = async () => {
    setShowErrors(true);
    if (problems || !filter) return;
    setSaving(true);
    setSaveError(null);
    try {
      if (isNew) {
        const w = await api.createWebhook(draftToCreateWebhook(d, filter));
        upsertWebhook(w);
        toast('success', t('webhooks.toastCreated', { name: w.name, from: w.start === 'earliest' ? t('webhooks.fromOldest') : w.start === 'latest' ? t('webhooks.fromNow') : fmtDateTime(w.start.timestamp_ms) }));
        navigate(`/webhooks/${encodeURIComponent(w.id)}`, { replace: true });
      } else {
        const w = await api.updateWebhook(initial.id, draftToUpdateWebhook(d, filter));
        upsertWebhook(w);
        setBaseline(w);
        setD(webhookToDraft(w));
        toast('success', t('webhooks.toastSaved'));
      }
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) setSaveError(errorMessage(e));
    } finally {
      setSaving(false);
    }
  };

  // New: test the draft as typed. Existing: send the edits (update shape, REDACTED = stored
  // secret / header value); the server merges them over the stored subscription, unsaved.
  const runTest = async () => {
    setShowErrors(true);
    if (problems || !filter) return;
    setTesting(true);
    setTest(null);
    try {
      const r = isNew ? await api.testWebhookDraft(draftToCreateWebhook(d, filter ?? [])) : await api.testWebhook(initial.id, dirty ? draftToUpdateWebhook(d, filter) : undefined);
      setTest(r);
      if (r.ok) toast('success', t('webhooks.testOk', { status: r.status ?? 200, ms: r.latency_ms }));
      else toast('error', t('webhooks.testFail', { error: r.error ? shortError(r.error) : `HTTP ${r.status}`, ms: r.latency_ms }));
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) toast('error', errorMessage(e));
    } finally {
      setTesting(false);
    }
  };

  const doDelete = async () => {
    if (!initial) return;
    setDeleting(true);
    try {
      await api.deleteWebhook(initial.id);
      removeWebhook(initial.id);
      toast('success', t('webhooks.toastDeleted', { name: initial.name }));
      navigate('/webhooks');
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) toast('error', errorMessage(e));
      setDeleting(false);
    }
  };

  const group = webhookGroup(d.name || '<name>');
  const raw = d.format === 'raw';
  const lim = WEBHOOK_LIMITS;
  const userChannels = channels.filter((c) => !isSystemChannel(c) || c === d.channel);

  return (
    <div className="page route-editor wh-editor">
      <div>
        <nav className="breadcrumbs" aria-label={t('webhooks.breadcrumb')}>
          <Link to="/webhooks">{t('webhooks.title')}</Link>
          <ChevronRight size={12} />
          <span aria-current="page">{isNew ? t('webhooks.newWebhook') : initial.name}</span>
        </nav>
        <div className="page-head re-head">
          <div className="titles re-titles">
            {isNew ? (
              <input className="re-name mono" value={d.name} onChange={(e) => set({ name: e.target.value.toLowerCase().replace(/\s+/g, '-') })} placeholder={t('webhooks.namePlaceholder')} aria-label={t('webhooks.nameAria')} readOnly={readOnly} autoFocus={!readOnly} />
            ) : (
              <h1 className="mono wh-title">{initial.name}</h1>
            )}
            {shown.name ? <span className="field-error">{shown.name}</span> : isNew && <span className="faint wh-name-hint">{t.rich('webhooks.nameHint', { group: <span className="mono">{group}</span> })}</span>}
          </div>
          <div className="page-actions">
            <span className="re-enabled">
              <Switch checked={d.enabled} onChange={(enabled) => set({ enabled })} label={t('webhooks.enabled')} disabled={readOnly} title={readOnly ? (gate.reason ?? undefined) : undefined} />
              <span>{d.enabled ? t('webhooks.enabled') : t('webhooks.pausedLabel')}</span>
            </span>
            {!isNew && (
              <GateButton need="admin" className="btn btn-ghost" onClick={() => setDeleteOpen(true)}>
                <Trash2 size={14} /> {t('common.delete')}
              </GateButton>
            )}
            <Link to="/webhooks" className="btn">
              {dirty && !readOnly && !isNew ? t('common.cancel') : t('common.back')}
            </Link>
            <GateButton need="admin" className="btn btn-primary" onClick={save} disabled={saving || !dirty}>
              {saving ? t('common.saving') : isNew ? t('webhooks.create') : t('webhooks.saveChanges')}
            </GateButton>
          </div>
        </div>
        {readOnly && (
          <div className="re-readonly">
            <Lock size={13} /> <span title={gate.reason ?? undefined}>{t('webhooks.readOnly')}</span>
          </div>
        )}
        {!isNew && breaker && initial.enabled && <BreakerBanner b={breaker} />}
      </div>

      <div className="re-grid">
        <div className="re-main">
          {!isNew && <ActivityCard id={initial.id} />}
          {!isNew && <AttemptsCard id={initial.id} />}
          <section className="card">
            <div className="card-head">
              <h2>{t('webhooks.endpoint')}</h2>
            </div>
            <fieldset className="card-body form-grid fs-plain" disabled={readOnly}>
              <Field label={t('webhooks.channel')} error={shown.channel} hint={isNew ? t('webhooks.channelHintNew') : t('webhooks.channelHintFixed')}>
                {isNew ? (
                  <select className={`select ${shown.channel ? 'invalid' : ''}`} value={d.channel} disabled={readOnly} onChange={(e) => set({ channel: e.target.value })} aria-label={t('webhooks.channel')}>
                    <option value="">{t('webhooks.selectChannel')}</option>
                    {userChannels.map((c) => (
                      <option key={c} value={c}>
                        {c}
                      </option>
                    ))}
                  </select>
                ) : (
                  <span className="wh-fixed">
                    <Link to={channelPath(d.channel)} className="chip static">
                      {d.channel}
                    </Link>
                  </span>
                )}
              </Field>
              <Field label={t('webhooks.format')} hint={raw ? t('webhooks.formatHintRaw') : t('webhooks.formatHintEnvelope')}>
                <Segmented
                  value={d.format}
                  onChange={(f) => setD(setFormat(d, f))}
                  label={t('webhooks.payloadFormat')}
                  options={[
                    { value: 'envelope', label: t('webhooks.formatEnvelope') },
                    { value: 'raw', label: t('webhooks.formatRaw') },
                  ]}
                />
              </Field>
              <Field label={t('webhooks.url')} htmlFor="wh-url" className="span-2" error={shown.url} hint={t('webhooks.urlHint')}>
                <input id="wh-url" className={`input mono ${shown.url ? 'invalid' : ''}`} value={d.url} disabled={readOnly} onChange={(e) => set({ url: e.target.value })} placeholder="https://api.example.com/hooks/fluxmq" />
              </Field>
              <Field label={t('webhooks.secret')} className="span-2" hint={t('webhooks.secretHint')}>
                <SecretInput value={d.secret} onChange={(secret) => set({ secret })} label={t('webhooks.secretAria')} optional disabled={readOnly} placeholder="whsec_…" />
              </Field>
              <Field label={t('webhooks.headers')} className="span-2" hint={t('webhooks.headersHint')}>
                <HeadersEditor rows={d.headers} onChange={(headers) => set({ headers })} disabled={readOnly} error={shown.header} />
              </Field>
            </fieldset>
          </section>

          <section className="card rule-card">
            <div className="rule-section">
              <div className="rule-k">{DSL_FILTER}</div>
              <div className="rule-v">
                <FilterBuilder fb={fb} readOnly={readOnly} showErrors={showErrors} emptyText={t('webhooks.filterEmpty')} allLabel={t('webhooks.filterAll')} />
                {fb.sampleCount != null && <span className="faint re-src-hint">{t('webhooks.fieldSuggestions', { count: fb.sampleCount, channel: d.channel })}</span>}
              </div>
            </div>
          </section>

          <section className="card">
            <div className="card-head">
              <h2>{t('webhooks.delivery')}</h2>
              <span className="card-sub">{t('webhooks.deliverySub')}</span>
            </div>
            <fieldset className="card-body form-grid wh-delivery fs-plain" disabled={readOnly}>
              <Field label={t('webhooks.timeout')} htmlFor="wh-timeout" error={shown.timeout_ms} hint={`${lim.timeout_ms[0]}–${lim.timeout_ms[1]} ${t('webhooks.ms')}${ackTimeout ? t('webhooks.belowAck', { ms: fmtMs(ackTimeout) }) : ''}`}>
                <input id="wh-timeout" className={`input mono ${shown.timeout_ms ? 'invalid' : ''}`} value={d.timeout_ms} disabled={readOnly} onChange={(e) => set({ timeout_ms: e.target.value })} inputMode="numeric" />
              </Field>
              <Field label={t('webhooks.maxInFlight')} htmlFor="wh-inflight" error={shown.max_in_flight} hint={d.max_in_flight === '1' ? t('webhooks.strictOrder') : t('webhooks.inFlightHint', { range: `${lim.max_in_flight[0]}–${lim.max_in_flight[1]}` })}>
                <input id="wh-inflight" className={`input mono ${shown.max_in_flight ? 'invalid' : ''}`} value={d.max_in_flight} disabled={readOnly} onChange={(e) => set({ max_in_flight: e.target.value })} inputMode="numeric" />
              </Field>
              <Field label={t('webhooks.batchSize')} htmlFor="wh-batch" error={shown.batch_max} hint={raw ? t('webhooks.rawAlways1') : t('webhooks.batchHint', { range: `${lim.batch_max[0]}–${lim.batch_max[1]}` })}>
                <input id="wh-batch" className={`input mono ${shown.batch_max ? 'invalid' : ''}`} value={d.batch_max} disabled={readOnly || raw} onChange={(e) => set({ batch_max: e.target.value })} inputMode="numeric" />
              </Field>
              <Field label={t('webhooks.batchLinger')} htmlFor="wh-linger" error={shown.batch_linger_ms} hint={t('webhooks.lingerHint', { range: `${lim.batch_linger_ms[0]}–${lim.batch_linger_ms[1]}` })}>
                <input id="wh-linger" className={`input mono ${shown.batch_linger_ms ? 'invalid' : ''}`} value={d.batch_linger_ms} disabled={readOnly || raw || d.batch_max === '1'} onChange={(e) => set({ batch_linger_ms: e.target.value })} inputMode="numeric" />
              </Field>
              <Field label={t('webhooks.4xx')} className="span-2">
                <label className="check-row">
                  <input type="checkbox" className="checkbox" checked={d.retry_on_4xx} disabled={readOnly} onChange={(e) => set({ retry_on_4xx: e.target.checked })} />
                  {t('webhooks.retry4xx')}
                </label>
              </Field>
              <Field label={t('webhooks.breaker.field')} className="span-2" error={shown.pause_after} hint={d.pause_enabled ? t('webhooks.breaker.hintOn') : t('webhooks.breaker.hintOff')}>
                <div className="wh-pause">
                  <label className="check-row">
                    <input type="checkbox" className="checkbox" checked={d.pause_enabled} disabled={readOnly} onChange={(e) => set({ pause_enabled: e.target.checked })} />
                    {t('webhooks.breaker.check')}
                  </label>
                  <input id="wh-pause-after" className={`input mono wh-pause-n ${shown.pause_after ? 'invalid' : ''}`} value={d.pause_after} disabled={readOnly || !d.pause_enabled} onChange={(e) => set({ pause_after: e.target.value })} inputMode="numeric" aria-label={t('webhooks.breaker.after')} />
                  <span className="muted">{t('webhooks.breaker.afterSuffix')}</span>
                </div>
              </Field>
              <Field label={t('webhooks.startFrom')} className="span-2" error={shown.start_at} hint={isNew ? t('webhooks.startHintNew') : t('webhooks.startHintFixed')}>
                {isNew ? (
                  <div className="wh-start">
                    <Segmented
                      value={d.start}
                      onChange={(start) => set({ start })}
                      label={t('webhooks.startPosition')}
                      options={[
                        { value: 'latest', label: t('webhooks.startLatest') },
                        { value: 'earliest', label: t('webhooks.startEarliest') },
                        { value: 'timestamp', label: t('webhooks.startTimestamp') },
                      ]}
                    />
                    {d.start === 'timestamp' && <input type="datetime-local" className={`input ${shown.start_at ? 'invalid' : ''}`} value={d.start_at} onChange={(e) => set({ start_at: e.target.value })} aria-label={t('webhooks.startTime')} />}
                  </div>
                ) : (
                  <span className="muted">{initial.start === 'latest' ? t('webhooks.startLatest') : initial.start === 'earliest' ? t('webhooks.startEarliestFull') : fmtDateTime(initial.start.timestamp_ms)}</span>
                )}
              </Field>
            </fieldset>
          </section>
          {showErrors && problems > 0 && <InlineError>{t(!filter ? (isNew ? 'webhooks.fixCreateFilter' : 'webhooks.fixSaveFilter') : isNew ? 'webhooks.fixCreate' : 'webhooks.fixSave')}</InlineError>}
          {saveError && <InlineError>{saveError}</InlineError>}
        </div>

        <aside className="re-side wh-side">
          <TestCard result={test} testing={testing} onTest={runTest} editing={!isNew && dirty} />
          {!isNew && <StatsCard w={initial} />}
          {!isNew && (
            <section className="card wh-links">
              <Link to={`${channelPath(d.channel, 'consumers')}#${encodeURIComponent(webhookGroup(initial.name))}`} className="wh-link">
                <Users size={15} />
                <span>
                  {t.rich('webhooks.consumerGroup', { group: <span className="mono">{webhookGroup(initial.name)}</span> })}
                  <small className="faint">{t('webhooks.consumerGroupSub')}</small>
                </span>
                <ChevronRight size={14} className="faint" />
              </Link>
              <GateButton need="operate" className="wh-link" onClick={() => setRewindOpen(true)}>
                <RotateCcw size={15} />
                <span>
                  {t('webhooks.rewind')}
                  <small className="faint">{t('webhooks.rewindSub')}</small>
                </span>
                <ChevronRight size={14} className="faint" />
              </GateButton>
              <Link to={`/dlq?channel=${encodeURIComponent(d.channel)}`} className="wh-link">
                <Inbox size={15} />
                <span>
                  {t('webhooks.dlqIn', { channel: d.channel })}
                  <small className="faint">{t('webhooks.dlqSub')}</small>
                </span>
                <ChevronRight size={14} className="faint" />
              </Link>
            </section>
          )}
          <section className="card wh-docs">
            <div className="card-head">
              <h2>{t('webhooks.receives')}</h2>
            </div>
            <div className="card-body form-stack">
              <p className="muted wh-doc-p">
                {raw ? (
                  t.rich('webhooks.rawDoc', { id: <span className="mono">{H_DELIVERY_ID}</span>, attempt: <span className="mono">{H_ATTEMPT}</span> })
                ) : (
                  t.rich('webhooks.envelopeDoc', {
                    lead: d.batch_max !== '1' ? t('webhooks.batchDocLead') : t('webhooks.singleDocLead'),
                    field: <span className="mono">{F_DELIVERY_ID}</span>,
                    header: <span className="mono">{H_DELIVERY_ID}</span>,
                  })
                )}
              </p>
              {!raw && <CodeBlock title={d.batch_max !== '1' ? t('webhooks.batchExample') : t('webhooks.envelopeExample')} code={exampleEnvelope(d.name, d.channel, d.batch_max !== '1')} />}
              <CodeBlock
                title={t('webhooks.verify')}
                actions={<Segmented value={lang} onChange={setLang} label={t('webhooks.codeLanguage')} options={CODE_LANGS} />}
                code={lang === 'node' ? VERIFY_NODE : VERIFY_PYTHON}
              />
              {d.url && <p className="faint wh-doc-p">{t('webhooks.requestsGo', { host: urlHost(d.url), channel: d.channel || t('webhooks.theChannel') })}</p>}
            </div>
          </section>
        </aside>
      </div>

      {!isNew && rewindOpen && <ResetOffsetsDialog channel={initial.channel} group={webhookGroup(initial.name)} open onClose={() => setRewindOpen(false)} />}
      <ConfirmDialog
        open={deleteOpen}
        title={t('webhooks.deleteTitle', { name: initial?.name ?? '' })}
        description={t('webhooks.deleteBody', { group: initial ? webhookGroup(initial.name) : '' })}
        confirmLabel={t('webhooks.deleteConfirm')}
        danger
        busy={deleting}
        onConfirm={doDelete}
        onClose={() => setDeleteOpen(false)}
      />
    </div>
  );
}

export default function WebhookEditorPage() {
  const t = useT();
  const { id } = useParams();
  const [params] = useSearchParams();
  useWebhooksPolling(5000);
  const w = useAsync((signal) => (id ? api.getWebhook(id, signal) : Promise.resolve(null)), [id]);
  if (id && w.error) {
    return (
      <div className="page">
        <EmptyState
          icon={<Webhook size={20} />}
          title={w.error instanceof ApiError && w.error.isNotFound ? t('webhooks.notFound') : t('webhooks.loadFailed')}
          actions={
            <Link to="/webhooks" className="btn">
              {t('webhooks.back')}
            </Link>
          }
        >
          {errorMessage(w.error)}
        </EmptyState>
      </div>
    );
  }
  if (id && (w.loading || !w.data)) {
    return (
      <div className="page">
        <Skeleton w={240} h={28} />
        <Skeleton h={320} style={{ marginTop: 20 }} />
      </div>
    );
  }
  return <Editor key={w.data?.id ?? 'new'} initial={w.data ?? null} presetChannel={params.get('channel')} />;
}
