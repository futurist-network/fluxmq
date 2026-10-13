import { useMemo, useState } from 'react';
import { useSearchParams } from 'react-router';
import { BellRing, History, Loader2, Pencil, Plus, Send, Trash2, Workflow } from 'lucide-react';
import { api, ApiError, errorMessage } from '../../api/client';
import { NOTIFIER_TYPES, type DeliveryStatus, type NotificationRecord, type NotificationRule, type Notifier, type NotifierType } from '../../api/types';
import { useAsync, useInterval, useNow } from '../../lib/hooks';
import { fmtDateTime, fmtRelative } from '../../lib/format';
import { ALERT_KIND_LABEL } from '../../lib/labels';
import { NOTIFIER_LABEL, notifierTarget } from '../../lib/notifierForm';
import { fmtInterval, ruleSummary, ruleToDraft } from '../../lib/ruleForm';
import { toast } from '../../store/ui';
import { useT, type TKey } from '../../i18n';
import { ConfirmDialog, EmptyState, InlineError, Segmented, SkeletonRows, Switch } from '../../components/ui';
import { GateButton } from '../../components/auth/Gate';
import { ReadOnlyNote } from '../../components/forms';
import { useGate } from '../../components/auth/useGate';
import { SettingsTabs } from '../Settings';
import { NotifierDialog, NotifierIcon } from './NotifierDialog';
import { toastTest } from './notifyHelpers';
import { RuleDialog } from './RuleDialog';
import { PullFeedCard } from './PullFeedCard';
import '../settings.css';
import './notifications.css';

const STATUS_CLASS: Record<DeliveryStatus, string> = { sent: 'badge-success', failed: 'badge-critical', suppressed: '' };

const STATUS_KEY: Record<DeliveryStatus, TKey> = { sent: 'notifications.status.sent', failed: 'notifications.status.failed', suppressed: 'notifications.status.suppressed' };

export function DeliveryBadge({ status }: { status: DeliveryStatus }) {
  const t = useT();
  return <span className={`badge status-badge ${STATUS_CLASS[status]}`}>{t(STATUS_KEY[status])}</span>;
}

type Dialog = { kind: 'notifier'; initial: Notifier | null; type?: NotifierType } | { kind: 'rule'; initial: NotificationRule | null } | null;

function NotifiersCard({ notifiers, history, onEdit, onAdd, reload }: { notifiers: Notifier[]; history: NotificationRecord[]; onEdit: (n: Notifier) => void; onAdd: (t?: NotifierType) => void; reload: () => void }) {
  const t = useT();
  const gate = useGate('admin');
  const now = useNow();
  const [testing, setTesting] = useState<string | null>(null);
  const [del, setDel] = useState<Notifier | null>(null);
  const [busy, setBusy] = useState(false);
  const last = useMemo(() => {
    const m = new Map<string, NotificationRecord>();
    for (const h of history) if (!m.has(h.notifier_id) && h.status !== 'suppressed') m.set(h.notifier_id, h);
    return m;
  }, [history]);

  const test = async (n: Notifier) => {
    setTesting(n.id);
    try {
      toastTest(await api.testNotifier(n.id), n.name);
      reload();
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) toast('error', errorMessage(e));
    } finally {
      setTesting(null);
    }
  };
  const toggle = async (n: Notifier, enabled: boolean) => {
    try {
      await api.updateNotifier(n.id, { enabled });
      toast('success', t(enabled ? 'notifications.notifiers.toastEnabled' : 'notifications.notifiers.toastPaused', { name: n.name }));
      reload();
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) toast('error', errorMessage(e));
    }
  };
  const doDelete = async () => {
    if (!del) return;
    setBusy(true);
    try {
      await api.deleteNotifier(del.id);
      toast('success', t('notifications.notifiers.toastDeleted', { name: del.name }));
      setDel(null);
      reload();
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) toast('error', errorMessage(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="card">
      <div className="card-head">
        <h2>{t('notifications.notifiers.title')}</h2>
        <span className="card-sub">{t('notifications.notifiers.sub')}</span>
        <span className="spacer" />
        <GateButton need="admin" className="btn btn-primary btn-sm" onClick={() => onAdd()}>
          <Plus size={14} /> {t('notifications.notifiers.add')}
        </GateButton>
      </div>
      {notifiers.length === 0 ? (
        <div className="ntf-empty">
          <EmptyState
            compact
            icon={<Send size={18} />}
            title={t('notifications.notifiers.emptyTitle')}
            actions={
              <GateButton need="admin" className="btn btn-primary" onClick={() => onAdd('telegram')}>
                <Send size={14} /> {t('notifications.notifiers.addTelegram')}
              </GateButton>
            }
          >
            {t.rich('notifications.notifiers.emptyBody', { sendTest: <b>{t('common.sendTest')}</b> })}
          </EmptyState>
          <div className="ntf-quick">
            {NOTIFIER_TYPES.filter((nt) => nt !== 'telegram').map((nt) => (
              <GateButton key={nt} need="admin" className="btn btn-sm btn-ghost" onClick={() => onAdd(nt)}>
                <NotifierIcon type={nt} size={13} /> {NOTIFIER_LABEL[nt]}
              </GateButton>
            ))}
          </div>
        </div>
      ) : (
        <div className="table-wrap">
          <table className="table ntf-table">
            <thead>
              <tr>
                <th>{t('notifications.notifiers.colNotifier')}</th>
                <th>{t('common.type')}</th>
                <th>{t('notifications.notifiers.colLastDelivery')}</th>
                <th>{t('common.enabled')}</th>
                <th aria-label={t('common.actions')} />
              </tr>
            </thead>
            <tbody>
              {notifiers.map((n) => {
                const l = last.get(n.id);
                return (
                  <tr key={n.id} className={n.enabled ? '' : 'row-muted'}>
                    <td>
                      <div className="ntf-name">
                        <NotifierIcon type={n.kind.type} />
                        <div style={{ minWidth: 0 }}>
                          <button type="button" className="btn-link ntf-title" onClick={() => onEdit(n)}>
                            {n.name}
                          </button>
                          <div className="faint mono ntf-target truncate">{notifierTarget(n.kind)}</div>
                        </div>
                      </div>
                    </td>
                    <td className="muted">{NOTIFIER_LABEL[n.kind.type]}</td>
                    <td>
                      {l ? (
                        <span className="ntf-last" title={l.error_detail ?? l.error ?? l.summary}>
                          <DeliveryBadge status={l.status} /> <span className="faint">{fmtRelative(l.ts, now)}</span>
                          {l.error && <div className="text-critical ntf-err truncate">{l.error}</div>}
                        </span>
                      ) : (
                        <span className="faint">{t('common.never')}</span>
                      )}
                    </td>
                    <td>
                      <Switch checked={n.enabled} onChange={(v) => void toggle(n, v)} label={t('notifications.notifiers.enabledLabel', { name: n.name })} disabled={!gate.allowed} title={gate.reason ?? undefined} />
                    </td>
                    <td className="r nowrap">
                      <GateButton need="admin" className="btn btn-sm" onClick={() => void test(n)} disabled={testing === n.id}>
                        {testing === n.id ? <Loader2 size={13} className="spin" /> : <Send size={13} />} {t('common.sendTest')}
                      </GateButton>
                      <button type="button" className="btn btn-sm btn-ghost btn-icon" onClick={() => onEdit(n)} aria-label={t('notifications.notifiers.editAria', { name: n.name })}>
                        <Pencil size={13} />
                      </button>
                      <GateButton need="admin" className="btn btn-sm btn-ghost btn-icon" onClick={() => setDel(n)} aria-label={t('notifications.notifiers.deleteAria', { name: n.name })}>
                        <Trash2 size={13} />
                      </GateButton>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      <ConfirmDialog
        open={del != null}
        title={t('notifications.notifiers.deleteTitle', { name: del?.name ?? '' })}
        description={t('notifications.notifiers.deleteBody')}
        confirmLabel={t('notifications.notifiers.deleteConfirm')}
        danger
        busy={busy}
        onConfirm={doDelete}
        onClose={() => setDel(null)}
      />
    </section>
  );
}

function RulesCard({ rules, notifiers, onEdit, onAdd, reload }: { rules: NotificationRule[]; notifiers: Notifier[]; onEdit: (r: NotificationRule) => void; onAdd: () => void; reload: () => void }) {
  const t = useT();
  const gate = useGate('admin');
  const [del, setDel] = useState<NotificationRule | null>(null);
  const [busy, setBusy] = useState(false);
  const byId = new Map(notifiers.map((n) => [n.id, n]));
  const toggle = async (r: NotificationRule, enabled: boolean) => {
    try {
      await api.updateRule(r.id, { enabled });
      toast('success', t(enabled ? 'notifications.rules.toastEnabled' : 'notifications.rules.toastPaused', { name: r.name }));
      reload();
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) toast('error', errorMessage(e));
    }
  };
  const doDelete = async () => {
    if (!del) return;
    setBusy(true);
    try {
      await api.deleteRule(del.id);
      toast('success', t('notifications.rules.toastDeleted', { name: del.name }));
      setDel(null);
      reload();
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) toast('error', errorMessage(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <section className="card">
      <div className="card-head">
        <h2>{t('notifications.rules.title')}</h2>
        <span className="card-sub">{t('notifications.rules.sub')}</span>
        <span className="spacer" />
        <GateButton need="admin" className="btn btn-sm" onClick={onAdd} disabled={!notifiers.length} title={!notifiers.length ? t('notifications.rules.addNotifierFirst') : undefined}>
          <Plus size={14} /> {t('notifications.rules.new')}
        </GateButton>
      </div>
      {rules.length === 0 ? (
        <EmptyState compact icon={<Workflow size={18} />} title={t(notifiers.length ? 'notifications.rules.emptyTitle' : 'notifications.rules.emptyTitleNoNotifiers')}>
          {t(notifiers.length ? 'notifications.rules.emptyBody' : 'notifications.rules.emptyBodyNoNotifiers')}
        </EmptyState>
      ) : (
        <ul className="rule-list">
          {rules.map((r) => (
            <li key={r.id} className={r.enabled ? '' : 'row-muted'}>
              <Switch checked={r.enabled} onChange={(v) => void toggle(r, v)} label={t('notifications.rules.enabledLabel', { name: r.name })} disabled={!gate.allowed} title={gate.reason ?? undefined} />
              <div className="rule-main">
                <button type="button" className="btn-link rule-name" onClick={() => onEdit(r)}>
                  {r.name}
                </button>
                <div className="rule-what muted">{ruleSummary(ruleToDraft(r), (k) => ALERT_KIND_LABEL[k])}</div>
                <div className="rule-meta faint">
                  {t('notifications.rules.meta', {
                    group: fmtInterval(r.group_wait_secs),
                    repeat: fmtInterval(r.repeat_interval_secs),
                    resolved: t(r.send_resolved ? 'notifications.rules.sendsResolved' : 'notifications.rules.noResolved'),
                  })}
                </div>
              </div>
              <div className="rule-to">
                {r.notifier_ids.map((id) => {
                  const n = byId.get(id);
                  return (
                    <span key={id} className={`chip static ${n && !n.enabled ? 'faint' : ''}`} title={n && !n.enabled ? t('notifications.rules.notifierPaused') : undefined}>
                      {n ? <NotifierIcon type={n.kind.type} size={11} /> : null} {n?.name ?? id}
                    </span>
                  );
                })}
              </div>
              <div className="rule-actions">
                <button type="button" className="btn btn-sm btn-ghost btn-icon" onClick={() => onEdit(r)} aria-label={t('notifications.notifiers.editAria', { name: r.name })}>
                  <Pencil size={13} />
                </button>
                <GateButton need="admin" className="btn btn-sm btn-ghost btn-icon" onClick={() => setDel(r)} aria-label={t('notifications.notifiers.deleteAria', { name: r.name })}>
                  <Trash2 size={13} />
                </GateButton>
              </div>
            </li>
          ))}
        </ul>
      )}
      <ConfirmDialog open={del != null} title={t('notifications.rules.deleteTitle', { name: del?.name ?? '' })} description={t('notifications.rules.deleteBody')} confirmLabel={t('notifications.rules.deleteConfirm')} danger busy={busy} onConfirm={doDelete} onClose={() => setDel(null)} />
    </section>
  );
}

function HistoryCard({ history, notifiers, rules }: { history: NotificationRecord[]; notifiers: Notifier[]; rules: NotificationRule[] }) {
  const t = useT();
  const [filter, setFilter] = useState<'all' | 'failed'>('all');
  const [limit, setLimit] = useState(25);
  const now = useNow();
  const nById = new Map(notifiers.map((n) => [n.id, n]));
  const rById = new Map(rules.map((r) => [r.id, r]));
  const failed = history.filter((h) => h.status === 'failed').length;
  const list = history.filter((h) => filter === 'all' || h.status === 'failed');
  return (
    <section className="card">
      <div className="card-head">
        <h2>{t('notifications.history.title')}</h2>
        <span className="card-sub">{t('notifications.history.sub')}</span>
        <span className="spacer" />
        <Segmented
          value={filter}
          onChange={setFilter}
          label={t('notifications.history.show')}
          options={[
            { value: 'all', label: t('common.all') },
            { value: 'failed', label: failed ? t('notifications.history.failedFilterCount', { count: failed }) : t('notifications.history.failedFilter') },
          ]}
        />
      </div>
      {list.length === 0 ? (
        <EmptyState compact icon={<History size={18} />} title={t(filter === 'failed' ? 'notifications.history.emptyFailedTitle' : 'notifications.history.emptyTitle')}>
          {t(filter === 'failed' ? 'notifications.history.emptyFailedBody' : 'notifications.history.emptyBody')}
        </EmptyState>
      ) : (
        <div className="table-wrap">
          <table className="table hist-table">
            <thead>
              <tr>
                <th>{t('common.when')}</th>
                <th>{t('common.status')}</th>
                <th>{t('notifications.notifiers.colNotifier')}</th>
                <th>{t('common.summary')}</th>
                <th className="r">{t('common.attempts')}</th>
              </tr>
            </thead>
            <tbody>
              {list.slice(0, limit).map((h) => {
                const n = nById.get(h.notifier_id);
                return (
                  <tr key={h.id}>
                    <td className="nowrap" title={fmtDateTime(h.ts)}>
                      {fmtRelative(h.ts, now)}
                    </td>
                    <td>
                      <DeliveryBadge status={h.status} />
                    </td>
                    <td className="nowrap">
                      {n ? <NotifierIcon type={n.kind.type} size={12} /> : null} {n?.name ?? <span className="faint mono">{h.notifier_id}</span>}
                      <div className="faint hist-rule">{h.rule_id ? (rById.get(h.rule_id)?.name ?? h.rule_id) : t('notifications.history.test')}</div>
                    </td>
                    <td className="hist-summary">
                      <div>{h.summary}</div>
                      {h.error && (
                        <div className={h.status === 'failed' ? 'text-critical hist-err' : 'faint hist-err'} title={h.error_detail ?? undefined}>
                          {h.error}
                        </div>
                      )}
                    </td>
                    <td className={`r num ${h.attempts > 1 ? 'text-warning' : ''}`}>{h.attempts}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {list.length > limit && (
            <div className="hist-more">
              <button type="button" className="btn btn-sm btn-ghost" onClick={() => setLimit(limit + 50)}>
                {t('notifications.history.showMore', { count: list.length - limit })}
              </button>
            </div>
          )}
        </div>
      )}
    </section>
  );
}

export default function NotificationsPage() {
  const t = useT();
  const [params, setParams] = useSearchParams();
  const notifiers = useAsync((s) => api.listNotifiers(s), []);
  const rules = useAsync((s) => api.listRules(s), []);
  const history = useAsync((s) => api.notifyHistory(s), []);
  useInterval(history.reload, 10_000);
  const addParam = params.get('add') as NotifierType | null;
  const [dialog, setDialog] = useState<Dialog>(() => (addParam && NOTIFIER_TYPES.includes(addParam) ? { kind: 'notifier', initial: null, type: addParam } : null));
  const close = () => {
    setDialog(null);
    if (params.has('add')) setParams({}, { replace: true });
  };
  const reloadAll = () => {
    notifiers.reload();
    rules.reload();
    history.reload();
  };
  const notFound = [notifiers.error, rules.error].some((e) => e instanceof ApiError && e.isNotFound);
  const nList = notifiers.data ?? [];

  return (
    <div className="page settings-page">
      <div className="page-head">
        <div className="titles">
          <h1>{t('common.settings')}</h1>
          <p>{t('notifications.subtitle')}</p>
        </div>
      </div>
      <SettingsTabs />
      <ReadOnlyNote what={t('notifications.readOnlyWhat')} />
      {notFound ? (
        <section className="card">
          <EmptyState icon={<BellRing size={20} />} title={t('notifications.unavailable.title')}>
            {t('notifications.unavailable.body')}
          </EmptyState>
        </section>
      ) : notifiers.error && !notifiers.data ? (
        <InlineError onRetry={reloadAll}>{errorMessage(notifiers.error)}</InlineError>
      ) : !notifiers.data || !rules.data ? (
        <section className="card">
          <SkeletonRows rows={4} />
        </section>
      ) : (
        <>
          <NotifiersCard notifiers={nList} history={history.data ?? []} onEdit={(n) => setDialog({ kind: 'notifier', initial: n })} onAdd={(type) => setDialog({ kind: 'notifier', initial: null, type })} reload={reloadAll} />
          <RulesCard rules={rules.data} notifiers={nList} onEdit={(r) => setDialog({ kind: 'rule', initial: r })} onAdd={() => setDialog({ kind: 'rule', initial: null })} reload={reloadAll} />
          <HistoryCard history={history.data ?? []} notifiers={nList} rules={rules.data} />
        </>
      )}
      <PullFeedCard />
      {dialog?.kind === 'notifier' && (
        <NotifierDialog
          key={dialog.initial?.id ?? `new-${dialog.type ?? ''}`}
          open
          onClose={close}
          initial={dialog.initial}
          initialType={dialog.type}
          existingNames={nList.map((n) => n.name)}
          onSaved={() => reloadAll()}
        />
      )}
      {dialog?.kind === 'rule' && <RuleDialog key={dialog.initial?.id ?? 'new'} open onClose={close} initial={dialog.initial} notifiers={nList} onSaved={() => reloadAll()} />}
    </div>
  );
}
