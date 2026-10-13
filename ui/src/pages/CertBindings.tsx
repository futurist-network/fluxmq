import { useState } from 'react';
import { Link } from 'react-router';
import { BadgeCheck, FileBadge, Plus, ShieldOff, Trash2 } from 'lucide-react';
import { api, ApiError, errorMessage } from '../api/client';
import { ROLES, TOPICS, type CertBinding, type ChannelsSummary } from '../api/types';
import { useAsync, useNow } from '../lib/hooks';
import { fmtDateTime, fmtRelative } from '../lib/format';
import { ROLE_HINT, ROLE_LABEL } from '../lib/rbac';
import { certGuideRef, certActor, certDraftToRequest, CLIENT_CA_FLAG, CN_MAX_BYTES, cnWarning, initialCertDraft, sortBindings, validateCertDraft, type CertDraft, type CertDraftErrors } from '../lib/certBindings';
import { channelNames } from '../store/selectors';
import { useTopicSelect } from '../telemetry';
import { toast } from '../store/ui';
import { ConfirmDialog, EmptyState, InlineError, Modal, SkeletonRows } from '../components/ui';
import { GateButton, RoleBadge } from '../components/auth/Gate';
import { useGate } from '../components/auth/useGate';
import { ScopeField, Scopes } from '../components/auth/Scopes';
import { useT } from '../i18n';

const CA_PEM = '<ca.pem>';
const TLS_CERT_FLAG = '--tls-cert';
const TLS_KEY_FLAG = '--tls-key';

/** Mount only while open: every opening starts from an empty draft. */
function CreateBindingDialog({ onClose, existing, onCreated, mtls }: { onClose: () => void; existing: string[]; onCreated: (b: CertBinding) => void; mtls: boolean }) {
  const channels = useTopicSelect<ChannelsSummary, string[]>(TOPICS.CHANNELS, channelNames);
  const [draft, setDraft] = useState<CertDraft>(initialCertDraft);
  const [errors, setErrors] = useState<CertDraftErrors>({});
  const [busy, setBusy] = useState(false);
  const [apiError, setApiError] = useState<string | null>(null);
  const t = useT();
  const set = (patch: Partial<CertDraft>) => {
    const next = { ...draft, ...patch };
    setDraft(next);
    if (Object.keys(errors).length) setErrors(validateCertDraft(next, existing));
  };

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    const errs = validateCertDraft(draft, existing);
    setErrors(errs);
    if (Object.keys(errs).length) return;
    setBusy(true);
    setApiError(null);
    try {
      const b = await api.createCertBinding(certDraftToRequest(draft));
      onCreated(b);
      onClose();
    } catch (err) {
      if (!(err instanceof ApiError && err.isForbidden)) setApiError(errorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal
      open
      onClose={onClose}
      wide
      title={t('security.cb.createTitle')}
      description={t('security.cb.createDesc')}
      footer={
        <>
          <button className="btn" type="button" onClick={onClose}>
            {t('common.cancel')}
          </button>
          <button className="btn btn-primary" type="submit" form="cb-form" disabled={busy}>
            <FileBadge size={14} /> {busy ? t('common.creating') : t('security.cb.createSubmit')}
          </button>
        </>
      }
    >
      <form id="cb-form" className="cb-form" onSubmit={submit}>
        {!mtls && (
          <div className="secret-warn" role="note">
            <ShieldOff size={16} />
            <span>
              {t.rich('security.cb.mtlsOffWarn', { inert: <b>{t('security.cb.inertWord')}</b>, flag: <span className="mono">{CLIENT_CA_FLAG}</span> })}
            </span>
          </div>
        )}
        <div className="field">
          <label htmlFor="cb-cn">{t('security.cb.cnLabel')}</label>
          <input id="cb-cn" className={`input mono ${errors.cn ? 'invalid' : ''}`} value={draft.cn} onChange={(e) => set({ cn: e.target.value })} placeholder="orders-worker.prod-eu-1" maxLength={CN_MAX_BYTES} data-autofocus autoComplete="off" spellCheck={false} />
          {errors.cn ? (
            <span className="field-error">{errors.cn}</span>
          ) : cnWarning(draft.cn) ? (
            <span className="field-hint text-warning">{cnWarning(draft.cn)}</span>
          ) : (
            <span className="field-hint">
              {t.rich('security.cb.cnHint', { actor: <span className="mono">{certActor(draft.cn.trim() || '<cn>')}</span> })}
            </span>
          )}
        </div>

        <div className="field">
          <span className="field-label">{t('common.role')}</span>
          <div className="role-options" role="radiogroup" aria-label={t('common.role')}>
            {ROLES.filter((r) => r !== 'subscriber').map((r) => (
              <label key={r} className={`role-option ${draft.role === r ? 'selected' : ''}`}>
                <input type="radio" name="cb-role" value={r} checked={draft.role === r} onChange={() => set({ role: r })} />
                <span className="ro-title">{ROLE_LABEL[r]}</span>
                <span className="ro-hint">{ROLE_HINT[r]}</span>
              </label>
            ))}
          </div>
          <span className="field-hint">{t('security.cb.roleHint')}</span>
        </div>

        <div className="token-scopes">
          <ScopeField label={t('tokens.create.publishTo')} hint={t('security.cb.publishHint')} value={draft.publish} onChange={(publish) => set({ publish })} error={errors.publish} channels={channels} />
          <ScopeField label={t('tokens.create.consumeFrom')} hint={t('security.cb.consumeHint')} value={draft.consume} onChange={(consume) => set({ consume })} error={errors.consume} channels={channels} />
        </div>
        {apiError && <InlineError>{apiError}</InlineError>}
      </form>
    </Modal>
  );
}

function InertWarning() {
  const t = useT();
  return (
    <div className="open-callout cb-inert" role="status">
      <ShieldOff size={16} />
      <div>
        <b>{t('security.cb.inertTitle')}</b>{' '}
        {t.rich('security.cb.inertBody', {
          flag: <code>{`${CLIENT_CA_FLAG} ${CA_PEM}`}</code>,
          cert: <code>{TLS_CERT_FLAG}</code>,
          key: <code>{TLS_KEY_FLAG}</code>,
          guide: <span className="mono">{certGuideRef()}</span>,
        })}
        <div>
          <Link to="/settings/security" className="btn btn-sm">
            {t('security.cb.openTls')}
          </Link>
        </div>
      </div>
    </div>
  );
}

export function CertBindingsSection() {
  const gate = useGate('admin');
  const [bump, setBump] = useState(0);
  // Admin-only endpoint: don't ask (and don't log 403s) for other roles.
  const res = useAsync((signal) => (gate.allowed ? api.listCertBindings(signal) : Promise.resolve(null)), [bump, gate.allowed]);
  const [createOpen, setCreateOpen] = useState(false);
  const [del, setDel] = useState<CertBinding | null>(null);
  const [delError, setDelError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const now = useNow();
  const t = useT();
  const forbidden = res.error instanceof ApiError && res.error.isForbidden;
  const notSupported = res.error instanceof ApiError && res.error.isNotFound;
  const data = res.data ?? null;
  const list = data ? sortBindings(data.bindings) : [];
  const inert = data != null && !data.client_mtls_enabled;

  const doDelete = async () => {
    if (!del) return;
    setBusy(true);
    setDelError(null);
    try {
      await api.deleteCertBinding(del.cn);
      toast('success', t('security.cb.deletedToast', { cn: del.cn }));
      setDel(null);
      setBump((b) => b + 1);
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) setDelError(errorMessage(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="card" aria-label={t('security.cb.title')}>
      <div className="card-head cb-head">
        <h2>{t('security.cb.title')}</h2>
        {data && (
          <>
            <span className="card-sub">{t('security.cb.count', { count: list.length })}</span>
            {data.client_mtls_enabled ? (
              <span className="badge badge-success cb-mtls" title={t('security.cb.mtlsOnTitle')}>
                <BadgeCheck size={11} /> {t('security.cb.mtlsOn')}
              </span>
            ) : (
              <span className="badge badge-warning cb-mtls" title={t('security.cb.inertBadgeTitle')}>
                <ShieldOff size={11} /> {t('security.cb.inertWord')}
              </span>
            )}
          </>
        )}
        <span className="spacer" />
        <GateButton need="admin" className="btn btn-primary btn-sm" onClick={() => setCreateOpen(true)} disabled={notSupported || !data}>
          <Plus size={14} /> {t('security.cb.newBinding')}
        </GateButton>
      </div>
      {!notSupported && gate.allowed && !forbidden && (
        <p className="faint cb-intro explain">
          {t('security.cb.intro')}
        </p>
      )}
      {inert && <InertWarning />}
      {notSupported ? (
        <EmptyState compact icon={<FileBadge size={18} />} title={t('tokens.unavailableTitle')}>
          {t('security.cb.unavailableBody')}
        </EmptyState>
      ) : forbidden || (!gate.allowed && !data) ? (
        <EmptyState compact icon={<FileBadge size={18} />} title={t('security.cb.adminOnlyTitle')}>
          {t('security.cb.adminOnlyBody')}
        </EmptyState>
      ) : res.error ? (
        <div style={{ padding: 16 }}>
          <InlineError onRetry={res.reload}>{errorMessage(res.error)}</InlineError>
        </div>
      ) : !data ? (
        <SkeletonRows rows={2} />
      ) : list.length === 0 ? (
        <EmptyState compact icon={<FileBadge size={18} />} title={t('security.cb.emptyTitle')}>
          {inert ? t('security.cb.emptyInert') : t('security.cb.emptyBody')}
        </EmptyState>
      ) : (
        <div className="table-wrap">
          <table className={`table tokens-table cb-table ${inert ? 'inert' : ''}`}>
            <thead>
              <tr>
                <th>{t('security.cb.subjectCn')}</th>
                <th>{t('common.role')}</th>
                <th>{t('common.scopes')}</th>
                <th>{t('common.created')}</th>
                <th>{t('common.lastUsed')}</th>
                <th aria-label={t('common.actions')} />
              </tr>
            </thead>
            <tbody>
              {list.map((b) => (
                <tr key={b.cn}>
                  <td>
                    <div className="mono cb-cn">{b.cn}</div>
                    <div className="faint mono cb-actor">
                      {certActor(b.cn)}
                    </div>
                  </td>
                  <td>
                    <RoleBadge role={b.role} />
                  </td>
                  <td>
                    <Scopes t={b} />
                  </td>
                  <td>
                    <span title={fmtDateTime(b.created_at_ms)}>{fmtRelative(b.created_at_ms, now)}</span>
                    <div className="faint" style={{ fontSize: 11.5 }}>
                      {t('tokens.by', { name: b.created_by })}
                    </div>
                  </td>
                  <td className={b.last_used_ms ? '' : 'faint'}>{b.last_used_ms ? <span title={fmtDateTime(b.last_used_ms)}>{fmtRelative(b.last_used_ms, now)}</span> : t('common.never')}</td>
                  <td className="r">
                    <GateButton
                      need="admin"
                      className="btn btn-sm btn-ghost"
                      onClick={() => {
                        setDel(b);
                        setDelError(null);
                      }}
                      aria-label={t('security.cb.deleteAria', { cn: b.cn })}
                    >
                      <Trash2 size={13} /> {t('common.delete')}
                    </GateButton>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {createOpen && data && (
        <CreateBindingDialog
          onClose={() => setCreateOpen(false)}
          existing={list.map((b) => b.cn)}
          mtls={data.client_mtls_enabled}
          onCreated={(b) => {
            toast('success', t('security.cb.createdToast', { cn: b.cn }));
            setBump((x) => x + 1);
          }}
        />
      )}
      <ConfirmDialog
        open={del != null}
        title={t('security.cb.deleteTitle', { cn: del?.cn ?? '' })}
        description={t('security.cb.deleteDesc')}
        confirmLabel={t('security.cb.deleteConfirm')}
        danger
        busy={busy}
        onConfirm={doDelete}
        onClose={() => setDel(null)}
      >
        {del && (
          <p className="faint" style={{ fontSize: 12.5, margin: 0 }}>
            {t.rich('security.cb.deleteFacts', {
              role: <b>{ROLE_LABEL[del.role]}</b>,
              created: fmtRelative(del.created_at_ms, now),
              by: del.created_by,
              used: del.last_used_ms ? fmtRelative(del.last_used_ms, now) : t('common.never'),
            })}
          </p>
        )}
        {delError && <InlineError>{delError}</InlineError>}
      </ConfirmDialog>
    </section>
  );
}
