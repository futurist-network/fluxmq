import { useState } from 'react';
import { useSearchParams } from 'react-router';
import { AlertTriangle, Check, Copy, KeyRound, Lock, Plus, ShieldAlert, ShieldCheck, ShieldOff, Trash2, Unplug } from 'lucide-react';
import { api, ApiError, errorMessage } from '../api/client';
import { TOPICS, type AuthMode, type ChannelsSummary, type CreateTokenResponse, type Role, type TokenInfo } from '../api/types';
import { useAsync, copyText, useNow } from '../lib/hooks';
import { fmtDateTime, fmtRelative } from '../lib/format';
import { ROLE_HINT, ROLE_LABEL } from '../lib/rbac';
import { defaultTtl, DIALOG_ROLES, firstTokenNote, initialDraft, revokeDisconnectNote, revokeErrorView, submitDraft, TTL_PRESETS, validateDraft, type DraftErrors, type RevokeErrorView, type TokenDraft } from '../lib/tokenFlow';
import { setToken, getToken } from '../lib/settings';
import { channelNames } from '../store/selectors';
import { useTopicSelect } from '../telemetry';
import { useAuth } from '../store/auth';
import { toast } from '../store/ui';
import { ConfirmDialog, EmptyState, InlineError, Modal, Segmented, SkeletonRows } from '../components/ui';
import { ScopeField, Scopes } from '../components/auth/Scopes';
import { GateButton, RoleBadge } from '../components/auth/Gate';
import { useGate } from '../components/auth/useGate';
import { SettingsTabs } from './Settings';
import { CertBindingsSection } from './CertBindings';
import { useT } from '../i18n';
import './settings.css';

/** Wire formats (technical, never translated). */
const BEARER_HEADER = 'Authorization: Bearer …';
const TOKEN_QUERY = '?token=';

/** Create → show the secret once. */
function CreateTokenDialog({ open, onClose, mode, existing, onCreated, initialRole, initialSubscribe }: { open: boolean; onClose: () => void; mode: AuthMode | null; existing: string[]; onCreated: () => void; initialRole?: TokenDraft['role']; initialSubscribe?: string[] }) {
  const channels = useTopicSelect<ChannelsSummary, string[]>(TOPICS.CHANNELS, channelNames);
  const [draft, setDraft] = useState<TokenDraft>(() => ({ ...initialDraft(mode, initialRole), subscribe: initialSubscribe ?? [] }));
  const [errors, setErrors] = useState<DraftErrors>({});
  const [busy, setBusy] = useState(false);
  const [apiError, setApiError] = useState<string | null>(null);
  const [created, setCreated] = useState<{ resp: CreateTokenResponse; adopted: boolean } | null>(null);
  const [copied, setCopied] = useState(false);
  const now = useNow();
  const t = useT();
  const set = (patch: Partial<TokenDraft>) => {
    const next = { ...draft, ...patch };
    setDraft(next);
    if (Object.keys(errors).length) setErrors(validateDraft(next, mode, existing));
  };

  const close = () => {
    onClose();
    setCreated(null);
    setCopied(false);
    setDraft({ ...initialDraft(mode, initialRole), subscribe: initialSubscribe ?? [] });
    setErrors({});
    setApiError(null);
  };

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    const errs = validateDraft(draft, mode, existing);
    setErrors(errs);
    if (Object.keys(errs).length) return;
    setBusy(true);
    setApiError(null);
    try {
      const r = await submitDraft(draft, { mode, existingNames: existing, create: api.createToken, adopt: setToken });
      setCreated(r);
      onCreated();
    } catch (err) {
      setApiError(errorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  const copy = async () => {
    if (!created) return;
    if (await copyText(created.resp.token)) {
      setCopied(true);
      toast('success', t('tokens.copiedToast'));
    }
  };

  if (created) {
    const { resp, adopted } = created;
    return (
      <Modal
        open={open}
        onClose={close}
        title={t('tokens.created.title', { name: resp.info.name })}
        description={
          <>
            {t.rich('tokens.created.role', { role: <b>{ROLE_LABEL[resp.info.role]}</b> })}
            {resp.info.expires_at_ms ? t('tokens.created.expires', { date: fmtDateTime(resp.info.expires_at_ms) }) : t('tokens.created.noExpiry')}
          </>
        }
        footer={
          <button className="btn btn-primary" onClick={close} data-autofocus={copied ? true : undefined}>
            {copied ? t('tokens.created.done') : t('tokens.created.stored')}
          </button>
        }
      >
        <div className="secret-warn" role="alert">
          <AlertTriangle size={16} />
          <span>
            <b>{t('tokens.created.warnTitle')}</b> {t('tokens.created.warnBody')}
          </span>
        </div>
        <div className="secret-box">
          <code className="mono" data-testid="token-secret">
            {resp.token}
          </code>
          <button className="btn btn-sm" onClick={copy} data-autofocus>
            {copied ? <Check size={13} /> : <Copy size={13} />} {copied ? t('common.copied') : t('common.copy')}
          </button>
        </div>
        {adopted && (
          <div className="secret-adopted">
            <ShieldCheck size={16} />
            <span>{t('tokens.created.adopted')}</span>
          </div>
        )}
        <p className="faint" style={{ fontSize: 12, marginTop: 12 }}>
          {t.rich('tokens.created.usage', { bearer: <span className="mono">{BEARER_HEADER}</span>, query: <span className="mono">{TOKEN_QUERY}</span> })}
        </p>
      </Modal>
    );
  }

  return (
    <Modal
      open={open}
      onClose={close}
      wide
      title={mode === 'open' ? t('tokens.create.titleFirst') : t('tokens.create.title')}
      description={
        mode === 'open' ? t('tokens.create.descFirst') : t('tokens.create.desc')
      }
      footer={
        <>
          <button className="btn" type="button" onClick={close}>
            {t('common.cancel')}
          </button>
          <button className="btn btn-primary" type="submit" form="token-form" disabled={busy}>
            <KeyRound size={14} /> {busy ? t('common.creating') : t('tokens.create.submit')}
          </button>
        </>
      }
    >
      <form id="token-form" className="token-form" onSubmit={submit}>
        <div className="field">
          <label htmlFor="t-name">{t('common.name')}</label>
          <input id="t-name" className={`input ${errors.name ? 'invalid' : ''}`} value={draft.name} onChange={(e) => set({ name: e.target.value })} placeholder={t('tokens.create.namePlaceholder')} data-autofocus autoComplete="off" />
          {errors.name ? <span className="field-error">{errors.name}</span> : <span className="field-hint">{t('tokens.create.nameHint')}</span>}
        </div>

        <div className="field">
          <span className="field-label">{t('common.role')}</span>
          <div className="role-options" role="radiogroup" aria-label={t('common.role')}>
            {DIALOG_ROLES.map((r) => {
              const locked = mode === 'open' && r !== 'admin';
              return (
                <label key={r} className={`role-option ${draft.role === r ? 'selected' : ''} ${locked ? 'locked' : ''}`} title={locked ? firstTokenNote() : undefined}>
                  <input type="radio" name="role" value={r} checked={draft.role === r} disabled={locked} onChange={() => set({ role: r, ttl: draft.ttl === defaultTtl(draft.role) ? defaultTtl(r) : draft.ttl })} />
                  <span className="ro-title">{ROLE_LABEL[r]}</span>
                  <span className="ro-hint">{ROLE_HINT[r]}</span>
                </label>
              );
            })}
          </div>
          {mode === 'open' ? (
            <span className="field-hint">
              <Lock size={12} aria-hidden /> {firstTokenNote()}
            </span>
          ) : (
            errors.role && <span className="field-error">{errors.role}</span>
          )}
        </div>

        {draft.role === 'subscriber' ? (
          <div className="token-scopes single">
            <ScopeField label={t('tokens.create.subscribeTo')} hint={t('tokens.create.subscribeHint')} value={draft.subscribe} onChange={(subscribe) => set({ subscribe })} error={errors.subscribe} channels={channels} />
          </div>
        ) : (
          <div className="token-scopes">
            <ScopeField label={t('tokens.create.publishTo')} hint={t('tokens.create.publishHint')} value={draft.publish} onChange={(publish) => set({ publish })} error={errors.publish} channels={channels} />
            <ScopeField label={t('tokens.create.consumeFrom')} hint={t('tokens.create.consumeHint')} value={draft.consume} onChange={(consume) => set({ consume })} error={errors.consume} channels={channels} />
          </div>
        )}

        <div className="field">
          <span className="field-label">{t('common.expires')}</span>
          <Segmented value={draft.ttl} onChange={(ttl) => set({ ttl })} label={t('tokens.create.lifetime')} options={TTL_PRESETS.map((p) => ({ value: p.id, label: t(p.labelKey) }))} />
          <span className="field-hint">{draft.ttl === 'never' ? t('tokens.create.neverExpires') : t('tokens.create.expiresAt', { date: fmtDateTime(now + (TTL_PRESETS.find((p) => p.id === draft.ttl)?.secs ?? 0) * 1000) })}</span>
        </div>
        {apiError && <InlineError>{apiError}</InlineError>}
      </form>
    </Modal>
  );
}

export default function AccessTokensPage() {
  const me = useAuth((s) => s.me);
  const status = useAuth((s) => s.status);
  const reloadMe = useAuth((s) => s.load);
  const [params, setParams] = useSearchParams();
  const [bump, setBump] = useState(0);
  const gate = useGate('admin');
  // Non-admins can't list tokens: don't ask (and don't log 403s).
  const tokens = useAsync((signal) => (gate.allowed ? api.listTokens(signal) : Promise.resolve(null)), [bump, me?.mode, gate.allowed]);
  const [createOpen, setCreateOpen] = useState(() => params.get('create') != null);
  // `?create=admin` (last-admin guard) or `?create=subscriber&channel=<name>` (live subscriptions page).
  const [createRole, setCreateRole] = useState<Role | undefined>(() => { const c = params.get('create'); return c === 'admin' || c === 'subscriber' ? c : undefined; });
  const initialSubscribe = params.get('channel') ? [params.get('channel')!] : undefined;
  const [revoke, setRevoke] = useState<TokenInfo | null>(null);
  const [revokeError, setRevokeError] = useState<RevokeErrorView | null>(null);
  const [busy, setBusy] = useState(false);
  const now = useNow();
  const t = useT();
  const mode = me?.mode ?? null;
  const forbidden = tokens.error instanceof ApiError && tokens.error.isForbidden;
  const notSupported = status === 'unsupported' || (tokens.error instanceof ApiError && tokens.error.isNotFound);

  const openRevoke = (tk: TokenInfo | null) => {
    setRevoke(tk);
    setRevokeError(null);
  };

  const doRevoke = async () => {
    if (!revoke) return;
    setBusy(true);
    setRevokeError(null);
    try {
      await api.revokeToken(revoke.id);
      toast('success', t('tokens.revokedToast', { name: revoke.name }));
      const self = revoke.id === me?.token_id;
      openRevoke(null);
      if (self) {
        setToken(null);
      } else {
        setBump((b) => b + 1);
        void reloadMe();
      }
    } catch (e) {
      // Shown inside the dialog: the last-admin guard needs more than a toast.
      if (!(e instanceof ApiError && e.isForbidden)) setRevokeError(revokeErrorView(e instanceof ApiError ? e : { message: errorMessage(e) }, revoke.name));
    } finally {
      setBusy(false);
    }
  };

  const list = [...(tokens.data ?? [])].sort((a, b) => b.created_at_ms - a.created_at_ms);

  return (
    <div className="page settings-page">
      <div className="page-head">
        <div className="titles">
          <h1>{t('common.settings')}</h1>
          <p>{t('tokens.pageSub')}</p>
        </div>
      </div>
      <SettingsTabs />

      <section className="card">
        <div className="card-head">
          <h2>{t('tokens.signedInAs')}</h2>
        </div>
        <div className="card-body identity-card">
          {me ? (
            <>
              <span className={`id-icon ${me.mode === 'open' ? 'open' : ''}`} aria-hidden>
                {me.mode === 'open' ? <ShieldOff size={18} /> : <ShieldCheck size={18} />}
              </span>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div className="id-name">
                  <b>{me.mode === 'open' ? t('tokens.anonymous') : me.name}</b> <RoleBadge role={me.role} />
                </div>
                <div className="faint" style={{ fontSize: 12.5 }}>
                  {me.mode === 'open'
                    ? t('tokens.openModeIdentity')
                    : me.token_id
                      ? t('tokens.tokenIdentity', { id: me.token_id, hint: ROLE_HINT[me.role] })
                      : t('tokens.bootstrapIdentity', { hint: ROLE_HINT[me.role] })}
                </div>
              </div>
              {me.mode !== 'open' && getToken() && (
                <button className="btn btn-sm" onClick={() => setToken(null)}>
                  {t('tokens.signOut')}
                </button>
              )}
            </>
          ) : notSupported ? (
            <span className="muted">{t('tokens.noTokensVersion')}</span>
          ) : (
            <span className="muted">{status === 'loading' ? t('common.loadingEllipsis') : t('tokens.notSignedIn')}</span>
          )}
        </div>
      </section>

      <section className="card">
        <div className="card-head">
          <h2>{t('tokens.title')}</h2>
          <span className="card-sub">{tokens.data ? t('tokens.count', { count: list.length }) : ''}</span>
          <span className="spacer" />
          <GateButton need="admin" className="btn btn-primary btn-sm" onClick={() => {
              setCreateRole(undefined);
              setCreateOpen(true);
            }} disabled={notSupported}>
            <Plus size={14} /> {mode === 'open' ? t('tokens.createAdmin') : t('tokens.newToken')}
          </GateButton>
        </div>
        {mode === 'open' && (
          <div className="open-callout">
            <ShieldOff size={16} />
            <span>
              <b>{t('tokens.openCalloutTitle')}</b> {t('tokens.openCalloutBody')}
            </span>
          </div>
        )}
        {notSupported ? (
          <EmptyState compact icon={<KeyRound size={18} />} title={t('tokens.unavailableTitle')}>
            {t('tokens.unavailableBody')}
          </EmptyState>
        ) : forbidden || (!gate.allowed && !tokens.data) ? (
          <EmptyState compact icon={<KeyRound size={18} />} title={t('tokens.adminOnlyTitle')}>
            {t('tokens.adminOnlyBody', { role: me ? ROLE_LABEL[me.role].toLowerCase() : t('common.unknown') })}
          </EmptyState>
        ) : tokens.error ? (
          <div style={{ padding: 16 }}>
            <InlineError onRetry={tokens.reload}>{errorMessage(tokens.error)}</InlineError>
          </div>
        ) : !tokens.data ? (
          <SkeletonRows rows={3} />
        ) : list.length === 0 ? (
          <EmptyState compact icon={<KeyRound size={18} />} title={t('tokens.emptyTitle')}>
            {mode === 'open' ? t('tokens.emptyOpen') : t('tokens.emptyEnforced')}
          </EmptyState>
        ) : (
          <div className="table-wrap">
            <table className="table tokens-table">
              <thead>
                <tr>
                  <th>{t('common.name')}</th>
                  <th>{t('common.role')}</th>
                  <th>{t('common.scopes')}</th>
                  <th>{t('common.created')}</th>
                  <th>{t('common.lastUsed')}</th>
                  <th>{t('common.expires')}</th>
                  <th aria-label={t('common.actions')} />
                </tr>
              </thead>
              <tbody>
                {list.map((tk) => {
                  const expired = tk.expires_at_ms != null && tk.expires_at_ms <= now;
                  const soon = !expired && tk.expires_at_ms != null && tk.expires_at_ms - now < 7 * 86_400_000;
                  return (
                    <tr key={tk.id}>
                      <td>
                        <div className="tok-name">
                          <b>{tk.name}</b>
                          {tk.id === me?.token_id && <span className="badge badge-primary you">{t('tokens.you')}</span>}
                        </div>
                        <div className="faint mono" style={{ fontSize: 11.5 }}>
                          {tk.id}
                        </div>
                      </td>
                      <td>
                        <RoleBadge role={tk.role} />
                      </td>
                      <td>
                        <Scopes t={tk} />
                      </td>
                      <td>
                        <span title={fmtDateTime(tk.created_at_ms)}>{fmtRelative(tk.created_at_ms, now)}</span>
                        <div className="faint" style={{ fontSize: 11.5 }}>
                          {t('tokens.by', { name: tk.created_by })}
                        </div>
                      </td>
                      <td className={tk.last_used_ms ? '' : 'faint'}>{tk.last_used_ms ? <span title={fmtDateTime(tk.last_used_ms)}>{fmtRelative(tk.last_used_ms, now)}</span> : t('common.never')}</td>
                      <td className={expired ? 'text-critical' : soon ? 'text-warning' : tk.expires_at_ms ? '' : 'faint'}>
                        {tk.expires_at_ms ? <span title={fmtDateTime(tk.expires_at_ms)}>{expired ? t('tokens.expiredAgo', { when: fmtRelative(tk.expires_at_ms, now) }) : fmtRelative(tk.expires_at_ms, now)}</span> : t('common.never')}
                      </td>
                      <td className="r">
                        <GateButton need="admin" className="btn btn-sm btn-ghost" onClick={() => openRevoke(tk)} aria-label={t('tokens.revokeAria', { name: tk.name })}>
                          <Trash2 size={13} /> {t('tokens.revoke')}
                        </GateButton>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <CertBindingsSection />

      <CreateTokenDialog
        // Remount once whoami has loaded (initial draft depends on the mode) — but never when the
        // mode flips open → enforced right after creating the first token: that would drop the
        // one-time secret on screen.
        key={`${me ? 'ready' : 'loading'}:${createRole ?? ''}`}
        open={createOpen}
        onClose={() => {
          setCreateOpen(false);
          if (params.has('create') || params.has('channel')) setParams({}, { replace: true });
        }}
        mode={mode}
        existing={list.map((tk) => tk.name)}
        initialRole={createRole}
        initialSubscribe={initialSubscribe}
        onCreated={() => {
          setBump((b) => b + 1);
          void reloadMe();
        }}
      />
      <ConfirmDialog
        open={revoke != null}
        title={t('tokens.revokeTitle', { name: revoke?.name ?? '' })}
        description={revoke?.id === me?.token_id ? t('tokens.revokeSelf') : undefined}
        confirmLabel={t('tokens.revokeConfirm')}
        danger
        busy={busy}
        onConfirm={doRevoke}
        onClose={() => openRevoke(null)}
      >
        {revoke && (
          <div className="revoke-body">
            <div className="revoke-note">
              <Unplug size={16} aria-hidden />
              <span>{revokeDisconnectNote()}</span>
            </div>
            <dl className="revoke-facts">
              <div>
                <dt>{t('common.role')}</dt>
                <dd>
                  <RoleBadge role={revoke.role} small />
                </dd>
              </div>
              <div>
                <dt>{t('common.lastUsed')}</dt>
                <dd>{revoke.last_used_ms ? fmtRelative(revoke.last_used_ms, now) : t('common.never')}</dd>
              </div>
              <div>
                <dt>{t('tokens.tokenId')}</dt>
                <dd className="mono">{revoke.id}</dd>
              </div>
            </dl>
            {revokeError && (
              <div className={`revoke-error ${revokeError.lastAdmin ? 'last-admin' : ''}`} role="alert">
                {revokeError.lastAdmin ? <ShieldAlert size={16} aria-hidden /> : <AlertTriangle size={16} aria-hidden />}
                <div>
                  <b>{revokeError.title}</b>
                  <p>{revokeError.body}</p>
                  {revokeError.lastAdmin && (
                    <button
                      type="button"
                      className="btn btn-sm"
                      onClick={() => {
                        openRevoke(null);
                        setCreateRole('admin');
                        setCreateOpen(true);
                      }}
                    >
                      <Plus size={13} /> {t('tokens.createAnotherAdmin')}
                    </button>
                  )}
                </div>
              </div>
            )}
          </div>
        )}
      </ConfirmDialog>
    </div>
  );
}
