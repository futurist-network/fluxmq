import { useState } from 'react';
import { useNavigate } from 'react-router';
import { CheckCircle2, Info, KeyRound, LogOut, RefreshCw, ShieldAlert, ShieldX, WifiOff, X, XCircle } from 'lucide-react';
import { useAuth } from '../store/auth';
import { telemetry } from '../telemetry';
import { useTelemetry } from '../store/telemetry';
import { toast, useUi } from '../store/ui';
import { useNow } from '../lib/hooks';
import { getToken, setToken } from '../lib/settings';
import { api, ApiError } from '../api/client';
import { Modal } from '../components/ui';
import { t as tr, useT } from '../i18n';

/** "Connection lost — reconnecting in 4s". Data on screen stays (stale) underneath. */
export function ConnectionBanner() {
  const t = useT();
  const conn = useTelemetry((s) => s.conn);
  const authRequired = useUi((s) => s.authRequired);
  const now = useNow();
  // The server answered 401: it is reachable, the token prompt explains what is wrong.
  if (authRequired && !conn.everOpened) return null;
  if (conn.status === 'open' || conn.status === 'idle' || conn.status === 'stopped') {
    if (conn.contractMismatch != null) {
      return (
        <div className="banner warning" role="status">
          <Info size={15} />
          <span>{t('shell.banner.contract', { server: conn.contractMismatch })}</span>
        </div>
      );
    }
    return null;
  }
  // First connection attempt in progress: skeletons are enough; no banner.
  if (conn.status === 'connecting' && !conn.everOpened) return null;
  const secs = conn.retryAt ? Math.max(0, Math.ceil((conn.retryAt - now) / 1000)) : 0;
  return (
    <div className="banner critical" role="alert">
      <WifiOff size={15} />
      <span>
        <b>{conn.everOpened ? t('shell.banner.lost') : t('shell.banner.unreachable')}</b>{' '}
        {conn.status === 'reconnecting' && conn.retryAt ? (secs > 0 ? t('shell.banner.reconnectingIn', { secs }) : t('shell.banner.reconnecting')) : t('shell.banner.connecting')}
        {conn.everOpened && <span className="faint"> {t('shell.banner.stale')}</span>}
      </span>
      <button className="btn btn-sm" onClick={() => telemetry.retryNow()}>
        <RefreshCw size={13} /> {t('shell.banner.retryNow')}
      </button>
    </div>
  );
}

/** Open mode: no tokens exist, so anyone who can reach the port is an admin. */
export function OpenModeBanner() {
  const t = useT();
  const open = useAuth((s) => s.me?.mode === 'open');
  const [dismissed, setDismissed] = useState(() => {
    try {
      return sessionStorage.getItem('fluxmq.openBannerDismissed') === '1';
    } catch {
      return false;
    }
  });
  const navigate = useNavigate();
  if (!open || dismissed) return null;
  const dismiss = () => {
    setDismissed(true);
    try {
      sessionStorage.setItem('fluxmq.openBannerDismissed', '1');
    } catch {
      /* ignore */
    }
  };
  return (
    <div className="banner warning open-mode-banner" role="status">
      <ShieldAlert size={15} />
      <span>
        <b>{t('shell.banner.openMode')}</b> {t('shell.banner.openModeHint')}
      </span>
      <button className="btn btn-sm" onClick={() => navigate('/settings/tokens?create=admin')}>
        <KeyRound size={13} /> {t('shell.banner.createAdminToken')}
      </button>
      <button className="btn btn-ghost btn-icon btn-sm" onClick={dismiss} aria-label={t('shell.banner.dismissSession')}>
        <X size={14} />
      </button>
    </div>
  );
}

/**
 * WS close 4401: the console's own token was revoked or expired. The telemetry client stopped
 * reconnecting; ask for a new token (verified before it is stored). Dismissing keeps a banner, so
 * the frozen data stays readable but never looks live.
 */
export function SessionRevoked() {
  const revoked = useTelemetry((s) => s.conn.revoked);
  if (!revoked) return null;
  // Remount per revocation: a later 4401 opens the dialog again even after a dismiss.
  return <SessionRevokedInner key={`${revoked.code}:${revoked.reason}`} reason={revoked.reason} />;
}

function SessionRevokedInner({ reason }: { reason: string }) {
  const t = useT();
  const [hidden, setHidden] = useState(false);
  const [value, setValue] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const hadToken = getToken() != null;

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    const tok = value.trim();
    if (!tok) return;
    setBusy(true);
    setError(null);
    try {
      await api.checkToken(tok);
      useUi.getState().setAuthRequired(false);
      setToken(tok); // restarts telemetry (clears the revoked state) and reloads whoami
      toast('success', tr('shell.revoked.signedIn'));
    } catch (err) {
      setError(err instanceof ApiError && err.isUnauthorized ? tr('shell.revoked.rejected') : tr('shell.revoked.unverifiable'));
    } finally {
      setBusy(false);
    }
  };

  const signOut = () => {
    // Without a token an enforced broker answers 401 and the regular token prompt takes over.
    setToken(null);
  };

  if (hidden) {
    return (
      <div className="banner critical session-revoked-banner" role="alert">
        <ShieldX size={15} />
        <span>
          <b>{t('shell.revoked.bannerTitle')}</b> {t('shell.revoked.bannerBody')}
        </span>
        <button className="btn btn-sm" onClick={() => setHidden(false)}>
          <KeyRound size={13} /> {t('shell.revoked.signInAgain')}
        </button>
      </div>
    );
  }

  return (
    <Modal
      open
      onClose={() => setHidden(true)}
      title={
        <span className="revoked-title">
          <span className="revoked-icon" aria-hidden>
            <ShieldX size={18} />
          </span>
          {t('shell.revoked.title')}
        </span>
      }
      description={t('shell.revoked.description')}
    >
      <form onSubmit={submit} className="field revoked-form">
        <div className="revoked-facts">
          <span>
            {t.rich('shell.revoked.closeCode', { code: <span className="mono">4401</span> })}
            {reason ? (
              <>
                {' '}
                {t.rich('shell.revoked.serverSaid', { reason: <span className="mono">“{reason}”</span> })}
              </>
            ) : null}
          </span>
          <span>{hadToken ? t('shell.revoked.kept') : t('shell.revoked.none')}</span>
        </div>
        <label htmlFor="revoked-token" className="field-label">
          {t('shell.revoked.newToken')}
        </label>
        <div className="input-group">
          <KeyRound size={15} />
          <input
            id="revoked-token"
            className={`input ${error ? 'invalid' : ''}`}
            type="password"
            autoComplete="off"
            placeholder={t('shell.auth.tokenPlaceholder')}
            value={value}
            onChange={(e) => setValue(e.target.value)}
            data-autofocus
          />
        </div>
        {error ? <span className="field-error">{error}</span> : <span className="field-hint">{t('shell.revoked.hint')}</span>}
        <div className="revoked-actions">
          <button type="button" className="btn btn-ghost" onClick={signOut}>
            <LogOut size={14} /> {t('shell.revoked.signOut')}
          </button>
          <span className="spacer" />
          <button type="button" className="btn" onClick={() => setHidden(true)}>
            {t('shell.revoked.viewFrozen')}
          </button>
          <button className="btn btn-primary" type="submit" disabled={busy || !value.trim()}>
            {busy ? t('shell.auth.checking') : t('shell.auth.signIn')}
          </button>
        </div>
      </form>
    </Modal>
  );
}

/** 401 → ask for an access token. */
export function AuthPrompt() {
  const t = useT();
  const revoked = useTelemetry((s) => s.conn.revoked != null);
  // The revoked-session dialog has its own token prompt; don't stack two.
  const open = useUi((s) => s.authRequired) && !revoked;
  const setOpen = useUi((s) => s.setAuthRequired);
  const [value, setValue] = useState(getToken() ?? '');
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    const prev = getToken();
    setToken(value);
    try {
      await api.checkAuth();
      setOpen(false);
      telemetry.restart();
      void useAuth.getState().load();
    } catch (err) {
      setToken(prev);
      setError(err instanceof ApiError && err.isUnauthorized ? tr('shell.auth.rejected') : tr('shell.auth.unverifiable'));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal
      open={open}
      onClose={() => setOpen(false)}
      title={t('shell.auth.title')}
      description={t('shell.auth.description')}
    >
      <form onSubmit={submit} className="field" style={{ gap: 12 }}>
        <div className="input-group">
          <KeyRound size={15} />
          <input
            className={`input ${error ? 'invalid' : ''}`}
            type="password"
            autoComplete="off"
            placeholder={t('shell.auth.tokenPlaceholder')}
            value={value}
            onChange={(e) => setValue(e.target.value)}
            aria-label={t('shell.auth.token')}
            data-autofocus
          />
        </div>
        {error && <span className="field-error">{error}</span>}
        <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
          <button className="btn btn-primary" type="submit" disabled={busy || !value.trim()}>
            {busy ? t('shell.auth.checking') : t('shell.auth.unlock')}
          </button>
        </div>
      </form>
    </Modal>
  );
}

export function Toasts() {
  const toasts = useUi((s) => s.toasts);
  const dismiss = useUi((s) => s.dismissToast);
  return (
    <div className="toasts" aria-live="polite">
      {toasts.map((m) => (
        <div key={m.id} className={`toast ${m.kind}`} role="status" onClick={() => dismiss(m.id)}>
          {m.kind === 'error' ? <XCircle size={16} /> : m.kind === 'success' ? <CheckCircle2 size={16} /> : <Info size={16} />}
          <span>{m.text}</span>
        </div>
      ))}
    </div>
  );
}
