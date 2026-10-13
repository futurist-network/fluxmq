import { useEffect, useState } from 'react';
import { NavLink } from 'react-router';
import { Check, KeyRound, Languages, Moon, Sun } from 'lucide-react';
import { api, ApiError, errorMessage } from '../api/client';
import { CONTRACT_VERSION } from '../api/types';
import { getToken, setToken } from '../lib/settings';
import { useAsync } from '../lib/hooks';
import { fmtDuration, fmtMs } from '../lib/format';
import { useTelemetry } from '../store/telemetry';
import { toast, useUi } from '../store/ui';
import { Segmented } from '../components/ui';
import { LANG_NAME, LANGS, t as tr, useLang, useT, type Lang, type TFn, type TKey } from '../i18n';
import { preloadSettingsPages } from './lazyPages';
import './settings.css';

/** The word "Language" in each language (never translated: shown in the other one). */
const LANG_WORD: Record<Lang, string> = { en: 'Language', ru: 'Язык' };

/** Telemetry topics (identifiers, never translated) and their push cadence. */
const CADENCES: [string, (t: TFn) => string][] = [
  ['cluster.overview', () => fmtMs(250)],
  ['channels.summary', () => fmtMs(500)],
  ['channel.{name} / .consumers', () => fmtMs(500)],
  ['channel.{name}.tail', (t) => t('settings.live.sampled', { cadence: fmtMs(250) })],
  ['nodes', () => fmtMs(1000)],
  ['alerts, events', (t) => t('settings.live.onChange')],
];

/** Tabs shared by the settings screens: switching keeps the page in place (no scroll, no page fade). */
export function SettingsTabs() {
  const t = useT();
  useEffect(preloadSettingsPages, []);
  const tabs: [string, TKey][] = [
    ['/settings', 'settings.tabs.general'],
    ['/settings/tokens', 'settings.tabs.tokens'],
    ['/settings/notifications', 'settings.tabs.notifications'],
    ['/settings/backups', 'settings.tabs.backups'],
    ['/settings/security', 'settings.tabs.security'],
  ];
  return (
    <nav className="tabs settings-tabs" aria-label={t('settings.tabs.aria')}>
      {tabs.map(([to, label]) => (
        <NavLink key={to} to={to} end={to === '/settings'}>
          {t(label)}
        </NavLink>
      ))}
    </nav>
  );
}

export default function SettingsPage() {
  const t = useT();
  const lang = useLang((s) => s.lang);
  const setLang = useLang((s) => s.setLang);
  const theme = useUi((s) => s.theme);
  const themePref = useUi((s) => s.themePref);
  const setTheme = useUi((s) => s.setTheme);
  const hints = useUi((s) => s.hints);
  const setHints = useUi((s) => s.setHints);
  const conn = useTelemetry((s) => s.conn);
  const [token, setTokenDraft] = useState(getToken() ?? '');
  const [status, setStatus] = useState<string | null>(null);
  const health = useAsync(() => api.health(), []);

  const save = async (e: React.FormEvent) => {
    e.preventDefault();
    setToken(token);
    setStatus(null);
    try {
      await api.checkAuth();
      setStatus('ok');
      toast('success', token ? tr('settings.token.saved') : tr('settings.token.cleared'));
    } catch (err) {
      setStatus(err instanceof ApiError && err.isUnauthorized ? tr('settings.token.rejected') : errorMessage(err));
    }
  };

  return (
    <div className="page settings-page">
      <div className="page-head">
        <div className="titles">
          <h1>{t('settings.title')}</h1>
          <p>{t('settings.subtitle')}</p>
        </div>
      </div>
      <SettingsTabs />

      <section className="card">
        <div className="card-head">
          <h2>{t('settings.token.title')}</h2>
        </div>
        <form className="card-body settings-row" onSubmit={save}>
          <div className="field" style={{ flex: 1 }}>
            <label htmlFor="token">{t('settings.token.label')}</label>
            <div className="input-group">
              <KeyRound size={14} />
              <input id="token" className="input mono" type="password" autoComplete="off" value={token} onChange={(e) => setTokenDraft(e.target.value)} placeholder={t('settings.token.placeholder')} />
            </div>
            <span className={status && status !== 'ok' ? 'field-error' : 'field-hint'}>
              {status === 'ok' ? (
                <>
                  <Check size={12} style={{ verticalAlign: -1 }} /> {t('settings.token.verified')}
                </>
              ) : (
                (status ?? t('settings.token.hint'))
              )}
            </span>
          </div>
          <button className="btn btn-primary" type="submit" style={{ alignSelf: 'flex-start', marginTop: 22 }}>
            {t('common.save')}
          </button>
        </form>
      </section>

      <section className="card">
        <div className="card-head">
          <h2>{t('settings.appearance.title')}</h2>
        </div>
        <div className="card-body settings-row">
          <span className="muted" style={{ flex: 1 }}>
            {t('settings.appearance.hint')}
          </span>
          <Segmented
            value={themePref}
            onChange={setTheme}
            label={t('settings.appearance.theme')}
            options={[
              { value: 'dark', label: t('settings.appearance.dark') },
              { value: 'light', label: t('settings.appearance.light') },
              { value: 'system', label: t('settings.appearance.system') },
            ]}
          />
          {theme === 'dark' ? <Moon size={16} className="faint" /> : <Sun size={16} className="faint" />}
        </div>
        <div className="card-body settings-row">
          <span className="muted" style={{ flex: 1 }}>
            {t('settings.appearance.hintsHint')}
          </span>
          <Segmented<'on' | 'off'>
            value={hints ? 'on' : 'off'}
            onChange={(v) => setHints(v === 'on')}
            label={t('settings.appearance.hints')}
            options={[
              { value: 'on', label: t('settings.appearance.hintsOn') },
              { value: 'off', label: t('settings.appearance.hintsOff') },
            ]}
          />
        </div>
      </section>

      <section className="card" id="language">
        <div className="card-head">
          <h2>{t('settings.language.title')}</h2>
          {/* The other language's word for "Language": findable by someone who can't read the active one. */}
          <span className="card-sub" lang={lang === 'ru' ? 'en' : 'ru'}>
            {lang === 'ru' ? LANG_WORD.en : LANG_WORD.ru}
          </span>
        </div>
        <div className="card-body settings-row">
          <span className="muted" style={{ flex: 1 }}>
            {t('settings.language.hint')}
          </span>
          <Segmented<Lang>
            value={lang}
            onChange={(l) => {
              setLang(l);
              toast('info', tr('settings.language.changed'));
            }}
            label={t('settings.language.label')}
            options={LANGS.map((l) => ({ value: l, label: LANG_NAME[l] }))}
          />
          <Languages size={16} className="faint" />
        </div>
      </section>

      <section className="card">
        <div className="card-head">
          <h2>{t('settings.live.title')}</h2>
          <span className="card-sub">{t('settings.live.sub')}</span>
        </div>
        <div className="card-body">
          <dl className="kv">
            {CADENCES.map(([topic, c]) => (
              <div key={topic} style={{ display: 'contents' }}>
                <dt className="mono">{topic}</dt>
                <dd>{c(t)}</dd>
              </div>
            ))}
            <dt>{t('settings.live.connection')}</dt>
            <dd>
              {t(`shell.conn.${conn.status}` as TKey)}
              {conn.rttMs != null && t('settings.live.rtt', { ms: conn.rttMs })}
              {conn.gaps > 0 && t('settings.live.resyncs', { count: conn.gaps })}
            </dd>
          </dl>
        </div>
      </section>

      <section className="card">
        <div className="card-head">
          <h2>{t('settings.about.title')}</h2>
        </div>
        <div className="card-body">
          <dl className="kv">
            <dt>{t('settings.about.console')}</dt>
            <dd>{t('settings.about.consoleValue', { version: __APP_VERSION__, contract: CONTRACT_VERSION })}</dd>
            <dt>{t('settings.about.broker')}</dt>
            <dd>{health.data ? t('settings.about.brokerValue', { version: health.data.version, node: health.data.node_id, uptime: fmtDuration(health.data.uptime_secs) }) : (conn.brokerVersion ?? '—')}</dd>
            <dt>Prometheus</dt>
            <dd>
              <a className="btn-link mono" href="/metrics" target="_blank" rel="noreferrer">
                {location.origin}/metrics
              </a>{' '}
              <span className="faint">{t('settings.about.prometheusNote')}</span>
            </dd>
            <dt>{t('settings.about.serverStatus')}</dt>
            <dd>{health.data?.status ?? (health.error ? errorMessage(health.error) : '—')}</dd>
          </dl>
        </div>
      </section>
    </div>
  );
}
