import { useState } from 'react';
import { CircleCheck, CircleX, Loader2, Send } from 'lucide-react';
import { NOTIFIER_ICON, toastTest } from './notifyHelpers';
import { api, ApiError, errorMessage } from '../../api/client';
import { NOTIFIER_TYPES, type Notifier, type NotifierType, type TestResult } from '../../api/types';
import { draftFromNotifier, draftToCreate, draftToUpdate, emptyNotifierDraft, NOTIFIER_HINT, NOTIFIER_LABEL, SMTP_TLS, validateNotifierDraft, validEmail, type NotifierDraft, type NotifierErrors } from '../../lib/notifierForm';
import { toast } from '../../store/ui';
import { InlineError, Modal, Segmented, Switch } from '../../components/ui';
import { ChipsInput } from '../../components/ChipsInput';
import { Field, HeadersEditor, SecretInput } from '../../components/forms';
import { GateButton } from '../../components/auth/Gate';
import { useGate } from '../../components/auth/useGate';
import { fmtMs } from '../../lib/format';
import { t as tr, useT } from '../../i18n';

export function NotifierIcon({ type, size = 16 }: { type: NotifierType; size?: number }) {
  const I = NOTIFIER_ICON[type];
  return (
    <span className={`ntf-icon ntf-${type}`} aria-hidden>
      <I size={size} />
    </span>
  );
}

export function TestResultLine({ r }: { r: TestResult | null }) {
  const t = useT();
  if (!r) return null;
  return r.ok ? (
    <span className="test-result ok" role="status">
      <CircleCheck size={14} /> {t('notifications.test.deliveredIn', { ms: r.latency_ms })}
    </span>
  ) : (
    <span className="test-result fail" role="status" title={r.error_detail ?? undefined}>
      <CircleX size={14} /> {r.error ?? t('notifications.test.failedShort')} <span className="faint">· {fmtMs(r.latency_ms)}</span>
    </span>
  );
}

function TelegramHelp() {
  const t = useT();
  return (
    <div className="helper-box">
      <b>{t('notifications.telegramHelp.title')}</b>
      <ol>
        <li>
          {t.rich('notifications.telegramHelp.step1', {
            botfather: (
              <a href="https://t.me/BotFather" target="_blank" rel="noreferrer">
                {'@BotFather'}
              </a>
            ),
            newbot: <code>{'/newbot'}</code>,
          })}
        </li>
        <li>{t('notifications.telegramHelp.step2')}</li>
        <li>
          {t.rich('notifications.telegramHelp.step3', {
            url: <code>{'https://api.telegram.org/bot<token>/getUpdates'}</code>,
            chatId: <code>{'message.chat.id'}</code>,
            prefix: <code>{'-100'}</code>,
            threadId: <code>{'message_thread_id'}</code>,
          })}
        </li>
      </ol>
    </div>
  );
}

export function NotifierDialog({ open, onClose, initial, initialType, existingNames, onSaved }: { open: boolean; onClose: () => void; initial: Notifier | null; initialType?: NotifierType; existingNames: string[]; onSaved: (n: Notifier) => void }) {
  const t = useT();
  const gate = useGate('admin');
  const readOnly = !gate.allowed;
  const [d, setD] = useState<NotifierDraft>(() => (initial ? draftFromNotifier(initial) : emptyNotifierDraft(initialType ?? 'telegram')));
  const [showErrors, setShowErrors] = useState(false);
  const [busy, setBusy] = useState(false);
  const [testing, setTesting] = useState(false);
  const [test, setTest] = useState<TestResult | null>(null);
  const [apiError, setApiError] = useState<string | null>(null);
  const isNew = initial == null;
  const errs: NotifierErrors = validateNotifierDraft(d);
  if (isNew && d.name.trim() && existingNames.includes(d.name.trim())) errs.name = tr('notifications.notifierForm.nameExists');
  const shown: NotifierErrors = showErrors ? errs : {};
  const set = (p: Partial<NotifierDraft>) => {
    setD({ ...d, ...p });
    setTest(null);
  };
  const dirty = !initial || JSON.stringify(draftToUpdate(d)) !== JSON.stringify(draftToUpdate(draftFromNotifier(initial)));

  const save = async () => {
    setShowErrors(true);
    if (Object.keys(errs).length) return;
    setBusy(true);
    setApiError(null);
    try {
      const n = isNew ? await api.createNotifier(draftToCreate(d)) : await api.updateNotifier(initial.id, draftToUpdate(d));
      toast('success', isNew ? t('notifications.notifierForm.toastAdded', { name: n.name }) : t('notifications.notifierForm.toastSaved'));
      onSaved(n);
      onClose();
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) setApiError(errorMessage(e));
    } finally {
      setBusy(false);
    }
  };

  // New: test the draft as typed. Existing: send the edits as an update-shaped draft; the server
  // merges it over the stored notifier (untouched secrets stay REDACTED = stored value) and
  // tests without saving.
  const runTest = async () => {
    setShowErrors(true);
    if (Object.keys(errs).length) return;
    setTesting(true);
    setTest(null);
    try {
      const r = isNew ? await api.testNotifierDraft(draftToCreate(d)) : await api.testNotifier(initial.id, dirty ? draftToUpdate(d) : undefined);
      setTest(r);
      toastTest(r, d.name.trim() || NOTIFIER_LABEL[d.type]);
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) toast('error', errorMessage(e));
    } finally {
      setTesting(false);
    }
  };

  const id = (s: string) => `nf-${s}`;
  return (
    <Modal
      open={open}
      onClose={onClose}
      wide
      title={isNew ? t('notifications.notifierForm.titleNew') : t('notifications.notifierForm.titleEdit', { name: initial.name })}
      description={isNew ? t('notifications.notifierForm.descNew') : t('notifications.notifierForm.descEdit', { type: NOTIFIER_LABEL[d.type] })}
      footer={
        <>
          <span className="nf-test">
            <GateButton need="admin" className="btn" onClick={runTest} disabled={testing} title={!isNew && dirty ? t('notifications.notifierForm.testsEdits') : undefined}>
              {testing ? <Loader2 size={14} className="spin" /> : <Send size={14} />} {t('common.sendTest')}
            </GateButton>
            <TestResultLine r={test} />
          </span>
          <span className="spacer" />
          <button className="btn" type="button" onClick={onClose}>
            {readOnly ? t('common.close') : t('common.cancel')}
          </button>
          <GateButton need="admin" className="btn btn-primary" onClick={save} disabled={busy || (!isNew && !dirty)}>
            {busy ? t('common.saving') : isNew ? t('notifications.notifiers.add') : t('common.save')}
          </GateButton>
        </>
      }
    >
      <fieldset className="form-stack fs-plain" disabled={readOnly}>
        {isNew && (
          <div className="ntf-types" role="radiogroup" aria-label={t('notifications.notifierForm.typeAria')}>
            {NOTIFIER_TYPES.map((nt) => (
              <label key={nt} className={`ntf-type ${d.type === nt ? 'selected' : ''}`}>
                <input type="radio" name="ntype" value={nt} checked={d.type === nt} onChange={() => set({ type: nt })} disabled={readOnly} />
                <NotifierIcon type={nt} />
                <span className="nt-title">{NOTIFIER_LABEL[nt]}</span>
                <span className="nt-hint">{NOTIFIER_HINT[nt]}</span>
              </label>
            ))}
          </div>
        )}
        <div className="form-grid">
          <Field label={t('common.name')} htmlFor={id('name')} error={shown.name} hint={t('notifications.notifierForm.nameHint')}>
            <input id={id('name')} className={`input ${shown.name ? 'invalid' : ''}`} value={d.name} disabled={readOnly} onChange={(e) => set({ name: e.target.value })} placeholder={d.type === 'telegram' ? t('notifications.notifierForm.namePhTelegram') : d.type === 'slack' ? t('notifications.notifierForm.namePhSlack') : t('common.name')} data-autofocus={isNew ? undefined : true} />
          </Field>
          <Field label={t('common.enabled')}>
            <span className="nf-enabled">
              <Switch checked={d.enabled} onChange={(enabled) => set({ enabled })} label={t('common.enabled')} disabled={readOnly} />
              <span className="muted">{t(d.enabled ? 'notifications.notifierForm.enabledOn' : 'notifications.notifierForm.enabledOff')}</span>
            </span>
          </Field>

          {d.type === 'telegram' && (
            <>
              <Field label={t('notifications.notifierForm.botToken')} error={shown.bot_token} className="span-2">
                <SecretInput value={d.bot_token} onChange={(bot_token) => set({ bot_token })} label={t('notifications.notifierForm.botToken')} placeholder="123456789:AAE…" invalid={!!shown.bot_token} disabled={readOnly} />
              </Field>
              <Field label={t('notifications.notifierForm.chatId')} htmlFor={id('chat')} error={shown.chat_id} hint={t('notifications.notifierForm.chatIdHint')}>
                <input id={id('chat')} className={`input mono ${shown.chat_id ? 'invalid' : ''}`} value={d.chat_id} disabled={readOnly} onChange={(e) => set({ chat_id: e.target.value })} placeholder="-1001234567890" />
              </Field>
              <Field label={t('notifications.notifierForm.topicId')} htmlFor={id('thread')} error={shown.thread_id} hint={t('notifications.notifierForm.topicIdHint')}>
                <input id={id('thread')} className={`input mono ${shown.thread_id ? 'invalid' : ''}`} value={d.thread_id} disabled={readOnly} onChange={(e) => set({ thread_id: e.target.value })} placeholder={t('notifications.notifierForm.topicIdPh')} inputMode="numeric" />
              </Field>
              <div className="span-2">
                <TelegramHelp />
              </div>
            </>
          )}

          {(d.type === 'slack' || d.type === 'discord') && (
            <Field
              label={t('notifications.notifierForm.webhookUrl')}
              className="span-2"
              error={shown.webhook_url}
              hint={t(d.type === 'slack' ? 'notifications.notifierForm.slackHint' : 'notifications.notifierForm.discordHint')}
            >
              <SecretInput value={d.webhook_url} onChange={(webhook_url) => set({ webhook_url })} label={t('notifications.notifierForm.webhookUrl')} placeholder={d.type === 'slack' ? 'https://hooks.slack.com/services/…' : 'https://discord.com/api/webhooks/…'} invalid={!!shown.webhook_url} disabled={readOnly} />
            </Field>
          )}

          {d.type === 'email' && (
            <>
              <Field label={t('notifications.notifierForm.smtpHost')} htmlFor={id('host')} error={shown.smtp_host}>
                <input id={id('host')} className={`input mono ${shown.smtp_host ? 'invalid' : ''}`} value={d.smtp_host} disabled={readOnly} onChange={(e) => set({ smtp_host: e.target.value })} placeholder="smtp.example.com" />
              </Field>
              <div className="nf-port-tls">
                <Field label={t('notifications.notifierForm.port')} htmlFor={id('port')} error={shown.smtp_port}>
                  <input id={id('port')} className={`input mono ${shown.smtp_port ? 'invalid' : ''}`} value={d.smtp_port} disabled={readOnly} onChange={(e) => set({ smtp_port: e.target.value })} inputMode="numeric" />
                </Field>
                <Field label={t('notifications.notifierForm.encryption')}>
                  <Segmented
                    value={d.tls}
                    label={t('notifications.notifierForm.smtpEncryption')}
                    onChange={(tls) => {
                      const def = SMTP_TLS.map((x) => String(x.port));
                      set({ tls, smtp_port: def.includes(d.smtp_port) ? String(SMTP_TLS.find((x) => x.value === tls)!.port) : d.smtp_port });
                    }}
                    options={SMTP_TLS.map((x) => ({ value: x.value, label: x.label }))}
                  />
                </Field>
              </div>
              <Field label={t('notifications.notifierForm.username')} htmlFor={id('user')}>
                <input id={id('user')} className="input" value={d.username} disabled={readOnly} onChange={(e) => set({ username: e.target.value })} autoComplete="off" />
              </Field>
              <Field label={t('notifications.notifierForm.password')}>
                <SecretInput value={d.password} onChange={(password) => set({ password })} label={t('notifications.notifierForm.smtpPassword')} optional disabled={readOnly} />
              </Field>
              <Field label={t('notifications.notifierForm.from')} htmlFor={id('from')} error={shown.from}>
                <input id={id('from')} className={`input ${shown.from ? 'invalid' : ''}`} value={d.from} disabled={readOnly} onChange={(e) => set({ from: e.target.value })} placeholder="fluxmq@example.com" />
              </Field>
              <Field label={t('notifications.notifierForm.to')} error={shown.to}>
                <ChipsInput label={t('notifications.notifierForm.recipients')} value={d.to} onChange={(to) => set({ to })} validate={(s) => (validEmail(s) ? null : tr('notifications.notifierForm.notEmail'))} placeholder={t('notifications.notifierForm.toPh')} invalid={!!shown.to} />
              </Field>
            </>
          )}

          {d.type === 'pagerduty' && (
            <Field label={t('notifications.notifierForm.routingKey')} className="span-2" error={shown.routing_key} hint={t('notifications.notifierForm.routingKeyHint')}>
              <SecretInput value={d.routing_key} onChange={(routing_key) => set({ routing_key })} label={t('notifications.notifierForm.routingKeyShort')} placeholder={t('notifications.notifierForm.routingKeyPh')} invalid={!!shown.routing_key} disabled={readOnly} />
            </Field>
          )}

          {d.type === 'webhook' && (
            <>
              <Field label={t('notifications.notifierForm.url')} htmlFor={id('url')} className="span-2" error={shown.url} hint={t('notifications.notifierForm.urlHint')}>
                <input id={id('url')} className={`input mono ${shown.url ? 'invalid' : ''}`} value={d.url} disabled={readOnly} onChange={(e) => set({ url: e.target.value })} placeholder="https://ops.example.com/fluxmq" />
              </Field>
              <Field label={t('notifications.notifierForm.signingSecret')} className="span-2" hint={t('notifications.notifierForm.signingSecretHint')}>
                <SecretInput value={d.secret} onChange={(secret) => set({ secret })} label={t('notifications.notifierForm.signingSecretShort')} optional disabled={readOnly} placeholder="whsec_…" />
              </Field>
              <Field label={t('notifications.notifierForm.headers')} className="span-2">
                <HeadersEditor rows={d.headers} onChange={(headers) => set({ headers })} disabled={readOnly} error={shown.header} />
              </Field>
            </>
          )}
        </div>
        {apiError && <InlineError>{apiError}</InlineError>}
      </fieldset>
    </Modal>
  );
}
