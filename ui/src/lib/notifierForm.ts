/** Notifier create/edit dialog: draft ⇄ `NotifierKind`, validation, labels. */
import type { CreateNotifierRequest, Notifier, NotifierKind, NotifierType, UpdateNotifierRequest } from '../api/types';
import { t, type TKey } from '../i18n';
import { headersFromRead, headersToRequest, newSecret, secretFromRead, secretMissing, secretToRequest, validateHeaders, type HeaderRow, type SecretField } from './secrets';

/** Product names stay as they are; only "Email" is translated. Getters: read in the current language. */
export const NOTIFIER_LABEL: Readonly<Record<NotifierType, string>> = {
  telegram: 'Telegram',
  slack: 'Slack',
  discord: 'Discord',
  get email() {
    return t('notifications.kind.email');
  },
  pagerduty: 'PagerDuty',
  webhook: 'Webhook',
};

const HINT_KEY: Record<NotifierType, TKey> = {
  telegram: 'notifications.hint.telegram',
  slack: 'notifications.hint.slack',
  discord: 'notifications.hint.discord',
  email: 'notifications.hint.email',
  pagerduty: 'notifications.hint.pagerduty',
  webhook: 'notifications.hint.webhook',
};

export const NOTIFIER_HINT: Readonly<Record<NotifierType, string>> = Object.defineProperties(
  {} as Record<NotifierType, string>,
  Object.fromEntries((Object.keys(HINT_KEY) as NotifierType[]).map((k) => [k, { enumerable: true, get: () => t(HINT_KEY[k]) }])),
);

export type SmtpTls = 'starttls' | 'tls' | 'none';
export const SMTP_TLS: { value: SmtpTls; label: string; port: number }[] = [
  { value: 'starttls', label: 'STARTTLS', port: 587 },
  { value: 'tls', label: 'TLS', port: 465 },
  {
    value: 'none',
    get label() {
      return t('notifications.notifierForm.tlsNone');
    },
    port: 25,
  },
];

export interface NotifierDraft {
  name: string;
  enabled: boolean;
  type: NotifierType;
  // telegram
  bot_token: SecretField;
  chat_id: string;
  thread_id: string;
  // slack / discord
  webhook_url: SecretField;
  // email
  smtp_host: string;
  smtp_port: string;
  username: string;
  password: SecretField;
  tls: SmtpTls;
  from: string;
  to: string[];
  // pagerduty
  routing_key: SecretField;
  // webhook
  url: string;
  secret: SecretField;
  headers: HeaderRow[];
}

export function emptyNotifierDraft(type: NotifierType = 'telegram'): NotifierDraft {
  return {
    name: '',
    enabled: true,
    type,
    bot_token: newSecret(),
    chat_id: '',
    thread_id: '',
    webhook_url: newSecret(),
    smtp_host: '',
    smtp_port: '587',
    username: '',
    password: newSecret(),
    tls: 'starttls',
    from: '',
    to: [],
    routing_key: newSecret(),
    url: '',
    secret: newSecret(),
    headers: [],
  };
}

export function draftFromNotifier(n: Notifier): NotifierDraft {
  const d = { ...emptyNotifierDraft(n.kind.type), name: n.name, enabled: n.enabled };
  const k = n.kind;
  switch (k.type) {
    case 'telegram':
      return { ...d, bot_token: secretFromRead(k.bot_token), chat_id: k.chat_id, thread_id: k.thread_id == null ? '' : String(k.thread_id) };
    case 'slack':
    case 'discord':
      return { ...d, webhook_url: secretFromRead(k.webhook_url) };
    case 'email':
      return {
        ...d,
        smtp_host: k.smtp_host,
        smtp_port: String(k.smtp_port),
        username: k.username ?? '',
        password: secretFromRead(k.password),
        tls: (['starttls', 'tls', 'none'].includes(k.tls) ? k.tls : 'starttls') as SmtpTls,
        from: k.from,
        to: [...k.to],
      };
    case 'pagerduty':
      return { ...d, routing_key: secretFromRead(k.routing_key) };
    case 'webhook':
      return { ...d, url: k.url, secret: secretFromRead(k.secret), headers: headersFromRead(k.headers) };
  }
}

const EMAIL = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
export const validEmail = (s: string) => EMAIL.test(s);

function validHttpUrl(s: string): boolean {
  try {
    const u = new URL(s);
    return u.protocol === 'https:' || u.protocol === 'http:';
  } catch {
    return false;
  }
}

export type NotifierErrors = Partial<Record<keyof NotifierDraft | 'header', string>>;

export function validateNotifierDraft(d: NotifierDraft): NotifierErrors {
  const e: NotifierErrors = {};
  if (!d.name.trim()) e.name = t('notifications.err.nameRequired');
  else if (d.name.trim().length > 64) e.name = t('notifications.err.nameTooLong');
  switch (d.type) {
    case 'telegram':
      if (secretMissing(d.bot_token)) e.bot_token = t('notifications.err.botTokenMissing');
      else if (!d.bot_token.stored || d.bot_token.replacing) {
        if (!/^\d+:[\w-]{20,}$/.test(d.bot_token.value.trim())) e.bot_token = t('notifications.err.botTokenWrong');
      }
      if (!d.chat_id.trim()) e.chat_id = t('notifications.err.chatIdRequired');
      else if (!/^(-?\d+|@[A-Za-z0-9_]{5,})$/.test(d.chat_id.trim())) e.chat_id = t('notifications.err.chatIdFormat');
      if (d.thread_id.trim() && !/^\d+$/.test(d.thread_id.trim())) e.thread_id = t('notifications.err.topicId');
      break;
    case 'slack':
    case 'discord':
      if (secretMissing(d.webhook_url)) e.webhook_url = t('notifications.err.webhookRequired');
      else if (d.webhook_url.value.trim() && !validHttpUrl(d.webhook_url.value.trim())) e.webhook_url = t('notifications.err.httpsUrl');
      break;
    case 'email': {
      if (!d.smtp_host.trim()) e.smtp_host = t('notifications.err.smtpHost');
      const port = Number(d.smtp_port);
      if (!Number.isInteger(port) || port < 1 || port > 65535) e.smtp_port = '1–65535';
      if (!validEmail(d.from.trim())) e.from = t('notifications.err.from');
      if (!d.to.length) e.to = t('notifications.err.toEmpty');
      else if (d.to.some((x) => !validEmail(x))) e.to = t('notifications.err.toInvalid');
      break;
    }
    case 'pagerduty':
      if (secretMissing(d.routing_key)) e.routing_key = t('notifications.err.routingKeyRequired');
      else if (d.routing_key.value.trim() && !/^[A-Za-z0-9]{32}$/.test(d.routing_key.value.trim())) e.routing_key = t('notifications.err.routingKeyFormat');
      break;
    case 'webhook': {
      if (!validHttpUrl(d.url.trim())) e.url = t('notifications.err.httpUrl');
      const he = validateHeaders(d.headers);
      const first = Object.values(he)[0];
      if (first) e.header = first;
      break;
    }
  }
  return e;
}

/** Draft → `NotifierKind` (secrets per the uniform rule: kept = REDACTED). */
export function draftToKind(d: NotifierDraft): NotifierKind {
  switch (d.type) {
    case 'telegram':
      return { type: 'telegram', bot_token: secretToRequest(d.bot_token), chat_id: d.chat_id.trim(), thread_id: d.thread_id.trim() ? Number(d.thread_id.trim()) : null };
    case 'slack':
    case 'discord':
      return { type: d.type, webhook_url: secretToRequest(d.webhook_url) };
    case 'email':
      return {
        type: 'email',
        smtp_host: d.smtp_host.trim(),
        smtp_port: Number(d.smtp_port),
        username: d.username.trim() || null,
        password: secretToRequest(d.password, true),
        tls: d.tls,
        from: d.from.trim(),
        to: d.to,
      };
    case 'pagerduty':
      return { type: 'pagerduty', routing_key: secretToRequest(d.routing_key) };
    case 'webhook':
      return { type: 'webhook', url: d.url.trim(), secret: secretToRequest(d.secret, true), headers: headersToRequest(d.headers) };
  }
}

export function draftToCreate(d: NotifierDraft): CreateNotifierRequest {
  return { name: d.name.trim(), enabled: d.enabled, kind: draftToKind(d) };
}

export function draftToUpdate(d: NotifierDraft): UpdateNotifierRequest {
  return { name: d.name.trim(), enabled: d.enabled, kind: draftToKind(d) };
}

/** One-line destination for the list ("chat −100123 · topic 7", "ops@acme.io +2"). */
export function notifierTarget(k: NotifierKind): string {
  switch (k.type) {
    case 'telegram':
      return k.thread_id != null ? t('notifications.target.telegramTopic', { chat: k.chat_id, topic: k.thread_id }) : t('notifications.target.telegram', { chat: k.chat_id });
    case 'slack':
      return t('notifications.target.slack');
    case 'discord':
      return t('notifications.target.discord');
    case 'email':
      return t('notifications.target.email', { to: `${k.to[0] ?? '—'}${k.to.length > 1 ? ` +${k.to.length - 1}` : ''}`, host: k.smtp_host });
    case 'pagerduty':
      return 'Events API v2';
    case 'webhook':
      try {
        return new URL(k.url).host;
      } catch {
        return k.url;
      }
  }
}
