import { Hash, Mail, MessagesSquare, Send, Siren, Webhook, type LucideIcon } from 'lucide-react';
import type { NotifierType, TestResult } from '../../api/types';
import { toast } from '../../store/ui';
import { t } from '../../i18n';

export const NOTIFIER_ICON: Record<NotifierType, LucideIcon> = { telegram: Send, slack: Hash, discord: MessagesSquare, email: Mail, pagerduty: Siren, webhook: Webhook };

/** Result toast of a "Send test" (latency or error). */
export function toastTest(r: TestResult, what: string): void {
  if (r.ok) toast('success', t('notifications.test.delivered', { what, ms: r.latency_ms }));
  else toast('error', t('notifications.test.failed', { what, error: r.error ?? t('notifications.test.failedGeneric'), ms: r.latency_ms }));
}
