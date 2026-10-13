/** English dictionary — the source of truth. Keys are flat `namespace.key`; each namespace lives in en/<ns>.ts. */
import { common } from './en/common';
import { shell } from './en/shell';
import { settings } from './en/settings';
import { labels } from './en/labels';
import { errors } from './en/errors';
import { components } from './en/components';
import { dashboard } from './en/dashboard';
import { channels } from './en/channels';
import { messages } from './en/messages';
import { consumers } from './en/consumers';
import { dlq } from './en/dlq';
import { alerts } from './en/alerts';
import { nodes } from './en/nodes';
import { rebalance } from './en/rebalance';
import { routing } from './en/routing';
import { webhooks } from './en/webhooks';
import { notifications } from './en/notifications';
import { backups } from './en/backups';
import { security } from './en/security';
import { tokens } from './en/tokens';
import { recipes } from './en/recipes';
import { live } from './en/live';
import { scheduled } from './en/scheduled';

export const enNamespaces = { common, shell, settings, labels, errors, components, dashboard, channels, messages, consumers, dlq, alerts, nodes, rebalance, routing, webhooks, notifications, backups, security, tokens, recipes, live, scheduled };

export const en = { ...common, ...shell, ...settings, ...labels, ...errors, ...components, ...dashboard, ...channels, ...messages, ...consumers, ...dlq, ...alerts, ...nodes, ...rebalance, ...routing, ...webhooks, ...notifications, ...backups, ...security, ...tokens, ...recipes, ...live, ...scheduled };

export type Dict = typeof en;
export type TKey = keyof Dict & string;
