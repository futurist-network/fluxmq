/** Russian dictionary, typed against the English key set (RuOf): a missing or extra key fails to compile. */
import type { RuOf } from './types';
import type { Dict } from './en';
import { common } from './ru/common';
import { shell } from './ru/shell';
import { settings } from './ru/settings';
import { labels } from './ru/labels';
import { errors } from './ru/errors';
import { components } from './ru/components';
import { dashboard } from './ru/dashboard';
import { channels } from './ru/channels';
import { messages } from './ru/messages';
import { consumers } from './ru/consumers';
import { dlq } from './ru/dlq';
import { alerts } from './ru/alerts';
import { nodes } from './ru/nodes';
import { rebalance } from './ru/rebalance';
import { routing } from './ru/routing';
import { webhooks } from './ru/webhooks';
import { notifications } from './ru/notifications';
import { backups } from './ru/backups';
import { security } from './ru/security';
import { tokens } from './ru/tokens';
import { recipes } from './ru/recipes';
import { live } from './ru/live';
import { scheduled } from './ru/scheduled';

export const ruNamespaces = { common, shell, settings, labels, errors, components, dashboard, channels, messages, consumers, dlq, alerts, nodes, rebalance, routing, webhooks, notifications, backups, security, tokens, recipes, live, scheduled };

export const ru: RuOf<Dict> = { ...common, ...shell, ...settings, ...labels, ...errors, ...components, ...dashboard, ...channels, ...messages, ...consumers, ...dlq, ...alerts, ...nodes, ...rebalance, ...routing, ...webhooks, ...notifications, ...backups, ...security, ...tokens, ...recipes, ...live, ...scheduled };
