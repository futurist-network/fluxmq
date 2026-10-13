import { lazyPage } from '../lib/lazyPage';

// v0.3 screens are split out of the main chunk (loaded on first visit, or preloaded).
export const NotificationsPage = lazyPage(() => import('./notifications/NotificationsPage'));
export const BackupsPage = lazyPage(() => import('./backups/BackupsPage'));
export const WebhooksPage = lazyPage(() => import('./webhooks/WebhooksPage'));
export const WebhookEditorPage = lazyPage(() => import('./webhooks/WebhookEditor'));
export const SecurityPage = lazyPage(() => import('./security/SecurityPage'));
export const ScheduledPage = lazyPage(() => import('./scheduled/ScheduledPage'));
export const LivePage = lazyPage(() => import('./live/LivePage'));
export const DocsPage = lazyPage(() => import('./docs/DocsPage'));

/** The settings tabs that live in their own chunks: preloaded when any settings tab shows. */
export function preloadSettingsPages() {
  NotificationsPage.preload();
  BackupsPage.preload();
  SecurityPage.preload();
}
