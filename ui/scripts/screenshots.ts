/**
 * Visual QA: screenshots of every screen.
 *
 *   pnpm mock &                       # :7421 demo scenario
 *   MOCK_SCENARIO=empty PORT=7422 pnpm mock &
 *   pnpm dev &                        # :5173 → :7421
 *   FLUXMQ_URL=http://localhost:7422 pnpm dev --port 5174 &   (empty states)
 *   pnpm screenshots
 *
 * Env: BASE (default http://localhost:5173), EMPTY_BASE (optional), DOWN_BASE (optional,
 * a dev server proxied to a dead port, for the connection-lost banner), ONLY (substring filter).
 *
 * Against the real server (it serves ui/dist itself):
 *   BASE=http://localhost:7421 OUT=screenshots/real KEY=<an existing order key> pnpm screenshots
 * OUT is relative to ui/ (default `screenshots`); KEY is the message key searched in
 * `channel-messages-key` (default `ord_8842`, a mock key); DLQ_REASON is the reason opened in
 * `dlq-entry` (default `PostgreSQL timeout`); TOKEN is put in localStorage as the
 * admin token (for a server started with --admin-token).
 *
 * v0.2 screens (routing, RBAC, cluster) — extra bases, each a dev server on its own mock:
 *   CLUSTER_BASE  MOCK_SCENARIO=cluster MOCK_ROLE=admin   (node failure cycle, seeded tokens;
 *                 start the mock ~15 s before so n3 is down when `nodes-cluster` is taken)
 *   VIEWER_BASE   MOCK_SCENARIO=cluster MOCK_ROLE=viewer  (role-gated UI)
 *   SINGLE_BASE   MOCK_SCENARIO=single                    (single-node Nodes page)
 * ROUTE_ID (default rt_hv01) is the route opened in `route-editor`; V02_ONLY=1 skips v0.1 shots.
 *
 * v0.3 screens (notifications, webhooks, backups): V03_ONLY=1 takes only those (OUT defaults to
 * `screenshots/v0.3` then); EMPTY_BASE / VIEWER_BASE add empty-state and read-only shots.
 * Against the real server: BASE=http://127.0.0.1:<port> OUT=screenshots/real-v0.3 V03_ONLY=1
 * WEBHOOK_ID=<id> WEBHOOK_FAIL_ID=<id> ONLY=… (mock ids otherwise).
 *
 * v0.4 screens (rebalancing, TLS): V04_ONLY=1 takes only those (OUT defaults to `screenshots/v0.4`).
 *   BASE            default mock (3 nodes, balanced; TLS on with a cluster cert expiring in 9 days)
 *   REBALANCE_BASE  MOCK_SCENARIO=rebalance — start the mock ~20 s before so the plan is mid-way
 *   REPAIR_BASE     MOCK_SCENARIO=rebalance MOCK_REBALANCE_MODE=repair (n4 empty → dry run, config)
 *   SINGLE_BASE     MOCK_SCENARIO=single MOCK_TLS=off (single-node empty state, plaintext TLS hint)
 *   VIEWER_BASE     optional, MOCK_SCENARIO=rebalance MOCK_ROLE=viewer (gated controls)
 *   CLUSTER_BASE    optional, MOCK_SCENARIO=cluster, started ~20 s before (n3 down → down node in the load)
 *
 * v0.5 screens (cert bindings, revocation, decommission): V05_ONLY=1 (OUT defaults to `screenshots/v0.5`).
 *   DECOM_BASE   MOCK_SCENARIO=decommission MOCK_ROLE=admin — tokens + bindings (mTLS on), revoke and
 *                last-admin, then decommission n4 end to end (confirm → in progress → removed; ~45 s,
 *                mutates the mock: restart it before re-running) and forget the dead n5
 *   INERT_BASE   MOCK_CLIENT_MTLS=off MOCK_NO_DECOMMISSION_DRY_RUN=1 MOCK_ROLE=admin — bindings inert
 *                warning; decommission dialog without a server dry run (3 nodes: 409 predicted)
 *   CANCEL_BASE  MOCK_SCENARIO=decommission MOCK_ROLE=admin (a second mock) — start, then cancel
 *   REVOKE_BASE  MOCK_ROLE=admin MOCK_WS_REVOKE_AFTER=3 — the console's session closed with 4401
 *                (once per token value: restart the mock before re-running)
 *
 * REAL_V03=1: the lean v0.3 set taken against a REAL server (integration QA; replaces every
 * other shot): BASE, TOKEN, WEBHOOK_ID (a failing webhook), NOTIFIER (name of a stored
 * notifier to edit), OUT (default screenshots/real-v0.3); CLUSTER_BASE adds the cluster
 * backups page of a real cluster node.
 */
import { mkdirSync } from 'node:fs';
import { chromium, type Page } from 'playwright';

const BASE = process.env.BASE ?? 'http://localhost:5173';
const EMPTY_BASE = process.env.EMPTY_BASE;
const DOWN_BASE = process.env.DOWN_BASE;
const ONLY = process.env.ONLY;
const KEY = process.env.KEY ?? 'ord_8842';
const DLQ_REASON = process.env.DLQ_REASON ?? 'PostgreSQL timeout';
const TOKEN = process.env.TOKEN;
const CLUSTER_BASE = process.env.CLUSTER_BASE;
const VIEWER_BASE = process.env.VIEWER_BASE;
const SINGLE_BASE = process.env.SINGLE_BASE;
const ROUTE_ID = process.env.ROUTE_ID ?? 'rt_hv01';
const V02_ONLY = process.env.V02_ONLY === '1';
const V03_ONLY = process.env.V03_ONLY === '1';
const V04_ONLY = process.env.V04_ONLY === '1';
const REBALANCE_BASE = process.env.REBALANCE_BASE;
const REPAIR_BASE = process.env.REPAIR_BASE;
const V05_ONLY = process.env.V05_ONLY === '1';
const DECOM_BASE = process.env.DECOM_BASE;
const INERT_BASE = process.env.INERT_BASE;
const REVOKE_BASE = process.env.REVOKE_BASE;
const CANCEL_BASE = process.env.CANCEL_BASE;
/** Webhook ids opened by the v0.3 editor shots (mock defaults). */
const WEBHOOK_ID = process.env.WEBHOOK_ID ?? 'wh_crm01';
const WEBHOOK_FAIL_ID = process.env.WEBHOOK_FAIL_ID ?? 'wh_erp03';
const OUT = new URL(`../${(process.env.OUT ?? (process.env.REAL_V03 === '1' ? 'screenshots/real-v0.3' : V05_ONLY ? 'screenshots/v0.5' : V04_ONLY ? 'screenshots/v0.4' : V03_ONLY ? 'screenshots/v0.3' : 'screenshots')).replace(/\/?$/, '/')}`, import.meta.url).pathname;
mkdirSync(OUT, { recursive: true });

interface Shot {
  name: string;
  path: string;
  base?: string;
  wait?: number;
  viewport?: { width: number; height: number };
  fullPage?: boolean;
  theme?: 'light';
  act?: (p: Page) => Promise<void>;
  /** Screenshot only this element (after `act`). */
  selector?: string;
  /** Stored console token for this shot (localStorage). */
  token?: string;
}

const desktop = { width: 1440, height: 900 };

const shots: Shot[] = [
  { name: 'dashboard', path: '/', wait: 6000, fullPage: true },
  { name: 'dashboard-viewport', path: '/', wait: 6000 },
  { name: 'dashboard-mobile', path: '/', wait: 6000, viewport: { width: 390, height: 844 }, fullPage: true },
  { name: 'dashboard-light', path: '/', wait: 6000, theme: 'light' },
  { name: 'channels', path: '/channels', wait: 3000 },
  { name: 'channel-overview', path: '/channels/orders.events', wait: 4000, fullPage: true },
  {
    name: 'channel-messages',
    path: '/channels/orders.events/messages',
    wait: 2500,
    act: async (p) => {
      await p.locator('.msg-row').nth(3).click();
      await p.waitForTimeout(1200);
    },
  },
  {
    name: 'channel-messages-key',
    path: `/channels/orders.events/messages?key=${encodeURIComponent(KEY)}`,
    wait: 2500,
    act: async (p) => {
      const rows = p.locator('.msg-row');
      if ((await rows.count()) > 0) await rows.last().click();
      await p.waitForTimeout(1200);
    },
  },
  {
    name: 'channel-tail',
    path: '/channels/orders.events/messages?mode=tail',
    wait: 3000,
    act: async (p) => {
      await p.locator('.msg-row').nth(2).click();
      await p.waitForTimeout(1200);
    },
  },
  { name: 'channel-consumers-email', path: '/channels/email/consumers', wait: 3000, fullPage: true },
  {
    name: 'instance-drawer',
    path: '/channels/email/consumers',
    wait: 3000,
    act: async (p) => {
      await p.locator('tr.row-warning, tr.row-critical').first().click();
      await p.waitForTimeout(600);
    },
  },
  {
    name: 'reset-offsets',
    path: '/channels/email/consumers',
    wait: 2500,
    act: async (p) => {
      await p.getByRole('button', { name: 'Reset offsets' }).first().click();
      await p.waitForTimeout(400);
    },
  },
  { name: 'channel-storage', path: '/channels/orders.events/storage', wait: 3000 },
  { name: 'channel-settings', path: '/channels/orders.events/settings', wait: 2000, fullPage: true },
  { name: 'consumers', path: '/consumers', wait: 3500, fullPage: true },
  { name: 'dlq', path: '/dlq', wait: 2500, fullPage: true },
  {
    name: 'dlq-entry',
    path: `/dlq?reason=${encodeURIComponent(DLQ_REASON)}`,
    wait: 2500,
    act: async (p) => {
      await p.locator('.dlq-table tbody tr').first().click();
      await p.waitForTimeout(600);
    },
  },
  { name: 'alerts', path: '/alerts', wait: 2500 },
  { name: 'nodes', path: '/nodes', wait: 2500 },
  { name: 'settings', path: '/settings', wait: 1500 },
  {
    name: 'palette',
    path: '/',
    wait: 2500,
    act: async (p) => {
      await p.keyboard.press('Meta+k');
      await p.waitForTimeout(200);
      await p.keyboard.type('orders');
      await p.waitForTimeout(500);
    },
  },
  {
    name: 'create-step1',
    path: '/channels',
    wait: 1500,
    act: async (p) => {
      await p.getByRole('button', { name: 'Create channel' }).first().click();
      await p.waitForTimeout(300);
    },
  },
  {
    name: 'create-step2',
    path: '/channels',
    wait: 1500,
    act: async (p) => {
      await p.getByRole('button', { name: 'Create channel' }).first().click();
      await p.getByText('Distribute jobs between workers').click();
      await p.getByRole('button', { name: 'Continue' }).click();
      await p.locator('#c-name').fill('Bad name!');
      await p.getByText('Advanced').click();
      await p.waitForTimeout(300);
    },
  },
  { name: 'not-found', path: '/nope', wait: 1000 },
];

const v02: Shot[] = [
  { name: 'v2-routing', path: '/routing', wait: 3000 },
  { name: 'v2-route-editor', path: `/routing/${ROUTE_ID}`, wait: 3500, fullPage: true },
  {
    name: 'v2-route-editor-new',
    path: '/routing/new?source=orders.events',
    wait: 2500,
    act: async (p) => {
      await p.getByLabel('Route name').fill('Moldova orders');
      const field = p.getByLabel('Field').first();
      await field.click();
      await p.waitForTimeout(300);
      await field.fill('shipping.');
      await p.waitForTimeout(400);
      await p.screenshot({ path: `${OUT}v2-route-editor-autocomplete.png` });
      await field.fill('shipping.country');
      await p.getByLabel('Value').first().fill('DE');
      await p.getByRole('button', { name: 'Add condition' }).click();
      await p.getByLabel('Field').nth(1).fill('total');
      await p.getByLabel('Operator').nth(1).selectOption('gte');
      await p.getByLabel('Value').nth(1).fill('100');
      await p.getByLabel('Add target channel').selectOption('orders.priority');
      await p.getByLabel('Add target channel').selectOption('notifications');
      await p.waitForTimeout(2200);
    },
  },
  {
    name: 'v2-route-editor-text-error',
    path: '/routing/new?source=orders.events&filter=' + encodeURIComponent('header.flux-type = "order.created" AND total >= OR 100'),
    wait: 2500,
  },
  {
    name: 'v2-route-editor-text-ok',
    path: '/routing/new?source=payments.events&filter=' + encodeURIComponent('header.flux-type=payment.failed AND amount > 100'),
    wait: 3000,
  },
  { name: 'v2-route-editor-mobile', path: `/routing/${ROUTE_ID}`, wait: 3500, viewport: { width: 390, height: 844 }, fullPage: true },
  { name: 'v2-channel-routing', path: '/channels/orders.events/routing', wait: 3000, fullPage: true },
  { name: 'v2-channel-routing-target', path: '/channels/orders.priority/routing', wait: 3000 },
  {
    name: 'v2-consumers-router',
    path: '/channels/orders.events/consumers',
    wait: 3000,
    act: async (p) => {
      await p.locator('[aria-label="Consumer group $router"]').scrollIntoViewIfNeeded();
      await p.waitForTimeout(300);
    },
  },
  {
    name: 'v2-messages-filter',
    path: '/channels/orders.events/messages?filter=' + encodeURIComponent('shipping.country = "DE" AND total >= 200'),
    wait: 3500,
    act: async (p) => {
      await p.locator('.msg-row').first().click();
      await p.waitForTimeout(1000);
    },
  },
  {
    name: 'v2-messages-filter-error',
    path: '/channels/orders.events/messages',
    wait: 2500,
    act: async (p) => {
      await p.locator('#q-filter').fill('order_id = 8842 AND country == ');
      await p.waitForTimeout(900);
    },
  },
  {
    name: 'v2-messages-filter-examples',
    path: '/channels/orders.events/messages',
    wait: 3000,
    act: async (p) => {
      await p.getByRole('button', { name: 'Examples' }).click();
      await p.waitForTimeout(300);
    },
  },
  {
    name: 'v2-palette-filter',
    path: '/channels/orders.events',
    wait: 2500,
    act: async (p) => {
      await p.keyboard.press('Meta+k');
      await p.waitForTimeout(200);
      await p.keyboard.type('total > 500');
      await p.waitForTimeout(500);
    },
  },
  { name: 'v2-tokens-open-mode', path: '/settings/tokens', wait: 2500 },
  {
    name: 'v2-token-create-first',
    path: '/settings/tokens?create=admin',
    wait: 2000,
  },
  { name: 'v2-nodes-cluster-healthy', path: '/nodes', wait: 3500, fullPage: true },
  { name: 'v2-dashboard', path: '/', wait: 5000 },
];
if (CLUSTER_BASE) {
  v02.push(
    { name: 'v2-nodes-cluster-failover', path: '/nodes', base: CLUSTER_BASE, wait: 3500, fullPage: true },
    {
      name: 'v2-nodes-reassign',
      path: '/nodes',
      base: CLUSTER_BASE,
      wait: 3000,
      act: async (p) => {
        await p.locator('.mx-cell.under, .mx-cell').first().click();
        await p.waitForTimeout(400);
      },
    },
    {
      name: 'v2-nodes-drain',
      path: '/nodes',
      base: CLUSTER_BASE,
      wait: 3000,
      act: async (p) => {
        await p.getByRole('button', { name: 'Drain leadership' }).first().click();
        await p.waitForTimeout(400);
      },
    },
    { name: 'v2-alerts-cluster', path: '/alerts', base: CLUSTER_BASE, wait: 3000 },
    { name: 'v2-tokens', path: '/settings/tokens', base: CLUSTER_BASE, wait: 2500, fullPage: true },
    {
      name: 'v2-token-create',
      path: '/settings/tokens',
      base: CLUSTER_BASE,
      wait: 2500,
      act: async (p) => {
        await p.getByRole('button', { name: 'New token' }).click();
        await p.locator('#t-name').fill('payments-consumer');
        await p.getByText('Viewer', { exact: true }).click();
        await p.getByLabel('Consume from').fill('payments.*');
        await p.keyboard.press('Enter');
        await p.getByLabel('Publish to').fill('billing.*');
        await p.keyboard.press('Enter');
        await p.waitForTimeout(300);
      },
    },
    {
      name: 'v2-token-secret',
      path: '/settings/tokens',
      base: CLUSTER_BASE,
      wait: 2500,
      act: async (p) => {
        await p.getByRole('button', { name: 'New token' }).click();
        await p.locator('#t-name').fill(`ci-${Date.now() % 10000}`);
        await p.getByRole('button', { name: 'Create token' }).click();
        await p.waitForTimeout(800);
      },
    },
    { name: 'v2-consumers-cluster', path: '/channels/email/consumers', base: CLUSTER_BASE, wait: 3000 },
  );
}
if (VIEWER_BASE) {
  v02.push(
    { name: 'v2-viewer-dlq', path: '/dlq', base: VIEWER_BASE, wait: 3000 },
    { name: 'v2-viewer-route-editor', path: `/routing/${ROUTE_ID}`, base: VIEWER_BASE, wait: 3500 },
    { name: 'v2-viewer-routing', path: '/routing', base: VIEWER_BASE, wait: 3000 },
    { name: 'v2-viewer-tokens', path: '/settings/tokens', base: VIEWER_BASE, wait: 2500 },
    { name: 'v2-viewer-channel-settings', path: '/channels/orders.events/settings', base: VIEWER_BASE, wait: 2500 },
    {
      name: 'v2-viewer-tooltip',
      path: '/channels/email/consumers',
      base: VIEWER_BASE,
      wait: 3000,
      act: async (p) => {
        await p.locator('[data-gated]').first().hover();
        await p.waitForTimeout(1200);
      },
    },
  );
}
if (SINGLE_BASE) {
  v02.push({ name: 'v2-nodes-single', path: '/nodes', base: SINGLE_BASE, wait: 3000 });
}
if (V02_ONLY) shots.length = 0;
shots.push(...v02);

const v03: Shot[] = [
  { name: 'v3-notifications', path: '/settings/notifications', wait: 3000, fullPage: true },
  {
    name: 'v3-notifier-new-telegram',
    path: '/settings/notifications?add=telegram',
    wait: 2000,
    act: async (p) => {
      await p.locator('#nf-name').fill('Team Telegram');
      await p.getByLabel('Bot token').fill('7123456789:AAE-demo-token-abcdefghijklmn');
      await p.locator('#nf-chat').fill('-1001987654321');
      await p.getByRole('dialog').getByRole('button', { name: 'Send test' }).click();
      await p.waitForTimeout(1200);
    },
  },
  {
    name: 'v3-notifier-edit-secrets',
    path: '/settings/notifications',
    wait: 2500,
    act: async (p) => {
      await p.getByRole('button', { name: 'Slack #payments-alerts', exact: true }).click();
      await p.waitForTimeout(400);
    },
  },
  {
    name: 'v3-rule-editor',
    path: '/settings/notifications',
    wait: 2500,
    act: async (p) => {
      await p.getByRole('button', { name: 'Payments team', exact: true }).click();
      await p.waitForTimeout(500);
    },
  },
  { name: 'v3-webhooks', path: '/webhooks', wait: 3500 },
  { name: 'v3-webhook-editor', path: `/webhooks/${WEBHOOK_ID}`, wait: 3500, fullPage: true },
  {
    name: 'v3-webhook-failing-test',
    path: `/webhooks/${WEBHOOK_FAIL_ID}`,
    wait: 3000,
    act: async (p) => {
      await p.getByRole('button', { name: 'Send test', exact: true }).first().click();
      await p.waitForTimeout(1500);
    },
  },
  {
    name: 'v3-webhook-new',
    path: '/webhooks/new?channel=orders.events',
    wait: 2500,
    fullPage: true,
    act: async (p) => {
      await p.getByLabel('Webhook name').fill('CRM Sync');
      await p.locator('#wh-url').fill('https://hooks.example.com/fluxmq');
      await p.getByRole('button', { name: 'Raw body' }).click();
      await p.getByRole('button', { name: 'Create webhook' }).click();
      await p.waitForTimeout(600);
    },
  },
  {
    name: 'v3-consumers-webhook',
    path: '/channels/inventory.updates/consumers',
    wait: 3000,
    act: async (p) => {
      await p.locator('[aria-label="Consumer group $webhook:legacy-erp"]').scrollIntoViewIfNeeded();
      await p.waitForTimeout(300);
    },
  },
  { name: 'v3-dlq-webhook', path: '/dlq?channel=inventory.updates', wait: 3000 },
  { name: 'v3-backups', path: '/settings/backups', wait: 3000, fullPage: true },
  {
    name: 'v3-backups-restore',
    path: '/settings/backups',
    wait: 3000,
    act: async (p) => {
      await p.getByRole('button', { name: 'Restore…' }).nth(2).click();
      await p.waitForTimeout(900);
    },
  },
  { name: 'v3-backups-mobile', path: '/settings/backups', wait: 3000, viewport: { width: 390, height: 844 } },
  { name: 'v3-channels-system', path: '/channels', wait: 3000 },
  { name: 'v3-system-channel-messages', path: '/channels/%24system.events/messages', wait: 3000 },
  {
    name: 'v3-palette',
    path: '/',
    wait: 2500,
    act: async (p) => {
      await p.keyboard.press('Meta+k');
      await p.waitForTimeout(200);
      await p.keyboard.type('backup');
      await p.waitForTimeout(400);
    },
  },
];
if (EMPTY_BASE) {
  v03.push(
    { name: 'v3-empty-notifications', path: '/settings/notifications', base: EMPTY_BASE, wait: 2500, fullPage: true },
    { name: 'v3-empty-webhooks', path: '/webhooks', base: EMPTY_BASE, wait: 2500 },
    { name: 'v3-empty-backups', path: '/settings/backups', base: EMPTY_BASE, wait: 2500, fullPage: true },
    {
      name: 'v3-backups-encryption',
      path: '/settings/backups',
      base: EMPTY_BASE,
      wait: 2500,
      act: async (p) => {
        await p.getByRole('button', { name: 'Cloudflare R2' }).click();
        await p.getByText('Encrypt backups client-side').click();
        await p.getByRole('button', { name: 'Generate' }).click();
        await p.getByRole('button', { name: 'Save & enable backups' }).click();
        await p.locator('.bk-enc').scrollIntoViewIfNeeded();
        await p.waitForTimeout(400);
      },
    },
  );
}
if (VIEWER_BASE) {
  v03.push(
    { name: 'v3-viewer-notifications', path: '/settings/notifications', base: VIEWER_BASE, wait: 3000 },
    { name: 'v3-viewer-webhook-editor', path: `/webhooks/${WEBHOOK_ID}`, base: VIEWER_BASE, wait: 3500 },
    { name: 'v3-viewer-backups', path: '/settings/backups', base: VIEWER_BASE, wait: 3000 },
  );
}
if (V03_ONLY) shots.length = 0;
shots.push(...v03);

if (EMPTY_BASE && !V03_ONLY) {
  shots.push(
    { name: 'empty-dashboard', path: '/', base: EMPTY_BASE, wait: 3500 },
    { name: 'empty-channels', path: '/channels', base: EMPTY_BASE, wait: 2500 },
    { name: 'empty-dlq', path: '/dlq', base: EMPTY_BASE, wait: 2500 },
    { name: 'empty-consumers', path: '/consumers', base: EMPTY_BASE, wait: 2500 },
  );
}
if (DOWN_BASE && !V03_ONLY) {
  shots.push({ name: 'connection-lost', path: '/', base: DOWN_BASE, wait: 2500 });
}

if (process.env.REAL_V03 === '1') {
  const notifier = process.env.NOTIFIER ?? 'generic-hook';
  shots.length = 0;
  shots.push(
    { name: 'notifications', path: '/settings/notifications', wait: 3000, fullPage: true },
    {
      name: 'notifier-edit-draft-test',
      path: '/settings/notifications',
      wait: 2500,
      act: async (p) => {
        await p.getByRole('button', { name: notifier, exact: true }).first().click();
        await p.waitForTimeout(400);
        const url = p.getByRole('dialog').getByLabel('URL', { exact: false }).first();
        await url.fill('http://127.0.0.1:9/hook');
        await p.getByRole('dialog').getByRole('button', { name: 'Send test' }).click();
        await p.waitForTimeout(1500);
      },
    },
    { name: 'webhooks', path: '/webhooks', wait: 3500 },
    { name: 'webhook-editor', path: `/webhooks/${WEBHOOK_ID}`, wait: 3500, fullPage: true },
    {
      name: 'webhook-test-blocked',
      path: `/webhooks/${WEBHOOK_ID}`,
      wait: 3000,
      act: async (p) => {
        await p.getByRole('button', { name: 'Send test', exact: true }).first().click();
        await p.waitForTimeout(1500);
      },
    },
    { name: 'dlq-webhook-reasons', path: '/dlq', wait: 3000 },
    { name: 'backups', path: '/settings/backups', wait: 3500, fullPage: true },
    {
      name: 'backups-restore',
      path: '/settings/backups',
      wait: 3500,
      act: async (p) => {
        await p.getByRole('button', { name: 'Restore…' }).first().click();
        await p.waitForTimeout(900);
        await p.getByText('Channels in this backup', { exact: false }).first().click();
        await p.waitForTimeout(300);
      },
    },
    { name: 'channels-system', path: '/channels', wait: 3000 },
    { name: 'system-channel-messages', path: '/channels/%24system.events/messages', wait: 3000 },
  );
  if (CLUSTER_BASE) {
    shots.push({ name: 'cluster-backups', path: '/settings/backups', base: CLUSTER_BASE, wait: 3500, fullPage: true });
  }
}

const scrollToRebalancing = async (p: Page) => {
  await p.locator('#rebalancing').scrollIntoViewIfNeeded();
  await p.waitForTimeout(300);
};

if (V04_ONLY) {
  shots.length = 0;
  shots.push(
    { name: 'rebalancing-idle', path: '/nodes', wait: 3500, selector: '#rebalancing', act: scrollToRebalancing },
    { name: 'nodes-idle-full', path: '/nodes', wait: 3500, fullPage: true },
    { name: 'tls', path: '/settings/security', wait: 2500, fullPage: true },
    { name: 'tls-light', path: '/settings/security', wait: 2500, fullPage: true, theme: 'light' },
  );
  if (REBALANCE_BASE) {
    shots.push(
      { name: 'rebalancing-running', path: '/nodes', base: REBALANCE_BASE, wait: 4000, selector: '#rebalancing', act: scrollToRebalancing },
      { name: 'nodes-running-full', path: '/nodes', base: REBALANCE_BASE, wait: 4000, fullPage: true },
      { name: 'events-v04', path: '/', base: REBALANCE_BASE, wait: 4000, selector: '.events' },
      { name: 'rebalancing-running-light', path: '/nodes', base: REBALANCE_BASE, wait: 4000, selector: '#rebalancing', theme: 'light', act: scrollToRebalancing },
      { name: 'rebalancing-running-mobile', path: '/nodes', base: REBALANCE_BASE, wait: 4000, viewport: { width: 390, height: 844 }, selector: '#rebalancing', act: scrollToRebalancing },
      {
        name: 'rebalance-cancel-confirm',
        path: '/nodes',
        base: REBALANCE_BASE,
        wait: 4000,
        act: async (p) => {
          await p.getByRole('button', { name: 'Cancel', exact: true }).first().click();
          await p.waitForTimeout(400);
        },
      },
      // Last on this mock: it really cancels the plan (pending moves → cancelled).
      {
        name: 'rebalancing-cancelled',
        path: '/nodes',
        base: REBALANCE_BASE,
        wait: 4000,
        selector: '#rebalancing',
        act: async (p) => {
          await p.getByRole('button', { name: 'Cancel', exact: true }).first().click();
          await p.waitForTimeout(300);
          await p.getByRole('button', { name: 'Cancel plan' }).click();
          await p.waitForTimeout(1500);
          const more = p.getByRole('button', { name: /Show \d+ more/ });
          if (await more.count()) await more.click();
          await p.waitForTimeout(300);
        },
      },
    );
  }
  if (REPAIR_BASE) {
    shots.push(
      { name: 'rebalancing-repair-mode', path: '/nodes', base: REPAIR_BASE, wait: 3500, selector: '#rebalancing', act: scrollToRebalancing },
      {
        name: 'rebalance-dry-run',
        path: '/nodes',
        base: REPAIR_BASE,
        wait: 3500,
        viewport: { width: 1440, height: 1100 },
        act: async (p) => {
          await p.getByRole('button', { name: 'Rebalance now' }).click();
          await p.waitForTimeout(1500);
        },
      },
      {
        name: 'rebalance-dry-run-moves',
        path: '/nodes',
        base: REPAIR_BASE,
        wait: 3500,
        viewport: { width: 1440, height: 1100 },
        act: async (p) => {
          await p.getByRole('button', { name: 'Rebalance now' }).click();
          await p.waitForTimeout(1500);
          await p.locator('.modal-body').evaluate((el) => el.scrollTo(0, el.scrollHeight));
          await p.waitForTimeout(300);
        },
      },
      {
        name: 'rebalance-dry-run-filtered',
        path: '/nodes',
        base: REPAIR_BASE,
        wait: 3500,
        act: async (p) => {
          await p.getByRole('button', { name: 'Rebalance now' }).click();
          await p.waitForTimeout(1200);
          const input = p.getByRole('dialog').getByLabel('Channels');
          await input.fill('orders.events');
          await input.press('Enter');
          await p.getByRole('button', { name: 'Preview again' }).click();
          await p.waitForTimeout(1200);
        },
      },
      {
        name: 'rebalance-config',
        path: '/nodes',
        base: REPAIR_BASE,
        wait: 3500,
        viewport: { width: 1440, height: 1000 },
        act: async (p) => {
          await p.getByRole('button', { name: 'Configure' }).click();
          await p.waitForTimeout(400);
          const d = p.getByRole('dialog');
          await d.getByText('Full', { exact: true }).click();
          await d.getByLabel('Unlimited').uncheck();
          await d.getByLabel('Throttle', { exact: true }).fill('40');
          await p.waitForTimeout(300);
        },
      },
    );
  }
  if (SINGLE_BASE) {
    shots.push(
      { name: 'rebalancing-single-node', path: '/nodes', base: SINGLE_BASE, wait: 3000, fullPage: true },
      { name: 'tls-plaintext', path: '/settings/security', base: SINGLE_BASE, wait: 2500, fullPage: true },
    );
  }
  if (CLUSTER_BASE) {
    // MOCK_SCENARIO=cluster: n3 is down between ~12 s and ~100 s after the mock starts.
    shots.push({ name: 'rebalancing-node-down', path: '/nodes', base: CLUSTER_BASE, wait: 3500, selector: '#rebalancing', act: scrollToRebalancing });
  }
  if (VIEWER_BASE) {
    shots.push({ name: 'rebalancing-viewer', path: '/nodes', base: VIEWER_BASE, wait: 4000, selector: '#rebalancing', act: scrollToRebalancing });
  }
}

if (V05_ONLY) {
  shots.length = 0;
  const bindings = 'section[aria-label="Certificate bindings"]';
  const mobile = { width: 390, height: 844 };
  /** Clip to an element without the sticky top bar (same as `selector`, usable inside `act`). */
  const snap = async (p: Page, name: string, selector?: string) => {
    if (!selector) {
      await p.screenshot({ path: `${OUT}${name}.png` });
    } else {
      const box = await p.locator(selector).first().evaluate((el) => {
        const r = el.getBoundingClientRect();
        return { x: r.left + window.scrollX, y: r.top + window.scrollY, width: r.width, height: r.height };
      });
      await p.evaluate(() => window.scrollTo(0, 0));
      await p.waitForTimeout(150);
      await p.screenshot({ path: `${OUT}${name}.png`, fullPage: true, clip: { x: Math.max(0, box.x - 12), y: Math.max(0, box.y - 12), width: box.width + 24, height: box.height + 24 } });
    }
    console.log(`${name}.png (in act)`);
  };
  if (DECOM_BASE) {
    shots.push(
      { name: 'cert-bindings', path: '/settings/tokens', base: DECOM_BASE, wait: 2500, selector: bindings },
      { name: 'cert-bindings-light', path: '/settings/tokens', base: DECOM_BASE, wait: 2500, selector: bindings, theme: 'light' },
      { name: 'cert-bindings-mobile', path: '/settings/tokens', base: DECOM_BASE, wait: 2500, selector: bindings, viewport: mobile },
      { name: 'tokens-page-full', path: '/settings/tokens', base: DECOM_BASE, wait: 2500, fullPage: true },
      {
        name: 'cert-binding-create',
        path: '/settings/tokens',
        base: DECOM_BASE,
        wait: 2500,
        viewport: { width: 1440, height: 1000 },
        act: async (p) => {
          await p.getByRole('button', { name: 'New binding' }).click();
          await p.waitForTimeout(300);
          await p.getByLabel('Certificate subject CN').fill('billing-worker.prod-eu-1');
          await p.getByRole('dialog').getByText('Operator', { exact: true }).click();
          const pub = p.getByRole('dialog').getByLabel('Publish to');
          await pub.fill('billing.*');
          await pub.press('Enter');
          await p.waitForTimeout(300);
        },
      },
      {
        name: 'revoke-confirm',
        path: '/settings/tokens',
        base: DECOM_BASE,
        wait: 2500,
        act: async (p) => {
          await p.getByRole('button', { name: 'Revoke grafana' }).click();
          await p.waitForTimeout(400);
        },
      },
      {
        name: 'revoke-confirm-mobile',
        path: '/settings/tokens',
        base: DECOM_BASE,
        wait: 2500,
        viewport: mobile,
        act: async (p) => {
          await p.getByRole('button', { name: 'Revoke grafana' }).click();
          await p.waitForTimeout(400);
        },
      },
      {
        name: 'last-admin-error',
        path: '/settings/tokens',
        base: DECOM_BASE,
        wait: 2500,
        act: async (p) => {
          await p.getByRole('button', { name: 'Revoke ops-admin' }).click();
          await p.waitForTimeout(300);
          await p.getByRole('button', { name: 'Revoke token' }).click();
          await p.waitForTimeout(800);
        },
      },
      {
        name: 'last-admin-error-light',
        path: '/settings/tokens',
        base: DECOM_BASE,
        wait: 2500,
        theme: 'light',
        act: async (p) => {
          await p.getByRole('button', { name: 'Revoke ops-admin' }).click();
          await p.waitForTimeout(300);
          await p.getByRole('button', { name: 'Revoke token' }).click();
          await p.waitForTimeout(800);
        },
      },
      { name: 'nodes-before', path: '/nodes', base: DECOM_BASE, wait: 3500, fullPage: true },
      {
        name: 'forget-confirm',
        path: '/nodes',
        base: DECOM_BASE,
        wait: 3500,
        act: async (p) => {
          await p.getByRole('button', { name: 'Forget node' }).click();
          await p.waitForTimeout(400);
        },
      },
      {
        // One context end to end: the removed card lives in this tab's tracking (a fresh browser
        // can't learn about a finished decommission — no list endpoint).
        name: 'decommission-removed',
        path: '/nodes',
        base: DECOM_BASE,
        wait: 3500,
        viewport: { width: 1440, height: 1000 },
        act: async (p) => {
          const card = p.locator('article[aria-label="Node n4"]');
          await card.getByRole('button', { name: 'Decommission' }).click();
          await p.waitForTimeout(500);
          await snap(p, 'decommission-confirm');
          await p.getByRole('button', { name: 'Decommission n4' }).click();
          await p.waitForTimeout(7000);
          await snap(p, 'decommission-in-progress', '.node-grid');
          await snap(p, 'decommission-in-progress-rebalancing', '#rebalancing');
          await p.locator('article[aria-label="Node n4"]').getByRole('button', { name: 'Cancel decommission' }).click();
          await p.waitForTimeout(400);
          await snap(p, 'decommission-cancel-confirm');
          await p.getByRole('button', { name: 'Keep going' }).click();
          await p.waitForTimeout(300);
          // A second decommission while the plan runs: predicted and refused with 409.
          await p.locator('article[aria-label="Node n3"]').getByRole('button', { name: 'Decommission' }).click();
          await p.waitForTimeout(400);
          await snap(p, 'decommission-conflict-predicted');
          await p.getByRole('button', { name: 'Decommission n3' }).click();
          await p.waitForTimeout(800);
          await snap(p, 'decommission-conflict-409');
          await p.getByRole('dialog').getByRole('button', { name: 'Cancel' }).click();
          await p.waitForTimeout(300);
          await p.evaluate(() => document.documentElement.setAttribute('data-theme', 'light'));
          await p.waitForTimeout(300);
          await snap(p, 'decommission-in-progress-light', 'article[aria-label="Node n4"]');
          await p.evaluate(() => document.documentElement.setAttribute('data-theme', 'dark'));
          await p.setViewportSize(mobile);
          await p.waitForTimeout(500);
          await snap(p, 'decommission-in-progress-mobile', 'article[aria-label="Node n4"]');
          await p.setViewportSize({ width: 1440, height: 1000 });
          await p.getByText('safe to stop the process').first().waitFor({ timeout: 90_000 });
          await p.waitForTimeout(2500);
          await snap(p, 'decommission-removed-card', 'article[aria-label="Node n4 (removed)"]');
          await p.evaluate(() => window.scrollTo(0, 0));
        },
      },
      // A fresh browser sees the removed card too (GET /api/cluster/decommissions).
      { name: 'decommission-removed-fresh', path: '/nodes', base: DECOM_BASE, wait: 4000, selector: '.node-grid' },
      {
        name: 'forget-done',
        path: '/nodes',
        base: DECOM_BASE,
        wait: 3500,
        fullPage: true,
        act: async (p) => {
          await p.getByRole('button', { name: 'Forget node' }).click();
          await p.waitForTimeout(300);
          await p.getByRole('button', { name: 'Forget n5' }).click();
          await p.waitForTimeout(2500);
        },
      },
    );
  }
  if (INERT_BASE) {
    shots.push(
      {
        name: 'decommission-confirm-fallback',
        path: '/nodes',
        base: INERT_BASE,
        wait: 3500,
        act: async (p) => {
          await p.locator('article[aria-label="Node n2"]').getByRole('button', { name: 'Decommission' }).click();
          await p.waitForTimeout(800);
        },
      },
      { name: 'cert-bindings-inert', path: '/settings/tokens', base: INERT_BASE, wait: 2500, selector: bindings },
      { name: 'cert-bindings-inert-light', path: '/settings/tokens', base: INERT_BASE, wait: 2500, selector: bindings, theme: 'light' },
      { name: 'cert-bindings-inert-mobile', path: '/settings/tokens', base: INERT_BASE, wait: 2500, selector: bindings, viewport: mobile },
    );
  }
  if (CANCEL_BASE) {
    shots.push({
      name: 'decommission-cancelled',
      path: '/nodes',
      base: CANCEL_BASE,
      wait: 3500,
      selector: 'article[aria-label="Node n4"]',
      act: async (p) => {
        const card = p.locator('article[aria-label="Node n4"]');
        await card.getByRole('button', { name: 'Decommission' }).click();
        await p.waitForTimeout(1200);
        await snap(p, 'decommission-confirm-dry-run');
        await p.getByRole('button', { name: 'Decommission n4' }).click();
        await p.waitForTimeout(5000);
        await card.getByRole('button', { name: 'Cancel decommission' }).click();
        await p.waitForTimeout(300);
        await p.getByRole('dialog').getByRole('button', { name: 'Cancel decommission' }).click();
        await card.getByText('Decommission cancelled').waitFor({ timeout: 60_000 });
        await p.waitForTimeout(1000);
      },
    });
  }
  if (REVOKE_BASE) {
    // Each shot presents its own token value (the mock revokes each value once).
    shots.push(
      { name: 'session-revoked', path: '/', base: REVOKE_BASE, wait: 6000, token: 'fmq_k7q2m9_rev01' },
      { name: 'session-revoked-light', path: '/', base: REVOKE_BASE, wait: 6000, token: 'fmq_k7q2m9_rev02', theme: 'light' },
      { name: 'session-revoked-mobile', path: '/', base: REVOKE_BASE, wait: 6000, token: 'fmq_k7q2m9_rev03', viewport: mobile },
      {
        name: 'session-revoked-banner',
        path: '/',
        base: REVOKE_BASE,
        wait: 6000,
        token: 'fmq_k7q2m9_rev04',
        act: async (p) => {
          await p.getByRole('button', { name: 'View frozen data' }).click();
          await p.waitForTimeout(400);
        },
      },
      {
        name: 'session-revoked-rejected',
        path: '/',
        base: REVOKE_BASE,
        wait: 6000,
        token: 'fmq_k7q2m9_rev05',
        act: async (p) => {
          // A token revoked earlier in this run is refused as well.
          await p.getByLabel('New access token').fill('fmq_k7q2m9_rev01');
          await p.getByRole('button', { name: 'Sign in', exact: true }).click();
          await p.waitForTimeout(800);
        },
      },
    );
  }
}

const browser = await chromium.launch();
for (const s of shots) {
  if (ONLY && !s.name.includes(ONLY)) continue;
  const ctx = await browser.newContext({ viewport: s.viewport ?? desktop, deviceScaleFactor: s.viewport && s.viewport.width < 800 ? 2 : 1, colorScheme: 'dark' });
  const token = s.token ?? TOKEN;
  if (token) await ctx.addInitScript(`try{localStorage.setItem('fluxmq.adminToken',${JSON.stringify(token)})}catch(e){}`);
  if (s.theme) await ctx.addInitScript(`try{localStorage.setItem('fluxmq.theme','${s.theme}')}catch(e){}`);
  const page = await ctx.newPage();
  const errors: string[] = [];
  page.on('pageerror', (e) => errors.push(e.message));
  page.on('console', (m) => {
    if (m.type() === 'error') errors.push(m.text());
  });
  await page.goto((s.base ?? BASE) + s.path, { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(s.wait ?? 2000);
  try {
    if (s.act) await s.act(page);
  } catch (e) {
    errors.push(`act failed: ${(e as Error).message.split('\n')[0]}`);
  }
  if (s.selector) {
    // A full-page capture clipped to the element: no sticky top bar painted over it.
    const box = await page.locator(s.selector).first().evaluate((el) => {
      const r = el.getBoundingClientRect();
      return { x: r.left + window.scrollX, y: r.top + window.scrollY, width: r.width, height: r.height };
    });
    const pad = 12;
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(150);
    await page.screenshot({ path: `${OUT}${s.name}.png`, fullPage: true, clip: { x: Math.max(0, box.x - pad), y: Math.max(0, box.y - pad), width: box.width + pad * 2, height: box.height + pad * 2 } });
  }
  else await page.screenshot({ path: `${OUT}${s.name}.png`, fullPage: s.fullPage ?? false });
  console.log(`${s.name}.png${errors.length ? `  ⚠ ${errors.slice(0, 3).join(' | ')}` : ''}`);
  await ctx.close();
}
await browser.close();
