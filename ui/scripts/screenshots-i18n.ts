/**
 * Visual QA for the bilingual console: key screens in English and Russian (light theme, plus a
 * dark shot per language) into screenshots/i18n/{en,ru}/.
 *
 *   PORT=7431 pnpm mock &                                   # never the live broker's :7421
 *   FLUXMQ_URL=http://localhost:7431 pnpm dev --port 5181 &
 *   BASE=http://localhost:5181 pnpm tsx scripts/screenshots-i18n.ts
 *
 * Env: BASE (default http://localhost:5173), LANGS (default "en,ru"), ONLY (substring filter),
 * OUT (default screenshots/i18n). Animations are off under automation (lib/motion.ts), so shots
 * are deterministic. Also checks each page for clipped buttons / overflowing text and prints them.
 */
import { mkdirSync } from 'node:fs';
import { chromium, type Page } from 'playwright';

const BASE = process.env.BASE ?? 'http://localhost:5173';
const LANGS = (process.env.LANGS ?? 'en,ru').split(',');
const ONLY = process.env.ONLY;
const OUT_ROOT = new URL(`../${(process.env.OUT ?? 'screenshots/i18n').replace(/\/?$/, '/')}`, import.meta.url).pathname;

interface Shot {
  name: string;
  path: string;
  wait?: number;
  theme?: 'light' | 'dark';
  fullPage?: boolean;
  viewport?: { width: number; height: number };
  act?: (p: Page) => Promise<void>;
}

const shots: Shot[] = [
  { name: 'dashboard', path: '/', wait: 5000, fullPage: true },
  { name: 'dashboard-dark', path: '/', wait: 5000, theme: 'dark' },
  { name: 'channels', path: '/channels', wait: 3000 },
  { name: 'channel-overview', path: '/channels/orders.events', wait: 3500, fullPage: true },
  {
    name: 'channel-messages',
    path: '/channels/orders.events/messages',
    wait: 2500,
    act: async (p) => {
      await p.locator('.msg-row').nth(2).click();
      await p.waitForTimeout(900);
    },
  },
  { name: 'channel-consumers', path: '/channels/email/consumers', wait: 3000, fullPage: true },
  { name: 'channel-storage', path: '/channels/orders.events/storage', wait: 2500, fullPage: true },
  { name: 'channel-settings', path: '/channels/orders.events/settings', wait: 2500, fullPage: true },
  {
    name: 'create-channel',
    path: '/channels',
    wait: 2000,
    act: async (p) => {
      await p.locator('.create-btn').first().click();
      await p.waitForTimeout(700);
    },
  },
  { name: 'consumers', path: '/consumers', wait: 3000, fullPage: true },
  { name: 'dlq', path: '/dlq', wait: 3000 },
  { name: 'alerts', path: '/alerts', wait: 2500, fullPage: true },
  { name: 'nodes', path: '/nodes', wait: 3500, fullPage: true },
  { name: 'routing', path: '/routing', wait: 2500 },
  { name: 'route-editor', path: '/routing/rt_hv01', wait: 2500, fullPage: true },
  { name: 'webhooks', path: '/webhooks', wait: 2500 },
  { name: 'webhook-editor', path: '/webhooks/wh_crm01', wait: 2500, fullPage: true },
  { name: 'settings', path: '/settings', wait: 2000, fullPage: true },
  { name: 'settings-dark', path: '/settings', wait: 2000, theme: 'dark' },
  { name: 'tokens', path: '/settings/tokens', wait: 2500, fullPage: true },
  { name: 'notifications', path: '/settings/notifications', wait: 2500, fullPage: true },
  { name: 'backups', path: '/settings/backups', wait: 2500, fullPage: true },
  { name: 'security', path: '/settings/security', wait: 2500, fullPage: true },
  {
    name: 'palette',
    path: '/',
    wait: 2500,
    act: async (p) => {
      await p.locator('.search-trigger').click();
      await p.keyboard.type('ord');
      await p.waitForTimeout(700);
    },
  },
  { name: 'not-found', path: '/nope', wait: 1500 },
  { name: 'dashboard-mobile', path: '/', wait: 5000, viewport: { width: 390, height: 844 } },
];

/** Elements whose text is cut off: buttons/badges/tabs with horizontal overflow, outside scrollers. */
async function findClipped(p: Page): Promise<string[]> {
  return p.evaluate(() => {
    const out: string[] = [];
    const sel = 'button, .btn, .badge, .tabs a, .segmented button, th, .stat-label, .nav a, .tchip, label, h1, h2, h3';
    for (const el of Array.from(document.querySelectorAll<HTMLElement>(sel))) {
      if (!el.offsetParent) continue;
      const cs = getComputedStyle(el);
      // Intentional ellipsis with a title is fine.
      if (cs.textOverflow === 'ellipsis' && el.title) continue;
      if (el.scrollWidth > el.clientWidth + 1 && cs.overflow !== 'visible') {
        const txt = (el.innerText || '').trim().replace(/\s+/g, ' ').slice(0, 60);
        if (txt) out.push(`${el.tagName.toLowerCase()}.${(el.className || '').toString().split(' ')[0]}: "${txt}"`);
      }
    }
    return [...new Set(out)].slice(0, 12);
  });
}

const browser = await chromium.launch();
for (const lang of LANGS) {
  const out = `${OUT_ROOT}${lang}/`;
  mkdirSync(out, { recursive: true });
  for (const s of shots) {
    if (ONLY && !s.name.includes(ONLY)) continue;
    const theme = s.theme ?? 'light';
    const vp = s.viewport ?? { width: 1440, height: 900 };
    const ctx = await browser.newContext({ viewport: vp, deviceScaleFactor: vp.width < 800 ? 2 : 1, colorScheme: theme, locale: lang === 'ru' ? 'ru-RU' : 'en-US', reducedMotion: 'reduce' });
    await ctx.addInitScript(`try{localStorage.setItem('fluxmq.lang','${lang}');localStorage.setItem('fluxmq.theme','${theme}')}catch(e){}`);
    const page = await ctx.newPage();
    const errors: string[] = [];
    page.on('pageerror', (e) => errors.push(e.message));
    await page.goto(BASE + s.path, { waitUntil: 'domcontentloaded' });
    await page.waitForTimeout(s.wait ?? 2000);
    try {
      if (s.act) await s.act(page);
    } catch (e) {
      errors.push(`act failed: ${(e as Error).message.split('\n')[0]}`);
    }
    const htmlLang = await page.evaluate(() => document.documentElement.lang);
    const clipped = await findClipped(page);
    await page.screenshot({ path: `${out}${s.name}.png`, fullPage: s.fullPage ?? false });
    const notes = [htmlLang !== lang ? `html lang=${htmlLang}` : '', ...errors.slice(0, 2), ...(clipped.length ? [`clipped: ${clipped.join(' · ')}`] : [])].filter(Boolean);
    console.log(`${lang}/${s.name}.png${notes.length ? `  ⚠ ${notes.join(' | ')}` : ''}`);
    await ctx.close();
  }
}
await browser.close();
