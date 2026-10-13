// Screenshots of every console page + layout checks, against a running broker.
//
//   docker run --rm --network <net> -v "$PWD:/w" -w /w mcr.microsoft.com/playwright:v1.47.2-jammy \
//     sh -c 'npm i --no-save --silent playwright@1.47.2 && node scripts/ui-shots.mjs http://fluxmq:7421 out'
//
// For each page × viewport (phone 390×844, desktop 1440×900) × theme × language it saves a full-page
// PNG and reports: horizontal page scroll, elements sticking out of the viewport, console errors and
// failed requests. The report is written to <out>/report.json and printed as a summary.
import { chromium } from 'playwright';
import fs from 'node:fs';
import path from 'node:path';

const base = process.argv[2] ?? 'http://127.0.0.1:7421';
const out = process.argv[3] ?? 'ui-shots';
const only = process.env.PAGES?.split(',');
fs.mkdirSync(out, { recursive: true });

// TOKEN=<admin token> for a broker with authentication on.
const token = process.env.TOKEN;
const auth = token ? { headers: { Authorization: `Bearer ${token}` } } : {};
const channels = await fetch(`${base}/api/channels`, auth).then((r) => r.json()).catch(() => []);
const list = Array.isArray(channels) ? channels : channels.channels ?? [];
const ch = encodeURIComponent((list.find((c) => !c.name.startsWith('$') && c.name.includes('orders')) ?? list.find((c) => !c.name.startsWith('$')))?.name ?? 'orders');

const hooks = await fetch(`${base}/api/webhooks`, auth).then((r) => r.json()).catch(() => []);
const webhook = Array.isArray(hooks) ? hooks[0]?.id : undefined;

const pages = [
  ['dashboard', '/'],
  ['channels', '/channels'],
  ['channel-overview', `/channels/${ch}`],
  ['channel-messages', `/channels/${ch}/messages`],
  ['channel-consumers', `/channels/${ch}/consumers`],
  ['channel-live', `/channels/${ch}/live`],
  ['channel-storage', `/channels/${ch}/storage`],
  ['channel-settings', `/channels/${ch}/settings`],
  ['create-channel', '/channels', '.create-btn'], // a wizard dialog opened from the top bar
  ['consumers', '/consumers'],
  ['dlq', '/dlq'],
  ['alerts', '/alerts'],
  ['nodes', '/nodes'],
  ['routing', '/routing'],
  ['webhooks', '/webhooks'],
  ['live', '/live'],
  ['scheduled', '/scheduled'],
  ['webhook-editor', webhook ? `/webhooks/${encodeURIComponent(webhook)}` : '/webhooks/new'],
  ['route-editor', '/routing/new'],
  ['settings', '/settings'],
  ['tokens', '/settings/tokens'],
  ['notifications', '/settings/notifications'],
  ['backups', '/settings/backups'],
  ['security', '/settings/security'],
  ['docs', '/docs'],
].filter(([n]) => !only || only.includes(n));

const allViewports = { phone: { width: 390, height: 844 }, desktop: { width: 1440, height: 900 } };
const viewports = Object.fromEntries(
  Object.entries(allViewports).filter(([n]) => !process.env.VIEWPORTS || process.env.VIEWPORTS.split(',').includes(n)),
);
const variants = (process.env.VARIANTS ?? 'dark-en,light-ru').split(',');

const browser = await chromium.launch();
const report = [];
for (const variant of variants) {
  const [theme, lang] = variant.split('-');
  for (const [vpName, viewport] of Object.entries(viewports)) {
    const ctx = await browser.newContext({ viewport, deviceScaleFactor: 1, isMobile: vpName === 'phone', hasTouch: vpName === 'phone' });
    await ctx.addInitScript(([t, l, tk]) => {
      localStorage.setItem('fluxmq.theme', t);
      localStorage.setItem('fluxmq.lang', l);
      if (tk) localStorage.setItem('fluxmq.adminToken', tk);
    }, [theme, lang, token]);
    for (const [name, url, click] of pages) {
      const page = await ctx.newPage();
      const errors = [];
      page.on('console', (m) => m.type() === 'error' && errors.push(m.text().slice(0, 200)));
      page.on('pageerror', (e) => errors.push(`pageerror: ${String(e).slice(0, 200)}`));
      page.on('requestfailed', (r) => {
        const u = r.url();
        const why = r.failure()?.errorText ?? '';
        // ERR_ABORTED = a fetch cancelled by its AbortController (re-render / navigation), not a failure.
        if (!u.includes('/ws') && !why.includes('ERR_ABORTED')) errors.push(`requestfailed: ${u} ${why}`);
      });
      // `load`, not `networkidle`: some pages long-poll (notifications feed).
      await page.goto(base + url, { waitUntil: 'load', timeout: 20000 }).catch((e) => errors.push(`goto: ${e.message.slice(0, 120)}`));
      await page.waitForTimeout(2500); // data fetches + live data over the WebSocket
      if (click) {
        await page.click(click, { timeout: 5000 }).catch((e) => errors.push(`click ${click}: ${e.message.slice(0, 100)}`));
        await page.waitForTimeout(800);
      }
      const layout = await page.evaluate(() => {
        const vw = document.documentElement.clientWidth;
        const scrollW = document.documentElement.scrollWidth;
        const offenders = [];
        for (const el of document.querySelectorAll('body *')) {
          const r = el.getBoundingClientRect();
          if (r.width === 0 || r.height === 0) continue;
          if (r.right <= 0 || r.left >= vw) continue; // fully off-screen on purpose (closed drawer)
          if (r.right > vw + 1 || r.left < -1) {
            // Skip children of horizontally scrollable containers (tables meant to scroll).
            let p = el.parentElement, scrollable = false;
            while (p && p !== document.body) {
              const s = getComputedStyle(p);
              if (/(auto|scroll|hidden)/.test(s.overflowX) && p.scrollWidth > p.clientWidth + 1) { scrollable = true; break; }
              p = p.parentElement;
            }
            if (scrollable) continue;
            const id = el.id ? `#${el.id}` : '';
            const cls = typeof el.className === 'string' && el.className ? `.${el.className.trim().split(/\s+/).slice(0, 2).join('.')}` : '';
            offenders.push(`${el.tagName.toLowerCase()}${id}${cls} [${Math.round(r.left)}..${Math.round(r.right)}] "${(el.textContent ?? '').trim().slice(0, 40)}"`);
          }
        }
        // Keep only the outermost offenders.
        return { vw, scrollW, offenders: offenders.slice(0, 8) };
      });
      const file = `${name}-${vpName}-${variant}.png`;
      // FULLPAGE=0: viewport-sized shots (README images; the sticky sidebar ends at the fold in full-page shots).
      await page.screenshot({ path: path.join(out, file), fullPage: process.env.FULLPAGE !== '0' });
      report.push({ page: name, viewport: vpName, variant, file, hscroll: layout.scrollW > layout.vw + 1, scrollW: layout.scrollW, vw: layout.vw, offenders: layout.offenders, errors });
      await page.close();
    }
    await ctx.close();
  }
}
await browser.close();
fs.writeFileSync(path.join(out, 'report.json'), JSON.stringify(report, null, 2));
const bad = report.filter((r) => r.hscroll || r.offenders.length || r.errors.length);
console.log(`${report.length} screenshots, ${bad.length} with findings`);
for (const r of bad) {
  console.log(`- ${r.page} ${r.viewport} ${r.variant}: ${r.hscroll ? `HSCROLL ${r.scrollW}>${r.vw} ` : ''}${r.offenders.length ? `offenders=${r.offenders.length} ` : ''}${r.errors.length ? `errors=${r.errors.length}` : ''}`);
  for (const o of r.offenders.slice(0, 3)) console.log(`    ${o}`);
  for (const e of r.errors.slice(0, 2)) console.log(`    ! ${e}`);
}
