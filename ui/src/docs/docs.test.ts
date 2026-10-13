import { describe, expect, it } from 'vitest';
import { blocksText, parseBlocks, parseFrontMatter, parseInline, slugify, splitRow, stripInline, type Block } from './markdown';
import { buildIndex, highlightParts, search, tokenize } from './search';
import { buildDocSet, parseArticle, PINNED_FIRST } from './registry';
import { docForPath, docsHrefForPath } from './helpMap';
import { DIAGRAMS, DOC_GROUPS, type DocSet } from './types';
import { DOC_STRINGS } from './strings';
import ruRaw from './content/ru';
import enRaw from './content/en';
// The broker's CLI definitions — the CLI reference is checked against them.
import configRs from '../../../crates/fluxmq-server/src/config.rs?raw';
import backupCliRs from '../../../crates/fluxmq-server/src/backup_cli.rs?raw';
import tlsRs from '../../../crates/fluxmq-server/src/tls.rs?raw';

const ru = buildDocSet('ru', ruRaw);
const en = buildDocSet('en', enRaw);

describe('inline markdown', () => {
  it('parses code, strong, em, links and kbd', () => {
    expect(parseInline('a `x*y` **b *c*** [d](/docs/x) <kbd>K</kbd>')).toEqual([
      { t: 'text', v: 'a ' },
      { t: 'code', v: 'x*y' },
      { t: 'text', v: ' ' },
      { t: 'strong', c: [{ t: 'text', v: 'b ' }, { t: 'em', c: [{ t: 'text', v: 'c' }] }] },
      { t: 'text', v: ' ' },
      { t: 'link', href: '/docs/x', c: [{ t: 'text', v: 'd' }] },
      { t: 'text', v: ' ' },
      { t: 'kbd', v: 'K' },
    ]);
  });

  it('keeps unclosed markers and lone asterisks literal', () => {
    expect(stripInline('2 * 3 = 6, `open')).toBe('2 * 3 = 6, `open');
    expect(stripInline('**not closed')).toBe('**not closed');
    expect(stripInline('[no link] here')).toBe('[no link] here');
  });

  it('honours escapes and double-backtick code containing a backtick', () => {
    expect(stripInline('\\*literal\\* a\\|b')).toBe('*literal* a|b');
    expect(parseInline('``a ` b``')).toEqual([{ t: 'code', v: 'a ` b' }]);
  });

  it('link text may contain code', () => {
    expect(parseInline('[`fluxmq` flags](/docs/ref-cli#x)')).toEqual([{ t: 'link', href: '/docs/ref-cli#x', c: [{ t: 'code', v: 'fluxmq' }, { t: 'text', v: ' flags' }] }]);
  });
});

describe('block markdown', () => {
  it('parses headings with explicit and generated, de-duplicated ids', () => {
    const b = parseBlocks('## Каналы (топики) {#channels}\n\n### Ёлка и `code`\n\n### Ёлка и `code`');
    expect(b).toEqual([
      { t: 'h', level: 2, text: 'Каналы (топики)', id: 'channels' },
      { t: 'h', level: 3, text: 'Ёлка и `code`', id: 'елка-и-code' },
      { t: 'h', level: 3, text: 'Ёлка и `code`', id: 'елка-и-code-2' },
    ]);
  });

  it('joins paragraph lines and stops at the next block', () => {
    expect(parseBlocks('one\ntwo\n- item\n\nthree')).toEqual([
      { t: 'p', text: 'one two' },
      { t: 'list', ordered: false, start: 1, items: [{ text: 'item', children: [] }] },
      { t: 'p', text: 'three' },
    ]);
  });

  it('parses fenced code verbatim, including blank lines and markdown-looking text', () => {
    const b = parseBlocks('```bash\n# not a heading\n\n- not a list\n```\nafter');
    expect(b[0]).toEqual({ t: 'code', lang: 'bash', code: '# not a heading\n\n- not a list' });
    expect(b[1]).toEqual({ t: 'p', text: 'after' });
  });

  it('parses nested and ordered lists with continuation lines and code inside items', () => {
    const src = ['1. First', '   continued', '   - nested a', '   - nested b', '', '2. Second', '', '   ```sh', '   echo hi', '   ```', '3. Third'].join('\n');
    const [list] = parseBlocks(src) as [Extract<Block, { t: 'list' }>];
    expect(list.ordered).toBe(true);
    expect(list.items.map((i) => i.text)).toEqual(['First continued', 'Second', 'Third']);
    expect(list.items[0]!.children[0]).toMatchObject({ t: 'list', items: [{ text: 'nested a' }, { text: 'nested b' }] });
    expect(list.items[1]!.children[0]).toEqual({ t: 'code', lang: 'sh', code: 'echo hi' });
  });

  it('starts ordered lists at their first number', () => {
    expect(parseBlocks('3. c\n4. d')[0]).toMatchObject({ t: 'list', ordered: true, start: 3 });
  });

  it('parses tables, padding short rows and keeping pipes in code', () => {
    const b = parseBlocks('| Flag | Env |\n|---|:---:|\n| `--a` | `A\\|B` |\n| `x \\| y` |');
    expect(b).toEqual([{ t: 'table', head: ['Flag', 'Env'], rows: [['`--a`', '`A|B`'], ['`x | y`', '']] }]);
    expect(splitRow('| a \\| b | c |')).toEqual(['a \\| b', 'c']);
  });

  it('parses callouts by kind, with blocks inside', () => {
    expect(parseBlocks('> [!WARNING]\n> Careful\n> - one')).toEqual([
      { t: 'callout', kind: 'warning', blocks: [{ t: 'p', text: 'Careful' }, { t: 'list', ordered: false, start: 1, items: [{ text: 'one', children: [] }] }] },
    ]);
    expect(parseBlocks('> plain')).toEqual([{ t: 'callout', kind: 'note', blocks: [{ t: 'p', text: 'plain' }] }]);
    expect(parseBlocks('> [!TIP]\n> ok')[0]).toMatchObject({ kind: 'tip' });
  });

  it('parses hr and diagrams', () => {
    expect(parseBlocks('a\n\n---\n\n:::diagram architecture')).toEqual([{ t: 'p', text: 'a' }, { t: 'hr' }, { t: 'diagram', name: 'architecture' }]);
  });

  it('extracts plain text for search', () => {
    expect(blocksText(parseBlocks('## H **b**\n\ntext `c`\n\n| a |\n|---|\n| [x](/y) |'))).toBe('H b\ntext c\na\nx');
  });

  it('slugify keeps letters of any script', () => {
    expect(slugify('Как сделать: TLS / mTLS!')).toBe('как-сделать-tls-mtls');
  });
});

describe('front matter', () => {
  it('reads key: value pairs and the body', () => {
    expect(parseFrontMatter('---\ntitle: "Hi: there"\norder: 2\n---\n## Body')).toEqual({ meta: { title: 'Hi: there', order: '2' }, body: '## Body' });
    expect(parseFrontMatter('no front matter')).toEqual({ meta: {}, body: 'no front matter' });
  });

  it('parseArticle falls back for missing fields', () => {
    const a = parseArticle('x', '## Only body');
    expect(a).toMatchObject({ slug: 'x', title: 'x', group: 'reference', order: 999, summary: '', consolePath: null });
  });
});

describe('registry ordering', () => {
  it('orders groups start → howto → console → reference, then by order', () => {
    expect([...DOC_GROUPS]).toEqual(['start', 'howto', 'console', 'reference']);
    const set = buildDocSet('en', {
      ref: '---\ngroup: reference\norder: 1\n---\nx',
      scr: '---\ngroup: console\norder: 1\n---\nx',
      h2: '---\ngroup: howto\norder: 2\n---\nx',
      h1: '---\ngroup: howto\norder: 1\n---\nx',
      st: '---\ngroup: start\norder: 5\n---\nx',
    });
    expect(set.articles.map((a) => a.slug)).toEqual(['st', 'h1', 'h2', 'scr', 'ref']);
  });

  it('pins the recipes hub first in its group whatever its order', () => {
    const set = buildDocSet('en', {
      a: '---\ngroup: howto\norder: 1\n---\nx',
      recipes: '---\ngroup: howto\norder: 50\n---\nx',
      b: '---\ngroup: howto\norder: 2\n---\nx',
    });
    expect(PINNED_FIRST).toContain('recipes');
    expect(set.articles.map((a) => a.slug)).toEqual(['recipes', 'a', 'b']);
  });
});

describe('search', () => {
  const set = buildDocSet('en', {
    a: '---\ntitle: Dead letters\ngroup: console\norder: 1\nsummary: Failed messages\n---\n## Retry all {#retry}\nRetry messages after you fix the cause.',
    b: '---\ntitle: Channels\ngroup: console\norder: 2\nsummary: Topics\n---\n## Modes\nStream, queue and keyed channels. Dead letters are kept per channel.',
    c: '---\ntitle: Каналы\ngroup: console\norder: 3\nsummary: Топики\n---\n## Режимы\nКанал хранит сообщения. Ёмкость и ёлка.',
  });
  const index = buildIndex(set.articles);

  it('requires every token and ranks title matches first', () => {
    const hits = search(index, 'dead letters');
    expect(hits.map((h) => h.slug)).toEqual(['a', 'b']);
    expect(search(index, 'dead zebra')).toEqual([]);
  });

  it('links to the matching heading and builds a snippet', () => {
    const [hit] = search(index, 'retry');
    expect(hit).toMatchObject({ slug: 'a', anchor: 'retry', section: 'Retry all' });
    expect(hit!.snippet.toLowerCase()).toContain('retry');
  });

  it('matches Russian word forms by substring and folds ё', () => {
    expect(search(index, 'каналы').map((h) => h.slug)).toEqual(['c']);
    expect(search(index, 'канал').map((h) => h.slug)).toEqual(['c']);
    expect(search(index, 'елка').map((h) => h.slug)).toEqual(['c']);
  });

  it('tokenizes and highlights', () => {
    expect(tokenize('  "Retry",  a  DLQ! ')).toEqual(['retry', 'dlq']);
    expect(tokenize('p99 7')).toEqual(['p99', '7']);
    expect(highlightParts('Retry all', 'ret')).toEqual([
      { text: 'Ret', hit: true },
      { text: 'ry all', hit: false },
    ]);
    expect(highlightParts('abc', '')).toEqual([{ text: 'abc', hit: false }]);
  });
});

describe('contextual help map', () => {
  it('maps console paths to articles', () => {
    expect(docForPath('/')).toEqual({ slug: 'dashboard' });
    expect(docForPath('/channels')).toEqual({ slug: 'channels' });
    expect(docForPath('/channels/orders')).toEqual({ slug: 'channel-detail' });
    expect(docsHrefForPath('/channels/orders/messages')).toBe('/docs/channel-detail#messages');
    expect(docsHrefForPath('/settings/tokens')).toBe('/docs/access-tokens');
    expect(docsHrefForPath('/routing/new')).toBe('/docs/routing#route-editor');
    expect(docsHrefForPath('/webhooks/abc')).toBe('/docs/webhooks#webhook-editor');
    expect(docsHrefForPath('/something-else')).toBe('/docs/overview');
  });

  it('every target article and anchor exists in both languages', () => {
    const paths = ['/', '/channels', '/channels/x', '/consumers', '/routing', '/routing/new', '/webhooks', '/webhooks/new', '/dlq', '/alerts', '/nodes', '/settings', '/settings/tokens', '/settings/notifications', '/settings/backups', '/settings/security', '/nope'];
    const tabs = ['messages', 'consumers', 'routing', 'storage', 'settings'].map((t) => `/channels/x/${t}`);
    for (const set of [ru, en]) {
      for (const p of [...paths, ...tabs]) {
        const t = docForPath(p);
        const a = set.bySlug[t.slug];
        expect(a, `${set.lang}: ${p} → ${t.slug}`).toBeDefined();
        if (t.anchor) expect(a!.headings.map((h) => h.id), `${set.lang}: ${p} → #${t.anchor}`).toContain(t.anchor);
      }
    }
  });
});

/** Every internal link `/docs/slug#anchor` in a set, with where it appears. */
function internalLinks(set: DocSet): { from: string; href: string }[] {
  const out: { from: string; href: string }[] = [];
  const visitInline = (from: string, src: string) => {
    const walk = (nodes: ReturnType<typeof parseInline>) => {
      for (const n of nodes) {
        if (n.t === 'link') {
          out.push({ from, href: n.href });
          walk(n.c);
        } else if (n.t === 'strong' || n.t === 'em') walk(n.c);
      }
    };
    walk(parseInline(src));
  };
  const visit = (from: string, blocks: Block[]) => {
    for (const b of blocks) {
      if (b.t === 'p' || b.t === 'h') visitInline(from, b.text);
      else if (b.t === 'list') {
        for (const it of b.items) {
          visitInline(from, it.text);
          visit(from, it.children);
        }
      }
      else if (b.t === 'table') for (const r of [b.head, ...b.rows]) for (const c of r) visitInline(from, c);
      else if (b.t === 'callout') visit(from, b.blocks);
    }
  };
  for (const a of set.articles) {
    visitInline(a.slug, a.summary);
    visit(a.slug, a.blocks);
  }
  return out;
}

const CONSOLE_ROUTES = [/^\/$/, /^\/channels(\/[^/#]+(\/(messages|consumers|routing|live|storage|settings))?)?$/, /^\/consumers$/, /^\/dlq$/, /^\/live$/, /^\/scheduled$/, /^\/alerts$/, /^\/nodes$/, /^\/routing(\/new)?$/, /^\/webhooks(\/new)?$/, /^\/settings(\/(tokens|notifications|backups|security))?$/, /^\/docs$/];

describe('content', () => {
  it('has articles in every group, in both languages', () => {
    expect(ru.articles.length).toBeGreaterThan(20);
    for (const set of [ru, en]) for (const g of DOC_GROUPS) expect(set.articles.some((a) => a.group === g), `${set.lang} ${g}`).toBe(true);
  });

  it('every ru article has an en counterpart and vice versa, with the same group, order and console link', () => {
    expect(Object.keys(ru.bySlug).sort()).toEqual(Object.keys(en.bySlug).sort());
    for (const a of ru.articles) {
      const b = en.bySlug[a.slug]!;
      expect([b.group, b.order, b.consolePath], a.slug).toEqual([a.group, a.order, a.consolePath]);
    }
  });

  it('explicit heading ids match across languages', () => {
    const explicit = (set: DocSet, slug: string) => {
      const raw = (set.lang === 'ru' ? ruRaw : enRaw)[slug]!;
      return [...raw.matchAll(/^#{2,4} .*\{#([\w-]+)\}\s*$/gm)].map((m) => m[1]).sort();
    };
    for (const a of ru.articles) expect(explicit(en, a.slug), a.slug).toEqual(explicit(ru, a.slug));
  });

  it('every article has front matter, a summary and a body', () => {
    for (const set of [ru, en]) {
      for (const a of set.articles) {
        expect(a.title, `${set.lang}/${a.slug}`).not.toBe(a.slug);
        expect(a.summary.length, `${set.lang}/${a.slug} summary`).toBeGreaterThan(10);
        expect(a.text.length, `${set.lang}/${a.slug} body`).toBeGreaterThan(400);
        expect(a.order, `${set.lang}/${a.slug} order`).toBeLessThan(999);
      }
    }
  });

  it('internal links point at existing articles, anchors and console screens', () => {
    for (const set of [ru, en]) {
      for (const { from, href } of internalLinks(set)) {
        if (/^(https?:|mailto:)/.test(href)) continue;
        const where = `${set.lang}/${from} → ${href}`;
        if (href.startsWith('#')) {
          expect(set.bySlug[from]!.headings.map((h) => h.id), where).toContain(href.slice(1));
          continue;
        }
        const m = /^\/docs\/([\w-]+)(?:#([\w-]+))?$/.exec(href);
        if (m) {
          const target = set.bySlug[m[1]!];
          expect(target, where).toBeDefined();
          if (m[2]) expect(target!.headings.map((h) => h.id), where).toContain(m[2]);
          continue;
        }
        expect(CONSOLE_ROUTES.some((re) => re.test(href)), where).toBe(true);
      }
    }
  });

  it('console links in front matter are real routes, diagrams are known', () => {
    for (const set of [ru, en]) {
      for (const a of set.articles) {
        if (a.consolePath) expect(CONSOLE_ROUTES.some((re) => re.test(a.consolePath!)), a.slug).toBe(true);
        const walk = (bs: Block[]): void => {
          for (const b of bs) {
            if (b.t === 'diagram') expect(DIAGRAMS as readonly string[], `${set.lang}/${a.slug}`).toContain(b.name);
            if (b.t === 'callout') walk(b.blocks);
            if (b.t === 'list') b.items.forEach((it) => walk(it.children));
          }
        };
        walk(a.blocks);
      }
    }
  });

  it('search finds core topics in both languages', () => {
    const r = buildIndex(ru.articles);
    const e = buildIndex(en.articles);
    expect(search(r, 'очередь недоставленных').length).toBeGreaterThan(0);
    expect(search(r, 'отставание')[0]?.slug).toBeDefined();
    expect(search(e, 'consumer group lag').length).toBeGreaterThan(0);
    expect(search(e, 'FLUXMQ_ADMIN_TOKEN').some((h) => h.slug === 'ref-cli')).toBe(true);
    expect(search(r, 'FLUXMQ_ADMIN_TOKEN').some((h) => h.slug === 'ref-cli')).toBe(true);
  });

  it('the CLI reference lists every flag and environment variable of the broker', () => {
    // Source of truth: clap `#[arg(long, env = "…")]` attributes followed by `pub field: …`.
    const sources = [configRs, backupCliRs, tlsRs];
    const flags = new Set<string>();
    const envs = new Set<string>();
    for (const src of sources) {
      for (const m of src.matchAll(/#\[arg\(long([^\]]*)\)\][\s\S]*?pub (\w+):/g)) {
        flags.add(`--${m[2]!.replace(/_/g, '-')}`);
        const env = /env = "(\w+)"/.exec(m[1]!);
        if (env) envs.add(env[1]!);
      }
    }
    expect(flags.size).toBeGreaterThan(40);
    for (const raw of [ruRaw['ref-cli']!, enRaw['ref-cli']!]) {
      for (const f of flags) expect(raw, f).toContain(`\`${f}`);
      for (const e of envs) expect(raw, e).toContain(e);
    }
  });

  it('chrome strings exist in both languages', () => {
    expect(Object.keys(DOC_STRINGS.ru).sort()).toEqual(Object.keys(DOC_STRINGS.en).sort());
  });
});
