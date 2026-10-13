/**
 * Turns the raw Markdown files of one language into a sorted, parsed `DocSet`.
 * Content lives in `content/<lang>/<slug>.md` with a small front matter:
 *
 *   ---
 *   title: Каналы (топики)
 *   group: console          # start | console | howto | reference
 *   order: 2
 *   summary: One line shown under the title and in search results.
 *   console: /channels      # optional: the screen this article describes
 *   ---
 */
import { blocksText, parseBlocks, parseFrontMatter, stripInline, type Block } from './markdown';
import { DOC_GROUPS, type DocArticle, type DocGroup, type DocHeading, type DocLang, type DocSet } from './types';

/** `import.meta.glob` result → `{ slug: raw }`. */
export function rawBySlug(modules: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [path, raw] of Object.entries(modules)) {
    const m = /([\w-]+)\.md$/.exec(path);
    if (m) out[m[1]!] = raw;
  }
  return out;
}

function collectHeadings(blocks: Block[], out: DocHeading[] = []): DocHeading[] {
  for (const b of blocks) {
    if (b.t === 'h') out.push({ level: b.level, id: b.id, text: stripInline(b.text) });
    else if (b.t === 'callout') collectHeadings(b.blocks, out);
  }
  return out;
}

export function parseArticle(slug: string, raw: string): DocArticle {
  const { meta, body } = parseFrontMatter(raw);
  const group = (DOC_GROUPS as readonly string[]).includes(meta.group ?? '') ? (meta.group as DocGroup) : 'reference';
  const blocks = parseBlocks(body);
  return {
    slug,
    title: meta.title || slug,
    group,
    order: Number(meta.order ?? 999) || 999,
    summary: meta.summary ?? '',
    consolePath: meta.console?.startsWith('/') ? meta.console : null,
    blocks,
    headings: collectHeadings(blocks),
    text: blocksText(blocks),
  };
}

/**
 * Articles shown first in their group whatever their `order` says — hubs readers should find
 * before the individual pages (the recipe collection opens the how-to group).
 */
export const PINNED_FIRST: readonly string[] = ['recipes'];

function rank(a: DocArticle): number {
  return PINNED_FIRST.includes(a.slug) ? -1 : a.order;
}

export function buildDocSet(lang: DocLang, raw: Record<string, string>): DocSet {
  const articles = Object.entries(raw)
    .map(([slug, src]) => parseArticle(slug, src))
    .sort((a, b) => DOC_GROUPS.indexOf(a.group) - DOC_GROUPS.indexOf(b.group) || rank(a) - rank(b) || a.slug.localeCompare(b.slug));
  return { lang, articles, bySlug: Object.fromEntries(articles.map((a) => [a.slug, a])) };
}

const cache = new Map<DocLang, Promise<DocSet>>();

/** Loads one language's docs (its own lazy chunk), parsed once per session. */
export function loadDocs(lang: DocLang): Promise<DocSet> {
  let p = cache.get(lang);
  if (!p) {
    p = (lang === 'ru' ? import('./content/ru') : import('./content/en')).then((m) => buildDocSet(lang, m.default));
    p.catch(() => cache.delete(lang));
    cache.set(lang, p);
  }
  return p;
}
