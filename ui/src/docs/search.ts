/**
 * Client-side full-text search over the docs of one language. Small corpus (a few hundred KB),
 * so a linear scan over pre-normalised strings is instant and needs no index library.
 *
 * Every query token must occur somewhere in the article (title, headings or body); tokens match
 * as substrings, which doubles as crude stemming for Russian («канал» finds «каналы», «канала»).
 */
import type { DocArticle } from './types';

export interface SearchHit {
  slug: string;
  title: string;
  /** Heading id when a heading matched — the result links straight to that section. */
  anchor: string | null;
  /** Heading text for `anchor`. */
  section: string | null;
  snippet: string;
  score: number;
}

interface Entry {
  article: DocArticle;
  title: string;
  summary: string;
  headings: { id: string; text: string; norm: string }[];
  text: string;
  /** Original body text, same length as `text` (normalisation keeps lengths). */
  raw: string;
}

export interface SearchIndex {
  entries: Entry[];
}

/** Lower-case, `ё` → `е`. Keeps the string length, so offsets map back to the original. */
export function normalize(s: string): string {
  return s.toLowerCase().replace(/ё/g, 'е');
}

export function tokenize(query: string): string[] {
  const seen = new Set<string>();
  for (const raw of normalize(query).split(/[\s,;:!?()"'«»]+/)) {
    const tok = raw.replace(/^[.\-–—/]+|[.\-–—/]+$/g, '');
    if (tok.length >= 2 || /\d/.test(tok)) seen.add(tok);
  }
  return [...seen];
}

export function buildIndex(articles: DocArticle[]): SearchIndex {
  return {
    entries: articles.map((a) => ({
      article: a,
      title: normalize(a.title),
      summary: normalize(a.summary),
      headings: a.headings.map((h) => ({ id: h.id, text: h.text, norm: normalize(h.text) })),
      text: normalize(a.text),
      raw: a.text,
    })),
  };
}

function countOf(hay: string, needle: string, cap: number): number {
  let n = 0;
  let i = hay.indexOf(needle);
  while (i >= 0 && n < cap) {
    n++;
    i = hay.indexOf(needle, i + needle.length);
  }
  return n;
}

const wordStart = (hay: string, needle: string) => {
  const i = hay.indexOf(needle);
  return i === 0 || (i > 0 && /[^\p{L}\p{N}]/u.test(hay[i - 1]!));
};

export function search(index: SearchIndex, query: string, limit = 20): SearchHit[] {
  const tokens = tokenize(query);
  if (!tokens.length) return [];
  const hits: SearchHit[] = [];
  for (const e of index.entries) {
    let score = 0;
    let ok = true;
    let bestHeading: Entry['headings'][number] | null = null;
    let bestHeadingHits = 0;
    for (const tok of tokens) {
      let s = 0;
      if (e.title.includes(tok)) s += wordStart(e.title, tok) ? 14 : 9;
      if (e.summary.includes(tok)) s += 3;
      for (const h of e.headings) if (h.norm.includes(tok)) s += 3;
      s += countOf(e.text, tok, 6);
      if (!s) {
        ok = false;
        break;
      }
      score += s;
    }
    if (!ok) continue;
    for (const h of e.headings) {
      const n = tokens.filter((t) => h.norm.includes(t)).length;
      if (n > bestHeadingHits) {
        bestHeadingHits = n;
        bestHeading = h;
      }
    }
    hits.push({
      slug: e.article.slug,
      title: e.article.title,
      anchor: bestHeading?.id ?? null,
      section: bestHeading?.text ?? null,
      snippet: snippet(e, tokens),
      score,
    });
  }
  hits.sort((a, b) => b.score - a.score || a.title.localeCompare(b.title));
  return hits.slice(0, limit);
}

/** ~160 characters of body text around the first token found, or the summary. */
function snippet(e: Entry, tokens: string[]): string {
  let at = -1;
  for (const t of tokens) {
    const i = e.text.indexOf(t);
    if (i >= 0 && (at < 0 || i < at)) at = i;
  }
  if (at < 0) return e.article.summary;
  const from = Math.max(0, at - 60);
  const to = Math.min(e.raw.length, at + 100);
  let s = e.raw.slice(from, to).replace(/\s+/g, ' ').trim();
  if (from > 0) s = '…' + s.replace(/^\S*\s/, '');
  if (to < e.raw.length) s = s.replace(/\s\S*$/, '') + '…';
  return s;
}

/** Splits `text` into plain and highlighted parts for the query tokens. */
export function highlightParts(text: string, query: string): { text: string; hit: boolean }[] {
  const tokens = tokenize(query).sort((a, b) => b.length - a.length);
  if (!tokens.length) return [{ text, hit: false }];
  const norm = normalize(text);
  const marks = new Array<boolean>(text.length).fill(false);
  for (const t of tokens) {
    let i = norm.indexOf(t);
    while (i >= 0) {
      for (let k = i; k < i + t.length; k++) marks[k] = true;
      i = norm.indexOf(t, i + t.length);
    }
  }
  const parts: { text: string; hit: boolean }[] = [];
  let cur = '';
  let curHit = marks[0] ?? false;
  for (let k = 0; k < text.length; k++) {
    if (marks[k] !== curHit) {
      if (cur) parts.push({ text: cur, hit: curHit });
      cur = '';
      curHit = marks[k]!;
    }
    cur += text[k];
  }
  if (cur) parts.push({ text: cur, hit: curHit });
  return parts;
}
