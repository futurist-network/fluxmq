/** Built-in console documentation: shared types. */
import type { Block } from './markdown';

export type DocLang = 'ru' | 'en';

/**
 * Sidebar groups, in display order: learn the basics, then do a task, then look a screen up,
 * then the technical reference. Tasks come before the screen-by-screen reference on purpose.
 */
export const DOC_GROUPS = ['start', 'howto', 'console', 'reference'] as const;
export type DocGroup = (typeof DOC_GROUPS)[number];

export interface DocHeading {
  level: 2 | 3 | 4;
  id: string;
  /** Plain text (inline markup stripped). */
  text: string;
}

export interface DocArticle {
  /** File name without `.md`; identical in every language. */
  slug: string;
  title: string;
  group: DocGroup;
  /** Position inside its group. */
  order: number;
  /** One line under the title and in search results. */
  summary: string;
  /** Console screen this article describes (front matter `console:`), for an "Open" link. */
  consolePath: string | null;
  blocks: Block[];
  headings: DocHeading[];
  /** Plain text of the body, for search. */
  text: string;
}

export interface DocSet {
  lang: DocLang;
  /** Sorted by group, then order. */
  articles: DocArticle[];
  bySlug: Record<string, DocArticle>;
}

/** `:::diagram <name>` blocks the docs page can draw. */
export const DIAGRAMS = ['architecture', 'delivery'] as const;
