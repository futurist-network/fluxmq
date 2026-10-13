import { useCallback, useEffect, useId, useLayoutEffect, useMemo, useRef, useState, type RefObject } from 'react';
import { Link, useLocation, useNavigate, useParams } from 'react-router';
import {
  ArrowLeft,
  ArrowRight,
  BookMarked,
  BookOpen,
  ChevronRight,
  Clock,
  ExternalLink,
  FileQuestion,
  Gauge,
  Inbox,
  PanelLeft,
  Plug,
  Rocket,
  Search,
  ShieldCheck,
  Waypoints,
  X,
} from 'lucide-react';
import { useLang } from '../../i18n';
import { loadDocs } from '../../docs/registry';
import { buildIndex, highlightParts, search, type SearchHit, type SearchIndex } from '../../docs/search';
import { DOC_STRINGS, GROUP_BLURB, GROUP_LABEL, POPULAR_TASKS, type DocStrings } from '../../docs/strings';
import { DOC_GROUPS, type DocArticle, type DocGroup, type DocLang, type DocSet } from '../../docs/types';
import { consoleReturnPath } from '../../docs/consoleReturn';
import { EmptyState, Skeleton } from '../../components/ui';
import { Blocks, Text } from './Markdown';
import './docs.css';

type LoadState = { lang: DocLang; set: DocSet | null; error: string | null };

function useDocSet(lang: DocLang): { set: DocSet | null; error: string | null; retry: () => void } {
  const [state, setState] = useState<LoadState>({ lang, set: null, error: null });
  const [nonce, setNonce] = useState(0);
  useEffect(() => {
    let live = true;
    loadDocs(lang).then(
      (set) => live && setState({ lang, set, error: null }),
      (e: unknown) => live && setState({ lang, set: null, error: e instanceof Error ? e.message : String(e) }),
    );
    return () => {
      live = false;
    };
  }, [lang, nonce]);
  const current = state.lang === lang ? state : { lang, set: null, error: null };
  return { set: current.set, error: current.error, retry: () => setNonce((n) => n + 1) };
}

/**
 * Keeps `--docs-top` on the docs shell equal to the height of the sticky chrome above it (top
 * bar + a sticky banner, if one is shown), so the docs nav, TOC and heading anchors sit right
 * under it instead of sliding beneath the banner.
 */
function useStickyTop(ref: RefObject<HTMLElement | null>) {
  useLayoutEffect(() => {
    const host = ref.current;
    const main = host?.closest('.app-main');
    if (!host || !main) return;
    const measure = () => {
      let top = 0;
      let banner = 0;
      for (const el of Array.from(main.children)) {
        if (el.classList.contains('topbar')) top = el.getBoundingClientRect().height;
        else if (el.classList.contains('banner')) banner = Math.max(banner, el.getBoundingClientRect().height);
      }
      host.style.setProperty('--docs-top', `${Math.round(top + banner)}px`);
    };
    measure();
    const ro = new ResizeObserver(measure);
    const observeAll = () => {
      ro.disconnect();
      for (const el of Array.from(main.children)) ro.observe(el);
      measure();
    };
    observeAll();
    const mo = new MutationObserver(observeAll);
    mo.observe(main, { childList: true });
    return () => {
      ro.disconnect();
      mo.disconnect();
    };
  }, [ref]);
}

// ── Collapsible nav groups, remembered per viewer ───────────────────────────────────────────

const GROUPS_KEY = 'fluxmq.docs.navGroups';
type GroupState = Partial<Record<DocGroup, boolean>>;

function readGroups(): GroupState {
  try {
    const raw = window.localStorage.getItem(GROUPS_KEY);
    const v: unknown = raw ? JSON.parse(raw) : null;
    return v && typeof v === 'object' ? (v as GroupState) : {};
  } catch {
    return {};
  }
}

function writeGroups(v: GroupState) {
  try {
    window.localStorage.setItem(GROUPS_KEY, JSON.stringify(v));
  } catch {
    /* storage unavailable — state is per session */
  }
}

/** Only the current group is open by default; the reader's own toggles are remembered. Moving to
 *  an article of another group always opens that group. */
function useGroupState(current: DocGroup) {
  const [stored, setStored] = useState<GroupState>(readGroups);
  const [seen, setSeen] = useState(current);
  if (seen !== current) {
    setSeen(current);
    if (stored[current] === false) setStored({ ...stored, [current]: true });
  }
  const isOpen = (g: DocGroup) => stored[g] ?? g === current;
  const toggle = (g: DocGroup) => {
    const next = { ...stored, [g]: !isOpen(g) };
    setStored(next);
    writeGroups(next);
  };
  return { isOpen, toggle };
}

// ── Search ──────────────────────────────────────────────────────────────────────────────────

function Highlight({ text, query }: { text: string; query: string }) {
  return (
    <>
      {highlightParts(text, query).map((p, i) =>
        p.hit ? (
          <mark key={i} className="doc-mark">
            {p.text}
          </mark>
        ) : (
          <span key={i}>{p.text}</span>
        ),
      )}
    </>
  );
}

function hitHref(h: SearchHit) {
  return `/docs/${h.slug}${h.anchor ? `#${h.anchor}` : ''}`;
}

function SearchResults({ hits, query, s, onPick, id }: { hits: SearchHit[]; query: string; s: DocStrings; onPick: () => void; id: string }) {
  if (!hits.length) return <p className="docs-noresults">{s.noResults}</p>;
  return (
    <div className="docs-results" id={id} role="region" aria-label={s.results}>
      <span className="docs-results-label">
        {s.results} · {hits.length}
      </span>
      <ul>
        {hits.map((h) => (
          <li key={`${h.slug}#${h.anchor ?? ''}`}>
            <Link to={hitHref(h)} onClick={onPick} className="docs-hit">
              <span className="docs-hit-title">
                <Highlight text={h.title} query={query} />
              </span>
              {h.section && (
                <span className="docs-hit-section">
                  <ChevronRight size={11} aria-hidden /> <Highlight text={h.section} query={query} />
                </span>
              )}
              <span className="docs-hit-snippet">
                <Highlight text={h.snippet} query={query} />
              </span>
            </Link>
          </li>
        ))}
      </ul>
    </div>
  );
}

/** A search box with results: Enter opens the best hit, ↓ moves into the list, Esc clears. */
function useSearchBox(index: SearchIndex | null, onPicked: () => void) {
  const navigate = useNavigate();
  const [query, setQuery] = useState('');
  const hits = useMemo(() => (index && query.trim() ? search(index, query) : []), [index, query]);
  const resultsId = `docs-results-${useId().replace(/:/g, '')}`;
  const pick = useCallback(() => {
    setQuery('');
    onPicked();
  }, [onPicked]);
  const onKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Escape') setQuery('');
    else if (e.key === 'Enter' && hits[0]) {
      navigate(hitHref(hits[0]));
      pick();
    } else if (e.key === 'ArrowDown' && hits.length) {
      e.preventDefault();
      document.getElementById(resultsId)?.querySelector<HTMLAnchorElement>('a')?.focus();
    }
  };
  const onResultsKey = (e: React.KeyboardEvent) => {
    if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp') return;
    const links = Array.from(document.getElementById(resultsId)?.querySelectorAll<HTMLAnchorElement>('a') ?? []);
    const i = links.indexOf(document.activeElement as HTMLAnchorElement);
    if (i < 0) return;
    e.preventDefault();
    links[e.key === 'ArrowDown' ? Math.min(i + 1, links.length - 1) : Math.max(i - 1, 0)]?.focus();
  };
  return { query, setQuery, hits, pick, onKeyDown, onResultsKey, resultsId };
}

function SearchInput({
  box,
  s,
  inputRef,
  placeholder,
  large,
}: {
  box: ReturnType<typeof useSearchBox>;
  s: DocStrings;
  inputRef?: RefObject<HTMLInputElement | null>;
  placeholder: string;
  large?: boolean;
}) {
  return (
    <div className={`input-group docs-search ${large ? 'large' : ''}`}>
      <Search size={large ? 18 : 14} aria-hidden />
      <input
        ref={inputRef}
        className="input"
        type="search"
        placeholder={placeholder}
        aria-label={s.searchLabel}
        aria-controls={box.query.trim() ? box.resultsId : undefined}
        value={box.query}
        onChange={(e) => box.setQuery(e.target.value)}
        onKeyDown={box.onKeyDown}
        autoComplete="off"
        spellCheck={false}
      />
      {box.query ? (
        <button type="button" className="docs-search-clear" onClick={() => box.setQuery('')} aria-label={s.clear}>
          <X size={13} />
        </button>
      ) : (
        <kbd className="docs-search-kbd" title={s.searchShortcut} aria-hidden>
          /
        </kbd>
      )}
    </div>
  );
}

// ── Left: documentation navigation ─────────────────────────────────────────────────────────

function NavGroups({ set, lang, active, onPick }: { set: DocSet; lang: DocLang; active: DocArticle | undefined; onPick: () => void }) {
  const { isOpen, toggle } = useGroupState(active?.group ?? 'start');
  return (
    <div className="docs-nav-groups">
      {DOC_GROUPS.map((g) => {
        const list = set.articles.filter((a) => a.group === g);
        if (!list.length) return null;
        const open = isOpen(g);
        const listId = `docs-nav-${g}`;
        return (
          <div key={g} className={`docs-nav-group ${open ? 'open' : ''} ${active?.group === g ? 'current' : ''}`}>
            <button type="button" className="docs-nav-group-btn" aria-expanded={open} aria-controls={listId} onClick={() => toggle(g)}>
              <ChevronRight size={14} className="docs-chevron" aria-hidden />
              <span>{GROUP_LABEL[lang][g]}</span>
            </button>
            {open && (
              <ul id={listId}>
                {list.map((a) => (
                  <li key={a.slug}>
                    <Link
                      to={`/docs/${a.slug}`}
                      className={a.slug === active?.slug ? 'active' : ''}
                      aria-current={a.slug === active?.slug ? 'page' : undefined}
                      onClick={onPick}
                    >
                      {a.title}
                    </Link>
                  </li>
                ))}
              </ul>
            )}
          </div>
        );
      })}
    </div>
  );
}

function DocsNav({
  set,
  lang,
  s,
  active,
  index,
  open,
  onClose,
  searchRef,
}: {
  set: DocSet | null;
  lang: DocLang;
  s: DocStrings;
  active: DocArticle | undefined;
  index: SearchIndex | null;
  open: boolean;
  onClose: () => void;
  searchRef: RefObject<HTMLInputElement | null>;
}) {
  const box = useSearchBox(index, onClose);
  const closeRef = useRef<HTMLButtonElement>(null);

  // Mobile sheet: focus moves in when it opens; Esc closes it.
  useEffect(() => {
    if (!open) return;
    closeRef.current?.focus();
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && onClose();
    window.addEventListener('keydown', onKey);
    const html = document.documentElement;
    const prev = html.style.overflow;
    html.style.overflow = 'hidden';
    return () => {
      window.removeEventListener('keydown', onKey);
      html.style.overflow = prev;
    };
  }, [open, onClose]);

  return (
    <>
      {open && <div className="scrim docs-scrim" onClick={onClose} aria-hidden />}
      <nav className={`docs-nav ${open ? 'open' : ''}`} aria-label={s.navLabel}>
        <div className="docs-nav-top">
          <Link to={consoleReturnPath()} className="docs-back">
            <ArrowLeft size={14} aria-hidden /> {s.backToConsole}
          </Link>
          <button ref={closeRef} type="button" className="btn btn-ghost btn-sm btn-icon docs-nav-close" onClick={onClose} aria-label={s.closeSections}>
            <X size={16} />
          </button>
        </div>
        <Link to="/docs" className="docs-brand" onClick={onClose}>
          <span className="docs-brand-icon">
            <BookOpen size={15} aria-hidden />
          </span>
          {s.title}
        </Link>
        <SearchInput box={box} s={s} inputRef={searchRef} placeholder={s.search} />
        <div className="docs-nav-scroll" onKeyDown={box.onResultsKey}>
          {box.query.trim() && set ? (
            <SearchResults hits={box.hits} query={box.query} s={s} onPick={box.pick} id={box.resultsId} />
          ) : (
            set && <NavGroups set={set} lang={lang} active={active} onPick={onClose} />
          )}
        </div>
      </nav>
    </>
  );
}

// ── Right: on this page ─────────────────────────────────────────────────────────────────────

/** Highlights the heading the reader is in (the last one above the top quarter of the viewport). */
function useActiveHeading(ids: string[]): string | null {
  const [active, setActive] = useState<string | null>(null);
  useEffect(() => {
    if (!ids.length) return;
    let frame = 0;
    const update = () => {
      frame = 0;
      const limit = window.innerHeight * 0.25;
      let cur: string | null = null;
      for (const id of ids) {
        const el = document.getElementById(id);
        if (!el) continue;
        if (el.getBoundingClientRect().top <= limit) cur = id;
        else break;
      }
      // At the very bottom the last sections can't reach the top quarter: mark the last one.
      const atBottom = window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 4;
      if (atBottom && window.scrollY > 0) cur = ids[ids.length - 1] ?? cur;
      setActive(cur ?? ids[0] ?? null);
    };
    const onScroll = () => {
      if (!frame) frame = requestAnimationFrame(update);
    };
    update();
    window.addEventListener('scroll', onScroll, { passive: true });
    return () => {
      window.removeEventListener('scroll', onScroll);
      if (frame) cancelAnimationFrame(frame);
    };
  }, [ids]);
  return active;
}

function Toc({ article, s }: { article: DocArticle; s: DocStrings }) {
  const items = useMemo(() => article.headings.filter((h) => h.level <= 3), [article]);
  const ids = useMemo(() => items.map((h) => h.id), [items]);
  const active = useActiveHeading(ids);
  if (items.length < 2) return <div className="docs-toc-empty" />;
  return (
    <nav className="docs-toc" aria-label={s.onThisPage}>
      <span className="docs-toc-label">{s.onThisPage}</span>
      <ul>
        {items.map((h) => (
          <li key={h.id} className={`lvl-${h.level} ${active === h.id ? 'active' : ''}`}>
            <a href={`#${h.id}`} aria-current={active === h.id ? 'location' : undefined}>
              {h.text}
            </a>
          </li>
        ))}
      </ul>
    </nav>
  );
}

// ── Center: article ─────────────────────────────────────────────────────────────────────────

function readMinutes(a: DocArticle): number {
  return Math.max(1, Math.round(a.text.split(/\s+/).length / 180));
}

function Breadcrumbs({ article, lang, s }: { article: DocArticle; lang: DocLang; s: DocStrings }) {
  return (
    <nav className="docs-crumbs" aria-label={s.breadcrumbs}>
      <ol>
        <li>
          <Link to="/docs">{s.home}</Link>
          <ChevronRight size={12} aria-hidden />
        </li>
        <li>
          <Link to={`/docs#${article.group}`}>{GROUP_LABEL[lang][article.group]}</Link>
          <ChevronRight size={12} aria-hidden />
        </li>
        <li aria-current="page">{article.title}</li>
      </ol>
    </nav>
  );
}

function ArticleView({ article, set, lang, s }: { article: DocArticle; set: DocSet; lang: DocLang; s: DocStrings }) {
  const idx = set.articles.findIndex((a) => a.slug === article.slug);
  const prev = idx > 0 ? set.articles[idx - 1] : undefined;
  const next = idx >= 0 && idx < set.articles.length - 1 ? set.articles[idx + 1] : undefined;
  return (
    <div className="docs-article-grid">
      <article className="docs-article" lang={lang}>
        <header className="docs-article-head">
          <Breadcrumbs article={article} lang={lang} s={s} />
          <h1>{article.title}</h1>
          {article.summary && (
            <p className="docs-summary">
              <Text src={article.summary} />
            </p>
          )}
          <div className="docs-meta">
            <span className="docs-readtime">
              <Clock size={13} aria-hidden /> {s.readTime.replace('{n}', String(readMinutes(article)))}
            </span>
            {article.consolePath && (
              <Link to={article.consolePath} className="btn btn-sm docs-open-console">
                <ExternalLink size={13} aria-hidden /> {s.openInConsole}
              </Link>
            )}
          </div>
        </header>
        <div className="doc-body">
          <Blocks blocks={article.blocks} ctx={{ slug: article.slug, lang, s }} />
        </div>
        <nav className="docs-pager" aria-label={s.pagerLabel}>
          {prev ? (
            <Link to={`/docs/${prev.slug}`} className="docs-pager-link prev" rel="prev">
              <span className="docs-pager-dir">
                <ArrowLeft size={13} aria-hidden /> {s.previous}
              </span>
              <span className="docs-pager-title">{prev.title}</span>
              {prev.group !== article.group && <span className="docs-pager-group">{GROUP_LABEL[lang][prev.group]}</span>}
            </Link>
          ) : (
            <span />
          )}
          {next && (
            <Link to={`/docs/${next.slug}`} className="docs-pager-link next" rel="next">
              <span className="docs-pager-dir">
                {s.next} <ArrowRight size={13} aria-hidden />
              </span>
              <span className="docs-pager-title">{next.title}</span>
              {next.group !== article.group && <span className="docs-pager-group">{GROUP_LABEL[lang][next.group]}</span>}
            </Link>
          )}
        </nav>
      </article>
      <aside className="docs-aside">
        <Toc key={article.slug} article={article} s={s} />
      </aside>
    </div>
  );
}

// ── Home page ───────────────────────────────────────────────────────────────────────────────

const TASK_ICON: Record<string, typeof Plug> = {
  'howto-create-channel': Waypoints,
  'howto-connect-client': Plug,
  recipes: BookMarked,
  quickstart: Rocket,
  'howto-handle-dlq': Inbox,
  'howto-investigate-lag': Gauge,
  'howto-secure': ShieldCheck,
};

function Landing({ set, lang, s, index }: { set: DocSet; lang: DocLang; s: DocStrings; index: SearchIndex }) {
  const noop = useCallback(() => {}, []);
  const box = useSearchBox(index, noop);
  const tasks = POPULAR_TASKS.map((alts) => alts.map((slug) => set.bySlug[slug]).find(Boolean)).filter((a): a is DocArticle => !!a);
  return (
    <div className="docs-landing" lang={lang}>
      <header className="docs-hero">
        <h1>{s.heroTitle}</h1>
        <p className="docs-hero-sub">{s.heroSubtitle}</p>
        <div className="docs-hero-search" onKeyDown={box.onResultsKey}>
          <SearchInput box={box} s={s} placeholder={s.heroSearch} large />
          {box.query.trim() && (
            <div className="docs-hero-results">
              <SearchResults hits={box.hits} query={box.query} s={s} onPick={box.pick} id={box.resultsId} />
            </div>
          )}
        </div>
      </header>

      {tasks.length > 0 && (
        <section className="docs-home-section" aria-labelledby="docs-popular">
          <h2 id="docs-popular">{s.popularTasks}</h2>
          <ul className="docs-tasks">
            {tasks.map((a) => {
              const Icon = TASK_ICON[a.slug] ?? BookOpen;
              return (
                <li key={a.slug}>
                  <Link to={`/docs/${a.slug}`} className="docs-task">
                    <span className="docs-task-icon">
                      <Icon size={17} aria-hidden />
                    </span>
                    <span className="docs-task-text">
                      <span className="docs-task-title">{s.tasks[a.slug] ?? a.title}</span>
                      <span className="docs-task-sum">
                        <Text src={a.summary} />
                      </span>
                    </span>
                    <ArrowRight size={15} className="docs-task-go" aria-hidden />
                  </Link>
                </li>
              );
            })}
          </ul>
        </section>
      )}

      <section className="docs-home-section" aria-labelledby="docs-browse">
        <h2 id="docs-browse">{s.browseAll}</h2>
        <div className="docs-sections">
          {DOC_GROUPS.map((g) => {
            const list = set.articles.filter((a) => a.group === g);
            if (!list.length) return null;
            return (
              <section key={g} id={g} className={`docs-section ${list.length > 8 ? 'wide' : ''}`} aria-labelledby={`docs-sec-${g}`}>
                <h3 id={`docs-sec-${g}`}>{GROUP_LABEL[lang][g]}</h3>
                <p>{GROUP_BLURB[lang][g]}</p>
                <ul>
                  {list.map((a) => (
                    <li key={a.slug}>
                      <Link to={`/docs/${a.slug}`}>
                        <ChevronRight size={13} aria-hidden />
                        {a.title}
                      </Link>
                    </li>
                  ))}
                </ul>
              </section>
            );
          })}
        </div>
      </section>
    </div>
  );
}

// ── Page ────────────────────────────────────────────────────────────────────────────────────

/** `/docs` and `/docs/:slug` — the built-in documentation, in the console's language. */
export default function DocsPage() {
  const lang = useLang((st) => st.lang);
  const s = DOC_STRINGS[lang];
  const { slug } = useParams();
  const { hash } = useLocation();
  const { set, error, retry } = useDocSet(lang);
  const [navOpen, setNavOpen] = useState(false);
  const searchRef = useRef<HTMLInputElement>(null);
  const shellRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  useStickyTop(shellRef);

  const index = useMemo(() => (set ? buildIndex(set.articles) : null), [set]);
  const article = slug && set ? set.bySlug[slug] : undefined;

  // Scroll to the anchor (or top) once the article is rendered. Deferred one frame so it runs
  // after the shell's own scroll-to-top on navigation.
  useEffect(() => {
    if (!set) return;
    const id = hash ? decodeURIComponent(hash.slice(1)) : '';
    const t = setTimeout(() => {
      const el = id ? document.getElementById(id) : null;
      if (el) el.scrollIntoView({ block: 'start' });
      else window.scrollTo({ top: 0 });
    }, 0);
    return () => clearTimeout(t);
  }, [set, slug, hash]);

  useEffect(() => {
    const prevTitle = document.title;
    document.title = `${article?.title ?? s.title} · FluxMQ`;
    return () => {
      document.title = prevTitle;
    };
  }, [article, s.title]);

  // "/" focuses the search box (unless the user is typing somewhere).
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== '/' || e.metaKey || e.ctrlKey || e.altKey) return;
      const el = e.target as HTMLElement | null;
      if (el && (el.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(el.tagName))) return;
      e.preventDefault();
      const hero = document.querySelector<HTMLInputElement>('.docs-hero-search input');
      if (hero) hero.focus();
      else if (window.matchMedia('(max-width: 900px)').matches) {
        setNavOpen(true);
        setTimeout(() => searchRef.current?.focus(), 0);
      } else searchRef.current?.focus();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  const closeNav = useCallback(() => {
    setNavOpen((was) => {
      if (was) setTimeout(() => triggerRef.current?.focus(), 0);
      return false;
    });
  }, []);

  return (
    <div className="docs-shell" ref={shellRef}>
      <DocsNav set={set} lang={lang} s={s} active={article} index={index} open={navOpen} onClose={closeNav} searchRef={searchRef} />

      <div className="docs-main">
        <div className="docs-mobilebar">
          <button ref={triggerRef} type="button" className="btn btn-sm docs-nav-trigger" onClick={() => setNavOpen(true)} aria-expanded={navOpen}>
            <PanelLeft size={15} aria-hidden /> {s.sections}
          </button>
          <span className="docs-mobilebar-title">{article ? GROUP_LABEL[lang][article.group] : s.title}</span>
        </div>

        {error ? (
          <EmptyState
            icon={<FileQuestion size={20} />}
            title={s.loadFailed}
            actions={
              <button type="button" className="btn" onClick={retry}>
                {s.retry}
              </button>
            }
          >
            {error}
          </EmptyState>
        ) : !set || !index ? (
          <div className="docs-article" aria-busy="true" aria-label={s.loading}>
            <Skeleton w={160} h={12} />
            <Skeleton w="60%" h={30} style={{ marginTop: 14 }} />
            <Skeleton w="80%" h={14} style={{ marginTop: 16 }} />
            <Skeleton h={220} style={{ marginTop: 28 }} />
          </div>
        ) : !slug ? (
          <Landing set={set} lang={lang} s={s} index={index} />
        ) : article ? (
          <ArticleView key={article.slug} article={article} set={set} lang={lang} s={s} />
        ) : (
          <EmptyState
            icon={<FileQuestion size={20} />}
            title={s.notFound}
            actions={
              <Link to="/docs" className="btn">
                {s.backToDocs}
              </Link>
            }
          >
            {s.notFoundText}
          </EmptyState>
        )}
      </div>
    </div>
  );
}
