import { Fragment, useState, type ReactNode } from 'react';
import { Link } from 'react-router';
import { AlertTriangle, Check, Copy, Info, Lightbulb, Link2 } from 'lucide-react';
import { parseInline, stripInline, type Block, type Inline } from '../../docs/markdown';
import type { DocStrings } from '../../docs/strings';
import type { DocLang } from '../../docs/types';
import { copyText } from '../../lib/hooks';
import { toast } from '../../store/ui';
import { Diagram } from './Diagram';

interface Ctx {
  slug: string;
  lang: DocLang;
  s: DocStrings;
}

function InlineNodes({ nodes }: { nodes: Inline[] }) {
  return (
    <>
      {nodes.map((n, i) => {
        switch (n.t) {
          case 'text':
            return <Fragment key={i}>{n.v}</Fragment>;
          case 'code':
            return <code key={i}>{n.v}</code>;
          case 'kbd':
            return <kbd key={i}>{n.v}</kbd>;
          case 'strong':
            return (
              <strong key={i}>
                <InlineNodes nodes={n.c} />
              </strong>
            );
          case 'em':
            return (
              <em key={i}>
                <InlineNodes nodes={n.c} />
              </em>
            );
          case 'link':
            return <DocLink key={i} href={n.href} nodes={n.c} />;
          default:
            return null;
        }
      })}
    </>
  );
}

function DocLink({ href, nodes }: { href: string; nodes: Inline[] }) {
  const body = <InlineNodes nodes={nodes} />;
  if (/^(https?:|mailto:)/.test(href)) {
    return (
      <a href={href} target="_blank" rel="noreferrer noopener" className="doc-link external">
        {body}
      </a>
    );
  }
  if (href.startsWith('#')) {
    return (
      <a href={href} className="doc-link">
        {body}
      </a>
    );
  }
  // In-app route: another article (/docs/…) or a console screen (/channels, /dlq …).
  return (
    <Link to={href} className="doc-link">
      {body}
    </Link>
  );
}

export function Text({ src }: { src: string }) {
  return <InlineNodes nodes={parseInline(src)} />;
}

async function copy(text: string, ok: string, fail: string): Promise<boolean> {
  const done = await copyText(text);
  toast(done ? 'success' : 'error', done ? ok : fail);
  return done;
}

function Heading({ level, text, id, ctx }: { level: 2 | 3 | 4; text: string; id: string; ctx: Ctx }) {
  const Tag = `h${level}` as 'h2' | 'h3' | 'h4';
  const onCopy = () => {
    const url = `${location.origin}/docs/${ctx.slug}#${id}`;
    history.replaceState(history.state, '', `#${id}`);
    void copy(url, ctx.s.linkCopied, ctx.s.copyFailed);
  };
  return (
    <Tag id={id} className="doc-h">
      <Text src={text} />
      <button type="button" className="doc-anchor" onClick={onCopy} aria-label={ctx.s.copyLink} title={ctx.s.copyLink}>
        <Link2 size={14} />
      </button>
    </Tag>
  );
}

function CodeBlock({ lang, code, s }: { lang: string; code: string; s: DocStrings }) {
  const [done, setDone] = useState(false);
  const onCopy = async () => {
    if (await copy(code, s.codeCopied, s.copyFailed)) {
      setDone(true);
      setTimeout(() => setDone(false), 1500);
    }
  };
  return (
    <div className="doc-code">
      <div className="doc-code-head">
        <span className="doc-code-lang">{lang || 'text'}</span>
        <button type="button" className="doc-code-copy" onClick={onCopy} aria-label={s.copyCode}>
          {done ? <Check size={13} /> : <Copy size={13} />}
          <span>{done ? s.copied : s.copy}</span>
        </button>
      </div>
      <pre tabIndex={0}>
        <code>{code}</code>
      </pre>
    </div>
  );
}

const CALLOUT_ICON = { note: Info, tip: Lightbulb, warning: AlertTriangle } as const;

function BlockView({ b, ctx }: { b: Block; ctx: Ctx }): ReactNode {
  switch (b.t) {
    case 'h':
      return <Heading level={b.level} text={b.text} id={b.id} ctx={ctx} />;
    case 'p':
      return (
        <p>
          <Text src={b.text} />
        </p>
      );
    case 'code':
      return <CodeBlock lang={b.lang} code={b.code} s={ctx.s} />;
    case 'list': {
      const items = b.items.map((it, i) => {
        // `- [ ] item` / `- [x] item` — a read-only checklist entry.
        const task = /^\[( |x|X)\]\s+/.exec(it.text);
        return (
          <li key={i} className={task ? 'doc-task' : undefined}>
            {task && <input type="checkbox" checked={task[1] !== ' '} readOnly tabIndex={-1} aria-hidden />}
            <Text src={task ? it.text.slice(task[0].length) : it.text} />
            {it.children.length > 0 && <Blocks blocks={it.children} ctx={ctx} />}
          </li>
        );
      });
      return b.ordered ? <ol start={b.start === 1 ? undefined : b.start}>{items}</ol> : <ul>{items}</ul>;
    }
    case 'table':
      return (
        <div className="doc-table-wrap" tabIndex={0}>
          <table className="doc-table">
            <thead>
              <tr>
                {b.head.map((h, i) => (
                  <th key={i}>
                    <Text src={h} />
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {b.rows.map((r, i) => (
                <tr key={i}>
                  {r.map((c, j) => (
                    <td key={j} data-label={stripInline(b.head[j] ?? '')}>
                      <Text src={c} />
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      );
    case 'callout': {
      const Icon = CALLOUT_ICON[b.kind];
      const title = b.kind === 'tip' ? ctx.s.calloutTip : b.kind === 'warning' ? ctx.s.calloutWarning : ctx.s.calloutNote;
      return (
        <div className={`doc-callout ${b.kind}`} role="note" aria-label={title}>
          <div className="doc-callout-title">
            <Icon size={15} className="doc-callout-icon" aria-hidden />
            {title}
          </div>
          <div className="doc-callout-body">
            <Blocks blocks={b.blocks} ctx={ctx} />
          </div>
        </div>
      );
    }
    case 'hr':
      return <hr />;
    case 'diagram':
      return <Diagram name={b.name} lang={ctx.lang} />;
    default:
      return null;
  }
}

export function Blocks({ blocks, ctx }: { blocks: Block[]; ctx: Ctx }) {
  return (
    <>
      {blocks.map((b, i) => (
        <BlockView key={i} b={b} ctx={ctx} />
      ))}
    </>
  );
}
