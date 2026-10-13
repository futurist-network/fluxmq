import { useEffect, useRef, useState } from 'react';
import { AlertTriangle, Check, ChevronDown, Filter, Loader2 } from 'lucide-react';
import type { FilterCheck } from '../../lib/useFilterParse';
import { errorSpan } from '../../lib/filterError';
import { useT } from '../../i18n';

/** Filter DSL tokens shown in the help line (code — not translated). */
const DSL = {
  ops: '= != > >= < <= in [..] not in prefix contains exists',
  key: 'key',
  partition: 'partition',
  header: 'header.<name>',
  path: 'items.0.sku',
  pkey: 'payload.key',
  ppartition: 'payload.partition',
  placeholder: 'order_id = 8842 AND shipping.country = "DE"',
};

/** Single-line filter DSL input with server validation state and an examples menu. */
export function FilterInput({ value, onChange, check, examples, autoFocus, id }: { value: string; onChange: (v: string) => void; check: FilterCheck; examples: string[]; autoFocus?: boolean; id?: string }) {
  const t = useT();
  const [menu, setMenu] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!menu) return;
    const close = (e: MouseEvent) => {
      if (!ref.current?.contains(e.target as Node)) setMenu(false);
    };
    document.addEventListener('mousedown', close);
    return () => document.removeEventListener('mousedown', close);
  }, [menu]);
  const err = check.status === 'error' ? check : null;
  let snippet: React.ReactNode = null;
  if (err?.index != null) {
    const [i, j] = errorSpan(value, err.index);
    snippet = (
      <code className="filter-snippet mono">
        {i > 30 ? '…' : ''}
        {value.slice(Math.max(0, i - 30), i)}
        <mark>{value.slice(i, j) || ' '}</mark>
        {value.slice(j, j + 30)}
        {value.length > j + 30 ? '…' : ''}
      </code>
    );
  }
  return (
    <div className="filter-input" ref={ref}>
      <div className={`input-group filter-group ${err ? 'invalid' : ''}`}>
        <Filter size={14} />
        <input
          id={id}
          className={`input mono ${err ? 'invalid' : ''}`}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={DSL.placeholder}
          spellCheck={false}
          autoComplete="off"
          aria-invalid={!!err}
          aria-describedby={id ? `${id}-status` : undefined}
          autoFocus={autoFocus}
        />
        <span className="filter-state" aria-hidden>
          {check.status === 'checking' ? <Loader2 size={13} className="spin faint" /> : check.status === 'ok' ? <Check size={13} className="text-success" /> : err ? <AlertTriangle size={13} className="text-critical" /> : null}
        </span>
        <button type="button" className="btn btn-ghost btn-sm filter-ex-btn" onClick={() => setMenu(!menu)} aria-expanded={menu} aria-haspopup="menu">
          {t('messages.filter.examples')} <ChevronDown size={13} />
        </button>
      </div>
      {menu && (
        <ul className="filter-menu" role="menu">
          {examples.map((x) => (
            <li key={x} role="none">
              <button
                type="button"
                role="menuitem"
                className="mono"
                onClick={() => {
                  onChange(x);
                  setMenu(false);
                }}
              >
                {x}
              </button>
            </li>
          ))}
          <li role="none" className="filter-menu-help">
            {t.rich('messages.filter.help', {
              ops: <span className="mono">{DSL.ops}</span>,
              key: <span className="mono">{DSL.key}</span>,
              partition: <span className="mono">{DSL.partition}</span>,
              header: <span className="mono">{DSL.header}</span>,
              path: <span className="mono">{DSL.path}</span>,
              pkey: <span className="mono">{DSL.pkey}</span>,
              ppartition: <span className="mono">{DSL.ppartition}</span>,
            })}
          </li>
        </ul>
      )}
      {err && (
        <div className="filter-error" id={id ? `${id}-status` : undefined} role="alert">
          <span className="text-critical">
            {err.index != null ? t('messages.filter.column', { col: err.index + 1 }) : ''}
            {err.detail}
          </span>
          {snippet}
        </div>
      )}
    </div>
  );
}
