/**
 * Filter builder shared by the route editor (WHEN) and the webhook editor: condition rows with
 * field autocomplete learned from the channel's recent messages ⇄ DSL text. The console has no
 * DSL code of its own: text is parsed and conditions rendered by the server.
 */
import { useId, useState } from 'react';
import { AlertTriangle, Check, Code2, ListFilter, Loader2, Plus, X } from 'lucide-react';
import { errorSpan } from '../../lib/filterError';
import { fieldFromText, OP_LABEL, opIsList, opNeedsValue, opsFor, validateRow, type FieldSuggestion, type FieldType, type Row } from '../../lib/routeRows';
import type { FilterBuilderState, ParseState } from './useFilterBuilder';
import { useT } from '../../i18n';
import './filter-builder.css';


/** DSL identifiers shown verbatim (never translated). */
const DSL_PLACEHOLDER = 'type = "order.created" AND shipping.country = "MD" AND total >= 100';
const DSL_KEY = 'key';
const DSL_PARTITION = 'partition';
const DSL_PAYLOAD_KEY = 'payload.key';

const TYPE_TAG: Record<FieldType, string> = { string: 'abc', number: '123', boolean: 'bool', array: '[ ]', object: '{ }', null: 'null', unknown: 'any' };

// ───────────────────────────── field picker ─────────────────────────────

function FieldPicker({ value, onChange, suggestions, disabled, invalid, autoFocus }: { value: string; onChange: (v: string) => void; suggestions: FieldSuggestion[]; disabled?: boolean; invalid?: boolean; autoFocus?: boolean }) {
  const t = useT();
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const listId = useId();
  const needle = value.trim().toLowerCase();
  const items = (needle ? suggestions.filter((s) => s.field.toLowerCase().includes(needle) && s.field.toLowerCase() !== needle) : suggestions).slice(0, 40);
  const pick = (s: FieldSuggestion) => {
    onChange(s.field);
    setOpen(false);
  };
  return (
    <div className="field-picker">
      <input
        className={`input mono ${invalid ? 'invalid' : ''}`}
        value={value}
        disabled={disabled}
        placeholder={t('routing.fb.fieldPlaceholder')}
        aria-label={t('routing.fb.field')}
        role="combobox"
        aria-expanded={open && items.length > 0}
        aria-controls={listId}
        aria-autocomplete="list"
        autoFocus={autoFocus}
        onFocus={() => setOpen(true)}
        onBlur={() => setOpen(false)}
        onChange={(e) => {
          onChange(e.target.value);
          setOpen(true);
          setActive(0);
        }}
        onKeyDown={(e) => {
          if (!open || !items.length) return;
          if (e.key === 'ArrowDown') {
            e.preventDefault();
            setActive((a) => Math.min(items.length - 1, a + 1));
          } else if (e.key === 'ArrowUp') {
            e.preventDefault();
            setActive((a) => Math.max(0, a - 1));
          } else if (e.key === 'Enter') {
            e.preventDefault();
            const s = items[active];
            if (s) pick(s);
          } else if (e.key === 'Escape') {
            e.stopPropagation();
            setOpen(false);
          }
        }}
      />
      {open && items.length > 0 && (
        <ul className="fp-list" id={listId} role="listbox">
          {items.map((s, i) => (
            <li
              key={s.field}
              role="option"
              aria-selected={i === active}
              className={i === active ? 'active' : ''}
              onMouseDown={(e) => {
                e.preventDefault();
                pick(s);
              }}
              onMouseEnter={() => setActive(i)}
            >
              <span className="mono fp-field">{s.field}</span>
              <span className="fp-type">{TYPE_TAG[s.type]}</span>
              <span className="fp-ex faint mono">{s.examples.slice(0, 3).join(' · ')}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

// ───────────────────────────── condition row ─────────────────────────────

function ConditionRow({ row, onChange, onRemove, suggestions, readOnly, showErrors, autoFocus }: { row: Row; onChange: (r: Row) => void; onRemove: () => void; suggestions: FieldSuggestion[]; readOnly: boolean; showErrors: boolean; autoFocus?: boolean }) {
  const t = useT();
  const field = fieldFromText(row.field);
  const sug = suggestions.find((s) => s.field === row.field.trim());
  const type: FieldType = sug?.type ?? (field?.kind === 'partition' ? 'number' : field?.kind === 'key' || field?.kind === 'header' ? 'string' : 'unknown');
  const ops = opsFor(field, type);
  const opList = ops.includes(row.op) ? ops : [row.op, ...ops];
  const err = validateRow(row);
  const listId = useId();
  const showErr = showErrors || (row.field !== '' && row.value !== '');
  return (
    <div className="cond-row">
      <div className="cond-cells">
        <FieldPicker
          value={row.field}
          onChange={(f) => {
            const nf = fieldFromText(f);
            const ns = suggestions.find((s) => s.field === f.trim());
            const nops = opsFor(nf, ns?.type ?? 'unknown');
            onChange({ ...row, field: f, op: nops.includes(row.op) ? row.op : nops[0]! });
          }}
          suggestions={suggestions}
          disabled={readOnly}
          invalid={showErr && !!err?.field}
          autoFocus={autoFocus}
        />
        <select className="select cond-op" value={row.op} disabled={readOnly} aria-label={t('routing.fb.operator')} onChange={(e) => onChange({ ...row, op: e.target.value as Row['op'] })}>
          {opList.map((o) => (
            <option key={o} value={o}>
              {OP_LABEL[o]}
            </option>
          ))}
        </select>
        {opNeedsValue(row.op) ? (
          <>
            <input
              className={`input mono cond-value ${showErr && err?.value ? 'invalid' : ''}`}
              value={row.value}
              disabled={readOnly}
              aria-label={t('routing.fb.value')}
              list={sug?.examples.length ? listId : undefined}
              placeholder={opIsList(row.op) ? 'MD, RO, "a, b"' : type === 'number' ? '100' : type === 'boolean' ? 'true' : t('routing.fb.valuePlaceholder')}
              onChange={(e) => onChange({ ...row, value: e.target.value })}
            />
            {sug && sug.examples.length > 0 && (
              <datalist id={listId}>
                {sug.examples.map((x) => (
                  <option key={x} value={x} />
                ))}
              </datalist>
            )}
          </>
        ) : (
          <span className="cond-novalue faint">{t('routing.fb.noValue')}</span>
        )}
        {!readOnly && (
          <button type="button" className="btn btn-ghost btn-icon btn-sm" onClick={onRemove} aria-label={t('routing.fb.removeCondition')}>
            <X size={15} />
          </button>
        )}
      </div>
      {showErr && err && <div className="field-error cond-err">{err.field ?? err.value}</div>}
      {!err && type === 'string' && ['gt', 'gte', 'lt', 'lte'].includes(row.op) && <div className="field-hint cond-err">{t('routing.fb.lexical')}</div>}
    </div>
  );
}

// ───────────────────────────── DSL text editor ─────────────────────────────


function DslEditor({ text, onChange, parse, readOnly }: { text: string; onChange: (t: string) => void; parse: ParseState | null; readOnly: boolean }) {
  const t = useT();
  const current = parse?.text === text ? parse : null;
  const checking = !current && text.trim() !== '';
  const errIdx = current?.status === 'error' ? current.index : null;
  let backdrop: React.ReactNode = text;
  if (errIdx != null) {
    const [i, j] = errorSpan(text, errIdx);
    backdrop = (
      <>
        {text.slice(0, i)}
        <mark>{text.slice(i, j) || ' '}</mark>
        {text.slice(j)}
      </>
    );
  }
  return (
    <div className="dsl">
      <div className={`dsl-wrap ${current?.status === 'error' ? 'invalid' : ''}`}>
        <pre className="dsl-backdrop" aria-hidden>
          {backdrop}
          {'\n'}
        </pre>
        <textarea
          className="dsl-input"
          value={text}
          readOnly={readOnly}
          spellCheck={false}
          aria-label={t('routing.fb.expression')}
          aria-invalid={current?.status === 'error'}
          placeholder={DSL_PLACEHOLDER}
          onChange={(e) => onChange(e.target.value)}
        />
      </div>
      <div className="dsl-status" aria-live="polite">
        {text.trim() === '' ? (
          <span className="faint">
            {t.rich('routing.fb.emptyHelp', {
              key: <span className="mono">{DSL_KEY}</span>,
              partition: <span className="mono">{DSL_PARTITION}</span>,
              payloadKey: <span className="mono">{DSL_PAYLOAD_KEY}</span>,
            })}
          </span>
        ) : checking ? (
          <span className="faint">
            <Loader2 size={12} className="spin" /> {t('routing.fb.checking')}
          </span>
        ) : current?.status === 'ok' ? (
          <span className="dsl-ok">
            <Check size={13} /> {t('routing.fb.valid', { count: current.conditions.length })}
            {current.rendered !== text.trim() && (
              <>
                {' '}
                · {t('routing.fb.canonical')} <span className="mono">{current.rendered}</span>
                {!readOnly && (
                  <button type="button" className="btn-link" onClick={() => onChange(current.rendered)}>
                    {t('routing.fb.useIt')}
                  </button>
                )}
              </>
            )}
          </span>
        ) : current?.status === 'error' ? (
          <span className="text-critical">
            <AlertTriangle size={13} /> {current.index != null ? t('routing.fb.column', { n: current.index + 1 }) : ''}
            {current.detail}
          </span>
        ) : current?.status === 'forbidden' ? (
          <span className="faint">{t('routing.fb.forbidden')}</span>
        ) : null}
      </div>
    </div>
  );
}

// ───────────────────────────── view ─────────────────────────────

export function FilterBuilder({ fb, readOnly, showErrors, emptyText, allLabel }: { fb: FilterBuilderState; readOnly: boolean; showErrors: boolean; emptyText: string; allLabel?: string }) {
  const t = useT();
  const { mode, rows, setRows, builderRender, switching } = fb;
  const textParse = fb.text.trim() === '' ? { status: 'ok' as const } : fb.parse?.text === fb.text ? fb.parse : null;
  const canSwitchToText = fb.mode === 'builder' && fb.conditions != null && builderRender.status !== 'error' && builderRender.status !== 'unknown';
  const textSwitchHint = fb.conditions == null ? t('routing.fb.completeFirst') : builderRender.status === 'error' ? builderRender.message : builderRender.status === 'unknown' ? t('routing.fb.cantConvert') : undefined;
  const canSwitchToBuilder = textParse?.status === 'ok';
  return (
    <>
      <div className="when-bar">
        <span className="faint">{allLabel ?? t('routing.fb.allMustMatch')}</span>
        <span className="spacer" />
        <div className="segmented" role="group" aria-label={t('routing.fb.editorMode')}>
          <button type="button" aria-pressed={mode === 'builder'} onClick={() => void fb.switchMode('builder')} disabled={mode === 'text' && !canSwitchToBuilder} title={mode === 'text' && !canSwitchToBuilder ? t('routing.fb.fixTextFirst') : undefined}>
            <ListFilter size={13} /> {t('routing.fb.builder')}
          </button>
          <button type="button" aria-pressed={mode === 'text'} onClick={() => void fb.switchMode('text')} disabled={mode === 'builder' && (!canSwitchToText || switching)} title={mode === 'builder' && !canSwitchToText ? textSwitchHint : undefined}>
            {switching ? <Loader2 size={13} className="spin" /> : <Code2 size={13} />} {t('routing.fb.text')}
          </button>
        </div>
      </div>
      {mode === 'builder' ? (
        <div className="conds">
          {rows.length === 0 && <div className="conds-empty faint">{emptyText}</div>}
          {rows.map((r, i) => (
            <div key={r.id}>
              {i > 0 && <div className="and-sep">AND</div>}
              <ConditionRow
                row={r}
                suggestions={fb.suggestions}
                readOnly={readOnly}
                showErrors={showErrors}
                autoFocus={fb.focusId === r.id}
                onChange={(nr) => setRows(rows.map((x) => (x.id === r.id ? nr : x)))}
                onRemove={() => setRows(rows.filter((x) => x.id !== r.id))}
              />
            </div>
          ))}
          {!readOnly && (
            <button type="button" className="btn btn-sm btn-ghost add-cond" onClick={fb.addRow}>
              <Plus size={14} /> {t('routing.fb.addCondition')}
            </button>
          )}
          {builderRender.status === 'ok' && builderRender.rendered && (
            <div className="builder-as-text faint" title={t('routing.fb.canonicalTitle')}>
              {t('routing.fb.asText')} <span className="mono">{builderRender.rendered}</span>
            </div>
          )}
          {builderRender.status === 'error' && <div className="field-error">{builderRender.message}</div>}
        </div>
      ) : (
        <DslEditor text={fb.text} onChange={fb.setText} parse={fb.parse} readOnly={readOnly} />
      )}
    </>
  );
}
