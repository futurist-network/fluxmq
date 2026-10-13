import { useId, useState } from 'react';
import { X } from 'lucide-react';
import { useT } from '../i18n';

/**
 * Free-text chips (Enter / comma / blur to add, Backspace on empty to remove the last).
 * `validate` returns an error for a candidate; invalid candidates stay in the input.
 */
export function ChipsInput({
  value,
  onChange,
  placeholder,
  validate,
  suggestions = [],
  label,
  invalid,
}: {
  value: string[];
  onChange: (v: string[]) => void;
  placeholder?: string;
  validate?: (s: string) => string | null;
  suggestions?: string[];
  label: string;
  invalid?: boolean;
}) {
  const t = useT();
  const [draft, setDraft] = useState('');
  const [err, setErr] = useState<string | null>(null);
  const listId = useId();
  const commit = (raw: string) => {
    const parts = raw
      .split(/[,\s]+/)
      .map((s) => s.trim())
      .filter(Boolean);
    if (!parts.length) return true;
    for (const p of parts) {
      const e = validate?.(p) ?? null;
      if (e) {
        setErr(`“${p}”: ${e}`);
        return false;
      }
    }
    onChange([...new Set([...value, ...parts])]);
    setDraft('');
    setErr(null);
    return true;
  };
  return (
    <div>
      <div className={`chips-input ${invalid || err ? 'invalid' : ''}`}>
        {value.map((v) => (
          <span key={v} className="chip">
            {v}
            <button type="button" aria-label={t('components.chips.remove', { value: v })} onClick={() => onChange(value.filter((x) => x !== v))}>
              <X size={12} />
            </button>
          </span>
        ))}
        <input
          aria-label={label}
          list={suggestions.length ? listId : undefined}
          value={draft}
          placeholder={value.length ? '' : placeholder}
          onChange={(e) => {
            setErr(null);
            const v = e.target.value;
            if (v.endsWith(',') || v.endsWith(' ')) commit(v);
            else setDraft(v);
          }}
          onKeyDown={(e) => {
            if (e.key === 'Enter') {
              e.preventDefault();
              commit(draft);
            } else if (e.key === 'Backspace' && !draft && value.length) {
              onChange(value.slice(0, -1));
            }
          }}
          onBlur={() => commit(draft)}
        />
        {suggestions.length > 0 && (
          <datalist id={listId}>
            {suggestions.map((s) => (
              <option key={s} value={s} />
            ))}
          </datalist>
        )}
      </div>
      {err && <span className="field-error">{err}</span>}
    </div>
  );
}
