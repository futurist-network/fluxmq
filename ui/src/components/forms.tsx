/** Form pieces shared by the v0.3 screens: secret inputs, header rows, copyable code, chip toggles. */
import { useState, type ReactNode } from 'react';
import { Check, Copy, Eye, EyeOff, Lock, Plus, RotateCcw, Trash2, X } from 'lucide-react';
import { useGate } from './auth/useGate';
import { useRole } from '../store/auth';
import { ROLE_LABEL } from '../lib/rbac';
import { copyText } from '../lib/hooks';
import { newHeaderRow, type HeaderRow, type SecretField } from '../lib/secrets';
import { toast } from '../store/ui';
import { t as tr, useT } from '../i18n';
import './forms.css';

/**
 * A secret the server never returns: stored values show as dots with Replace (and Remove for
 * optional ones); Replace switches to a real input, Cancel goes back to keeping the stored value.
 */
export function SecretInput({
  id,
  value,
  onChange,
  placeholder,
  optional,
  disabled,
  invalid,
  label,
}: {
  id?: string;
  value: SecretField;
  onChange: (v: SecretField) => void;
  placeholder?: string;
  optional?: boolean;
  disabled?: boolean;
  invalid?: boolean;
  label: string;
}) {
  const t = useT();
  const [shown, setShown] = useState(false);
  if (value.stored && !value.replacing) {
    return (
      <div className="secret-stored">
        <span className="secret-dots" aria-label={t('components.secret.storedAria', { label })}>
          ••••••••
        </span>
        <span className="faint secret-note">{t('components.secret.stored')}</span>
        <span className="spacer" />
        {!disabled && (
          <>
            <button type="button" className="btn btn-sm" onClick={() => onChange({ ...value, replacing: true, value: '' })}>
              {t('components.secret.replace')}
            </button>
            {optional && (
              <button type="button" className="btn btn-sm btn-ghost" onClick={() => onChange({ ...value, replacing: true, value: '' })} title={t('components.secret.removeOnSave')} aria-label={t('components.secret.removeAria', { label })}>
                <Trash2 size={13} />
              </button>
            )}
          </>
        )}
      </div>
    );
  }
  return (
    <div className="secret-edit">
      <div className="secret-field">
        <input
          id={id}
          className={`input mono ${invalid ? 'invalid' : ''}`}
          type={shown ? 'text' : 'password'}
          autoComplete="new-password"
          spellCheck={false}
          value={value.value}
          disabled={disabled}
          placeholder={value.stored ? (optional ? t('components.secret.emptyRemoves') : t('components.secret.newValue')) : placeholder}
          aria-label={label}
          onChange={(e) => onChange({ ...value, value: e.target.value })}
        />
        <button type="button" className="btn btn-ghost btn-icon btn-sm" onClick={() => setShown(!shown)} aria-label={shown ? t('components.secret.hide') : t('components.secret.show')} disabled={disabled}>
          {shown ? <EyeOff size={14} /> : <Eye size={14} />}
        </button>
      </div>
      {value.stored && (
        <button type="button" className="btn btn-sm btn-ghost" onClick={() => onChange({ ...value, replacing: false, value: '' })} title={t('components.secret.keepHint')}>
          <RotateCcw size={13} /> {t('components.secret.keep')}
        </button>
      )}
    </div>
  );
}

/** Custom request headers. Every value is a secret: stored ones show as dots. */
export function HeadersEditor({ rows, onChange, disabled, error }: { rows: HeaderRow[]; onChange: (r: HeaderRow[]) => void; disabled?: boolean; error?: string }) {
  const t = useT();
  const set = (id: string, patch: Partial<HeaderRow>) => onChange(rows.map((r) => (r.id === id ? { ...r, ...patch } : r)));
  return (
    <div className="headers-editor">
      {rows.length === 0 && <div className="faint headers-empty">{t('components.headers.empty')}</div>}
      {rows.map((r) => (
        <div key={r.id} className="header-row">
          <input className="input mono" value={r.key} placeholder="Authorization" aria-label={t('components.headers.name')} disabled={disabled} onChange={(e) => set(r.id, { key: e.target.value })} />
          <SecretInput value={r.value} onChange={(value) => set(r.id, { value })} label={t('components.headers.valueOf', { name: r.key || t('components.headers.header') })} placeholder="Bearer …" disabled={disabled} />
          {!disabled && (
            <button type="button" className="btn btn-ghost btn-icon btn-sm" aria-label={t('components.headers.remove', { name: r.key })} onClick={() => onChange(rows.filter((x) => x.id !== r.id))}>
              <X size={15} />
            </button>
          )}
        </div>
      ))}
      {!disabled && (
        <button type="button" className="btn btn-sm btn-ghost add-header" onClick={() => onChange([...rows, newHeaderRow()])}>
          <Plus size={14} /> {t('components.headers.add')}
        </button>
      )}
      {error && <span className="field-error">{error}</span>}
    </div>
  );
}

export function CopyButton({ text, label, small = true }: { text: string; label?: string; small?: boolean }) {
  const t = useT();
  const [done, setDone] = useState(false);
  return (
    <button
      type="button"
      className={`btn ${small ? 'btn-sm' : ''}`}
      onClick={async () => {
        if (await copyText(text)) {
          setDone(true);
          toast('success', tr('common.copied'));
          setTimeout(() => setDone(false), 1500);
        }
      }}
    >
      {done ? <Check size={13} /> : <Copy size={13} />} {done ? t('common.copied') : (label ?? t('common.copy'))}
    </button>
  );
}

/** Monospace block with a copy button. */
export function CodeBlock({ code, title, actions }: { code: string; title?: ReactNode; actions?: ReactNode }) {
  return (
    <div className="code-block">
      <div className="code-head">
        {title && <span className="code-title">{title}</span>}
        <span className="spacer" />
        {actions}
        <CopyButton text={code} />
      </div>
      <pre className="mono">{code}</pre>
    </div>
  );
}

/** Multi-select as toggle chips (empty = “any”). */
export function ToggleChips<T extends string>({ options, value, onChange, disabled, label, anyLabel }: { options: { value: T; label: string }[]; value: T[]; onChange: (v: T[]) => void; disabled?: boolean; label: string; anyLabel?: string }) {
  return (
    <div className="toggle-chips" role="group" aria-label={label}>
      {anyLabel && (
        <button type="button" className="tchip" aria-pressed={value.length === 0} disabled={disabled} onClick={() => onChange([])}>
          {anyLabel}
        </button>
      )}
      {options.map((o) => {
        const on = value.includes(o.value);
        return (
          <button key={o.value} type="button" className="tchip" aria-pressed={on} disabled={disabled} onClick={() => onChange(on ? value.filter((x) => x !== o.value) : [...value, o.value])}>
            {o.label}
          </button>
        );
      })}
    </div>
  );
}

/** Label + control + hint/error, the common field layout. */
export function Field({ label, htmlFor, hint, error, children, className }: { label: ReactNode; htmlFor?: string; hint?: ReactNode; error?: string; children: ReactNode; className?: string }) {
  return (
    <div className={`field ${className ?? ''}`}>
      {htmlFor ? <label htmlFor={htmlFor}>{label}</label> : <span className="field-label">{label}</span>}
      {children}
      {error ? <span className="field-error">{error}</span> : hint ? <span className="field-hint">{hint}</span> : null}
    </div>
  );
}

/** "Read-only — … requires admin" strip for roles that can only look. */
export function ReadOnlyNote({ what }: { what: string }) {
  const t = useT();
  const role = useRole();
  const g = useGate('admin');
  if (g.allowed) return null;
  return (
    <div className="readonly-strip">
      <Lock size={13} /> {t('components.readOnly', { what, reason: role ? t('components.readOnlyReason', { role: ROLE_LABEL[role].toLowerCase() }) : '' })}
    </div>
  );
}
