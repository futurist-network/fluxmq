import { useEffect, useId, useRef, useState, type ReactNode } from 'react';
import { MoreHorizontal } from 'lucide-react';
import type { Need } from '../lib/rbac';
import { useGate } from './auth/useGate';
import { useT } from '../i18n';

export interface MenuItem {
  key: string;
  label: string;
  icon?: ReactNode;
  /** Destructive: red text, separated from the safe items above it. */
  danger?: boolean;
  need?: Need;
  disabled?: boolean;
  hint?: string;
  onSelect: () => void;
}

function Item({ item, onDone }: { item: MenuItem; onDone: () => void }) {
  const gate = useGate(item.need ?? 'operate');
  const g = item.need ? gate : { allowed: true, reason: null };
  const disabled = item.disabled || !g.allowed;
  return (
    <button
      type="button"
      role="menuitem"
      className={`menu-item ${item.danger ? 'danger' : ''}`}
      disabled={disabled}
      title={!g.allowed ? (g.reason ?? undefined) : item.hint}
      onClick={() => {
        onDone();
        item.onSelect();
      }}
    >
      {item.icon}
      <span>{item.label}</span>
    </button>
  );
}

/**
 * "⋯" menu for rare and destructive actions, so they don't sit next to everyday buttons.
 * Keyboard: Enter/Space opens, arrows move, Escape closes and returns focus.
 */
export function OverflowMenu({ items, label }: { items: MenuItem[]; label?: string }) {
  const t = useT();
  const [open, setOpen] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  const btn = useRef<HTMLButtonElement>(null);
  const id = useId();

  useEffect(() => {
    if (!open) return;
    const first = root.current?.querySelector<HTMLButtonElement>('.menu-item:not([disabled])');
    first?.focus();
    const onDown = (e: MouseEvent) => {
      if (root.current && !root.current.contains(e.target as Node)) setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.stopPropagation();
        setOpen(false);
        btn.current?.focus();
      } else if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        e.preventDefault();
        const list = [...(root.current?.querySelectorAll<HTMLButtonElement>('.menu-item:not([disabled])') ?? [])];
        const i = list.indexOf(document.activeElement as HTMLButtonElement);
        const next = list[(i + (e.key === 'ArrowDown' ? 1 : list.length - 1)) % list.length];
        next?.focus();
      }
    };
    document.addEventListener('mousedown', onDown);
    document.addEventListener('keydown', onKey, true);
    return () => {
      document.removeEventListener('mousedown', onDown);
      document.removeEventListener('keydown', onKey, true);
    };
  }, [open]);

  const safe = items.filter((i) => !i.danger);
  const danger = items.filter((i) => i.danger);
  return (
    <div className="menu-root" ref={root} onClick={(e) => e.stopPropagation()}>
      <button
        ref={btn}
        type="button"
        className="btn btn-sm btn-icon btn-ghost"
        aria-haspopup="menu"
        aria-expanded={open}
        aria-controls={open ? id : undefined}
        aria-label={label ?? t('components.moreActions')}
        title={label ?? t('components.moreActions')}
        onClick={() => setOpen((o) => !o)}
      >
        <MoreHorizontal size={16} />
      </button>
      {open && (
        <div className="menu" role="menu" id={id}>
          {safe.map((i) => (
            <Item key={i.key} item={i} onDone={() => setOpen(false)} />
          ))}
          {safe.length > 0 && danger.length > 0 && <div className="menu-sep" role="separator" />}
          {danger.map((i) => (
            <Item key={i.key} item={i} onDone={() => setOpen(false)} />
          ))}
        </div>
      )}
    </div>
  );
}
