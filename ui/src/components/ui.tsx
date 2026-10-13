import { useEffect, useId, useRef, useState, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { AlertTriangle, X } from 'lucide-react';
import type { ChannelMode, Health } from '../api/types';
import { HEALTH_LABEL, MODE_HINT, MODE_LABEL } from '../lib/labels';
import { useT } from '../i18n';

export function ModeBadge({ mode }: { mode: ChannelMode }) {
  useT(); // re-render the tooltip on language change
  return (
    <span className={`badge badge-mode mode-${mode}`} title={MODE_HINT[mode]}>
      <span className="glyph" aria-hidden />
      {MODE_LABEL[mode]}
    </span>
  );
}

export function HealthDot({ health, pulse, title }: { health: Health; pulse?: boolean; title?: string }) {
  if (title)
    return (
      <span className={`dot ${health === 'healthy' ? '' : health} has-why`} title={title} role="img" aria-label={title} />
    );
  return <span className={`dot ${health === 'healthy' ? '' : health} ${pulse && health === 'healthy' ? 'pulse' : ''}`} aria-hidden />;
}

/** Health as text. Healthy stays neutral grey — only problems get colour. `why` = tooltip with the reasons. */
export function HealthText({ health, label, why }: { health: Health; label?: string; why?: string }) {
  useT();
  return (
    <span className={`health-text ${health === 'healthy' ? '' : health} ${why ? 'has-why' : ''}`} title={why}>
      <HealthDot health={health} />
      {label ?? HEALTH_LABEL[health]}
      {why && <span className="sr-only">. {why}</span>}
    </span>
  );
}

export function Skeleton({ w = '100%', h = 14, r, style }: { w?: number | string; h?: number | string; r?: number; style?: React.CSSProperties }) {
  return <span className="skeleton" style={{ width: w, height: h, borderRadius: r, ...style }} aria-hidden />;
}

export function SkeletonRows({ rows = 5, h = 44 }: { rows?: number; h?: number }) {
  const t = useT();
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10, padding: '8px 20px 16px' }} aria-busy="true" aria-label={t('components.loading')}>
      {Array.from({ length: rows }, (_, i) => (
        <Skeleton key={i} h={h - 10} w={`${92 - ((i * 13) % 30)}%`} />
      ))}
    </div>
  );
}

export function EmptyState({ icon, title, children, actions, compact }: { icon: ReactNode; title: string; children?: ReactNode; actions?: ReactNode; compact?: boolean }) {
  return (
    <div className={`empty ${compact ? 'compact' : ''}`}>
      <div className="empty-icon" aria-hidden>
        {icon}
      </div>
      <h3>{title}</h3>
      {children && <p>{children}</p>}
      {actions && <div className="empty-actions">{actions}</div>}
    </div>
  );
}

export function InlineError({ children, onRetry }: { children: ReactNode; onRetry?: () => void }) {
  const t = useT();
  return (
    <div className="inline-error" role="alert">
      <AlertTriangle size={16} />
      <span style={{ flex: 1 }}>{children}</span>
      {onRetry && (
        <button className="btn btn-sm" onClick={onRetry}>
          {t('common.retry')}
        </button>
      )}
    </div>
  );
}

export function Segmented<T extends string>({ value, options, onChange, label }: { value: T; options: { value: T; label: string }[]; onChange: (v: T) => void; label: string }) {
  return (
    <div className="segmented" role="group" aria-label={label}>
      {options.map((o) => (
        <button key={o.value} type="button" aria-pressed={o.value === value} onClick={() => onChange(o.value)}>
          {o.label}
        </button>
      ))}
    </div>
  );
}

export function Switch({ checked, onChange, label, disabled, title }: { checked: boolean; onChange: (v: boolean) => void; label: string; disabled?: boolean; title?: string }) {
  const sw = <button type="button" role="switch" aria-checked={checked} aria-label={label} className="switch" disabled={disabled} onClick={() => onChange(!checked)} />;
  // Disabled buttons swallow hover in some browsers: put the explanation on a wrapper.
  return title ? (
    <span className={disabled ? 'gate-wrap' : undefined} title={title} style={{ display: 'inline-flex' }}>
      {sw}
    </span>
  ) : (
    sw
  );
}

const FOCUSABLE = 'button:not([disabled]), [href], input:not([disabled]), select, textarea, [tabindex]:not([tabindex="-1"])';

/** Escape-to-close, focus-in on open, focus-restore on close, Tab trapping. */
function useDialog(open: boolean, onClose: () => void, ref: React.RefObject<HTMLElement | null>) {
  const closeRef = useRef(onClose);
  useEffect(() => {
    closeRef.current = onClose;
  });
  useEffect(() => {
    if (!open) return;
    const prev = document.activeElement as HTMLElement | null;
    const el = ref.current;
    const first = el?.querySelector<HTMLElement>('[data-autofocus]') ?? el?.querySelector<HTMLElement>(FOCUSABLE);
    first?.focus();
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.stopPropagation();
        closeRef.current();
      } else if (e.key === 'Tab' && el) {
        const items = [...el.querySelectorAll<HTMLElement>(FOCUSABLE)].filter((n) => n.offsetParent !== null);
        if (!items.length) return;
        const a = items[0]!;
        const z = items[items.length - 1]!;
        if (e.shiftKey && document.activeElement === a) {
          e.preventDefault();
          z.focus();
        } else if (!e.shiftKey && document.activeElement === z) {
          e.preventDefault();
          a.focus();
        }
      }
    };
    document.addEventListener('keydown', onKey, true);
    return () => {
      document.removeEventListener('keydown', onKey, true);
      prev?.focus?.();
    };
  }, [open, ref]);
}

export function Modal({
  open,
  onClose,
  title,
  description,
  children,
  footer,
  wide,
}: {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  description?: ReactNode;
  children?: ReactNode;
  footer?: ReactNode;
  wide?: boolean;
}) {
  const t = useT();
  const ref = useRef<HTMLDivElement>(null);
  const id = useId();
  useDialog(open, onClose, ref);
  if (!open) return null;
  return createPortal(
    <>
      <div className="scrim" onClick={onClose} />
      <div ref={ref} className={`modal ${wide ? 'wide' : ''}`} role="dialog" aria-modal="true" aria-labelledby={id}>
        <div className="modal-head">
          <div style={{ flex: 1, minWidth: 0 }}>
            <h2 id={id}>{title}</h2>
            {description && <p>{description}</p>}
          </div>
          <button className="btn btn-ghost btn-icon btn-sm" onClick={onClose} aria-label={t('common.close')}>
            <X size={16} />
          </button>
        </div>
        {children && <div className="modal-body">{children}</div>}
        {footer && <div className="modal-foot">{footer}</div>}
      </div>
    </>,
    document.body,
  );
}

export function Drawer({ open, onClose, title, subtitle, children }: { open: boolean; onClose: () => void; title: ReactNode; subtitle?: ReactNode; children: ReactNode }) {
  const t = useT();
  const ref = useRef<HTMLDivElement>(null);
  const id = useId();
  useDialog(open, onClose, ref);
  if (!open) return null;
  return createPortal(
    <>
      <div className="scrim" onClick={onClose} />
      <aside ref={ref} className="drawer" role="dialog" aria-modal="true" aria-labelledby={id}>
        <div className="drawer-head">
          <div style={{ flex: 1, minWidth: 0 }}>
            <h2 id={id} className="truncate" style={{ fontSize: 16 }}>
              {title}
            </h2>
            {subtitle && <div className="faint" style={{ fontSize: 12, marginTop: 2 }}>{subtitle}</div>}
          </div>
          <button className="btn btn-ghost btn-icon btn-sm" onClick={onClose} aria-label={t('common.close')}>
            <X size={16} />
          </button>
        </div>
        <div className="drawer-body">{children}</div>
      </aside>
    </>,
    document.body,
  );
}

export function ConfirmDialog({
  open,
  title,
  description,
  confirmLabel,
  cancelLabel,
  danger,
  busy,
  onConfirm,
  onClose,
  children,
  wide,
  typeToConfirm,
}: {
  open: boolean;
  title: string;
  description?: ReactNode;
  confirmLabel: string;
  /** Dismiss button text (default "Cancel" — rename when the action itself is a cancel). */
  cancelLabel?: string;
  danger?: boolean;
  busy?: boolean;
  onConfirm: () => void;
  onClose: () => void;
  children?: ReactNode;
  /** Wider dialog (tables inside). */
  wide?: boolean;
  /** Bulk destructive actions: the confirm button stays disabled until this exact text is typed. */
  typeToConfirm?: string;
}) {
  const t = useT();
  const [typed, setTyped] = useState('');
  const inputId = useId();
  // Start empty every time the dialog opens (state adjusted during render, no effect).
  const [wasOpen, setWasOpen] = useState(open);
  if (open !== wasOpen) {
    setWasOpen(open);
    setTyped('');
  }
  const blocked = typeToConfirm != null && typed.trim() !== typeToConfirm;
  return (
    <Modal
      open={open}
      onClose={onClose}
      wide={wide}
      title={title}
      description={description}
      footer={
        <>
          {/* Destructive dialogs focus Cancel first, so Enter never deletes by accident. */}
          <button className="btn" onClick={onClose} data-autofocus={danger && typeToConfirm == null ? true : undefined}>
            {cancelLabel ?? t('common.cancel')}
          </button>
          <button className={`btn ${danger ? 'btn-danger' : 'btn-primary'}`} onClick={onConfirm} disabled={busy || blocked} data-autofocus={danger ? undefined : true}>
            {busy ? t('components.working') : confirmLabel}
          </button>
        </>
      }
    >
      {children || typeToConfirm != null ? (
        <>
      {children}
      {typeToConfirm != null && (
        <label className="confirm-type" htmlFor={inputId}>
          <span>{t.rich('components.typeToConfirm', { name: <b className="mono">{typeToConfirm}</b> })}</span>
          <input
            id={inputId}
            className="input mono"
            autoComplete="off"
            spellCheck={false}
            value={typed}
            onChange={(e) => setTyped(e.target.value)}
            aria-label={t('components.typeToConfirmAria', { name: typeToConfirm })}
            data-autofocus
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !blocked && !busy) onConfirm();
            }}
          />
        </label>
      )}
        </>
      ) : undefined}
    </Modal>
  );
}

export function Stat({ label, value, unit, tone }: { label: string; value: ReactNode; unit?: string; tone?: Health }) {
  return (
    <div className="stat">
      <span className="stat-label">{label}</span>
      <span className={`stat-value ${tone === 'warning' ? 'text-warning' : tone === 'critical' ? 'text-critical' : ''}`}>
        {value}
        {unit && <span className="unit">{unit}</span>}
      </span>
    </div>
  );
}
