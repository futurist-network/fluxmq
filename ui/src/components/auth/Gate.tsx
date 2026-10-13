import type { ButtonHTMLAttributes, ReactNode } from 'react';
import { ROLE_LABEL, type Need } from '../../lib/rbac';
import type { Role } from '../../api/types';
import { useT } from '../../i18n';
import { useGate } from './useGate';

/**
 * A button that stays visible but is disabled (with a tooltip saying which role is needed)
 * when the current role can't perform the action. Disabled buttons swallow hover events in
 * some browsers, so the tooltip lives on a wrapper.
 */
export function GateButton({ need, children, disabled, title, ...rest }: ButtonHTMLAttributes<HTMLButtonElement> & { need: Need; children: ReactNode }) {
  const g = useGate(need);
  if (g.allowed) {
    return (
      <button {...rest} disabled={disabled} title={title}>
        {children}
      </button>
    );
  }
  return (
    <span className="gate-wrap" title={g.reason ?? undefined} data-gated>
      <button {...rest} disabled aria-disabled="true" aria-label={typeof children === 'string' ? `${children} (${g.reason})` : rest['aria-label']}>
        {children}
      </button>
    </span>
  );
}

export function RoleBadge({ role, small }: { role: string; small?: boolean }) {
  useT(); // re-render on language change
  const label = (ROLE_LABEL as Record<string, string | undefined>)[role as Role] ?? role;
  return <span className={`badge role-badge role-${role} ${small ? 'sm' : ''}`}>{label}</span>;
}
