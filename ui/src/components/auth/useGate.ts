import { gateReason, roleAllows, type Need } from '../../lib/rbac';
import { useRole } from '../../store/auth';
import { useT } from '../../i18n';

/** `{ allowed, reason }` for an action needing `need`. */
export function useGate(need: Need): { allowed: boolean; reason: string | null } {
  const role = useRole();
  useT(); // the reason text follows the console language
  return { allowed: roleAllows(role, need), reason: gateReason(role, need) };
}
