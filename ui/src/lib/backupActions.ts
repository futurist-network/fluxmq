/** "Run backup now" — shared by the Backups page and ⌘K. Returns true when a run started. */
import { api, ApiError, errorMessage, isApiErrorCode } from '../api/client';
import { ERROR_UNSUPPORTED_IN_CLUSTER } from '../api/types';
import { toast } from '../store/ui';
import { t } from '../i18n';

export async function runBackupNow(onRefused?: (message: string) => void): Promise<boolean> {
  try {
    const r = await api.runBackup();
    toast('success', r?.id ? t('backups.toast.startedId', { id: r.id }) : t('backups.toast.started'));
    return true;
  } catch (e) {
    if (e instanceof ApiError && e.isForbidden) return false; // the shell explains 403 once
    if (isClusterRefusal(e)) onRefused?.(errorMessage(e));
    toast('error', e instanceof ApiError && e.isConflict ? errorMessage(e) : t('backups.toast.startFailed', { error: errorMessage(e) }));
    return false;
  }
}

/** The cluster can't run backups (`ApiError.error = "unsupported_in_cluster"`, e.g. a node too old
 *  for the internal backup API). Branches on the machine-readable code only — never the message. */
export function isClusterRefusal(e: unknown): boolean {
  return isApiErrorCode(e, ERROR_UNSUPPORTED_IN_CLUSTER);
}
