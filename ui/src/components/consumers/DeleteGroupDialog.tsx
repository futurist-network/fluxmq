import { useState } from 'react';
import { api, ApiError, errorMessage } from '../../api/client';
import { TOPICS } from '../../api/types';
import { telemetry } from '../../telemetry';
import { toast } from '../../store/ui';
import { ConfirmDialog } from '../ui';
import { t as tr, useT } from '../../i18n';

/**
 * `DELETE /api/channels/{name}/groups/{group}` (admin). Offered only for idle groups; the server
 * still refuses with 409 when subscribers connected meanwhile, or for a `$…` system group a
 * route still uses — its message is shown as is.
 */
export function DeleteGroupDialog({ channel, group, open, onClose }: { channel: string; group: string; open: boolean; onClose: () => void }) {
  const [busy, setBusy] = useState(false);
  const t = useT();

  const submit = async () => {
    setBusy(true);
    try {
      await api.deleteGroup(channel, group);
      toast('success', tr('consumers.delete.done', { group }));
      onClose();
      // Fresh consumers snapshot so the card disappears without waiting for the next delta.
      telemetry.refresh(TOPICS.consumers(channel));
    } catch (e) {
      // 403 is already explained by the shell's toast.
      if (!(e instanceof ApiError && e.isForbidden)) toast('error', tr('consumers.delete.failed', { error: errorMessage(e) }));
    } finally {
      setBusy(false);
    }
  };

  return (
    <ConfirmDialog
      open={open}
      title={t('consumers.delete.title', { group })}
      description={
        <>{t.rich('consumers.delete.body', { group: <b className="mono">{group}</b>, channel: <b className="mono">{channel}</b> })}</>
      }
      confirmLabel={t('consumers.delete.confirm')}
      danger
      busy={busy}
      onConfirm={submit}
      onClose={onClose}
    />
  );
}
