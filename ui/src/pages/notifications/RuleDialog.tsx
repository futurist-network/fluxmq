import { useState } from 'react';
import { api, ApiError, errorMessage } from '../../api/client';
import { TOPICS, type AlertKind, type ChannelsSummary, type EventKind, type NotificationRule, type Notifier } from '../../api/types';
import { ALERT_KIND_LABEL, EVENT_KIND_LABEL } from '../../lib/labels';
import { validateGlob } from '../../lib/rbac';
import { draftToCreateRule, draftToUpdateRule, emptyRuleDraft, GROUP_WAIT_PRESETS, matchedChannels, REPEAT_PRESETS, ruleSummary, ruleToDraft, validateRuleDraft, type RuleDraft } from '../../lib/ruleForm';
import { channelNames } from '../../store/selectors';
import { useTopicSelect } from '../../telemetry';
import { toast } from '../../store/ui';
import { InlineError, Modal, Segmented, Switch } from '../../components/ui';
import { ChipsInput } from '../../components/ChipsInput';
import { Field, ToggleChips } from '../../components/forms';
import { GateButton } from '../../components/auth/Gate';
import { useGate } from '../../components/auth/useGate';
import { NotifierIcon } from './NotifierDialog';
import { useT } from '../../i18n';

// Built per render: the labels are read in the current language.
const alertOpts = () => (Object.keys(ALERT_KIND_LABEL) as AlertKind[]).map((k) => ({ value: k, label: ALERT_KIND_LABEL[k] }));
const eventOpts = () => (Object.keys(EVENT_KIND_LABEL) as EventKind[]).filter((k) => k !== 'alert_opened' && k !== 'alert_resolved').map((k) => ({ value: k, label: EVENT_KIND_LABEL[k] }));

export function RuleDialog({ open, onClose, initial, notifiers, onSaved }: { open: boolean; onClose: () => void; initial: NotificationRule | null; notifiers: Notifier[]; onSaved: (r: NotificationRule) => void }) {
  const t = useT();
  const gate = useGate('admin');
  const readOnly = !gate.allowed;
  const channels = useTopicSelect<ChannelsSummary, string[]>(TOPICS.CHANNELS, channelNames);
  const [d, setD] = useState<RuleDraft>(() => (initial ? ruleToDraft(initial) : emptyRuleDraft(notifiers.length === 1 ? [notifiers[0]!.id] : [])));
  const [showErrors, setShowErrors] = useState(false);
  const [busy, setBusy] = useState(false);
  const [apiError, setApiError] = useState<string | null>(null);
  const isNew = initial == null;
  const errs = validateRuleDraft(d, notifiers.map((n) => n.id));
  const shown = showErrors ? errs : {};
  const set = (p: Partial<RuleDraft>) => setD({ ...d, ...p });
  const patch = initial ? draftToUpdateRule(d, initial) : null;
  const dirty = isNew || Object.keys(patch ?? {}).length > 0;
  const matched = matchedChannels(d.channels, channels);

  const save = async () => {
    setShowErrors(true);
    if (Object.keys(errs).length) return;
    setBusy(true);
    setApiError(null);
    try {
      const r = isNew ? await api.createRule(draftToCreateRule(d)) : await api.updateRule(initial.id, patch!);
      toast('success', isNew ? t('notifications.ruleForm.toastCreated', { name: r.name }) : t('notifications.ruleForm.toastSaved'));
      onSaved(r);
      onClose();
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) setApiError(errorMessage(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal
      open={open}
      onClose={onClose}
      wide
      title={isNew ? t('notifications.ruleForm.titleNew') : t('notifications.ruleForm.titleEdit', { name: initial.name })}
      description={t('notifications.ruleForm.desc')}
      footer={
        <>
          <span className="faint rule-preview truncate" title={ruleSummary(d, (k) => ALERT_KIND_LABEL[k])}>
            {ruleSummary(d, (k) => ALERT_KIND_LABEL[k])}
          </span>
          <span className="spacer" />
          <button className="btn" type="button" onClick={onClose}>
            {readOnly ? t('common.close') : t('common.cancel')}
          </button>
          <GateButton need="admin" className="btn btn-primary" onClick={save} disabled={busy || !dirty}>
            {busy ? t('common.saving') : isNew ? t('notifications.ruleForm.create') : t('common.save')}
          </GateButton>
        </>
      }
    >
      <fieldset className="form-stack fs-plain" disabled={readOnly}>
        <div className="form-grid">
          <Field label={t('common.name')} htmlFor="rl-name" error={shown.name}>
            <input id="rl-name" className={`input ${shown.name ? 'invalid' : ''}`} value={d.name} disabled={readOnly} onChange={(e) => set({ name: e.target.value })} placeholder={t('notifications.ruleForm.namePh')} data-autofocus />
          </Field>
          <Field label={t('common.enabled')}>
            <span className="nf-enabled">
              <Switch checked={d.enabled} onChange={(enabled) => set({ enabled })} label={t('notifications.ruleForm.enabledLabel')} disabled={readOnly} />
              <span className="muted">{t(d.enabled ? 'notifications.ruleForm.active' : 'notifications.ruleForm.paused')}</span>
            </span>
          </Field>
        </div>

        <Field label={t('notifications.ruleForm.sendTo')} error={shown.notifier_ids}>
          <div className="toggle-chips" role="group" aria-label={t('notifications.ruleForm.notifiers')}>
            {notifiers.map((n) => {
              const on = d.notifier_ids.includes(n.id);
              return (
                <button key={n.id} type="button" className="tchip tchip-icon" aria-pressed={on} disabled={readOnly} onClick={() => set({ notifier_ids: on ? d.notifier_ids.filter((x) => x !== n.id) : [...d.notifier_ids, n.id] })}>
                  <NotifierIcon type={n.kind.type} size={12} /> {n.name}
                  {!n.enabled && <span className="faint">{t('notifications.ruleForm.pausedSuffix')}</span>}
                </button>
              );
            })}
          </div>
        </Field>

        <div className="rule-match">
          <div className="rm-title">{t('notifications.ruleForm.match')}</div>
          <Field label={t('notifications.ruleForm.severity')} hint={t('notifications.ruleForm.severityHint')}>
            <Segmented
              value={d.min_severity}
              onChange={(min_severity) => set({ min_severity })}
              label={t('notifications.ruleForm.minSeverity')}
              options={[
                { value: 'warning', label: t('labels.health.warning') },
                { value: 'critical', label: t('labels.health.critical') },
              ]}
            />
          </Field>
          <Field label={t('notifications.ruleForm.alertKinds')}>
            <ToggleChips options={alertOpts()} value={d.alert_kinds} onChange={(alert_kinds) => set({ alert_kinds })} disabled={readOnly} label={t('notifications.ruleForm.alertKinds')} anyLabel={t('notifications.ruleForm.anyKind')} />
          </Field>
          <Field label={t('notifications.ruleForm.events')} hint={t('notifications.ruleForm.eventsHint')}>
            <ToggleChips options={eventOpts()} value={d.event_kinds} onChange={(event_kinds) => set({ event_kinds })} disabled={readOnly} label={t('notifications.ruleForm.eventKinds')} />
          </Field>
          <Field
            label={t('notifications.ruleForm.channels')}
            hint={
              d.channels.length ? (
                matched.length ? (
                  <>
                    {t.rich('notifications.ruleForm.matches', { count: matched.length, list: <span className="mono">{matched.slice(0, 4).join(', ')}</span> })}
                    {matched.length > 4 ? ` +${matched.length - 4}` : ''}
                  </>
                ) : (
                  t('notifications.ruleForm.matchesNone')
                )
              ) : (
                t('notifications.ruleForm.channelsEmpty', { count: channels.length })
              )
            }
          >
            <ChipsInput label={t('notifications.ruleForm.channelGlobs')} value={d.channels} onChange={(c) => set({ channels: c })} validate={validateGlob} placeholder={t('notifications.ruleForm.channelGlobsPh')} suggestions={channels} />
          </Field>
        </div>

        <div className="form-grid">
          <Field label={t('notifications.ruleForm.groupWait')} hint={t('notifications.ruleForm.groupWaitHint')} error={shown.group_wait_secs}>
            <Segmented value={String(d.group_wait_secs)} onChange={(v) => set({ group_wait_secs: Number(v) })} label={t('notifications.ruleForm.groupWait')} options={GROUP_WAIT_PRESETS.map((p) => ({ value: String(p.value), label: p.label }))} />
          </Field>
          <Field label={t('notifications.ruleForm.repeat')} hint={t('notifications.ruleForm.repeatHint')}>
            <Segmented value={String(d.repeat_interval_secs)} onChange={(v) => set({ repeat_interval_secs: Number(v) })} label={t('notifications.ruleForm.repeatInterval')} options={REPEAT_PRESETS.map((p) => ({ value: String(p.value), label: p.label }))} />
          </Field>
          <Field label={t('notifications.ruleForm.recovery')}>
            <label className="check-row">
              <input type="checkbox" className="checkbox" checked={d.send_resolved} disabled={readOnly} onChange={(e) => set({ send_resolved: e.target.checked })} />
              {t('notifications.ruleForm.sendResolved')}
            </label>
          </Field>
        </div>
        {apiError && <InlineError>{apiError}</InlineError>}
      </fieldset>
    </Modal>
  );
}
