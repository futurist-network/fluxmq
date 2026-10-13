import { useEffect } from 'react';
import { Users } from 'lucide-react';
import { TOPICS, type ConsumersState } from '../../api/types';
import { useTopicError, useTopicSelect } from '../../telemetry';
import { flattenGroups } from '../../store/selectors';
import { GroupCard } from '../../components/consumers/GroupCard';
import { useAlerts } from '../../components/HealthWhy';
import { EmptyState, InlineError, SkeletonRows } from '../../components/ui';
import { useT } from '../../i18n';

export function ChannelConsumersTab({ name }: { name: string }) {
  const t = useT();
  const topic = TOPICS.consumers(name);
  // Only the problems-first group order; each card selects its own group.
  const alerts = useAlerts();
  const order = useTopicSelect<ConsumersState, string[] | null>(topic, (c) => (c ? flattenGroups({ [name]: c }, alerts).map((g) => g.group.group) : null));
  const error = useTopicError(topic);

  useEffect(() => {
    if (!order || !location.hash) return;
    document.getElementById(location.hash.slice(1))?.scrollIntoView({ block: 'start' });
  }, [order]);

  if (error) return <InlineError>{t('channels.consumers.unavailable', { error })}</InlineError>;
  if (!order) return <section className="card"><SkeletonRows rows={4} /></section>;
  if (!order.length)
    return (
      <section className="card">
        <EmptyState icon={<Users size={20} />} title={t('channels.consumers.none')}>
          {t('channels.consumers.noneBody')}
        </EmptyState>
      </section>
    );
  return (
    <div className="stack">
      {order.map((g) => (
        <GroupCard key={g} channel={name} group={g} />
      ))}
    </div>
  );
}
