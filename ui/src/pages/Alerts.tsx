import { ShieldCheck } from 'lucide-react';
import { TOPICS, type AlertsState } from '../api/types';
import { useTopicData } from '../telemetry';
import { alertCounts, openAlerts } from '../store/selectors';
import { useNow } from '../lib/hooks';
import { AlertCard } from '../components/AlertCard';
import { EmptyState, SkeletonRows } from '../components/ui';
import { EventsFeedList } from '../components/EventsFeedList';
import { useT } from '../i18n';
import './alerts.css';

export default function AlertsPage() {
  const alerts = useTopicData<AlertsState>(TOPICS.ALERTS);
  const now = useNow();
  const t = useT();
  const list = openAlerts(alerts);
  const c = alertCounts(alerts);
  return (
    <div className="page">
      <div className="page-head">
        <div className="titles">
          <h1>{t('alerts.title')}</h1>
          <p>
            {alerts == null
              ? t('common.loadingEllipsis')
              : c.total === 0
                ? t('alerts.subtitle.none')
                : t('alerts.subtitle.counts', { open: c.total, critical: c.critical, warning: c.warning })}
          </p>
        </div>
      </div>
      <div className="alerts-layout">
        <div className="stack">
          {alerts == null ? (
            <section className="card">
              <SkeletonRows rows={4} h={64} />
            </section>
          ) : list.length === 0 ? (
            <section className="card">
              <EmptyState icon={<ShieldCheck size={20} />} title={t('alerts.empty.title')}>
                {t('alerts.empty.body')}
              </EmptyState>
            </section>
          ) : (
            list.map((a) => <AlertCard key={a.id} alert={a} now={now} />)
          )}
        </div>
        <section className="card">
          <div className="card-head">
            <h2>{t('common.history')}</h2>
          </div>
          <div className="card-body flush">
            <EventsFeedList limit={20} />
          </div>
        </section>
      </div>
    </div>
  );
}
