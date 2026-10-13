import { ArrowDown, ArrowRight } from 'lucide-react';
import type { DocLang } from '../../docs/types';

/** Diagrams are boxes in CSS (not SVG text) so they reflow on a phone and follow the theme. */

const ARCH = {
  ru: {
    caption: 'Устройство FluxMQ: один бинарник на узел, кластер — опционально',
    clients: [
      ['Отправители и получатели', 'Rust SDK, fluxmq-bench, fluxmq-demo · TCP :7420'],
      ['Браузер и скрипты', 'веб-консоль, REST /api, WebSocket /ws · HTTP(S) :7421'],
      ['Внешний мониторинг', 'Prometheus /metrics, лента событий, уведомления'],
    ],
    node: 'Узел fluxmq',
    parts: [
      ['Клиентский протокол', 'бинарные кадры, пачки записей, TLS / mTLS'],
      ['HTTP API и консоль', 'REST, телеметрия по WebSocket, права (RBAC)'],
      ['Ядро (data plane)', 'сегментированный лог, fsync до подтверждения, группы, повторы, DLQ'],
      ['Телеметрия', 'скорости, перцентили, правила здоровья, алерты, история'],
      ['Встроенные службы', 'маршрутизатор $router, вебхуки $webhook:*, уведомления, бэкапы'],
    ],
    cluster: 'Только в кластере: Raft :7422 (метаданные, контроллер) · репликация :7423 (разделы, ISR)',
    peers: ['Узел n2', 'Узел n3'],
  },
  en: {
    caption: 'How FluxMQ is built: one binary per node, clustering optional',
    clients: [
      ['Producers and consumers', 'Rust SDK, fluxmq-bench, fluxmq-demo · TCP :7420'],
      ['Browser and scripts', 'web console, REST /api, WebSocket /ws · HTTP(S) :7421'],
      ['External monitoring', 'Prometheus /metrics, events feed, notifications'],
    ],
    node: 'fluxmq node',
    parts: [
      ['Client protocol', 'binary frames, record batches, TLS / mTLS'],
      ['HTTP API and console', 'REST, WebSocket telemetry, permissions (RBAC)'],
      ['Core (data plane)', 'segmented log, fsync before ack, groups, retries, DLQ'],
      ['Telemetry', 'rates, percentiles, health rules, alerts, history'],
      ['Built-in services', 'router $router, webhooks $webhook:*, notifications, backups'],
    ],
    cluster: 'Cluster only: Raft :7422 (metadata, controller) · replication :7423 (partitions, ISR)',
    peers: ['Node n2', 'Node n3'],
  },
} as const;

const FLOW = {
  ru: {
    caption: 'Путь сообщения: подтверждение, повторы и очередь недоставленных (DLQ)',
    steps: [
      ['publish', 'отправитель публикует'],
      ['fsync', 'записано на диск (и на ISR в кластере)'],
      ['подтверждение отправителю', 'только после записи'],
      ['доставка', 'попытка 1 — каждой группе получателей'],
    ],
    ok: ['ack', 'обработано — готово'],
    retry: ['nack(retry) или ack timeout', 'ждём backoff → попытка 2, 3 …'],
    dlq: ['DLQ', 'попытки кончились или nack(retry = false)'],
  },
  en: {
    caption: 'Life of a message: acknowledgement, retries and the dead-letter queue (DLQ)',
    steps: [
      ['publish', 'producer sends'],
      ['fsync', 'durable on disk (and on the ISR in a cluster)'],
      ['ack to producer', 'only now'],
      ['delivery', 'attempt 1 to every group'],
    ],
    ok: ['ack', 'processed'],
    retry: ['nack(retry) or ack timeout', 'wait backoff → attempt 2, 3 …'],
    dlq: ['DLQ', 'attempts exhausted or nack(retry = false)'],
  },
} as const;

function Box({ title, sub, tone }: { title: string; sub?: string; tone?: 'primary' | 'success' | 'warning' | 'critical' }) {
  return (
    <div className={`dia-box ${tone ?? ''}`}>
      <span className="dia-title">{title}</span>
      {sub && <span className="dia-sub">{sub}</span>}
    </div>
  );
}

function Architecture({ lang }: { lang: DocLang }) {
  const d = ARCH[lang];
  return (
    <figure className="doc-diagram" aria-label={d.caption}>
      <div className="dia-row">
        {d.clients.map(([t, s]) => (
          <Box key={t} title={t} sub={s} />
        ))}
      </div>
      <ArrowDown size={18} className="dia-arrow" aria-hidden />
      <div className="dia-node">
        <span className="dia-node-label">{d.node}</span>
        <div className="dia-grid">
          {d.parts.map(([t, s], i) => (
            <Box key={t} title={t} sub={s} tone={i === 2 ? 'primary' : undefined} />
          ))}
        </div>
      </div>
      <ArrowDown size={18} className="dia-arrow" aria-hidden />
      <p className="dia-note">{d.cluster}</p>
      <div className="dia-row peers">
        {d.peers.map((p) => (
          <Box key={p} title={p} />
        ))}
      </div>
      <figcaption>{d.caption}</figcaption>
    </figure>
  );
}

function Flow({ lang }: { lang: DocLang }) {
  const d = FLOW[lang];
  return (
    <figure className="doc-diagram" aria-label={d.caption}>
      <div className="dia-flow">
        {d.steps.map(([t, s], i) => (
          <div key={t} className="dia-step">
            {i > 0 && <ArrowRight size={16} className="dia-arrow" aria-hidden />}
            <Box title={t} sub={s} tone={i === 1 ? 'primary' : undefined} />
          </div>
        ))}
      </div>
      <div className="dia-outcomes">
        <Box title={d.ok[0]} sub={d.ok[1]} tone="success" />
        <Box title={d.retry[0]} sub={d.retry[1]} tone="warning" />
        <Box title={d.dlq[0]} sub={d.dlq[1]} tone="critical" />
      </div>
      <figcaption>{d.caption}</figcaption>
    </figure>
  );
}

export function Diagram({ name, lang }: { name: string; lang: DocLang }) {
  if (name === 'architecture') return <Architecture lang={lang} />;
  if (name === 'delivery') return <Flow lang={lang} />;
  return null;
}
