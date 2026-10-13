---
title: Кластер из трёх узлов с TLS
group: howto
order: 1
summary: Поднять кластер из трёх узлов на одной машине или в Docker — с репликацией, взаимным TLS между узлами, HTTPS и TLS для клиентов.
---

## 1. Соберите бинарник {#build}

```bash
cd ui && pnpm install && pnpm build && cd ..
cargo build --release -p fluxmq-server -p fluxmq-client
```

## 2. Выпустите сертификаты {#certs}

Для разработки — встроенной командой (в продакшене выпускайте сертификаты своим CA):

```bash
./target/release/fluxmq tls gen-dev --out ./certs --nodes n1,n2,n3 --hosts localhost,127.0.0.1
```

| Файл | Для чего |
|---|---|
| `ca.pem` / `ca-key.pem` | учебный CA: `--tls-client-ca`, `--cluster-tls-ca`, CA для клиентов |
| `server.pem` / `server-key.pem` | клиентский и HTTP-порт (`--tls-cert` / `--tls-key`) |
| `client.pem` / `client-key.pem` | клиентский сертификат для mTLS (CN `client`) |
| `node-<id>.pem` / `node-<id>-key.pem` | сертификат узла кластера (CN = id узла) |

Ключи пишутся с правами `0600`; существующие файлы не перезаписываются без `--force`. `--valid-days N` задаёт срок (по умолчанию 397 дней).

## 3. Запустите три узла {#start}

По терминалу на узел. Порты сдвинуты (17420…), чтобы не мешать одиночному брокеру.

```bash
export FLUXMQ_CLUSTER_SECRET=dev-secret
PEERS=n1@127.0.0.1:17422,n2@127.0.0.1:17432,n3@127.0.0.1:17442
TLS="--tls-cert ./certs/server.pem --tls-key ./certs/server-key.pem --tls-client-ca ./certs/ca.pem --cluster-tls-ca ./certs/ca.pem"

./target/release/fluxmq --node-id n1 --peers $PEERS --data-dir ./data/n1 \
  --client-addr 127.0.0.1:17420 --http-addr 127.0.0.1:17421 \
  --raft-addr 127.0.0.1:17422 --replication-addr 127.0.0.1:17423 \
  --cluster-tls-cert ./certs/node-n1.pem --cluster-tls-key ./certs/node-n1-key.pem $TLS

./target/release/fluxmq --node-id n2 --peers $PEERS --data-dir ./data/n2 \
  --client-addr 127.0.0.1:17430 --http-addr 127.0.0.1:17431 \
  --raft-addr 127.0.0.1:17432 --replication-addr 127.0.0.1:17433 \
  --cluster-tls-cert ./certs/node-n2.pem --cluster-tls-key ./certs/node-n2-key.pem $TLS

./target/release/fluxmq --node-id n3 --peers $PEERS --data-dir ./data/n3 \
  --client-addr 127.0.0.1:17440 --http-addr 127.0.0.1:17441 \
  --raft-addr 127.0.0.1:17442 --replication-addr 127.0.0.1:17443 \
  --cluster-tls-cert ./certs/node-n3.pem --cluster-tls-key ./certs/node-n3-key.pem $TLS
```

Узлы можно запускать в любом порядке; кластер инициализирует узел с наименьшим id, как только наберётся большинство.

- `--node-id` и `--cluster-secret` обязательны с `--peers`; секрет одинаковый на всех узлах. Сертификаты его дополняют, а не заменяют.
- **Все узлы настраиваются одинаково**: узел без cluster TLS не подключится к TLS-кластеру, и наоборот.
- HTTPS на HTTP-порту в кластере требует cluster TLS — узлы вызывают `/internal/*` друг друга через этот порт.
- На нескольких машинах обязательно задайте `--advertise-host` — адрес, по которому узел видят другие узлы и клиенты.

## 4. Проверьте {#verify}

```bash
C="--cacert ./certs/ca.pem"
curl -s $C https://127.0.0.1:17421/api/cluster/metadata | jq '.controller, [.nodes[] | {id, status, is_voter}]'
curl -s $C -X POST https://127.0.0.1:17431/api/channels -H 'content-type: application/json' \
  -d '{"name":"orders","mode":"stream","partitions":6}'
curl -s $C https://127.0.0.1:17441/api/cluster/metadata | jq -c '.channels.orders[] | {partition, leader, replicas, isr}'
curl -s $C https://127.0.0.1:17421/api/cluster/status | jq -c '.raft, .under_replicated_partitions, .offline_partitions'
```

Откройте консоль любого узла — `https://127.0.0.1:17421/nodes`. Браузер предупредит о недоверенном CA (`gen-dev`) — для разработки это нормально. Должны быть три узла «Работает», контроллер, 0 недореплицированных и 0 недоступных разделов. Карточка [Безопасность](/docs/security) покажет TLS на всех трёх поверхностях.

Демо-нагрузка с mTLS-сертификатом клиента:

```bash
./target/release/fluxmq-demo --seeds tls://127.0.0.1:17420,tls://127.0.0.1:17430,tls://127.0.0.1:17440 \
  --tls-ca ./certs/ca.pem --tls-cert ./certs/client.pem --tls-key ./certs/client-key.pem
```

## Вариант: Docker {#docker}

Профиль `cluster` делает всё то же самое сам: init-контейнер выпускает сертификаты `gen-dev` и случайный секрет кластера в том `certs`, затем стартуют `n1`–`n3` с RF=3, `--min-isr 2`, TLS клиентского порта, HTTPS и mTLS между узлами.

```bash
dc() { docker compose -f deploy/docker-compose.yml "$@"; }
dc --profile cluster up -d                     # консоль n1: https://localhost:37431
dc --profile cluster --profile demo up -d      # + демо-нагрузка
dc --profile cluster cp n1:/certs/ca.pem ./ca.pem && curl --cacert ca.pem https://localhost:37431/api/health
```

`FLUXMQ_CLUSTER_TLS=off` — кластер без TLS, `FLUXMQ_CLIENT_MTLS=on` — обязательный mTLS для клиентов. Адреса узлов в метаданных (`n1:7420` …) резолвятся только внутри сети compose, поэтому SDK-клиентов кластера запускайте в этой сети.

## Проверка отказоустойчивости {#resilience}

```bash
FLUXMQ_CLIENT_TLS_CA=certs/ca.pem FLUXMQ_CLIENT_TLS_CERT=certs/client.pem \
FLUXMQ_CLIENT_TLS_KEY=certs/client-key.pem RESILIENCE_PARTITIONS=6 \
  cargo run --release -p fluxmq-client --example resilience -- \
  tls://127.0.0.1:17420,tls://127.0.0.1:17430,tls://127.0.0.1:17440 120
# в другом терминале: kill -9 <pid узла>, через 15 с запустите его той же командой
```

Пример публикует нумерованные сообщения и в конце проверяет, что ни одно подтверждённое не потерялось: `acked but missing 0`. Что видно в консоли во время сбоя — [Узлы и кластер](/docs/nodes#failure).
