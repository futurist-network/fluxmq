---
title: Флаги и переменные окружения
group: reference
order: 1
summary: Все параметры запуска fluxmq (из crates/fluxmq-server/src/config.rs) — флаг, переменная окружения, значение по умолчанию — и подкоманды backup, restore, tls.
---

Каждый параметр задаётся флагом, переменной окружения или ключом в TOML-файле `--config` (ключ — имя флага: `data-dir = "/var/lib/fluxmq"`). Флаг важнее переменной, переменная важнее файла; неизвестный ключ в файле — ошибка. Пустое значение переменной (`FLUXMQ_TLS_CERT=`) считается незаданным. Полный список с описаниями — `fluxmq --help`.

## Основные {#general}

| Флаг | Переменная | По умолчанию | Описание |
|---|---|---|---|
| `--config` | `FLUXMQ_CONFIG` | — | TOML-файл с любыми из этих параметров, ключ — имя флага (пример: `deploy/fluxmq.toml`) |
| `--data-dir` | `FLUXMQ_DATA_DIR` | `./data` | каталог данных: сообщения, смещения, DLQ, журнал аудита |
| `--client-addr` | `FLUXMQ_CLIENT_ADDR` | `0.0.0.0:7420` | адрес клиентского протокола (TCP) |
| `--http-addr` | `FLUXMQ_HTTP_ADDR` | `0.0.0.0:7421` | адрес HTTP: REST, WebSocket-телеметрия, консоль |
| `--cluster-name` | `FLUXMQ_CLUSTER_NAME` | `local` | имя кластера в консоли |
| `--ui` | `FLUXMQ_UI` | `on` | веб-консоль на HTTP-порту; `off` — только API |
| `--ui-dir` | `FLUXMQ_UI_DIR` | — | раздавать консоль из каталога вместо встроенной в бинарник |
| `--admin-token` | `FLUXMQ_ADMIN_TOKEN` | — | bootstrap-токен администратора; включает авторизацию для REST, WebSocket и клиентского протокола |
| `--admin-token-file` | `FLUXMQ_ADMIN_TOKEN_FILE` | — | прочитать токен администратора из файла (секреты Docker / Kubernetes, credentials systemd) |
| `--log` | `FLUXMQ_LOG` | `RUST_LOG`, иначе `info` | фильтр логов, например `info,fluxmq_core=debug` |
| `--public-url` | `FLUXMQ_PUBLIC_URL` | — | публичный адрес консоли (`https://mq.example.com`) для ссылок в уведомлениях |
| `--webhooks-allow-private` | `FLUXMQ_WEBHOOKS_ALLOW_PRIVATE` | не задано | `true`/`false`: разрешить вебхукам loopback-, приватные и link-local-адреса. Не задано — разрешено в открытом режиме, запрещено при включённой авторизации |

## Хранилище и производительность {#storage}

| Флаг | Переменная | По умолчанию | Описание |
|---|---|---|---|
| `--fsync` | `FLUXMQ_FSYNC` | `group-commit` | `group-commit` — подтверждение после fsync (пакеты до 2 мс); `os` — после записи в ОС, быстрее, но теряет данные при сбое питания |
| `--segment-bytes` | `FLUXMQ_SEGMENT_BYTES` | `67108864` (64 MiB) | размер сегмента, от 1 MiB до 2 GiB; каждый раздел закрывает сегмент в пределах ±10 %; действует на новые сегменты |
| `--fsync-threads` | `FLUXMQ_FSYNC_THREADS` | `0` | размер общего пула потоков fsync; 0 — вдвое больше числа CPU, от 4 до 32 |
| `--commit-pipeline` | `FLUXMQ_COMMIT_PIPELINE` | `on` | `on` (по умолчанию с 0.9): следующие пакеты пишутся, пока предыдущие fsync'аются; каждый раздел подтверждается только после своего fsync и по порядку. `off` — ждать fsync каждого пакета (как в 0.8) |
| `--commit-pipeline-depth` | `FLUXMQ_COMMIT_PIPELINE_DEPTH` | `1` | сколько пакетов группового коммита канала может быть в полёте при включённом конвейере (1–16) |
| `--partition-cache-bytes` | `FLUXMQ_PARTITION_CACHE_BYTES` | `16777216` (16 MiB) | байт недавних пачек в памяти на раздел, чтобы догнавшие получатели читали без диска; 0 — выключить |
| `--delivery-threads` | `FLUXMQ_DELIVERY_THREADS` | `2` | потоки отдельного рантайма доставки (кодирование и запись в сокеты), чтобы получатели не голодали при перегрузке отправителями; 0 — общий рантайм (как в 0.6) |


## Живые подписки (v0.10) {#live}

| Флаг | Переменная | По умолчанию | Описание |
|---|---|---|---|
| `--live-max-connections` | `FLUXMQ_LIVE_MAX_CONNECTIONS` | `10000` | соединений живых подписок (WebSocket `/live` + SSE `/events`) на узел; сверх — 503 |
| `--live-max-connections-per-token` | `FLUXMQ_LIVE_MAX_CONNECTIONS_PER_TOKEN` | `1000` | соединений на один токен; сверх — 429 (bootstrap-админ и открытый режим не ограничены) |
| `--live-max-connections-per-channel` | `FLUXMQ_LIVE_MAX_CONNECTIONS_PER_CHANNEL` | `10000` | подписок на канал на этом узле; сверх — 429 |
| `--live-max-subscriptions` | `FLUXMQ_LIVE_MAX_SUBSCRIPTIONS` | `16` | подписок в одном WebSocket-соединении |
| `--live-max-msgs-per-sec` | `FLUXMQ_LIVE_MAX_MSGS_PER_SEC` | `10000` | сообщений в секунду на соединение; притормаживает, ничего не теряет; 0 — без лимита |
| `--live-max-bytes-per-sec` | `FLUXMQ_LIVE_MAX_BYTES_PER_SEC` | `16777216` (16 MiB) | байт в секунду на соединение; притормаживает; 0 — без лимита |
| `--live-max-payload-bytes` | `FLUXMQ_LIVE_MAX_PAYLOAD_BYTES` | `65536` | более длинные payload обрезаются в событиях (`truncated: true`, полный `size` указан) |
| `--live-max-batch` | `FLUXMQ_LIVE_MAX_BATCH` | `256` | сообщений в одном событии `messages` |
| `--live-send-buffer` | `FLUXMQ_LIVE_SEND_BUFFER` | `64` | событий в очереди на отправку на соединение |
| `--live-send-timeout` | `FLUXMQ_LIVE_SEND_TIMEOUT` | `10` | секунд, которые клиент может не читать соединение, прежде чем его закроют как медленного |
| `--live-heartbeat` | `FLUXMQ_LIVE_HEARTBEAT` | `15` | интервал WebSocket ping / SSE heartbeat, секунды |
| `--live-idle-timeout` | `FLUXMQ_LIVE_IDLE_TIMEOUT` | `60` | WebSocket: закрыть, если столько секунд ничего не приходило (даже pong) |
| `--live-fanout-buffer` | `FLUXMQ_LIVE_FANOUT_BUFFER` | `1024` | пачек в общем буфере рассылки на канал |
| `--live-allowed-origins` | `FLUXMQ_LIVE_ALLOWED_ORIGINS` | — | разрешённые Origin сайтов кроме своего, через запятую (`https://app.example.com`) или `*` |

## Кластер {#cluster}

Без `--peers` брокер — одиночный узел.

| Флаг | Переменная | По умолчанию | Описание |
|---|---|---|---|
| `--node-id` | `FLUXMQ_NODE_ID` | — | id узла; обязателен с `--peers`, перекрывает id, сохранённый в каталоге данных |
| `--peers` | `FLUXMQ_PEERS` | — | начальные голосующие узлы `id@raft_host:port,…` (может включать сам узел); узел не из списка присоединяется через них learner'ом |
| `--raft-addr` | `FLUXMQ_RAFT_ADDR` | `0.0.0.0:7422` | Raft и внутренние RPC |
| `--replication-addr` | `FLUXMQ_REPLICATION_ADDR` | `0.0.0.0:7423` | репликация разделов |
| `--advertise-host` | `FLUXMQ_ADVERTISE_HOST` | IP прослушивания, для `0.0.0.0` — `127.0.0.1` | адрес, по которому узел видят другие узлы и клиенты; на нескольких машинах задайте обязательно |
| `--replication-factor` | `FLUXMQ_REPLICATION_FACTOR` | `3` | число реплик новых каналов (не больше числа живых узлов) |
| `--min-isr` | `FLUXMQ_MIN_ISR` | `1` | минимум синхронных реплик для подтверждения публикации, иначе `Overloaded` |
| `--cluster-secret` | `FLUXMQ_CLUSTER_SECRET` | — | общий секрет межузлового трафика; обязателен с `--peers`, одинаковый на всех узлах |
| `--cluster-secret-file` | `FLUXMQ_CLUSTER_SECRET_FILE` | — | прочитать секрет кластера из файла |
| `--auto-rebalance` | `FLUXMQ_AUTO_REBALANCE` | `full` | `off` / `repair` / `full` — режим ребалансировки, пока в кластере нет сохранённых настроек; дальше источник истины — `PUT /api/cluster/rebalance/config` |

## TLS {#tls}

Без этих флагов все порты работают открытым текстом.

| Флаг | Переменная | Описание |
|---|---|---|
| `--tls-cert` | `FLUXMQ_TLS_CERT` | PEM-цепочка: TLS на клиентском порту и (если не переопределено и не `--http-tls off`) на HTTP; нужен `--tls-key` |
| `--tls-key` | `FLUXMQ_TLS_KEY` | закрытый ключ для `--tls-cert` |
| `--tls-client-ca` | `FLUXMQ_TLS_CLIENT_CA` | CA клиентских сертификатов: требовать mTLS на клиентском порту |
| `--http-tls-cert` | `FLUXMQ_HTTP_TLS_CERT` | отдельная цепочка для HTTP-порта (по умолчанию `--tls-cert`) |
| `--http-tls-key` | `FLUXMQ_HTTP_TLS_KEY` | ключ для `--http-tls-cert` |
| `--http-tls` | `FLUXMQ_HTTP_TLS` | `on` / `off`; по умолчанию TLS, если есть сертификат; `off` — HTTP открытым текстом (за TLS-прокси) |
| `--cluster-tls-cert` | `FLUXMQ_CLUSTER_TLS_CERT` | сертификат узла для взаимного TLS между узлами; нужны также ключ и CA, одинаково на всех узлах |
| `--cluster-tls-key` | `FLUXMQ_CLUSTER_TLS_KEY` | ключ для `--cluster-tls-cert` |
| `--cluster-tls-ca` | `FLUXMQ_CLUSTER_TLS_CA` | CA, которым подписаны сертификаты всех узлов |

## Подкоманды {#subcommands}

Без подкоманды `fluxmq` запускает брокер.

| Команда | Что делает |
|---|---|
| `fluxmq backup list` | список бэкапов в хранилище, сначала новые (`--json` — в JSON) |
| `fluxmq backup run` | бэкап каталога данных **остановленного** брокера: `--data-dir`, `--encryption-key`, `--no-messages` |
| `fluxmq restore` | восстановить бэкап в пустой каталог: `--backup <id\|latest>`, `--data-dir`, `--encryption-key` |
| `fluxmq tls gen-dev` | выпустить учебный CA и сертификаты сервера, узлов и клиента (только для разработки) |

**Хранилище для `backup` и `restore`:**

| Флаг | Переменная | По умолчанию | Описание |
|---|---|---|---|
| `--fs-path` | `FLUXMQ_BACKUP_FS_PATH` | — | каталог в файловой системе (вместо S3) |
| `--s3-bucket` | `FLUXMQ_BACKUP_S3_BUCKET` | — | бакет S3 |
| `--s3-endpoint` | `FLUXMQ_BACKUP_S3_ENDPOINT` | — | свой endpoint (MinIO, R2, B2…); пусто — AWS |
| `--s3-region` | `FLUXMQ_BACKUP_S3_REGION` | `us-east-1` | регион (`auto` для R2) |
| `--s3-prefix` | `FLUXMQ_BACKUP_S3_PREFIX` | пусто | префикс ключей в бакете |
| `--s3-path-style` | `FLUXMQ_BACKUP_S3_PATH_STYLE` | выкл. | path-style адресация (MinIO) |
| `--s3-access-key-id` | `AWS_ACCESS_KEY_ID` | — | ключ доступа |
| `--s3-secret-access-key` | `AWS_SECRET_ACCESS_KEY` | — | секретный ключ |
| `--encryption-key` | `FLUXMQ_BACKUP_ENCRYPTION_KEY` | — | ключ шифрования (base64, 32 байта) для `backup run` и `restore` |

**`fluxmq tls gen-dev`:** `--out <каталог>` (обязателен), `--nodes n1,n2,…` (сертификаты узлов `node-<id>.pem`, CN = id), `--hosts` (SAN, по умолчанию `localhost,127.0.0.1`), `--force` (перезаписать файлы), `--valid-days N` (по умолчанию 397), `--reuse-ca` (выпустить от существующего `ca.pem`, не меняя CA).

## Клиенты: demo и bench {#tools}

`fluxmq-demo` и `fluxmq-bench` принимают `--addr` (`FLUXMQ_ADDR`, по умолчанию `127.0.0.1:7420`), `--seeds` (`FLUXMQ_SEEDS`), `--token` (`FLUXMQ_TOKEN`), `--tls-ca`, `--tls-cert`, `--tls-key` (`FLUXMQ_CLIENT_TLS_CA`, `FLUXMQ_CLIENT_TLS_CERT`, `FLUXMQ_CLIENT_TLS_KEY`) и адреса `tls://`.

## Порты {#ports}

| Порт | Протокол | Для чего |
|---|---|---|
| 7420 | TCP, бинарный протокол (TLS с `--tls-cert`) | отправители и получатели |
| 7421 | HTTP или HTTPS | REST `/api/*`, WebSocket `/ws`, `/metrics`, консоль |
| 7422 | HTTP+JSON (mTLS с cluster TLS) | Raft и внутренние RPC — только кластер |
| 7423 | TCP (mTLS с cluster TLS) | репликация разделов — только кластер |
