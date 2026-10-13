---
title: Быстрый старт
group: start
order: 3
summary: Установка FluxMQ одной командой, запуск из исходников или в Docker; проверка и демо-нагрузка за пару минут.
---

## Что нужно {#requirements}

| Что | Версия | Зачем |
|---|---|---|
| Rust (rustup) | 1.88+ | сборка брокера и SDK |
| Node.js и pnpm | Node 20.19+, pnpm 9 | только для сборки консоли |
| macOS или Linux | — | |
| Docker | любой свежий | если запускаете образ вместо сборки |

## Быстрее всего: готовый релиз {#install}

```bash
# Docker (amd64 / arm64)
docker run -d --name fluxmq -p 7420:7420 -p 7421:7421 -v fluxmq:/var/lib/fluxmq ghcr.io/futurist-network/fluxmq

# Linux-сервер с systemd (x86_64 / aarch64, glibc 2.36+): бинарник, /etc/fluxmq/fluxmq.toml,
# сгенерированный токен администратора (sudo cat /etc/fluxmq/admin-token) и сервис fluxmq
curl -fsSL https://raw.githubusercontent.com/futurist-network/fluxmq/master/deploy/install.sh | sudo sh
```

Консоль встроена в бинарник: откройте `http://localhost:7421`. Настройки — в `/etc/fluxmq/fluxmq.toml` (ключи — [имена флагов](/docs/ref-cli)); повторный запуск скрипта обновляет брокер и сохраняет конфиг и данные.

## Вариант 1. Из исходников (как в продакшене) {#from-source}

Команды выполняются из корня репозитория.

```bash
cd ui && pnpm install && pnpm build && cd ..        # консоль -> ui/dist, встраивается в бинарник
cargo build --release -p fluxmq-server -p fluxmq-client
./target/release/fluxmq --data-dir ./data          # брокер: TCP 7420, HTTP 7421
```

Во втором терминале — по желанию — демо-нагрузка, которая создаёт каналы, трафик и реальные проблемы (медленный воркер, зависающий получатель, сообщения в DLQ с причиной `Invalid schema`):

```bash
./target/release/fluxmq-demo            # --speed 2 — вдвое больше трафика
```

Откройте `http://localhost:7421`. Если бинарник собран раньше консоли, по этому адресу будет страница-подсказка «FluxMQ is running»: соберите консоль и пересоберите брокер (или передайте `--ui-dir ui/dist`).

## Вариант 2. Режим разработки одной командой {#dev}

```bash
scripts/dev.sh
```

Скрипт собирает брокер и демо (debug), запускает брокер с данными в `./data-dev`, ждёт `/api/health`, запускает `fluxmq-demo` и dev-сервер консоли на `http://localhost:5173` с горячей перезагрузкой. `Ctrl-C` останавливает всё. Переменные: `NO_DEMO=1`, `NO_UI=1`, `FLUXMQ_DATA_DIR=…`, `FLUXMQ_ADMIN_TOKEN=…`.

## Вариант 3. Docker {#docker}

```bash
deploy/build.sh                                                       # образ fluxmq:dev (первая сборка 6–10 мин)
docker compose -f deploy/docker-compose.yml --profile single up -d    # консоль: http://localhost:37421
docker compose -f deploy/docker-compose.yml --profile single --profile demo up -d   # + демо-нагрузка
docker compose -f deploy/docker-compose.yml --profile '*' down -v     # остановить всё и удалить данные
```

Или одним контейнером:

```bash
docker run --rm -p 127.0.0.1:37420:7420 -p 127.0.0.1:37421:7421 \
  -v fluxmq-data:/var/lib/fluxmq fluxmq:dev
```

В образе есть `fluxmq`, `fluxmq-bench` и `fluxmq-demo`; данные — в томе `/var/lib/fluxmq`, процесс работает от пользователя `fluxmq` (uid 10001), healthcheck — `/api/health`. Порты на хосте в compose — из диапазона 3xxxx и только на `127.0.0.1`. Кластер из трёх узлов с TLS — профиль `cluster`, см. [Кластер из трёх узлов с TLS](/docs/howto-cluster).

## Проверка {#verify}

```bash
curl -s http://localhost:7421/api/health
# {"status":"ok","version":"0.7.0","node_id":"...","uptime_secs":12}

curl -s http://localhost:7421/api/channels | head -c 300
```

## Что дальше {#next}

1. Посмотрите [навигацию по консоли](/docs/console-basics) и [дашборд](/docs/dashboard).
2. [Создайте канал](/docs/howto-create-channel) и [подключите клиента](/docs/howto-connect-client).
3. Прежде чем открывать брокер в сеть, [защитите его](/docs/howto-secure): по умолчанию авторизация выключена и любой может им управлять.

> [!WARNING]
> Без `--admin-token` (или созданного токена) брокер работает в открытом режиме: REST, консоль и клиентский порт доступны всем как администратору. Наверху консоли в этом режиме висит предупреждение «Auth disabled».
