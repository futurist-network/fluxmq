---
title: Защитить брокер
group: howto
order: 7
summary: Чек-лист перед выходом в сеть — токен администратора, токены сервисов с ролями и областями действия, TLS, mTLS и привязки сертификатов.
---

По умолчанию FluxMQ работает в **открытом режиме**: без авторизации и без шифрования. Это удобно на ноутбуке и опасно везде ещё.

> [!WARNING]
> Не открывайте порты 7420 и 7421 в сеть без токена и TLS — встроенного или TLS-прокси (nginx, Caddy) перед ними. Порты кластера 7422/7423 держите в приватной сети.

## 1. Включите авторизацию {#admin-token}

Вариант А — bootstrap-токен при запуске (не хранится в брокере, удобно для автоматизации):

```bash
./target/release/fluxmq --admin-token "$(openssl rand -hex 32)"     # или FLUXMQ_ADMIN_TOKEN=…
```

Вариант Б — из консоли: Настройки → Токены доступа → **Создать токен администратора**. Брокер сразу переходит в режим с авторизацией, консоль сохраняет токен у себя. Скопируйте секрет — он показывается один раз.

С авторизацией REST требует `Authorization: Bearer <токен>` (кроме `/api/health`), WebSocket — заголовок или `?token=`, клиенты SDK — `auth_token`, демо и бенчмарк — `--token` или `FLUXMQ_TOKEN`.

## 2. Выдайте каждому свой токен {#tokens}

| Кому | Роль | Области действия |
|---|---|---|
| Сервис, который пишет в `orders.*` и читает `payments.events` | `viewer` | публикация `orders.*`, чтение `payments.events` |
| Prometheus | `viewer` | — (читает `/metrics`) |
| Дежурный инженер | `operator` | — (DLQ, переигрывание, сброс смещений) |
| Администратор платформы | `admin` | — |

```bash
curl -s -X POST localhost:7421/api/auth/tokens \
  -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"name":"orders-svc","role":"viewer",
       "scopes":{"publish":["orders.*"],"consume":["payments.events"]},
       "ttl_secs":2592000}'
# {"token":"fmq_k3x9…_9f2c…","info":{…}}  — секрет показывается один раз
```

Токен с истёкшим сроком или отозванный обрывает живые сессии на всех узлах в течение ~1 с. Подробно — [Токены доступа и роли](/docs/access-tokens).

## 3. Включите TLS {#tls}

| Флаг | Переменная | Что делает |
|---|---|---|
| `--tls-cert`, `--tls-key` | `FLUXMQ_TLS_CERT`, `FLUXMQ_TLS_KEY` | TLS на клиентском порту и (по умолчанию) HTTPS на HTTP-порту |
| `--tls-client-ca` | `FLUXMQ_TLS_CLIENT_CA` | требовать клиентские сертификаты, подписанные этим CA (mTLS) |
| `--http-tls-cert`, `--http-tls-key` | `FLUXMQ_HTTP_TLS_CERT`, `FLUXMQ_HTTP_TLS_KEY` | отдельный сертификат для консоли и REST |
| `--http-tls on\|off` | `FLUXMQ_HTTP_TLS` | `off` — HTTP открытым текстом (например, за TLS-прокси) |
| `--cluster-tls-cert`, `--cluster-tls-key`, `--cluster-tls-ca` | `FLUXMQ_CLUSTER_TLS_CERT`, `_KEY`, `_CA` | взаимный TLS между узлами; нужны все три, только с `--peers` |

```bash
./target/release/fluxmq --data-dir ./data \
  --tls-cert ./certs/server.pem --tls-key ./certs/server-key.pem
curl --cacert ./certs/ca.pem https://localhost:7421/api/health
```

Неполные сочетания (сертификат без ключа, `--tls-client-ca` без `--tls-cert`) и битый PEM — ошибка старта, а не тихий откат на открытый текст. Сертификаты для разработки — `fluxmq tls gen-dev` (см. [Кластер с TLS](/docs/howto-cluster#certs)). Клиенты подключаются по `tls://host:7420`. Клиент без TLS на TLS-порту получает понятную ошибку `this port requires TLS`.

## 4. mTLS и привязки сертификатов {#mtls}

С `--tls-client-ca` клиент обязан предъявить сертификат, подписанный этим CA, иначе соединение рвётся на рукопожатии. CN сертификата виден в консоли рядом с адресом экземпляра (`tls:orders-svc`).

Сам по себе mTLS отвечает на вопрос «кто подключился», а права даёт токен. Чтобы сервис подключался **только по сертификату**, создайте **привязку**: Настройки → Токены доступа → Привязки сертификатов → **Новая привязка** — CN, роль и области действия.

```bash
curl -s --cacert ./certs/ca.pem -X POST https://localhost:7421/api/auth/cert-bindings \
  -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"cn":"billing-svc","role":"viewer","scopes":{"publish":["billing.*"],"consume":["billing.events"]}}'
```

В SDK тогда достаточно `TlsOptions::identity_files(…)` без `auth_token`. Привязки работают только на клиентском порту; консоль и REST по-прежнему требуют токен.

## 5. Остальное {#rest}

- **Вебхуки**: при включённой авторизации запрещены приватные адреса (защита от SSRF); разрешить — `--webhooks-allow-private`.
- **Секреты** уведомлений, вебхуков и бэкапов сервер никогда не возвращает (`********`), в журнал аудита и логи они не попадают.
- **Журнал аудита** — `GET /api/audit` и `<data-dir>/audit.log`: кто и что менял.
- **Ротация сертификатов** без перезапуска — замените файлы или отправьте `SIGHUP`, см. [Безопасность (TLS)](/docs/security#rotation).

## Чек-лист {#checklist}

- [ ] задан `--admin-token` или создан токен администратора; предупреждения «Аутентификация выключена» нет;
- [ ] у каждого сервиса свой токен с минимальной ролью и областями действия, со сроком;
- [ ] TLS на клиентском и HTTP-портах (или TLS-прокси), сертификаты от вашего CA;
- [ ] в кластере — cluster TLS и общий `--cluster-secret`, порты Raft/репликации в приватной сети;
- [ ] уведомления на `tls_cert_expiring`;
- [ ] есть второй токен администратора на случай потери первого.
