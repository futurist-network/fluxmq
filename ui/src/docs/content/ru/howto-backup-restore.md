---
title: Резервное копирование и восстановление
group: howto
order: 9
summary: Настроить бэкапы в S3 или на диск, проверить их и восстановить брокер командой fluxmq restore.
---

## 1. Настройте хранилище {#configure}

**В консоли**: Настройки → Резервные копии → «Хранилище и расписание»: S3-совместимое (шаблоны AWS S3 / MinIO / Cloudflare R2) или путь на диске → **Проверить подключение** → интервал и сколько хранить → при желании шифрование (**Сгенерировать**, сохранить ключ в менеджере секретов, отметить «Я сохранил ключ…») → **Сохранить настройки**. Подробно о полях — [Резервные копии](/docs/backups).

**Через REST** (роль администратора):

```bash
# Каталог на диске
curl -X PUT http://localhost:7421/api/backups/config \
  -H 'Content-Type: application/json' -H "Authorization: Bearer $ADMIN" \
  -d '{"enabled": true,
       "target": {"type": "filesystem", "path": "/var/backups/fluxmq"},
       "interval_secs": 3600, "keep_last": 24, "keep_days": 7,
       "include_messages": true, "encryption_key": null}'

# Проверить хранилище, ничего не сохраняя
curl -X POST http://localhost:7421/api/backups/test-target \
  -H 'Content-Type: application/json' -H "Authorization: Bearer $ADMIN" \
  -d '{"type": "filesystem", "path": "/var/backups/fluxmq"}'
```

> [!WARNING]
> Не направляйте два брокера (или брокер и `fluxmq backup run`) в один и тот же префикс хранилища: сборка мусора одного может удалить объекты другого.

## 2. Запустите и проверьте {#run}

**Запустить** в консоли (или `POST /api/backups/run`). Первый бэкап загружает всё, следующие — только новые сегменты. Первый плановый запуск — через минуту после старта брокера.

```bash
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:7421/api/backups/status   # прогресс
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:7421/api/backups          # история
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:7421/api/backups/latest   # состав последнего
```

## 3. Восстановите {#restore}

Восстановление выполняется при **остановленном** брокере и всегда в **пустой** (или ещё не существующий) каталог. Готовую команду для конкретного бэкапа показывает кнопка **Восстановить…** в истории.

1. Посмотрите, какие бэкапы есть:

   ```bash
   fluxmq backup list --fs-path /var/backups/fluxmq
   # S3 (ключи — из окружения AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY):
   fluxmq backup list --s3-bucket fluxmq-bk --s3-endpoint http://minio.local:9000 --s3-prefix prod --s3-path-style
   ```

2. Восстановите нужный (`latest` — самый свежий):

   ```bash
   fluxmq restore --fs-path /var/backups/fluxmq --backup latest --data-dir /var/lib/fluxmq-restored
   ```

   Для зашифрованного бэкапа добавьте `--encryption-key <base64>` или `FLUXMQ_BACKUP_ENCRYPTION_KEY`; с чужим ключом restore сразу остановится и покажет отпечатки ключей. Каждый сегмент проверяется по CRC; при ошибке удалите каталог и повторите.

3. Запустите брокер на восстановленном каталоге:

   ```bash
   fluxmq --data-dir /var/lib/fluxmq-restored
   ```

Группы получателей продолжат с сохранённых смещений (хвост может прийти повторно — at-least-once); DLQ, маршруты, токены, каналы уведомлений, вебхуки и настройки каналов — на месте. Настройки самих бэкапов не восстанавливаются — задайте их заново.

| Хранилище | Флаги |
|---|---|
| Каталог | `--fs-path /mnt/backup` |
| AWS S3 | `--s3-bucket my-fluxmq-backups --s3-region eu-central-1 --s3-prefix prod` |
| MinIO | `--s3-bucket fluxmq --s3-endpoint http://minio.local:9000 --s3-path-style` |
| Cloudflare R2 | `--s3-bucket fluxmq --s3-endpoint https://<account_id>.r2.cloudflarestorage.com --s3-region auto --s3-path-style` |

## Кластер {#cluster}

В кластере бэкап координирует контроллер, а каждый раздел загружает его лидер. **Restore всегда создаёт одиночный узел** — запустите его, затем при необходимости добавьте узлы, как описано в статье [Добавить или убрать узел](/docs/howto-nodes#add).

## Бэкап остановленного брокера {#offline}

Перед переездом, без консоли:

```bash
fluxmq backup run --data-dir /var/lib/fluxmq --fs-path /mnt/backup   # брокер должен быть остановлен!
```

Есть ещё `--encryption-key` и `--no-messages`. Ротация при этом не выполняется. Самый простой вариант для остановленного брокера — скопировать каталог данных целиком.
