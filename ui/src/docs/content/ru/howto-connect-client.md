---
title: Подключить клиента (Rust SDK)
group: howto
order: 3
summary: Отправитель и получатель на Rust SDK — публикация, пачки и сжатие (0.7), подтверждения, режимы подтверждения, токен, TLS и кластер.
---

Официальный SDK — `fluxmq-client` (Rust, tokio). Он говорит по бинарному протоколу на порту 7420, сам переподключается, в кластере публикует прямо лидерам разделов и раз в 2 секунды отправляет брокеру статистику обработки — её видно на экране [Получатели](/docs/consumers). Для языков без SDK используйте REST (`POST /api/channels/{имя}/publish`) или [вебхуки](/docs/webhooks) для доставки.

## Подключение зависимостей {#deps}

SDK пока подключается по пути (в crates.io не опубликован):

```toml
[dependencies]
fluxmq-client = { path = "../FluxMQ/crates/fluxmq-client" }
tokio = { version = "1", features = ["full"] }
futures = "0.3"
```

## Отправитель {#producer}

```rust
use fluxmq_client::{message, ChannelConfig, ChannelMode, Client, ClientOptions};

#[tokio::main]
async fn main() -> fluxmq_client::Result<()> {
    let mut opts = ClientOptions::new("127.0.0.1:7420", "order-api-1");
    opts.auth_token = std::env::var("FLUXMQ_TOKEN").ok();     // если включена авторизация
    let client = Client::connect(opts).await?;

    client.ensure_channel(ChannelConfig::new("orders", ChannelMode::Stream)).await?;

    let producer = client.producer("orders");
    let id = producer
        .send(
            message(r#"{"order_id":8842,"amount":129.9}"#)
                .key("8842")                  // раздел и порядок — по ключу
                .event_type("OrderCreated")   // заголовок flux-type, виден в консоли
                .content_type("application/json")
                .new_trace_id(),              // flux-trace-id
        )
        .await?;
    println!("partition {} offset {}", id.partition, id.offset);
    Ok(())
}
```

`send` возвращается **после** того, как сообщение записано на диск (и, в кластере, реплицировано на ISR). Заголовок `flux-producer` с именем клиента ставится автоматически (отключается `ClientOptions::stamp_producer(false)`).

## Высокая нагрузка: пачки и сжатие {#batching}

`BatchingProducer` сам собирает сообщения каждого раздела в пачки и возвращает future на каждое сообщение. Linger адаптивный: если по разделу ничего не в полёте, сообщение уходит сразу; пачка копится, только пока предыдущий запрос раздела не подтверждён — задержка при малой нагрузке не растёт.

```rust
use fluxmq_client::{message, BatchingOptions, Client, ClientOptions, Compression};
use futures::stream::{FuturesUnordered, StreamExt};

let client = Client::connect(
    ClientOptions::new("127.0.0.1:7420", "ingest-1").compression(Compression::Lz4),
).await?;
let producer = client.batching_producer("clicks", BatchingOptions::default());

let mut pending = FuturesUnordered::new();
for i in 0..100_000u32 {
    // send() ждёт только постановки в очередь; future завершится после записи на диск
    pending.push(producer.send(message(format!(r#"{{"n":{i}}}"#)).key(format!("user-{}", i % 500))).await?);
}
while let Some(r) = pending.next().await {
    r?; // MessageId; ошибка — сообщение не сохранено
}
producer.close().await; // дождаться отправки всего, что в очереди
```

| `BatchingOptions` | По умолчанию | Смысл |
|---|---|---|
| `linger` | 1 мс | сколько пачка может ждать попутчиков, пока у раздела уже есть пачка в полёте |
| `max_batch` | 1000 | сообщений в пачке |
| `max_batch_bytes` | 1 MiB | байт в пачке (держите сильно меньше 16 MiB — предела кадра) |
| `max_in_flight` | 8 | запросов в полёте на все разделы |
| `queue_capacity` | 16 384 | очередь раздела; `send` ждёт, когда она полна (backpressure) |
| `ordered` | `false` | `true` — не больше одной пачки в полёте на раздел: порядок по ключу сохраняется и при повторах |

С брокером 0.7+ (протокол v4) каждая пачка кодируется один раз в **пачку записей** (`PublishBatch`), брокер дописывает её в лог как есть. Надёжность та же: ответ — после fsync и репликации.

**Сжатие** — `ClientOptions::compression(…)`, для каждой пачки:

| Вариант | Когда | Цена |
|---|---|---|
| `Compression::None` (по умолчанию) | маленькие или уже сжатые/зашифрованные данные, минимум задержки | — |
| `Compression::Lz4` | JSON, логи, текст; узкое место — сеть или диск | немного CPU отправителя, распаковка очень дешёвая |
| `Compression::Zstd` | максимум экономии трафика и места (репликация, долгое хранение) | больше CPU у обеих сторон |

Брокер хранит, реплицирует и отдаёт пачку сжатой, распаковывая только для фильтров, просмотра и поиска. Пачка, которая не уменьшилась, уходит несжатой. С брокерами до 0.7 настройка игнорируется.

## Защита от дублей при отправке {#idempotent}

Если ответ брокера потерялся (обрыв сети, таймаут, смена лидера), SDK повторяет отправку. Раньше такой повтор мог записать пачку второй раз. С брокером v0.12 этого не происходит: SDK помечает каждую пачку номером клиента и номером пачки (заголовок `flux-pseq` у первого сообщения, около 30 байт на пачку), а брокер на повтор уже записанной пачки отвечает теми же номерами сообщений и ничего не пишет.

- Включено по умолчанию; выключить — `ClientOptions::idempotent(false)`.
- Работает и при смене лидера: новый лидер восстанавливает таблицу из последней минуты своего журнала.
- Повтор, пока первая отправка ещё пишется на диск, ждёт её и получает тот же ответ.
- Защищает от повторов самого SDK. Если приложение само отправит то же сообщение заново, это новая пачка — такие повторы отличайте по своему id в заголовке.
- Сколько повторов отброшено — метрика `fluxmq_channel_publish_duplicates_total`.

## Получатель {#consumer}

```rust
use fluxmq_client::{Client, ClientOptions, StartFrom, SubscribeOptions};
use futures::StreamExt;

let client = Client::connect(ClientOptions::new("127.0.0.1:7420", "billing-worker-1")).await?;
let mut consumer = client
    .subscribe("orders", "billing",
        SubscribeOptions::default().prefetch(32).start(StartFrom::Earliest))
    .await?;

while let Some(delivery) = consumer.next().await {
    let delivery = delivery?;   // Err — только неисправимые ошибки (Forbidden, канал удалён, клиент закрыт)
    match handle(delivery.payload()).await {
        Ok(()) => delivery.ack().await?,
        // причина видна в DLQ и группирует ошибки — пишите коротко и стабильно
        Err(e) if e.is_transient() => delivery.nack("PostgreSQL timeout", true).await?,   // повторить
        Err(_) => delivery.nack("Invalid schema", false).await?,                           // сразу в DLQ
    }
}
```

- `"billing"` — **группа получателей**. Запустите несколько экземпляров с одной группой — они поделят работу; другая группа получит все сообщения заново.
- `prefetch` (по умолчанию 64) — сколько неподтверждённых сообщений одновременно у экземпляра.
- `start` действует только для **новой** группы: `Earliest`, `Latest` (по умолчанию) или `TimestampMs(…)`.
- У доставки есть `key()`, `payload()`, `headers()`, `header(…)`, `attempt()` (номер попытки).
- Подтверждения копятся и уходят пачкой: каждые `ack_batch` штук (64, но не больше половины `prefetch`) или через `ack_linger` (1 мс). `nack` уходит сразу. Настройка: `SubscribeOptions::default().ack_batch(128).ack_linger(Duration::from_millis(2))`.

## Режимы подтверждения {#ack-modes}

| Режим | Как включить | Что работает | Когда выбирать |
|---|---|---|---|
| `AckMode::Individual` (по умолчанию) | — | ack/nack каждого сообщения, повторы, тайм-аут подтверждения, DLQ | почти всегда |
| `AckMode::Cumulative` | `SubscribeOptions::default().cumulative()` | фиксация позиции в разделе; **без** nack, тайм-аута и DLQ (`nack` вернёт `BadRequest`) | только `stream`-каналы, аналитика и потоковая обработка, где важна максимальная скорость |

В режиме `Individual` подряд идущие подтверждения уходят диапазонами (`AckRanges`) — подтверждение целой пачки стоит несколько байт. В режиме `Cumulative` `delivery.ack()` фиксирует всё до этого сообщения включительно в его разделе, а `consumer.commit().await` — всё полученное, дожидаясь ответа брокера. После сбоя доставка продолжится с последней зафиксированной позиции — всё, что после неё, придёт повторно. Подтверждайте сообщение только после обработки всех предыдущих в том же разделе.

```rust
let mut consumer = client
    .subscribe("clicks", "analytics", SubscribeOptions::default().prefetch(2000).cumulative())
    .await?;
while let Some(d) = consumer.next().await {
    let d = d?;
    process(&d).await?;
    d.ack().await?;   // фиксирует раздел до d включительно
}
```

Прочие опции: `prefetch_bytes(n)` — ограничить неподтверждённое и в байтах; `verify_crc(false)` — не проверять CRC-32C пачек (проверка аппаратная и почти бесплатна); `raw_records(false)` — получать по одному сообщению, как в 0.6.

## Токен, TLS и кластер {#auth-tls}

```rust
use std::time::Duration;
use fluxmq_client::{Client, ClientOptions, ReconnectPolicy, TlsOptions};

let tls = TlsOptions::new()
    .ca_file("certs/ca.pem")                                       // CA брокера
    .identity_files("certs/client.pem", "certs/client-key.pem");   // для mTLS
let client = Client::connect(
    ClientOptions::with_seeds(["tls://n1:7420", "tls://n2:7420", "tls://n3:7420"], "order-api-1")
        .auth_token(std::env::var("FLUXMQ_TOKEN").unwrap())      // не нужен при привязке сертификата
        .tls(tls)
        .reconnect(ReconnectPolicy::default().backoff(Duration::from_millis(100), Duration::from_secs(5)))
        .publish_timeout(Duration::from_secs(30)),
).await?;
```

- **Несколько адресов (seeds)**: клиент загружает метаданные кластера, публикует прямо лидерам разделов и читает со всех узлов-лидеров одним потоком.
- **Переподключение** с экспоненциальной паузой и разбросом; `client.state()` — состояние соединения (`Connecting` → `Connected` ⇄ `Reconnecting` → `Closed`).
- **Повторы публикации**: обрыв, таймаут, `NotLeader` и `Overloaded` повторяются до `publish_timeout`; `BadRequest`, `Forbidden`, `Unauthorized` — никогда.
- `tls://host:port` без `TlsOptions` — TLS с системным хранилищем сертификатов (публичный CA).

> [!WARNING]
> **Повтор публикации может создать дубликат**: брокер мог сохранить пачку, а ответ потерялся с соединением. Доставка at-least-once — делайте обработчики идемпотентными, например по бизнес-ключу.

## Совместимость версий {#compat}

SDK 0.7 работает с брокерами 0.4–0.7: предлагает протокол v4 и, если брокер старше, сразу переходит на версию, которую тот поддерживает (запоминает на 5 минут). API одинаковый на всех версиях; пачки записей, сжатие и cumulative дают выигрыш только с брокером 0.7+.

Готовые примеры: `cargo run -p fluxmq-client --example producer -- 127.0.0.1:7420 10` и `--example consumer -- 127.0.0.1:7420`.
