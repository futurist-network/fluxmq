---
title: Язык фильтров
group: reference
order: 5
summary: Условия для поиска сообщений, маршрутов и вебхуков — поля, операторы, правила сравнения и примеры.
---

Один и тот же язык используется в поиске сообщений (вкладка «Сообщения» канала, параметр `filter` в REST), в условиях WHEN [маршрутов](/docs/routing) и в фильтре [вебхуков](/docs/webhooks). Условия объединяются через `AND`; `OR` не поддерживается — для «или» заведите несколько маршрутов. Ключевые слова пишутся в любом регистре.

## Поля {#fields}

| Поле | Что это |
|---|---|
| `key` | ключ сообщения |
| `partition` | номер раздела |
| `header.<имя>` | заголовок, например `header.flux-type` |
| `a.b.0.c` | путь в JSON-содержимом; числа — индексы массивов |
| `payload.<путь>` | явный путь в содержимом. **Обязателен** для полей с именами `key` и `partition`: без префикса это ключ и раздел сообщения, а `payload.key` — поле `key` в JSON |

## Операторы {#operators}

| Оператор | Пример |
|---|---|
| `=`, `!=` | `country = MD`, `status != cancelled` |
| `>`, `>=`, `<`, `<=` | `amount >= 100`, `created_at >= "2026-10-01"` |
| `in [..]`, `not in [..]` | `country in [MD, RO]` |
| `prefix` | `header.flux-type prefix payment.` |
| `contains` | `tags contains vip` (подстрока в строке или элемент массива) |
| `exists`, `not exists` | `header.x-trace-id exists` |

## Правила сравнения {#rules}

- числа сравниваются как числа, строки — лексикографически;
- число равно строке с этим числом: `order_id = 8842` найдёт и `8842`, и `"8842"`;
- строки без пробелов можно не брать в кавычки (`country = MD`);
- отсутствующее поле не подходит ни под какой оператор, кроме `!=`, `not in` и `not exists`;
- условия по содержимому никогда не срабатывают, если оно не JSON.

## Примеры {#examples}

```text
order_id = 8842
type = "order.created" AND country = "MD" AND amount >= 100
country in [MD, RO] AND header.flux-type prefix payment.
customer.tier != gold AND items.0.sku = "A-1"
tags contains vip
header.x-trace-id exists
key prefix "user-" AND partition = 3
created_at >= "2026-10-01"
```

## Ошибки {#errors}

Синтаксическая ошибка возвращается с позицией в байтах — консоль подчёркивает это место:

```bash
curl -s -X POST localhost:7421/api/filter/parse -H 'content-type: application/json' -d '{"filter":"country ~ MD"}'
# {"error":"bad_request","message":"filter syntax error at 8: unexpected character '~'","position":8}
```

`POST /api/filter/parse` превращает текст в условия (так делает редактор маршрутов), `POST /api/filter/render` — условия обратно в текст.

## Производительность {#performance}

Индекса по содержимому нет: поиск просматривает сообщения, пока не наберёт нужное число совпадений, но не больше 200 000 за запрос (порядка 2 млн сообщений в секунду, то есть ~0,1 с). В ответе `scanned` — сколько просмотрено, `has_more: true` — просмотр остановился раньше конца диапазона: сузьте время, раздел или ключ. Поиск по `key` без раздела смотрит только раздел этого ключа.
