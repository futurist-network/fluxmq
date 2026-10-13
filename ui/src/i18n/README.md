# Console i18n (English / Русский)

A small in-house layer, no runtime dependency beyond React and zustand, which the console already uses.

- `en/<ns>.ts` hold the source dictionaries. Keys are flat `namespace.key`, and every key starts with its namespace (a test enforces this).
- `ru/<ns>.ts` hold the translations, typed `RuOf<typeof en>`. A missing or extra key fails to compile. Plural entries need `one / few / many / other`.
- `en.ts` / `ru.ts` aggregate the namespaces. `index.ts` holds the store and `t()`.

```tsx
const t = useT();                                // re-renders on language change
t('shell.nav.channels')                          // string
t('common.messages', { count: n })               // plural: Intl.PluralRules picks the form
t.rich('shell.notFound.body', { keys: <kbd>⌘K</kbd> })   // React nodes as params
```

Rules for using it:

- Outside React (toasts, validators, `lib/*`), call the plain `t()` from `src/i18n`. It reads the current language.
- `{count}` is formatted with locale grouping. Other params are inserted verbatim (ports, ids). Format numbers yourself with `lib/format.ts`.
- Domain label maps (`MODE_LABEL`, `HEALTH_LABEL`, `ALERT_KIND_LABEL`, `EVENT_KIND_LABEL`, `ROLE_LABEL`…) are `localized()` getter maps: `LABEL[kind]` translates on access.
- `lib/format.ts` formats for the active language: numbers, bytes, durations, dates and relative time ("5 минут назад").

Persistence and switching:

- The language is stored in `localStorage['fluxmq.lang']`, next to the theme (`lib/settings.ts`).
- The default comes from `navigator.languages`: `ru*` gives `ru`, anything else gives `en`.
- `setLang()` updates `<html lang>` and re-renders immediately. There are three switches: Settings → General → Language, the `EN/RU` button in the top bar, and the command palette.

Checks:

- `i18n.test.ts` checks that ru and en have the same keys, that no string is empty, that interpolation params match, that plural shapes are correct, that namespaces don't collide, and that no English sentence was copied into ru.
- `lib/format.test.ts` covers both locales.
- `lib/settings.test.ts` covers persistence and browser detection.
- The ESLint rule `i18n/no-hardcoded-ui-text` (`eslint-rules/`) runs on `src/pages`, `src/components`, `src/shell` and `App.tsx`. It fails on JSX text, labelled attributes (`title`, `placeholder`, `aria-label`, `label`…), string ternaries inside JSX and `toast()` text that aren't translated.
  - Escape hatch for a genuine technical literal: `// eslint-disable-next-line i18n/no-hardcoded-ui-text`.
  - Limitation: string literals in plain object/array data (e.g. `{ label: 'X' }` outside JSX) are not detected. Keep such labels as `TKey`s.

Not translated, on purpose:

- Channel, group and node names; JSON keys and config fields (`retention_secs`); metric names; DSL keywords and operators.
- CLI flags and commands (`--admin-token`, `fluxmq restore`); HTTP headers; code snippets; product names (Slack, Telegram, S3).
- Messages the broker itself returns for validation errors. Known `ApiError.code`s (`network`, `unauthorized`, `forbidden`, `unavailable`, `internal`, `unsupported_in_cluster`, `unsupported_single_node`, `last_admin`) get their own translated text (`errors.*`) in Russian.

## Glossary (Russian)

Use these terms consistently. Write natural, professional Russian: no word-for-word calques, use «вы» for the reader, and keep sentences short.

| English | Русский | Notes |
|---|---|---|
| channel | канал | Nav/page title: **«Каналы (топики)»**; English keeps "Channels" with a "topics" tooltip |
| stream / queue / keyed (mode) | поток / очередь / по ключу | Hint: stream ≈ топик Kafka, queue ≈ очередь RabbitMQ, keyed = строгий порядок по ключу; the mode identifier stays as is in badges |
| consumer group | группа получателей | |
| consumer | получатель | |
| instance | экземпляр | |
| producer / publish | отправитель / публикация | |
| message | сообщение | |
| payload | содержимое (payload) | |
| headers | заголовки | |
| partition | раздел (партиция) | First occurrence on a screen: «Раздел (партиция)», then «раздел» |
| replica / leader | реплика / лидер | |
| offset | смещение | |
| reset offsets | сбросить смещения | |
| ack / acknowledged | подтверждение / подтверждено | |
| lag | отставание | |
| throughput | пропускная способность | |
| latency | задержка | |
| retention | хранение | Forever → «Бессрочно» |
| replay | переигрывание (повторное чтение) | |
| retry | повтор | |
| DLQ / dead letters | Очередь недоставленных (DLQ) / недоставленные сообщения | First occurrence: full form, then «DLQ»; nav item «Недоставленные» |
| discard | удалить | |
| route / routing | маршрут / маршрутизация | |
| filter / condition / match | фильтр / условие / совпадение | |
| target channel | целевой канал | |
| webhook / delivery | вебхук / доставка | |
| signing secret | секрет подписи | |
| alert | оповещение | |
| notification / notifier | уведомление / канал уведомлений (notifier) | Not to be confused with a FluxMQ «канал» |
| rule | правило | |
| node / cluster / controller | узел / кластер / контроллер | |
| decommission | вывод узла (из кластера) | |
| rebalance | ребалансировка | |
| backup / restore | резервная копия / восстановление | |
| backup target | хранилище | |
| access token / scope | токен доступа / область действия | |
| viewer / operator / admin | наблюдатель / оператор / администратор | |
| revoke | отозвать | |
| certificate / binding | сертификат / привязка сертификата | |
| healthy / warning / critical | в норме / внимание / критично | |
| live (updates) / paused | онлайн / пауза | |
| dashboard | обзор | |
| settings | настройки | |

Formatting in Russian:

- Decimal comma, and a non-breaking space as the thousands separator.
- Compact counts: «1,2 тыс.», «1,4 млн», «2,1 млрд».
- Units: «мс», «с», «мин», «ч», «д»; bytes «Б, КБ, МБ, ГБ, ТБ»; «сообщ./с».
- Percent: «42 %».
- Dates: «7 окт., 14:03:22».
- Relative time: «только что», «5 минут назад», «через 6 секунд».
