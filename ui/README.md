# FluxMQ web console (`ui/`)

Vite + React 19 + TypeScript (strict). Talks to `fluxmq-api` over REST (`/api/*`) and one
WebSocket (`/ws`, telemetry contract v1). Types mirror `crates/fluxmq-proto` in
`src/api/types.ts`.

## Develop against the mock (no Rust needed)

```bash
pnpm install
pnpm mock            # mock broker on :7421 (REST + WS, simulated cluster)
pnpm dev             # http://localhost:5173, proxies /api and /ws to :7421
```

Mock env vars: `MOCK_SCENARIO=empty` (no channels, single node, for empty states) |
`single` (demo data on one node) | `cluster` (3 nodes; n3 goes down 12 s after start, partitions
fail over, it rejoins and leadership moves back — 240 s cycle); default is a healthy 3-node
cluster. `MOCK_ROLE=viewer|operator|admin` (auth enforced with seeded tokens; requests act as that
role — try the role-aware UI), `MOCK_ADMIN_TOKEN=…` (bootstrap token, enforced auth). Without
either the mock starts in open mode (no tokens) — creating the first token enforces auth, like the
broker. `MOCK_FAST=1` (`audit.logs` appears after 8 s instead of 45 s), `MOCK_DROP_SEQ=1` (drops
~0.5% of deltas to exercise seq-gap resync), `PORT`. The mock implements every v0.2 endpoint
(auth incl. the first-token rules and `adopt`, routes + backtest + stats, `POST /api/filter/parse`
and `POST /api/filter/render`, message `filter=` (syntax errors carry `ApiError.position`),
`DELETE /api/channels/{name}/groups/{group}` (409 with subscribers or for a `$router` group still
used by a route; `orders.priority/legacy-export` is a seeded idle group), cluster
status/drain/reassign, `/metrics`). Its filter DSL is a port of
`crates/fluxmq-router/src/filter.rs` (`mock/dsl.ts`, evaluation in `mock/filter.ts`) — the console
itself has no DSL code: it always asks the server to parse and render filters.

## Develop against the real server

```bash
cargo run -p fluxmq-server                  # HTTP on :7421
cargo run -p fluxmq-client --bin fluxmq-demo  # optional demo traffic
pnpm dev                                    # same proxy, same UI
FLUXMQ_URL=http://host:7421 pnpm dev        # a server elsewhere
```

## Build

```bash
pnpm build      # tsc + vite → ui/dist (served by fluxmq at --ui-dir, SPA fallback)
pnpm typecheck  # app + node (mock/scripts) projects
pnpm lint       # eslint (typescript-eslint, react-hooks incl. compiler rules)
pnpm test       # vitest: merge patch, telemetry client, formatters, selectors, route rows, mock DSL
```

Visual QA: with the mock and dev server running, `pnpm screenshots` writes every screen to
`screenshots/` (Playwright, 1440×900 + 390×844 mobile). `EMPTY_BASE` / `DOWN_BASE` add empty-state
and connection-lost shots (see the header of `scripts/screenshots.ts`).

## Layout

```
src/api/        types.ts (proto mirror incl. auth/routing/cluster), client.ts (typed REST + ApiError), mergePatch.ts
src/telemetry/  client.ts (TelemetryClient), index.ts (singleton + useTopic hooks)
src/store/      telemetry.ts (zustand view), series.ts (sparkline buffers), selectors.ts, ui.ts, webhooks.ts,
                auth.ts (whoami → role), routes.ts (routes + polled stats), cluster.ts (status poller)
src/lib/        secrets.ts (secret fields + header rows: REDACTED keeps, "" removes), notifierForm.ts,
                ruleForm.ts, webhookForm.ts (draft ⇄ request + validation), backup.ts (config draft,
                savings/progress, restore command), backupActions.ts,
                rbac.ts (role gating), routeRows.ts (editor rows ⇄ conditions), useFilterParse.ts /
                useFilterRender.ts (server parse/render of the filter DSL, debounced + cached),
                filterError.ts (ApiError.position byte offset → string index), tokenFlow.ts
                (create-token flow), cluster.ts (partition matrix shaping)
src/components/ charts (uPlot), messages (inspector, payload, retry timeline), consumers, ui,
                routing/FilterBuilder + useFilterBuilder (WHEN builder shared by routes and webhooks),
                forms.tsx (SecretInput, HeadersEditor, CodeBlock, ToggleChips)
src/pages/      dashboard, channels, channel/* tabs (incl. Routing), consumers, DLQ, alerts, nodes,
                routing/ (list + WHEN/THEN editor with live backtest), settings + access tokens,
                notifications/ (notifiers, rules, history, pull feed), webhooks/ (list + editor),
                backups/ (status, target/schedule/encryption, history, restore)
src/shell/      layout, sidebar, top bar, ⌘K palette, banners
mock/           mock broker (node:http + ws); dsl.ts = filter DSL port (+ dsl.test.ts)
```

## Telemetry client architecture

```
WebSocket ──onmessage──► JSON.parse ──► queue ──(one rAF)──► apply in order ──► commit
                                                              │ snapshot: replace topic state
                                                              │ delta:    RFC 7386 merge patch
                                                              │ seq gap:  unsubscribe+subscribe, drop deltas until snapshot
                                                              ▼
                      store.commitFrame(view)  ──► zustand  ──► components (narrow selectors)
                      recordFrame(view)        ──► SeriesStore ──► uPlot charts (imperative redraw)
```

* **One socket per tab**, ref-counted topics: `useTopic(topic)` subscribes on first mount and
  unsubscribes after the last unmount (1.5 s grace, so route changes don't churn). Subscribe /
  unsubscribe calls in one tick are coalesced into one frame.
* **Decode off render**: frames are parsed in the socket callback and queued; all queued messages
  are applied **at most once per animation frame** (250 ms timer when the tab is hidden), producing
  one immutable view. Merge patches share structure, so unchanged sub-objects keep identity and
  selectors (`useTopicSelect` + shallow compare) only re-render what changed — a DLQ change does not
  re-render the throughput card.
* **Live tail**: tail deltas replace `messages` with the newest batch; the client accumulates a
  bounded buffer (2000) per channel.
* **Reconnect**: exponential backoff with jitter (0.5 s → 30 s), resubscribes every active topic,
  fresh snapshots on reopen. Ping every 15 s; a socket silent for 45 s is closed and reopened.
  When a connection fails before opening, a token-protected REST call (`/api/alerts`) is probed to
  distinguish "down" from 401 (`/api/health` is always open on the server).
* **LIVE / PAUSED**: the client keeps applying frames (deltas must stay consistent); while paused
  the store just stops publishing them to React, and charts skip redraws. Resume publishes the
  latest frame.
* **Charts**: uPlot on canvas; sparklines read from `SeriesStore` (1 s buckets, 15 min), seeded from
  `/api/history?range=5m`. History charts merge `/api/history` with live samples (≤1 s buckets) or
  refetch periodically (longer ranges).

Auth: the access token (Settings, the 401 prompt, or adopted automatically when the server answers
`adopt: true` — the first token, which must be admin, created in open mode) is stored in `localStorage`, sent as `Authorization: Bearer` and as
`?token=` on the WebSocket. `GET /api/auth/me` (on start and on token change) gives the role;
`GateButton` / `useGate` disable — not hide — controls the role can't use, with a tooltip saying
which role is needed. 403 → one toast ("Your role (viewer) can't do this"), 401 → token prompt.
A v0.1 server (no `/api/auth/me`) is treated as "single admin token, no roles".

v0.2 screenshots: see the header of `scripts/screenshots.ts` (`CLUSTER_BASE`, `VIEWER_BASE`,
`SINGLE_BASE`, `V02_ONLY=1`); sets live in `screenshots/v0.2/` (mock) and
`screenshots/real-v0.2/` (real server). v0.3: `V03_ONLY=1` (+ `EMPTY_BASE`, `VIEWER_BASE`) →
`screenshots/v0.3/`.

## Future work

* Web Worker for decoding the live tail (JSON parse + accumulation) — currently on the main thread,
  batched per frame.
* Route-level code splitting (bundle is ~190 kB gzipped, one chunk).
* Unsaved-changes guard in the route editor; per-target forward stats once the contract has them.
* Field search becomes fast once the broker indexes payloads (today: scan-based, `scanned` shown).
