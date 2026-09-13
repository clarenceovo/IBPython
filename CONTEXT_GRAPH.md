# CONTEXT_GRAPH — IBPython

FastAPI REST + MCP gateway over the IBKR TWS socket API (`ib_insync`). Thin HTTP/MCP surface → feed/transport layer → `ib_insync` client. External services (Redis, QuestDB, MySQL, Telegram) are not bundled.

## Entry Points

| Entry | Command | Purpose |
| --- | --- | --- |
| REST API | `make run-api` (`src/webapp/__main__.py`) | FastAPI app on :8000, `/api/v1/*`, Swagger at `/docs` |
| MCP server | `.venv/bin/python -m src.mcp_server` (stdio) | 64 tools, separate client ID (`301`), idle disconnect |
| Scheduler | `make run` (`main.py`) | GenericScheduler job runner; job specs in `schedulejob/*.json` |
| Backfiller | `backfiller.py` | Historical OHLCV backfill |

## Layer Map

```
src/webapp/routers/*        thin HTTP endpoints (no business logic)
  └─> src/feeds/*           business logic + Pydantic DTOs (ib_insync calls live here)
        ├─> src/feeds/ibkr_connection.py    connection, retry, pacing leases, circuit breaker
        └─> src/transport/*                 Redis / QuestDB / MySQL / Telegram / scheduler persistence
src/mcp_server.py           MCP tool wrappers over the same feeds (shares DTOs with REST)
src/config/*                settings, config constants, reference/index data
src/webapp/docs/business_api_examples.md    single source of OpenAPI request examples
src/webapp/openapi_markdown.py              parses example markers -> OpenAPI examples
```

## Key Feed Modules

- `ibkr_feed.py` — `IBKRFeedClient` facade composed of: `ibkr_historical` (OHLCV/historical ticks), `ibkr_marketdata_ext` (depth, ticks, histograms, scanners, bulletins, symbol search), `ibkr_options_feed` (chains/Greeks/snapshots), `ibkr_reference_feed` (contract search, news, WSH), `ibkr_order_client` + `ibkr_order_client_ops` (orders, brackets, OCA, exercise), `ibkr_account_feed` (positions/PnL)
- `equity_reference.py` — bounded generic-tick subscriptions: shortability (tick 236), dividends (tick 456); deadline-bounded, explicit null-vs-zero
- `capabilities.py` — `gateway_capabilities()`: client version, negotiated protocol, per-feature status (no network calls)
- `pacing.py` / `market_data_budget.py` / `circuit_breaker.py` — IBKR pacing guards, line budget, circuit breaker
- `fundamental_data.py` — legacy fundamentals; IBKR removed the API in 10.47 → REST returns **410**, MCP tool errors explicitly
- Domain feeds: `fixed_income.py`, `bond_curve.py`, `bonds.py`, `options.py`, `news.py`, `scanner.py`, `streaming.py`, `tick_data.py`, `event_contracts.py`, `snapshotter.py`

## REST Route Groups (`/api/v1`)

- `business/*` — research wrappers (curves, news, panels, returns, skew, commodities, portfolio risk, event contracts, fixed income)
- `market-data/*` — OHLCV, equity shortability/dividends, options, futures, bonds, depth
- `reference-data/*` — option chains, WSH/economic calendar, news; `fundamentals` → 410
- `orders/*` — place/cancel/modify, `open/all` (all-client snapshot, bearer-protected), executions, preview, cache
- `account/*`, `system/*` (health, capabilities, server-time, market-data-type), `histogram`, `realtime-bars/*` (SSE), `snapshot/*`, `streaming/*`, `tick-data/*`, `scanner/*`

Exception→status mapping in `src/webapp/app.py` `_exc_map` (e.g. `IBKRUnsupportedFeatureError` → 410).

## Error/Status Conventions

- 201 for resource creation; 200 for DELETE (with body); 410 for removed IBKR features
- Equity reference: `available` / `partial` / `unavailable` + `timed_out`; null = unobserved, 0 = valid value
- OpenAPI request examples live ONLY in `src/webapp/docs/business_api_examples.md` (`<!-- openapi-example: group key -->` markers), wired via `markdown_openapi_examples(group)` in routers — do not inline examples in router code

## Tests & Docs

- `tests/` — pytest, broker-free (fake sockets/ib_insync decoders); focused: `test_equity_reference.py`, `test_gateway_additions.py`, `test_mcp_server.py`
- `docs/gateway_equity_reference.md` — equity reference/capabilities delivery notes
- `docs/ibkr_official_docs_research_2026-09-13.md` — IBKR 10.43→10.50 comparison + implementation plan (next: bulletins, Adaptive orders, combo orders; gated on client migration: odd-lot quotes, settlement type, overnight conditions)
- `IBKR_API_PARAMETER_REVIEW.md` / `IBKR_API_FIXES_SUMMARY.md` — June 2025 parameter audit, all fixes applied
- `PROJECT_SETUP_ARCHITECTURE.md` — full architecture/caveats

## Recent Changes (2026-09-13, f5a0d8b)

- New REST: `GET /system/capabilities`, `POST /market-data/equity/shortability`, `POST /market-data/equity/dividends`, `GET /orders/open/all`
- New MCP tools: `get_gateway_capabilities`, `load_equity_shortability`, `load_equity_dividends`; `load_fundamentals` now errors (10.47 removal)
- New modules: `feeds/capabilities.py`, `feeds/equity_reference.py`; `IBKRUnsupportedFeatureError` → 410 mapping
- MCP dependency pinned `>=1.0,<2` (v1 `FastMCP` interface)

## Environment Notes

- `ib-insync>=0.9.86` (archived upstream, `MaxClientVersion=176` ceiling) — newer IBKR API features require client migration first
- Redis = pacing/auth/cache backend; MCP uses its own client ID; REST order routes are bearer-token protected
- Config via `src/config/settings.py` + `.env` (`IBKR_HOST/PORT/CLIENT_ID`, MCP client ID, Telegram logging)
