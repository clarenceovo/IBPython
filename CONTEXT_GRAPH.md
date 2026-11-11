# CONTEXT_GRAPH

Project map for IBPython (IBKR data + trading bridge). ASD-STE100 style: short sentences, one idea per line.

## Purpose

Python service over TWS API (ib_async) plus IBKR Web API. It serves market data, reference data, research, and guarded order access. Three front ends share one state: REST (FastAPI), MCP server, scheduler.

## Layers

- `src/webapp/` — FastAPI app. `routers/` holds all HTTP endpoints. `dependencies.py` builds the shared state (`feed`, `loader`, `redis`, `event_contracts`). `consolidated.py` holds the shared section runner for consolidated endpoints.
- `src/webapp/routers/business_consolidated.py` — `GET /business/consolidated/overview`: account summary, PnL, positions, portfolio, open orders. One call, isolated failures.
- `src/webapp/routers/market_data_consolidated.py` — `GET /market-data/consolidated/symbol/{symbol}`: snapshot, ticks, depth, trading schedule. Section whitelist via query param. Also `GET .../resolve/{symbol}` (universal converter) and `GET .../universal/{symbol}` (all-asset-class fetch: resolve, then ohlcv/ticks/depth/schedule/snapshot sections).
- `src/feeds/symbol_resolver.py` — universal symbol converter. One notation in (equity suffixes, FX separators, crypto roots, futures continuous/dated notations, index aliases), one ResolvedSymbol out. `matched_rule` records why. Explicit `asset_class` pins option/bond pass-through.
- `src/webapp/routers/account.py` — account data plus live PnL SSE streams (`/account/pnl/*/stream`). Poll-and-dedupe; release on disconnect.
- `src/webapp/routers/streaming.py` — SSE ticker subscriptions plus `DELETE /streaming/subscriptions` cancel-all.
- `src/mcp_server.py` — MCP tools over the same feed state.
- `src/feeds/` — IBKR clients. `ibkr_feed.py` is the facade. Sub-clients: `ibkr_order_client.py` (orders), `ibkr_account_feed.py`, `ibkr_options_feed.py`, `ibkr_historical.py`, `event_contracts.py` (Web API).
- `src/transport/` — Redis, QuestDB, MySQL, metrics, scheduler.
- `src/config/` — settings (pydantic, env-driven) and constants.
- `tests/` — pytest. Router tests fake the IB connection.

## Order Pathway Master Switch (IBKR_ORDERS_ENABLED)

Default `false`. The deployment is read-only.

Three gates:

- REST: `require_orders_enabled` dependency in `routers/orders.py`. It returns `503` on place, cancel, modify, preview, bracket, OCA. `routers/business_event_contracts.py` checks the same flag before its own live-order flag.
- MCP: each order-writing tool checks `state.settings.ibkr_orders_enabled` and returns `MCP_ORDERS_DISABLED_ERROR`.
- Service: `IBKROrderClient` carries `orders_enabled`. The `_require_orders_enabled` decorator raises `IBKROrdersDisabledError` on all eight writing methods.

Read-only order queries stay open: open orders, completed orders, executions, order cache, TCA.

`gateway_capabilities(..., orders_enabled=...)` reports `order_pathways.availability`: `disabled` or `unknown`.

## Key Decisions

- Order writes fail closed. A missing or unreadable config means "no orders".
- The event-contract live order needs two flags: `IBKR_ORDERS_ENABLED=true` and `IBKR_EVENT_CONTRACTS_LIVE_ORDERS_ENABLED=true`.
- Order REST endpoints also need a bearer token from Redis (`OrderAuth::bearer_token`).
- TWS API baseline 10.47. Fundamental data endpoints are removed; WSH events replace them. See `src/feeds/capabilities.py`.

## Test Entry Points

- `tests/test_orders_disabled.py` — master-switch behavior at every layer.
- `tests/test_orders.py` — order lifecycle with `ibkr_orders_enabled=True`.
- `tests/test_webapp.py` — REST app wiring, event-contract gates.
- `tests/test_capabilities.py` — capability surface.
