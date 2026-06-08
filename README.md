# IBPython Market Data Foundation

Production-oriented IBKR market data, transport, scheduler, and REST API layer for systematic trading research, live risk monitoring, and portfolio analytics.

## What This Provides

- Async IBKR TWS / IB Gateway adapter through `ib_insync`
- Unified OHLCV DTOs across equities, FX, futures, bonds, indices, crypto, and options
- Option-chain discovery and option analytics DTOs
- Fundamental, economic-event, and news DTOs
- Bond yield, CTD, and yield-curve bootstrap DTOs
- Account summary, live position, portfolio, account PnL, and position PnL DTOs
- Redis cache for latest bars, index compositions, scheduler jobs, and IBKR pacing bookmarks
- OHLCV persistence through a storage interface with QuestDB as the main time-series backend and MySQL as an alternate relational backend
- Generic Redis-defined scheduler jobs
- FastAPI app: `IBKRRestApp`
- Notebook workflows for local debugging and research

## Requirements

- Python 3.13+
- IBKR TWS or IB Gateway running locally or on a reachable host
- Redis
- QuestDB
- MySQL, optional when `MARKET_DATA_DB_BACKEND=mysql`
- Docker Desktop or Docker Engine, if using Compose

## Quick Start

```bash
make install-dev
cp .env.example .env
make services-up
make test
```

Start TWS or IB Gateway, confirm API access is enabled, then run the REST API:

```bash
make run-api
```

`make run-api` forces uvicorn to use the standard `asyncio` loop. This matters for `ib_insync`; uvicorn's `uvloop` backend can conflict with nested event-loop patching and prevent the API from connecting even when notebooks can.

Open:

- FastAPI docs: http://localhost:8000/docs
- Health check: http://localhost:8000/api/v1/system/health
- QuestDB UI: http://localhost:9000

## Configuration

Configuration is centralized in `src/config/`.

```text
src/config/
  config_constant.py   # Defaults, env names, Redis key templates, table names
  config_loader.py     # Loads defaults, .env, process env, explicit overrides
  settings.py          # Pydantic validation around ConfigLoader output
```

Use `load_settings()` everywhere:

```python
from src.config.settings import load_settings

settings = load_settings()
```

Load order:

```text
config_constant defaults -> .env -> process environment -> explicit overrides
```

Blank `.env` values are ignored. For example, if `.env` contains `IBKR_HOST=`, the loader keeps `DEFAULT_IBKR_HOST` from `config_constant.py`.

Core variables:

| Variable | Default | Purpose |
|---|---:|---|
| `IBKR_HOST` | `127.0.0.1` | TWS or IB Gateway host |
| `IBKR_PORT` | `4001` | IBKR API port |
| `IBKR_CLIENT_ID` | `1` | IBKR client ID; must be unique across notebooks/API clients |
| `IBKR_MCP_CLIENT_ID` | `301` | IBKR client ID used by the MCP server |
| `IBKR_MARKET_DATA_LINES` | `100` | Market data entitlement baseline for pacing analysis |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis URL |
| `REDIS_PASSWORD` | empty | Optional Redis AUTH password |
| `IBKR_REST_BASE_URL` | `http://localhost:8000` | Base URL the scheduler uses when calling FastAPI OHLCV endpoints |
| `QUESTDB_HOST` | `127.0.0.1` | QuestDB PostgreSQL wire host |
| `QUESTDB_PORT` | `8812` | QuestDB PostgreSQL wire port |
| `QUESTDB_WRITE_PORT` | `9009` | QuestDB ILP/TCP write port |
| `QUESTDB_USER` | `admin` | QuestDB user |
| `QUESTDB_PASSWORD` | `quest` | QuestDB password |
| `QUESTDB_DATABASE` | `qdb` | QuestDB database |
| `MYSQL_HOST` | `127.0.0.1` | MySQL host for alternate OHLCV persistence |
| `MYSQL_PORT` | `3306` | MySQL port |
| `MYSQL_USER` | `root` | MySQL user |
| `MYSQL_PASSWORD` | empty | MySQL password |
| `MYSQL_DATABASE` | `trading` | MySQL database |
| `MARKET_DATA_DB_BACKEND` | `questdb` | OHLCV persistence backend: `questdb` or `mysql` |
| `INDEX_SYNC_INTERVAL_SECONDS` | `86400` | Default index reload interval |
| `INDEX_COMPOSITION_PROVIDER` | empty | External index constituent provider name |
| `FIXED_INCOME_REFERENCE_PROVIDER` | empty | Optional import path for CTD basket and conversion-factor provider. For local demos only, use `src.feeds.fixed_income_reference:provider`. |
| `IBKR_REST_APP_NAME` | `IBKRRestApp` | FastAPI title |
| `IBKR_REST_CONNECT_ON_STARTUP` | `false` | Connect IBKR and Redis during API startup |
| `IBKR_REST_MARKET_DATA_TTL_SECONDS` | `5` | REST market-data TTL cache default |
| `IBKR_REST_MARKET_DATA_CACHE_MAXSIZE` | `512` | REST TTL cache max entries |
| `IBKR_REST_OHLCV_RATE_LIMIT_RETRY_DELAY_SECONDS` | `60` | Snapshotter wait before retrying a FastAPI OHLCV request that returns HTTP 429 |
| `IBKR_REST_OHLCV_RATE_LIMIT_RETRY_COUNT` | `1` | Number of HTTP 429 retries per snapshotter OHLCV API call |
| `IBKR_ORDER_AUTH_REDIS_KEY` | `OrderAuth::bearer_token` | Redis key containing the bearer token payload required by `/api/v1/orders/*` |
| `IBKR_RATE_LIMIT_ENABLED` | `true` | Enable the internal app-wide IBKR pacing controller |
| `IBKR_RATE_LIMIT_GLOBAL_MESSAGES_PER_SECOND` | `50` | Outgoing IBKR socket messages per second; IBKR's documented default cap is 50 |
| `IBKR_RATE_LIMIT_MARKET_DATA_RESERVE` | unset | Market-data-line reserve; unset means `max(5, ceil(IBKR_MARKET_DATA_LINES * 0.10))` |
| `IBKR_RATE_LIMIT_MARKET_DATA_LEASE_TTL_SECONDS` | `3600` | TTL safety net for active market-data-line leases |
| `IBKR_WEB_API_BASE_URL` | `https://localhost:5000/v1/api` | IBKR Web API base URL for Client Portal Gateway or OAuth routing |
| `IBKR_WEB_API_BEARER_TOKEN` | empty | Optional OAuth bearer token for IBKR Web API calls |
| `IBKR_WEB_API_COOKIE` | empty | Optional raw Cookie header for an authenticated Client Portal Gateway session |
| `IBKR_WEB_API_VERIFY_SSL` | `false` | Verify TLS certificates for IBKR Web API calls; local CPGW commonly uses a self-signed cert |
| `IBKR_EVENT_CONTRACTS_LIVE_ORDERS_ENABLED` | `false` | Safety switch for live Event Contract Web API order submission |

## Local Commands

```bash
make install-dev
make lint
make typecheck
make services-up
make test
make notebook
make run
make validate-scheduler
make run-api
make run-api-dev
make services-down
```

`make run` starts the Redis-backed scheduler worker. `make run-api` starts `IBKRRestApp` without hot reload, which is the safer default for IBKR client sessions. Use `make run-api-dev` when you explicitly want uvicorn reload during local API development.

Equivalent direct API command:

```bash
python -m uvicorn src.webapp.app:get_app --host 0.0.0.0 --port 8000 --factory --loop asyncio --lifespan on
```

Development reload command:

```bash
make run-api-dev
```

The scheduler worker is dependency-aware:

- market snapshot jobs connect to IBKR
- market/OHLCV snapshot jobs with `persist=true` also connect to the configured market OHLCV store
- index-composition-only jobs do not connect to IBKR or the market OHLCV store
- unsupported job types are skipped with a warning instead of crashing the worker
- Redis leases prevent duplicate execution when multiple scheduler workers see the same job
- scheduler run state is written to Redis for latest-run inspection and short history
- local and Redis job definitions are reconciled periodically while the worker is running
- SIGINT/SIGTERM request graceful scheduler shutdown

Validate scheduler configuration before starting a live worker:

```bash
make validate-scheduler
python scripts/validate_scheduler_jobs.py --schedule-dir schedulejob --include-redis
```

## Docker

The API and scheduler run as separate containers:

- `ibkr-rest-app`: FastAPI bridge on port `8000`.
- `ibkr-scheduler`: Redis-defined OHLCV/index scheduler worker with no public port.

Build and run both app containers with Redis, QuestDB, and MySQL:

```bash
cp .env.example .env
docker compose up -d --build ibkr-rest-app ibkr-scheduler
```

Useful commands:

```bash
make docker-build
make docker-up
make docker-up-api
make docker-up-scheduler
make docker-logs-api
make docker-logs-scheduler
docker compose down
```

Compose uses separate Dockerfiles:

- `Dockerfile.api`
- `Dockerfile.scheduler`

`Dockerfile` remains an API-compatible default for tools that expect the conventional filename.

The Compose app services set `IBKR_HOST` from `IBKR_DOCKER_HOST`, defaulting to `host.docker.internal`, which lets containers reach TWS or IB Gateway running on the host machine through Docker Desktop. Override `IBKR_DOCKER_HOST` and `IBKR_DOCKER_PORT` if IB Gateway runs elsewhere.

The scheduler uses `IBKR_DOCKER_REST_BASE_URL` inside Compose, defaulting to `http://ibkr-rest-app:8000`, so it calls the FastAPI service over the Docker network instead of container-local `localhost`.

The API, scheduler, and MCP server use separate IBKR client IDs by default:

- `IBKR_API_CLIENT_ID=101`
- `IBKR_SCHEDULER_CLIENT_ID=201`
- `IBKR_MCP_CLIENT_ID=301`

Keep those distinct from notebooks and other API clients.

Docker operational defaults:

- FastAPI intentionally runs as one uvicorn worker because each worker would create its own IBKR session, in-process cache, and streaming state.
- `ibkr-rest-app` has a Docker healthcheck against `/api/v1/system/health`; scheduler startup waits for that service health.
- App containers use `init: true`, a 30-second `stop_grace_period`, and non-root `appuser` images so SIGTERM can drain lifespan shutdown and disconnect IBKR cleanly.
- Redis and MySQL have Compose healthchecks; QuestDB is still treated as `service_started` because the upstream image does not guarantee a portable shell/curl healthcheck tool.

QuestDB remains the default scheduler/snapshotter market-data store. To route OHLCV snapshot persistence to MySQL instead, set `MARKET_DATA_DB_BACKEND=mysql` and configure the `MYSQL_*` variables in `.env`.

## REST API

`IBKRRestApp` is a thin async HTTP bridge over the Pydantic DTOs in `src/feeds`. It keeps business logic in the feed/transport layer and exposes a clean API surface for notebooks, dashboards, services, and internal tools. It does not open QuestDB/MySQL connections; durable market-data persistence and historical store reads belong to the scheduler/snapshotter side.

Main route groups:

- `/api/v1/business/*`: research-friendly wrappers for curves, news, market panels, returns, option skew, commodity futures, and portfolio risk
- `/api/v1/business/event-contracts/*`: ForecastEx/CME Event Contract discovery, snapshots, history, websocket message helpers, and guarded Web API order tickets
- `/api/v1/business/fixed-income/*`: bond futures quotes, CTD analytics, futures-implied curves, and cash/futures curve comparison
- `/api/v1/system/*`: health, readiness, rate-limit diagnostics, and TTL cache controls
- `/api/v1/market-data/*`: OHLCV, latest Redis bars, option analytics, commodity futures/options, bond yield history
- `/api/v1/reference-data/*`: option chains, fundamentals, WSH events/economic calendar, news
- `/api/v1/account/*`: account summary, live positions, portfolio, PnL snapshots
- `/api/v1/orders/*`: protected order lifecycle, execution lookup, what-if preview, and order-envelope cache

Common endpoints:

```text
GET  /api/v1/business/getBondCurve
GET  /api/v1/business/getNewsProviders
POST /api/v1/business/getSymbolNews
POST /api/v1/business/getNewsArticle
POST /api/v1/business/getMarketPanel
POST /api/v1/business/getUniverseBars
POST /api/v1/business/getReturns
POST /api/v1/business/getOptionSkew
POST /api/v1/business/commodities/getFutures
POST /api/v1/business/portfolio/getRiskSnapshot
POST /api/v1/business/fixed-income/getBondFutureQuotes
POST /api/v1/business/fixed-income/getCTD
POST /api/v1/business/fixed-income/getFuturesImpliedCurve
POST /api/v1/business/fixed-income/getCashBondCurve
POST /api/v1/business/fixed-income/getCurveComparison
GET  /api/v1/system/health
GET  /api/v1/system/readiness
GET  /api/v1/system/rate-limits
GET  /api/v1/system/cache/market-data
POST /api/v1/market-data/ohlcv
POST /api/v1/market-data/ohlcv/equity
POST /api/v1/market-data/ohlcv/futures
POST /api/v1/market-data/ohlcv/commodities
POST /api/v1/market-data/ohlcv/commodity-options
POST /api/v1/market-data/ohlcv/fx
POST /api/v1/market-data/ohlcv/fx-options
POST /api/v1/market-data/ohlcv/bond
GET  /api/v1/market-data/latest-bar
POST /api/v1/market-data/options/analytics
POST /api/v1/market-data/options/skew
POST /api/v1/market-data/commodities/options/analytics
POST /api/v1/market-data/commodities/metadata
POST /api/v1/market-data/commodities/historical-ticks
POST /api/v1/market-data/commodities/news
POST /api/v1/market-data/bonds/yields/history
POST /api/v1/snapshot/fx-options/capture
GET  /api/v1/snapshot/fx-options/latest
POST /api/v1/snapshot/fx-options/query
POST /api/v1/reference-data/options/chains
POST /api/v1/reference-data/fundamentals
GET  /api/v1/reference-data/wsh/metadata
POST /api/v1/reference-data/wsh/events
POST /api/v1/reference-data/economic-calendar
GET  /api/v1/reference-data/news/providers
POST /api/v1/reference-data/news/historical
POST /api/v1/reference-data/news/article
GET  /api/v1/account/summary
GET  /api/v1/account/positions
GET  /api/v1/account/portfolio
POST /api/v1/account/pnl/account
POST /api/v1/account/pnl/position
POST /api/v1/orders/place
POST /api/v1/orders/{order_id}/cancel
POST /api/v1/orders/{order_id}/modify
GET  /api/v1/orders/open
POST /api/v1/orders/executions
POST /api/v1/orders/preview
GET  /api/v1/orders/completed
GET  /api/v1/orders/cache/{order_uuid}
GET  /api/v1/orders/cache
```

### Order Endpoint Authentication

Order endpoints require a bearer token:

```text
Authorization: Bearer <token>
```

The API reads the expected token payload from Redis using `IBKR_ORDER_AUTH_REDIS_KEY`, which defaults to:

```text
OrderAuth::bearer_token
```

Configure it before using `/api/v1/orders/*`:

```bash
redis-cli SET OrderAuth::bearer_token 'replace-with-a-long-random-token'
```

If the Redis key is missing or Redis cannot be read, order endpoints fail closed with `503`. If the bearer token is missing or wrong, they return `401`. Swagger exposes this as `OrderBearerAuth`; use the **Authorize** button in `/docs` before trying order routes.

Example order preview:

```bash
ORDER_TOKEN='replace-with-a-long-random-token'

curl -X POST http://localhost:8000/api/v1/orders/preview \
  -H "Authorization: Bearer ${ORDER_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "AAPL",
    "sec_type": "STK",
    "exchange": "SMART",
    "currency": "USD",
    "primary_exchange": "NASDAQ",
    "action": "BUY",
    "order_type": "LMT",
    "quantity": 10,
    "price": 150.0,
    "account_id": "DU123"
  }'
```

Use `/api/v1/orders/preview` for IBKR what-if margin and commission checks. The live `/api/v1/orders/place` endpoint is the submission path and must not automatically run a what-if request for every order; clients that require pre-trade margin review should call preview explicitly before placing.

Example live order placement:

```bash
curl -X POST http://localhost:8000/api/v1/orders/place \
  -H "Authorization: Bearer ${ORDER_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "AAPL",
    "sec_type": "STK",
    "exchange": "SMART",
    "currency": "USD",
    "primary_exchange": "NASDAQ",
    "action": "BUY",
    "order_type": "LMT",
    "quantity": 10,
    "price": 150.0,
    "tif": "DAY",
    "account_id": "DU123"
  }'
```

Example trailing stop limit order:

```bash
curl -X POST http://localhost:8000/api/v1/orders/place \
  -H "Authorization: Bearer ${ORDER_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "AAPL",
    "sec_type": "STK",
    "exchange": "SMART",
    "currency": "USD",
    "action": "SELL",
    "order_type": "TRAIL LIMIT",
    "quantity": 10,
    "trail_stop_price": 145.0,
    "trailing_type": "amt",
    "trailing_amount": 1.0,
    "limit_price_offset": 0.25,
    "tif": "DAY",
    "account_id": "DU123"
  }'
```

Trailing stop limit payloads should expose both `trail_stop_price` and `limit_price_offset` so clients can distinguish the initial trailing stop trigger from the limit offset submitted to IBKR.

Order placement writes a pending UUID-tagged envelope and submits the live order. Cancel and modify require `account_id` and reject account mismatches when IBKR exposes the order account. In-place modify is intentionally narrow: only `price`, `quantity`, and `tif` are accepted for an existing order. Cached order envelopes are stored without TTL by the order client so they can serve as an operational audit trail.

Example OHLCV request:

```bash
curl -X POST http://localhost:8000/api/v1/market-data/ohlcv \
  -H "Content-Type: application/json" \
  -d '{
    "request": {
      "symbol": "SPY",
      "asset_class": "equity",
      "exchange": "SMART",
      "currency": "USD",
      "duration": "1 D",
      "bar_size": "1 min",
      "what_to_show": "TRADES",
      "use_rth": true
    },
    "persist": false,
    "cache_latest": true,
    "use_ttl_cache": true
  }'
```

Minimal OHLCV wrapper examples:

```bash
curl -X POST http://localhost:8000/api/v1/market-data/ohlcv/equity \
  -H "Content-Type: application/json" \
  -d '{"symbol":"SPY"}'

curl -X POST http://localhost:8000/api/v1/market-data/ohlcv/equity \
  -H "Content-Type: application/json" \
  -d '{"symbol":"SPY","start_datetime":"2026-05-01T13:30:00Z","end_datetime":"2026-05-01T20:00:00Z","bar_size":"1 min"}'

curl -X POST http://localhost:8000/api/v1/market-data/ohlcv/fx \
  -H "Content-Type: application/json" \
  -d '{"symbol":"EURUSD"}'

curl -X POST http://localhost:8000/api/v1/market-data/ohlcv/fx-options \
  -H "Content-Type: application/json" \
  -d '{"symbol":"EURUSD","expiry":"20260619","strike":1.10,"right":"C"}'

curl -X POST http://localhost:8000/api/v1/market-data/ohlcv/futures \
  -H "Content-Type: application/json" \
  -d '{"symbol":"ES","last_trade_date_or_contract_month":"202606"}'

curl -X POST http://localhost:8000/api/v1/market-data/ohlcv/commodities \
  -H "Content-Type: application/json" \
  -d '{"symbol":"CL","last_trade_date_or_contract_month":"202606"}'

curl -X POST http://localhost:8000/api/v1/market-data/ohlcv/commodity-options \
  -H "Content-Type: application/json" \
  -d '{"underlying_symbol":"CL","expiry":"20260617","strike":80,"right":"C","multiplier":"1000"}'

curl -X POST http://localhost:8000/api/v1/market-data/ohlcv/bond \
  -H "Content-Type: application/json" \
  -d '{"sec_id_type":"CUSIP","sec_id":"91282CJN2"}'
```

The asset-specific OHLCV wrappers are the business-friendly OHLCV API: callers pass minimal identifiers and the service presets `asset_class` plus common IBKR defaults. They accept the same optional controls as the generic OHLCV endpoint: `duration`, `bar_size`, `start_datetime`, `end_datetime`, `what_to_show`, `use_rth`, `persist`, `cache_latest`, `use_ttl_cache`, `cache_ttl_seconds`, and `metadata`. `persist` is accepted for backward-compatible payloads but API-side persistence is disabled; the scheduler/snapshotter owns durable writes.

Commodity routes are futures-first. Commodity futures remain `asset_class="future"` and commodity futures options remain `asset_class="option"` with IBKR `secType="FOP"`. The commodity OHLCV wrapper presets common roots (`CL`/`NG` to `NYMEX`, `GC`/`SI`/`HG` to `COMEX`, and grain/oilseed roots such as `ZC`/`ZS`/`ZW`/`ZL`/`ZM` to `CBOT`) while allowing explicit exchange and currency overrides. Related IBKR-native commodity endpoints expose futures-option analytics, contract metadata, historical ticks, and historical news without adding external COT, weather, inventory, or shipping providers.

FX option routes use pair-style inputs. For example, `EURUSD` maps to `option_sec_type="OPT"`, `underlying_symbol="EUR"`, and `currency="USD"` unless `currency`, `local_symbol`, or `con_id` is supplied to disambiguate an IBKR contract. Historical FX option bars return `OptionOHLCVBar`. Live FX option collection uses short-lived market-data subscriptions and stores latest snapshots in Redis. Durable FX option snapshot persistence belongs to the scheduler/snapshotter layer, not the API process.

Example FX option live snapshot:

```bash
curl -X POST http://localhost:8000/api/v1/snapshot/fx-options/capture \
  -H "Content-Type: application/json" \
  -d '{
    "contracts": [
      {"symbol":"EURUSD","expiry":"20260619","strike":1.10,"right":"C","exchange":"SMART"}
    ],
    "snapshot_wait_seconds": 2.0,
    "persist": true,
    "cache_latest": true
  }'
```

IBKR live option Greeks require market-data subscriptions for both the option and the underlying instrument. If an account lacks the required FX/option entitlements, the snapshot can still return available bid/ask/last fields while Greeks, OI, or volume fields remain `null`.

When `start_datetime` is supplied, the API uses the paginated historical range loader and treats `end_datetime` as the end of the requested range. If `end_datetime` is omitted, the range ends at current UTC time. This is the preferred business payload for explicit backfill windows; `duration` remains useful for simple one-shot IBKR historical requests.

OHLCV DTOs are layered for extension: `BaseOHLCVBar` carries `symbol`, `timestamp`, and OHLCV prices; `OHLCVBar` adds market metadata; `FutureOHLCVBar` adds `contract_month` and `is_continuous`; `FXOHLCVBar` adds `base_currency` and `quote_currency`; `OptionOHLCVBar` adds option contract identity such as underlying, expiry, strike, right, multiplier, trading class, contract month, and `con_id`. The futures and FX wrappers document their specific response schemas in OpenAPI.

OHLCV persistence is backend-neutral. The scheduler/snapshotter writes through the configured `MarketOHLCVStore`, implemented by QuestDB and MySQL:

```bash
MARKET_DATA_DB_BACKEND=questdb  # default main time-series store
MARKET_DATA_DB_BACKEND=mysql    # alternate relational store
```

Every normalized bar now carries a deterministic `contract_key`. The strongest available identifier wins: IBKR `con_id`, then `local_symbol`, option identity, futures contract month, and finally symbol/exchange/currency. QuestDB and MySQL store this key plus nullable contract identity columns (`con_id`, `local_symbol`, `contract_month`, `expiry`, `strike`, `right`, `trading_class`, `what_to_show`, and `use_rth`) so same-root futures/options at the same timestamp do not collide.

`OHLCVLoader` runs a data-quality report after sorting and before persistence/cache. It checks UTC timestamps, monotonic ordering, duplicates by `(contract_key, timestamp)`, interval gaps, non-finite values, invalid OHLC ranges, and stale latest bars when configured. Fatal invariants block persistence; warnings are logged and included in scheduler metrics.

QuestDB is preferred for high-volume time-series capture and stores OHLCV bars in `EquityOHLCV`. MySQL keeps the `market_ohlcv` logical table with an idempotent primary key on contract identity for operational/reporting deployments that need bars in a relational database.

Example TSLA option-chain request:

```bash
curl -X POST http://localhost:8000/api/v1/reference-data/options/chains \
  -H "Content-Type: application/json" \
  -d '{
    "request": {
      "symbol": "TSLA",
      "asset_class": "equity",
      "exchange": "SMART",
      "currency": "USD",
      "primary_exchange": "NASDAQ"
    },
    "use_ttl_cache": true,
    "cache_ttl_seconds": 300
  }'
```

For SMART-routed US equities, include `primary_exchange` to avoid ambiguous IBKR contract qualification. If you already know the IBKR underlying `conId`, pass `underlying_con_id` in the request to skip qualification.

Example bounded option skew request:

```bash
curl -X POST http://localhost:8000/api/v1/market-data/options/skew \
  -H "Content-Type: application/json" \
  -d '{
    "request": {
      "chain_request": {
        "symbol": "TSLA",
        "asset_class": "equity",
        "exchange": "SMART",
        "currency": "USD",
        "primary_exchange": "NASDAQ"
      },
      "spot_price": 250.0,
      "strike_window_pct": 0.30,
      "max_expirations": 4,
      "max_strikes_per_expiry": 11,
      "target_abs_delta": 0.25,
      "max_concurrent_requests": 4
    },
    "use_ttl_cache": true,
    "cache_ttl_seconds": 60
  }'
```

The skew endpoint computes per-expiry `put IV - call IV` using the nearest target-delta contracts when Greeks are available, falls back to symmetric moneyness when they are not, and reports the largest open-interest call and put strike for each maturity. Because IBKR rejects snapshot market data with generic ticks, option skew uses short-lived streaming subscriptions for the sampled contracts, waits briefly, then cancels them.

Example standard sovereign bond curve request:

```bash
curl "http://localhost:8000/api/v1/business/getBondCurve?market=UST&valuation_date=2026-05-16"
```

Supported `market` aliases include `UST`, `JGB`, `KTB`, `BUND`, `GERMAN_BUND`, `UK`, `UK_GILT`, and `GILT`. The response contains `standard_ctd_points`, the bootstrapped `curve`, and chart-ready `render_points` with tenor, par yield, zero rate, discount factor, CTD symbol, and futures symbol. The built-in provider is an indicative workflow stub: production CTD selection needs official delivery-basket, conversion-factor, accrued-interest, delivery-date, financing, futures-price, and bond quote data from an exchange or vendor.

Fixed-income research endpoints:

```text
POST /api/v1/business/fixed-income/getBondFutureQuotes
POST /api/v1/business/fixed-income/getCTD
POST /api/v1/business/fixed-income/getFuturesImpliedCurve
POST /api/v1/business/fixed-income/getBondYieldCurve
POST /api/v1/business/fixed-income/getCashBondCurve
POST /api/v1/business/fixed-income/getCurveComparison
POST /api/v1/business/fixed-income/getFedFundsFuturesRate
```

`getBondFutureQuotes` uses IBKR historical OHLCV on futures contracts and accepts the business minimum: `market` plus `contract_month` for the default curve futures, or an explicit `futures` list. For IBKR futures qualification the generated contract request uses `symbol`, `exchange`, `currency`, and one of `contract_month`, `local_symbol`, or `con_id`. `getCTD`, `getFuturesImpliedCurve`, and `getCurveComparison` additionally require `FIXED_INCOME_REFERENCE_PROVIDER`, because IBKR does not provide a complete official CTD delivery basket or conversion-factor feed through the standard TWS historical bar API. To make these routes work in local demos, set `FIXED_INCOME_REFERENCE_PROVIDER=src.feeds.fixed_income_reference:provider`; that built-in provider is indicative only and must not be used for trading, backtesting, or risk. IBKR currently documents historical/live bar limitations for OSE, so treat JGB futures as entitlement/feed dependent and validate with your gateway before relying on them in production.

`getBondYieldCurve` is the single sovereign curve API for `UST`, `JGB`, and `BUND`/`GERMAN_BUND`. Use `source_mode="futures_implied"` with `contract_month` or explicit futures plus `FIXED_INCOME_REFERENCE_PROVIDER` for an IBKR futures-price-backed CTD curve. Use `source_mode="indicative_placeholder"` for the built-in standard-tenor workflow stub, or `source_mode="auto"` with `allow_indicative_fallback=true` when you explicitly accept fallback. IBKR historical yield fields are documented as corporate-bond-only, so this endpoint does not pretend there is a direct IBKR UST/JGB/Bund sovereign yield-curve feed.

`getFedFundsFuturesRate` loads 30-Day Fed Funds futures (`ZQ`, CBOT/USD) from IBKR and returns the implied average rate as `100 - futures_price`. This is an IBKR-native futures proxy, not the official overnight effective Fed Funds fixing; use Federal Reserve/FRED-style benchmark data for the actual overnight time series.

Commodity business endpoint:

```text
POST /api/v1/business/commodities/getFutures
```

`commodities/getFutures` derives the front contract and requested forward contracts from `as_of_date` using the root's listed-month cycle, then loads the latest IBKR OHLCV bar for each selected futures contract. For example, `GC` with `as_of_date="2026-05-18"` selects `202606` as front and `202608` as the first forward.

```bash
curl -X POST http://localhost:8000/api/v1/business/commodities/getFutures \
  -H "Content-Type: application/json" \
  -d '{"symbol":"GC","as_of_date":"2026-05-18","forward_count":1,"bar_size":"5 mins"}'
```

Example business news request:

```bash
curl -X POST http://localhost:8000/api/v1/business/getSymbolNews \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "TSLA",
    "primary_exchange": "NASDAQ",
    "total_results": 20,
    "include_articles": false
  }'
```

`getSymbolNews` resolves the symbol to an IBKR `conId`, uses entitled news providers when `provider_codes` is omitted, then returns historical headlines. Set `include_articles=true` when you also need article bodies; this makes extra IBKR article requests. Real-time news is intentionally not part of the business wrapper because IBKR real-time news uses long-lived market-data/news subscriptions.

Portfolio risk business endpoint:

```text
POST /api/v1/business/portfolio/getRiskSnapshot
```

`getRiskSnapshot` turns existing IBKR account summary, portfolio, position, and account PnL primitives into a single read-only risk view: liquidity fields, leverage, live account PnL, position exposure, currency/asset-class exposure, and top concentrations. If account PnL is unavailable, the endpoint returns the account/position snapshot with a warning instead of failing the whole response. It does not call Client Portal Web API or open any durable market-data database connection.

Example business market panel request:

```bash
curl -X POST http://localhost:8000/api/v1/business/getMarketPanel \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["SPY", "QQQ", "TSLA"],
    "asset_class": "equity",
    "start_datetime": "2026-05-01T13:30:00Z",
    "end_datetime": "2026-05-01T20:00:00Z",
    "bar_size": "5 mins"
  }'
```

Business wrappers return normalized long-form DTOs and keep raw IBKR-style controls under the lower-level `/market-data` and `/reference-data` routes. Swagger examples for these business routes are sourced from `src/webapp/docs/business_api_examples.md`.

Example latest-bar request:

```bash
curl "http://localhost:8000/api/v1/market-data/latest-bar?asset_class=equity&bar_size=1%20min&symbol=SPY"
```

Query parameters:

| Parameter | Required | Meaning |
|---|---:|---|
| `asset_class` | yes | Asset-class namespace, for example `equity`, `fx`, `future`, `bond`, `index`, `crypto`, or `option`. |
| `bar_size` | yes | Bar size used when loading OHLCV, for example `1 min`. URL-encode spaces as `%20`; Redis normalizes spaces to underscores. |
| `symbol` | no | Symbol-scoped latest bar selector, for example `SPY`. Omit only for the legacy asset-class latest key. |

`GET /latest-bar` reads Redis only. It does not call IBKR or QuestDB. Populate it by loading OHLCV with `cache_latest=true` or by running a scheduler snapshot job.

The REST market-data cache is process-local and short-lived. It includes per-key single-flight protection so concurrent duplicate requests share one IBKR call. Redis remains the distributed cache for latest bars and scheduler/rate-limit state.

## IBKR Rate Limits

The app uses a central `IBKRRateLimitController` before IBKR socket calls. It preserves historical pacing and adds app-wide controls for global outgoing messages, active market-data subscriptions, snapshots, option analytics, FX option live capture, tick streams, PnL subscriptions, and order actions.

Controller defaults are conservative:

- Global IBKR messages: 50 per second.
- Historical data: 60 weighted requests per 600 seconds, 15-second identical-request cooldown, at most five same-contract historical requests per 2 seconds, and `BID_ASK` counts twice.
- Market-data lines: `IBKR_MARKET_DATA_LINES` minus a reserve of `max(5, ceil(lines * 0.10))` unless `IBKR_RATE_LIMIT_MARKET_DATA_RESERVE` is set.
- Redis coordinates limits across API and scheduler processes; if Redis is unavailable, the controller falls back to local in-process limiting.

Inspect current limiter state:

```bash
curl http://localhost:8000/api/v1/system/rate-limits
```

## Scheduler Jobs

Redis scheduler jobs are JSON payloads stored under:

```text
SchedulerJob::<job_name>
```

Operational JSON files live in `schedulejob/`.

The scheduler reads runnable jobs from both local `schedulejob/*.json` files and Redis `SchedulerJob::*` keys. Local files are deployable defaults; Redis jobs override local jobs when the `name` matches. The worker periodically reconciles those sources, starts new jobs, removes disabled jobs, and restarts changed jobs by payload hash.

Load a local job into Redis when you want to override it operationally:

```bash
redis-cli SET SchedulerJob::reload_g10_index_composition "$(cat schedulejob/reload_g10_index_composition.json)"
```

Then run the scheduler:

```bash
make run
```

The default G10 reload job is provider-neutral. IBKR does not expose index constituents or weights through the TWS API, so this job intentionally requires a real external constituent provider before production use.

Configure a real index provider with an import path:

```bash
INDEX_COMPOSITION_PROVIDER=my_package.providers:build_provider
```

The target may be a provider instance, a provider class, or a zero-argument factory returning an object with:

- `name`
- async `fetch(index_symbol)`

If `INDEX_COMPOSITION_PROVIDER` is blank, `configured_provider`, `placeholder`, or `todo`, the scheduler will not register the index reload handler and Redis index reload jobs will be skipped with a clear warning.

OHLCV snapshot jobs use `job_type="ohlcv_snapshot"` and support either `interval_seconds` or a five-field `cron` expression. The scheduler calls `POST /api/v1/market-data/ohlcv` on `IBKR_REST_BASE_URL` with API persistence/cache disabled, then persists and caches the returned bars itself. If the API returns HTTP 429 for an IBKR pacing/rate-limit response, the snapshotter waits `IBKR_REST_OHLCV_RATE_LIMIT_RETRY_DELAY_SECONDS` and retries up to `IBKR_REST_OHLCV_RATE_LIMIT_RETRY_COUNT` times before failing the symbol. If latest Redis cache writes fail after durable persistence, the symbol stays successful with a `cache_warning`; bookmarks update only after the durable write has succeeded. The OHLCV job still evaluates its market window before loading data, so a cron trigger outside `start_time`/`end_time` is logged and skipped.

If both `cron` and `interval_seconds` are present, `cron` controls trigger timing. `interval_seconds` remains operational metadata and must match `params.snap_interval_seconds` for OHLCV jobs. If a job sets `timeout_seconds`, include enough wall-clock time for API timeout plus configured rate-limit retry sleeps.

For bounded historical backfills, use `backfiller.py`. It calls the same OHLCV API with `persist=false`, `cache_latest=false`, and `use_ttl_cache=false`, then persists returned bars from the backfiller process and optionally caches the latest bar in Redis.

Example symbol-control JSON:

```json
{
  "defaults": {
    "asset_class": "future",
    "exchange": "HKFE",
    "currency": "HKD",
    "bar_size": "1 min",
    "what_to_show": "TRADES",
    "use_rth": true
  },
  "symbols": [
    {"symbol": "HSI", "last_trade_date_or_contract_month": "202606"},
    {"symbol": "HTI", "last_trade_date_or_contract_month": "202606"}
  ]
}
```

Run a date-bounded backfill:

```bash
python backfiller.py \
  --start 2026-05-01 \
  --end 2026-05-28 \
  --timezone Asia/Hong_Kong \
  --symbols-file backfill_symbols.json \
  --max-concurrency 1
```

Date-only `--end` includes the full calendar date in the selected timezone. Datetime `--end` values are treated as exclusive.

Example local jobs:

- `schedulejob/ohlcv_us_equity_1m.json`
- `schedulejob/ohlcv_hk_futures_1m.json`
- `schedulejob/ohlcv_major_indices_5m.json`

Execution state is logged through `src.transport.scheduler.execution` with states such as `running`, `lease_skipped`, `skipped_window`, `skipped_holiday`, `success`, `partial_success`, `failed`, `timeout`, `cancelled`, and `bookmark_updated`.

Snapshot bookmarks are inclusive: the next request starts at the previous successful latest timestamp. Keep OHLCV persistence idempotent by contract identity, bar size, and timestamp so replayed boundary bars do not duplicate durable rows.

## Redis Keys

Latest OHLCV bar:

```text
MarketData::<asset_class>::<SYMBOL>::<bar_size>:latest
MarketData::<asset_class>::<bar_size>:latest
```

The symbol-scoped key is the production-safe key. The asset-class-only key is still written as a backward-compatible legacy pointer to the most recently cached bar in that asset-class/bar-size bucket.

Index composition:

```text
GlobalIndex:<INDEX_SYMBOL>:composition
```

IBKR historical pacing bookmarks:

```text
IBKRRateLimit:historical:window
IBKRRateLimit:historical:identical:<request_hash>
IBKRRateLimit:historical:same_contract:<contract_hash>
IBKRRateLimit:global:window
IBKRRateLimit:market_data:leases
```

Scheduler leases and run history:

```text
SchedulerLease::<JOB_NAME>
SchedulerRun::<JOB_NAME>:latest
SchedulerRun::<JOB_NAME>:history
```

OHLCV snapshot bookmarks and status:

```text
OhlcvSnapshot::<JOB_NAME>::<SYMBOL>::<BAR_SIZE>:last_ts
OhlcvSnapshot::<JOB_NAME>::<SYMBOL>::<BAR_SIZE>:status
OhlcvSnapshotCalendar::<ASSET_CLASS>::<EXCHANGE>::<SYMBOL>::<CONTRACT_FINGERPRINT>::<YYYY-MM-DD>::<true|false>:has_session
```

Order auth and order envelopes:

```text
OrderAuth::bearer_token
OrderCache::<order_uuid>
```

## Verification

```bash
python3 -m pytest -q
python3 -m compileall -q src tests
python3 scripts/validate_scheduler_jobs.py --schedule-dir schedulejob
python3 -m json.tool schedulejob/reload_g10_index_composition.json >/dev/null
```

Current expected test status:

```text
380 passed
```

## Important Quant And IBKR Caveats

- IBKR historical pacing is enforced in-process and through Redis bookmarks.
- Current index compositions are not point-in-time historical constituents.
- IBKR bond yield historical fields are documented, but yield history is only available for corporate bonds.
- CTD analytics require exchange/vendor delivery-basket data beyond the IBKR TWS API.
- Full option chains should not be requested as market data in one shot. Discover the chain, filter contracts, then request selected analytics.
- Option skew scans sample a bounded set of strikes per maturity. Increase `max_expirations`, `max_strikes_per_expiry`, and `max_concurrent_requests` carefully because each strike/right pair consumes a temporary market-data line while the short-lived subscription is open.
- REST PnL endpoints use short-lived subscriptions; durable streaming PnL should live in a dedicated risk-engine process.
- Order endpoints are protected by a Redis-backed bearer token, but this is not a full execution risk engine. Use `/api/v1/orders/preview` deliberately for IBKR what-if checks, and add portfolio-level limits, user identity, strategy IDs, approval workflows, event-sourced order lifecycle callbacks, and durable database-backed audit before exposing live trading beyond a trusted internal environment.

## More Detail

Read [PROJECT_SETUP_ARCHITECTURE.md](PROJECT_SETUP_ARCHITECTURE.md) for the full architecture, IBKR pacing notes, job JSON schema, Docker setup, and implementation caveats.
