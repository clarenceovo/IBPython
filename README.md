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
- QuestDB persistence over PostgreSQL wire protocol
- Generic Redis-defined scheduler jobs
- FastAPI app: `IBKRRestApp`
- Notebook workflows for local debugging and research

## Requirements

- Python 3.13+
- IBKR TWS or IB Gateway running locally or on a reachable host
- Redis
- QuestDB
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
| `IBKR_MARKET_DATA_LINES` | `100` | Market data entitlement baseline for pacing analysis |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis URL |
| `REDIS_PASSWORD` | empty | Optional Redis AUTH password |
| `QUESTDB_HOST` | `127.0.0.1` | QuestDB PostgreSQL wire host |
| `QUESTDB_PORT` | `8812` | QuestDB PostgreSQL wire port |
| `QUESTDB_USER` | `admin` | QuestDB user |
| `QUESTDB_PASSWORD` | `quest` | QuestDB password |
| `QUESTDB_DATABASE` | `qdb` | QuestDB database |
| `INDEX_SYNC_INTERVAL_SECONDS` | `86400` | Default index reload interval |
| `INDEX_COMPOSITION_PROVIDER` | empty | External index constituent provider name |
| `IBKR_REST_APP_NAME` | `IBKRRestApp` | FastAPI title |
| `IBKR_REST_CONNECT_ON_STARTUP` | `false` | Connect transports during API startup |
| `IBKR_REST_MARKET_DATA_TTL_SECONDS` | `5` | REST market-data TTL cache default |
| `IBKR_REST_MARKET_DATA_CACHE_MAXSIZE` | `512` | REST TTL cache max entries |

## Local Commands

```bash
make install-dev
make services-up
make test
make notebook
make run
make run-api
make services-down
```

`make run` starts the Redis-backed scheduler worker. `make run-api` starts `IBKRRestApp`.

Equivalent direct API command:

```bash
python -m uvicorn src.webapp.app:get_app --host 0.0.0.0 --port 8000 --factory --reload --loop asyncio
```

The scheduler worker is dependency-aware:

- market snapshot jobs connect to IBKR
- market snapshot jobs with `persist=true` also connect to QuestDB
- index-composition-only jobs do not connect to IBKR or QuestDB
- unsupported job types are skipped with a warning instead of crashing the worker
- SIGINT/SIGTERM request graceful scheduler shutdown

## Docker

Build and run the API, Redis, and QuestDB:

```bash
cp .env.example .env
docker compose up -d --build ibkr-rest-app
```

Useful commands:

```bash
make docker-build
make docker-up
docker compose logs -f ibkr-rest-app
docker compose down
```

The Compose API service defaults `IBKR_HOST` to `host.docker.internal`, which lets the container reach TWS or IB Gateway running on the host machine through Docker Desktop. Override `IBKR_HOST` if IB Gateway runs elsewhere.

## REST API

`IBKRRestApp` is a thin async HTTP bridge over the Pydantic DTOs in `src/feeds`. It keeps business logic in the feed/transport layer and exposes a clean API surface for notebooks, dashboards, services, and internal tools.

Main route groups:

- `/api/v1/system/*`: health and TTL cache controls
- `/api/v1/market-data/*`: OHLCV, latest Redis bars, option analytics, bond yield history
- `/api/v1/reference-data/*`: option chains, fundamentals, WSH events, news
- `/api/v1/account/*`: account summary, live positions, portfolio, PnL snapshots

Common endpoints:

```text
GET  /api/v1/system/health
GET  /api/v1/system/cache/market-data
POST /api/v1/market-data/ohlcv
GET  /api/v1/market-data/latest-bar
POST /api/v1/market-data/options/analytics
POST /api/v1/market-data/bonds/yields/history
POST /api/v1/reference-data/options/chains
POST /api/v1/reference-data/fundamentals
GET  /api/v1/reference-data/news/providers
GET  /api/v1/account/summary
GET  /api/v1/account/positions
GET  /api/v1/account/portfolio
POST /api/v1/account/pnl/account
POST /api/v1/account/pnl/position
```

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

The REST market-data cache is process-local and short-lived. It includes per-key single-flight protection so concurrent duplicate requests share one IBKR call. Redis remains the distributed cache for latest bars and scheduler/rate-limit state.

## Scheduler Jobs

Redis scheduler jobs are JSON payloads stored under:

```text
SchedulerJob::<job_name>
```

Operational JSON files live in `schedulejob/`.

Load the default G10 index composition reload job:

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

## Redis Keys

Latest OHLCV bar:

```text
MarketData::<asset_class>::<bar_size>:latest
```

Index composition:

```text
GlobalIndex:<INDEX_SYMBOL>:composition
```

IBKR historical pacing bookmarks:

```text
IBKRRateLimit:historical:window
IBKRRateLimit:historical:identical:<request_hash>
IBKRRateLimit:historical:same_contract:<contract_hash>
```

## Verification

```bash
python3 -m pytest -q
python3 -m compileall -q src tests
python3 -m json.tool schedulejob/reload_g10_index_composition.json >/dev/null
```

Current expected test status:

```text
64 passed
```

## Important Quant And IBKR Caveats

- IBKR historical pacing is enforced in-process and through Redis bookmarks.
- Current index compositions are not point-in-time historical constituents.
- IBKR bond yield historical fields are documented, but yield history is only available for corporate bonds.
- CTD analytics require exchange/vendor delivery-basket data beyond the IBKR TWS API.
- Full option chains should not be requested as market data in one shot. Discover the chain, filter contracts, then request selected analytics.
- REST PnL endpoints use short-lived subscriptions; durable streaming PnL should live in a dedicated risk-engine process.

## More Detail

Read [PROJECT_SETUP_ARCHITECTURE.md](PROJECT_SETUP_ARCHITECTURE.md) for the full architecture, IBKR pacing notes, job JSON schema, Docker setup, and implementation caveats.
