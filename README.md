# IBPython Market Data Foundation

Async, production-shaped IBKR market data and transport layer for systematic trading research, live risk monitoring, and portfolio analytics.

The project provides:

- IBKR TWS / IB Gateway connectivity through `ib_insync`
- Unified OHLCV DTOs for equities, FX, futures, bonds, indices, crypto, and options
- Option chain and option analytics DTOs
- Fundamental, economic event, and news DTOs
- Bond yield, CTD, and yield-curve bootstrap DTOs
- Account summary, live position, portfolio, and PnL DTOs
- Redis latest-data cache, scheduler jobs, and distributed IBKR pacing bookmarks
- QuestDB persistence over PostgreSQL wire protocol
- FastAPI app: `IBKRRestApp`
- Notebook workflows for debugging and research

## Quick Start

Use Python 3.13+.

```bash
make install-dev
cp .env.example .env
make services-up
make test
```

Configuration is loaded through `src.config.settings.load_settings()`. The loader reads defaults from `src/config/config_constant.py`, then `.env`, then process environment variables. Blank `.env` values are ignored and fall back to defaults.

Start IBKR TWS or IB Gateway locally, then run the REST API:

```bash
make run-api
```

Open:

- FastAPI docs: http://localhost:8000/docs
- Health check: http://localhost:8000/api/v1/system/health
- QuestDB UI: http://localhost:9000

## Docker

```bash
cp .env.example .env
docker compose up -d --build ibkr-rest-app
```

The API container uses `host.docker.internal` by default for `IBKR_HOST`, which works for TWS or IB Gateway running on the host machine through Docker Desktop.

## Core Commands

```bash
make test
make notebook
make run
make run-api
make docker-up
```

## REST API Surface

Main route groups:

- `/api/v1/market-data/*`: OHLCV, latest Redis bars, option analytics, bond yield history
- `/api/v1/reference-data/*`: option chains, fundamentals, Wall Street Horizon events, news
- `/api/v1/account/*`: account summary, live positions, portfolio, account PnL, position PnL
- `/api/v1/system/*`: health and TTL cache management

The REST app is async and uses a short in-process TTL cache for pacing-sensitive market-data snapshots. Redis remains the distributed cache and scheduler/rate-limit state store.

## Scheduler Jobs

Operational scheduler JSON lives in `schedulejob/`.

Load the default G10 index reload job:

```bash
redis-cli SET SchedulerJob::reload_g10_index_composition "$(cat schedulejob/reload_g10_index_composition.json)"
```

Index composition providers are intentionally placeholder-only until a production constituent vendor is selected. IBKR does not expose current index constituents/weights through the TWS API.

## Project Notes

Read [PROJECT_SETUP_ARCHITECTURE.md](PROJECT_SETUP_ARCHITECTURE.md) for the full architecture, IBKR pacing assumptions, Redis key formats, job JSON shape, Docker details, and quant caveats.

Important caveats:

- IBKR historical pacing is enforced in process and through Redis bookmarks.
- Current index composition snapshots are not point-in-time historical constituents.
- IBKR bond yield historical fields are documented for bonds, but yield history is only available for corporate bonds.
- CTD analytics require exchange/vendor delivery-basket data beyond the IBKR TWS API.
- REST PnL endpoints use short-lived subscriptions; durable streaming PnL should run in a dedicated risk-engine process.
