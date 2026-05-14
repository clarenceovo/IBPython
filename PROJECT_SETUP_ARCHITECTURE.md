# Market Feed + Transport Layer

Production-oriented async market data foundation for systematic trading, research, and portfolio analytics.

## Design Assumptions

- Python target: 3.13+.
- Timestamp standard: all normalized OHLCV bars use UTC-aware timestamps.
- Bar semantics: timestamps represent the provider bar timestamp returned by IBKR, normalized to UTC.
- External systems: IBKR TWS or IB Gateway runs locally; Redis and QuestDB can run through Docker Compose.
- Reliability posture: live connections retry with exponential backoff, scheduled jobs isolate failures, and tests mock external systems.
- Research caution: index composition snapshots are provider-dependent and can introduce survivorship bias if reused as historical truth without point-in-time history.

## Architecture

```text
src/
  config/
    config_constant.py      # Central defaults, env names, Redis key templates, QuestDB table name
    config_loader.py        # .env/environment/default loader that ignores blank values
    settings.py             # Pydantic validation over ConfigLoader output
  feeds/
    models.py               # OHLCVBar, OHLCVRequest, AssetClass
    contracts.py            # Vendor-neutral contract specs -> IBKR mapping
    account.py              # Account, portfolio, live position, and PnL DTOs
    bonds.py                # Bond yield, CTD, and yield curve DTOs
    fundamental_data.py     # IBKR fundamental, WSH event, and forecast/event data contracts
    ibkr_feed.py            # Async IBKR historical feed client
    news.py                 # IBKR news provider/headline/article/bulletin contracts
    options.py              # Option analytics DTOs and IBKR option contract mapping
    ohlcv_loader.py         # Feed -> normalize -> persist/cache orchestration
    index_composition.py    # Provider abstraction and Redis-backed sync service
  transport/
    redis_client.py         # Latest bar, index composition, scheduler job storage
    ibkr_rate_limit.py      # Redis-backed distributed IBKR pacing bookmarks
    questdb_client.py       # PostgreSQL wire client and SQL builders
    scheduler.py            # Generic async scheduler and market snapshot job handler
  webapp/
    app.py                  # IBKRRestApp FastAPI application factory
    dependencies.py         # Shared async clients, loader, and cache state
    cache.py                # Async TTL cache with per-key single-flight protection
    routers/
      account.py            # Account summary, live positions, portfolio, PnL snapshots
      market_data.py        # OHLCV, latest Redis bar, options analytics, bond yields
      reference_data.py     # Option chains, fundamentals, WSH events, news
      system.py             # Health and cache operations
schedulejob/
  reload_g10_index_composition.json
Dockerfile
docker-compose.yml
tests/
notebooks/
```

## Quick Start

1. Create the virtual environment:

```bash
make install-dev
```

If `python3.13` is not on your `PATH`, override it:

```bash
make install-dev PYTHON=/path/to/python3.13
```

2. Create your local environment file:

```bash
cp .env.example .env
```

3. Start Redis and QuestDB:

```bash
make services-up
```

QuestDB UI: http://localhost:9000

4. Start IBKR TWS or IB Gateway locally.

Paper trading defaults usually use:

- `IBKR_HOST=127.0.0.1`
- `IBKR_PORT=7497`
- `IBKR_CLIENT_ID=101`

Live trading commonly uses port `7496`. Confirm your TWS or Gateway API settings before running live jobs.

5. Run tests:

```bash
make test
```

6. Start the REST API locally:

```bash
make run-api
```

FastAPI docs: http://localhost:8000/docs

Health check:

```bash
curl http://localhost:8000/api/v1/system/health
```

7. Open notebooks:

```bash
make notebook
```

## Docker Setup

Build and run the API with Redis and QuestDB:

```bash
cp .env.example .env
docker compose up -d --build ibkr-rest-app
```

The Compose service exposes:

- FastAPI: http://localhost:8000/docs
- QuestDB UI: http://localhost:9000
- Redis: `localhost:6379`

Inside Docker, `IBKR_HOST` defaults to `host.docker.internal` so the container can reach TWS or IB Gateway running on the host machine. If you run IB Gateway in another container or remote host, override `IBKR_HOST` in `.env`.

Useful commands:

```bash
make docker-build
make docker-up
docker compose logs -f ibkr-rest-app
docker compose down
```

## Configuration

All central defaults live in `src/config/config_constant.py`. Runtime settings are loaded by `src/config/settings.py` from `.env` and environment variables.

`src/config/config_loader.py` owns the load order:

```text
config_constant defaults -> .env values -> process environment -> explicit overrides
```

Blank or missing `.env` values are treated as null and skipped, so the corresponding `config_constant.py` default remains active. Use `load_settings()` in apps, notebooks, jobs, and scripts instead of calling `python-dotenv` directly.

| Variable | Default | Purpose |
|---|---:|---|
| `IBKR_HOST` | `127.0.0.1` | TWS or IB Gateway host |
| `IBKR_PORT` | `7497` | IBKR API port |
| `IBKR_CLIENT_ID` | `101` | IBKR client ID |
| `IBKR_MARKET_DATA_LINES` | `100` | Entitlement baseline used for pacing analysis |
| `IBKR_REST_APP_NAME` | `IBKRRestApp` | FastAPI application title |
| `IBKR_REST_CONNECT_ON_STARTUP` | `false` | Connect to IBKR/Redis/QuestDB during API startup instead of first request |
| `IBKR_REST_MARKET_DATA_TTL_SECONDS` | `5` | Default in-process TTL for REST market data snapshots |
| `IBKR_REST_MARKET_DATA_CACHE_MAXSIZE` | `512` | Maximum in-process REST market data cache entries |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `QUESTDB_HOST` | `127.0.0.1` | QuestDB PostgreSQL wire host |
| `QUESTDB_PORT` | `8812` | QuestDB PostgreSQL wire port |
| `QUESTDB_USER` | `admin` | QuestDB user |
| `QUESTDB_PASSWORD` | `quest` | QuestDB password |
| `QUESTDB_DATABASE` | `qdb` | QuestDB database |
| `INDEX_SYNC_INTERVAL_SECONDS` | `86400` | Default index composition sync interval |
| `INDEX_COMPOSITION_PROVIDER` | empty | Enables an external index composition provider when implemented |

## IBKRRestApp FastAPI Bridge

`IBKRRestApp` is a thin async HTTP bridge over the domain DTOs in `src/feeds`. It does not duplicate trading logic; it validates request bodies with the same Pydantic models used by notebooks, schedulers, and batch workers.

Run locally:

```bash
uvicorn src.webapp.app:app --host 0.0.0.0 --port 8000 --reload
```

Router split:

- `GET /api/v1/system/health`
- `GET /api/v1/system/cache/market-data`
- `DELETE /api/v1/system/cache/market-data`
- `POST /api/v1/market-data/ohlcv`
- `GET /api/v1/market-data/latest-bar`
- `POST /api/v1/market-data/options/analytics`
- `POST /api/v1/market-data/bonds/yields/history`
- `POST /api/v1/reference-data/options/chains`
- `POST /api/v1/reference-data/fundamentals`
- `GET /api/v1/reference-data/wsh/metadata`
- `POST /api/v1/reference-data/wsh/events`
- `GET /api/v1/reference-data/news/providers`
- `POST /api/v1/reference-data/news/historical`
- `POST /api/v1/reference-data/news/article`
- `GET /api/v1/account/summary`
- `GET /api/v1/account/positions`
- `GET /api/v1/account/portfolio`
- `POST /api/v1/account/pnl/account`
- `POST /api/v1/account/pnl/position`

The app uses async FastAPI endpoints end-to-end. Market-data endpoints can use the in-process `AsyncTTLCache`; it has per-key single-flight protection so concurrent duplicate requests share one IBKR call. The TTL cache is intentionally short-lived and local to the API process. Redis remains the distributed cache for latest bars, index compositions, and scheduler/rate-limit bookmarks.

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

## Redis Keys

Latest OHLCV bar:

```text
MarketData::<asset_class>::<bar_size>:latest
```

Example:

```text
MarketData::equity::1_min:latest
```

Index composition:

```text
GlobalIndex:<INDEX_SYMBOL>:composition
```

Examples:

```text
GlobalIndex:SPX:composition
GlobalIndex:NDX:composition
```

Scheduler job:

```text
SchedulerJob::<job_name>
```

## Redis-Defined Market Snapshot Jobs

Scheduler jobs are JSON payloads stored in Redis. Python code registers handlers for known `job_type` values; Redis stores job configuration, not executable code.

Example job:

```json
{
  "name": "snapshot_spy_1m",
  "job_type": "market_snapshot",
  "interval_seconds": 60,
  "enabled": true,
  "run_immediately": true,
  "params": {
    "symbol": "SPY",
    "asset_class": "equity",
    "exchange": "SMART",
    "currency": "USD",
    "duration": "1 D",
    "bar_size": "1 min",
    "what_to_show": "TRADES",
    "use_rth": true,
    "persist": true,
    "cache_latest": true
  }
}
```

Add it with `redis-cli`:

```bash
redis-cli SET SchedulerJob::snapshot_spy_1m '{
  "name":"snapshot_spy_1m",
  "job_type":"market_snapshot",
  "interval_seconds":60,
  "enabled":true,
  "run_immediately":true,
  "params":{
    "symbol":"SPY",
    "asset_class":"equity",
    "exchange":"SMART",
    "currency":"USD",
    "duration":"1 D",
    "bar_size":"1 min",
    "what_to_show":"TRADES",
    "use_rth":true,
    "persist":true,
    "cache_latest":true
  }
}'
```

Then run:

```bash
make run
```

## Schedule Job Definitions

The root-level `schedulejob/` folder stores Redis scheduler job payloads. These files are operational configuration, not importable Python modules.

Load the G10 index composition reload job into Redis:

```bash
redis-cli SET SchedulerJob::reload_g10_index_composition "$(cat schedulejob/reload_g10_index_composition.json)"
```

The job JSON shape is:

```json
{
  "name": "reload_g10_index_composition",
  "job_type": "index_composition_reload",
  "interval_seconds": 86400,
  "enabled": true,
  "run_immediately": true,
  "params": {
    "index_symbols": ["SPX", "TSX", "FTSE100", "DAX", "CAC40", "FTSEMIB", "NIKKEI225", "AEX", "BEL20", "OMXS30", "SMI"],
    "provider": "configured_provider"
  }
}
```

Field meanings:

- `name`: stable scheduler job name and Redis key suffix.
- `job_type`: selects the Python handler; `index_composition_reload` calls `IndexCompositionService.sync_many(...)`.
- `interval_seconds`: repeat interval; the bundled file uses the default `INDEX_SYNC_INTERVAL_SECONDS` value of `86400`.
- `enabled`: disabled jobs are ignored when loading from Redis.
- `run_immediately`: run once on scheduler startup before waiting for the first interval.
- `params.index_symbols`: default G10 headline equity index universe from `src/config/config_constant.py`.
- `params.provider`: placeholder marker; replace this when a production constituent provider is implemented and configured.

IBKR does not provide a direct index composition endpoint. The documented IBKR methods can resolve index contracts/security definitions and request market or historical data, but not constituents or index weights. The reload job is therefore provider-neutral and intentionally fails loudly unless a dedicated constituent provider is registered. Use point-in-time constituent data for research; current reloads are not historical membership truth.

## IBKR Index And Option Chain Capabilities

IBKR can qualify index contracts with `secType="IND"` and can load market/historical data for subscribed index instruments. IBKR also exposes option-chain metadata through `reqSecDefOptParams`, which is the preferred method for discovering stock and index option expirations, strikes, trading classes, multipliers, and exchanges after resolving the underlying `conId`.

This project implements `IBKRFeedClient.load_option_chains(...)` for equity and index underlyings. It deliberately does not use broad `reqContractDetails` sweeps for chains because IBKR documents `reqSecDefOptParams` as the option-chain endpoint and notes that broad option-chain contract-detail requests can be throttled and slow.

## IBKR Options Analytics

Options analytics are represented in `src/feeds/options.py`:

- `OptionContractSpec`: one specific option contract, including underlying, expiry, strike, right, multiplier, trading class, and optional `conId`.
- `OptionAnalyticsRequest`: snapshot request using IBKR generic ticks `100`, `101`, `104`, `105`, and `106`.
- `OptionGreekSet`: bid, ask, last, or model Greek payload.
- `OptionAnalyticsSnapshot`: delta, gamma, theta, vega, implied volatility, historical volatility, open interest, and option volume.

IBKR exposes option Greeks through option computation market data fields such as `bidGreeks`, `askGreeks`, `lastGreeks`, and `modelGreeks` in `ib_insync`. Option volume/open-interest style values require generic ticks. The project default asks for:

| Generic tick | Meaning |
|---:|---|
| `100` | Option volume |
| `101` | Option open interest |
| `104` | Historical volatility |
| `105` | Average option volume |
| `106` | Option implied volatility |

Do not request a full option chain as market data in one shot. Use `load_option_chains(...)` to discover expiries/strikes/trading classes, filter to the slice you need, then call `load_option_analytics(...)` only for selected contracts.

REST endpoint:

```text
POST /api/v1/market-data/options/analytics
```

## IBKR Fundamental And Economic Data

IBKR has three relevant API surfaces:

- TWS `reqFundamentalData`: legacy/deprecated company fundamental reports returned as XML. Supported report types in this project are `ReportSnapshot`, `ReportsFinSummary`, `ReportRatios`, `ReportsFinStatements`, `ReportsOwnership`, `CalendarReport`, and `RESC`.
- TWS Wall Street Horizon: corporate/event calendar metadata and events returned as JSON through `getWshMetaDataAsync` and `getWshEventDataAsync` in `ib_insync`. This requires a Wall Street Horizon subscription, and metadata must be requested before event data.
- Client Portal Forecast/Event Contracts: economic-indicator-related tradable event contract metadata such as category trees. This is not a macroeconomic time-series endpoint.

This project stores these as explicit data contracts in `src/feeds/fundamental_data.py`:

- `FundamentalDataRequest` and `FundamentalDataReport`
- `WSHEventDataRequest`, `WSHMetadataReport`, and `WSHEventDataReport`
- `ForecastEventContractCategory`

Quant caveat: IBKR fundamental reports and WSH events are vendor payloads with their own licensing and revision behavior. Preserve raw XML/JSON and timestamps before deriving factors, and do not treat Forecast/Event Contract metadata as released macroeconomic observations.

## IBKR News Feed

IBKR offers API news, subject to API-specific news subscriptions and provider entitlements. The TWS API supports:

- `reqNewsProviders`: list subscribed news providers.
- Contract-specific real-time news through `reqMktData` using generic tick `292`.
- BroadTape news through `NEWS` contracts and generic tick `292`.
- `tickNews`: real-time headline callback containing timestamp, provider, article id, headline, and extra data.
- `reqHistoricalNews`: historical headline lookup by contract id and provider codes.
- `reqNewsArticle`: article body lookup by provider code and article id.
- `reqNewsBulletins`: IBKR system/news bulletins.

This project stores those wire shapes in `src/feeds/news.py`:

- `NewsProvider`
- `HistoricalNewsRequest` and `HistoricalNewsHeadline`
- `NewsArticleRequest` and `NewsArticle`
- `NewsTick`
- `NewsBulletin`

`IBKRFeedClient` has one-shot helpers for providers, historical headlines, and article body lookup. Real-time news subscriptions should be added as a separate streaming layer because they are long-lived market data subscriptions and consume the account’s market data/news entitlements.

## Bond Data, CTD, And Yield Curves

Bond and yield-curve DTOs live in `src/feeds/bonds.py`.

Supported DTOs:

- `BondInstrument`: sovereign/corporate bond identity using symbol, ISIN, CUSIP, or IBKR `conId`.
- `BondYieldQuote`: latest bid, ask, last, and computed mid yield.
- `BondYieldHistoryRequest` and `BondYieldBar`: historical yield bars for `YIELD_BID`, `YIELD_ASK`, `YIELD_BID_ASK`, and `YIELD_LAST`.
- `CTDFutureDefinition`, `CTDBondCandidate`, and `CTDBondSnapshot`: CTD delivery-basket analytics for US Treasury, JGB, KTB, and German Bund futures.
- `YieldCurveBootstrapInstrument`, `YieldCurvePoint`, and `YieldCurveDTO`: par-yield curve inputs and bootstrapped discount/zero curve outputs.

IBKR historical bar documentation lists yield fields for bonds, but the same table notes that yield historical data is only available for corporate bonds. For US Treasury, JGB, KTB, and German Bund CTD capture, IBKR should be treated as a quote/contract source only. The actual deliverable basket, conversion factor, accrued interest, delivery date, financing/carry, and CTD selection require an exchange, vendor, or internally controlled delivery-basket provider.

`YieldCurveDTO.bootstrap()` uses a deterministic par-yield bootstrap with ACT/365F year fractions, regular coupon dates, continuous zero rates, and log-linear discount-factor interpolation. `YieldCurveDTO.bootscrape()` is included as a backward-compatible alias for the requested method name.

REST endpoint:

```text
POST /api/v1/market-data/bonds/yields/history
```

## IBKR Account, Position, And PnL Data

Account/risk DTOs live in `src/feeds/account.py`.

Supported DTOs:

- `AccountValueDTO`
- `AccountSummaryDTO`
- `LivePositionDTO`
- `PortfolioItemDTO`
- `AccountPnLDTO`
- `PositionPnLDTO`

`IBKRFeedClient` exposes:

- `load_account_summary(...)`
- `load_live_positions(...)`
- `load_portfolio_items(...)`
- `subscribe_account_pnl(...)`
- `subscribe_position_pnl(...)`
- `load_account_pnl_snapshot(...)`
- `load_position_pnl_snapshot(...)`

The REST API uses short-lived PnL snapshot helpers for HTTP calls, then cancels the IBKR PnL subscription. Long-lived streaming PnL should be handled by a dedicated risk-engine service, not by holding open normal REST requests.

IBKR caveats:

- Account Window PnL and Portfolio Window PnL can differ because they use different sources and reset schedules.
- `reqPnL` and `reqPnLSingle` update roughly once per second, subject to IBKR changes.
- Some PnL values can be unset max-double sentinels; the DTO normalizers convert those to `None`.
- Virtual FX PnL behavior depends on TWS Account Window configuration.

REST endpoints:

```text
GET  /api/v1/account/summary
GET  /api/v1/account/positions
GET  /api/v1/account/portfolio
POST /api/v1/account/pnl/account
POST /api/v1/account/pnl/position
```

## IBKR Pacing And Rate-Limit Standard

This section caches the IBKR rate-limit assumptions used by this project as of the documentation reviewed on 2026-05-14.

Primary sources:

- [IBKR Campus TWS API Documentation](https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/)
- [IBKR Campus Web API v1.0 Documentation](https://ibkrcampus.com/campus/ibkr-api-page/cpapi-v1/)
- [IBKR Campus Market Data Subscriptions](https://ibkrcampus.com/campus/ibkr-api-page/market-data-subscriptions/)
- [Legacy TWS API Historical Data Limitations](https://interactivebrokers.github.io/tws-api/historical_limitations.html)
- [Legacy TWS API Historical Bar Data](https://interactivebrokers.github.io/tws-api/historical_bars.html)
- [Legacy TWS API Options](https://interactivebrokers.github.io/tws-api/options.html)
- [Legacy TWS API Profit And Loss](https://interactivebrokers.github.io/tws-api/pnl.html)
- [Legacy TWS API Available Tick Types](https://interactivebrokers.github.io/tws-api/tick_types.html)
- [Legacy TWS API News](https://interactivebrokers.github.io/tws-api/news.html)
- [Legacy TWS API EClient Reference](https://interactivebrokers.github.io/tws-api/classIBApi_1_1EClient.html)
- [ib_insync API Reference](https://ib-insync.readthedocs.io/api.html)

Documented standards:

| Area | IBKR Standard | Project Control |
|---|---:|---|
| General API request pacing | Maximum requests per second equals maximum market data lines divided by 2. Default 100 lines implies 50 requests/second. | Snapshot jobs are periodic; no general high-frequency request loop is created. |
| Market data lines | Default minimum is 100 concurrent real-time market data lines, shared across TWS watchlists and API clients. | Current implementation uses historical snapshots, not persistent streaming lines. Future streaming feeds must track active subscriptions. |
| Historical small-bar pacing | Avoid identical historical requests inside 15 seconds. | `IBKRHistoricalPacingGuard` enforces identical request cooldown. |
| Historical same-contract burst | Avoid six or more historical requests for the same contract, exchange, and tick type within 2 seconds. | Guard allows at most five in the 2-second window. |
| Historical rolling window | Avoid more than 60 historical requests in 10 minutes. `BID_ASK` counts twice. | Guard enforces 60 weighted requests per 600 seconds. |
| Real-time bars | 5-second real-time bars combine top-of-book line limits and historical pacing; no more than 60 new requests in 600 seconds. | Not implemented yet; use the same guard before adding real-time bars. |
| Tick-by-tick and depth | Specialized limits scale with market data lines; default examples show 5 tick-by-tick and 3 depth streams at 100 lines. | Out of scope for this OHLCV foundation; must be separately budgeted. |

Will this setup hit IBKR pacing limits?

For the default snapshot design, it should not hit limits if you keep the number of Redis `market_snapshot` jobs conservative. The implementation sends one historical request per snapshot job run, then lets `IBKRHistoricalPacingGuard` throttle bursts across all jobs sharing the same `IBKRFeedClient`.

In `main.py`, the system uses `RedisIBKRHistoricalPacingGuard`, which stores pacing bookmarks in Redis with atomic Lua checks. This is safer than a pure in-memory limiter because notebooks, background workers, and multiple scheduler processes share the same request budget.

Redis pacing keys:

```text
IBKRRateLimit:historical:window
IBKRRateLimit:historical:identical:<request_hash>
IBKRRateLimit:historical:same_contract:<contract_hash>
```

The limiter uses sorted sets for rolling windows and short-lived keys for identical-request cooldowns. If Redis is unavailable, the guard falls back to the local in-process limiter so the application fails conservatively instead of sending an unbounded burst.

Capacity rule of thumb:

```text
safe_jobs_at_interval <= floor(60 * interval_seconds / 600)
```

Examples:

| Job interval | Approximate safe historical jobs |
|---:|---:|
| 60 seconds | 6 jobs |
| 120 seconds | 12 jobs |
| 300 seconds | 30 jobs |
| 600 seconds | 60 jobs |

Use lower limits for `BID_ASK` because IBKR counts each request twice. For example, at a 60-second interval, treat the safe budget as roughly 3 `BID_ASK` jobs.

Operational recommendations:

- Prefer `bar_size="1 min"` or larger for regular snapshot jobs.
- Stagger job intervals or use different start times when scaling beyond a handful of symbols.
- Keep TWS watchlists small during API runs, because watchlists consume market data lines too.
- For large historical backfills, add a dedicated chunked backfill worker with durable progress state instead of using high-frequency scheduler jobs.
- If your strategy needs broad real-time streaming, use a proper market data vendor or explicitly budget IBKR market data lines and subscriptions before relying on TWS API.

## QuestDB Schema

Main table: `market_ohlcv`

The table is timestamped on `timestamp`, partitioned by day, and stores metadata as JSON text. Query builders are parameterized to avoid unsafe string interpolation.

## Quant Engineering Notes

- Treat IBKR historical data as vendor data, not ground truth. Corporate actions, roll methodology, adjusted versus unadjusted series, and trading calendar gaps need explicit treatment before research conclusions.
- Do not use current index constituents as if they were historical constituents. For index research, store point-in-time composition snapshots and label provider/as-of metadata.
- Keep all derived factor logic outside the feed adapter. The feed layer should deliver timestamp-safe normalized bars; research modules can decide alignment, forward-fill, and lagging policy.
- For live trading, add monitoring around gateway health, request pacing, missed bars, Redis staleness, and QuestDB insert lag before attaching execution.
