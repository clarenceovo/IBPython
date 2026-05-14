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
    settings.py             # pydantic-settings loader
  feeds/
    models.py               # OHLCVBar, OHLCVRequest, AssetClass
    contracts.py            # Vendor-neutral contract specs -> IBKR mapping
    ibkr_feed.py            # Async IBKR historical feed client
    ohlcv_loader.py         # Feed -> normalize -> persist/cache orchestration
    index_composition.py    # Provider abstraction and Redis-backed sync service
  transport/
    redis_client.py         # Latest bar, index composition, scheduler job storage
    ibkr_rate_limit.py      # Redis-backed distributed IBKR pacing bookmarks
    questdb_client.py       # PostgreSQL wire client and SQL builders
    scheduler.py            # Generic async scheduler and market snapshot job handler
schedulejob/
  reload_g10_index_composition.json
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

6. Open notebooks:

```bash
make notebook
```

## Configuration

All central defaults live in `src/config/config_constant.py`. Runtime settings are loaded by `src/config/settings.py` from `.env` and environment variables.

| Variable | Default | Purpose |
|---|---:|---|
| `IBKR_HOST` | `127.0.0.1` | TWS or IB Gateway host |
| `IBKR_PORT` | `7497` | IBKR API port |
| `IBKR_CLIENT_ID` | `101` | IBKR client ID |
| `IBKR_MARKET_DATA_LINES` | `100` | Entitlement baseline used for pacing analysis |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `QUESTDB_HOST` | `127.0.0.1` | QuestDB PostgreSQL wire host |
| `QUESTDB_PORT` | `8812` | QuestDB PostgreSQL wire port |
| `QUESTDB_USER` | `admin` | QuestDB user |
| `QUESTDB_PASSWORD` | `quest` | QuestDB password |
| `QUESTDB_DATABASE` | `qdb` | QuestDB database |
| `INDEX_SYNC_INTERVAL_SECONDS` | `86400` | Default index composition sync interval |
| `INDEX_COMPOSITION_PROVIDER` | empty | Enables an external index composition provider when implemented |

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

## IBKR Pacing And Rate-Limit Standard

This section caches the IBKR rate-limit assumptions used by this project as of the documentation reviewed on 2026-05-14.

Primary sources:

- [IBKR Campus TWS API Documentation](https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/)
- [IBKR Campus Web API v1.0 Documentation](https://ibkrcampus.com/campus/ibkr-api-page/cpapi-v1/)
- [IBKR Campus Market Data Subscriptions](https://ibkrcampus.com/campus/ibkr-api-page/market-data-subscriptions/)
- [Legacy TWS API Historical Data Limitations](https://interactivebrokers.github.io/tws-api/historical_limitations.html)

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
