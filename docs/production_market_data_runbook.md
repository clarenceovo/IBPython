# Production Market Data Runbook

## IBKR Controls

- Equity snapshots use IBKR one-shot market data semantics with `snapshot=True`, no generic tick list, and `regulatorySnapshot=False`.
- Default snapshot wait is `IBKR_EQUITY_SNAPSHOT_WAIT_SECONDS=11.5`, matching IBKR's documented snapshot window.
- Snapshot market-data-line leases default to `IBKR_EQUITY_SNAPSHOT_LEASE_TTL_SECONDS=30`.
- Historical OHLCV requests are auto-chunked when `duration` exceeds the IBKR max for `bar_size`.
- `IBKR_HISTORICAL_MAX_CHUNKS` defaults to `60`; requests above the cap are rejected before any IBKR call.

## Operational Signals

- Scrape `/metrics` from the FastAPI service.
- Watch these counters:
  - `market_data_snapshot_total`
  - `market_data_snapshot_cleanup_failures_total`
  - `market_data_historical_auto_chunks_total`
  - `market_data_historical_chunks_total`
  - `market_data_historical_bars_total`
  - `market_data_quality_failures_total`
- Metrics avoid raw-symbol labels to keep cardinality bounded.
- Logs include operation, asset class, symbol, con id when available, auto-chunk status, estimated chunks, and cleanup status.

## Live Smoke Checks

Live smoke checks are intentionally opt-in and should not run in normal CI.

```bash
IBPYTHON_LIVE_SMOKE=1 pytest tests/smoke/test_live_market_data_smoke.py -q
```

Required services:

- TWS or IB Gateway reachable through `IBKR_HOST`, `IBKR_PORT`, and `IBKR_CLIENT_ID`.
- Redis reachable through `REDIS_URL`.
- QuestDB reachable through `QUESTDB_HOST`, `QUESTDB_PORT`, `QUESTDB_USER`, `QUESTDB_PASSWORD`, and `QUESTDB_DATABASE`.

## Incident Triage

- IBKR error `162` / HMDS no data: confirm the contract is active, entitled, and has historical data for the requested date range. Expired futures may require an `end_datetime` near the expiry.
- Auto-chunk rejection: reduce requested duration, increase bar size, or raise `IBKR_HISTORICAL_MAX_CHUNKS` only after confirming IBKR pacing budget.
- Snapshot quality failures: inspect bid/ask crossing, missing usable price fields, negative sizes/volume, and delayed/closed-market conditions.
- Cleanup failures: check `market_data_snapshot_cleanup_failures_total`; repeated failures can exhaust market-data-line budget.
