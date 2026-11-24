from __future__ import annotations

import asyncio
import logging
import time as monotonic_time
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, status
from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing import Annotated

from src.config import config_constant as constants
from src.feeds.contracts import ContractSpec
from src.feeds.exchange_resolver import resolve_equity
from src.feeds.options import DEFAULT_OPTION_ANALYTICS_GENERIC_TICKS, OptionContractSpec, OptionRight
from src.feeds.snapshotter import (
    EquitySnapshot,
    FXOptionSnapshot,
    FXOptionSnapshotQuery,
    SnapshotQuery,
    SnapshotResult,
    SnapshotWatchlist,
    fx_pair_parts,
    ticker_to_snapshot,
    validate_equity_snapshot_quality,
)
from src.transport.metrics import metrics
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/snapshot", tags=["market-data"])


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class CaptureSnapshotsRequest(BaseModel):
    """Capture point-in-time snapshots for a list of symbols."""

    model_config = ConfigDict(extra="forbid")

    symbols: list[str] = Field(min_length=1, description="List of equity symbols to snapshot")
    persist: bool = Field(default=False, description="Deprecated: API snapshot persistence is disabled; use the scheduler snapshotter")
    cache_latest: bool = Field(default=True, description="Cache latest snapshots in Redis")

    @classmethod
    def from_watchlist(cls, watchlist: SnapshotWatchlist) -> "CaptureSnapshotsRequest":
        return cls(symbols=list(watchlist.symbols))


class WatchlistCreateRequest(BaseModel):
    """Create or update a named watchlist."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(
        min_length=1,
        examples=["us_tech", "hk_large_cap"],
        description="Watchlist name. Stored lower-cased; creating an existing name replaces it.",
    )
    symbols: list[str] = Field(
        min_length=1,
        description="Equity symbols to track, e.g. ['AAPL', 'MSFT'] or ['0700.HK', '9988.HK'].",
    )
    exchange: str = Field(default="SMART", min_length=1, description="Default IBKR exchange for the watchlist symbols.")
    currency: str = Field(default="USD", min_length=1, description="Default IBKR currency for the watchlist symbols.")
    snapshot_interval_seconds: float = Field(
        default=60, gt=0, description="Suggested capture cadence in seconds used by scheduler-driven captures."
    )


class WatchlistCaptureRequest(BaseModel):
    """Capture snapshots for an entire watchlist by name."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, description="Watchlist name to capture.")
    persist: bool = Field(
        default=False,
        description="Deprecated: API snapshot persistence is disabled (501 when true); use the scheduler snapshotter.",
    )
    cache_latest: bool = Field(default=True, description="Cache captured snapshots in Redis as the latest values.")


class FXOptionContractRequest(BaseModel):
    """Pair-style FX option contract for snapshot capture."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=6, examples=["EURUSD"], description="FX pair, e.g. EURUSD or GBPUSD.")
    expiry: str = Field(
        min_length=6,
        examples=["20260619"],
        description="Option expiry as YYYYMMDD (6+ characters), e.g. 20260619.",
    )
    strike: float = Field(gt=0, examples=[1.10], description="Strike price in quote-currency terms, e.g. 1.10.")
    right: str = Field(
        min_length=1,
        examples=["C"],
        description="Option right: C/CALL or P/PUT.",
    )
    exchange: str = Field(default="SMART", min_length=1, description="IBKR exchange; SMART routes FX option quotes.")
    currency: str | None = Field(
        default=None,
        min_length=1,
        description="Optional quote-currency override. Defaults to the pair's right-hand currency.",
    )
    multiplier: str = Field(default="100", min_length=1, description="IBKR contract multiplier as a string, e.g. '100'.")
    trading_class: str | None = Field(default=None, description="Optional IBKR trading class for disambiguation.")
    local_symbol: str | None = Field(
        default=None,
        description="Optional IBKR local symbol (OCC-style) for exact contract disambiguation.",
    )
    con_id: int | None = Field(default=None, gt=0, description="Optional IBKR contract id for exact disambiguation.")

    @classmethod
    def _normalize_right_value(cls, value: object) -> str:
        normalized = str(value).strip().upper()
        if normalized in {"C", "CALL"}:
            return "C"
        if normalized in {"P", "PUT"}:
            return "P"
        raise ValueError("right must be C/CALL or P/PUT")

    @field_validator("right", mode="before")
    @classmethod
    def normalize_right(cls, value: object) -> str:
        return cls._normalize_right_value(value)

    @property
    def pair_parts(self) -> tuple[str, str, str]:
        return fx_pair_parts(self.symbol, self.currency)

    def to_option_contract_spec(self) -> OptionContractSpec:
        _pair, base, quote = self.pair_parts
        return OptionContractSpec(
            sec_type="OPT",
            underlying_symbol=base,
            expiry=self.expiry,
            strike=self.strike,
            right=OptionRight(self._normalize_right_value(self.right)),
            exchange=self.exchange,
            currency=quote,
            multiplier=self.multiplier,
            trading_class=self.trading_class,
            local_symbol=self.local_symbol,
            con_id=self.con_id,
        )


class FXOptionCaptureRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contracts: list[FXOptionContractRequest] = Field(
        min_length=1,
        description="FX option contracts to capture; each needs symbol, expiry, strike, and right.",
    )
    snapshot_wait_seconds: float = Field(
        default=2.0,
        gt=0,
        le=30,
        description="How long to hold the short-lived IBKR market-data subscription while values arrive.",
    )
    generic_ticks: tuple[str, ...] = Field(
        default=DEFAULT_OPTION_ANALYTICS_GENERIC_TICKS,
        description="IBKR generic tick ids requested for Greeks and OI fields.",
    )
    persist: bool = Field(
        default=False,
        description="Deprecated: API snapshot persistence is disabled (501 when true); use the scheduler snapshotter.",
    )
    cache_latest: bool = Field(default=True, description="Cache captured snapshots in Redis as the latest values.")


class FXOptionCaptureResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requested: int = Field(description="Number of contracts requested.")
    captured: int = Field(description="Number of snapshots successfully captured.")
    persisted: int = Field(default=0, description="Always 0: API persistence is disabled.")
    cached: int = Field(description="Number of snapshots written to the Redis latest cache.")
    snapshots: list[FXOptionSnapshot] = Field(description="Captured FX option snapshots.")


class WatchlistDeleteResponse(BaseModel):
    """Confirmation returned after deleting a watchlist."""

    model_config = ConfigDict(extra="forbid")

    status: str = Field(description="Always 'deleted'.")
    name: str = Field(description="Name of the deleted watchlist.")


CAPTURE_SNAPSHOTS_EXAMPLES = {
    "tech_tickers": {
        "summary": "Snapshot major tech stocks",
        "value": {
            "symbols": ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META"],
        },
    },
    "single_symbol": {
        "summary": "Snapshot a single equity",
        "value": {"symbols": ["SPY"]},
    },
}

WATCHLIST_CREATE_EXAMPLES = {
    "us_tech": {
        "summary": "US tech watchlist",
        "value": {
            "name": "us_tech",
            "symbols": ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"],
            "snapshot_interval_seconds": 30,
        },
    },
    "hk_large_cap": {
        "summary": "HK large cap",
        "value": {
            "name": "hk_large_cap",
            "symbols": ["0700.HK", "9988.HK", "0005.HK", "1299.HK", "3690.HK"],
            "exchange": "SEHK",
            "currency": "HKD",
            "snapshot_interval_seconds": 60,
        },
    },
}

FX_OPTION_CAPTURE_EXAMPLES = {
    "eurusd_call": {
        "summary": "Capture EURUSD FX option snapshot",
        "description": "Captures price, volatility, Greeks, volume, and OI fields with a short-lived IBKR market-data subscription.",
        "value": {
            "contracts": [
                {
                    "symbol": "EURUSD",
                    "expiry": "20260619",
                    "strike": 1.10,
                    "right": "C",
                    "exchange": "SMART",
                    "multiplier": "100",
                }
            ],
            "snapshot_wait_seconds": 2.0,
            "persist": False,
            "cache_latest": True,
        },
    }
}


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post(
    "/capture",
    response_model=SnapshotResult,
    summary="Capture equity snapshots",
    description=(
        "Captures point-in-time market data snapshots for a list of equity symbols. "
        "Resolves exchange/currency automatically from symbol suffixes (.HK, .T, .L, etc.). "
        "Caches latest snapshots in Redis. Durable persistence is owned by the scheduler snapshotter."
    ),
)
async def capture_snapshots(
    payload: Annotated[CaptureSnapshotsRequest, Body(openapi_examples=CAPTURE_SNAPSHOTS_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> SnapshotResult:
    """Capture point-in-time market data snapshots for a list of equities.

    Opens one-shot IBKR snapshot subscriptions for every symbol at once,
    resolves exchange/currency from symbol suffixes (.HK, .T, .L, etc.),
    and caches the latest values in Redis. Symbols whose snapshot fails
    validation or arrives empty are listed in failed_symbols. Durable
    persistence is owned by the scheduler snapshotter (persist=true
    returns 501).
    """
    if payload.persist:
        raise HTTPException(
            status_code=501,
            detail="API snapshot persistence is disabled; use the scheduler snapshotter",
        )

    t0 = monotonic_time.monotonic()
    snapshots: list[EquitySnapshot] = []
    failed: list[str] = []

    # Resolve symbols to contract specs
    specs: list[tuple[str, str, str, str]] = []  # (symbol, exchange, currency, primary_exchange)
    for raw_symbol in payload.symbols:
        resolved = resolve_equity(raw_symbol)
        specs.append((resolved.symbol, resolved.exchange, resolved.currency, resolved.primary_exchange))

    # Request all one-shot IBKR snapshots at once. Results preserve requested symbol identity.
    symbol_params = [(s, ex, cur, pe, 0) for s, ex, cur, pe in specs]
    capture_results = await state.feed.capture_equity_snapshots(
        symbol_params,
        snapshot_wait_seconds=state.settings.ibkr_equity_snapshot_wait_seconds,
        lease_ttl_seconds=state.settings.ibkr_equity_snapshot_lease_ttl_seconds,
    )
    tickers_to_cancel = [result.ticker for result in capture_results if getattr(result, "ticker", None) is not None]

    try:
        # Convert successful per-symbol results to snapshots without relying on list positions.
        for result in capture_results:
            ticker = getattr(result, "ticker", None)
            if ticker is None:
                failed.append(getattr(result, "symbol", getattr(result, "requested_symbol", "UNKNOWN")))
                metrics.market_data_snapshot_total.inc({"asset_class": "equity", "status": "failed", "source": "fastapi"})
                continue
            try:
                ticker_time = getattr(ticker, "time", None)
                snap = ticker_to_snapshot(
                    ticker,
                    symbol=result.symbol,
                    exchange=result.exchange,
                    currency=result.currency,
                    primary_exchange=result.primary_exchange,
                    con_id=result.con_id,
                    timestamp=ticker_time if isinstance(ticker_time, datetime) else None,
                )
                validate_equity_snapshot_quality(snap)
                snapshots.append(snap)
                metrics.market_data_snapshot_total.inc({"asset_class": "equity", "status": "captured", "source": "fastapi"})
            except Exception:
                failed.append(result.symbol)
                metrics.market_data_snapshot_total.inc({"asset_class": "equity", "status": "failed", "source": "fastapi"})
                metrics.market_data_quality_failures_total.inc(
                    {"asset_class": "equity", "data_type": "snapshot", "severity": "error"}
                )
                logger.warning("failed to build snapshot for %s", result.symbol, exc_info=True)

        # Also track symbols that didn't get a successfully converted snapshot.
        captured_symbols = {snapshot.symbol for snapshot in snapshots}
        for raw_symbol in payload.symbols:
            resolved = resolve_equity(raw_symbol)
            if resolved.symbol not in captured_symbols and resolved.symbol not in failed:
                failed.append(resolved.symbol)

        # Cache latest in Redis
        if payload.cache_latest and snapshots and state.redis is not None:
            for snap in snapshots:
                try:
                    await state.redis.set_latest_equity_snapshot(snap)
                except Exception:
                    logger.warning("failed to cache snapshot for %s", snap.symbol, exc_info=True)
    finally:
        try:
            await state.feed.cancel_equity_tickers(tickers_to_cancel)
        except Exception:
            metrics.market_data_snapshot_cleanup_failures_total.inc(
                {"asset_class": "equity", "operation": "cancelMktData"},
                amount=len(tickers_to_cancel),
            )
            logger.warning("failed to clean up equity snapshot tickers", exc_info=True)

    duration = monotonic_time.monotonic() - t0
    return SnapshotResult(
        watchlist_name="adhoc",
        symbols_requested=len(payload.symbols),
        symbols_captured=len(snapshots),
        symbols_failed=len(failed),
        failed_symbols=tuple(failed),
        duration_seconds=round(duration, 3),
        snapshots=snapshots,
    )


@router.post(
    "/fx-options/capture",
    response_model=FXOptionCaptureResult,
    summary="Capture FX option snapshots",
    description=(
        "Captures point-in-time FX option market data and analytics using short-lived IBKR "
        "market-data subscriptions. Live Greeks require IBKR market-data subscriptions for "
        "both the option and the underlying."
    ),
)
async def capture_fx_option_snapshots(
    payload: Annotated[FXOptionCaptureRequest, Body(openapi_examples=FX_OPTION_CAPTURE_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> FXOptionCaptureResult:
    """Capture point-in-time FX option quotes, Greeks, and OI fields.

    Uses short-lived IBKR market-data subscriptions held for
    snapshot_wait_seconds while generic ticks arrive. Live Greeks require
    IBKR market-data subscriptions for both the option and the underlying.
    Captured snapshots are cached in Redis as the latest values when
    cache_latest is true; persist=true returns 501.
    """
    if payload.persist:
        raise HTTPException(
            status_code=501,
            detail="API FX option snapshot persistence is disabled; use the scheduler snapshotter",
        )

    symbols: list[str] = []
    contracts: list[OptionContractSpec] = []
    for item in payload.contracts:
        pair, _base, _quote = item.pair_parts
        symbols.append(pair)
        contracts.append(item.to_option_contract_spec())

    snapshots = await state.feed.capture_fx_option_snapshots(
        contracts,
        symbols=symbols,
        generic_ticks=tuple(payload.generic_ticks),
        snapshot_wait_seconds=payload.snapshot_wait_seconds,
    )

    cached = 0
    if payload.cache_latest and snapshots and state.redis is not None:
        for snapshot in snapshots:
            await state.redis.set_latest_fx_option_snapshot(snapshot)
            cached += 1

    return FXOptionCaptureResult(
        requested=len(payload.contracts),
        captured=len(snapshots),
        persisted=0,
        cached=cached,
        snapshots=snapshots,
    )


@router.get(
    "/fx-options/latest",
    response_model=FXOptionSnapshot | None,
    summary="Get latest cached FX option snapshot",
)
async def get_latest_fx_option_snapshot(
    symbol: str = Query(min_length=6, examples=["EURUSD"], description="FX pair of the option, e.g. EURUSD."),
    expiry: str = Query(
        min_length=6,
        examples=["20260619"],
        description="Option expiry as YYYYMMDD matching the captured contract, e.g. 20260619.",
    ),
    strike: float = Query(gt=0, examples=[1.10], description="Strike price matching the captured contract."),
    right: str = Query(min_length=1, examples=["C"], description="Option right: C/CALL or P/PUT."),
    exchange: str = Query(default="SMART", min_length=1, description="Exchange the snapshot was captured on."),
    local_symbol: str | None = Query(
        default=None,
        description="Optional IBKR local symbol used instead of the strike/right/expiry key.",
    ),
    con_id: int | None = Query(default=None, gt=0, description="Optional IBKR contract id used as the lookup key."),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> FXOptionSnapshot | None:
    """Read the most recently captured FX option snapshot from Redis.

    Reads the Redis latest-value cache only; it does not contact IBKR.
    Returns null when no snapshot matches the key combination. Populate
    the cache with POST /snapshot/fx-options/capture first.
    """
    return await state.redis.get_latest_fx_option_snapshot(
        symbol=symbol,
        expiry=expiry,
        strike=strike,
        right=right,
        exchange=exchange,
        local_symbol=local_symbol,
        con_id=con_id,
    )


@router.post(
    "/fx-options/query",
    summary="Query historical FX option snapshots",
    description=(
        "Removed: historical FX option snapshot queries moved to the scheduler/snapshotter layer. "
        "This endpoint always responds 410 Gone."
    ),
    deprecated=True,
    status_code=410,
    responses={410: {"description": "Gone — QuestDB historical snapshot queries are owned by the scheduler/snapshotter layer."}},
)
async def query_fx_option_snapshots(
    query: FXOptionSnapshotQuery,
    state: IBKRRestAppState = Depends(get_rest_state),
):
    """Historical FX option snapshot queries are gone (410)."""
    _ = (query, state)
    raise HTTPException(status_code=410, detail="QuestDB historical snapshot queries are owned by the scheduler/snapshotter layer")


@router.get(
    "/latest",
    response_model=list[EquitySnapshot],
    summary="Get latest cached snapshots",
    description="Returns the latest cached snapshot for each symbol from Redis. Fast, sub-ms.",
)
async def get_latest_snapshots(
    symbols: str = Query(
        ...,
        min_length=1,
        description="Comma-separated list of symbols, e.g. 'AAPL,MSFT,SPY'",
    ),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[EquitySnapshot]:
    """Read the latest cached equity snapshots from Redis.

    Reads the Redis latest-value cache only; it does not contact IBKR.
    Snapshots return in requested order and missing symbols are skipped
    silently. Populate the cache with POST /snapshot/capture first.
    """
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        raise HTTPException(status_code=400, detail="No valid symbols provided")

    cached = await state.redis.get_latest_equity_snapshots(symbol_list)
    # Return in requested order, skip missing
    return [cached[s] for s in symbol_list if s in cached]


@router.post(
    "/query",
    summary="Query historical snapshots from QuestDB",
    description=(
        "Removed: historical equity snapshot queries moved to the scheduler/snapshotter layer. "
        "This endpoint always responds 410 Gone."
    ),
    deprecated=True,
    status_code=410,
    responses={410: {"description": "Gone — QuestDB historical snapshot queries are owned by the scheduler/snapshotter layer."}},
)
async def query_historical_snapshots(
    query: SnapshotQuery,
    state: IBKRRestAppState = Depends(get_rest_state),
):
    """Historical equity snapshot queries are gone (410)."""
    _ = (query, state)
    raise HTTPException(status_code=410, detail="QuestDB historical snapshot queries are owned by the scheduler/snapshotter layer")


@router.get(
    "/latest-all",
    summary="Get latest snapshots for all tracked symbols",
    description=(
        "Removed: latest-all snapshot queries moved to the scheduler/snapshotter layer (QuestDB LATEST ON). "
        "This endpoint always responds 410 Gone."
    ),
    deprecated=True,
    status_code=410,
    responses={410: {"description": "Gone — QuestDB latest-all snapshot queries are owned by the scheduler/snapshotter layer."}},
)
async def get_all_latest_snapshots(
    limit: int = Query(default=100, ge=1, le=1000, description="Unused: the endpoint always returns 410."),
    state: IBKRRestAppState = Depends(get_rest_state),
):
    """Latest-all snapshot queries are gone (410)."""
    _ = (limit, state)
    raise HTTPException(status_code=410, detail="QuestDB latest-all snapshot queries are owned by the scheduler/snapshotter layer")


# ---------------------------------------------------------------------------
# Watchlist management
# ---------------------------------------------------------------------------

@router.post(
    "/watchlists",
    response_model=SnapshotWatchlist,
    summary="Create or update a snapshot watchlist",
    status_code=status.HTTP_201_CREATED,
)
async def create_watchlist(
    payload: Annotated[WatchlistCreateRequest, Body(openapi_examples=WATCHLIST_CREATE_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> SnapshotWatchlist:
    """Create or replace a named equity watchlist in Redis.

    The name is stored lower-cased and posting an existing name replaces
    the symbol list. Returns the stored watchlist; symbols keep their
    input order. Capture it with POST /snapshot/watchlists/{name}/capture.
    """
    watchlist = SnapshotWatchlist(
        name=payload.name.strip().lower(),
        symbols=tuple(payload.symbols),
        exchange=payload.exchange,
        currency=payload.currency,
        snapshot_interval_seconds=payload.snapshot_interval_seconds,
    )
    await state.redis.set_snapshot_watchlist(watchlist.name, watchlist.model_dump_json())
    return watchlist


@router.get(
    "/watchlists",
    response_model=list[str],
    summary="List all watchlist names",
)
async def list_watchlists(
    limit: int | None = Query(default=None, ge=1, le=1000, description="Optional cap on the number of names returned."),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[str]:
    """List every watchlist name stored in Redis.

    Scans the Redis watchlist keyspace and returns the lower-cased names
    only. Fetch a single definition with GET /snapshot/watchlists/{name}.
    """
    names = await state.redis.scan_snapshot_watchlists()
    return names[:limit] if limit is not None else names


@router.get(
    "/watchlists/{name}",
    response_model=SnapshotWatchlist,
    summary="Get a watchlist by name",
)
async def get_watchlist(
    name: str = Path(description="Watchlist name (matched case-insensitively), e.g. us_tech."),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> SnapshotWatchlist:
    """Read one watchlist definition from Redis.

    Returns the stored symbol list, exchange/currency defaults, and capture
    interval. Unknown names return 404.
    """
    key = constants.REDIS_SNAPSHOT_WATCHLIST_KEY_TEMPLATE.format(name=name.strip().lower())
    payload = await state.redis.get_raw(key)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Watchlist '{name}' not found")
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    return SnapshotWatchlist.model_validate_json(payload)


@router.post(
    "/watchlists/{name}/capture",
    response_model=SnapshotResult,
    summary="Capture snapshots for a watchlist",
)
async def capture_watchlist(
    name: str = Path(description="Watchlist name (matched case-insensitively), e.g. us_tech."),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> SnapshotResult:
    """Capture equity snapshots for every symbol in a stored watchlist.

    Loads the watchlist from Redis and runs the same capture path as
    POST /snapshot/capture, so exchange/currency resolution, Redis
    latest-caching, and failed-symbol reporting behave identically.
    Unknown names return 404.
    """
    key = constants.REDIS_SNAPSHOT_WATCHLIST_KEY_TEMPLATE.format(name=name.strip().lower())
    payload = await state.redis.get_raw(key)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Watchlist '{name}' not found")
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    watchlist = SnapshotWatchlist.model_validate_json(payload)

    request = CaptureSnapshotsRequest(
        symbols=list(watchlist.symbols),
    )
    result = await capture_snapshots(request, state)
    # Patch the watchlist name in the result
    result.watchlist_name = watchlist.name
    return result


@router.delete(
    "/watchlists/{name}",
    summary="Delete a watchlist",
    response_model=WatchlistDeleteResponse,
)
async def delete_watchlist(
    name: str = Path(description="Watchlist name (matched case-insensitively), e.g. us_tech."),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> WatchlistDeleteResponse:
    """Delete one watchlist from Redis.

    Removes the stored definition permanently. Unknown names return 404;
    deleted watchlists are confirmed with {status, name}.
    """
    key = constants.REDIS_SNAPSHOT_WATCHLIST_KEY_TEMPLATE.format(name=name.strip().lower())
    payload = await state.redis.get_raw(key)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Watchlist '{name}' not found")
    raw = await state.redis.raw_client()
    await raw.delete(key)
    return WatchlistDeleteResponse(status="deleted", name=name)
