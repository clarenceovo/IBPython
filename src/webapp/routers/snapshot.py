from __future__ import annotations

import asyncio
import logging
import time as monotonic_time
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query
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
)
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
    persist: bool = Field(default=True, description="Persist snapshots to QuestDB")
    cache_latest: bool = Field(default=True, description="Cache latest snapshots in Redis")

    @classmethod
    def from_watchlist(cls, watchlist: SnapshotWatchlist) -> "CaptureSnapshotsRequest":
        return cls(symbols=list(watchlist.symbols))


class WatchlistCreateRequest(BaseModel):
    """Create or update a named watchlist."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, examples=["us_tech", "hk_large_cap"])
    symbols: list[str] = Field(min_length=1)
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    snapshot_interval_seconds: float = Field(default=60, gt=0)


class WatchlistCaptureRequest(BaseModel):
    """Capture snapshots for an entire watchlist by name."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    persist: bool = True
    cache_latest: bool = True


class FXOptionContractRequest(BaseModel):
    """Pair-style FX option contract for snapshot capture."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=6, examples=["EURUSD"])
    expiry: str = Field(min_length=6, examples=["20260619"])
    strike: float = Field(gt=0, examples=[1.10])
    right: str = Field(min_length=1, examples=["C"])
    exchange: str = Field(default="SMART", min_length=1)
    currency: str | None = Field(default=None, min_length=1)
    multiplier: str = Field(default="100", min_length=1)
    trading_class: str | None = None
    local_symbol: str | None = None
    con_id: int | None = Field(default=None, gt=0)

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

    contracts: list[FXOptionContractRequest] = Field(min_length=1)
    snapshot_wait_seconds: float = Field(default=2.0, gt=0, le=30)
    generic_ticks: tuple[str, ...] = DEFAULT_OPTION_ANALYTICS_GENERIC_TICKS
    persist: bool = True
    cache_latest: bool = True


class FXOptionCaptureResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requested: int
    captured: int
    persisted: int = 0
    cached: int = 0
    snapshots: list[FXOptionSnapshot]


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
            "persist": True,
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
        "Persists to QuestDB and caches latest in Redis."
    ),
)
async def capture_snapshots(
    payload: Annotated[CaptureSnapshotsRequest, Body(openapi_examples=CAPTURE_SNAPSHOTS_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> SnapshotResult:
    t0 = monotonic_time.monotonic()
    snapshots: list[EquitySnapshot] = []
    failed: list[str] = []

    # Resolve symbols to contract specs
    specs: list[tuple[str, str, str, str]] = []  # (symbol, exchange, currency, primary_exchange)
    for raw_symbol in payload.symbols:
        resolved = resolve_equity(raw_symbol)
        specs.append((resolved.symbol, resolved.exchange, resolved.currency, resolved.primary_exchange))

    # Subscribe to all tickers at once
    symbol_params = [(s, ex, cur, pe, 0) for s, ex, cur, pe in specs]
    tickers = await state.feed.capture_equity_snapshots(symbol_params)

    # Convert tickers to snapshots
    for i, ticker in enumerate(tickers):
        if i < len(specs):
            s, ex, cur, pe = specs[i]
            try:
                # Use ticker's time if available (exchange arrival time)
                ticker_time = getattr(ticker, "time", None)
                snap = ticker_to_snapshot(
                    ticker, symbol=s, exchange=ex, currency=cur, primary_exchange=pe,
                    timestamp=ticker_time if isinstance(ticker_time, datetime) else None,
                )
                snapshots.append(snap)
            except Exception:
                failed.append(s)
                logger.warning("failed to build snapshot for %s", s, exc_info=True)

    # Also track symbols that didn't get a ticker at all
    captured_symbols = {s.symbol for s in snapshots}
    for raw_symbol in payload.symbols:
        resolved = resolve_equity(raw_symbol)
        if resolved.symbol not in captured_symbols:
            failed.append(resolved.symbol)

    # Persist to QuestDB
    if payload.persist and snapshots and state.questdb is not None:
        try:
            await state.questdb.insert_snapshots(snapshots)
        except Exception:
            logger.exception("failed to persist snapshots to QuestDB")

    # Cache latest in Redis
    if payload.cache_latest and snapshots and state.redis is not None:
        for snap in snapshots:
            try:
                await state.redis.set_latest_equity_snapshot(snap)
            except Exception:
                logger.warning("failed to cache snapshot for %s", snap.symbol, exc_info=True)

    # Clean up tickers
    await state.feed.cancel_equity_tickers(tickers)

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

    persisted = 0
    if payload.persist and snapshots and state.questdb is not None:
        if hasattr(state.questdb, "create_fx_option_snapshot_table"):
            await state.questdb.create_fx_option_snapshot_table()
        persisted = await state.questdb.insert_fx_option_snapshots(snapshots)

    cached = 0
    if payload.cache_latest and snapshots and state.redis is not None:
        for snapshot in snapshots:
            await state.redis.set_latest_fx_option_snapshot(snapshot)
            cached += 1

    return FXOptionCaptureResult(
        requested=len(payload.contracts),
        captured=len(snapshots),
        persisted=persisted,
        cached=cached,
        snapshots=snapshots,
    )


@router.get(
    "/fx-options/latest",
    response_model=FXOptionSnapshot | None,
    summary="Get latest cached FX option snapshot",
)
async def get_latest_fx_option_snapshot(
    symbol: str = Query(min_length=6, examples=["EURUSD"]),
    expiry: str = Query(min_length=6, examples=["20260619"]),
    strike: float = Query(gt=0, examples=[1.10]),
    right: str = Query(min_length=1, examples=["C"]),
    exchange: str = Query(default="SMART", min_length=1),
    local_symbol: str | None = Query(default=None),
    con_id: int | None = Query(default=None, gt=0),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> FXOptionSnapshot | None:
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
    response_model=list[dict[str, Any]],
    summary="Query historical FX option snapshots",
)
async def query_fx_option_snapshots(
    query: FXOptionSnapshotQuery,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[dict[str, Any]]:
    if state.questdb is None:
        raise HTTPException(status_code=503, detail="QuestDB not configured")
    return await state.questdb.query_fx_option_snapshots(
        symbol=query.symbol,
        expiry=query.expiry,
        strike=query.strike,
        right=query.right,
        start=query.start,
        end=query.end,
        limit=query.limit,
    )


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
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        raise HTTPException(status_code=400, detail="No valid symbols provided")

    cached = await state.redis.get_latest_equity_snapshots(symbol_list)
    # Return in requested order, skip missing
    return [cached[s] for s in symbol_list if s in cached]


@router.post(
    "/query",
    response_model=list[dict[str, Any]],
    summary="Query historical snapshots from QuestDB",
    description="Query historical equity snapshots stored in QuestDB with time range filters.",
)
async def query_historical_snapshots(
    query: SnapshotQuery,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[dict[str, Any]]:
    if state.questdb is None:
        raise HTTPException(status_code=503, detail="QuestDB not configured")
    return await state.questdb.query_snapshots(
        symbol=query.symbol,
        start=query.start,
        end=query.end,
        limit=query.limit,
    )


@router.get(
    "/latest-all",
    response_model=list[dict[str, Any]],
    summary="Get latest snapshots for all tracked symbols",
    description="Queries QuestDB LATEST ON to get the most recent snapshot per symbol.",
)
async def get_all_latest_snapshots(
    limit: int = Query(default=100, ge=1, le=1000),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[dict[str, Any]]:
    if state.questdb is None:
        raise HTTPException(status_code=503, detail="QuestDB not configured")
    return await state.questdb.query_latest_snapshots(limit=limit)


# ---------------------------------------------------------------------------
# Watchlist management
# ---------------------------------------------------------------------------

@router.post(
    "/watchlists",
    response_model=SnapshotWatchlist,
    summary="Create or update a snapshot watchlist",
)
async def create_watchlist(
    payload: Annotated[WatchlistCreateRequest, Body(openapi_examples=WATCHLIST_CREATE_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> SnapshotWatchlist:
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
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[str]:
    return await state.redis.scan_snapshot_watchlists()


@router.get(
    "/watchlists/{name}",
    response_model=SnapshotWatchlist,
    summary="Get a watchlist by name",
)
async def get_watchlist(
    name: str,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> SnapshotWatchlist:
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
    name: str,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> SnapshotResult:
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
)
async def delete_watchlist(
    name: str,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> dict[str, str]:
    key = constants.REDIS_SNAPSHOT_WATCHLIST_KEY_TEMPLATE.format(name=name.strip().lower())
    payload = await state.redis.get_raw(key)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Watchlist '{name}' not found")
    raw = await state.redis.raw_client()
    await raw.delete(key)
    return {"status": "deleted", "name": name}
