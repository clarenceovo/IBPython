"""Advanced analytics endpoints — historical volatility, option IV series, trading schedule."""

from __future__ import annotations

from datetime import date as date_type
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from src.webapp.dependencies import IBKRRestAppState, get_rest_state

router = APIRouter(prefix="/market-data", tags=["market-data"])


# ------------------------------------------------------------------
# Historical volatility
# ------------------------------------------------------------------

class HistoricalVolatilityRequest(BaseModel):
    """Request body for historical volatility time series."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, examples=["AAPL", "SPY"])
    asset_class: str = Field(default="EQUITY", examples=["EQUITY", "FUTURE", "INDEX"])
    exchange: str = Field(default="SMART")
    currency: str = Field(default="USD")
    bar_size: str = Field(default="1 day", examples=["1 min", "5 mins", "1 hour", "1 day"])
    duration: str = Field(default="1 Y", examples=["1 M", "6 M", "1 Y"])
    use_rth: bool = True
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=300, ge=0)


HISTORICAL_VOLATILITY_EXAMPLES = {
    "aapl_1y": {
        "summary": "AAPL 1-year daily volatility",
        "value": {"symbol": "AAPL", "duration": "1 Y", "bar_size": "1 day"},
    },
    "spy_6m": {
        "summary": "SPY 6-month daily volatility",
        "value": {"symbol": "SPY", "duration": "6 M", "bar_size": "1 day"},
    },
}


@router.post(
    "/historical-volatility",
    summary="Get historical volatility time series",
)
async def get_historical_volatility(
    payload: Annotated[
        HistoricalVolatilityRequest,
        Body(openapi_examples=HISTORICAL_VOLATILITY_EXAMPLES),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[dict[str, Any]]:
    """Get historical volatility time series from IBKR.

    Uses IBKR's built-in HISTORICAL_VOLATILITY whatToShow — no manual computation needed.
    """
    from src.feeds.ibkr_historical import ensure_historical_chunk_limit, plan_historical_auto_chunk
    from src.feeds.models import AssetClass, OHLCVRequest, normalize_bar_size

    try:
        asset = _parse_asset_class(payload.asset_class)
        request = OHLCVRequest(
            symbol=payload.symbol,
            asset_class=asset,
            exchange=payload.exchange,
            currency=payload.currency,
            bar_size=normalize_bar_size(payload.bar_size),
            duration=payload.duration,
            what_to_show="HISTORICAL_VOLATILITY",
            use_rth=payload.use_rth,
        )
        auto_chunk_plan = plan_historical_auto_chunk(request)
        if auto_chunk_plan is not None:
            ensure_historical_chunk_limit(
                request, auto_chunk_plan,
                max_chunks=state.settings.ibkr_historical_max_chunks,
            )
            bars = await state.feed.load_historical_ohlcv_range(
                request.model_copy(update={"end_datetime": auto_chunk_plan.end_datetime}),
                start_datetime=auto_chunk_plan.start_datetime,
                end_datetime=auto_chunk_plan.end_datetime,
                max_chunks=state.settings.ibkr_historical_max_chunks,
            )
        else:
            bars = await state.feed.load_historical_ohlcv(
                request,
                max_chunks=state.settings.ibkr_historical_max_chunks,
            )
        return [
            {
                "timestamp": bar.timestamp.isoformat(),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            }
            for bar in bars
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ------------------------------------------------------------------
# Option implied volatility series
# ------------------------------------------------------------------

class OptionIVSeriesRequest(BaseModel):
    """Request body for option implied volatility time series."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, examples=["AAPL", "SPY"])
    asset_class: str = Field(default="EQUITY", examples=["EQUITY", "FUTURE", "INDEX"])
    exchange: str = Field(default="SMART")
    currency: str = Field(default="USD")
    bar_size: str = Field(default="1 day", examples=["1 min", "5 mins", "1 hour", "1 day"])
    duration: str = Field(default="6 M", examples=["1 M", "6 M", "1 Y"])
    use_rth: bool = True
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=300, ge=0)


OPTION_IV_SERIES_EXAMPLES = {
    "aapl_6m": {
        "summary": "AAPL 6-month daily IV",
        "value": {"symbol": "AAPL", "duration": "6 M", "bar_size": "1 day"},
    },
}


@router.post(
    "/options/iv-series",
    summary="Get option implied volatility time series",
)
async def get_option_iv_series(
    payload: Annotated[
        OptionIVSeriesRequest,
        Body(openapi_examples=OPTION_IV_SERIES_EXAMPLES),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[dict[str, Any]]:
    """Get option implied volatility time series from IBKR.

    Uses IBKR's built-in OPTION_IMPLIED_VOLATILITY whatToShow for the underlying.
    """
    from src.feeds.ibkr_historical import ensure_historical_chunk_limit, plan_historical_auto_chunk
    from src.feeds.models import AssetClass, OHLCVRequest, normalize_bar_size

    try:
        asset = _parse_asset_class(payload.asset_class)
        request = OHLCVRequest(
            symbol=payload.symbol,
            asset_class=asset,
            exchange=payload.exchange,
            currency=payload.currency,
            bar_size=normalize_bar_size(payload.bar_size),
            duration=payload.duration,
            what_to_show="OPTION_IMPLIED_VOLATILITY",
            use_rth=payload.use_rth,
        )
        auto_chunk_plan = plan_historical_auto_chunk(request)
        if auto_chunk_plan is not None:
            ensure_historical_chunk_limit(
                request, auto_chunk_plan,
                max_chunks=state.settings.ibkr_historical_max_chunks,
            )
            bars = await state.feed.load_historical_ohlcv_range(
                request.model_copy(update={"end_datetime": auto_chunk_plan.end_datetime}),
                start_datetime=auto_chunk_plan.start_datetime,
                end_datetime=auto_chunk_plan.end_datetime,
                max_chunks=state.settings.ibkr_historical_max_chunks,
            )
        else:
            bars = await state.feed.load_historical_ohlcv(
                request,
                max_chunks=state.settings.ibkr_historical_max_chunks,
            )
        return [
            {
                "timestamp": bar.timestamp.isoformat(),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            }
            for bar in bars
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ------------------------------------------------------------------
# Trading schedule
# ------------------------------------------------------------------

class TradingScheduleRequest(BaseModel):
    """Request body for trading schedule."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, examples=["AAPL", "ES", "EURUSD"])
    asset_class: str = Field(default="EQUITY", examples=["EQUITY", "FUTURE", "FX", "INDEX"])
    exchange: str = Field(default="SMART")
    currency: str = Field(default="USD")
    end_date: str = Field(default="", examples=["20260101", ""])
    num_days: int = Field(default=7, ge=1, le=30)


TRADING_SCHEDULE_EXAMPLES = {
    "aapl_schedule": {
        "summary": "AAPL trading schedule",
        "value": {"symbol": "AAPL", "num_days": 7},
    },
    "es_futures_schedule": {
        "summary": "ES futures trading schedule",
        "value": {"symbol": "ES", "asset_class": "FUTURE", "exchange": "CME", "num_days": 7},
    },
}


@router.post(
    "/trading-schedule",
    summary="Get trading schedule for any instrument",
)
async def get_trading_schedule(
    payload: Annotated[
        TradingScheduleRequest,
        Body(openapi_examples=TRADING_SCHEDULE_EXAMPLES),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[dict[str, Any]]:
    """Get trading schedule for any instrument from IBKR.

    Returns session open/close times, overnight flags, and trading status.
    """
    from src.feeds.models import OHLCVRequest

    try:
        ref_date = date_type.today()
        if payload.end_date:
            ref_date = date_type.fromisoformat(payload.end_date)

        asset = _parse_asset_class(payload.asset_class)
        request = OHLCVRequest(
            symbol=payload.symbol,
            asset_class=asset,
            exchange=payload.exchange,
            currency=payload.currency,
            bar_size="1 day",
            duration=f"{payload.num_days} D",
            what_to_show="SCHEDULE",
            use_rth=True,
        )
        schedule = await state.feed.load_trading_schedule(request, ref_date=ref_date, use_rth=True)
        if isinstance(schedule, (list, tuple)):
            return [
                item.model_dump(mode="json") if hasattr(item, "model_dump") else item
                for item in schedule
            ]
        return [schedule.model_dump(mode="json")] if hasattr(schedule, "model_dump") else [schedule]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _parse_asset_class(asset_class: str) -> Any:
    """Parse asset_class text using AssetClass values, names, and documented aliases."""
    from src.feeds.models import AssetClass

    normalized = asset_class.strip().lower()
    aliases = {
        "futures": AssetClass.FUTURE.value,
        "stocks": AssetClass.EQUITY.value,
        "stock": AssetClass.EQUITY.value,
        "equities": AssetClass.EQUITY.value,
        "forex": AssetClass.FX.value,
    }
    normalized = aliases.get(normalized, normalized)
    try:
        return AssetClass(normalized)
    except ValueError:
        member = AssetClass.__members__.get(asset_class.strip().upper())
        if member is not None:
            return member
        supported = ", ".join(item.value for item in AssetClass)
        raise ValueError(f"unsupported asset_class '{asset_class}'; expected one of: {supported}")
