"""FX OHLCV endpoints and models."""

from __future__ import annotations

import time
from typing import Annotated

from fastapi import APIRouter, Body, Depends
from pydantic import Field, field_validator

from src.feeds.models import (
    AssetClass,
    FXOHLCVBar,
    FXOHLCVResponseEnvelope,
    OHLCVRequest,
    OHLCVRequestMeta,
    OptionOHLCVBar,
    OptionOHLCVResponseEnvelope,
    compute_ohlcv_quality,
)
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.routers.market_data_shared import MinimalOHLCVLoadControls, load_ohlcv_with_controls

router = APIRouter(prefix="/market-data", tags=["market-data"])


def _fx_pair_parts(symbol: str, currency: str | None = None) -> tuple[str, str, str]:
    normalized = symbol.replace("/", "").strip().upper()
    if len(normalized) != 6:
        raise ValueError("FX option symbols must be six-character pairs such as EURUSD")
    base = normalized[:3]
    quote = currency.strip().upper() if currency else normalized[3:]
    return normalized, base, quote


class FXOHLCVLoadRequest(MinimalOHLCVLoadControls):
    symbol: str = Field(min_length=1, examples=["EURUSD"])
    exchange: str = Field(default="IDEALPRO", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    what_to_show: str = Field(default="MIDPOINT", min_length=1)
    use_rth: bool = False

    def to_request(self) -> OHLCVRequest:
        return self.to_ohlcv_request(
            AssetClass.FX,
            symbol=self.symbol,
            exchange=self.exchange,
            currency=self.currency,
        )


class FXOptionOHLCVLoadRequest(MinimalOHLCVLoadControls):
    symbol: str = Field(min_length=6, examples=["EURUSD"])
    expiry: str = Field(min_length=6, examples=["20260619"])
    strike: float = Field(gt=0, examples=[1.10])
    right: str = Field(min_length=1, examples=["C"])
    exchange: str = Field(default="SMART", min_length=1)
    currency: str | None = Field(default=None, min_length=1)
    multiplier: str | None = Field(default=None, examples=["100000"])
    trading_class: str | None = None
    local_symbol: str | None = None
    con_id: int | None = Field(default=None, gt=0)
    use_rth: bool = False

    @field_validator("right", mode="before")
    @classmethod
    def normalize_right(cls, value: object) -> str:
        normalized = str(value).strip().upper()
        if normalized in {"C", "CALL"}:
            return "C"
        if normalized in {"P", "PUT"}:
            return "P"
        raise ValueError("right must be C/CALL or P/PUT")

    def to_request(self) -> OHLCVRequest:
        pair, base, quote = _fx_pair_parts(self.symbol, self.currency)
        symbol = self.local_symbol or f"{pair} {self.expiry}{self.right}{self.strike:g}"
        return self.to_ohlcv_request(
            AssetClass.OPTION,
            symbol=symbol,
            exchange=self.exchange,
            currency=quote,
            option_sec_type="OPT",
            underlying_symbol=base,
            expiry=self.expiry,
            strike=self.strike,
            right=self.right,
            multiplier=self.multiplier,
            trading_class=self.trading_class,
            local_symbol=self.local_symbol,
            con_id=self.con_id,
            metadata={**self.metadata, "market": "fx_option", "pair": pair, "option_sec_type": "OPT"},
        )


FX_OHLCV_REQUEST_EXAMPLES = {
    "eurusd_minimal": {"summary": "EURUSD midpoint bars", "description": "FX wrapper presets asset_class=fx, exchange=IDEALPRO, what_to_show=MIDPOINT, and use_rth=false.", "value": {"symbol": "EURUSD"}},
    "usdjpy_hourly": {"summary": "USDJPY hourly midpoint bars", "value": {"symbol": "USDJPY", "currency": "JPY", "start_datetime": "2026-05-01T00:00:00Z", "end_datetime": "2026-05-05T00:00:00Z", "duration": "5 D", "bar_size": "1 hour"}},
    "gbpusd": {"summary": "GBPUSD (Cable)", "description": "British Pound / US Dollar.", "value": {"symbol": "GBPUSD"}},
    "usdchf": {"summary": "USDCHF (Swissy)", "description": "US Dollar / Swiss Franc.", "value": {"symbol": "USDCHF"}},
    "audusd": {"summary": "AUDUSD (Aussie)", "description": "Australian Dollar / US Dollar.", "value": {"symbol": "AUDUSD"}},
    "nzdusd": {"summary": "NZDUSD (Kiwi)", "description": "New Zealand Dollar / US Dollar.", "value": {"symbol": "NZDUSD"}},
    "usdcad": {"summary": "USDCAD (Loonie)", "description": "US Dollar / Canadian Dollar.", "value": {"symbol": "USDCAD"}},
    "usdnok": {"summary": "USDNOK (Norwegian Krone)", "description": "US Dollar / Norwegian Krone.", "value": {"symbol": "USDNOK"}},
    "usdsek": {"summary": "USDSEK (Swedish Krona)", "description": "US Dollar / Swedish Krona.", "value": {"symbol": "USDSEK"}},
    "eurgbp_cross": {"summary": "EURGBP (Euro/Sterling cross)", "description": "Euro / British Pound cross rate.", "value": {"symbol": "EURGBP", "currency": "GBP"}},
    "eurjpy_cross": {"summary": "EURJPY (Euro/Yen cross)", "description": "Euro / Japanese Yen cross rate.", "value": {"symbol": "EURJPY", "currency": "JPY"}},
    "eurcad_cross": {"summary": "EURCAD (Euro/Loonie cross)", "description": "Euro / Canadian Dollar cross rate.", "value": {"symbol": "EURCAD", "currency": "CAD"}},
    "gbpjpy_cross": {"summary": "GBPJPY (Gopher)", "description": "British Pound / Japanese Yen cross rate.", "value": {"symbol": "GBPJPY", "currency": "JPY"}},
    "audjpy_cross": {"summary": "AUDJPY (Aussie/Yen cross)", "description": "Australian Dollar / Japanese Yen cross rate.", "value": {"symbol": "AUDJPY", "currency": "JPY"}},
}

FX_OPTION_OHLCV_REQUEST_EXAMPLES = {
    "eurusd_call": {
        "summary": "EURUSD FX option call",
        "description": "Pair-style FX option OHLCV. EURUSD maps to underlying_symbol=EUR and currency=USD.",
        "value": {"symbol": "EURUSD", "expiry": "20260619", "strike": 1.10, "right": "C", "duration": "1 D", "bar_size": "1 day"},
    },
    "eurusd_put_with_local_symbol": {
        "summary": "EURUSD FX option by local symbol",
        "description": "Use local_symbol or con_id when IBKR needs exact contract disambiguation.",
        "value": {"symbol": "EURUSD", "expiry": "20260619", "strike": 1.05, "right": "P", "local_symbol": "EURUSD  260619P00001050", "duration": "1 D", "bar_size": "1 day"},
    },
}


@router.post(
    "/ohlcv/fx",
    response_model=FXOHLCVResponseEnvelope,
    summary="Load FX OHLCV with preset asset_class",
)
async def load_fx_ohlcv(
    payload: Annotated[FXOHLCVLoadRequest, Body(openapi_examples=FX_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> FXOHLCVResponseEnvelope:
    req = payload.to_request()
    request_meta = OHLCVRequestMeta(
        symbol=req.symbol,
        asset_class=req.asset_class.value,
        exchange=req.exchange,
        currency=req.currency,
        bar_size=req.bar_size,
        what_to_show=req.what_to_show,
        use_rth=req.use_rth,
        start_time=req.start_datetime,
        end_time=req.end_datetime,
        duration=req.duration,
    )
    t0 = time.monotonic()
    bars = await load_ohlcv_with_controls(
        request=req,
        start_datetime=payload.start_datetime,
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv_fx",
        state=state,
    )
    latency_ms = (time.monotonic() - t0) * 1000.0
    return FXOHLCVResponseEnvelope(
        bars=bars,  # type: ignore[arg-type]
        request=request_meta,
        quality=compute_ohlcv_quality(bars),
        latency_ms=latency_ms,
        cache_hit=False,
        chunk_count=1,
        source="ibkr",
    )


@router.post(
    "/ohlcv/fx-options",
    response_model=OptionOHLCVResponseEnvelope,
    summary="Load FX option OHLCV",
)
async def load_fx_option_ohlcv(
    payload: Annotated[FXOptionOHLCVLoadRequest, Body(openapi_examples=FX_OPTION_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> OptionOHLCVResponseEnvelope:
    req = payload.to_request()
    request_meta = OHLCVRequestMeta(
        symbol=req.symbol,
        asset_class=req.asset_class.value,
        exchange=req.exchange,
        currency=req.currency,
        bar_size=req.bar_size,
        what_to_show=req.what_to_show,
        use_rth=req.use_rth,
        start_time=req.start_datetime,
        end_time=req.end_datetime,
        duration=req.duration,
    )
    t0 = time.monotonic()
    bars = await load_ohlcv_with_controls(
        request=req,
        start_datetime=payload.start_datetime,
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv_fx_options",
        state=state,
    )
    latency_ms = (time.monotonic() - t0) * 1000.0
    return OptionOHLCVResponseEnvelope(
        bars=bars,  # type: ignore[arg-type]
        request=request_meta,
        quality=compute_ohlcv_quality(bars),
        latency_ms=latency_ms,
        cache_hit=False,
        chunk_count=1,
        source="ibkr",
    )
