"""Options analytics and skew endpoints and models."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, Depends
from pydantic import BaseModel, ConfigDict, Field

from src.feeds.options import OptionAnalyticsRequest, OptionAnalyticsSnapshot, OptionSkewSurfaceRequest, OptionSkewSurfaceResponse
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

router = APIRouter(prefix="/market-data", tags=["market-data"])


class CachedOptionAnalyticsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: OptionAnalyticsRequest
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=None, ge=0)


class CachedOptionSkewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: OptionSkewSurfaceRequest
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=60, ge=0)


OPTION_SKEW_REQUEST_EXAMPLES = {
    "tsla_bounded_skew": {
        "summary": "TSLA per-maturity skew",
        "description": "Samples a bounded strike window around spot, computes put-minus-call IV skew, and reports max call/put OI per expiry.",
        "value": {
            "request": {
                "chain_request": {"symbol": "TSLA", "asset_class": "equity", "exchange": "SMART", "currency": "USD", "primary_exchange": "NASDAQ"},
                "spot_price": 250.0,
                "strike_window_pct": 0.30,
                "max_expirations": 4,
                "max_strikes_per_expiry": 11,
                "target_abs_delta": 0.25,
                "max_concurrent_requests": 4,
            },
            "use_ttl_cache": True,
            "cache_ttl_seconds": 60,
        },
    },
    "spx_bounded_skew": {
        "summary": "SPX index skew",
        "description": "For index options, specify the index exchange and optionally a trading class such as SPX or SPXW.",
        "value": {
            "request": {
                "chain_request": {"symbol": "SPX", "asset_class": "index", "exchange": "CBOE", "currency": "USD"},
                "chain_exchange": "CBOE",
                "trading_class": "SPX",
                "spot_price": 5200.0,
                "strike_window_pct": 0.20,
                "max_expirations": 4,
                "max_strikes_per_expiry": 11,
            },
            "use_ttl_cache": True,
            "cache_ttl_seconds": 60,
        },
    },
}


@router.post("/options/analytics", response_model=OptionAnalyticsSnapshot)
async def load_option_analytics(
    payload: CachedOptionAnalyticsRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> OptionAnalyticsSnapshot:
    async def load() -> OptionAnalyticsSnapshot:
        return await state.feed.load_option_analytics(payload.request)

    if payload.use_ttl_cache:
        key = stable_cache_key("option_analytics", payload.request)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()


@router.post("/options/skew", response_model=OptionSkewSurfaceResponse)
async def load_option_skew_surface(
    payload: Annotated[CachedOptionSkewRequest, Body(openapi_examples=OPTION_SKEW_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> OptionSkewSurfaceResponse:
    async def load() -> OptionSkewSurfaceResponse:
        return await state.feed.load_option_skew_surface(payload.request)

    if payload.use_ttl_cache:
        key = stable_cache_key("option_skew", payload.request)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()
