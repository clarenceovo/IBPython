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

    request: OptionAnalyticsRequest = Field(
        description="Option analytics request: contract, generic ticks, snapshot wait, and regulatory snapshot flag."
    )
    use_ttl_cache: bool = Field(default=True, description="Serve identical repeated requests from the TTL cache.")
    cache_ttl_seconds: float | None = Field(
        default=None, ge=0, description="TTL override in seconds for the response cache. None uses the configured default."
    )


class CachedOptionSkewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: OptionSkewSurfaceRequest = Field(
        description="Skew surface request: option chain request, spot price, strike window, and sampling bounds."
    )
    use_ttl_cache: bool = Field(default=True, description="Serve identical repeated requests from the TTL cache.")
    cache_ttl_seconds: float | None = Field(
        default=60, ge=0, description="TTL override in seconds for the response cache."
    )


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


@router.post("/options/analytics", response_model=OptionAnalyticsSnapshot, summary="Load option analytics")
async def load_option_analytics(
    payload: CachedOptionAnalyticsRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> OptionAnalyticsSnapshot:
    """Load price, model volatility, Greeks, volume, and OI for one option.

    Runs a short-lived IBKR market-data subscription with the requested
    generic ticks and returns the snapshot. Live Greeks require IBKR
    market-data subscriptions; unavailable fields stay null. Identical
    requests are served from a TTL cache when enabled.
    """
    async def load() -> OptionAnalyticsSnapshot:
        return await state.feed.load_option_analytics(payload.request)

    if payload.use_ttl_cache:
        key = stable_cache_key("option_analytics", payload.request)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()


@router.post("/options/skew", response_model=OptionSkewSurfaceResponse, summary="Load option skew surface")
async def load_option_skew_surface(
    payload: Annotated[CachedOptionSkewRequest, Body(openapi_examples=OPTION_SKEW_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> OptionSkewSurfaceResponse:
    """Sample a bounded option chain and compute the IV skew surface.

    Walks a strike window around spot for up to max_expirations maturities,
    computes put-minus-call implied-volatility skew per expiry, and reports
    max call/put open interest per expiry. Sampling bounds cap the IBKR
    requests issued; identical surfaces are served from a TTL cache when
    enabled.
    """
    async def load() -> OptionSkewSurfaceResponse:
        return await state.feed.load_option_skew_surface(payload.request)

    if payload.use_ttl_cache:
        key = stable_cache_key("option_skew", payload.request)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()
