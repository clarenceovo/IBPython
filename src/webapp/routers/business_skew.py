"""Option skew surface endpoints for the business domain."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, Depends
from pydantic import BaseModel, ConfigDict, Field

from src.feeds.contracts import OptionChainRequest
from src.feeds.models import AssetClass
from src.feeds.options import OptionSkewSurfaceRequest, OptionSkewSurfaceResponse
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.openapi_markdown import markdown_openapi_examples
from src.webapp.routers.business_shared import BusinessCacheControls, resolve_business_symbol

router = APIRouter()


class BusinessOptionSkewRequest(BusinessCacheControls):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1, description="Instrument symbol, for example 'AAPL', 'ES', 'EUR.USD'.")
    asset_class: AssetClass = Field(default=AssetClass.EQUITY, description="Asset class: 'equity', 'fx', 'future', 'index', 'bond', 'option', 'crypto' (case-insensitive; aliases accepted).")
    exchange: str | None = Field(default=None, min_length=1, description="Exchange or routing destination, for example 'SMART', 'NASDAQ', 'CME', 'IDEALPRO'.")
    currency: str | None = Field(default=None, min_length=1, description="IBKR currency code, for example 'USD', 'HKD', 'JPY'.")
    primary_exchange: str | None = Field(default=None, min_length=1, description="Primary exchange; use to disambiguate 'SMART' routing.")
    chain_exchange: str | None = Field(default=None, min_length=1, description="Exchange used to fetch the option chain.")
    trading_class: str | None = Field(default=None, min_length=1, description="IBKR trading class, for example 'ESM6' versus 'CN8H6'.")
    option_exchange: str | None = Field(default=None, min_length=1, description="Exchange used for option contract details.")
    spot_price: float | None = Field(default=None, gt=0, description="Spot price override for the underlying.")
    strike_window_pct: float = Field(default=0.30, gt=0, le=2.0, description="Keep strikes within this percent of spot.")
    max_expirations: int = Field(default=4, ge=1, le=36, description="Cap on expirations fetched per underlying.")
    max_strikes_per_expiry: int = Field(default=11, ge=3, le=50, description="Cap on strikes fetched per expiry.")
    target_abs_delta: float = Field(default=0.25, gt=0, lt=1, description="Absolute delta targeted by strike selection around ATM (0.25 = 25 delta).")
    max_concurrent_requests: int = Field(default=4, ge=1, le=20, description="Cap on concurrent IBKR requests.")
    snapshot_wait_seconds: float = Field(default=2.0, gt=0, description="Seconds to wait for snapshot convergence.")


@router.post(
    "/getOptionSkew",
    response_model=OptionSkewSurfaceResponse,
    summary="Get option skew from a minimal business payload",
)
async def get_option_skew(
    payload: Annotated[
        BusinessOptionSkewRequest,
        Body(openapi_examples=markdown_openapi_examples("business.getOptionSkew")),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> OptionSkewSurfaceResponse:
    resolved = resolve_business_symbol(
        symbol=payload.symbol,
        asset_class=payload.asset_class,
        exchange=payload.exchange,
        currency=payload.currency,
        primary_exchange=payload.primary_exchange,
    )
    request = OptionSkewSurfaceRequest(
        chain_request=OptionChainRequest(
            symbol=resolved.symbol,
            asset_class=payload.asset_class,
            exchange=resolved.exchange,
            currency=resolved.currency,
            primary_exchange=resolved.primary_exchange,
        ),
        chain_exchange=payload.chain_exchange,
        trading_class=payload.trading_class,
        option_exchange=payload.option_exchange,
        spot_price=payload.spot_price,
        strike_window_pct=payload.strike_window_pct,
        max_expirations=payload.max_expirations,
        max_strikes_per_expiry=payload.max_strikes_per_expiry,
        target_abs_delta=payload.target_abs_delta,
        max_concurrent_requests=payload.max_concurrent_requests,
        snapshot_wait_seconds=payload.snapshot_wait_seconds,
    )

    async def load() -> OptionSkewSurfaceResponse:
        return await state.feed.load_option_skew_surface(request)

    if payload.use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("business_option_skew", request),
            load,
            ttl_seconds=payload.cache_ttl_seconds,
        )
    return await load()
