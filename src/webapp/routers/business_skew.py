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

    symbol: str = Field(min_length=1)
    asset_class: AssetClass = AssetClass.EQUITY
    exchange: str | None = Field(default=None, min_length=1)
    currency: str | None = Field(default=None, min_length=1)
    primary_exchange: str | None = Field(default=None, min_length=1)
    chain_exchange: str | None = Field(default=None, min_length=1)
    trading_class: str | None = Field(default=None, min_length=1)
    option_exchange: str | None = Field(default=None, min_length=1)
    spot_price: float | None = Field(default=None, gt=0)
    strike_window_pct: float = Field(default=0.30, gt=0, le=2.0)
    max_expirations: int = Field(default=4, ge=1, le=36)
    max_strikes_per_expiry: int = Field(default=11, ge=3, le=50)
    target_abs_delta: float = Field(default=0.25, gt=0, lt=1)
    max_concurrent_requests: int = Field(default=4, ge=1, le=20)
    snapshot_wait_seconds: float = Field(default=2.0, gt=0)


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
