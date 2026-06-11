"""REST router for IBKR histogram data requests."""

from __future__ import annotations

from typing import Any  # kept for feed compatibility

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field

from src.webapp.dependencies import IBKRRestAppState, get_rest_state

router = APIRouter(prefix="/histogram", tags=["market-data"])


class HistogramRequestBody(BaseModel):
    """Request body for histogram data."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1)
    asset_class: str = Field(default="EQUITY", min_length=1)
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    use_rth: bool = True
    time_period: str = Field(default="1 day", min_length=1)


class HistogramBucket(BaseModel):
    price: float
    count: int
    size: int | None = None


class HistogramResponse(BaseModel):
    symbol: str
    asset_class: str
    exchange: str
    currency: str
    time_period: str
    use_rth: bool
    buckets: list[HistogramBucket]
    total_count: int


@router.post("", summary="Request histogram data", response_model=HistogramResponse)
async def request_histogram(
    payload: HistogramRequestBody,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> HistogramResponse:
    """Request histogram data (price/size buckets) for a contract."""
    result = await state.feed.request_histogram(
        symbol=payload.symbol,
        asset_class=payload.asset_class,
        exchange=payload.exchange,
        currency=payload.currency,
        use_rth=payload.use_rth,
        time_period=payload.time_period,
    )
    buckets = [HistogramBucket(**b) if isinstance(b, dict) else b for b in result]
    return HistogramResponse(
        symbol=payload.symbol,
        asset_class=payload.asset_class,
        exchange=payload.exchange,
        currency=payload.currency,
        time_period=payload.time_period,
        use_rth=payload.use_rth,
        buckets=buckets,
        total_count=len(buckets),
    )
