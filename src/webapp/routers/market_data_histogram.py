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

    symbol: str = Field(
        min_length=1,
        description="Ticker in IBKR notation, e.g. AAPL, ES, or 0700.HK.",
    )
    asset_class: str = Field(
        default="EQUITY",
        min_length=1,
        description="Asset class: equity, fx, future, bond, index, crypto, or option.",
    )
    exchange: str = Field(default="SMART", min_length=1, description="IBKR exchange or SMART for smart routing.")
    currency: str = Field(default="USD", min_length=1, description="IBKR currency of the contract.")
    use_rth: bool = Field(default=True, description="Build the histogram from regular trading hours only.")
    time_period: str = Field(
        default="1 day",
        min_length=1,
        description="IBKR histogram period, e.g. '1 day', '1 week', '1 month'.",
    )


class HistogramBucket(BaseModel):
    """One price bucket of the histogram."""

    price: float = Field(description="Bucket price level.")
    count: int = Field(description="Number of trades in the bucket.")
    size: int | None = Field(default=None, description="Traded size in the bucket when IBKR reports it.")


class HistogramResponse(BaseModel):
    """Histogram of trading activity by price level."""

    symbol: str = Field(description="Requested symbol.")
    asset_class: str = Field(description="Requested asset class.")
    exchange: str = Field(description="Requested exchange.")
    currency: str = Field(description="Requested currency.")
    time_period: str = Field(description="Histogram period used, e.g. '1 day'.")
    use_rth: bool = Field(description="Whether only regular trading hours were counted.")
    buckets: list[HistogramBucket] = Field(description="Price buckets ordered as IBKR returns them.")
    total_count: int = Field(description="Number of buckets returned.")


@router.post("", summary="Request histogram data", response_model=HistogramResponse)
async def request_histogram(
    payload: HistogramRequestBody,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> HistogramResponse:
    """Request histogram data (price/size buckets) for a contract.

    Runs IBKR's reqHistogramData over one time period and returns price
    buckets with trade counts and sizes. Use it for volume-at-price
    profiles; the /tick-data/histogram endpoint offers the same feed call
    with a fully typed body.
    """
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
