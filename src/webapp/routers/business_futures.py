"""Commodity futures endpoints for the business domain."""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Body, Depends
from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.openapi_markdown import markdown_openapi_examples
from src.webapp.routers.business_shared import (
    BusinessDateRangeControls,
    commodity_contract_months,
    resolve_commodity_market,
)

router = APIRouter()


class CommodityFuturesRequest(BusinessDateRangeControls):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1, examples=["CL", "GC", "NG"])
    as_of_date: date = Field(default_factory=date.today)
    forward_count: int = Field(default=1, ge=0, le=12)
    exchange: str | None = Field(default=None, min_length=1)
    currency: str | None = Field(default=None, min_length=1)
    multiplier: str | None = Field(default=None, min_length=1)
    duration: str = Field(default="1 D", min_length=1)
    bar_size: str = Field(default="1 min", min_length=1)
    what_to_show: str = Field(default="TRADES", min_length=1)
    use_rth: bool = False
    cache_latest: bool = False
    max_concurrent_requests: int = Field(default=4, ge=1, le=20)

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: object) -> str:
        if value is None:
            raise ValueError("symbol is required")
        normalized = str(value).strip().upper()
        if not normalized:
            raise ValueError("symbol cannot be empty")
        return normalized


class CommodityFuturePoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    role: str
    symbol: str
    contract_month: str
    exchange: str
    currency: str
    bar: OHLCVBar | None = None


class CommodityFuturesResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    as_of_date: date
    contracts: tuple[CommodityFuturePoint, ...]
    source: str = "ibkr"


@router.post(
    "/commodities/getFutures",
    response_model=CommodityFuturesResponse,
    summary="Load front and forward commodity futures",
)
async def get_commodity_futures(
    payload: Annotated[
        CommodityFuturesRequest,
        Body(openapi_examples=markdown_openapi_examples("business.commodities.getFutures")),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> CommodityFuturesResponse:
    async def load() -> CommodityFuturesResponse:
        root = payload.symbol
        exchange, currency = resolve_commodity_market(root, payload.exchange, payload.currency)
        contract_months = commodity_contract_months(root, payload.as_of_date, payload.forward_count + 1)
        semaphore = asyncio.Semaphore(payload.max_concurrent_requests)

        async def load_contract(index: int, contract_month: str) -> CommodityFuturePoint:
            role = "front" if index == 0 else f"forward_{index}"
            request = OHLCVRequest(
                symbol=root,
                asset_class=AssetClass.FUTURE,
                exchange=exchange,
                currency=currency,
                last_trade_date_or_contract_month=contract_month,
                multiplier=payload.multiplier,
                duration=payload.duration,
                bar_size=payload.bar_size,
                start_datetime=payload.start_datetime,
                end_datetime=payload.end_datetime,
                what_to_show=payload.what_to_show,
                use_rth=payload.use_rth,
                metadata={"market": "commodity", "role": role},
            )
            async with semaphore:
                if payload.start_datetime is not None:
                    bars = await state.feed.load_historical_ohlcv_range(
                        request,
                        start_datetime=payload.start_datetime,
                        end_datetime=payload.end_datetime,
                        max_chunks=state.settings.ibkr_historical_max_chunks,
                    )
                    if payload.cache_latest and bars:
                        await state.loader.cache_latest_bar(bars[-1])
                else:
                    bars = await state.loader.load(request, persist=False, cache_latest=payload.cache_latest)
            return CommodityFuturePoint(
                role=role,
                symbol=root,
                contract_month=contract_month,
                exchange=exchange,
                currency=currency,
                bar=bars[-1] if bars else None,
            )

        points = await asyncio.gather(*(load_contract(index, month) for index, month in enumerate(contract_months)))
        return CommodityFuturesResponse(symbol=root, as_of_date=payload.as_of_date, contracts=tuple(points))

    if payload.use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("business_commodity_futures", payload),
            load,
            ttl_seconds=payload.cache_ttl_seconds,
        )
    return await load()
