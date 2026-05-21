"""Returns / market panel endpoints for the business domain."""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from src.feeds.models import AssetClass, OHLCVBar
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.openapi_markdown import markdown_openapi_examples
from src.webapp.routers.business_shared import (
    MarketPanelRequest,
    UniverseBarsRequest,
    load_many_ohlcv,
    symbol_to_ohlcv_request,
)

router = APIRouter()


class ReturnPoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    timestamp: object
    close: float
    previous_close: float
    simple_return: float
    log_return: float


class SymbolReturnSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    observations: int
    return_count: int
    cumulative_return: float | None
    realized_volatility: float | None
    first_timestamp: object
    last_timestamp: object
    points: list[ReturnPoint]


class ReturnsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    asset_class: AssetClass
    bar_size: str
    summaries: list[SymbolReturnSummary]
    warnings: list[str] = Field(default_factory=list)


@router.post(
    "/getMarketPanel",
    response_model=list[OHLCVBar],
    summary="Load a multi-symbol OHLCV panel",
)
async def get_market_panel(
    payload: Annotated[
        MarketPanelRequest,
        Body(openapi_examples=markdown_openapi_examples("business.getMarketPanel")),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OHLCVBar]:
    async def load() -> list[OHLCVBar]:
        requests = [symbol_to_ohlcv_request(item, payload) for item in payload.symbols]
        return await load_many_ohlcv(requests, payload, state)

    if payload.use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("business_market_panel", payload),
            load,
            ttl_seconds=payload.cache_ttl_seconds,
        )
    return await load()


@router.post(
    "/getUniverseBars",
    response_model=list[OHLCVBar],
    summary="Load OHLCV bars for a named universe",
)
async def get_universe_bars(
    payload: Annotated[
        UniverseBarsRequest,
        Body(openapi_examples=markdown_openapi_examples("business.getUniverseBars")),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OHLCVBar]:
    async def load() -> list[OHLCVBar]:
        symbols = await _resolve_universe_symbols(payload, state)
        panel = MarketPanelRequest(
            symbols=symbols,
            asset_class=payload.asset_class,
            exchange=payload.exchange,
            currency=payload.currency,
            duration=payload.duration,
            bar_size=payload.bar_size,
            start_datetime=payload.start_datetime,
            end_datetime=payload.end_datetime,
            what_to_show=payload.what_to_show,
            use_rth=payload.use_rth,
            cache_latest=payload.cache_latest,
            max_concurrent_requests=payload.max_concurrent_requests,
            use_ttl_cache=False,
        )
        requests = [symbol_to_ohlcv_request(item, panel) for item in panel.symbols[: payload.max_symbols]]
        return await load_many_ohlcv(requests, panel, state)

    if payload.use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("business_universe_bars", payload),
            load,
            ttl_seconds=payload.cache_ttl_seconds,
        )
    return await load()


@router.post(
    "/getReturns",
    response_model=ReturnsResponse,
    summary="Load bars and compute close-to-close returns",
)
async def get_returns(
    payload: Annotated[
        MarketPanelRequest,
        Body(openapi_examples=markdown_openapi_examples("business.getReturns")),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> ReturnsResponse:
    bars = await get_market_panel(payload, state)
    return _bars_to_returns(asset_class=payload.asset_class, bar_size=payload.bar_size, bars=bars)


async def _resolve_universe_symbols(payload: UniverseBarsRequest, state: IBKRRestAppState) -> list[str]:
    if payload.symbols:
        return [symbol.strip().upper() for symbol in payload.symbols if symbol.strip()][: payload.max_symbols]
    composition = await state.redis.get_index_composition(payload.universe)
    if composition is None:
        raise HTTPException(status_code=404, detail=f"universe {payload.universe!r} not found in Redis index composition")
    return [item.symbol for item in composition.constituents][: payload.max_symbols]


def _bars_to_returns(*, asset_class: AssetClass, bar_size: str, bars: list[OHLCVBar]) -> ReturnsResponse:
    grouped: dict[str, list[OHLCVBar]] = defaultdict(list)
    for bar in bars:
        grouped[bar.symbol].append(bar)
    summaries: list[SymbolReturnSummary] = []
    warnings: list[str] = []
    for symbol, symbol_bars in sorted(grouped.items()):
        ordered = sorted(symbol_bars, key=lambda bar: bar.timestamp)
        points: list[ReturnPoint] = []
        for previous, current in zip(ordered, ordered[1:], strict=False):
            if previous.close <= 0 or current.close <= 0:
                warnings.append(f"{symbol}: skipped non-positive close at {current.timestamp.isoformat()}")
                continue
            simple_return = current.close / previous.close - 1.0
            points.append(
                ReturnPoint(
                    symbol=symbol,
                    timestamp=current.timestamp,
                    close=current.close,
                    previous_close=previous.close,
                    simple_return=simple_return,
                    log_return=math.log(current.close / previous.close),
                )
            )
        cumulative_return = ordered[-1].close / ordered[0].close - 1.0 if len(ordered) >= 2 and ordered[0].close > 0 else None
        summaries.append(
            SymbolReturnSummary(
                symbol=symbol,
                observations=len(ordered),
                return_count=len(points),
                cumulative_return=cumulative_return,
                realized_volatility=_sample_volatility([point.log_return for point in points]),
                first_timestamp=ordered[0].timestamp if ordered else None,
                last_timestamp=ordered[-1].timestamp if ordered else None,
                points=points,
            )
        )
    return ReturnsResponse(asset_class=asset_class, bar_size=bar_size, summaries=summaries, warnings=warnings)


def _sample_volatility(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)
