"""REST router for tick data, market rules, IV calculations, and symbol search."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, Query, status
from pydantic import BaseModel

from src.feeds.contracts import ContractSpec
from src.feeds.models import AssetClass
from src.feeds.tick_data import (
    HeadTimestampRequest,
    HistogramDataRequest,
    HistogramDataResponse,
    HistoricalTickRequest,
    HistoricalTickResponse,
    IVCalcRequest,
    MarketRule,
    OptionPriceCalcRequest,
    SmartComponent,
    SymbolDescription,
    TickByTickData,
    TickSubscribeRequest,
    TickType,
    TickUnsubscribeRequest,
)
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

router = APIRouter(prefix="/tick-data", tags=["market-data"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class TickSubscribeResponse(BaseModel):
    """Typed response for tick subscription."""

    status: str
    symbol: str
    sec_type: str
    exchange: str
    tick_type: str


class HeadTimestampResponse(BaseModel):
    """Typed response for head timestamp lookup."""

    symbol: str
    what_to_show: str
    head_timestamp: str | None


class IVCalcResponse(BaseModel):
    """Typed response for implied volatility calculation."""

    symbol: str
    strike: float
    right: str
    expiry: str
    option_price: float
    under_price: float
    implied_volatility: float | None


class OptionPriceCalcResponse(BaseModel):
    """Typed response for option price calculation."""

    symbol: str
    strike: float
    right: str
    expiry: str
    volatility: float
    under_price: float
    option_price: float | None


# ---------------------------------------------------------------------------
# Tick-by-tick streaming
# ---------------------------------------------------------------------------


@router.post("/subscribe", summary="Start tick-by-tick subscription", status_code=status.HTTP_201_CREATED, response_model=TickSubscribeResponse)
async def subscribe_ticks(
    payload: TickSubscribeRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> TickSubscribeResponse:
    """Start a tick-by-tick data subscription for a symbol."""
    await state.feed.start_tick_by_tick(
        symbol=payload.symbol,
        sec_type=payload.sec_type,
        exchange=payload.exchange,
        currency=payload.currency,
        tick_type=payload.tick_type,
        max_ticks=payload.max_ticks,
    )
    return TickSubscribeResponse(
        status="subscribed",
        symbol=payload.symbol,
        sec_type=payload.sec_type,
        exchange=payload.exchange,
        tick_type=payload.tick_type.value,
    )


@router.post("/unsubscribe", summary="Stop tick-by-tick subscription")
async def unsubscribe_ticks(
    payload: TickUnsubscribeRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> dict[str, str]:
    """Stop a tick-by-tick data subscription."""
    await state.feed.stop_tick_by_tick(
        symbol=payload.symbol,
        sec_type=payload.sec_type,
        exchange=payload.exchange,
    )
    return {"status": "unsubscribed", "symbol": payload.symbol}


@router.get("/latest/{symbol}", summary="Get latest N ticks for symbol")
async def get_latest_ticks(
    symbol: str,
    sec_type: str = Query(default="STK", min_length=1),
    exchange: str = Query(default="SMART", min_length=1),
    n: int = Query(default=100, ge=1, le=10_000),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[TickByTickData]:
    """Retrieve the latest N ticks from the in-memory buffer."""
    return state.feed.get_latest_ticks(symbol, sec_type, exchange, n)


# ---------------------------------------------------------------------------
# Historical ticks
# ---------------------------------------------------------------------------


@router.post("/historical", summary="Load historical ticks", response_model=HistoricalTickResponse)
async def load_historical_ticks(
    payload: HistoricalTickRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> HistoricalTickResponse:
    """Load historical tick-level data (trades, bid/ask, midpoints)."""
    return await state.feed.load_historical_ticks(payload)


# ---------------------------------------------------------------------------
# Price histograms
# ---------------------------------------------------------------------------


@router.post("/histogram", summary="Load price histogram", response_model=HistogramDataResponse)
async def load_histogram_data(
    payload: HistogramDataRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> HistogramDataResponse:
    """Load price histogram data."""
    return await state.feed.load_histogram_data(payload)


# ---------------------------------------------------------------------------
# Market rules
# ---------------------------------------------------------------------------


@router.get(
    "/market-rules/{magnitude}",
    summary="Get market rule",
    response_model=MarketRule,
)
async def get_market_rule(
    magnitude: int,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> MarketRule:
    """Get exchange-specific market rules (min tick, price increments)."""
    return await state.feed.load_market_rule(magnitude)


# ---------------------------------------------------------------------------
# Smart components
# ---------------------------------------------------------------------------


@router.get(
    "/smart-components/{exchange}",
    summary="Get smart components",
    response_model=list[SmartComponent],
)
async def get_smart_components(
    exchange: str,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[SmartComponent]:
    """Get smart-routing component exchanges for a given exchange."""
    return await state.feed.load_smart_components(exchange)


# ---------------------------------------------------------------------------
# Head timestamp
# ---------------------------------------------------------------------------


@router.get("/head-timestamp", summary="Get earliest data date", response_model=HeadTimestampResponse)
async def get_head_timestamp(
    symbol: str = Query(min_length=1),
    sec_type: str = Query(default="STK", min_length=1),
    exchange: str = Query(default="SMART", min_length=1),
    currency: str = Query(default="USD", min_length=1),
    what_to_show: str = Query(default="TRADES", min_length=1),
    use_rth: bool = Query(default=True),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> HeadTimestampResponse:
    """Get the earliest available data date for a contract."""
    request = HeadTimestampRequest(
        symbol=symbol,
        sec_type=sec_type,
        exchange=exchange,
        currency=currency,
        what_to_show=what_to_show,
        use_rth=use_rth,
    )
    ts = await state.feed.load_head_timestamp(request)
    return HeadTimestampResponse(
        symbol=symbol,
        what_to_show=what_to_show,
        head_timestamp=ts.isoformat() if ts else None,
    )


# ---------------------------------------------------------------------------
# Implied volatility & option price calculation
# ---------------------------------------------------------------------------


def _option_calc_contract_spec(payload: IVCalcRequest | OptionPriceCalcRequest) -> ContractSpec:
    return ContractSpec(
        symbol=payload.symbol,
        asset_class=AssetClass.OPTION,
        exchange=payload.exchange,
        currency=payload.currency,
        option_sec_type=payload.sec_type,
        underlying_symbol=payload.symbol,
        expiry=payload.expiry,
        last_trade_date_or_contract_month=payload.expiry,
        strike=payload.strike,
        right=payload.right,
        multiplier=payload.multiplier,
    )


@router.post("/calculate/iv", summary="Calculate implied volatility", response_model=IVCalcResponse)
async def calculate_iv(
    payload: IVCalcRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> IVCalcResponse:
    """Calculate implied volatility from the IBKR engine."""
    contract = await state.feed.qualify_contract(_option_calc_contract_spec(payload))
    iv = await state.feed.calculate_iv(contract, payload.option_price, payload.under_price)
    return IVCalcResponse(
        symbol=payload.symbol,
        strike=payload.strike,
        right=payload.right,
        expiry=payload.expiry,
        option_price=payload.option_price,
        under_price=payload.under_price,
        implied_volatility=iv,
    )


@router.post("/calculate/option-price", summary="Calculate option price", response_model=OptionPriceCalcResponse)
async def calculate_option_price(
    payload: OptionPriceCalcRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> OptionPriceCalcResponse:
    """Calculate option price from the IBKR engine."""
    contract = await state.feed.qualify_contract(_option_calc_contract_spec(payload))
    price = await state.feed.calculate_option_price(contract, payload.volatility, payload.under_price)
    return OptionPriceCalcResponse(
        symbol=payload.symbol,
        strike=payload.strike,
        right=payload.right,
        expiry=payload.expiry,
        volatility=payload.volatility,
        under_price=payload.under_price,
        option_price=price,
    )


# ---------------------------------------------------------------------------
# Symbol search
# ---------------------------------------------------------------------------


@router.get(
    "/symbol-search",
    summary="Fuzzy symbol search",
    response_model=list[SymbolDescription],
)
async def symbol_search(
    pattern: str = Query(min_length=1),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[SymbolDescription]:
    """Search for matching symbols using IBKR's fuzzy symbol search."""
    return await state.feed.search_matching_symbols(pattern)
