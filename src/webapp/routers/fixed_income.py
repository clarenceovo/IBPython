from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.feeds.bond_curve import BondCurveRequest, BondCurveResponse
from src.feeds.fixed_income import (
    BondFutureContractSpec,
    BondFutureQuote,
    CTDAnalyticsResponse,
    CurveComparisonResponse,
    DeliverableBasketRequest,
    FixedIncomeReferenceProvider,
    FuturesImpliedCurveResponse,
    build_cash_bond_curve,
    build_futures_implied_curve,
    calculate_ctd_analytics,
    compare_curves,
    default_bond_future_specs,
    fixed_income_market_config,
    quote_from_latest_bar,
)
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.openapi_markdown import markdown_openapi_examples

router = APIRouter(prefix="/business/fixed-income", tags=["business"])


class FixedIncomeCacheControls(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=30, ge=0)


class FixedIncomeMarketRequest(FixedIncomeCacheControls):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    market: str = Field(min_length=1, examples=["UST"])
    valuation_date: date = Field(default_factory=lambda: datetime.now(timezone.utc).date())

    @field_validator("market", mode="before")
    @classmethod
    def normalize_market(cls, value: object) -> str:
        if value is None:
            raise ValueError("market is required")
        normalized = str(value).strip().upper()
        if not normalized:
            raise ValueError("market cannot be empty")
        return normalized


class BondFutureQuotesRequest(FixedIncomeMarketRequest):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    contract_month: str | None = Field(default=None, min_length=1, examples=["202606"])
    futures: tuple[BondFutureContractSpec, ...] | None = None
    duration: str = Field(default="1 D", min_length=1)
    bar_size: str = Field(default="1 min", min_length=1)
    what_to_show: str = Field(default="TRADES", min_length=1)
    use_rth: bool = True
    cache_latest: bool = False
    max_concurrent_requests: int = Field(default=4, ge=1, le=20)

    @field_validator("contract_month", mode="before")
    @classmethod
    def normalize_contract_month(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        return str(value).strip().upper() if str(value).strip() else None

    @field_validator("duration", "bar_size", "what_to_show", mode="before")
    @classmethod
    def normalize_required_text(cls, value: object) -> str:
        if value is None:
            raise ValueError("value is required")
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("value cannot be empty")
        return normalized

    @model_validator(mode="after")
    def validate_futures_or_contract_month(self) -> "BondFutureQuotesRequest":
        if self.futures is None and not self.contract_month:
            raise ValueError("contract_month is required when futures are omitted")
        return self

    def resolved_futures(self) -> tuple[BondFutureContractSpec, ...]:
        if self.futures is not None:
            return self.futures
        assert self.contract_month is not None
        return default_bond_future_specs(self.market, contract_month=self.contract_month)


class CTDRequest(FixedIncomeCacheControls):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    future: BondFutureContractSpec
    valuation_date: date = Field(default_factory=lambda: datetime.now(timezone.utc).date())
    duration: str = Field(default="1 D", min_length=1)
    bar_size: str = Field(default="1 min", min_length=1)
    what_to_show: str = Field(default="TRADES", min_length=1)
    use_rth: bool = True
    cache_latest: bool = False


class FuturesImpliedCurveRequest(BondFutureQuotesRequest):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class CashBondCurveBusinessRequest(FixedIncomeMarketRequest):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    coupon_frequency: int | None = Field(default=None, ge=1)


class CurveComparisonRequest(FuturesImpliedCurveRequest):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    coupon_frequency: int | None = Field(default=None, ge=1)


@router.post(
    "/getBondFutureQuotes",
    response_model=list[BondFutureQuote],
    summary="Load latest IBKR bond futures prices for a sovereign market",
)
async def get_bond_future_quotes(
    payload: Annotated[
        BondFutureQuotesRequest,
        Body(openapi_examples=markdown_openapi_examples("business.fixedIncome.getBondFutureQuotes")),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[BondFutureQuote]:
    async def load() -> list[BondFutureQuote]:
        return await _load_bond_future_quotes(payload, state)

    if payload.use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("business_fixed_income_bond_future_quotes", payload),
            load,
            ttl_seconds=payload.cache_ttl_seconds,
        )
    return await load()


@router.post(
    "/getCTD",
    response_model=CTDAnalyticsResponse,
    summary="Calculate CTD analytics for one bond future",
)
async def get_ctd(
    payload: Annotated[
        CTDRequest,
        Body(openapi_examples=markdown_openapi_examples("business.fixedIncome.getCTD")),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> CTDAnalyticsResponse:
    provider = _fixed_income_provider(state)

    async def load() -> CTDAnalyticsResponse:
        return await _calculate_ctd(payload, provider, state)

    if payload.use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("business_fixed_income_ctd", payload),
            load,
            ttl_seconds=payload.cache_ttl_seconds,
        )
    return await load()


@router.post(
    "/getFuturesImpliedCurve",
    response_model=FuturesImpliedCurveResponse,
    summary="Build a futures-implied CTD yield curve",
)
async def get_futures_implied_curve(
    payload: Annotated[
        FuturesImpliedCurveRequest,
        Body(openapi_examples=markdown_openapi_examples("business.fixedIncome.getFuturesImpliedCurve")),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> FuturesImpliedCurveResponse:
    provider = _fixed_income_provider(state)

    async def load() -> FuturesImpliedCurveResponse:
        return await _build_futures_implied_curve(payload, provider, state)

    if payload.use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("business_fixed_income_futures_implied_curve", payload),
            load,
            ttl_seconds=payload.cache_ttl_seconds,
        )
    return await load()


@router.post(
    "/getCashBondCurve",
    response_model=BondCurveResponse,
    summary="Build a cash-bond sovereign curve",
)
async def get_cash_bond_curve(
    payload: Annotated[
        CashBondCurveBusinessRequest,
        Body(openapi_examples=markdown_openapi_examples("business.fixedIncome.getCashBondCurve")),
    ],
) -> BondCurveResponse:
    try:
        return build_cash_bond_curve(
            BondCurveRequest(
                market=payload.market,
                valuation_date=payload.valuation_date,
                coupon_frequency=payload.coupon_frequency,
            )
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post(
    "/getCurveComparison",
    response_model=CurveComparisonResponse,
    summary="Compare indicative cash and futures-implied curves",
)
async def get_curve_comparison(
    payload: Annotated[
        CurveComparisonRequest,
        Body(openapi_examples=markdown_openapi_examples("business.fixedIncome.getCurveComparison")),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> CurveComparisonResponse:
    provider = _fixed_income_provider(state)

    async def load() -> CurveComparisonResponse:
        cash_curve = build_cash_bond_curve(
            BondCurveRequest(
                market=payload.market,
                valuation_date=payload.valuation_date,
                coupon_frequency=payload.coupon_frequency,
            )
        )
        futures_curve = await _build_futures_implied_curve(payload, provider, state)
        return compare_curves(cash_curve=cash_curve, futures_curve=futures_curve)

    if payload.use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("business_fixed_income_curve_comparison", payload),
            load,
            ttl_seconds=payload.cache_ttl_seconds,
        )
    return await load()


async def _load_bond_future_quotes(payload: BondFutureQuotesRequest, state: IBKRRestAppState) -> list[BondFutureQuote]:
    futures = payload.resolved_futures()
    semaphore = asyncio.Semaphore(payload.max_concurrent_requests)

    async def load_one(spec: BondFutureContractSpec) -> BondFutureQuote:
        async with semaphore:
            request = spec.to_ohlcv_request(
                duration=payload.duration,
                bar_size=payload.bar_size,
                what_to_show=payload.what_to_show,
                use_rth=payload.use_rth,
            )
            bars = await state.loader.load(request, persist=False, cache_latest=payload.cache_latest)
            return quote_from_latest_bar(spec, bars)

    try:
        return list(await asyncio.gather(*(load_one(spec) for spec in futures)))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


async def _calculate_ctd(
    payload: CTDRequest,
    provider: FixedIncomeReferenceProvider,
    state: IBKRRestAppState,
) -> CTDAnalyticsResponse:
    quote_payload = BondFutureQuotesRequest(
        market=payload.future.market,
        valuation_date=payload.valuation_date,
        futures=(payload.future,),
        duration=payload.duration,
        bar_size=payload.bar_size,
        what_to_show=payload.what_to_show,
        use_rth=payload.use_rth,
        cache_latest=payload.cache_latest,
        use_ttl_cache=False,
    )
    quote = (await _load_bond_future_quotes(quote_payload, state))[0]
    future = payload.future.resolved_definition()
    basket = await provider.get_deliverable_basket(
        DeliverableBasketRequest(
            market=future.market.value,
            futures_symbol=future.futures_symbol,
            contract_month=payload.future.contract_month,
            valuation_date=payload.valuation_date,
        )
    )
    if not basket:
        raise HTTPException(status_code=503, detail=f"fixed-income provider returned no basket for {future.futures_symbol}")
    try:
        return calculate_ctd_analytics(
            future=future,
            contract_month=payload.future.contract_month,
            valuation_date=payload.valuation_date,
            futures_price=quote.price,
            basket=tuple(basket),
            provider_name=provider.name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


async def _build_futures_implied_curve(
    payload: FuturesImpliedCurveRequest,
    provider: FixedIncomeReferenceProvider,
    state: IBKRRestAppState,
) -> FuturesImpliedCurveResponse:
    config = fixed_income_market_config(payload.market)
    ctd_results = []
    for future in payload.resolved_futures():
        ctd_results.append(
            await _calculate_ctd(
                CTDRequest(
                    future=future,
                    valuation_date=payload.valuation_date,
                    duration=payload.duration,
                    bar_size=payload.bar_size,
                    what_to_show=payload.what_to_show,
                    use_rth=payload.use_rth,
                    cache_latest=payload.cache_latest,
                    use_ttl_cache=False,
                ),
                provider,
                state,
            )
        )
    try:
        return build_futures_implied_curve(
            market=config.market,
            currency=config.currency,
            valuation_date=payload.valuation_date,
            ctd_results=tuple(ctd_results),
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _fixed_income_provider(state: IBKRRestAppState) -> FixedIncomeReferenceProvider:
    provider = getattr(state, "fixed_income_reference_provider", None)
    if provider is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "fixed-income reference provider is not configured; CTD and futures-implied curves require "
                "deliverable basket, conversion factor, accrued interest, and bond term data"
            ),
        )
    return provider
