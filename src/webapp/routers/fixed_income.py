from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timezone
from enum import StrEnum
from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.feeds.bond_curve import BondCurveRequest, BondCurveResponse, resolve_bond_curve_market
from src.feeds.bonds import SovereignBondMarket, YieldCurveDTO
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
from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.openapi_markdown import markdown_openapi_examples

router = APIRouter(prefix="/business/fixed-income", tags=["business"])


class BondYieldCurveSourceMode(StrEnum):
    AUTO = "auto"
    FUTURES_IMPLIED = "futures_implied"
    INDICATIVE_PLACEHOLDER = "indicative_placeholder"


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


class BondYieldCurveRequest(FixedIncomeMarketRequest):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    source_mode: BondYieldCurveSourceMode = Field(
        default=BondYieldCurveSourceMode.INDICATIVE_PLACEHOLDER,
        description=(
            "indicative_placeholder returns static standard-tenor curves; futures_implied uses IBKR bond-futures "
            "prices plus the configured fixed-income reference provider; auto uses futures when contract IDs and "
            "a provider are available, otherwise falls back only when allow_indicative_fallback is true."
        ),
    )
    allow_indicative_fallback: bool = False
    contract_month: str | None = Field(default=None, min_length=1, examples=["202606"])
    futures: tuple[BondFutureContractSpec, ...] | None = None
    duration: str = Field(default="1 D", min_length=1)
    bar_size: str = Field(default="1 min", min_length=1)
    what_to_show: str = Field(default="TRADES", min_length=1)
    use_rth: bool = True
    cache_latest: bool = False
    max_concurrent_requests: int = Field(default=4, ge=1, le=20)
    coupon_frequency: int | None = Field(default=None, ge=1)

    @field_validator("contract_month", mode="before")
    @classmethod
    def normalize_contract_month(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        normalized = str(value).strip().upper()
        return normalized or None

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
    def validate_supported_curve_market(self) -> "BondYieldCurveRequest":
        market = resolve_bond_curve_market(self.market)
        if market not in {
            SovereignBondMarket.US_TREASURY,
            SovereignBondMarket.JGB,
            SovereignBondMarket.GERMAN_BUND,
        }:
            raise ValueError("bond yield curve endpoint currently supports UST, JGB, and BUND/GERMAN_BUND")
        if self.source_mode is BondYieldCurveSourceMode.FUTURES_IMPLIED and self.futures is None and not self.contract_month:
            raise ValueError("futures_implied bond yield curves require contract_month or explicit futures")
        return self

    def to_futures_curve_request(self) -> FuturesImpliedCurveRequest:
        return FuturesImpliedCurveRequest(
            market=self.market,
            valuation_date=self.valuation_date,
            contract_month=self.contract_month,
            futures=self.futures,
            duration=self.duration,
            bar_size=self.bar_size,
            what_to_show=self.what_to_show,
            use_rth=self.use_rth,
            cache_latest=self.cache_latest,
            max_concurrent_requests=self.max_concurrent_requests,
            use_ttl_cache=False,
        )


class BondYieldCurvePoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenor: str | None = None
    tenor_years: float = Field(gt=0)
    maturity_date: date
    par_yield: float
    zero_rate: float
    discount_factor: float
    bond_symbol: str
    futures_symbol: str | None = None
    source: str


class BondYieldCurveResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    market: SovereignBondMarket
    currency: str
    valuation_date: date
    source_mode: BondYieldCurveSourceMode
    source: str
    curve: YieldCurveDTO
    points: tuple[BondYieldCurvePoint, ...]
    caveats: tuple[str, ...] = ()
    indicative_curve: BondCurveResponse | None = None
    futures_implied_curve: FuturesImpliedCurveResponse | None = None


class FedFundsFuturesRateRequest(FixedIncomeCacheControls):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(default="ZQ", min_length=1, description="30-Day Fed Funds futures root.")
    exchange: str = Field(default="CBOT", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    contract_month: str | None = Field(default=None, min_length=1, examples=["202606"])
    local_symbol: str | None = Field(default=None, min_length=1)
    con_id: int | None = Field(default=None, gt=0)
    multiplier: str | None = Field(default=None, min_length=1)
    duration: str = Field(default="1 D", min_length=1)
    bar_size: str = Field(default="1 min", min_length=1)
    what_to_show: str = Field(default="TRADES", min_length=1)
    use_rth: bool = True
    cache_latest: bool = False

    @field_validator("symbol", "exchange", "currency", "contract_month", "local_symbol", "multiplier", mode="before")
    @classmethod
    def normalize_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        normalized = str(value).strip().upper()
        return normalized or None

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
    def validate_contract_identifier(self) -> "FedFundsFuturesRateRequest":
        if not (self.contract_month or self.local_symbol or self.con_id):
            raise ValueError("Fed Funds futures rate proxy requires contract_month, local_symbol, or con_id")
        return self

    def to_ohlcv_request(self) -> OHLCVRequest:
        return OHLCVRequest(
            symbol=self.symbol,
            asset_class=AssetClass.FUTURE,
            exchange=self.exchange,
            currency=self.currency,
            duration=self.duration,
            bar_size=self.bar_size,
            what_to_show=self.what_to_show,
            use_rth=self.use_rth,
            last_trade_date_or_contract_month=self.contract_month,
            local_symbol=self.local_symbol,
            con_id=self.con_id,
            multiplier=self.multiplier,
            metadata={"rate_proxy": "fed_funds_futures", "rate_formula": "100 - futures_price"},
        )


class FedFundsFuturesRateResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    exchange: str
    currency: str
    contract_month: str | None
    timestamp: datetime
    futures_price: float
    implied_average_rate_percent: float
    rate_formula: str = "100 - futures_price"
    source: str = "ibkr_30_day_fed_funds_futures"
    actual_overnight_fixing_available_from_ibkr: bool = False
    bar: OHLCVBar
    caveats: tuple[str, ...] = (
        "This is an implied average rate from 30-Day Fed Funds futures, not the official overnight effective Fed Funds fixing.",
        "For the official overnight EFFR time series, use a rates source such as the Federal Reserve/FRED rather than IBKR market data.",
    )


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
    "/getBondYieldCurve",
    response_model=BondYieldCurveResponse,
    summary="Get a sovereign bond yield curve for UST, JGB, or German Bunds",
)
async def get_bond_yield_curve(
    payload: BondYieldCurveRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> BondYieldCurveResponse:
    async def load() -> BondYieldCurveResponse:
        return await _build_bond_yield_curve(payload, state)

    if payload.use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("business_fixed_income_bond_yield_curve", payload),
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


@router.post(
    "/getFedFundsFuturesRate",
    response_model=FedFundsFuturesRateResponse,
    summary="Load IBKR 30-Day Fed Funds futures implied average rate",
)
async def get_fed_funds_futures_rate(
    payload: FedFundsFuturesRateRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> FedFundsFuturesRateResponse:
    async def load() -> FedFundsFuturesRateResponse:
        return await _load_fed_funds_futures_rate(payload, state)

    if payload.use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("business_fixed_income_fed_funds_futures_rate", payload),
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

    raw_quotes = await asyncio.gather(*(load_one(spec) for spec in futures), return_exceptions=True)
    quotes: list[BondFutureQuote] = []
    for spec, raw in zip(futures, raw_quotes, strict=True):
        if isinstance(raw, Exception):
            if isinstance(raw, ValueError):
                raise HTTPException(status_code=422, detail=str(raw)) from raw
            logging.getLogger("fixed_income").warning("Failed to load bond future quote: %s", raw)
            continue
        quotes.append(raw)  # type: ignore[arg-type]
    return quotes


async def _build_bond_yield_curve(payload: BondYieldCurveRequest, state: IBKRRestAppState) -> BondYieldCurveResponse:
    provider = getattr(state, "fixed_income_reference_provider", None)
    can_use_futures = provider is not None and (payload.contract_month is not None or payload.futures is not None)

    if payload.source_mode is BondYieldCurveSourceMode.FUTURES_IMPLIED:
        futures_curve = await _build_futures_implied_curve(payload.to_futures_curve_request(), _fixed_income_provider(state), state)
        return _yield_curve_response_from_futures(futures_curve)

    if payload.source_mode is BondYieldCurveSourceMode.AUTO and can_use_futures:
        futures_curve = await _build_futures_implied_curve(payload.to_futures_curve_request(), provider, state)
        return _yield_curve_response_from_futures(futures_curve)

    if (
        payload.source_mode is BondYieldCurveSourceMode.INDICATIVE_PLACEHOLDER
        or payload.allow_indicative_fallback
    ):
        try:
            cash_curve = build_cash_bond_curve(
                BondCurveRequest(
                    market=payload.market,
                    valuation_date=payload.valuation_date,
                    coupon_frequency=payload.coupon_frequency,
                )
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        caveats = cash_curve.caveats
        if payload.source_mode is BondYieldCurveSourceMode.AUTO:
            caveats = (
                "AUTO mode did not have both a fixed-income reference provider and futures contract identifiers; returned indicative fallback.",
                *cash_curve.caveats,
            )
        return _yield_curve_response_from_indicative(cash_curve, caveats=caveats)

    raise HTTPException(
        status_code=503,
        detail=(
            "no live IBKR-backed sovereign yield curve is available for this request; provide contract_month or "
            "explicit futures plus FIXED_INCOME_REFERENCE_PROVIDER, or set allow_indicative_fallback=true"
        ),
    )


def _yield_curve_response_from_indicative(
    curve: BondCurveResponse,
    *,
    caveats: tuple[str, ...] | None = None,
) -> BondYieldCurveResponse:
    curve_points_by_id = {point.source_instrument_id: point for point in curve.curve.points}
    points = []
    for render_point in curve.render_points:
        curve_point = curve_points_by_id[render_point.ctd_symbol]
        points.append(
            BondYieldCurvePoint(
                tenor=render_point.tenor,
                tenor_years=render_point.tenor_years,
                maturity_date=render_point.maturity_date,
                par_yield=render_point.par_yield,
                zero_rate=curve_point.zero_rate,
                discount_factor=curve_point.discount_factor,
                bond_symbol=render_point.ctd_symbol,
                futures_symbol=render_point.futures_symbol,
                source="indicative_static_standard_tenor_provider",
            )
        )
    return BondYieldCurveResponse(
        market=curve.market,
        currency=curve.currency,
        valuation_date=curve.valuation_date,
        source_mode=BondYieldCurveSourceMode.INDICATIVE_PLACEHOLDER,
        source=curve.source,
        curve=curve.curve,
        points=tuple(points),
        caveats=caveats or curve.caveats,
        indicative_curve=curve,
    )


def _yield_curve_response_from_futures(curve: FuturesImpliedCurveResponse) -> BondYieldCurveResponse:
    par_yields_by_id = {
        f"{point.futures_symbol}_{point.selected_ctd.symbol}": point.implied_yield
        for point in curve.points
    }
    futures_symbol_by_id = {
        f"{point.futures_symbol}_{point.selected_ctd.symbol}": point.futures_symbol
        for point in curve.points
    }
    points = tuple(
        BondYieldCurvePoint(
            tenor=None,
            tenor_years=curve_point.tenor_years,
            maturity_date=curve_point.maturity_date,
            par_yield=par_yields_by_id[curve_point.source_instrument_id],
            zero_rate=curve_point.zero_rate,
            discount_factor=curve_point.discount_factor,
            bond_symbol=curve_point.source_instrument_id.split("_", 1)[1],
            futures_symbol=futures_symbol_by_id[curve_point.source_instrument_id],
            source="ibkr_futures_implied_ctd",
        )
        for curve_point in curve.curve.points
    )
    caveats = (
        "Futures-implied curve uses IBKR futures prices plus external/provider CTD basket, conversion factor, and bond term data.",
        "It is not a direct IBKR cash sovereign yield-curve feed.",
        *curve.diagnostics.warnings,
    )
    return BondYieldCurveResponse(
        market=curve.market,
        currency=curve.currency,
        valuation_date=curve.valuation_date,
        source_mode=BondYieldCurveSourceMode.FUTURES_IMPLIED,
        source="ibkr_futures_implied_ctd",
        curve=curve.curve,
        points=points,
        caveats=caveats,
        futures_implied_curve=curve,
    )


async def _load_fed_funds_futures_rate(
    payload: FedFundsFuturesRateRequest,
    state: IBKRRestAppState,
) -> FedFundsFuturesRateResponse:
    bars = await state.loader.load(payload.to_ohlcv_request(), persist=False, cache_latest=payload.cache_latest)
    if not bars:
        raise HTTPException(status_code=503, detail="IBKR returned no bars for Fed Funds futures")
    latest = max(bars, key=lambda bar: bar.timestamp)
    return FedFundsFuturesRateResponse(
        symbol=payload.symbol,
        exchange=payload.exchange,
        currency=payload.currency,
        contract_month=payload.contract_month,
        timestamp=latest.timestamp,
        futures_price=latest.close,
        implied_average_rate_percent=100.0 - latest.close,
        bar=latest,
    )


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
