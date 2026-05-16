from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.feeds.bonds import (
    BondInstrument,
    CTDFutureDefinition,
    SovereignBondMarket,
    YieldCurveBootstrapInstrument,
    YieldCurveDTO,
)


class BondCurveRequest(BaseModel):
    """Business request for an indicative sovereign bond curve."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    market: str = Field(
        min_length=1,
        description="Sovereign market alias, for example UST, JGB, UK, UK_GILT, BUND, GERMAN_BUND, or KTB.",
        examples=["UST"],
    )
    valuation_date: date = Field(default_factory=lambda: datetime.now(timezone.utc).date())
    coupon_frequency: int | None = Field(default=None, ge=1)

    @field_validator("market", mode="before")
    @classmethod
    def normalize_market(cls, value: Any) -> str:
        if value is None:
            raise ValueError("market is required")
        normalized = str(value).strip().upper()
        if not normalized:
            raise ValueError("market cannot be empty")
        return normalized


class StandardTenorCTDPoint(BaseModel):
    """Standard-tenor CTD or benchmark point used as a curve input."""

    model_config = ConfigDict(extra="forbid")

    tenor: str
    tenor_years: float = Field(gt=0)
    maturity_date: date
    bond: BondInstrument
    future: CTDFutureDefinition
    par_yield: float = Field(description="Decimal par yield, e.g. 0.042 for 4.2%")
    yield_source: str
    ctd_status: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class BondCurveRenderPoint(BaseModel):
    """Chart-ready curve point."""

    model_config = ConfigDict(extra="forbid")

    tenor: str
    tenor_years: float
    maturity_date: date
    par_yield: float
    zero_rate: float
    discount_factor: float
    ctd_symbol: str
    futures_symbol: str


class BondCurveResponse(BaseModel):
    """Business response for getBondCurve."""

    model_config = ConfigDict(extra="forbid")

    curve_id: str
    market: SovereignBondMarket
    market_alias: str
    currency: str
    valuation_date: date
    standard_ctd_points: tuple[StandardTenorCTDPoint, ...]
    curve: YieldCurveDTO
    render_points: tuple[BondCurveRenderPoint, ...]
    source: str
    caveats: tuple[str, ...]


class _StandardTenorTemplate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenor: str
    tenor_years: int
    futures_symbol: str
    futures_exchange: str
    par_yield: float


class _MarketTemplate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    market: SovereignBondMarket
    currency: str
    country: str
    issuer: str
    futures_exchange: str
    coupon_frequency: int = 2
    tenors: tuple[_StandardTenorTemplate, ...]


_MARKET_ALIASES: dict[str, SovereignBondMarket] = {
    "UST": SovereignBondMarket.US_TREASURY,
    "US": SovereignBondMarket.US_TREASURY,
    "US_TREASURY": SovereignBondMarket.US_TREASURY,
    "TREASURY": SovereignBondMarket.US_TREASURY,
    "JGB": SovereignBondMarket.JGB,
    "JAPAN": SovereignBondMarket.JGB,
    "JP": SovereignBondMarket.JGB,
    "KTB": SovereignBondMarket.KTB,
    "KOREA": SovereignBondMarket.KTB,
    "KR": SovereignBondMarket.KTB,
    "BUND": SovereignBondMarket.GERMAN_BUND,
    "GERMAN_BUND": SovereignBondMarket.GERMAN_BUND,
    "GERMANY": SovereignBondMarket.GERMAN_BUND,
    "DE": SovereignBondMarket.GERMAN_BUND,
    "UK": SovereignBondMarket.UK_GILT,
    "UK_GILT": SovereignBondMarket.UK_GILT,
    "GILT": SovereignBondMarket.UK_GILT,
    "GB": SovereignBondMarket.UK_GILT,
}


_MARKET_TEMPLATES: dict[SovereignBondMarket, _MarketTemplate] = {
    SovereignBondMarket.US_TREASURY: _MarketTemplate(
        market=SovereignBondMarket.US_TREASURY,
        currency="USD",
        country="US",
        issuer="US TREASURY",
        futures_exchange="CBOT",
        tenors=(
            _StandardTenorTemplate(tenor="2Y", tenor_years=2, futures_symbol="ZT", futures_exchange="CBOT", par_yield=0.0430),
            _StandardTenorTemplate(tenor="5Y", tenor_years=5, futures_symbol="ZF", futures_exchange="CBOT", par_yield=0.0410),
            _StandardTenorTemplate(tenor="10Y", tenor_years=10, futures_symbol="ZN", futures_exchange="CBOT", par_yield=0.0420),
            _StandardTenorTemplate(tenor="30Y", tenor_years=30, futures_symbol="ZB", futures_exchange="CBOT", par_yield=0.0440),
        ),
    ),
    SovereignBondMarket.JGB: _MarketTemplate(
        market=SovereignBondMarket.JGB,
        currency="JPY",
        country="JP",
        issuer="JAPAN GOVERNMENT",
        futures_exchange="OSE.JPN",
        tenors=(
            _StandardTenorTemplate(tenor="2Y", tenor_years=2, futures_symbol="JGB2Y", futures_exchange="OSE.JPN", par_yield=0.0060),
            _StandardTenorTemplate(tenor="5Y", tenor_years=5, futures_symbol="JGB5Y", futures_exchange="OSE.JPN", par_yield=0.0090),
            _StandardTenorTemplate(tenor="10Y", tenor_years=10, futures_symbol="JGB", futures_exchange="OSE.JPN", par_yield=0.0140),
            _StandardTenorTemplate(tenor="20Y", tenor_years=20, futures_symbol="JGB20Y", futures_exchange="OSE.JPN", par_yield=0.0220),
            _StandardTenorTemplate(tenor="30Y", tenor_years=30, futures_symbol="JGB30Y", futures_exchange="OSE.JPN", par_yield=0.0260),
        ),
    ),
    SovereignBondMarket.UK_GILT: _MarketTemplate(
        market=SovereignBondMarket.UK_GILT,
        currency="GBP",
        country="GB",
        issuer="UK DMO",
        futures_exchange="ICEEU",
        tenors=(
            _StandardTenorTemplate(tenor="2Y", tenor_years=2, futures_symbol="G2", futures_exchange="ICEEU", par_yield=0.0410),
            _StandardTenorTemplate(tenor="5Y", tenor_years=5, futures_symbol="G5", futures_exchange="ICEEU", par_yield=0.0390),
            _StandardTenorTemplate(tenor="10Y", tenor_years=10, futures_symbol="G", futures_exchange="ICEEU", par_yield=0.0410),
            _StandardTenorTemplate(tenor="30Y", tenor_years=30, futures_symbol="GL", futures_exchange="ICEEU", par_yield=0.0470),
        ),
    ),
    SovereignBondMarket.GERMAN_BUND: _MarketTemplate(
        market=SovereignBondMarket.GERMAN_BUND,
        currency="EUR",
        country="DE",
        issuer="BUNDESREPUBLIK DEUTSCHLAND",
        futures_exchange="EUREX",
        tenors=(
            _StandardTenorTemplate(tenor="2Y", tenor_years=2, futures_symbol="FGBS", futures_exchange="EUREX", par_yield=0.0220),
            _StandardTenorTemplate(tenor="5Y", tenor_years=5, futures_symbol="FGBM", futures_exchange="EUREX", par_yield=0.0240),
            _StandardTenorTemplate(tenor="10Y", tenor_years=10, futures_symbol="GBL", futures_exchange="EUREX", par_yield=0.0260),
            _StandardTenorTemplate(tenor="30Y", tenor_years=30, futures_symbol="FGBX", futures_exchange="EUREX", par_yield=0.0290),
        ),
    ),
    SovereignBondMarket.KTB: _MarketTemplate(
        market=SovereignBondMarket.KTB,
        currency="KRW",
        country="KR",
        issuer="KOREA TREASURY",
        futures_exchange="KRX",
        tenors=(
            _StandardTenorTemplate(tenor="3Y", tenor_years=3, futures_symbol="KTB3", futures_exchange="KRX", par_yield=0.0310),
            _StandardTenorTemplate(tenor="5Y", tenor_years=5, futures_symbol="KTB5", futures_exchange="KRX", par_yield=0.0320),
            _StandardTenorTemplate(tenor="10Y", tenor_years=10, futures_symbol="KTB", futures_exchange="KRX", par_yield=0.0340),
            _StandardTenorTemplate(tenor="30Y", tenor_years=30, futures_symbol="KTB30", futures_exchange="KRX", par_yield=0.0360),
        ),
    ),
}


def resolve_bond_curve_market(value: str) -> SovereignBondMarket:
    alias = value.strip().upper()
    try:
        return _MARKET_ALIASES[alias]
    except KeyError as exc:
        supported = ", ".join(sorted(_MARKET_ALIASES))
        raise ValueError(f"unsupported bond curve market {value!r}; supported aliases: {supported}") from exc


def build_standard_bond_curve(request: BondCurveRequest) -> BondCurveResponse:
    market = resolve_bond_curve_market(request.market)
    template = _MARKET_TEMPLATES[market]
    coupon_frequency = request.coupon_frequency or template.coupon_frequency
    standard_points = tuple(
        _build_standard_point(
            request=request,
            template=template,
            tenor=tenor,
            coupon_frequency=coupon_frequency,
        )
        for tenor in template.tenors
    )
    curve = YieldCurveDTO(
        curve_id=f"{market.value}_{request.valuation_date.isoformat()}",
        currency=template.currency,
        valuation_date=request.valuation_date,
        input_instruments=tuple(
            YieldCurveBootstrapInstrument(
                instrument_id=point.bond.symbol,
                maturity_date=point.maturity_date,
                par_yield=point.par_yield,
                coupon_frequency=coupon_frequency,
                metadata={
                    "tenor": point.tenor,
                    "market": market.value,
                    "ctd_status": point.ctd_status,
                    "futures_symbol": point.future.futures_symbol,
                },
            )
            for point in standard_points
        ),
        metadata={
            "market": market.value,
            "market_alias": request.market,
            "provider": "indicative_static_standard_tenor_provider",
            "ctd_status": "indicative_placeholder",
        },
    ).bootstrap()
    render_points = tuple(_render_point(point, curve) for point in standard_points)
    return BondCurveResponse(
        curve_id=curve.curve_id,
        market=market,
        market_alias=request.market,
        currency=template.currency,
        valuation_date=request.valuation_date,
        standard_ctd_points=standard_points,
        curve=curve,
        render_points=render_points,
        source="indicative_static_standard_tenor_provider",
        caveats=(
            "Built-in standard-tenor CTD bonds are indicative placeholders, not exchange-official CTD selections.",
            "Production CTD selection requires delivery basket, conversion factor, accrued interest, delivery date, financing, and futures price data from an exchange/vendor/provider.",
            "Par yields are static defaults for API workflow validation; replace with a point-in-time rates source before trading or research decisions.",
        ),
    )


def _build_standard_point(
    *,
    request: BondCurveRequest,
    template: _MarketTemplate,
    tenor: _StandardTenorTemplate,
    coupon_frequency: int,
) -> StandardTenorCTDPoint:
    maturity_date = _add_years(request.valuation_date, tenor.tenor_years)
    future = CTDFutureDefinition(
        market=template.market,
        futures_symbol=tenor.futures_symbol,
        exchange=tenor.futures_exchange,
        currency=template.currency,
        description=f"{template.market.value} {tenor.tenor} standard-tenor delivery basket",
        metadata={"tenor": tenor.tenor},
    )
    symbol = f"{template.market.value}_{tenor.tenor}_CTD"
    bond = BondInstrument(
        symbol=symbol,
        market=template.market,
        country=template.country,
        issuer=template.issuer,
        currency=template.currency,
        maturity_date=maturity_date,
        coupon_rate=tenor.par_yield,
        coupon_frequency=coupon_frequency,
        metadata={
            "tenor": tenor.tenor,
            "tenor_years": tenor.tenor_years,
            "ctd_status": "indicative_placeholder",
            "futures_symbol": tenor.futures_symbol,
        },
    )
    return StandardTenorCTDPoint(
        tenor=tenor.tenor,
        tenor_years=float(tenor.tenor_years),
        maturity_date=maturity_date,
        bond=bond,
        future=future,
        par_yield=tenor.par_yield,
        yield_source="indicative_static_default",
        ctd_status="indicative_placeholder",
    )


def _render_point(point: StandardTenorCTDPoint, curve: YieldCurveDTO) -> BondCurveRenderPoint:
    curve_point = next(
        item
        for item in curve.points
        if item.source_instrument_id == point.bond.symbol
    )
    return BondCurveRenderPoint(
        tenor=point.tenor,
        tenor_years=curve_point.tenor_years,
        maturity_date=curve_point.maturity_date,
        par_yield=point.par_yield,
        zero_rate=curve_point.zero_rate,
        discount_factor=curve_point.discount_factor,
        ctd_symbol=point.bond.symbol,
        futures_symbol=point.future.futures_symbol,
    )


def _add_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year + years)
    except ValueError:
        return value.replace(month=2, day=28, year=value.year + years)
