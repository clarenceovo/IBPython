from __future__ import annotations

import math
from datetime import date, datetime, timezone
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.feeds.bond_curve import BondCurveRequest, BondCurveResponse, build_standard_bond_curve, resolve_bond_curve_market
from src.feeds.bonds import (
    BondInstrument,
    CTDBondCandidate,
    CTDBondSnapshot,
    CTDFutureDefinition,
    SovereignBondMarket,
    YieldCurveBootstrapInstrument,
    YieldCurveDTO,
    year_fraction,
)
from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest


class FixedIncomeCurveMode(StrEnum):
    CASH_BOND = "cash_bond"
    FUTURES_IMPLIED = "futures_implied"
    INDICATIVE_PLACEHOLDER = "indicative_placeholder"


class FixedIncomeMarketConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    market: SovereignBondMarket
    currency: str
    futures_exchange: str
    default_futures: tuple[str, ...]


class BondFutureContractSpec(BaseModel):
    """Minimal business identifier for a sovereign bond future."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    market: str = Field(min_length=1)
    futures_symbol: str | None = Field(default=None, min_length=1)
    exchange: str | None = Field(default=None, min_length=1)
    currency: str | None = Field(default=None, min_length=1)
    contract_month: str | None = Field(default=None, min_length=1)
    local_symbol: str | None = Field(default=None, min_length=1)
    con_id: int | None = Field(default=None, gt=0)
    multiplier: str | None = Field(default=None, min_length=1)

    @field_validator("market", "futures_symbol", "exchange", "currency", "contract_month", "local_symbol", "multiplier", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> str | None:
        if value is None or value == "":
            return None
        return str(value).strip().upper()

    @model_validator(mode="after")
    def validate_contract_identifier(self) -> "BondFutureContractSpec":
        if not (self.contract_month or self.local_symbol or self.con_id):
            raise ValueError("bond future requires contract_month, local_symbol, or con_id")
        return self

    def resolved_definition(self) -> CTDFutureDefinition:
        config = fixed_income_market_config(self.market)
        futures_symbol = self.futures_symbol or config.default_futures[0]
        return CTDFutureDefinition(
            market=config.market,
            futures_symbol=futures_symbol,
            exchange=self.exchange or config.futures_exchange,
            currency=self.currency or config.currency,
            description=f"{config.market.value} {futures_symbol} bond future",
            metadata={
                "contract_month": self.contract_month,
                "local_symbol": self.local_symbol,
                "con_id": self.con_id,
            },
        )

    def to_ohlcv_request(
        self,
        *,
        duration: str,
        bar_size: str,
        what_to_show: str,
        use_rth: bool,
    ) -> OHLCVRequest:
        definition = self.resolved_definition()
        return OHLCVRequest(
            symbol=definition.futures_symbol,
            asset_class=AssetClass.FUTURE,
            exchange=definition.exchange,
            currency=definition.currency,
            duration=duration,
            bar_size=bar_size,
            what_to_show=what_to_show,
            use_rth=use_rth,
            last_trade_date_or_contract_month=self.contract_month,
            local_symbol=self.local_symbol,
            con_id=self.con_id,
            multiplier=self.multiplier,
            metadata={"market": definition.market.value, "fixed_income_future": True},
        )


class BondFutureQuote(BaseModel):
    model_config = ConfigDict(extra="forbid")

    market: SovereignBondMarket
    futures_symbol: str
    exchange: str
    currency: str
    contract_month: str | None = None
    timestamp: datetime
    price: float = Field(gt=0)
    bar: OHLCVBar
    source: str = "ibkr"


class DeliverableBondInput(BaseModel):
    """Provider-supplied row for one deliverable bond in a futures basket."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    bond: BondInstrument
    conversion_factor: float = Field(gt=0)
    clean_price: float = Field(gt=0)
    accrued_interest: float = 0.0
    carry: float = 0.0
    delivery_date: date | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DeliverableBasketRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    market: str = Field(min_length=1)
    futures_symbol: str = Field(min_length=1)
    contract_month: str | None = Field(default=None, min_length=1)
    valuation_date: date = Field(default_factory=lambda: datetime.now(timezone.utc).date())

    @field_validator("market", "futures_symbol", "contract_month", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> str | None:
        if value is None or value == "":
            return None
        return str(value).strip().upper()


class FixedIncomeCurveDiagnostics(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: FixedIncomeCurveMode
    provider: str
    warnings: tuple[str, ...] = ()
    inputs_used: int = 0
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class CTDAnalyticsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    market: SovereignBondMarket
    futures_symbol: str
    contract_month: str | None
    valuation_date: date
    futures_price: float
    snapshot: CTDBondSnapshot
    selected: CTDBondCandidate
    provider: str
    diagnostics: FixedIncomeCurveDiagnostics


class FuturesImpliedCurvePoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    futures_symbol: str
    contract_month: str | None
    selected_ctd: BondInstrument
    implied_clean_price: float = Field(gt=0)
    implied_yield: float = Field(description="Decimal yield to maturity solved from the futures-implied clean price.")
    maturity_date: date


class FuturesImpliedCurveResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    market: SovereignBondMarket
    currency: str
    valuation_date: date
    curve: YieldCurveDTO
    points: tuple[FuturesImpliedCurvePoint, ...]
    ctd_snapshots: tuple[CTDAnalyticsResponse, ...]
    diagnostics: FixedIncomeCurveDiagnostics


class CurveComparisonResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    market: SovereignBondMarket
    valuation_date: date
    cash_curve: BondCurveResponse
    futures_implied_curve: FuturesImpliedCurveResponse
    zero_rate_spreads: tuple[dict[str, Any], ...]


@runtime_checkable
class FixedIncomeReferenceProvider(Protocol):
    name: str

    async def get_deliverable_basket(self, request: DeliverableBasketRequest) -> tuple[DeliverableBondInput, ...]:
        """Return point-in-time deliverable basket rows for one futures contract."""


_MARKET_CONFIGS: dict[SovereignBondMarket, FixedIncomeMarketConfig] = {
    SovereignBondMarket.US_TREASURY: FixedIncomeMarketConfig(
        market=SovereignBondMarket.US_TREASURY,
        currency="USD",
        futures_exchange="CBOT",
        default_futures=("ZT", "ZF", "ZN", "ZB"),
    ),
    SovereignBondMarket.JGB: FixedIncomeMarketConfig(
        market=SovereignBondMarket.JGB,
        currency="JPY",
        futures_exchange="OSE.JPN",
        default_futures=("JGB",),
    ),
    SovereignBondMarket.KTB: FixedIncomeMarketConfig(
        market=SovereignBondMarket.KTB,
        currency="KRW",
        futures_exchange="KRX",
        default_futures=("KTB3", "KTB5", "KTB", "KTB30"),
    ),
    SovereignBondMarket.GERMAN_BUND: FixedIncomeMarketConfig(
        market=SovereignBondMarket.GERMAN_BUND,
        currency="EUR",
        futures_exchange="EUREX",
        default_futures=("FGBS", "FGBM", "GBL", "FGBX"),
    ),
    SovereignBondMarket.UK_GILT: FixedIncomeMarketConfig(
        market=SovereignBondMarket.UK_GILT,
        currency="GBP",
        futures_exchange="ICEEU",
        default_futures=("G", "GL"),
    ),
}


def fixed_income_market_config(market_alias: str) -> FixedIncomeMarketConfig:
    return _MARKET_CONFIGS[resolve_bond_curve_market(market_alias)]


def default_bond_future_specs(market_alias: str, *, contract_month: str) -> tuple[BondFutureContractSpec, ...]:
    config = fixed_income_market_config(market_alias)
    return tuple(
        BondFutureContractSpec(
            market=config.market.value,
            futures_symbol=symbol,
            exchange=config.futures_exchange,
            currency=config.currency,
            contract_month=contract_month,
        )
        for symbol in config.default_futures
    )


def quote_from_latest_bar(spec: BondFutureContractSpec, bars: list[OHLCVBar]) -> BondFutureQuote:
    if not bars:
        raise ValueError(f"IBKR returned no bars for bond future {spec.futures_symbol or spec.local_symbol or spec.con_id}")
    latest = max(bars, key=lambda bar: bar.timestamp)
    definition = spec.resolved_definition()
    return BondFutureQuote(
        market=definition.market,
        futures_symbol=definition.futures_symbol,
        exchange=definition.exchange,
        currency=definition.currency,
        contract_month=spec.contract_month,
        timestamp=latest.timestamp,
        price=latest.close,
        bar=latest,
    )


def calculate_ctd_analytics(
    *,
    future: CTDFutureDefinition,
    contract_month: str | None,
    valuation_date: date,
    futures_price: float,
    basket: tuple[DeliverableBondInput, ...],
    provider_name: str,
) -> CTDAnalyticsResponse:
    if not basket:
        raise ValueError("deliverable basket cannot be empty")
    candidates = tuple(
        CTDBondCandidate(
            future=future,
            bond=item.bond,
            futures_price=futures_price,
            clean_price=item.clean_price,
            conversion_factor=item.conversion_factor,
            accrued_interest=item.accrued_interest,
            carry=item.carry,
            delivery_date=item.delivery_date,
            metadata=item.metadata,
        )
        for item in basket
    )
    snapshot = CTDBondSnapshot(
        market=future.market,
        futures_symbol=future.futures_symbol,
        timestamp=datetime.now(timezone.utc),
        candidates=candidates,
        source=provider_name,
        metadata={"contract_month": contract_month, "valuation_date": valuation_date.isoformat()},
    )
    selected = snapshot.selected_ctd()
    return CTDAnalyticsResponse(
        market=future.market,
        futures_symbol=future.futures_symbol,
        contract_month=contract_month,
        valuation_date=valuation_date,
        futures_price=futures_price,
        snapshot=snapshot,
        selected=selected,
        provider=provider_name,
        diagnostics=FixedIncomeCurveDiagnostics(
            mode=FixedIncomeCurveMode.FUTURES_IMPLIED,
            provider=provider_name,
            inputs_used=len(candidates),
        ),
    )


def futures_implied_clean_price(candidate: CTDBondCandidate) -> float:
    return candidate.futures_price * candidate.conversion_factor


def yield_to_maturity_from_clean_price(
    *,
    bond: BondInstrument,
    clean_price: float,
    valuation_date: date,
) -> float:
    if bond.maturity_date is None:
        raise ValueError(f"bond {bond.symbol} missing maturity_date")
    if bond.maturity_date <= valuation_date:
        raise ValueError(f"bond {bond.symbol} maturity_date must be after valuation_date")
    if bond.coupon_rate is None:
        raise ValueError(f"bond {bond.symbol} missing coupon_rate")
    dirty_price = clean_price / 100.0
    maturity_years = year_fraction(valuation_date, bond.maturity_date)
    coupon_frequency = bond.coupon_frequency
    coupon = bond.coupon_rate / coupon_frequency
    cashflow_times = _regular_cashflow_times(maturity_years, coupon_frequency)

    def price_at_yield(yield_value: float) -> float:
        price = 0.0
        for payment_time in cashflow_times:
            discount = (1.0 + yield_value / coupon_frequency) ** (-coupon_frequency * payment_time)
            payment = coupon
            if math.isclose(payment_time, maturity_years, rel_tol=0, abs_tol=1e-10):
                payment += 1.0
            price += payment * discount
        return price

    low, high = -0.95, 1.0
    for _ in range(100):
        mid = (low + high) / 2.0
        if price_at_yield(mid) > dirty_price:
            low = mid
        else:
            high = mid
    return (low + high) / 2.0


def build_futures_implied_curve(
    *,
    market: SovereignBondMarket,
    currency: str,
    valuation_date: date,
    ctd_results: tuple[CTDAnalyticsResponse, ...],
) -> FuturesImpliedCurveResponse:
    points: list[FuturesImpliedCurvePoint] = []
    instruments: list[YieldCurveBootstrapInstrument] = []
    warnings: list[str] = []
    for result in ctd_results:
        selected = result.selected
        implied_price = futures_implied_clean_price(selected)
        try:
            implied_yield = yield_to_maturity_from_clean_price(
                bond=selected.bond,
                clean_price=implied_price,
                valuation_date=valuation_date,
            )
        except ValueError as exc:
            warnings.append(str(exc))
            continue
        assert selected.bond.maturity_date is not None
        points.append(
            FuturesImpliedCurvePoint(
                futures_symbol=result.futures_symbol,
                contract_month=result.contract_month,
                selected_ctd=selected.bond,
                implied_clean_price=implied_price,
                implied_yield=implied_yield,
                maturity_date=selected.bond.maturity_date,
            )
        )
        instruments.append(
            YieldCurveBootstrapInstrument(
                instrument_id=f"{result.futures_symbol}_{selected.bond.symbol}",
                maturity_date=selected.bond.maturity_date,
                par_yield=implied_yield,
                coupon_frequency=selected.bond.coupon_frequency,
                metadata={
                    "market": market.value,
                    "futures_symbol": result.futures_symbol,
                    "ctd_symbol": selected.bond.symbol,
                    "curve_mode": FixedIncomeCurveMode.FUTURES_IMPLIED.value,
                },
            )
        )
    if not instruments:
        raise ValueError("no usable CTD instruments for futures-implied curve")
    curve = YieldCurveDTO(
        curve_id=f"{market.value}_FUTURES_IMPLIED_{valuation_date.isoformat()}",
        currency=currency,
        valuation_date=valuation_date,
        input_instruments=tuple(instruments),
        source="futures_implied_ctd_bootstrap",
        metadata={"market": market.value, "curve_mode": FixedIncomeCurveMode.FUTURES_IMPLIED.value},
    ).bootstrap()
    provider = ctd_results[0].provider if ctd_results else "unknown"
    return FuturesImpliedCurveResponse(
        market=market,
        currency=currency,
        valuation_date=valuation_date,
        curve=curve,
        points=tuple(points),
        ctd_snapshots=ctd_results,
        diagnostics=FixedIncomeCurveDiagnostics(
            mode=FixedIncomeCurveMode.FUTURES_IMPLIED,
            provider=provider,
            warnings=tuple(warnings),
            inputs_used=len(instruments),
        ),
    )


def build_cash_bond_curve(request: BondCurveRequest) -> BondCurveResponse:
    return build_standard_bond_curve(request)


def compare_curves(
    *,
    cash_curve: BondCurveResponse,
    futures_curve: FuturesImpliedCurveResponse,
) -> CurveComparisonResponse:
    spreads: list[dict[str, Any]] = []
    for cash_point, futures_point in zip(cash_curve.curve.points, futures_curve.curve.points, strict=False):
        spreads.append(
            {
                "cash_maturity_date": cash_point.maturity_date.isoformat(),
                "futures_maturity_date": futures_point.maturity_date.isoformat(),
                "cash_zero_rate": cash_point.zero_rate,
                "futures_zero_rate": futures_point.zero_rate,
                "spread": futures_point.zero_rate - cash_point.zero_rate,
            }
        )
    return CurveComparisonResponse(
        market=cash_curve.market,
        valuation_date=cash_curve.valuation_date,
        cash_curve=cash_curve,
        futures_implied_curve=futures_curve,
        zero_rate_spreads=tuple(spreads),
    )


def _regular_cashflow_times(maturity_years: float, coupon_frequency: int) -> list[float]:
    step = 1 / coupon_frequency
    raw_periods = maturity_years * coupon_frequency
    nearest_periods = round(raw_periods)
    if nearest_periods >= 1 and abs(raw_periods - nearest_periods) <= 0.05:
        return [period * step for period in range(1, nearest_periods)] + [maturity_years]

    times: list[float] = []
    current = step
    while current < maturity_years - 1e-10:
        times.append(current)
        current += step
    times.append(maturity_years)
    return times
