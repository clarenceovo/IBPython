from __future__ import annotations

import math
from datetime import date, datetime, time, timezone
from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator, model_validator

from src.feeds.contracts import ContractSpec
from src.feeds.models import AssetClass, OHLCVRequest


class SovereignBondMarket(StrEnum):
    US_TREASURY = "US_TREASURY"
    JGB = "JGB"
    KTB = "KTB"
    GERMAN_BUND = "GERMAN_BUND"


class BondYieldField(StrEnum):
    BID = "YIELD_BID"
    ASK = "YIELD_ASK"
    BID_ASK = "YIELD_BID_ASK"
    LAST = "YIELD_LAST"


class YieldUnit(StrEnum):
    PERCENT = "percent"
    DECIMAL = "decimal"


class BondInstrument(BaseModel):
    """Vendor-neutral bond identifier with enough fields to qualify in IBKR when available."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(default="", description="IBKR/TWS symbol when known")
    market: SovereignBondMarket | str | None = None
    country: str = Field(default="")
    issuer: str = Field(default="")
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    isin: str | None = None
    cusip: str | None = None
    con_id: int | None = Field(default=None, gt=0)
    maturity_date: date | None = None
    coupon_rate: float | None = Field(default=None, ge=0)
    coupon_frequency: int = Field(default=2, ge=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol", "country", "issuer", "exchange", "currency", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip().upper()

    @field_validator("isin", "cusip", mode="before")
    @classmethod
    def normalize_optional_identifier(cls, value: Any) -> str | None:
        if value is None or value == "":
            return None
        return str(value).strip().upper()

    @model_validator(mode="after")
    def validate_identifier(self) -> Self:
        if not (self.symbol or self.isin or self.cusip or self.con_id):
            raise ValueError("bond instrument requires symbol, ISIN, CUSIP, or IBKR con_id")
        return self

    def to_contract_spec(self) -> ContractSpec:
        sec_id_type: str | None = None
        sec_id: str | None = None
        if self.isin:
            sec_id_type = "ISIN"
            sec_id = self.isin
        elif self.cusip:
            sec_id_type = "CUSIP"
            sec_id = self.cusip

        return ContractSpec(
            symbol=self.symbol or sec_id or str(self.con_id),
            asset_class=AssetClass.BOND,
            exchange=self.exchange,
            currency=self.currency,
            con_id=self.con_id,
            sec_id_type=sec_id_type,
            sec_id=sec_id,
            metadata=self.metadata,
        )


class BondYieldQuote(BaseModel):
    """Latest bond yield quote. IBKR yield values are kept in provider units by default."""

    model_config = ConfigDict(extra="forbid")

    bond: BondInstrument
    timestamp: datetime
    bid_yield: float | None = None
    ask_yield: float | None = None
    last_yield: float | None = None
    yield_unit: YieldUnit = YieldUnit.PERCENT
    source: str = Field(default="ibkr", min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("timestamp", mode="before")
    @classmethod
    def normalize_timestamp(cls, value: Any) -> datetime:
        return normalize_utc_datetime(value)

    @model_validator(mode="after")
    def validate_has_yield(self) -> Self:
        if self.bid_yield is None and self.ask_yield is None and self.last_yield is None:
            raise ValueError("at least one of bid_yield, ask_yield, or last_yield is required")
        return self

    @computed_field
    @property
    def mid_yield(self) -> float | None:
        if self.bid_yield is None or self.ask_yield is None:
            return None
        return (self.bid_yield + self.ask_yield) / 2

    def yield_as_decimal(self, value: float | None) -> float | None:
        if value is None:
            return None
        return value / 100 if self.yield_unit is YieldUnit.PERCENT else value


class BondYieldBar(BaseModel):
    """Historical yield OHLC bar for one IBKR yield field."""

    model_config = ConfigDict(extra="forbid")

    bond: BondInstrument
    timestamp: datetime
    yield_field: BondYieldField
    open: float
    high: float
    low: float
    close: float
    bar_size: str
    yield_unit: YieldUnit = YieldUnit.PERCENT
    source: str = Field(default="ibkr", min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("timestamp", mode="before")
    @classmethod
    def normalize_timestamp(cls, value: Any) -> datetime:
        return normalize_utc_datetime(value)

    @model_validator(mode="after")
    def validate_yield_range(self) -> Self:
        if self.high < self.low:
            raise ValueError("high yield must be greater than or equal to low yield")
        return self


class BondYieldHistoryRequest(BaseModel):
    """IBKR historical bond yield request."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    bond: BondInstrument
    end_datetime: datetime | None = None
    duration: str = Field(default="1 D", min_length=1)
    bar_size: str = Field(default="1 day", min_length=1)
    yield_fields: tuple[BondYieldField, ...] = (BondYieldField.BID, BondYieldField.ASK, BondYieldField.LAST)
    use_rth: bool = True
    source: str = Field(default="ibkr", min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("end_datetime", mode="before")
    @classmethod
    def normalize_end_datetime(cls, value: Any) -> datetime | None:
        if value is None or value == "":
            return None
        return normalize_utc_datetime(value)

    @field_validator("yield_fields", mode="before")
    @classmethod
    def normalize_yield_fields(cls, value: Any) -> tuple[BondYieldField, ...]:
        if not isinstance(value, (list, tuple, set)):
            raise TypeError("yield_fields must be a sequence")
        normalized = tuple(BondYieldField(item) for item in value)
        if not normalized:
            raise ValueError("yield_fields cannot be empty")
        return normalized

    def to_pacing_request(self, yield_field: BondYieldField) -> OHLCVRequest:
        return OHLCVRequest(
            symbol=self.bond.symbol or self.bond.isin or self.bond.cusip or str(self.bond.con_id),
            asset_class=AssetClass.BOND,
            exchange=self.bond.exchange,
            currency=self.bond.currency,
            end_datetime=self.end_datetime,
            duration=self.duration,
            bar_size=self.bar_size,
            what_to_show=yield_field.value,
            use_rth=self.use_rth,
            source=self.source,
            metadata={**self.metadata, **self.bond.metadata},
        )


class CTDFutureDefinition(BaseModel):
    """Futures contract family whose delivery basket should be evaluated for CTD."""

    model_config = ConfigDict(extra="forbid")

    market: SovereignBondMarket
    futures_symbol: str
    exchange: str
    currency: str
    description: str
    metadata: dict[str, Any] = Field(default_factory=dict)


DEFAULT_CTD_FUTURE_DEFINITIONS: tuple[CTDFutureDefinition, ...] = (
    CTDFutureDefinition(
        market=SovereignBondMarket.US_TREASURY,
        futures_symbol="ZN",
        exchange="CBOT",
        currency="USD",
        description="US 10-Year Treasury Note futures delivery basket",
    ),
    CTDFutureDefinition(
        market=SovereignBondMarket.JGB,
        futures_symbol="JGB",
        exchange="OSE.JPN",
        currency="JPY",
        description="Japanese Government Bond futures delivery basket",
    ),
    CTDFutureDefinition(
        market=SovereignBondMarket.KTB,
        futures_symbol="KTB",
        exchange="KRX",
        currency="KRW",
        description="Korean Treasury Bond futures delivery basket",
    ),
    CTDFutureDefinition(
        market=SovereignBondMarket.GERMAN_BUND,
        futures_symbol="GBL",
        exchange="EUREX",
        currency="EUR",
        description="German Bund futures delivery basket",
    ),
)


class CTDBondCandidate(BaseModel):
    """One deliverable bond candidate for CTD selection."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    future: CTDFutureDefinition
    bond: BondInstrument
    futures_price: float = Field(gt=0)
    clean_price: float = Field(gt=0)
    conversion_factor: float = Field(gt=0)
    accrued_interest: float = 0
    carry: float = 0
    delivery_date: date | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @computed_field
    @property
    def invoice_price(self) -> float:
        return self.futures_price * self.conversion_factor + self.accrued_interest

    @computed_field
    @property
    def gross_basis(self) -> float:
        return self.clean_price - self.invoice_price

    @computed_field
    @property
    def net_basis(self) -> float:
        return self.gross_basis - self.carry


class CTDBondSnapshot(BaseModel):
    """CTD candidate snapshot for one futures delivery basket."""

    model_config = ConfigDict(extra="forbid")

    market: SovereignBondMarket
    futures_symbol: str
    timestamp: datetime
    candidates: tuple[CTDBondCandidate, ...]
    source: str = Field(default="external_ctd_provider", min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("timestamp", mode="before")
    @classmethod
    def normalize_timestamp(cls, value: Any) -> datetime:
        return normalize_utc_datetime(value)

    @model_validator(mode="after")
    def validate_candidates(self) -> Self:
        if not self.candidates:
            raise ValueError("CTD snapshot requires at least one candidate")
        return self

    def selected_ctd(self) -> CTDBondCandidate:
        return min(self.candidates, key=lambda candidate: candidate.net_basis)


class YieldCurveBootstrapInstrument(BaseModel):
    """Par-yield instrument used to bootstrap a discount curve."""

    model_config = ConfigDict(extra="forbid")

    instrument_id: str = Field(min_length=1)
    maturity_date: date
    par_yield: float = Field(gt=-1, lt=1, description="Decimal par yield, e.g. 0.045 for 4.5%")
    coupon_frequency: int = Field(default=2, ge=1)
    metadata: dict[str, Any] = Field(default_factory=dict)


class YieldCurvePoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    maturity_date: date
    tenor_years: float = Field(gt=0)
    discount_factor: float = Field(gt=0)
    zero_rate: float = Field(description="Continuously compounded decimal zero rate")
    source_instrument_id: str


class YieldCurveDTO(BaseModel):
    """Yield curve DTO with a deterministic par-yield bootstrap method."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    curve_id: str = Field(min_length=1)
    currency: str = Field(min_length=1)
    valuation_date: date
    input_instruments: tuple[YieldCurveBootstrapInstrument, ...]
    points: tuple[YieldCurvePoint, ...] = Field(default_factory=tuple)
    source: str = Field(default="internal_bootstrap", min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("currency", mode="before")
    @classmethod
    def normalize_currency(cls, value: Any) -> str:
        if value is None:
            raise ValueError("currency is required")
        return str(value).strip().upper()

    @model_validator(mode="after")
    def validate_inputs(self) -> Self:
        if not self.input_instruments:
            raise ValueError("yield curve requires at least one bootstrap instrument")
        maturities = [instrument.maturity_date for instrument in self.input_instruments]
        if any(maturity <= self.valuation_date for maturity in maturities):
            raise ValueError("all bootstrap instruments must mature after valuation_date")
        if len(set(maturities)) != len(maturities):
            raise ValueError("bootstrap instruments must have unique maturity dates")
        return self

    def bootstrap(self) -> "YieldCurveDTO":
        bootstrapped_points: list[YieldCurvePoint] = []
        sorted_instruments = sorted(self.input_instruments, key=lambda item: item.maturity_date)
        known: list[tuple[float, float]] = [(0.0, 1.0)]

        for instrument in sorted_instruments:
            maturity_years = year_fraction(self.valuation_date, instrument.maturity_date)
            cashflow_times = _regular_cashflow_times(maturity_years, instrument.coupon_frequency)
            coupon = instrument.par_yield / instrument.coupon_frequency

            def price_for_maturity_df(maturity_df: float) -> float:
                curve = [*known, (maturity_years, maturity_df)]
                price = 0.0
                for payment_time in cashflow_times:
                    payment = coupon
                    if math.isclose(payment_time, maturity_years, rel_tol=0, abs_tol=1e-10):
                        payment += 1.0
                    price += payment * _log_linear_discount(payment_time, curve)
                return price

            maturity_df = _solve_discount_factor(price_for_maturity_df)
            zero_rate = -math.log(maturity_df) / maturity_years
            known.append((maturity_years, maturity_df))
            known.sort(key=lambda item: item[0])
            bootstrapped_points.append(
                YieldCurvePoint(
                    maturity_date=instrument.maturity_date,
                    tenor_years=maturity_years,
                    discount_factor=maturity_df,
                    zero_rate=zero_rate,
                    source_instrument_id=instrument.instrument_id,
                )
            )

        return self.model_copy(update={"points": tuple(bootstrapped_points)})

    def bootscrape(self) -> "YieldCurveDTO":
        """Backward-compatible alias for the requested 'bootscrape' wording."""

        return self.bootstrap()


def normalize_utc_datetime(value: Any) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, time.min)
    if not isinstance(value, datetime):
        raise TypeError("value must be a datetime")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def year_fraction(start: date, end: date) -> float:
    return (end - start).days / 365.0


def _regular_cashflow_times(maturity_years: float, coupon_frequency: int) -> list[float]:
    step = 1 / coupon_frequency
    times: list[float] = []
    current = step
    while current < maturity_years - 1e-10:
        times.append(current)
        current += step
    times.append(maturity_years)
    return times


def _solve_discount_factor(price_fn: Any) -> float:
    low = 1e-8
    high = 2.0
    while price_fn(high) < 1.0:
        high *= 2
        if high > 100:
            raise ValueError("could not bracket discount factor during bootstrap")

    for _ in range(100):
        mid = (low + high) / 2
        if price_fn(mid) >= 1.0:
            high = mid
        else:
            low = mid
    return (low + high) / 2


def _log_linear_discount(target_time: float, curve: list[tuple[float, float]]) -> float:
    curve = sorted(curve, key=lambda item: item[0])
    for time_value, discount_factor in curve:
        if math.isclose(target_time, time_value, rel_tol=0, abs_tol=1e-10):
            return discount_factor

    previous = curve[0]
    for current in curve[1:]:
        if previous[0] <= target_time <= current[0]:
            left_t, left_df = previous
            right_t, right_df = current
            weight = (target_time - left_t) / (right_t - left_t)
            log_df = math.log(left_df) + weight * (math.log(right_df) - math.log(left_df))
            return math.exp(log_df)
        previous = current
    raise ValueError(f"cannot interpolate discount factor for t={target_time}")


def normalize_ibkr_bond_yield_bars(
    raw_bars: list[Any],
    request: BondYieldHistoryRequest,
    yield_field: BondYieldField,
) -> list[BondYieldBar]:
    return [
        BondYieldBar(
            bond=request.bond,
            timestamp=getattr(bar, "date"),
            yield_field=yield_field,
            open=float(getattr(bar, "open")),
            high=float(getattr(bar, "high")),
            low=float(getattr(bar, "low")),
            close=float(getattr(bar, "close")),
            bar_size=request.bar_size,
            source=request.source,
            metadata=request.metadata,
        )
        for bar in raw_bars
    ]
