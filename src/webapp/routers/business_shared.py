"""Shared models, helpers, and presets for the business domain routers."""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal

from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field, SerializeAsAny, field_validator, model_validator

from src.config.reference_data import resolve_index
from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest
from src.feeds.exchange_resolver import resolve_equity


class BusinessCacheControls(BaseModel):
    """Shared TTL-cache controls for business read endpoints."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    use_ttl_cache: bool = Field(default=True, description="Serve the response from the shared TTL cache when possible.")
    cache_ttl_seconds: float | None = Field(default=300, ge=0, description="Cache entry lifetime in seconds; null uses the cache default.")


class BusinessDateRangeControls(BusinessCacheControls):
    """Shared UTC time-window controls for business read endpoints."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    start_datetime: datetime | None = Field(default=None, description="Window start (ISO 8601). Naive values are treated as UTC; values are normalized to UTC.")
    end_datetime: datetime | None = Field(default=None, description="Window end (ISO 8601). Naive values are treated as UTC; values are normalized to UTC. Must follow start_datetime when both are set.")

    @field_validator("start_datetime", "end_datetime", mode="before")
    @classmethod
    def normalize_datetime_utc(cls, value: object) -> datetime | None:
        if value is None or value == "":
            return None
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime):
            raise TypeError("datetime fields must be datetimes")
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def validate_time_range(self) -> "BusinessDateRangeControls":
        if self.start_datetime and self.end_datetime and self.start_datetime >= self.end_datetime:
            raise ValueError("start_datetime must be before end_datetime")
        return self


class BusinessOHLCVSymbol(BaseModel):
    """One instrument in a business OHLCV request, as a string or an object."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1, description="Instrument symbol in the notation of the request's asset class, e.g. AAPL (equity), EUR.USD (fx), 0700.HK (HK equity).")
    exchange: str | None = Field(default=None, min_length=1, description="Exchange override, e.g. SMART, NASDAQ, IDEALPRO; defaults are resolved per asset class.")
    currency: str | None = Field(default=None, min_length=1, description="ISO 4217 currency override, e.g. USD, HKD.")
    primary_exchange: str | None = Field(default=None, min_length=1, description="Primary listing exchange for SMART-routed equities, e.g. NASDAQ.")
    last_trade_date_or_contract_month: str | None = Field(default=None, min_length=1, description="Futures contract month as YYYYMM (e.g. 202606) or option/future expiry.")
    multiplier: str | None = Field(default=None, min_length=1, description="Contract multiplier, e.g. 1000 for CL.")
    local_symbol: str | None = Field(default=None, min_length=1, description="IBKR local symbol for an exact contract, e.g. ESZ6.")
    con_id: int | None = Field(default=None, gt=0, description="IBKR contract id for an exact contract, e.g. 76792991.")
    sec_id_type: str | None = Field(default=None, min_length=1, description="Security identifier type (e.g. ISIN) when identifying by sec_id.")
    sec_id: str | None = Field(default=None, min_length=1, description="Security identifier value matching sec_id_type.")


class MarketPanelRequest(BusinessDateRangeControls):
    """Request body for the market panel: latest bars for several symbols."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbols: list[str | BusinessOHLCVSymbol] = Field(min_length=1, description="Instruments to load: plain symbol strings or objects with contract qualifiers.")
    asset_class: AssetClass = Field(default=AssetClass.EQUITY, description="Asset class applied to plain symbol strings: equity, fx, future, bond, index, crypto, option.")
    exchange: str | None = Field(default=None, min_length=1, description="Default exchange for symbols that do not carry one.")
    currency: str | None = Field(default=None, min_length=1, description="Default ISO 4217 currency for symbols that do not carry one.")
    duration: str = Field(default="1 D", min_length=1, description="IBKR duration string for the lookback, e.g. '1 D', '1 W', '1 M', '1 Y'.")
    bar_size: str = Field(default="1 min", min_length=1, description="IBKR bar size with its exact spacing, e.g. '1 min', '5 mins', '1 hour', '1 day'.")
    what_to_show: str = Field(default="TRADES", min_length=1, description="IBKR whatToShow value, e.g. TRADES, MIDPOINT, BID, ASK, YIELD_LAST, OPTION_IMPLIED_VOLATILITY.")
    use_rth: bool = Field(default=True, description="Restrict bars to regular trading hours.")
    cache_latest: bool = Field(default=False, description="Store the newest bar of each symbol in the latest-bar cache.")
    max_concurrent_requests: int = Field(default=4, ge=1, le=20, description="Upper bound on concurrent IBKR history requests.")


class UniverseBarsRequest(BusinessDateRangeControls):
    """Request body for universe bars: bars for every member of a universe."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    universe: str = Field(min_length=1, description="Universe key whose Redis index composition supplies the symbols.")
    symbols: list[str] | None = Field(
        default=None,
        description="Optional explicit symbols. When omitted, the endpoint reads Redis index composition by universe.",
    )
    asset_class: AssetClass = Field(default=AssetClass.EQUITY, description="Asset class applied to the symbols: equity, fx, future, bond, index, crypto, option.")
    exchange: str | None = Field(default=None, min_length=1, description="Default exchange for symbols that do not carry one.")
    currency: str | None = Field(default=None, min_length=1, description="Default ISO 4217 currency for symbols that do not carry one.")
    duration: str = Field(default="1 D", min_length=1, description="IBKR duration string for the lookback, e.g. '1 D', '1 W', '1 M', '1 Y'.")
    bar_size: str = Field(default="1 min", min_length=1, description="IBKR bar size with its exact spacing, e.g. '1 min', '5 mins', '1 hour', '1 day'.")
    what_to_show: str = Field(default="TRADES", min_length=1, description="IBKR whatToShow value, e.g. TRADES, MIDPOINT, BID, ASK, YIELD_LAST, OPTION_IMPLIED_VOLATILITY.")
    use_rth: bool = Field(default=True, description="Restrict bars to regular trading hours.")
    cache_latest: bool = Field(default=False, description="Store the newest bar of each symbol in the latest-bar cache.")
    max_symbols: int = Field(default=100, ge=1, le=500, description="Upper bound on symbols loaded from the universe.")
    max_concurrent_requests: int = Field(default=4, ge=1, le=20, description="Upper bound on concurrent IBKR history requests.")


_COMMODITY_FUTURES_PRESETS: dict[str, tuple[str, str]] = {
    "CL": ("NYMEX", "USD"),
    "NG": ("NYMEX", "USD"),
    "GC": ("COMEX", "USD"),
    "SI": ("COMEX", "USD"),
    "HG": ("COMEX", "USD"),
    "ZC": ("CBOT", "USD"),
    "ZS": ("CBOT", "USD"),
    "ZW": ("CBOT", "USD"),
    "ZL": ("CBOT", "USD"),
    "ZM": ("CBOT", "USD"),
}

_COMMODITY_FUTURES_MONTHS: dict[str, tuple[int, ...]] = {
    "CL": tuple(range(1, 13)),
    "NG": tuple(range(1, 13)),
    "GC": (2, 4, 6, 8, 10, 12),
    "SI": (3, 5, 7, 9, 12),
    "HG": (3, 5, 7, 9, 12),
    "ZC": (3, 5, 7, 9, 12),
    "ZS": (3, 5, 7, 8, 9, 11),
    "ZW": (3, 5, 7, 9, 12),
    "ZL": (1, 3, 5, 7, 8, 9, 10, 12),
    "ZM": (1, 3, 5, 7, 8, 9, 10, 12),
}

_COMMODITY_EXPIRY_RULES: dict[str, str] = {
    "CL": "nymex_crude_oil",
}


def resolve_commodity_market(symbol: str, exchange: str | None, currency: str | None) -> tuple[str, str]:
    preset_exchange, preset_currency = _COMMODITY_FUTURES_PRESETS.get(symbol.strip().upper(), ("NYMEX", "USD"))
    return exchange or preset_exchange, currency or preset_currency


def commodity_contract_months(symbol: str, as_of_date: date, count: int) -> tuple[str, ...]:
    root = symbol.strip().upper()
    listed_months = _COMMODITY_FUTURES_MONTHS.get(root, tuple(range(1, 13)))
    months: list[str] = []
    year = as_of_date.year
    while len(months) < count:
        for listed_month in listed_months:
            contract_month = f"{year}{listed_month:02d}"
            if not _commodity_contract_available_on(root, contract_month, as_of_date):
                continue
            months.append(contract_month)
            if len(months) == count:
                break
        year += 1
    return tuple(months)


def _commodity_contract_available_on(symbol: str, contract_month: str, as_of_date: date) -> bool:
    rule = _COMMODITY_EXPIRY_RULES.get(symbol.strip().upper())
    if rule == "nymex_crude_oil":
        expiry = nymex_crude_oil_last_trade_date(contract_month)
        return expiry is not None and as_of_date <= expiry
    return contract_month >= f"{as_of_date.year}{as_of_date.month:02d}"


def nymex_crude_oil_last_trade_date(contract_month: str) -> date | None:
    try:
        year = int(contract_month[:4])
        month = int(contract_month[4:6])
    except (TypeError, ValueError):
        return None
    if month == 1:
        preceding_month = 12
        preceding_year = year - 1
    else:
        preceding_month = month - 1
        preceding_year = year

    twenty_fifth = date(preceding_year, preceding_month, 25)
    reference_day = _previous_nymex_business_day(twenty_fifth) if not _is_nymex_business_day(twenty_fifth) else twenty_fifth
    return _subtract_nymex_business_days(reference_day, 3)


def _subtract_nymex_business_days(value: date, count: int) -> date:
    current = value
    remaining = count
    while remaining > 0:
        current -= timedelta(days=1)
        if _is_nymex_business_day(current):
            remaining -= 1
    return current


def _previous_nymex_business_day(value: date) -> date:
    current = value
    while not _is_nymex_business_day(current):
        current -= timedelta(days=1)
    return current


def _is_nymex_business_day(value: date) -> bool:
    return value.weekday() < 5 and value not in _us_market_holidays(value.year)


def _us_market_holidays(year: int) -> set[date]:
    return {
        _observed_fixed_holiday(year, 1, 1),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _good_friday(year),
        _last_weekday(year, 5, 0),
        _observed_fixed_holiday(year, 6, 19),
        _observed_fixed_holiday(year, 7, 4),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed_fixed_holiday(year, 12, 25),
    }


def _observed_fixed_holiday(year: int, month: int, day: int) -> date:
    holiday = date(year, month, day)
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    current = date(year, month, 1)
    offset = (weekday - current.weekday()) % 7
    return current + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    current = date(year + int(month == 12), 1 if month == 12 else month + 1, 1) - timedelta(days=1)
    while current.weekday() != weekday:
        current -= timedelta(days=1)
    return current


def _good_friday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    ell = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ell) // 451
    easter_month = (h + ell - 7 * m + 114) // 31
    easter_day = ((h + ell - 7 * m + 114) % 31) + 1
    return date(year, easter_month, easter_day) - timedelta(days=2)


def resolve_business_symbol(
    *,
    symbol: str,
    asset_class: AssetClass,
    exchange: str | None = None,
    currency: str | None = None,
    primary_exchange: str | None = None,
) -> BusinessOHLCVSymbol:
    if asset_class is AssetClass.EQUITY:
        resolved = resolve_equity(symbol)
        return BusinessOHLCVSymbol(
            symbol=resolved.symbol,
            exchange=exchange or resolved.exchange,
            currency=currency or resolved.currency,
            primary_exchange=primary_exchange or resolved.primary_exchange or None,
        )
    if asset_class is AssetClass.FX:
        return BusinessOHLCVSymbol(
            symbol=symbol.strip().upper(),
            exchange=exchange or "IDEALPRO",
            currency=currency or symbol.strip().upper()[3:6] or "USD",
            primary_exchange=primary_exchange,
        )
    if asset_class is AssetClass.INDEX:
        resolved = resolve_index(symbol)
        return BusinessOHLCVSymbol(
            symbol=resolved["symbol"],
            exchange=exchange or resolved["exchange"],
            currency=currency or resolved["currency"],
            primary_exchange=primary_exchange,
        )
    return BusinessOHLCVSymbol(
        symbol=symbol.strip().upper(),
        exchange=exchange or "SMART",
        currency=currency or "USD",
        primary_exchange=primary_exchange,
    )


def symbol_to_ohlcv_request(item: str | BusinessOHLCVSymbol, payload: MarketPanelRequest) -> OHLCVRequest:
    symbol = BusinessOHLCVSymbol(symbol=item) if isinstance(item, str) else item
    resolved = resolve_business_symbol(
        symbol=symbol.symbol,
        asset_class=payload.asset_class,
        exchange=symbol.exchange or payload.exchange,
        currency=symbol.currency or payload.currency,
        primary_exchange=symbol.primary_exchange,
    )
    return OHLCVRequest(
        symbol=resolved.symbol,
        asset_class=payload.asset_class,
        exchange=resolved.exchange,
        currency=resolved.currency,
        primary_exchange=resolved.primary_exchange,
        duration=payload.duration,
        bar_size=payload.bar_size,
        start_datetime=payload.start_datetime,
        end_datetime=payload.end_datetime,
        what_to_show=payload.what_to_show,
        use_rth=payload.use_rth,
        last_trade_date_or_contract_month=symbol.last_trade_date_or_contract_month,
        multiplier=symbol.multiplier,
        local_symbol=symbol.local_symbol,
        con_id=symbol.con_id,
        sec_id_type=symbol.sec_id_type,
        sec_id=symbol.sec_id,
    )


class InstrumentBarsResult(BaseModel):
    """Per-instrument outcome of a multi-symbol bar load."""

    model_config = ConfigDict(extra="forbid")

    input_index: int = Field(description="Zero-based position of the instrument in the request.")
    requested_symbol: str = Field(description="Symbol as the caller wrote it (string form or object's symbol).")
    request: OHLCVRequest = Field(description="The resolved OHLCV request sent to IBKR.")
    status: Literal["success", "empty", "error"] = Field(description="success = bars returned; empty = IBKR returned none; error = load failed.")
    bars: list[SerializeAsAny[OHLCVBar]] = Field(default_factory=list, description="Bars for this instrument; empty on error or empty result.")
    error: str | None = Field(default=None, description="Error text when status is error; null otherwise.")


class DetailedBarsResponse(BaseModel):
    """Envelope for multi-instrument bar loads with per-instrument status."""

    model_config = ConfigDict(extra="forbid")

    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="UTC time the response was assembled.")
    results: list[InstrumentBarsResult] = Field(description="One entry per requested instrument, in request order.")

    def flattened_bars(self) -> list[OHLCVBar]:
        return sorted([bar for result in self.results for bar in result.bars], key=lambda bar: (bar.symbol, bar.timestamp))

    def cacheable(self) -> bool:
        return all(result.status != "error" for result in self.results)


async def load_many_ohlcv_detailed(
    requests: list[OHLCVRequest],
    payload: MarketPanelRequest,
    state: Any,
) -> DetailedBarsResponse:
    import asyncio

    semaphore = asyncio.Semaphore(payload.max_concurrent_requests)

    async def load_one(request: OHLCVRequest) -> list[OHLCVBar]:
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
                return bars
            return await state.loader.load(request, persist=False, cache_latest=payload.cache_latest)

    raw_batches = await asyncio.gather(*(load_one(request) for request in requests), return_exceptions=True)
    results: list[InstrumentBarsResult] = []
    for index, (request, raw) in enumerate(zip(requests, raw_batches, strict=True)):
        item = payload.symbols[index]
        requested_symbol = item if isinstance(item, str) else item.symbol
        if isinstance(raw, BaseException) and not isinstance(raw, Exception):
            raise raw
        if isinstance(raw, Exception):
            logging.getLogger("business_shared").warning("Failed to load OHLCV for %s: %s", request.symbol, raw)
        results.append(InstrumentBarsResult(
            input_index=index, requested_symbol=requested_symbol, request=request,
            status="error" if isinstance(raw, Exception) else "success" if raw else "empty",
            bars=[] if isinstance(raw, Exception) else raw,
            error=(str(raw) or type(raw).__name__) if isinstance(raw, Exception) else None,
        ))
    return DetailedBarsResponse(results=results)


async def load_many_ohlcv(requests: list[OHLCVRequest], payload: MarketPanelRequest, state: Any) -> list[OHLCVBar]:
    """Compatibility projection of the shared per-instrument loader."""
    return (await load_many_ohlcv_detailed(requests, payload, state)).flattened_bars()
