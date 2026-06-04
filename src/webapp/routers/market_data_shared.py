"""Shared models and helpers for market-data router modules."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

from fastapi import Depends, HTTPException
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator

from src.config.reference_data import is_known_index, resolve_future, resolve_index
from src.feeds.exchange_resolver import resolve_equity
from src.feeds.ibkr_historical import HistoricalRequestTooLargeError, ensure_historical_chunk_limit, plan_historical_auto_chunk
from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest
from src.transport.metrics import metrics
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

logger = logging.getLogger(__name__)


class HistoricalOHLCVLoadRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: OHLCVRequest
    persist: bool = False
    cache_latest: bool = True
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=None, ge=0)


class UnifiedOHLCVLoadRequest(BaseModel):
    """Integrated OHLCV request that resolves asset class and venue from compact inputs."""

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        json_schema_extra={
            "description": (
                "Compact OHLCV request that auto-resolves equity, FX, index, and futures contracts. "
                "Futures are selected when continuous, contract_month, local_symbol, or con_id is provided. "
                "continuous=true uses IBKR secType=CONTFUT for historical data only. "
                "Options are intentionally excluded because IBKR OPT/FOP contracts require expiry, strike, and right; "
                "use the option-specific endpoints for those requests."
            )
        },
    )

    starttime: datetime = Field(
        validation_alias=AliasChoices("starttime", "start_time", "start_datetime"),
        description="Inclusive start timestamp.",
        examples=["2026-06-01T01:15:00Z"],
    )
    endtime: datetime = Field(
        validation_alias=AliasChoices("endtime", "end_time", "end_datetime"),
        description="Exclusive end timestamp.",
        examples=["2026-06-01T08:00:00Z"],
    )
    interval: str = Field(
        validation_alias=AliasChoices("interval", "bar_size", "barSize"),
        min_length=1,
        description="Bar interval. Accepts IBKR values or compact forms such as 1m, 5m, 1h, 1d.",
        examples=["1m"],
    )
    symbol: str = Field(
        min_length=1,
        description=(
            "Ticker, index symbol, FX pair, or futures root. Examples: TSLA equity, HSI index or future, "
            "MHI mini-HSI future, LE CME live cattle future, VX CFE VIX future, EURUSD FX."
        ),
        examples=["TSLA", "HSI", "MHI", "LE", "VX", "EURUSD"],
    )
    contract_month: str | None = Field(
        default=None,
        validation_alias=AliasChoices("contract_month", "contractMonth", "last_trade_date_or_contract_month"),
        description=(
            "Optional futures contract month or last trade date. Use YYYYMM for a contract month or YYYYMMDD "
            "when IBKR needs an exact expiry. When present, the auto endpoint resolves the request as secType=FUT."
        ),
        examples=["202606"],
    )
    continuous: bool = Field(
        default=False,
        description=(
            "Request IBKR continuous futures historical bars using secType=CONTFUT. "
            "Use this instead of contract_month/local_symbol/con_id. It is historical-data only, not tradable."
        ),
    )
    asset_class: AssetClass | None = Field(
        default=None,
        description="Optional override when auto-detection is ambiguous. Supported values here: equity, fx, index, future.",
    )
    exchange: str | None = Field(default=None, min_length=1, description="Optional IBKR exchange override.")
    currency: str | None = Field(default=None, min_length=1, description="Optional IBKR currency override.")
    primary_exchange: str | None = Field(
        default=None,
        min_length=1,
        description="Optional primary exchange for ambiguous SMART-routed equities.",
    )
    multiplier: str | None = Field(
        default=None,
        min_length=1,
        description="Optional futures multiplier when IBKR needs it to disambiguate the contract.",
    )
    local_symbol: str | None = Field(
        default=None,
        min_length=1,
        description="Optional IBKR local symbol. For futures, this can be used instead of contract_month.",
        examples=["MHIJ6", "ESM6"],
    )
    con_id: int | None = Field(
        default=None,
        gt=0,
        description="Optional IBKR contract id. Use this to bypass symbol/exchange ambiguity.",
    )
    what_to_show: str | None = Field(
        default=None,
        min_length=1,
        description="Optional IBKR historical data type override. Defaults to TRADES except FX, which defaults to MIDPOINT.",
    )
    use_rth: bool | None = Field(
        default=None,
        description="Optional regular-trading-hours override. Defaults to false for futures/FX and true for equity/index.",
    )
    persist: bool = False
    cache_latest: bool = True
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=None, ge=0)
    metadata: dict[str, object] = Field(default_factory=dict)

    @field_validator("starttime", "endtime", mode="before")
    @classmethod
    def normalize_datetime_utc(cls, value: object) -> datetime:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime):
            raise TypeError("datetime fields must be datetimes")
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @field_validator("interval")
    @classmethod
    def normalize_interval(cls, value: str) -> str:
        return normalize_ohlcv_interval(value)

    @field_validator("symbol", "exchange", "currency", "primary_exchange", "contract_month", "multiplier", "local_symbol", mode="before")
    @classmethod
    def normalize_optional_upper_text(cls, value: object) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None

    @field_validator("what_to_show", mode="before")
    @classmethod
    def normalize_optional_upper_token(cls, value: object) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None

    @model_validator(mode="after")
    def validate_range(self) -> "UnifiedOHLCVLoadRequest":
        if self.starttime >= self.endtime:
            raise ValueError("starttime must be before endtime")
        if self.asset_class is not None and self.asset_class not in {
            AssetClass.EQUITY,
            AssetClass.FX,
            AssetClass.INDEX,
            AssetClass.FUTURE,
        }:
            raise ValueError("integrated OHLCV auto endpoint supports equity, fx, index, and future requests")
        if self.continuous and self.asset_class not in {None, AssetClass.FUTURE}:
            raise ValueError("continuous auto OHLCV requests are only supported for futures")
        if self.continuous and (self.contract_month or self.local_symbol or self.con_id):
            raise ValueError("continuous future auto OHLCV requests cannot include contract_month, local_symbol, or con_id")
        if self.resolved_asset_class is AssetClass.FUTURE and not (
            self.continuous or self.contract_month or self.local_symbol or self.con_id
        ):
            raise ValueError("future auto OHLCV requests require continuous=true, contract_month, local_symbol, or con_id")
        return self

    @property
    def resolved_asset_class(self) -> AssetClass:
        if self.asset_class is not None:
            return self.asset_class
        if self.continuous or self.contract_month or self.local_symbol:
            return AssetClass.FUTURE
        if _looks_like_fx_pair(self.symbol):
            return AssetClass.FX
        if is_known_index(self.symbol):
            return AssetClass.INDEX
        return AssetClass.EQUITY

    def to_request(self) -> OHLCVRequest:
        asset_class = self.resolved_asset_class
        if asset_class is AssetClass.FUTURE:
            resolved = resolve_future(self.symbol)
            return _unified_request(
                self,
                asset_class,
                symbol=resolved["symbol"],
                exchange=self.exchange or resolved["exchange"],
                currency=self.currency or resolved["currency"],
                what_to_show=self.what_to_show or "TRADES",
                use_rth=self.use_rth if self.use_rth is not None else False,
                last_trade_date_or_contract_month=self.contract_month,
                multiplier=self.multiplier,
                local_symbol=self.local_symbol,
                con_id=self.con_id,
                continuous=self.continuous,
            )
        if asset_class is AssetClass.FX:
            normalized_symbol = self.symbol.strip().upper()
            return _unified_request(
                self,
                asset_class,
                symbol=normalized_symbol,
                exchange=self.exchange or "IDEALPRO",
                currency=self.currency or normalized_symbol[3:6],
                what_to_show=self.what_to_show or "MIDPOINT",
                use_rth=self.use_rth if self.use_rth is not None else False,
            )
        if asset_class is AssetClass.INDEX:
            resolved = resolve_index(self.symbol)
            return _unified_request(
                self,
                asset_class,
                symbol=resolved["symbol"],
                exchange=self.exchange or resolved["exchange"],
                currency=self.currency or resolved["currency"],
                what_to_show=self.what_to_show or "TRADES",
                use_rth=self.use_rth if self.use_rth is not None else True,
                con_id=self.con_id,
            )
        resolved = resolve_equity(self.symbol)
        return _unified_request(
            self,
            AssetClass.EQUITY,
            symbol=resolved.symbol,
            exchange=self.exchange or resolved.exchange,
            currency=self.currency or resolved.currency,
            primary_exchange=self.primary_exchange or resolved.primary_exchange or None,
            what_to_show=self.what_to_show or "TRADES",
            use_rth=self.use_rth if self.use_rth is not None else True,
            con_id=self.con_id,
        )


class MinimalOHLCVLoadControls(BaseModel):
    """Common wrapper controls with production defaults."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    duration: str = Field(default="1 D", min_length=1)
    bar_size: str = Field(default="1 min", min_length=1)
    start_datetime: datetime | None = Field(
        default=None,
        description="Start of the date range (inclusive). When set with end_datetime, the wrapper paginates automatically.",
    )
    end_datetime: datetime | None = Field(
        default=None,
        description="End of the date range (inclusive). Defaults to now.",
    )
    what_to_show: str = Field(default="TRADES", min_length=1)
    use_rth: bool = True
    persist: bool = False
    cache_latest: bool = True
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=None, ge=0)
    metadata: dict[str, object] = Field(default_factory=dict)

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
    def validate_datetime_range(self) -> "MinimalOHLCVLoadControls":
        if self.start_datetime is not None and self.end_datetime is not None and self.start_datetime >= self.end_datetime:
            raise ValueError("start_datetime must be before end_datetime")
        return self

    def to_ohlcv_request(self, asset_class: AssetClass, **overrides: object) -> OHLCVRequest:
        metadata = overrides.pop("metadata", self.metadata)
        return OHLCVRequest(
            asset_class=asset_class,
            duration=self.duration,
            bar_size=self.bar_size,
            start_datetime=self.start_datetime,
            end_datetime=self.end_datetime,
            what_to_show=self.what_to_show,
            use_rth=self.use_rth,
            metadata=metadata,
            **overrides,
        )


_INTERVAL_ALIASES: dict[str, str] = {
    "1S": "1 sec",
    "1SEC": "1 sec",
    "1SECOND": "1 sec",
    "5S": "5 secs",
    "5SEC": "5 secs",
    "5SECS": "5 secs",
    "10S": "10 secs",
    "10SEC": "10 secs",
    "10SECS": "10 secs",
    "15S": "15 secs",
    "15SEC": "15 secs",
    "15SECS": "15 secs",
    "30S": "30 secs",
    "30SEC": "30 secs",
    "30SECS": "30 secs",
    "1M": "1 min",
    "1MIN": "1 min",
    "1MINUTE": "1 min",
    "2M": "2 mins",
    "2MIN": "2 mins",
    "2MINS": "2 mins",
    "3M": "3 mins",
    "3MIN": "3 mins",
    "3MINS": "3 mins",
    "5M": "5 mins",
    "5MIN": "5 mins",
    "5MINS": "5 mins",
    "10M": "10 mins",
    "10MIN": "10 mins",
    "10MINS": "10 mins",
    "15M": "15 mins",
    "15MIN": "15 mins",
    "15MINS": "15 mins",
    "20M": "20 mins",
    "20MIN": "20 mins",
    "20MINS": "20 mins",
    "30M": "30 mins",
    "30MIN": "30 mins",
    "30MINS": "30 mins",
    "1H": "1 hour",
    "1HR": "1 hour",
    "1HOUR": "1 hour",
    "2H": "2 hours",
    "2HR": "2 hours",
    "2HRS": "2 hours",
    "2HOURS": "2 hours",
    "3H": "3 hours",
    "3HR": "3 hours",
    "3HRS": "3 hours",
    "3HOURS": "3 hours",
    "4H": "4 hours",
    "4HR": "4 hours",
    "4HRS": "4 hours",
    "4HOURS": "4 hours",
    "8H": "8 hours",
    "8HR": "8 hours",
    "8HRS": "8 hours",
    "8HOURS": "8 hours",
    "1D": "1 day",
    "1DAY": "1 day",
    "1W": "1 week",
    "1WK": "1 week",
    "1WEEK": "1 week",
    "1MO": "1 month",
    "1MON": "1 month",
    "1MONTH": "1 month",
}

_CURRENCY_CODES: frozenset[str] = frozenset(
    {
        "AUD",
        "CAD",
        "CHF",
        "CNH",
        "CNY",
        "EUR",
        "GBP",
        "HKD",
        "JPY",
        "MXN",
        "NOK",
        "NZD",
        "SEK",
        "SGD",
        "USD",
        "ZAR",
    }
)


def normalize_ohlcv_interval(value: str) -> str:
    text = value.strip()
    if not text:
        raise ValueError("interval cannot be empty")
    canonical = re.sub(r"[\s_-]+", "", text).upper()
    if canonical in _INTERVAL_ALIASES:
        return _INTERVAL_ALIASES[canonical]
    normalized = " ".join(text.lower().split())
    allowed = set(_INTERVAL_ALIASES.values())
    if normalized in allowed:
        return normalized
    raise ValueError("interval must be an IBKR bar size or compact alias like 1m, 5m, 1h, or 1d")


def _looks_like_fx_pair(symbol: str) -> bool:
    normalized = symbol.strip().upper().replace("/", "")
    return len(normalized) == 6 and normalized[:3] in _CURRENCY_CODES and normalized[3:] in _CURRENCY_CODES


def _unified_request(
    payload: UnifiedOHLCVLoadRequest,
    asset_class: AssetClass,
    *,
    symbol: str,
    exchange: str,
    currency: str,
    what_to_show: str,
    use_rth: bool,
    primary_exchange: str | None = None,
    last_trade_date_or_contract_month: str | None = None,
    multiplier: str | None = None,
    local_symbol: str | None = None,
    con_id: int | None = None,
    continuous: bool = False,
) -> OHLCVRequest:
    return OHLCVRequest(
        symbol=symbol,
        asset_class=asset_class,
        exchange=exchange,
        currency=currency,
        start_datetime=payload.starttime,
        end_datetime=payload.endtime,
        duration="1 D",
        bar_size=payload.interval,
        what_to_show=what_to_show,
        use_rth=use_rth,
        primary_exchange=primary_exchange,
        last_trade_date_or_contract_month=last_trade_date_or_contract_month,
        multiplier=multiplier,
        local_symbol=local_symbol,
        continuous=continuous,
        con_id=con_id,
        metadata=payload.metadata,
    )


async def load_ohlcv_with_controls(
    *,
    request: OHLCVRequest,
    start_datetime: datetime | None = None,
    persist: bool,
    cache_latest: bool,
    use_ttl_cache: bool,
    cache_ttl_seconds: float | None,
    cache_namespace: str,
    state: IBKRRestAppState,
) -> list[OHLCVBar]:
    auto_chunk_plan = None
    if start_datetime is None:
        auto_chunk_plan = plan_historical_auto_chunk(request)
        if auto_chunk_plan is not None:
            try:
                ensure_historical_chunk_limit(
                    request,
                    auto_chunk_plan,
                    max_chunks=state.settings.ibkr_historical_max_chunks,
                )
            except HistoricalRequestTooLargeError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            metrics.market_data_historical_auto_chunks_total.inc(
                {"asset_class": request.asset_class.value, "operation": "fastapi", "status": "planned"}
            )
            logger.info(
                "FastAPI historical OHLCV auto_chunking symbol=%s bar_size=%s duration=%s "
                "max_duration=%s estimated_chunks=%d range=%s -> %s",
                request.symbol,
                request.bar_size,
                request.duration,
                auto_chunk_plan.max_duration,
                auto_chunk_plan.estimated_chunks,
                auto_chunk_plan.start_datetime.isoformat(),
                auto_chunk_plan.end_datetime.isoformat(),
            )
            start_datetime = auto_chunk_plan.start_datetime
            request = request.model_copy(update={"end_datetime": auto_chunk_plan.end_datetime})

    # When start_datetime is provided or inferred for an oversized request, use paginated range fetch.
    if start_datetime is not None and start_datetime != request.end_datetime:
        if use_ttl_cache and not persist:
            key = stable_cache_key(
                f"{cache_namespace}:range",
                {
                    "request": request.model_dump(mode="json"),
                    "start_datetime": start_datetime.isoformat(),
                    "auto_chunk": auto_chunk_plan is not None,
                },
            )
            cached = await state.market_data_cache.get(key)
            if cached is not None:
                return cached

        try:
            bars = await state.feed.load_historical_ohlcv_range(
                request,
                start_datetime=start_datetime,
                end_datetime=request.end_datetime,
                max_chunks=state.settings.ibkr_historical_max_chunks,
            )
        except HistoricalRequestTooLargeError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        if persist:
            logger.info("FastAPI OHLCV persist requested for %s but API persistence is disabled; scheduler owns storage", request.symbol)
        if cache_latest and bars:
            await state.loader.cache_latest_bar(bars[-1])

        if use_ttl_cache and not persist:
            await state.market_data_cache.set(key, bars, ttl_seconds=cache_ttl_seconds)
        return bars

    # Single-chunk fetch (original behavior).
    async def load() -> list[OHLCVBar]:
        if persist:
            logger.info("FastAPI OHLCV persist requested for %s but API persistence is disabled; scheduler owns storage", request.symbol)
        return await state.loader.load(
            request,
            persist=False,
            cache_latest=cache_latest,
        )

    if use_ttl_cache and not persist:
        key = stable_cache_key(
            cache_namespace,
            {
                "request": request.model_dump(mode="json"),
                "cache_latest": cache_latest,
            },
        )
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=cache_ttl_seconds)
    return await load()


def contract_int(value: Any, *names: str) -> int | None:
    for name in names:
        raw = getattr(value, name, None)
        if raw not in (None, ""):
            try:
                return int(str(raw))
            except (TypeError, ValueError):
                return None
    return None


def contract_text(value: Any, *names: str) -> str | None:
    for name in names:
        raw = getattr(value, name, None)
        if raw not in (None, ""):
            return str(raw).strip() or None
    return None


def market_rule_ids(value: Any) -> tuple[int, ...]:
    raw = contract_text(value, "marketRuleIds", "market_rule_ids") or ""
    ids: list[int] = []
    for item in raw.replace(";", ",").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            ids.append(int(item))
        except ValueError:
            continue
    return tuple(ids)


def session_to_dict(session: Any) -> dict[str, Any]:
    return {
        key: getattr(session, key, None)
        for key in ("startDateTime", "endDateTime", "refDate", "start", "end")
        if getattr(session, key, None) is not None
    }
