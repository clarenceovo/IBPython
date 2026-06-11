"""Historical OHLCV data loading, contract qualification, deduplication, and bar normalization."""

from __future__ import annotations

import logging
import math
import time as monotonic_time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from src.config import config_constant as constants
from src.feeds.contracts import ContractSpec, build_ibkr_contract
from src.feeds.ibkr_connection import (
    IBKRConnectionManager,
    _contract_int,
    _contract_text,
    _last_ibkr_error_message,
    _qualification_hint,
    _root_cause_message,
)
from src.feeds.models import AssetClass, FXOHLCVBar, FutureOHLCVBar, OHLCVBar, OHLCVRequest, OptionOHLCVBar
from src.transport.metrics import metrics

logger = logging.getLogger(__name__)

US_EQUITY_PRIMARY_EXCHANGE_PREFERENCE: tuple[str, ...] = ("NASDAQ", "NYSE", "ARCA", "AMEX", "BATS")


@dataclass(frozen=True)
class HistoricalAutoChunkPlan:
    """Computed range plan for an oversized IBKR historical request."""

    start_datetime: datetime
    end_datetime: datetime
    requested_duration_seconds: float
    max_duration: str
    max_duration_seconds: float
    estimated_chunks: int


class HistoricalRequestTooLargeError(ValueError):
    """Raised when an OHLCV request would exceed the configured IBKR chunk cap."""

    def __init__(self, *, symbol: str, bar_size: str, estimated_chunks: int, max_chunks: int) -> None:
        self.symbol = symbol
        self.bar_size = bar_size
        self.estimated_chunks = estimated_chunks
        self.max_chunks = max_chunks
        super().__init__(
            f"historical OHLCV request for {symbol} bar_size={bar_size!r} requires "
            f"~{estimated_chunks} IBKR chunks, exceeding configured max {max_chunks}"
        )


def _contract_details_contract(detail: Any) -> Any:
    return getattr(detail, "contract", detail)


def _is_contract_details_candidate(contract: Any, requested_contract: Any) -> bool:
    for attribute_name in ("secType", "symbol", "currency"):
        expected = _contract_text(requested_contract, attribute_name)
        actual = _contract_text(contract, attribute_name)
        if expected and actual and expected != actual:
            return False
    return True


def _contract_detail_score(contract: Any, spec: ContractSpec, requested_contract: Any) -> int:
    score = 0
    requested_exchange = _contract_text(requested_contract, "exchange")
    contract_exchange = _contract_text(contract, "exchange")
    primary_exchange = _contract_text(contract, "primaryExchange", "primaryExch")
    con_id = _contract_int(contract, "conId")

    if spec.con_id and con_id == spec.con_id:
        score += 10_000
    if _contract_text(contract, "symbol") == _contract_text(requested_contract, "symbol"):
        score += 100
    if _contract_text(contract, "secType") == _contract_text(requested_contract, "secType"):
        score += 80
    if _contract_text(contract, "currency") == _contract_text(requested_contract, "currency"):
        score += 60

    if spec.primary_exchange:
        target_primary = spec.primary_exchange.upper()
        if primary_exchange == target_primary:
            score += 500
        if contract_exchange == target_primary:
            score += 200

    elif spec.asset_class is AssetClass.EQUITY and primary_exchange in US_EQUITY_PRIMARY_EXCHANGE_PREFERENCE:
        score += 100 - US_EQUITY_PRIMARY_EXCHANGE_PREFERENCE.index(primary_exchange)

    if requested_exchange and requested_exchange != "SMART":
        if contract_exchange == requested_exchange:
            score += 300
        if primary_exchange == requested_exchange:
            score += 150
    elif requested_exchange == "SMART" and contract_exchange == "SMART":
        score += 20

    if con_id:
        score += 5
    return score


def _select_contract_from_details(details: Sequence[Any], spec: ContractSpec, requested_contract: Any) -> Any | None:
    if not details:
        return None
    candidates = [
        _contract_details_contract(detail)
        for detail in details
        if _is_contract_details_candidate(_contract_details_contract(detail), requested_contract)
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda contract: _contract_detail_score(contract, spec, requested_contract))


# IBKR maximum duration per bar_size for a single reqHistoricalData call.
_IBKR_MAX_DURATION_BY_BAR_SIZE: dict[str, str] = {
    "1 sec": "1800 S",
    "5 secs": "3600 S",
    "10 secs": "7200 S",
    "15 secs": "14400 S",
    "30 secs": "28800 S",
    "1 min": "1 D",
    "2 mins": "2 D",
    "3 mins": "3 D",
    "5 mins": "7 D",
    "10 mins": "14 D",
    "15 mins": "30 D",
    "20 mins": "30 D",
    "30 mins": "60 D",
    "1 hour": "365 D",
    "2 hours": "365 D",
    "3 hours": "365 D",
    "4 hours": "365 D",
    "8 hours": "365 D",
    "1 day": "18 M",
    "1 week": "10 Y",
    "1 month": "10 Y",
}


def _ibkr_max_duration_for_bar_size(bar_size: str) -> str:
    """Return the maximum IBKR duration string for a given bar size."""
    normalized = bar_size.strip().lower()
    for key, value in _IBKR_MAX_DURATION_BY_BAR_SIZE.items():
        if key == normalized or key.rstrip("s") == normalized.rstrip("s"):
            return value
    return "365 D"


def _ibkr_duration_to_seconds(duration: str) -> float:
    """Convert an IBKR duration string to approximate seconds."""
    duration = duration.strip()
    parts = duration.split()
    if len(parts) != 2:
        return 86400.0
    try:
        amount = float(parts[0])
    except ValueError:
        return 86400.0
    unit = parts[1].upper()
    if unit == "S":
        return amount
    if unit == "D":
        return amount * 86400
    if unit == "W":
        return amount * 86400 * 7
    if unit == "M":
        return amount * 86400 * 30
    if unit == "Y":
        return amount * 86400 * 365
    return 86400.0


def _seconds_to_timedelta(seconds: float) -> timedelta:
    from datetime import timedelta as td
    return td(seconds=seconds)


def _ibkr_duration_between(start: datetime, end: datetime) -> str:
    """Compute an IBKR duration string that covers the interval from start to end."""
    total_seconds = (end - start).total_seconds()
    if total_seconds <= 0:
        return "1 D"
    days = total_seconds / 86400
    if days <= 1:
        return f"{int(total_seconds)} S"
    if days <= 365:
        return f"{math.ceil(days)} D"
    months = math.ceil(days / 30)
    if months <= 18:
        return f"{months} M"
    years = math.ceil(days / 365)
    return f"{years} Y"


def plan_historical_auto_chunk(request: OHLCVRequest, *, now: datetime | None = None) -> HistoricalAutoChunkPlan | None:
    """Return a range plan when a request exceeds IBKR's max duration for its bar size."""
    if request.start_datetime is not None:
        return None
    requested_seconds = _ibkr_duration_to_seconds(request.duration)
    max_duration = _ibkr_max_duration_for_bar_size(request.bar_size)
    max_seconds = _ibkr_duration_to_seconds(max_duration)
    if requested_seconds <= max_seconds:
        return None

    end_datetime = request.end_datetime or now or datetime.now(timezone.utc)
    if end_datetime.tzinfo is None:
        end_datetime = end_datetime.replace(tzinfo=timezone.utc)
    end_datetime = end_datetime.astimezone(timezone.utc)
    start_datetime = end_datetime - timedelta(seconds=requested_seconds)
    return HistoricalAutoChunkPlan(
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        requested_duration_seconds=requested_seconds,
        max_duration=max_duration,
        max_duration_seconds=max_seconds,
        estimated_chunks=max(1, math.ceil(requested_seconds / max_seconds)),
    )


def ensure_historical_chunk_limit(request: OHLCVRequest, plan: HistoricalAutoChunkPlan | None, *, max_chunks: int) -> None:
    """Fail before issuing IBKR requests when a range would exceed the production chunk cap."""
    if max_chunks <= 0:
        raise ValueError("max_chunks must be positive")
    if plan is not None and plan.estimated_chunks > max_chunks:
        metrics.market_data_historical_auto_chunks_total.inc(
            {"asset_class": request.asset_class.value, "operation": "plan", "status": "rejected"}
        )
        raise HistoricalRequestTooLargeError(
            symbol=request.symbol,
            bar_size=request.bar_size,
            estimated_chunks=plan.estimated_chunks,
            max_chunks=max_chunks,
        )


def _format_ibkr_end_datetime(value: datetime | None) -> str:
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y%m%d %H:%M:%S UTC")


def _expired_future_end_datetime(contract: Any, request: OHLCVRequest) -> datetime | None:
    if request.asset_class is not AssetClass.FUTURE or request.continuous or request.end_datetime is not None:
        return None
    last_trade = _contract_text(contract, "lastTradeDateOrContractMonth")
    if len(last_trade) != 8 or not last_trade.isdigit():
        return None
    last_trade_date = datetime.strptime(last_trade, "%Y%m%d").date()
    if last_trade_date >= date.today():
        return None
    return datetime.combine(last_trade_date + timedelta(days=1), time.min, tzinfo=timezone.utc)


def _parse_ibkr_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    elif isinstance(value, str):
        parsed = _parse_timestamp_string(value)
    else:
        raise TypeError(f"unsupported IBKR timestamp type: {type(value)!r}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_timestamp_string(value: str) -> datetime:
    text = value.strip().replace("Z", "+00:00")
    for fmt in ("%Y%m%d %H:%M:%S %Z", "%Y%m%d %H:%M:%S", "%Y%m%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt.endswith("%Z") and text.endswith("UTC"):
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            continue
    return datetime.fromisoformat(text)


def _ohlcv_bar_model_for_request(request: OHLCVRequest) -> type[OHLCVBar]:
    if request.asset_class is AssetClass.FUTURE:
        return FutureOHLCVBar
    if request.asset_class is AssetClass.FX:
        return FXOHLCVBar
    if request.asset_class is AssetClass.OPTION:
        return OptionOHLCVBar
    return OHLCVBar


def _fx_base_currency(symbol: str) -> str | None:
    normalized = symbol.replace("/", "").strip().upper()
    if len(normalized) >= 6:
        return normalized[:3]
    return None


def _normalized_ibkr_volume(value: Any) -> float:
    volume = float(value or 0)
    return max(volume, 0.0)


def normalize_ibkr_bars(bars: Sequence[Any], request: OHLCVRequest) -> list[OHLCVBar]:
    normalized: list[OHLCVBar] = []
    bar_model = _ohlcv_bar_model_for_request(request)
    is_continuous_future = request.asset_class is AssetClass.FUTURE and request.continuous
    for bar in bars:
        normalized.append(
            bar_model(
                symbol=request.symbol,
                asset_class=request.asset_class,
                exchange=request.exchange,
                currency=request.currency,
                timestamp=_parse_ibkr_timestamp(getattr(bar, "date")),
                open=float(getattr(bar, "open")),
                high=float(getattr(bar, "high")),
                low=float(getattr(bar, "low")),
                close=float(getattr(bar, "close")),
                volume=_normalized_ibkr_volume(getattr(bar, "volume", 0)),
                bar_size=request.bar_size,
                source=request.source,
                vwap=float(getattr(bar, "average", 0) or 0) or None,
                trade_count=int(getattr(bar, "barCount", 0) or 0) or None,
                **(
                    {
                        "contract_month": request.last_trade_date_or_contract_month,
                        "is_continuous": bool(is_continuous_future or request.metadata.get("is_continuous", False)),
                    }
                    if request.asset_class is AssetClass.FUTURE
                    else {}
                ),
                **(
                    {
                        "base_currency": request.metadata.get("base_currency") or _fx_base_currency(request.symbol),
                        "quote_currency": request.metadata.get("quote_currency") or request.currency,
                    }
                    if request.asset_class is AssetClass.FX
                    else {}
                ),
                **(
                    {
                        "underlying_symbol": request.underlying_symbol or request.symbol,
                        "expiry": request.expiry or request.last_trade_date_or_contract_month,
                        "strike": request.strike,
                        "right": request.right,
                        "multiplier": request.multiplier,
                        "trading_class": request.trading_class,
                        "contract_month": (request.expiry or request.last_trade_date_or_contract_month or "")[:6],
                        "con_id": request.con_id,
                    }
                    if request.asset_class is AssetClass.OPTION
                    else {}
                ),
                metadata={
                    **request.metadata,
                    "what_to_show": request.what_to_show,
                    "use_rth": request.use_rth,
                    "con_id": request.con_id,
                    "local_symbol": request.local_symbol,
                    "contract_month": request.last_trade_date_or_contract_month,
                    "is_continuous": bool(is_continuous_future or request.metadata.get("is_continuous", False)),
                    "continuous": request.continuous,
                    "expiry": request.expiry,
                    "strike": request.strike,
                    "right": request.right,
                    "trading_class": request.trading_class,
                },
            )
        )
    return normalized


def _historical_identical_key(request: OHLCVRequest) -> tuple[Any, ...]:
    return (
        request.symbol,
        request.asset_class,
        request.exchange,
        request.currency,
        request.continuous,
        request.start_datetime,
        request.end_datetime,
        request.duration,
        request.bar_size,
        request.what_to_show.upper(),
        request.use_rth,
    )


def _historical_same_contract_key(request: OHLCVRequest) -> tuple[Any, ...]:
    return (
        request.symbol,
        request.asset_class,
        request.exchange,
        request.continuous,
        request.what_to_show.upper(),
    )


class IBKRHistoricalClient:
    """Historical OHLCV data loading with pacing guard support."""

    def __init__(self, connection: IBKRConnectionManager) -> None:
        self._connection = connection

    @property
    def _ib(self) -> Any:
        return self._connection.ib

    async def qualify_contract(self, spec: ContractSpec) -> Any:
        await self._connection.ensure_connected()
        logger.info(
            "qualify_contract: symbol=%s asset_class=%s exchange=%s primary_exchange=%s con_id=%s",
            spec.symbol,
            spec.asset_class,
            spec.exchange,
            spec.primary_exchange,
            spec.con_id,
        )
        t0 = monotonic_time.monotonic()
        contract = build_ibkr_contract(spec)
        qualified = await self._connection.with_retry(
            lambda: self._ib.qualifyContractsAsync(contract),
            operation=f"qualify_contract:{spec.symbol}",
        )
        if qualified:
            selected = qualified[0]
            logger.debug(
                "qualify_contract completed in %.2fs for %s con_id=%s primary_exchange=%s",
                monotonic_time.monotonic() - t0,
                spec.symbol,
                _contract_int(selected, "conId"),
                _contract_text(selected, "primaryExchange", "primaryExch"),
            )
            return selected

        logger.warning(
            "qualifyContractsAsync returned no contract for %s; requesting contract details fallback",
            spec.symbol,
        )
        try:
            selected = await self._resolve_contract_from_details(contract, spec)
        except Exception as exc:
            raise RuntimeError(
                f"IBKR could not qualify contract for {spec.symbol}.{_qualification_hint(spec)} "
                f"contract_details_root_cause={_root_cause_message(exc)} "
                f"{_last_ibkr_error_message(self._connection.last_ibkr_error)}"
            ) from exc
        if selected is None:
            raise RuntimeError(
                f"IBKR could not qualify contract for {spec.symbol}.{_qualification_hint(spec)} "
                f"{_last_ibkr_error_message(self._connection.last_ibkr_error)}"
            )
        logger.info(
            "qualify_contract fallback selected %s con_id=%s exchange=%s primary_exchange=%s in %.2fs",
            spec.symbol,
            _contract_int(selected, "conId"),
            _contract_text(selected, "exchange"),
            _contract_text(selected, "primaryExchange", "primaryExch"),
            monotonic_time.monotonic() - t0,
        )
        return selected

    async def _resolve_contract_from_details(self, contract: Any, spec: ContractSpec) -> Any | None:
        details = await self._connection.with_retry(
            lambda: self._ib.reqContractDetailsAsync(contract),
            operation=f"contract_details:{spec.symbol}",
        )
        selected = _select_contract_from_details(details, spec, contract)
        if (
            selected is not None
            and spec.asset_class is AssetClass.EQUITY
            and spec.exchange.upper() == "SMART"
            and _contract_text(selected, "exchange")
        ):
            setattr(selected, "exchange", "SMART")
        return selected

    async def load_historical_ohlcv_range(
        self,
        request: OHLCVRequest,
        *,
        start_datetime: datetime,
        end_datetime: datetime | None = None,
        max_chunks: int = constants.DEFAULT_IBKR_HISTORICAL_MAX_CHUNKS,
    ) -> list[OHLCVBar]:
        """Paginated historical OHLCV fetch across a date range."""
        if end_datetime is None:
            end_datetime = datetime.now(timezone.utc)
        if start_datetime.tzinfo is None:
            start_datetime = start_datetime.replace(tzinfo=timezone.utc)
        if end_datetime.tzinfo is None:
            end_datetime = end_datetime.replace(tzinfo=timezone.utc)

        chunk_duration = _ibkr_max_duration_for_bar_size(request.bar_size)
        chunk_seconds = _ibkr_duration_to_seconds(chunk_duration)

        total_seconds = (end_datetime - start_datetime).total_seconds()
        if total_seconds <= 0:
            logger.info("load_historical_ohlcv_range: empty range, returning []")
            return []
        estimated_chunks = max(1, math.ceil(total_seconds / chunk_seconds))
        if estimated_chunks > max_chunks:
            metrics.market_data_historical_auto_chunks_total.inc(
                {"asset_class": request.asset_class.value, "operation": "range", "status": "rejected"}
            )
            raise HistoricalRequestTooLargeError(
                symbol=request.symbol,
                bar_size=request.bar_size,
                estimated_chunks=estimated_chunks,
                max_chunks=max_chunks,
            )

        await self._connection.ensure_connected()

        logger.info(
            "load_historical_ohlcv_range: symbol=%s bar_size=%s range=%s → %s (%.0f seconds, ~%d chunks)",
            request.symbol, request.bar_size, start_datetime.isoformat(), end_datetime.isoformat(),
            total_seconds, estimated_chunks,
        )

        all_bars: list[OHLCVBar] = []
        chunk_end = end_datetime
        chunk_count = 0

        while chunk_end > start_datetime and chunk_count < max_chunks:
            chunk_start = max(start_datetime, chunk_end - _seconds_to_timedelta(chunk_seconds))
            chunk_duration_actual = _ibkr_duration_between(chunk_start, chunk_end)

            chunk_request = request.model_copy(update={
                "end_datetime": chunk_end,
                "duration": chunk_duration_actual,
                "start_datetime": None,
            })

            logger.info(
                "ohlcv_range chunk %d: fetching %s → %s (duration=%s)",
                chunk_count + 1, chunk_start.isoformat(), chunk_end.isoformat(), chunk_duration_actual,
            )

            bars = await self.load_historical_ohlcv(chunk_request)
            bars = [b for b in bars if b.timestamp >= start_datetime]
            all_bars = bars + all_bars
            chunk_count += 1

            if not bars:
                chunk_end = chunk_start
                continue

            earliest = bars[0].timestamp
            if earliest <= chunk_start:
                break
            chunk_end = earliest

        # Deduplicate by timestamp
        seen: set[datetime] = set()
        unique_bars: list[OHLCVBar] = []
        for bar in all_bars:
            if bar.timestamp not in seen:
                seen.add(bar.timestamp)
                unique_bars.append(bar)
        unique_bars.sort(key=lambda b: b.timestamp)

        logger.info(
            "load_historical_ohlcv_range: %d bars for %s across %d chunks (range %s → %s)",
            len(unique_bars), request.symbol, chunk_count,
            start_datetime.date().isoformat(), end_datetime.date().isoformat(),
        )
        metrics.market_data_historical_chunks_total.inc(
            {"asset_class": request.asset_class.value, "operation": "range"},
            amount=chunk_count,
        )
        metrics.market_data_historical_bars_total.inc(
            {"asset_class": request.asset_class.value, "operation": "range"},
            amount=len(unique_bars),
        )
        return unique_bars

    async def load_historical_ohlcv(
        self,
        request: OHLCVRequest,
        *,
        max_chunks: int = constants.DEFAULT_IBKR_HISTORICAL_MAX_CHUNKS,
    ) -> list[OHLCVBar]:
        if request.start_datetime is not None:
            range_request = request.model_copy(update={"start_datetime": None})
            return await self.load_historical_ohlcv_range(
                range_request,
                start_datetime=request.start_datetime,
                end_datetime=request.end_datetime,
                max_chunks=max_chunks,
            )

        auto_chunk_plan = plan_historical_auto_chunk(request)
        if auto_chunk_plan is not None:
            ensure_historical_chunk_limit(request, auto_chunk_plan, max_chunks=max_chunks)
            metrics.market_data_historical_auto_chunks_total.inc(
                {"asset_class": request.asset_class.value, "operation": "feed", "status": "planned"}
            )
            logger.info(
                "load_historical_ohlcv: auto_chunking oversized request symbol=%s bar_size=%s duration=%s "
                "max_duration=%s estimated_chunks=%d range=%s → %s",
                request.symbol,
                request.bar_size,
                request.duration,
                auto_chunk_plan.max_duration,
                auto_chunk_plan.estimated_chunks,
                auto_chunk_plan.start_datetime.isoformat(),
                auto_chunk_plan.end_datetime.isoformat(),
            )
            range_request = request.model_copy(
                update={
                    "start_datetime": None,
                    "end_datetime": auto_chunk_plan.end_datetime,
                }
            )
            return await self.load_historical_ohlcv_range(
                range_request,
                start_datetime=auto_chunk_plan.start_datetime,
                end_datetime=auto_chunk_plan.end_datetime,
                max_chunks=max_chunks,
            )

        await self._connection.ensure_connected()
        logger.info("load_historical_ohlcv: symbol=%s bar_size=%s duration=%s", request.symbol, request.bar_size, request.duration)
        t0 = monotonic_time.monotonic()
        contract = await self.qualify_contract(ContractSpec.from_ohlcv_request(request))
        effective_end_datetime = request.end_datetime or _expired_future_end_datetime(contract, request)
        if effective_end_datetime is not request.end_datetime and effective_end_datetime is not None:
            logger.info(
                "load_historical_ohlcv: clipping expired future %s end_datetime to %s",
                request.symbol,
                effective_end_datetime.isoformat(),
            )
        end_datetime = _format_ibkr_end_datetime(effective_end_datetime)

        try:
            await self._connection.pacing_guard.acquire(request)
            bars = await self._connection.with_retry(
                lambda: self._ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime=end_datetime,
                    durationStr=request.duration,
                    barSizeSetting=request.bar_size,
                    whatToShow=request.what_to_show,
                    useRTH=request.use_rth,
                    formatDate=2,
                    keepUpToDate=False,
                ),
                operation=f"historical_ohlcv:{request.symbol}:{request.bar_size}",
            )
        finally:
            self._connection.pacing_guard.release()
        result = normalize_ibkr_bars(bars, request)
        logger.info("load_historical_ohlcv: %d bars for %s in %.2fs", len(result), request.symbol, monotonic_time.monotonic() - t0)
        return result

    async def load_trading_schedule(
        self,
        request: OHLCVRequest,
        *,
        ref_date: date,
        use_rth: bool = True,
    ) -> tuple[Any, ...]:
        """Load IBKR historical trading schedule sessions for one contract/date."""
        await self._connection.ensure_connected()
        contract = await self.qualify_contract(ContractSpec.from_ohlcv_request(request))
        end_datetime = datetime.combine(ref_date + timedelta(days=1), time.min, tzinfo=timezone.utc)
        schedule = await self._connection.with_retry(
            lambda: self._ib.reqHistoricalScheduleAsync(
                contract,
                1,
                endDateTime=_format_ibkr_end_datetime(end_datetime),
                useRTH=use_rth,
            ),
            operation=f"trading_schedule:{request.symbol}:{ref_date.isoformat()}",
        )
        sessions = tuple(getattr(schedule, "sessions", ()) or ())
        logger.info(
            "load_trading_schedule: symbol=%s date=%s use_rth=%s sessions=%d",
            request.symbol,
            ref_date.isoformat(),
            use_rth,
            len(sessions),
        )
        return sessions
