"""Converter functions for ticker-to-snapshot transformations."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from src.feeds.options import OptionContractSpec, OptionGreekSet, OptionGreekSource
from src.feeds.snapshot_models import EquitySnapshot, FXOptionSnapshot


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        v = float(value)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def ticker_to_snapshot(
    ticker: Any,
    *,
    symbol: str,
    exchange: str,
    currency: str,
    primary_exchange: str = "",
    con_id: int = 0,
    timestamp: datetime | None = None,
) -> EquitySnapshot:
    """Convert an ib_insync Ticker to an EquitySnapshot."""
    return EquitySnapshot(
        symbol=symbol,
        exchange=exchange,
        currency=currency,
        primary_exchange=primary_exchange,
        con_id=con_id,
        timestamp=timestamp or datetime.now(timezone.utc),
        last=_safe_float(getattr(ticker, "last", None)),
        bid=_safe_float(getattr(ticker, "bid", None)),
        ask=_safe_float(getattr(ticker, "ask", None)),
        bid_size=_safe_float(getattr(ticker, "bidSize", None)),
        ask_size=_safe_float(getattr(ticker, "askSize", None)),
        last_size=_safe_float(getattr(ticker, "lastSize", None)),
        volume=_safe_float(getattr(ticker, "volume", None)),
        open=_safe_float(getattr(ticker, "open_", None)),
        high=_safe_float(getattr(ticker, "high", None)),
        low=_safe_float(getattr(ticker, "low", None)),
        close=_safe_float(getattr(ticker, "close", None)),
        vwap=_safe_float(getattr(ticker, "vwap", None)),
        mark_price=_safe_float(getattr(ticker, "markPrice", None)),
        halted=getattr(ticker, "halted", None),
    )


def fx_pair_parts(symbol: str, currency: str | None = None) -> tuple[str, str, str]:
    normalized = symbol.replace("/", "").strip().upper()
    if len(normalized) != 6:
        raise ValueError("FX option symbols must be six-character pairs such as EURUSD")
    base = normalized[:3]
    quote = currency.strip().upper() if currency else normalized[3:]
    return normalized, base, quote


def fx_option_contract_key(
    *,
    symbol: str,
    expiry: str,
    strike: float,
    right: str,
    exchange: str = "SMART",
    local_symbol: str | None = None,
    con_id: int | None = None,
) -> str:
    normalized_right = right.strip().upper()
    if normalized_right == "CALL":
        normalized_right = "C"
    elif normalized_right == "PUT":
        normalized_right = "P"
    components = [
        symbol.replace("/", "").strip().upper(),
        expiry.strip().upper(),
        f"{float(strike):g}",
        normalized_right,
        exchange.strip().upper(),
        (local_symbol or "").strip().upper(),
        str(con_id or ""),
    ]
    return ":".join(component.replace(" ", "_") for component in components if component)


def _sum_optional(*values: float | None) -> float | None:
    present = [value for value in values if value is not None]
    return sum(present) if present else None


def _normalize_snapshot_greeks(value: Any, source: OptionGreekSource) -> OptionGreekSet | None:
    if value is None:
        return None
    return OptionGreekSet(
        source=source,
        implied_vol=getattr(value, "impliedVol", None),
        delta=getattr(value, "delta", None),
        gamma=getattr(value, "gamma", None),
        theta=getattr(value, "theta", None),
        vega=getattr(value, "vega", None),
        option_price=getattr(value, "optPrice", None),
        pv_dividend=getattr(value, "pvDividend", None),
        underlying_price=getattr(value, "undPrice", None),
    )


def ticker_to_fx_option_snapshot(
    ticker: Any,
    contract: OptionContractSpec,
    *,
    symbol: str,
    timestamp: datetime | None = None,
) -> FXOptionSnapshot:
    return FXOptionSnapshot(
        symbol=symbol,
        underlying_symbol=contract.underlying_symbol,
        expiry=contract.expiry,
        strike=contract.strike,
        right=contract.right.value,
        exchange=contract.exchange,
        currency=contract.currency,
        multiplier=contract.multiplier,
        trading_class=contract.trading_class,
        local_symbol=contract.local_symbol,
        con_id=contract.con_id or getattr(getattr(ticker, "contract", None), "conId", None),
        timestamp=timestamp or datetime.now(timezone.utc),
        last=_safe_float(getattr(ticker, "last", None)),
        bid=_safe_float(getattr(ticker, "bid", None)),
        ask=_safe_float(getattr(ticker, "ask", None)),
        bid_size=_safe_float(getattr(ticker, "bidSize", None)),
        ask_size=_safe_float(getattr(ticker, "askSize", None)),
        last_size=_safe_float(getattr(ticker, "lastSize", None)),
        volume=_safe_float(getattr(ticker, "volume", None)),
        mark_price=_safe_float(getattr(ticker, "markPrice", None)),
        implied_volatility=_safe_float(getattr(ticker, "impliedVolatility", None)),
        historical_volatility=_safe_float(getattr(ticker, "histVolatility", None)),
        average_option_volume=_safe_float(getattr(ticker, "avOptionVolume", None)),
        call_open_interest=_safe_float(getattr(ticker, "callOpenInterest", None)),
        put_open_interest=_safe_float(getattr(ticker, "putOpenInterest", None)),
        call_volume=_safe_float(getattr(ticker, "callVolume", None)),
        put_volume=_safe_float(getattr(ticker, "putVolume", None)),
        open_interest=_sum_optional(
            _safe_float(getattr(ticker, "callOpenInterest", None)),
            _safe_float(getattr(ticker, "putOpenInterest", None)),
        ),
        option_volume=_safe_float(getattr(ticker, "volume", None))
        or _sum_optional(_safe_float(getattr(ticker, "callVolume", None)), _safe_float(getattr(ticker, "putVolume", None))),
        bid_greeks=_normalize_snapshot_greeks(getattr(ticker, "bidGreeks", None), OptionGreekSource.BID),
        ask_greeks=_normalize_snapshot_greeks(getattr(ticker, "askGreeks", None), OptionGreekSource.ASK),
        last_greeks=_normalize_snapshot_greeks(getattr(ticker, "lastGreeks", None), OptionGreekSource.LAST),
        model_greeks=_normalize_snapshot_greeks(getattr(ticker, "modelGreeks", None), OptionGreekSource.MODEL),
    )
