"""Auto-resolve IBKR exchange, currency, and primary_exchange from a ticker symbol.

This module is used by the wrapper OHLCV endpoints so users can pass just
a symbol like ``TSLA`` or ``0700.HK`` without manually specifying exchange/currency.

Rules (applied in order):
  1. Explicit suffix  →  suffix mapping  (e.g. ``.HK`` → SEHK/HKD)
  2. Recognised ticker patterns (FX pairs, futures, crypto)
  3. Default to SMART/USD (US equities)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar


@dataclass(frozen=True, slots=True)
class ResolvedExchange:
    """Resolved IBKR contract fields from a ticker symbol."""

    symbol: str  # stripped symbol (suffix removed)
    exchange: str
    currency: str
    primary_exchange: str = ""


# ---------------------------------------------------------------------------
# Suffix → (exchange, currency, primary_exchange)
# ---------------------------------------------------------------------------
_SUFFIX_MAP: dict[str, tuple[str, str, str]] = {
    # Hong Kong
    ".HK": ("SEHK", "HKD", "SEHK"),
    # Japan
    ".T": ("TSEJ", "JPY", "TSEJ"),
    # London
    ".L": ("LSE", "GBP", "LSE"),
    # Frankfurt / Xetra
    ".F": ("IBIS2", "EUR", "IBIS2"),
    ".DE": ("IBIS2", "EUR", "IBIS2"),
    # Paris
    ".PA": ("SBF", "EUR", "SBF"),
    # Amsterdam
    ".AS": ("AEB", "EUR", "AEB"),
    # Madrid
    ".MC": ("BM", "EUR", "BM"),
    # Milan
    ".MI": ("BVME", "EUR", "BVME"),
    # Switzerland
    ".SW": ("EBS", "CHF", "EBS"),
    # Toronto / TSX
    ".TO": ("TSE", "CAD", "TSE"),
    # TSX Venture
    ".V": ("TSE", "CAD", "TSE"),
    # Australia
    ".AX": ("ASX", "AUD", "ASX"),
    # Singapore
    ".SI": ("SGX", "SGD", "SGX"),
    # India (NSE)
    ".NS": ("NSE", "INR", "NSE"),
    # India (BSE)
    ".BO": ("BSE", "INR", "BSE"),
    # Korea
    ".KS": ("KSE", "KRW", "KSE"),
    ".KQ": ("KSE", "KRW", "KSE"),
    # Shanghai A-shares (Stock Connect)
    ".SS": ("SEHKNTL", "CNH", "SEHKNTL"),
    # Shenzhen A-shares (Stock Connect)
    ".SZ": ("SEHKSZSE", "CNH", "SEHKSZSE"),
    # Mexico
    ".MX": ("MEXI", "MXN", "MEXI"),
    # Brazil
    ".SA": ("BVMF", "BRL", "BVMF"),
}

# IBKR exchange codes for specific US exchanges (used as primary_exchange)
_US_PRIMARY_EXCHANGES: ClassVar[dict[str, str]] = {
    # Common ETFs
    "SPY": "ARCA",
    "QQQ": "ARCA",
    "IWM": "ARCA",
    "DIA": "ARCA",
    "GLD": "ARCA",
    "SLV": "ARCA",
    "TLT": "ARCA",
    "HYG": "ARCA",
    "LQD": "ARCA",
    "EEM": "ARCA",
    "VWO": "ARCA",
    "VEA": "ARCA",
    "IEFA": "ARCA",
    "AGG": "ARCA",
    "BND": "ARCA",
    "VTI": "ARCA",
    "VOO": "ARCA",
    "VEU": "ARCA",
    # Big tech — NASDAQ
    "AAPL": "NASDAQ",
    "MSFT": "NASDAQ",
    "GOOGL": "NASDAQ",
    "GOOG": "NASDAQ",
    "AMZN": "NASDAQ",
    "NVDA": "NASDAQ",
    "META": "NASDAQ",
    "TSLA": "NASDAQ",
    "NFLX": "NASDAQ",
    "AMD": "NASDAQ",
    "INTC": "NASDAQ",
    "CSCO": "NASDAQ",
    "PEP": "NASDAQ",
    "COST": "NASDAQ",
    "AVGO": "NASDAQ",
    "TXN": "NASDAQ",
    "QCOM": "NASDAQ",
    "BKNG": "NASDAQ",
    "PYPL": "NASDAQ",
    "SBUX": "NASDAQ",
    "CMCSA": "NASDAQ",
    "ADBE": "NASDAQ",
    "CRM": "NASDAQ",
    "NFLX": "NASDAQ",
    "FISV": "NASDAQ",
    "GILD": "NASDAQ",
    "MRVL": "NASDAQ",
    "MELI": "NASDAQ",
    "PANW": "NASDAQ",
    "SHOP": "NASDAQ",
    # NYSE-listed
    "BRK.B": "NYSE",
    "JPM": "NYSE",
    "V": "NYSE",
    "MA": "NYSE",
    "UNH": "NYSE",
    "JNJ": "NYSE",
    "WMT": "NYSE",
    "PG": "NYSE",
    "HD": "NYSE",
    "DIS": "NYSE",
    "BAC": "NYSE",
    "XOM": "NYSE",
    "CVX": "NYSE",
    "KO": "NYSE",
    "PFE": "NYSE",
    "ABBV": "NYSE",
    "MRK": "NYSE",
    "TMO": "NYSE",
    "LLY": "NYSE",
    "COP": "NYSE",
    "C": "NYSE",
    "GS": "NYSE",
    "MS": "NYSE",
    "CAT": "NYSE",
    "BA": "NYSE",
    "IBM": "NYSE",
    "GE": "NYSE",
    "MCD": "NYSE",
    "NKE": "NYSE",
    "VZ": "NYSE",
    "T": "NYSE",
}


def resolve_equity(symbol: str) -> ResolvedExchange:
    """Resolve an equity ticker to IBKR exchange/currency/primary_exchange.

    Parameters
    ----------
    symbol:
        A ticker with optional suffix, e.g. ``TSLA``, ``0700.HK``, ``7203.T``.
        Case-insensitive.

    Returns
    -------
    ResolvedExchange with stripped symbol, exchange, currency, and primary_exchange.

    Examples
    --------
    >>> resolve_equity("TSLA")
    ResolvedExchange(symbol='TSLA', exchange='SMART', currency='USD', primary_exchange='NASDAQ')
    >>> resolve_equity("0700.HK")
    ResolvedExchange(symbol='0700', exchange='SEHK', currency='HKD', primary_exchange='SEHK')
    >>> resolve_equity("7203.T")
    ResolvedExchange(symbol='7203', exchange='TSEJ', currency='JPY', primary_exchange='TSEJ')
    """
    raw = symbol.strip()
    if not raw:
        raise ValueError("symbol must not be empty")

    upper = raw.upper()

    # 1. Check suffix
    for suffix, (exchange, currency, primary) in _SUFFIX_MAP.items():
        if upper.endswith(suffix):
            stripped = upper[: -len(suffix)]
            return ResolvedExchange(
                symbol=stripped,
                exchange=exchange,
                currency=currency,
                primary_exchange=primary,
            )

    # 2. Check US primary exchange lookup
    primary = _US_PRIMARY_EXCHANGES.get(upper, "")

    # 3. Default US equity
    return ResolvedExchange(
        symbol=upper,
        exchange="SMART",
        currency="USD",
        primary_exchange=primary,
    )
