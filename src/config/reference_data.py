"""Reference data for market-data router endpoints.

Loads index exchange maps and commodity futures presets from an optional
YAML or JSON config file. Falls back to hardcoded defaults when no config
is found.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATHS: list[Path] = [
    Path("config/reference_data.json"),
    Path("config/reference_data.yaml"),
    Path("config/reference_data.yml"),
]

# ---------------------------------------------------------------------------
# Hardcoded defaults
# ---------------------------------------------------------------------------

_INDEX_EXCHANGE_MAP: dict[str, tuple[str, str]] = {
    # US indices
    "SPX": ("CBOE", "USD"),
    "NDX": ("CBOE", "USD"),
    "VIX": ("CBOE", "USD"),
    "RUT": ("ICE", "USD"),
    "DJI": ("CBOE", "USD"),
    "OEX": ("CBOE", "USD"),
    "NDXP": ("CBOE", "USD"),
    # Hong Kong
    "HSI": ("SEHK", "HKD"),
    "HSCEI": ("SEHK", "HKD"),
    "HSTECH": ("SEHK", "HKD"),
    # Japan
    "NIKKEI": ("TSEJ", "JPY"),
    "NKY": ("TSEJ", "JPY"),
    "TOPIX": ("TSEJ", "JPY"),
    # Europe
    "DAX": ("EUREX", "EUR"),
    "FDAX": ("EUREX", "EUR"),
    "ESTX50": ("EUREX", "EUR"),
    "SMI": ("EBS", "CHF"),
    "CAC40": ("SBF", "EUR"),
    "FTSE100": ("LSE", "GBP"),
    "FTSE250": ("LSE", "GBP"),
    # Australia
    "SPI": ("ASX", "AUD"),
    "XJO": ("ASX", "AUD"),
    # Korea
    "KOSPI": ("KSE", "KRW"),
    "KOSPI200": ("KSE", "KRW"),
    "KOSDQ150": ("KSE", "KRW"),
    # India
    "NIFTY": ("NSE", "INR"),
    "BANKNIFTY": ("NSE", "INR"),
    # Singapore
    "STI": ("SGX", "SGD"),
    # Taiwan
    "TAIEX": ("TWSE", "TWD"),
    # Canada
    "SPTSX": ("TSE", "CAD"),
}

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


def _load_yaml(path: Path) -> dict[str, Any] | None:
    """Try to load a YAML file; return None on failure."""
    try:
        import yaml  # type: ignore[import-untyped]

        with path.open("r") as fh:
            data = yaml.safe_load(fh)
        return data if isinstance(data, dict) else None
    except ImportError:
        logger.debug("PyYAML not installed; skipping YAML config %s", path)
        return None
    except FileNotFoundError:
        return None
    except Exception:
        logger.debug("Failed to load YAML config %s", path, exc_info=True)
        return None


def _load_json(path: Path) -> dict[str, Any] | None:
    """Try to load a JSON file; return None on failure."""
    try:
        with path.open("r") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else None
    except FileNotFoundError:
        return None
    except Exception:
        logger.debug("Failed to load JSON config %s", path, exc_info=True)
        return None


def _parse_str_tuple_map(raw: dict[str, Any]) -> dict[str, tuple[str, str]]:
    """Convert {"KEY": ["A", "B"]} → {"KEY": ("A", "B")}."""
    result: dict[str, tuple[str, str]] = {}
    for key, value in raw.items():
        if isinstance(value, (list, tuple)) and len(value) == 2:
            result[str(key)] = (str(value[0]), str(value[1]))
    return result


def _load_config() -> tuple[dict[str, tuple[str, str]], dict[str, tuple[str, str]]]:
    """Return (index_exchange_map, commodity_futures_presets) from config or defaults."""
    for path in _DEFAULT_CONFIG_PATHS:
        if path.suffix in (".yaml", ".yml"):
            data = _load_yaml(path)
        else:
            data = _load_json(path)
        if data is None:
            continue

        index_map = _DEFAULT_INDEX_EXCHANGE_MAP
        if "index_exchange_map" in data:
            index_map = _parse_str_tuple_map(data["index_exchange_map"])

        commodity_presets = _DEFAULT_COMMODITY_FUTURES_PRESETS
        if "commodity_futures_presets" in data:
            commodity_presets = _parse_str_tuple_map(data["commodity_futures_presets"])

        logger.info("Loaded reference data from %s", path)
        return index_map, commodity_presets

    return _DEFAULT_INDEX_EXCHANGE_MAP, _DEFAULT_COMMODITY_FUTURES_PRESETS


# Module-level singletons — loaded once on first import.
_DEFAULT_INDEX_EXCHANGE_MAP = _INDEX_EXCHANGE_MAP
_DEFAULT_COMMODITY_FUTURES_PRESETS = _COMMODITY_FUTURES_PRESETS

# Lazy initialization: defer config file I/O until first access.
# This avoids file reads at import time (which can fail in test environments
# and slows down module loading).
_lazy_index_map: dict[str, tuple[str, str]] | None = None
_lazy_commodity_presets: dict[str, tuple[str, str]] | None = None


def _get_index_exchange_map() -> dict[str, tuple[str, str]]:
    global _lazy_index_map
    if _lazy_index_map is None:
        _lazy_index_map, _lazy_commodity_presets = _load_config()
    return _lazy_index_map


def _get_commodity_futures_presets() -> dict[str, tuple[str, str]]:
    global _lazy_commodity_presets
    if _lazy_commodity_presets is None:
        _lazy_index_map, _lazy_commodity_presets = _load_config()
    return _lazy_commodity_presets


def __getattr__(name: str) -> Any:
    """Module-level __getattr__ for lazy initialization of public constants."""
    if name == "INDEX_EXCHANGE_MAP":
        return _get_index_exchange_map()
    if name == "COMMODITY_FUTURES_PRESETS":
        return _get_commodity_futures_presets()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def resolve_index(symbol: str) -> dict[str, str]:
    """Look up IBKR exchange and currency for a known index symbol."""
    upper = symbol.strip().upper()
    index_map = _get_index_exchange_map()
    if upper in index_map:
        exchange, currency = index_map[upper]
        return {"symbol": upper, "exchange": exchange, "currency": currency}
    return {"symbol": upper, "exchange": "CBOE", "currency": "USD"}


def resolve_commodity_future(symbol: str) -> dict[str, str]:
    """Look up exchange and currency for a commodity futures symbol."""
    upper = symbol.strip().upper()
    presets = _get_commodity_futures_presets()
    exchange, currency = presets.get(upper, ("NYMEX", "USD"))
    return {"symbol": upper, "exchange": exchange, "currency": currency}
