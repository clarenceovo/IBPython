from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import pytest

from backfiller import api_bar_to_ohlcv_bar, build_requests, chunk_ranges, fetch_chunk, load_symbol_specs, parse_datetime
from src.feeds.models import AssetClass, OHLCVRequest
from src.transport.questdb_queries import bar_to_row


def _args(**overrides: object) -> argparse.Namespace:
    defaults = {
        "asset_class": "equity",
        "exchange": "SMART",
        "currency": "USD",
        "bar_size": "1 min",
        "duration": "1 D",
        "what_to_show": "TRADES",
        "use_rth": True,
        "primary_exchange": None,
        "last_trade_date_or_contract_month": None,
        "local_symbol": None,
        "multiplier": None,
        "con_id": None,
        "symbols": (),
        "symbols_file": None,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def test_load_symbol_specs_accepts_json_defaults_and_symbols(tmp_path: Path) -> None:
    symbols_file = tmp_path / "symbols.json"
    symbols_file.write_text(
        json.dumps(
            {
                "defaults": {
                    "asset_class": "future",
                    "exchange": "HKFE",
                    "currency": "HKD",
                    "bar_size": "1 min",
                    "what_to_show": "TRADES",
                },
                "symbols": [
                    {"symbol": "HSI", "last_trade_date_or_contract_month": "202606"},
                    {"symbol": "HTI", "last_trade_date_or_contract_month": "202606"},
                ],
            }
        ),
        encoding="utf-8",
    )

    defaults, symbols = load_symbol_specs(_args(symbols_file=symbols_file))

    assert defaults["asset_class"] == "future"
    assert defaults["exchange"] == "HKFE"
    assert [symbol["symbol"] for symbol in symbols] == ["HSI", "HTI"]


def test_build_requests_normalizes_json_symbol_dates_to_utc(tmp_path: Path) -> None:
    symbols_file = tmp_path / "symbols.json"
    symbols_file.write_text(
        json.dumps({"defaults": {"asset_class": "equity", "exchange": "SMART", "currency": "USD"}, "symbols": ["SPY"]}),
        encoding="utf-8",
    )
    start = parse_datetime("2026-05-28", ZoneInfo("Asia/Hong_Kong"))
    end = parse_datetime("2026-05-28", ZoneInfo("Asia/Hong_Kong"), is_end=True)

    requests = build_requests(_args(symbols_file=symbols_file), start, end)

    assert requests[0].symbol == "SPY"
    assert requests[0].start_datetime == datetime(2026, 5, 27, 16, 0, tzinfo=timezone.utc)
    assert requests[0].end_datetime == datetime(2026, 5, 28, 16, 0, tzinfo=timezone.utc)


def test_chunk_ranges_uses_exclusive_end() -> None:
    chunks = chunk_ranges(
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 1, 3, tzinfo=timezone.utc),
        86400,
    )

    assert chunks == (
        (datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 1, 2, tzinfo=timezone.utc)),
        (datetime(2026, 1, 2, tzinfo=timezone.utc), datetime(2026, 1, 3, tzinfo=timezone.utc)),
    )


def test_api_bar_timestamp_is_normalized_to_utc_before_db_row_mapping() -> None:
    request = OHLCVRequest(symbol="SPY", asset_class=AssetClass.EQUITY, exchange="SMART", currency="USD")

    bar = api_bar_to_ohlcv_bar(
        {
            "symbol": "SPY",
            "asset_class": "equity",
            "exchange": "SMART",
            "currency": "USD",
            "timestamp": "2026-05-28T09:30:00-04:00",
            "open": 100,
            "high": 101,
            "low": 99,
            "close": 100.5,
            "volume": 1000,
            "bar_size": "1 min",
            "source": "ibkr",
            "metadata": {},
        },
        request,
    )
    row = bar_to_row(bar)

    assert bar.timestamp == datetime(2026, 5, 28, 13, 30, tzinfo=timezone.utc)
    assert row[4] == datetime(2026, 5, 28, 13, 30)


@pytest.mark.asyncio
async def test_fetch_chunk_disables_api_persistence_and_latest_cache() -> None:
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["payload"] = json.loads(request.content.decode())
        return httpx.Response(
            200,
            json={
                "bars": [
                    {
                        "symbol": "SPY",
                        "asset_class": "equity",
                        "exchange": "SMART",
                        "currency": "USD",
                        "timestamp": "2026-05-28T13:30:00Z",
                        "open": 100,
                        "high": 101,
                        "low": 99,
                        "close": 100.5,
                        "volume": 1000,
                        "bar_size": "1 min",
                        "source": "ibkr",
                    }
                ],
                "request": {
                    "symbol": "SPY",
                    "asset_class": "equity",
                    "exchange": "SMART",
                    "currency": "USD",
                    "bar_size": "1 min",
                    "what_to_show": "TRADES",
                    "use_rth": True,
                },
                "quality": {"total_bars": 1},
                "latency_ms": 10.0,
                "cache_hit": False,
                "chunk_count": 1,
                "source": "ibkr",
            },
        )

    request = OHLCVRequest(symbol="SPY", asset_class=AssetClass.EQUITY, exchange="SMART", currency="USD")
    async with httpx.AsyncClient(base_url="http://testserver", transport=httpx.MockTransport(handler)) as client:
        bars = await fetch_chunk(
            client,
            request,
            datetime(2026, 5, 28, 13, 30, tzinfo=timezone.utc),
            datetime(2026, 5, 28, 13, 31, tzinfo=timezone.utc),
        )

    payload = captured["payload"]
    assert captured["url"] == "http://testserver/api/v1/market-data/ohlcv"
    assert isinstance(payload, dict)
    assert payload["persist"] is False
    assert payload["cache_latest"] is False
    assert payload["use_ttl_cache"] is False
    assert bars[0].timestamp == datetime(2026, 5, 28, 13, 30, tzinfo=timezone.utc)
