from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest


def test_ohlcv_bar_normalizes_tokens_and_timestamp_to_utc() -> None:
    bar = OHLCVBar(
        symbol="spy",
        asset_class=AssetClass.EQUITY,
        exchange="smart",
        currency="usd",
        timestamp="2026-01-02T09:30:00-05:00",
        open=100,
        high=101,
        low=99,
        close=100.5,
        volume=123,
        bar_size="1 min",
    )

    assert bar.symbol == "SPY"
    assert bar.exchange == "SMART"
    assert bar.currency == "USD"
    assert bar.timestamp == datetime(2026, 1, 2, 14, 30, tzinfo=timezone.utc)


def test_ohlcv_bar_rejects_invalid_price_range() -> None:
    with pytest.raises(ValidationError):
        OHLCVBar(
            symbol="SPY",
            asset_class="equity",
            exchange="SMART",
            currency="USD",
            timestamp=datetime.now(timezone.utc),
            open=100,
            high=99,
            low=98,
            close=98.5,
            volume=100,
            bar_size="1 min",
        )


def test_ohlcv_bar_round_trips_redis_json() -> None:
    bar = OHLCVBar(
        symbol="EURUSD",
        asset_class="fx",
        exchange="IDEALPRO",
        currency="USD",
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        open=1.1,
        high=1.2,
        low=1.0,
        close=1.15,
        volume=0,
        bar_size="1 hour",
        metadata={"provider_bar_count": 1},
    )

    assert OHLCVBar.from_redis_json(bar.to_redis_json()) == bar


def test_ohlcv_request_normalizes_end_datetime() -> None:
    request = OHLCVRequest(
        symbol="aapl",
        asset_class="equity",
        end_datetime="2026-01-02T16:00:00-05:00",
    )

    assert request.symbol == "AAPL"
    assert request.end_datetime == datetime(2026, 1, 2, 21, 0, tzinfo=timezone.utc)


def test_ohlcv_request_accepts_contract_disambiguators() -> None:
    request = OHLCVRequest(
        symbol="es",
        asset_class="future",
        exchange="cme",
        currency="usd",
        last_trade_date_or_contract_month="202606",
        multiplier="50",
        local_symbol="esm6",
    )

    assert request.symbol == "ES"
    assert request.exchange == "CME"
    assert request.last_trade_date_or_contract_month == "202606"
    assert request.local_symbol == "ESM6"


def test_future_ohlcv_request_requires_contract_identifier() -> None:
    with pytest.raises(ValidationError):
        OHLCVRequest(symbol="ES", asset_class="future", exchange="CME", currency="USD")
