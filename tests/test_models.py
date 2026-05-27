from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.feeds.ibkr_feed import normalize_ibkr_bars
from src.feeds.models import AssetClass, BaseOHLCVBar, FXOHLCVBar, FutureOHLCVBar, OHLCVBar, OHLCVRequest, OptionOHLCVBar
from src.feeds.snapshotter import fx_pair_parts


class RawBar:
    date = "20260102 14:30:00 UTC"
    open = 100
    high = 101
    low = 99
    close = 100.5
    volume = 123


def test_base_ohlcv_bar_contains_shared_ohlcv_symbol_and_timestamp() -> None:
    bar = BaseOHLCVBar(
        symbol="spy",
        timestamp="2026-01-02T09:30:00-05:00",
        open=100,
        high=101,
        low=99,
        close=100.5,
        volume=123,
    )

    assert bar.symbol == "SPY"
    assert bar.timestamp == datetime(2026, 1, 2, 14, 30, tzinfo=timezone.utc)
    assert bar.close == 100.5


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


def test_future_ohlcv_bar_extends_ohlcv_with_contract_identity() -> None:
    bar = FutureOHLCVBar(
        symbol="hsi",
        exchange="hkfe",
        currency="hkd",
        timestamp="2026-01-02T09:30:00+08:00",
        open=20000,
        high=20100,
        low=19900,
        close=20050,
        volume=2500,
        bar_size="1 min",
        contract_month="202606",
        is_continuous=False,
    )

    assert isinstance(bar, OHLCVBar)
    assert bar.asset_class is AssetClass.FUTURE
    assert bar.symbol == "HSI"
    assert bar.exchange == "HKFE"
    assert bar.currency == "HKD"
    assert bar.contract_month == "202606"
    assert bar.is_continuous is False


def test_future_ohlcv_bar_rejects_non_future_asset_class() -> None:
    with pytest.raises(ValidationError, match="asset_class=future"):
        FutureOHLCVBar(
            symbol="SPY",
            asset_class="equity",
            exchange="SMART",
            currency="USD",
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            open=100,
            high=101,
            low=99,
            close=100.5,
            volume=100,
            bar_size="1 min",
        )


def test_fx_ohlcv_bar_extends_ohlcv_with_pair_identity() -> None:
    bar = FXOHLCVBar(
        symbol="eurusd",
        exchange="idealpro",
        currency="usd",
        timestamp="2026-01-02T14:30:00Z",
        open=1.10,
        high=1.12,
        low=1.09,
        close=1.11,
        volume=0,
        bar_size="1 hour",
    )

    assert isinstance(bar, OHLCVBar)
    assert bar.asset_class is AssetClass.FX
    assert bar.base_currency == "EUR"
    assert bar.quote_currency == "USD"


def test_option_ohlcv_bar_extends_ohlcv_with_contract_identity() -> None:
    bar = OptionOHLCVBar(
        symbol="TSLA  260619C00250000",
        exchange="SMART",
        currency="USD",
        timestamp="2026-01-02T14:30:00Z",
        open=12.0,
        high=13.5,
        low=11.5,
        close=12.5,
        volume=100,
        bar_size="1 day",
        underlying_symbol="tsla",
        expiry="20260619",
        strike=250,
        right="call",
        multiplier="100",
        trading_class="TSLA",
        con_id=123456789,
    )

    assert isinstance(bar, OHLCVBar)
    assert bar.asset_class is AssetClass.OPTION
    assert bar.underlying_symbol == "TSLA"
    assert bar.expiry == "20260619"
    assert bar.right == "C"
    assert bar.contract_month == "202606"


def test_option_ohlcv_bar_rejects_invalid_right() -> None:
    with pytest.raises(ValidationError, match="right must be C/CALL or P/PUT"):
        OptionOHLCVBar(
            symbol="BADOPT",
            exchange="SMART",
            currency="USD",
            timestamp="2026-01-02T14:30:00Z",
            open=1,
            high=1,
            low=1,
            close=1,
            volume=0,
            bar_size="1 day",
            underlying_symbol="TSLA",
            expiry="20260619",
            strike=250,
            right="X",
        )


def test_ibkr_future_bar_normalization_returns_future_ohlcv_dto() -> None:
    request = OHLCVRequest(
        symbol="HSI",
        asset_class="future",
        exchange="HKFE",
        currency="HKD",
        last_trade_date_or_contract_month="202606",
        bar_size="1 min",
    )

    bars = normalize_ibkr_bars([RawBar()], request)

    assert isinstance(bars[0], FutureOHLCVBar)
    assert bars[0].contract_month == "202606"
    assert bars[0].is_continuous is False


def test_ibkr_option_bar_normalization_returns_option_ohlcv_dto_for_fop() -> None:
    request = OHLCVRequest(
        symbol="CL 20260617C80",
        asset_class="option",
        exchange="NYMEX",
        currency="USD",
        option_sec_type="FOP",
        underlying_symbol="CL",
        expiry="20260617",
        strike=80,
        right="call",
        multiplier="1000",
        bar_size="1 day",
    )

    bars = normalize_ibkr_bars([RawBar()], request)

    assert isinstance(bars[0], OptionOHLCVBar)
    assert bars[0].underlying_symbol == "CL"
    assert bars[0].expiry == "20260617"
    assert bars[0].right == "C"
    assert bars[0].contract_month == "202606"


def test_ibkr_option_bar_normalization_returns_option_ohlcv_dto_for_fx_option() -> None:
    request = OHLCVRequest(
        symbol="EURUSD 20260619C1.1",
        asset_class="option",
        exchange="SMART",
        currency="USD",
        option_sec_type="OPT",
        underlying_symbol="EUR",
        expiry="20260619",
        strike=1.10,
        right="call",
        multiplier="100",
        bar_size="1 day",
        metadata={"pair": "EURUSD"},
    )

    bars = normalize_ibkr_bars([RawBar()], request)

    assert isinstance(bars[0], OptionOHLCVBar)
    assert bars[0].underlying_symbol == "EUR"
    assert bars[0].currency == "USD"
    assert bars[0].right == "C"


def test_fx_pair_parts_splits_pair_and_allows_quote_override() -> None:
    assert fx_pair_parts("eurusd") == ("EURUSD", "EUR", "USD")
    assert fx_pair_parts("EUR/USD", "usd") == ("EURUSD", "EUR", "USD")


def test_ibkr_fx_bar_normalization_returns_fx_ohlcv_dto() -> None:
    request = OHLCVRequest(
        symbol="EURUSD",
        asset_class="fx",
        exchange="IDEALPRO",
        currency="USD",
        bar_size="1 hour",
    )

    bars = normalize_ibkr_bars([RawBar()], request)

    assert isinstance(bars[0], FXOHLCVBar)
    assert bars[0].base_currency == "EUR"
    assert bars[0].quote_currency == "USD"


def test_ibkr_fx_bar_normalization_clamps_unavailable_negative_volume() -> None:
    class RawFXMidpointBar(RawBar):
        volume = -1.0

    request = OHLCVRequest(
        symbol="USDSEK",
        asset_class="fx",
        exchange="IDEALPRO",
        currency="SEK",
        bar_size="1 min",
        what_to_show="MIDPOINT",
        use_rth=False,
    )

    bars = normalize_ibkr_bars([RawFXMidpointBar()], request)

    assert isinstance(bars[0], FXOHLCVBar)
    assert bars[0].volume == 0


def test_ohlcv_request_normalizes_end_datetime() -> None:
    request = OHLCVRequest(
        symbol="aapl",
        asset_class="equity",
        start_datetime="2026-01-02T09:30:00-05:00",
        end_datetime="2026-01-02T16:00:00-05:00",
    )

    assert request.symbol == "AAPL"
    assert request.start_datetime == datetime(2026, 1, 2, 14, 30, tzinfo=timezone.utc)
    assert request.end_datetime == datetime(2026, 1, 2, 21, 0, tzinfo=timezone.utc)


def test_ohlcv_request_rejects_invalid_datetime_range() -> None:
    with pytest.raises(ValidationError, match="start_datetime must be before end_datetime"):
        OHLCVRequest(
            symbol="SPY",
            asset_class="equity",
            start_datetime="2026-01-02T16:00:00Z",
            end_datetime="2026-01-02T09:30:00Z",
        )


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


def test_option_ohlcv_request_requires_option_identity() -> None:
    with pytest.raises(ValidationError, match="option OHLCV requests require"):
        OHLCVRequest(symbol="CL", asset_class="option", exchange="NYMEX", currency="USD", option_sec_type="FOP")
