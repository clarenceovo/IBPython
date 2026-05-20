from datetime import datetime, timezone

from src.feeds.models import AssetClass, FutureOHLCVBar, OHLCVBar, OptionOHLCVBar
from src.feeds.snapshotter import FXOptionSnapshot
from src.transport.market_data_store import MarketOHLCVStore
from src.transport.questdb_client import (
    CREATE_FX_OPTION_SNAPSHOT_TABLE_SQL,
    CREATE_MARKET_OHLCV_TABLE_SQL,
    INSERT_MARKET_OHLCV_SQL,
    QuestDBClient,
    bar_to_row,
    build_historical_query,
    build_latest_query,
    fx_option_snapshot_to_row,
)


def test_create_table_sql_uses_partitioned_market_table() -> None:
    assert "CREATE TABLE IF NOT EXISTS EquityOHLCV" in CREATE_MARKET_OHLCV_TABLE_SQL
    assert "TIMESTAMP(timestamp) PARTITION BY DAY" in CREATE_MARKET_OHLCV_TABLE_SQL
    assert "CREATE TABLE IF NOT EXISTS fx_option_snapshot" in CREATE_FX_OPTION_SNAPSHOT_TABLE_SQL


def test_insert_sql_targets_market_table() -> None:
    assert INSERT_MARKET_OHLCV_SQL.startswith("INSERT INTO EquityOHLCV")
    assert "VALUES (%s" in INSERT_MARKET_OHLCV_SQL


def test_bar_to_row_serializes_metadata() -> None:
    bar = OHLCVBar(
        symbol="SPY",
        asset_class="equity",
        exchange="SMART",
        currency="USD",
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        open=1,
        high=2,
        low=1,
        close=1.5,
        volume=100,
        bar_size="1 min",
        metadata={"a": 1},
    )

    row = bar_to_row(bar)

    assert row[0] == "SPY"
    assert row[1] == "equity"
    assert row[4] == datetime(2026, 1, 1)
    assert row[12] == "EQUITY:SPY:SMART:USD"
    assert row[-1] == '{"a": 1}'


def test_bar_to_row_preserves_future_contract_identity() -> None:
    bar = FutureOHLCVBar(
        symbol="CL",
        exchange="NYMEX",
        currency="USD",
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        open=70,
        high=71,
        low=69,
        close=70.5,
        volume=100,
        bar_size="1 min",
        contract_month="202606",
        metadata={"what_to_show": "TRADES", "use_rth": False},
    )

    row = bar_to_row(bar)

    assert row[12] == "FUTURE:CL:202606:NYMEX:USD"
    assert row[15] == "202606"
    assert row[20] == "TRADES"
    assert row[21] is False


def test_bar_to_row_preserves_option_contract_identity() -> None:
    bar = OptionOHLCVBar(
        symbol="CL",
        exchange="NYMEX",
        currency="USD",
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        open=1,
        high=2,
        low=1,
        close=1.5,
        volume=10,
        bar_size="1 min",
        underlying_symbol="CL",
        expiry="20260617",
        strike=75,
        right="CALL",
        trading_class="CL",
    )

    row = bar_to_row(bar)

    assert row[12] == "OPTION:CL:20260617:75:C:NYMEX:USD:CL"
    assert row[16] == "20260617"
    assert row[17] == 75.0
    assert row[18] == "C"


def test_fx_option_snapshot_to_row_serializes_contract_identity() -> None:
    snapshot = FXOptionSnapshot(
        symbol="EURUSD",
        underlying_symbol="EUR",
        expiry="20260619",
        strike=1.1,
        right="C",
        exchange="SMART",
        currency="USD",
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        bid=0.01,
        ask=0.012,
    )

    row = fx_option_snapshot_to_row(snapshot)

    assert row[0] == "EURUSD"
    assert row[1] == "EUR"
    assert row[2] == "20260619"
    assert row[3] == 1.1
    assert row[4] == "C"


def test_build_historical_query_is_parameterized() -> None:
    sql, params = build_historical_query(
        symbol="spy",
        asset_class=AssetClass.EQUITY,
        bar_size="1 min",
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        limit=500,
    )

    assert "symbol = %s" in sql
    assert "asset_class = %s" in sql
    assert params[0] == "SPY"
    assert params[1] == "equity"
    assert params[-1] == 500


def test_build_latest_query_uses_latest_on() -> None:
    sql, params = build_latest_query(asset_class="future", bar_size="5 mins", limit=10)

    assert "LATEST ON timestamp PARTITION BY symbol, contract_key" in sql
    assert params == ["future", "5 mins", 10]


def test_build_historical_query_can_filter_contract_key() -> None:
    sql, params = build_historical_query(symbol="cl", contract_key="FUTURE:CL:202606:NYMEX:USD", limit=50)

    assert "contract_key = %s" in sql
    assert params == ["CL", "FUTURE:CL:202606:NYMEX:USD", 50]


def test_questdb_client_implements_market_ohlcv_store_interface() -> None:
    client = QuestDBClient(connection=object())

    assert isinstance(client, MarketOHLCVStore)
