from datetime import datetime, timezone

from src.feeds.models import AssetClass, OHLCVBar
from src.transport.market_data_store import MarketOHLCVStore
from src.transport.questdb_client import (
    CREATE_MARKET_OHLCV_TABLE_SQL,
    INSERT_MARKET_OHLCV_SQL,
    QuestDBClient,
    bar_to_row,
    build_historical_query,
    build_latest_query,
)


def test_create_table_sql_uses_partitioned_market_table() -> None:
    assert "CREATE TABLE IF NOT EXISTS market_ohlcv" in CREATE_MARKET_OHLCV_TABLE_SQL
    assert "TIMESTAMP(timestamp) PARTITION BY DAY" in CREATE_MARKET_OHLCV_TABLE_SQL


def test_insert_sql_targets_market_table() -> None:
    assert INSERT_MARKET_OHLCV_SQL.startswith("INSERT INTO market_ohlcv")
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
    assert row[-1] == '{"a": 1}'


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

    assert "LATEST ON timestamp PARTITION BY symbol" in sql
    assert params == ["future", "5 mins", 10]


def test_questdb_client_implements_market_ohlcv_store_interface() -> None:
    client = QuestDBClient(connection=object())

    assert isinstance(client, MarketOHLCVStore)
