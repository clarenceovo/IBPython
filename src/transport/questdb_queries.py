"""QuestDB SQL constants and row-mapping helpers.

Extracted from questdb_client.py to keep the client class focused on
connection management and query execution.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.config import config_constant as constants
from src.feeds.models import AssetClass, OHLCVBar, ohlcv_contract_identity
from src.feeds.snapshotter import EquitySnapshot, FXOptionSnapshot


# ── DDL ──────────────────────────────────────────────────────────────────────

CREATE_MARKET_OHLCV_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {constants.QUESTDB_MARKET_OHLCV_TABLE} (
    symbol SYMBOL,
    asset_class SYMBOL,
    exchange SYMBOL,
    currency SYMBOL,
    timestamp TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    bar_size SYMBOL,
    source SYMBOL,
    contract_key SYMBOL,
    con_id LONG,
    local_symbol SYMBOL,
    contract_month SYMBOL,
    expiry SYMBOL,
    strike DOUBLE,
    right SYMBOL,
    trading_class SYMBOL,
    what_to_show SYMBOL,
    use_rth BOOLEAN,
    metadata STRING
) TIMESTAMP(timestamp) PARTITION BY DAY WAL
""".strip()

CREATE_EQUITY_SNAPSHOT_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {constants.EQUITY_SNAPSHOT_TABLE} (
    symbol SYMBOL,
    exchange SYMBOL,
    currency SYMBOL,
    primary_exchange SYMBOL,
    con_id LONG,
    timestamp TIMESTAMP,
    last DOUBLE,
    bid DOUBLE,
    ask DOUBLE,
    bid_size DOUBLE,
    ask_size DOUBLE,
    last_size DOUBLE,
    volume DOUBLE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    vwap DOUBLE,
    mark_price DOUBLE,
    mid_price DOUBLE,
    spread DOUBLE,
    spread_bps DOUBLE,
    halted BOOLEAN,
    source SYMBOL
) TIMESTAMP(timestamp) PARTITION BY DAY WAL
""".strip()

CREATE_FX_OPTION_SNAPSHOT_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {constants.FX_OPTION_SNAPSHOT_TABLE} (
    symbol SYMBOL,
    underlying_symbol SYMBOL,
    expiry SYMBOL,
    strike DOUBLE,
    right SYMBOL,
    exchange SYMBOL,
    currency SYMBOL,
    multiplier SYMBOL,
    trading_class SYMBOL,
    local_symbol SYMBOL,
    con_id LONG,
    timestamp TIMESTAMP,
    last DOUBLE,
    bid DOUBLE,
    ask DOUBLE,
    bid_size DOUBLE,
    ask_size DOUBLE,
    last_size DOUBLE,
    volume DOUBLE,
    mark_price DOUBLE,
    implied_volatility DOUBLE,
    historical_volatility DOUBLE,
    option_volume DOUBLE,
    average_option_volume DOUBLE,
    open_interest DOUBLE,
    call_open_interest DOUBLE,
    put_open_interest DOUBLE,
    call_volume DOUBLE,
    put_volume DOUBLE,
    bid_delta DOUBLE,
    ask_delta DOUBLE,
    last_delta DOUBLE,
    model_delta DOUBLE,
    model_gamma DOUBLE,
    model_theta DOUBLE,
    model_vega DOUBLE,
    mid_price DOUBLE,
    spread DOUBLE,
    spread_bps DOUBLE,
    source SYMBOL
) TIMESTAMP(timestamp) PARTITION BY DAY WAL
""".strip()

# ── DML ──────────────────────────────────────────────────────────────────────

INSERT_MARKET_OHLCV_SQL = f"""
INSERT INTO {constants.QUESTDB_MARKET_OHLCV_TABLE}
(symbol, asset_class, exchange, currency, timestamp, open, high, low, close, volume, bar_size, source,
 contract_key, con_id, local_symbol, contract_month, expiry, strike, right, trading_class, what_to_show, use_rth, metadata)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""".strip()

INSERT_EQUITY_SNAPSHOT_SQL = f"""
INSERT INTO {constants.EQUITY_SNAPSHOT_TABLE}
(symbol, exchange, currency, primary_exchange, con_id, timestamp,
 last, bid, ask, bid_size, ask_size, last_size, volume,
 open, high, low, close, vwap, mark_price,
 mid_price, spread, spread_bps, halted, source)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""".strip()

INSERT_FX_OPTION_SNAPSHOT_SQL = f"""
INSERT INTO {constants.FX_OPTION_SNAPSHOT_TABLE}
(symbol, underlying_symbol, expiry, strike, right, exchange, currency,
 multiplier, trading_class, local_symbol, con_id, timestamp,
 last, bid, ask, bid_size, ask_size, last_size, volume, mark_price,
 implied_volatility, historical_volatility, option_volume, average_option_volume,
 open_interest, call_open_interest, put_open_interest, call_volume, put_volume,
 bid_delta, ask_delta, last_delta, model_delta, model_gamma, model_theta, model_vega,
 mid_price, spread, spread_bps, source)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""".strip()

MARKET_OHLCV_IDENTITY_ALTER_SQL = (
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN contract_key SYMBOL",
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN con_id LONG",
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN local_symbol SYMBOL",
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN contract_month SYMBOL",
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN expiry SYMBOL",
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN strike DOUBLE",
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN right SYMBOL",
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN trading_class SYMBOL",
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN what_to_show SYMBOL",
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN use_rth BOOLEAN",
)


# ── Row mappers ──────────────────────────────────────────────────────────────

def bar_to_row(bar: OHLCVBar) -> tuple[Any, ...]:
    identity = ohlcv_contract_identity(bar)
    return (
        bar.symbol,
        str(bar.asset_class),
        bar.exchange,
        bar.currency,
        bar.timestamp.astimezone(timezone.utc).replace(tzinfo=None),
        bar.open,
        bar.high,
        bar.low,
        bar.close,
        bar.volume,
        bar.bar_size,
        bar.source,
        identity["contract_key"],
        identity["con_id"],
        identity["local_symbol"],
        identity["contract_month"],
        identity["expiry"],
        identity["strike"],
        identity["right"],
        identity["trading_class"],
        identity["what_to_show"],
        identity["use_rth"],
        bar.metadata_json(),
    )


def snapshot_to_row(snapshot: EquitySnapshot) -> tuple[Any, ...]:
    return (
        snapshot.symbol,
        snapshot.exchange,
        snapshot.currency,
        snapshot.primary_exchange,
        snapshot.con_id,
        snapshot.timestamp.astimezone(timezone.utc).replace(tzinfo=None),
        snapshot.last,
        snapshot.bid,
        snapshot.ask,
        snapshot.bid_size,
        snapshot.ask_size,
        snapshot.last_size,
        snapshot.volume,
        snapshot.open,
        snapshot.high,
        snapshot.low,
        snapshot.close,
        snapshot.vwap,
        snapshot.mark_price,
        snapshot.mid_price,
        snapshot.spread,
        snapshot.spread_bps,
        snapshot.halted,
        snapshot.source,
    )


def fx_option_snapshot_to_row(snapshot: FXOptionSnapshot) -> tuple[Any, ...]:
    return (
        snapshot.symbol,
        snapshot.underlying_symbol,
        snapshot.expiry,
        snapshot.strike,
        snapshot.right,
        snapshot.exchange,
        snapshot.currency,
        snapshot.multiplier,
        snapshot.trading_class,
        snapshot.local_symbol,
        snapshot.con_id,
        snapshot.timestamp.astimezone(timezone.utc).replace(tzinfo=None),
        snapshot.last,
        snapshot.bid,
        snapshot.ask,
        snapshot.bid_size,
        snapshot.ask_size,
        snapshot.last_size,
        snapshot.volume,
        snapshot.mark_price,
        snapshot.implied_volatility,
        snapshot.historical_volatility,
        snapshot.option_volume,
        snapshot.average_option_volume,
        snapshot.open_interest,
        snapshot.call_open_interest,
        snapshot.put_open_interest,
        snapshot.call_volume,
        snapshot.put_volume,
        snapshot.bid_greeks.delta if snapshot.bid_greeks else None,
        snapshot.ask_greeks.delta if snapshot.ask_greeks else None,
        snapshot.last_greeks.delta if snapshot.last_greeks else None,
        snapshot.model_greeks.delta if snapshot.model_greeks else None,
        snapshot.model_greeks.gamma if snapshot.model_greeks else None,
        snapshot.model_greeks.theta if snapshot.model_greeks else None,
        snapshot.model_greeks.vega if snapshot.model_greeks else None,
        snapshot.mid_price,
        snapshot.spread,
        snapshot.spread_bps,
        snapshot.source,
    )


# ── Query builders ───────────────────────────────────────────────────────────

def build_historical_query(
    *,
    symbol: str,
    asset_class: AssetClass | str | None = None,
    bar_size: str | None = None,
    contract_key: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 10_000,
) -> tuple[str, list[Any]]:
    clauses = ["symbol = %s"]
    params: list[Any] = [symbol.upper()]
    if asset_class is not None:
        clauses.append("asset_class = %s")
        params.append(str(asset_class))
    if bar_size is not None:
        clauses.append("bar_size = %s")
        params.append(bar_size)
    if contract_key is not None:
        clauses.append("contract_key = %s")
        params.append(contract_key)
    if start is not None:
        clauses.append("timestamp >= %s")
        params.append(_questdb_timestamp(start))
    if end is not None:
        clauses.append("timestamp < %s")
        params.append(_questdb_timestamp(end))
    params.append(limit)
    sql = (
        f"SELECT * FROM {constants.QUESTDB_MARKET_OHLCV_TABLE} "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY timestamp ASC "
        "LIMIT %s"
    )
    return sql, params


def build_latest_query(
    *,
    asset_class: AssetClass | str | None = None,
    bar_size: str | None = None,
    contract_key: str | None = None,
    limit: int = 100,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if asset_class is not None:
        clauses.append("asset_class = %s")
        params.append(str(asset_class))
    if bar_size is not None:
        clauses.append("bar_size = %s")
        params.append(bar_size)
    if contract_key is not None:
        clauses.append("contract_key = %s")
        params.append(contract_key)
    where = f"WHERE {' AND '.join(clauses)} " if clauses else ""
    params.append(limit)
    sql = (
        f"SELECT * FROM {constants.QUESTDB_MARKET_OHLCV_TABLE} "
        f"{where}"
        "LATEST ON timestamp PARTITION BY symbol, contract_key "
        "LIMIT %s"
    )
    return sql, params


def _questdb_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(tzinfo=None)
