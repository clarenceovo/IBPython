"""Market-data MCP tools: live historical bars, volatility/yield series, depth, ticks, histograms, schedules."""

from __future__ import annotations

import asyncio
import logging
import time as _time
from datetime import datetime
from typing import Any

from mcp.server.fastmcp import Context, FastMCP

from src.mcp_app.common import (
    _AssetClassParam,
    _BarSizeParam,
    _MarketDataTypeOptParam,
    _READ_TOOL,
    _RightOptParam,
    _WRITE_TOOL,
    _WhatToShowParam,
    _parse_asset_class,
    _state,
    _tool_error,
)

logger = logging.getLogger(__name__)


async def load_historical_ohlcv_live(
    ctx: Context,
    symbol: str,
    asset_class: _AssetClassParam = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    bar_size: _BarSizeParam = "1 day",
    duration: str = "1 M",
    what_to_show: _WhatToShowParam = "TRADES",
    use_rth: bool = True,
) -> dict[str, Any]:
    """Load historical OHLCV bars directly from IBKR (not QuestDB).

    Use this when you need data not yet persisted, or for real-time snapshots.

    Args:
        symbol: Ticker symbol
        asset_class: CRYPTO, FUTURES, INDEX, FX, EQUITY, BOND (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        bar_size: '1 min', '5 mins', '15 mins', '1 hour', '4 hours', '1 day', '1 week'
        duration: IBKR duration string: '30 D', '1 M', '1 Y', etc
        what_to_show: TRADES, MIDPOINT, BID, ASK, BID_ASK (default 'TRADES')
        use_rth: Use regular trading hours only (default True)
    """
    state = _state(ctx)
    from src.feeds.ibkr_historical import ensure_historical_chunk_limit, plan_historical_auto_chunk
    from src.feeds.models import (
        OHLCVRequest,
        OHLCVRequestMeta,
        OHLCVResponseEnvelope,
        compute_ohlcv_quality,
    )
    from src.transport.metrics import metrics

    try:
        request = OHLCVRequest(
            symbol=symbol,
            asset_class=_parse_asset_class(asset_class),
            exchange=exchange,
            currency=currency,
            bar_size=bar_size,
            duration=duration,
            what_to_show=what_to_show,
            use_rth=use_rth,
        )

        t0 = _time.monotonic()
        auto_chunk_plan = plan_historical_auto_chunk(request)
        if auto_chunk_plan is not None:
            ensure_historical_chunk_limit(request, auto_chunk_plan, max_chunks=state.settings.ibkr_historical_max_chunks)
            metrics.market_data_historical_auto_chunks_total.inc(
                {"asset_class": request.asset_class.value, "operation": "mcp", "status": "planned"}
            )
            logger.info(
                "MCP historical OHLCV auto_chunking symbol=%s bar_size=%s duration=%s estimated_chunks=%d",
                request.symbol,
                request.bar_size,
                request.duration,
                auto_chunk_plan.estimated_chunks,
            )
            range_request = request.model_copy(update={"end_datetime": auto_chunk_plan.end_datetime})
            bars = await state.feed.load_historical_ohlcv_range(
                range_request,
                start_datetime=auto_chunk_plan.start_datetime,
                end_datetime=auto_chunk_plan.end_datetime,
                max_chunks=state.settings.ibkr_historical_max_chunks,
            )
        else:
            bars = await state.feed.load_historical_ohlcv(request, max_chunks=state.settings.ibkr_historical_max_chunks)
        latency_ms = (_time.monotonic() - t0) * 1000.0

        request_meta = OHLCVRequestMeta(
            symbol=request.symbol,
            asset_class=request.asset_class.value,
            exchange=request.exchange,
            currency=request.currency,
            bar_size=request.bar_size,
            what_to_show=request.what_to_show,
            use_rth=request.use_rth,
            duration=request.duration,
        )
        envelope = OHLCVResponseEnvelope(
            bars=bars,
            request=request_meta,
            quality=compute_ohlcv_quality(bars),
            latency_ms=latency_ms,
            cache_hit=False,
            chunk_count=1,
            source="ibkr",
        )
        return envelope.model_dump(mode="json")
    except Exception as e:
        return _tool_error(e)


async def get_latest_ticks(
    ctx: Context,
    symbol: str,
    sec_type: str = "STK",
    exchange: str = "SMART",
    n: int = 100,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Get latest N ticks from in-memory tick buffer.

    Requires an active tick-by-tick subscription (start_tick_by_tick) for the symbol.

    Args:
        symbol: Ticker symbol
        sec_type: Security type (default 'STK')
        exchange: Exchange (default 'SMART')
        n: Number of ticks to return (default 100)
    """
    state = _state(ctx)
    try:
        ticks = state.feed.get_latest_ticks(symbol, sec_type, exchange, n)
        return [t.model_dump(mode="json") for t in ticks]
    except Exception as e:
        return _tool_error(e)


async def load_historical_ticks(
    ctx: Context,
    symbol: str,
    start_date: str,
    end_date: str,
    sec_type: str = "STK",
    exchange: str = "SMART",
    currency: str = "USD",
    what_to_show: _WhatToShowParam = "TRADES",
    use_rth: bool = True,
    max_ticks: int = 10000,
) -> dict[str, Any]:
    """Load historical tick data from IBKR.

    Returns timestamped ticks with price, size, bid/ask, etc.

    Args:
        symbol: Ticker symbol
        start_date: Start datetime ISO format (e.g. '2026-06-01T09:30:00Z')
        end_date: End datetime ISO format (e.g. '2026-06-01T16:00:00Z')
        sec_type: Security type (default 'STK')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        what_to_show: TRADES, BID_ASK, or MIDPOINT (default 'TRADES')
        use_rth: Regular trading hours only (default True)
        max_ticks: Max ticks to return (default 10000)
    """
    state = _state(ctx)
    from src.feeds.tick_data import HistoricalTickRequest

    try:
        request = HistoricalTickRequest(
            symbol=symbol,
            sec_type=sec_type,
            exchange=exchange,
            currency=currency,
            start_date=datetime.fromisoformat(start_date.replace("Z", "+00:00")),  # type: ignore[arg-type]
            end_date=datetime.fromisoformat(end_date.replace("Z", "+00:00")),  # type: ignore[arg-type]
            what_to_show=what_to_show,
            use_rth=use_rth,
            max_ticks=max_ticks,
        )
        response = await state.feed.load_historical_ticks(request)
        return response.model_dump(mode="json")
    except Exception as e:
        return _tool_error(e)


async def get_head_timestamp(
    ctx: Context,
    symbol: str,
    sec_type: str = "STK",
    exchange: str = "SMART",
    currency: str = "USD",
    what_to_show: _WhatToShowParam = "TRADES",
    use_rth: bool = True,
) -> dict[str, Any]:
    """Get earliest available data date for a symbol from IBKR.

    Args:
        symbol: Ticker symbol
        sec_type: Security type (default 'STK')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        what_to_show: Data type (default 'TRADES')
        use_rth: Regular trading hours only (default True)
    """
    state = _state(ctx)
    from src.feeds.tick_data import HeadTimestampRequest

    try:
        request = HeadTimestampRequest(
            symbol=symbol,
            sec_type=sec_type,
            exchange=exchange,
            currency=currency,
            what_to_show=what_to_show,
            use_rth=use_rth,
        )
        ts = await state.feed.load_head_timestamp(request)
        return {
            "symbol": symbol,
            "what_to_show": what_to_show,
            "head_timestamp": ts.isoformat() if ts else None,
        }
    except Exception as e:
        return _tool_error(e)


async def load_bond_yield_history(
    ctx: Context,
    symbol: str,
    exchange: str = "SMART",
    currency: str = "USD",
    duration: str = "1 D",
    bar_size: _BarSizeParam = "1 day",
    use_rth: bool = True,
) -> dict[str, Any] | list[dict[str, Any]]:
    """Load historical bond yields from IBKR.

    Args:
        symbol: Bond symbol, CUSIP, or identifier (e.g. '91282CJN2')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        duration: IBKR duration string (default '1 D')
        bar_size: Bar size (default '1 day')
        use_rth: Regular trading hours only (default True)
    """
    state = _state(ctx)
    from src.feeds.bonds import BondInstrument, BondYieldHistoryRequest

    try:
        bond = BondInstrument(symbol=symbol, exchange=exchange, currency=currency)
        request = BondYieldHistoryRequest(
            bond=bond,
            duration=duration,
            bar_size=bar_size,
            use_rth=use_rth,
        )
        bars = await state.feed.load_bond_yield_history(request)
        return [b.model_dump(mode="json") for b in bars]
    except Exception as e:
        return _tool_error(e)


async def get_market_depth(
    ctx: Context,
    symbol: str,
    asset_class: _AssetClassParam = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    num_rows: int = 5,
    sec_type: str = "STK",
    contract_month: str | None = None,
    strike: float | None = None,
    right: _RightOptParam = None,
) -> dict[str, Any]:
    """Load order book depth (Level 2 DOM snapshot) from IBKR.

    Args:
        symbol: Ticker symbol
        asset_class: EQUITY, FX, INDEX, or FUTURE (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        num_rows: Depth levels per side, 1-5 (default 5)
        sec_type: IBKR secType override (default 'STK')
        contract_month: Futures contract month (required for futures)
        strike: Strike price (for options)
        right: Option right 'C'/'P' (for options)
    """
    state = _state(ctx)
    from src.feeds.contracts import ContractSpec

    try:
        spec = ContractSpec(
            symbol=symbol,
            asset_class=_parse_asset_class(asset_class),
            exchange=exchange,
            currency=currency,
            last_trade_date_or_contract_month=contract_month,
            strike=strike,
            right=right,
        )
        snapshot = await state.feed.load_market_depth_snapshot(spec, num_rows=num_rows)
        return snapshot.model_dump(mode="json")
    except Exception as e:
        return _tool_error(e)


async def request_histogram(
    ctx: Context,
    symbol: str,
    asset_class: _AssetClassParam = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    use_rth: bool = True,
    time_period: str = "1 day",
) -> dict[str, Any]:
    """Request price histogram data from IBKR.

    Returns price distribution for execution analysis and microstructure research.

    Args:
        symbol: Ticker symbol
        asset_class: EQUITY, FUTURE, FX, INDEX, BOND, OPTION (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        use_rth: Use regular trading hours only (default True)
        time_period: IBKR time period string (e.g. '1 day', '1 week')
    """
    state = _state(ctx)
    try:
        buckets_raw = await state.feed.request_histogram(
            symbol=symbol,
            asset_class=asset_class,
            exchange=exchange,
            currency=currency,
            use_rth=use_rth,
            time_period=time_period,
        )
        buckets = [
            {"price": b["price"], "count": b["count"]} if isinstance(b, dict) else b
            for b in buckets_raw
        ]
        return {
            "symbol": symbol,
            "asset_class": asset_class,
            "exchange": exchange,
            "currency": currency,
            "time_period": time_period,
            "use_rth": use_rth,
            "buckets": buckets,
            "total_count": len(buckets),
        }
    except Exception as e:
        return _tool_error(e)


async def subscribe_realtime_bars(
    ctx: Context,
    symbol: str,
    asset_class: _AssetClassParam = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    what_to_show: _WhatToShowParam = "TRADES",
    use_rth: bool = True,
    duration_seconds: float = 60.0,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Subscribe to 5-second real-time bars from IBKR.

    Returns collected 5-second bars over the specified duration.
    Use for intraday signal generation and execution quality analysis.

    Args:
        symbol: Ticker symbol
        asset_class: EQUITY, FUTURE, FX, INDEX, BOND, OPTION
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        what_to_show: TRADES, MIDPOINT, BID, ASK, BID_ASK (default 'TRADES')
        use_rth: Regular trading hours only (default True)
        duration_seconds: How long to collect bars in seconds (default 60, max 300)
    """
    state = _state(ctx)
    duration_seconds = min(max(duration_seconds, 1.0), 300.0)
    bars: list[dict[str, Any]] = []
    try:
        bar_stream = await state.feed.subscribe_realtime_bars(
            symbol=symbol,
            asset_class=asset_class,
            exchange=exchange,
            currency=currency,
            what_to_show=what_to_show,
            use_rth=use_rth,
        )
        async with asyncio.timeout(duration_seconds):
            async for raw_bar in bar_stream:
                bar = {
                    "symbol": symbol,
                    "timestamp": raw_bar.get("time") if isinstance(raw_bar, dict) else getattr(raw_bar, "time", None),
                    "open": raw_bar.get("open", 0) if isinstance(raw_bar, dict) else getattr(raw_bar, "open", 0),
                    "high": raw_bar.get("high", 0) if isinstance(raw_bar, dict) else getattr(raw_bar, "high", 0),
                    "low": raw_bar.get("low", 0) if isinstance(raw_bar, dict) else getattr(raw_bar, "low", 0),
                    "close": raw_bar.get("close", 0) if isinstance(raw_bar, dict) else getattr(raw_bar, "close", 0),
                    "volume": raw_bar.get("volume", 0) if isinstance(raw_bar, dict) else getattr(raw_bar, "volume", 0),
                    "vwap": raw_bar.get("wap", 0) if isinstance(raw_bar, dict) else getattr(raw_bar, "wap", 0),
                    "trade_count": raw_bar.get("count", 0) if isinstance(raw_bar, dict) else getattr(raw_bar, "count", 0),
                }
                bars.append(bar)
        return bars
    except TimeoutError:
        return bars if bars else [{"message": f"collected {len(bars)} bars in {duration_seconds}s"}]
    except Exception as e:
        return _tool_error(e)


async def get_depth_exchanges(ctx: Context) -> list[dict[str, Any]] | dict[str, Any]:
    """Get available Level 2 market depth exchanges from IBKR."""
    state = _state(ctx)
    try:
        return await state.feed.get_depth_exchanges()
    except Exception as e:
        return _tool_error(e)


async def set_market_data_type(
    ctx: Context,
    market_data_type: _MarketDataTypeOptParam = 1,
) -> dict[str, Any]:
    """Switch IBKR market data type.

    1=Live, 2=Frozen, 3=Delayed(15min), 4=DelayedFrozen.
    Essential for pre/post-market data and when live subscriptions are unavailable.

    Args:
        market_data_type: 1=Live, 2=Frozen, 3=Delayed, 4=DelayedFrozen (default 1)
    """
    state = _state(ctx)
    try:
        return await state.feed.set_market_data_type(market_data_type)
    except Exception as e:
        return _tool_error(e)


async def get_historical_volatility(
    ctx: Context,
    symbol: str,
    asset_class: _AssetClassParam = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    bar_size: _BarSizeParam = "1 day",
    duration: str = "1 Y",
    use_rth: bool = True,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Get historical volatility time series from IBKR.

    Uses IBKR's built-in HISTORICAL_VOLATILITY whatToShow — no manual computation needed.

    Args:
        symbol: Ticker symbol
        asset_class: EQUITY, FUTURE, INDEX, etc (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        bar_size: '1 min', '5 mins', '1 hour', '1 day' (default '1 day')
        duration: IBKR duration string (default '1 Y')
        use_rth: Regular trading hours only (default True)
    """
    state = _state(ctx)
    from src.feeds.ibkr_historical import ensure_historical_chunk_limit, plan_historical_auto_chunk
    from src.feeds.models import OHLCVRequest, normalize_bar_size

    try:
        request = OHLCVRequest(
            symbol=symbol,
            asset_class=_parse_asset_class(asset_class),
            exchange=exchange,
            currency=currency,
            bar_size=normalize_bar_size(bar_size),
            duration=duration,
            what_to_show="HISTORICAL_VOLATILITY",
            use_rth=use_rth,
        )
        auto_chunk_plan = plan_historical_auto_chunk(request)
        if auto_chunk_plan is not None:
            ensure_historical_chunk_limit(request, auto_chunk_plan, max_chunks=state.settings.ibkr_historical_max_chunks)
            bars = await state.feed.load_historical_ohlcv_range(
                request.model_copy(update={"end_datetime": auto_chunk_plan.end_datetime}),
                start_datetime=auto_chunk_plan.start_datetime,
                end_datetime=auto_chunk_plan.end_datetime,
                max_chunks=state.settings.ibkr_historical_max_chunks,
            )
        else:
            bars = await state.feed.load_historical_ohlcv(request, max_chunks=state.settings.ibkr_historical_max_chunks)
        return [
            {
                "timestamp": bar.timestamp.isoformat(),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "volatility": bar.vwap,
            }
            for bar in bars
        ]
    except Exception as e:
        return _tool_error(e)


async def get_option_implied_volatility_series(
    ctx: Context,
    symbol: str,
    asset_class: _AssetClassParam = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    bar_size: _BarSizeParam = "1 day",
    duration: str = "6 M",
    use_rth: bool = True,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Get option implied volatility time series from IBKR.

    Uses IBKR's built-in OPTION_IMPLIED_VOLATILITY whatToShow for the underlying.

    Args:
        symbol: Underlying ticker symbol
        asset_class: EQUITY, FUTURE, INDEX (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        bar_size: Bar size (default '1 day')
        duration: IBKR duration string (default '6 M')
        use_rth: Regular trading hours only (default True)
    """
    state = _state(ctx)
    from src.feeds.ibkr_historical import ensure_historical_chunk_limit, plan_historical_auto_chunk
    from src.feeds.models import OHLCVRequest, normalize_bar_size

    try:
        request = OHLCVRequest(
            symbol=symbol,
            asset_class=_parse_asset_class(asset_class),
            exchange=exchange,
            currency=currency,
            bar_size=normalize_bar_size(bar_size),
            duration=duration,
            what_to_show="OPTION_IMPLIED_VOLATILITY",
            use_rth=use_rth,
        )
        auto_chunk_plan = plan_historical_auto_chunk(request)
        if auto_chunk_plan is not None:
            ensure_historical_chunk_limit(request, auto_chunk_plan, max_chunks=state.settings.ibkr_historical_max_chunks)
            bars = await state.feed.load_historical_ohlcv_range(
                request.model_copy(update={"end_datetime": auto_chunk_plan.end_datetime}),
                start_datetime=auto_chunk_plan.start_datetime,
                end_datetime=auto_chunk_plan.end_datetime,
                max_chunks=state.settings.ibkr_historical_max_chunks,
            )
        else:
            bars = await state.feed.load_historical_ohlcv(request, max_chunks=state.settings.ibkr_historical_max_chunks)
        return [
            {
                "timestamp": bar.timestamp.isoformat(),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "implied_volatility": bar.vwap,
            }
            for bar in bars
        ]
    except Exception as e:
        return _tool_error(e)


async def get_yield_data(
    ctx: Context,
    symbol: str,
    what_to_show: _WhatToShowParam = "YIELD_LAST",
    bar_size: _BarSizeParam = "1 day",
    duration: str = "1 Y",
    exchange: str = "SMART",
    currency: str = "USD",
    use_rth: bool = True,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Get bond yield time series from IBKR.

    Uses IBKR yield-specific whatToShow values.

    Args:
        symbol: Bond symbol or CUSIP
        what_to_show: YIELD_ASK, YIELD_BID, YIELD_BID_ASK, YIELD_LAST (default 'YIELD_LAST')
        bar_size: Bar size (default '1 day')
        duration: Duration string (default '1 Y')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        use_rth: Regular trading hours (default True)
    """
    state = _state(ctx)
    from src.feeds.ibkr_historical import ensure_historical_chunk_limit, plan_historical_auto_chunk
    from src.feeds.models import OHLCVRequest, normalize_bar_size

    try:
        request = OHLCVRequest(
            symbol=symbol,
            asset_class=_parse_asset_class("BOND"),
            exchange=exchange,
            currency=currency,
            bar_size=normalize_bar_size(bar_size),
            duration=duration,
            what_to_show=what_to_show,
            use_rth=use_rth,
        )
        auto_chunk_plan = plan_historical_auto_chunk(request)
        if auto_chunk_plan is not None:
            ensure_historical_chunk_limit(request, auto_chunk_plan, max_chunks=state.settings.ibkr_historical_max_chunks)
            bars = await state.feed.load_historical_ohlcv_range(
                request.model_copy(update={"end_datetime": auto_chunk_plan.end_datetime}),
                start_datetime=auto_chunk_plan.start_datetime,
                end_datetime=auto_chunk_plan.end_datetime,
                max_chunks=state.settings.ibkr_historical_max_chunks,
            )
        else:
            bars = await state.feed.load_historical_ohlcv(request, max_chunks=state.settings.ibkr_historical_max_chunks)
        return [
            {
                "timestamp": bar.timestamp.isoformat(),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "average_yield": bar.vwap,
            }
            for bar in bars
        ]
    except Exception as e:
        return _tool_error(e)


async def get_trading_schedule(
    ctx: Context,
    symbol: str,
    asset_class: _AssetClassParam = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    end_date: str = "",
    num_days: int = 7,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Get trading schedule for any instrument from IBKR.

    Returns session open/close times, overnight flags, and trading status.

    Args:
        symbol: Ticker symbol
        asset_class: Asset class (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        end_date: End date YYYYMMDD (default today)
        num_days: Number of days to return (default 7)
    """
    state = _state(ctx)
    from datetime import date as date_type
    from src.feeds.models import OHLCVRequest

    try:
        ref_date = date_type.fromisoformat(end_date) if end_date else date_type.today()
        request = OHLCVRequest(
            symbol=symbol,
            asset_class=_parse_asset_class(asset_class),
            exchange=exchange,
            currency=currency,
            bar_size="1 day",
            duration=f"{num_days} D",
            what_to_show="SCHEDULE",
            use_rth=True,
        )
        schedule = await state.feed.load_trading_schedule(request, ref_date=ref_date, use_rth=True)
        if isinstance(schedule, (list, tuple)):
            return [
                item.model_dump(mode="json") if hasattr(item, "model_dump") else item
                for item in schedule
            ]
        return [schedule.model_dump(mode="json")] if hasattr(schedule, "model_dump") else [schedule]
    except Exception as e:
        return _tool_error(e)


def register(mcp: FastMCP) -> None:
    for fn in (
        load_historical_ohlcv_live,
        get_latest_ticks,
        load_historical_ticks,
        get_head_timestamp,
        load_bond_yield_history,
        get_market_depth,
        request_histogram,
        get_depth_exchanges,
        get_historical_volatility,
        get_option_implied_volatility_series,
        get_yield_data,
        get_trading_schedule,
    ):
        mcp.tool(annotations=_READ_TOOL)(fn)
    for fn in (subscribe_realtime_bars, set_market_data_type):
        mcp.tool(annotations=_WRITE_TOOL)(fn)
