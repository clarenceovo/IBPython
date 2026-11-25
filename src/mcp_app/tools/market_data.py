"""Market-data MCP tools: live historical bars, volatility/yield series, depth, ticks, histograms, schedules."""

from __future__ import annotations

import asyncio
import logging
import time as _time
from datetime import datetime
from typing import Any

from mcp.server.fastmcp import Context, FastMCP

from src.mcp_app.common import (
    MCP_DEFAULT_BAR_LIMIT,
    MCP_DEFAULT_TICK_LIMIT,
    MCP_MAX_BAR_LIMIT,
    MCP_MAX_TICK_LIMIT,
    _AssetClassParam,
    _BarSizeParam,
    _MarketDataTypeOptParam,
    _READ_TOOL,
    _RightOptParam,
    _WRITE_TOOL,
    _WhatToShowParam,
    _clamp,
    _parse_asset_class,
    _state,
    _tool_error,
    _truncate_items,
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
    max_bars: int = MCP_DEFAULT_BAR_LIMIT,
) -> dict[str, Any]:
    """Load historical OHLCV bars directly from IBKR (not QuestDB).

    Use this when you need data not yet persisted, or for real-time snapshots.
    When more than ``max_bars`` arrive, keeps the most recent ``max_bars`` bars
    and sets ``truncated=True`` with ``total_count`` reflecting the full load.

    Args:
        symbol: Ticker symbol
        asset_class: CRYPTO, FUTURES, INDEX, FX, EQUITY, BOND (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        bar_size: '1 min', '5 mins', '15 mins', '1 hour', '4 hours', '1 day', '1 week'
        duration: IBKR duration string: '30 D', '1 M', '1 Y', etc
        what_to_show: TRADES, MIDPOINT, BID, ASK, BID_ASK (default 'TRADES')
        use_rth: Use regular trading hours only (default True)
        max_bars: Max most-recent bars returned (default 500, max 5000)
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
        max_bars = _clamp(max_bars, low=1, high=MCP_MAX_BAR_LIMIT)
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

        total_bars = len(bars)
        truncated = total_bars > max_bars
        kept_bars = bars[-max_bars:] if truncated else bars

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
            bars=kept_bars,
            request=request_meta,
            quality=compute_ohlcv_quality(bars),
            latency_ms=latency_ms,
            cache_hit=False,
            chunk_count=1,
            source="ibkr",
        )
        payload = envelope.model_dump(mode="json")
        payload["returned_count"] = len(kept_bars)
        payload["total_count"] = total_bars
        payload["truncated"] = truncated
        return payload
    except Exception as e:
        return _tool_error(e)


async def get_latest_ticks(
    ctx: Context,
    symbol: str,
    sec_type: str = "STK",
    exchange: str = "SMART",
    n: int = 100,
) -> dict[str, Any]:
    """Get latest N ticks from in-memory tick buffer.

    Requires an active tick-by-tick subscription (start_tick_by_tick) for the symbol.

    Args:
        symbol: Ticker symbol
        sec_type: Security type (default 'STK')
        exchange: Exchange (default 'SMART')
        n: Number of ticks to return (default 100, max 1000)
    """
    state = _state(ctx)
    try:
        n = _clamp(n, low=1, high=1000)
        ticks = state.feed.get_latest_ticks(symbol, sec_type, exchange, n + 1)
        has_more = len(ticks) > n
        kept = ticks[-n:] if has_more else ticks
        serialized = [t.model_dump(mode="json") for t in kept]
        return {
            "symbol": symbol,
            "sec_type": sec_type,
            "exchange": exchange,
            "ticks": serialized,
            "returned_count": len(serialized),
            "total_count": len(ticks),
            "truncated": has_more,
        }
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
    max_ticks: int = MCP_DEFAULT_TICK_LIMIT,
) -> dict[str, Any]:
    """Load historical tick data from IBKR with bounded output.

    Returns timestamped ticks alongside ``returned_count``, ``total_count``,
    and ``truncated`` (True when IBKR or ``max_ticks`` caps the window).

    Args:
        symbol: Ticker symbol
        start_date: Start datetime ISO format (e.g. '2026-06-01T09:30:00Z')
        end_date: End datetime ISO format (e.g. '2026-06-01T16:00:00Z')
        sec_type: Security type (default 'STK')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        what_to_show: TRADES, BID_ASK, or MIDPOINT (default 'TRADES')
        use_rth: Regular trading hours only (default True)
        max_ticks: Max ticks to return (default 500, max 5000)
    """
    state = _state(ctx)
    from src.feeds.tick_data import HistoricalTickRequest

    try:
        max_ticks = _clamp(max_ticks, low=1, high=MCP_MAX_TICK_LIMIT)
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
        serialized = [t.model_dump(mode="json") for t in response.ticks]
        return _truncate_items(
            serialized,
            max_ticks,
            key="ticks",
            upstream_truncated=response.truncated,
            total_count=response.total_count,
            extra={"symbol": response.symbol},
        )
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
    max_bars: int = MCP_DEFAULT_BAR_LIMIT,
) -> dict[str, Any]:
    """Load historical bond yields from IBKR.

    Args:
        symbol: Bond symbol, CUSIP, or identifier (e.g. '91282CJN2')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        duration: IBKR duration string (default '1 D')
        bar_size: Bar size (default '1 day')
        use_rth: Regular trading hours only (default True)
        max_bars: Max most-recent bars returned (default 500, max 5000)
    """
    state = _state(ctx)
    from src.feeds.bonds import BondInstrument, BondYieldHistoryRequest

    try:
        max_bars = _clamp(max_bars, low=1, high=MCP_MAX_BAR_LIMIT)
        bond = BondInstrument(symbol=symbol, exchange=exchange, currency=currency)
        request = BondYieldHistoryRequest(
            bond=bond,
            duration=duration,
            bar_size=bar_size,
            use_rth=use_rth,
        )
        bars = await state.feed.load_bond_yield_history(request)
        serialized = [b.model_dump(mode="json") for b in bars]
        return _truncate_items(
            serialized,
            max_bars,
            key="bars",
            keep_tail=True,
            extra={"symbol": symbol},
        )
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
    max_buckets: int = 200,
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
        max_buckets: Max price buckets returned (default 200, max 2000)
    """
    state = _state(ctx)
    try:
        max_buckets = _clamp(max_buckets, low=1, high=2000)
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
        return _truncate_items(
            buckets,
            max_buckets,
            key="buckets",
            extra={
                "symbol": symbol,
                "asset_class": asset_class,
                "exchange": exchange,
                "currency": currency,
                "time_period": time_period,
                "use_rth": use_rth,
            },
        )
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
) -> dict[str, Any]:
    """Subscribe to 5-second real-time bars from IBKR.

    Returns collected 5-second bars over the specified duration (at most 60 bars for 300s).
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
    except TimeoutError:
        pass
    except Exception as e:
        return _tool_error(e)

    return _truncate_items(
        bars,
        100,
        key="bars",
        extra={"symbol": symbol, "duration_seconds": duration_seconds},
    )


async def get_depth_exchanges(
    ctx: Context,
    max_exchanges: int = 100,
) -> dict[str, Any]:
    """Get available Level 2 market depth exchanges from IBKR.

    Args:
        max_exchanges: Max exchanges returned (default 100, max 500)
    """
    state = _state(ctx)
    try:
        max_exchanges = _clamp(max_exchanges, low=1, high=500)
        exchanges = await state.feed.get_depth_exchanges()
        return _truncate_items(exchanges, max_exchanges, key="exchanges")
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
    max_bars: int = MCP_DEFAULT_BAR_LIMIT,
) -> dict[str, Any]:
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
        max_bars: Max most-recent bars returned (default 500, max 5000)
    """
    state = _state(ctx)
    from src.feeds.ibkr_historical import ensure_historical_chunk_limit, plan_historical_auto_chunk
    from src.feeds.models import OHLCVRequest, normalize_bar_size

    try:
        max_bars = _clamp(max_bars, low=1, high=MCP_MAX_BAR_LIMIT)
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
        serialized = [
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
        return _truncate_items(serialized, max_bars, key="bars", keep_tail=True, extra={"symbol": symbol})
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
    max_bars: int = MCP_DEFAULT_BAR_LIMIT,
) -> dict[str, Any]:
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
        max_bars: Max most-recent bars returned (default 500, max 5000)
    """
    state = _state(ctx)
    from src.feeds.ibkr_historical import ensure_historical_chunk_limit, plan_historical_auto_chunk
    from src.feeds.models import OHLCVRequest, normalize_bar_size

    try:
        max_bars = _clamp(max_bars, low=1, high=MCP_MAX_BAR_LIMIT)
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
        serialized = [
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
        return _truncate_items(serialized, max_bars, key="bars", keep_tail=True, extra={"symbol": symbol})
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
    max_bars: int = MCP_DEFAULT_BAR_LIMIT,
) -> dict[str, Any]:
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
        max_bars: Max most-recent bars returned (default 500, max 5000)
    """
    state = _state(ctx)
    from src.feeds.ibkr_historical import ensure_historical_chunk_limit, plan_historical_auto_chunk
    from src.feeds.models import OHLCVRequest, normalize_bar_size

    try:
        max_bars = _clamp(max_bars, low=1, high=MCP_MAX_BAR_LIMIT)
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
        serialized = [
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
        return _truncate_items(serialized, max_bars, key="bars", keep_tail=True, extra={"symbol": symbol})
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
) -> dict[str, Any]:
    """Get trading schedule for any instrument from IBKR.

    Returns session open/close times, overnight flags, and trading status.

    Args:
        symbol: Ticker symbol
        asset_class: Asset class (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        end_date: End date YYYYMMDD or YYYY-MM-DD (default today)
        num_days: Number of days to return (default 7, max 60)
    """
    state = _state(ctx)
    from datetime import date as date_type
    from src.feeds.models import OHLCVRequest

    try:
        num_days = _clamp(num_days, low=1, high=60)
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
        items = (
            [item.model_dump(mode="json") if hasattr(item, "model_dump") else item for item in schedule]
            if isinstance(schedule, (list, tuple))
            else ([schedule.model_dump(mode="json")] if hasattr(schedule, "model_dump") else [schedule])
        )
        return _truncate_items(items, num_days, key="sessions", extra={"symbol": symbol})
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
