"""Core MCP tools: symbol resolution, capabilities, server time, rate limits, cache & scheduler health."""

from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import Context, FastMCP

from src.mcp_app.common import (
    _AssetClassParam,
    _READ_TOOL,
    _parse_asset_class,
    _state,
    _tool_error,
)


async def resolve_symbol(
    ctx: Context,
    symbol: str,
    asset_class: _AssetClassParam = None,
    exchange: str | None = None,
    currency: str | None = None,
) -> dict[str, Any]:
    """Resolve any ticker notation into normalized IBKR contract fields.

    Accepts equity suffix notations (0700.HK, 7203.T, BRK.B), FX pairs
    (EURUSD, EUR.USD, EUR-USD, EUR/USD), crypto roots (BTC, ETHUSD),
    futures notations (ESZ26, ESZ2026, ES1!, ES=F, -ES), and index aliases
    (^GSPC -> SPX). Returns the canonical symbol, asset class, exchange,
    currency, primary exchange, contract fields, and the matched rule.

    Args:
        symbol: Raw ticker in any supported notation
        asset_class: Optional asset class pin (equity, fx, future, crypto, index, option, bond)
        exchange: Optional exchange override
        currency: Optional currency override
    """
    from src.feeds.symbol_resolver import resolve_symbol as _resolve_symbol

    try:
        pinned = _parse_asset_class(asset_class) if asset_class else None
        resolved = _resolve_symbol(symbol, asset_class=pinned, exchange=exchange, currency=currency)
        return resolved.describe()
    except Exception as e:
        return _tool_error(e)


async def get_gateway_capabilities(ctx: Context) -> dict[str, Any]:
    """Report implementation and client capabilities without connecting to IBKR."""
    return _state(ctx).feed.get_capabilities().model_dump(mode="json")


async def get_server_time(ctx: Context) -> dict[str, Any]:
    """Get IBKR server time. Useful for latency measurement and clock sync."""
    state = _state(ctx)
    try:
        return await state.feed.get_server_time()
    except Exception as e:
        return _tool_error(e)


async def get_ibkr_rate_limits(ctx: Context) -> dict[str, Any]:
    """Get IBKR rate limit / pacing controller status."""
    state = _state(ctx)
    try:
        feed = getattr(state, "feed", None)
        connection = getattr(feed, "_connection", None)
        snapshot_fn = getattr(connection, "rate_limit_snapshot", None) if connection is not None else None
        if callable(snapshot_fn):
            return await snapshot_fn()  # type: ignore[misc]
        return {"enabled": False, "reason": "not_configured"}
    except Exception as e:
        return _tool_error(e)


async def get_scheduler_health(ctx: Context) -> dict[str, Any]:
    """Get scheduler health status for all tracked jobs."""
    state = _state(ctx)
    try:
        scheduler = getattr(state, "scheduler", None)
        if scheduler is None or getattr(scheduler, "_health_monitor", None) is None:
            return {"status": "not_configured", "jobs": {}}
        report = scheduler._health_monitor.get_health_status()
        return report.model_dump(mode="json")
    except Exception as e:
        return _tool_error(e)


async def get_cache_stats(ctx: Context) -> dict[str, Any]:
    """Get market data TTL cache statistics."""
    state = _state(ctx)
    try:
        stats = await state.market_data_cache.stats()
        return {"size": stats.size, "max_size": stats.max_size, "default_ttl_seconds": stats.default_ttl_seconds}
    except Exception as e:
        return _tool_error(e)


def register(mcp: FastMCP) -> None:
    for fn in (
        resolve_symbol,
        get_gateway_capabilities,
        get_server_time,
        get_ibkr_rate_limits,
        get_scheduler_health,
        get_cache_stats,
    ):
        mcp.tool(annotations=_READ_TOOL)(fn)
