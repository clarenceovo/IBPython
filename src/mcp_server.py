"""
MCP (Model Context Protocol) server for IBPython — backward-compatible entrypoint.

The modular implementation lives in ``src/mcp_app/`` (split into domain tool
modules with ``MCP_TOOLSETS`` filtering). This module keeps existing imports
and CLI invocations working without changes.

Run:
    python -m src.mcp_server
    # or with a filtered toolset preset (research, options_desk, readonly_all, full):
    MCP_TOOLSETS=research python -m src.mcp_server
    python -m src.mcp_server --toolsets core,storage,market_data
    # or with Streamable HTTP (binds 127.0.0.1:9000 by default):
    MCP_HTTP_HOST=0.0.0.0 MCP_HTTP_PORT=9000 MCP_API_KEY=secret python -m src.mcp_server --http
    # or with uvicorn (enforces MCP_API_KEY via MCPBearerAuthMiddleware):
    uvicorn src.mcp_server:get_http_app --factory --host 127.0.0.1 --port 9000
"""

from __future__ import annotations

import json

from src.config import config_constant as constants
from src.mcp_app import (
    MCPBearerAuthMiddleware,
    TOOLSET_NAMES,
    TOOLSET_PRESETS,
    TOOLSET_REGISTRY,
    create_mcp_server,
    get_http_app,
    main,
    main_stdio,
    main_streamable_http,
    mcp_lifespan,
    register_toolsets,
    resolve_toolsets,
)
from src.mcp_app.common import (
    MCP_ORDERS_DISABLED_ERROR,
    MCP_ORDER_SUBMISSION_DISABLED_ERROR,
    _DESTRUCTIVE_TOOL,
    _ORDER_TOOL,
    _READ_TOOL,
    _SENSITIVE_KEY_PATTERNS,
    _WRITE_TOOL,
    _error,
    _parse_asset_class,
    _require_ibkr,
    _require_questdb,
    _require_redis,
    _state,
    _tool_error,
)
from src.mcp_app.resources import _registered_tool_summaries, get_table_schema
from src.mcp_app.runtime import (
    _MCPSharedState,
    _acquire_mcp_shared_state,
    _build_mcp_shared_state,
    _cancel_mcp_idle_close,
    _close_mcp_shared_state,
    _close_mcp_shared_state_after_idle,
    _get_mcp_shared_state_lock,
    _release_mcp_shared_state,
    make_mcp_lifespan,
)
from src.mcp_app.tools.account import (
    get_all_open_orders,
    load_account_pnl,
    load_account_summary,
    load_live_positions,
    load_portfolio_items,
    load_position_pnl,
)
from src.mcp_app.tools.core import (
    get_cache_stats,
    get_gateway_capabilities,
    get_ibkr_rate_limits,
    get_server_time,
    resolve_symbol,
)
from src.mcp_app.tools.discovery import (
    get_index_composition,
    get_scanner_parameters,
    scan_contracts,
    scan_market,
    search_contracts,
)
from src.mcp_app.tools.market_data import (
    get_depth_exchanges,
    get_head_timestamp,
    get_historical_volatility,
    get_latest_ticks,
    get_market_depth,
    get_option_implied_volatility_series,
    get_trading_schedule,
    get_yield_data,
    load_bond_yield_history,
    load_historical_ohlcv_live,
    load_historical_ticks,
    request_histogram,
    set_market_data_type,
    subscribe_realtime_bars,
)
from src.mcp_app.tools.options import (
    calculate_iv,
    calculate_option_price,
    load_option_analytics,
    load_option_chains,
    load_option_skew_surface,
)
from src.mcp_app.tools.reference import (
    get_news_bulletins,
    load_economic_calendar,
    load_equity_dividends,
    load_equity_shortability,
    load_fundamentals,
    load_news,
    load_news_article,
    load_news_providers,
    load_wsh_event_data,
    load_wsh_metadata,
)
from src.mcp_app.tools.storage import (
    get_all_latest_equity_snapshots,
    get_latest_bar_from_cache,
    get_latest_equity_snapshots,
    get_latest_fx_option_snapshot,
    get_redis_key,
    get_snapshot_watchlist,
    list_snapshot_watchlists,
    query_equity_snapshots,
    query_fx_option_snapshots,
    query_historical_ohlcv,
    query_latest_bars,
    query_raw_sql,
)
from src.mcp_app.tools.trading import (
    cancel_all_orders,
    exercise_option,
    modify_order,
    place_bracket_order,
    place_oca_group,
    preview_order,
)
from src.mcp_app.transport import _get_mcp_http_config

mcp = create_mcp_server()


def get_server_status() -> str:
    """IBPython MCP server status and available capabilities."""
    return json.dumps({
        "server": "IBPython Market Data MCP",
        "version": constants.APP_VERSION,
        "tools": _registered_tool_summaries(mcp),
        "databases": {
            "questdb": "Time-series OHLCV, snapshots, tick data",
            "redis": "Latest bars cache, pacing bookmarks",
            "ibkr": "Live TWS/Gateway connection for real-time data",
        },
    }, indent=2)


if __name__ == "__main__":
    main()
