"""MCP static & dynamic resources (status and QuestDB table schema)."""

from __future__ import annotations

import json
from collections.abc import Iterable

from mcp.server.fastmcp import FastMCP

from src.config import config_constant as constants


def get_table_schema() -> str:
    """Available QuestDB tables and their structure."""
    return json.dumps({
        "EquityOHLCV": {
            "columns": [
                "symbol", "asset_class", "exchange", "currency", "timestamp",
                "open", "high", "low", "close", "volume", "bar_size", "source",
                "contract_key", "con_id", "local_symbol", "contract_month", "expiry",
                "strike", "right", "trading_class", "what_to_show", "use_rth", "metadata",
            ],
            "partition": "DAY",
            "indexed": ["symbol", "asset_class", "exchange", "bar_size", "contract_key"],
        },
        "equity_snapshots": {
            "description": "Real-time equity snapshots from IBKR scanner",
            "key_fields": ["symbol", "exchange", "timestamp"],
        },
        "fx_option_snapshots": {
            "description": "FX option volatility surface snapshots",
            "key_fields": ["currency", "timestamp", "tenor", "delta"],
        },
    }, indent=2)


def _registered_tool_summaries(mcp: FastMCP) -> list[str]:
    tool_manager = getattr(mcp, "_tool_manager", None)
    if tool_manager is None:
        return []
    list_tools = getattr(tool_manager, "list_tools", None)
    if not callable(list_tools):
        return []
    summaries: list[str] = []
    for tool in list_tools():
        first_line = (tool.description or "").strip().splitlines()[0] if tool.description else ""
        summaries.append(f"{tool.name} — {first_line}" if first_line else tool.name)
    return summaries


def register_resources(mcp: FastMCP, enabled_toolsets: Iterable[str]) -> tuple:
    """Register MCP resources on the server and return the resource callables."""
    active_toolsets = list(enabled_toolsets)

    @mcp.resource("ibpython://status")
    def get_server_status() -> str:
        """IBPython MCP server status and available capabilities."""
        return json.dumps({
            "server": "IBPython Market Data MCP",
            "version": constants.APP_VERSION,
            "enabled_toolsets": active_toolsets,
            "tools": _registered_tool_summaries(mcp),
            "databases": {
                "questdb": "Time-series OHLCV, snapshots, tick data",
                "redis": "Latest bars cache, pacing bookmarks",
                "ibkr": "Live TWS/Gateway connection for real-time data",
            },
        }, indent=2)

    mcp.resource("ibpython://schema/tables")(get_table_schema)
    return get_server_status, get_table_schema
