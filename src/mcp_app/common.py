"""Shared helpers, error builders, annotations, and typed parameters for MCP tools."""

from __future__ import annotations

import re
from typing import Annotated, Any

from mcp.server.fastmcp import Context
from mcp.types import ToolAnnotations
from pydantic import Field

from src.webapp.dependencies import IBKRRestAppState

MCP_ORDERS_DISABLED_ERROR = (
    "order pathways are disabled; set IBKR_ORDERS_ENABLED=true to enable "
    "place/modify/cancel/bracket/OCA/preview/exercise operations"
)

_READ_TOOL = ToolAnnotations(readOnlyHint=True, openWorldHint=False)
_WRITE_TOOL = ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False)
_ORDER_TOOL = ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=False)
_DESTRUCTIVE_TOOL = ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=True, openWorldHint=False)

_AssetClassParam = Annotated[
    str | None,
    Field(
        description=(
            "Asset class filter: crypto, equity, fx, future, index, bond, option. "
            "Case-insensitive; aliases futures/stocks/equities/forex accepted."
        )
    ),
]
_BarSizeParam = Annotated[
    str | None,
    Field(description="IBKR bar size spelled exactly: '5 secs', '1 min', '5 mins', '15 mins', '30 mins', '1 hour', '4 hours', '1 day', '1 week', '1 month'."),
]
_WhatToShowParam = Annotated[
    str,
    Field(description="IBKR whatToShow value, e.g. TRADES, MIDPOINT, BID, ASK, BID_ASK, HISTORICAL_VOLATILITY, OPTION_IMPLIED_VOLATILITY, YIELD_BID, YIELD_ASK."),
]
_RightParam = Annotated[str, Field(description="Option right: 'C' or 'CALL', 'P' or 'PUT' (case-insensitive).")]
_MarketDataTypeParam = Annotated[
    int,
    Field(description="IBKR market data type: 1=live, 2=frozen, 3=delayed, 4=delayed frozen."),
]
_ExerciseActionParam = Annotated[int, Field(description="Exercise action: 1=exercise, 2=lapse.")]
_OcaTypeParam = Annotated[int, Field(description="OCA group type: 1=CancelAll, 2=ReduceRemaining, 3=ReducePosition (default 1).")]
_MarketDataTypeOptParam = Annotated[int | None, Field(description="IBKR market data type: 1=live, 2=frozen, 3=delayed, 4=delayed frozen.")]
_TifParam = Annotated[str, Field(description="Time in force: DAY, GTC, IOC, GTD, OPG, DTC, or ERL/EPD (EARLY session, live 2026-12-06; outside_rth must be false).")]
_TifOptParam = Annotated[str | None, Field(description="Time in force: DAY, GTC, IOC, GTD, OPG, DTC, or ERL/EPD (EARLY session, live 2026-12-06; outside_rth must be false).")]
_OrderTypeParam = Annotated[str, Field(description="Order type: LMT, MKT, STP, STP_LIMIT, TRAIL, TRAIL_LIMIT, MIDPRICE, BOX, ...")]
_RightOptParam = Annotated[str | None, Field(description="Option right: 'C' or 'CALL', 'P' or 'PUT' (case-insensitive).")]

_SENSITIVE_KEY_PATTERNS = re.compile(
    r"(auth|token|secret|password|bearer)",
    re.IGNORECASE,
)


def _state(ctx: Context) -> IBKRRestAppState:
    return ctx.request_context.lifespan_context["ibkr_state"]


def _error(message: str, *, code: str, retryable: bool = False) -> dict[str, Any]:
    """Build the shared MCP error payload: message, stable code, retry flag."""
    return {"error": message, "code": code, "retryable": retryable}


def _tool_error(exc: BaseException) -> dict[str, Any]:
    """Classify an exception into the shared MCP error payload."""
    from src.feeds.exceptions import (
        IBKRCircuitOpenError,
        IBKRConnectionDisabledError,
        IBKRConnectionError,
        IBKRContractResolutionError,
        IBKRMarketDataUnavailableError,
        IBKROrderError,
        IBKROrdersDisabledError,
        IBKRPacingError,
        IBKRUnsupportedFeatureError,
        QuestDBConnectionError,
    )

    if isinstance(exc, IBKROrdersDisabledError):
        return _error(MCP_ORDERS_DISABLED_ERROR, code="ORDERS_DISABLED")
    if isinstance(exc, IBKRConnectionDisabledError):
        return _error(str(exc), code="IBKR_DISABLED")
    if isinstance(exc, IBKRCircuitOpenError):
        return _error(str(exc), code="IBKR_CIRCUIT_OPEN", retryable=True)
    if isinstance(exc, IBKRPacingError):
        return _error(str(exc), code="IBKR_PACING", retryable=True)
    if isinstance(exc, (IBKRConnectionError, ConnectionError, TimeoutError)):
        return _error(str(exc), code="IBKR_UNAVAILABLE", retryable=True)
    if isinstance(exc, IBKRContractResolutionError):
        return _error(str(exc), code="CONTRACT_RESOLUTION_FAILED")
    if isinstance(exc, IBKRMarketDataUnavailableError):
        return _error(str(exc), code="MARKET_DATA_UNAVAILABLE", retryable=True)
    if isinstance(exc, IBKROrderError):
        return _error(str(exc), code="ORDER_REJECTED")
    if isinstance(exc, IBKRUnsupportedFeatureError):
        return _error(str(exc), code="FEATURE_REMOVED")
    if isinstance(exc, QuestDBConnectionError):
        return _error(str(exc), code="QUESTDB_UNAVAILABLE", retryable=True)
    if isinstance(exc, ValueError):
        return _error(str(exc), code="BAD_REQUEST")
    return _error(str(exc), code="TOOL_ERROR")


def _parse_asset_class(asset_class: str) -> Any:
    """Parse MCP asset_class text using AssetClass values, names, and documented aliases."""
    from src.feeds.models import AssetClass

    normalized = asset_class.strip().lower()
    aliases = {
        "futures": AssetClass.FUTURE.value,
        "stocks": AssetClass.EQUITY.value,
        "stock": AssetClass.EQUITY.value,
        "equities": AssetClass.EQUITY.value,
        "forex": AssetClass.FX.value,
    }
    normalized = aliases.get(normalized, normalized)
    try:
        return AssetClass(normalized)
    except ValueError:
        member = AssetClass.__members__.get(asset_class.strip().upper())
        if member is not None:
            return member
        supported = ", ".join(item.value for item in AssetClass)
        raise ValueError(f"unsupported asset_class '{asset_class}'; expected one of: {supported}")


def _require_questdb(ctx: Context) -> dict[str, Any] | None:
    """Return an error dict if QuestDB is not connected, else None."""
    if not ctx.request_context.lifespan_context.get("questdb_connected"):
        return _error("QuestDB is not available", code="QUESTDB_UNAVAILABLE", retryable=True)
    return None


def _require_redis(ctx: Context) -> dict[str, Any] | None:
    """Return an error dict if Redis is not connected, else None."""
    if not ctx.request_context.lifespan_context.get("redis_connected"):
        return _error("Redis is not available", code="REDIS_UNAVAILABLE", retryable=True)
    return None


def _require_ibkr(ctx: Context) -> dict[str, Any] | None:
    """Return an error dict if IBKR feed is not connected, else None."""
    feed = _state(ctx).feed
    connection = getattr(feed, "_connection", None)
    if connection is not None:
        if not connection.connection_enabled:
            return _error("IBKR connection disabled by Redis control switch", code="IBKR_DISABLED")
        return None if connection.is_connected else _error(
            "IBKR feed is not available", code="IBKR_UNAVAILABLE", retryable=True
        )
    if not ctx.request_context.lifespan_context.get("ibkr_connected"):
        return _error("IBKR feed is not available", code="IBKR_UNAVAILABLE", retryable=True)
    return None
