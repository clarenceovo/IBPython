"""Account MCP tools: read-only account summary, live positions, portfolio items, PnL, open orders."""

from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import Context, FastMCP

from src.mcp_app.common import _READ_TOOL, _clamp, _state, _tool_error, _truncate_items


async def load_account_summary(
    ctx: Context,
    account: str = "",
) -> list[dict[str, Any]] | dict[str, Any]:
    """Load IBKR account summary (NetLiquidation, AvailableFunds, etc).

    Args:
        account: Account ID (empty string for default account)
    """
    state = _state(ctx)
    try:
        rows = await state.feed.load_account_summary(account=account)
        if rows and hasattr(rows[0], "values") and not hasattr(rows[0], "tag"):
            return [row.model_dump(mode="json") for row in rows]
        return {row.tag: {"value": row.value, "currency": row.currency} for row in rows}
    except Exception as e:
        return _tool_error(e)


async def load_live_positions(
    ctx: Context,
    limit: int = 200,
) -> dict[str, Any]:
    """Load live positions from IBKR with bounded output.

    Args:
        limit: Max positions returned (default 200, max 1000)
    """
    state = _state(ctx)
    try:
        limit = _clamp(limit, low=1, high=1000)
        positions = await state.feed.load_live_positions()
        serialized = [
            {
                "symbol": p.symbol,
                "sec_type": p.sec_type,
                "exchange": p.exchange,
                "currency": p.currency,
                "position": p.position,
                "average_cost": p.average_cost,
                "account": p.account,
                "con_id": p.con_id,
            }
            for p in positions
        ]
        return _truncate_items(serialized, limit, key="positions")
    except Exception as e:
        return _tool_error(e)


async def load_portfolio_items(
    ctx: Context,
    account: str = "",
    limit: int = 200,
) -> dict[str, Any]:
    """Load portfolio items (positions with market data) from IBKR with bounded output.

    Returns portfolio items with symbol, position, market price, market value,
    average cost, unrealized PnL, realized PnL, plus ``total_count`` and ``truncated``.

    Args:
        account: Account ID (empty string for default account)
        limit: Max portfolio items returned (default 200, max 1000)
    """
    state = _state(ctx)
    try:
        limit = _clamp(limit, low=1, high=1000)
        items = await state.feed.load_portfolio_items(account=account)
        serialized = [item.model_dump(mode="json") for item in items]
        return _truncate_items(serialized, limit, key="items", extra={"account": account})
    except Exception as e:
        return _tool_error(e)


async def load_account_pnl(
    ctx: Context,
    account: str = "",
    model_code: str = "",
    wait_seconds: float = 4.0,
) -> dict[str, Any]:
    """Load account-level PnL snapshot from IBKR.

    Subscribes to account PnL, waits for the first update, then returns
    daily PnL, unrealized PnL, and realized PnL.

    Args:
        account: Account ID (empty string for default account)
        model_code: Optional model/portfolio code
        wait_seconds: Seconds to wait for PnL subscription data (default 4.0)
    """
    state = _state(ctx)
    try:
        pnl = await state.feed.load_account_pnl_snapshot(
            account=account,
            model_code=model_code,
            wait_seconds=wait_seconds,
        )
        return pnl.model_dump()
    except Exception as e:
        return _tool_error(e)


async def load_position_pnl(
    ctx: Context,
    account: str = "",
    con_id: int = 0,
    model_code: str = "",
) -> dict[str, Any]:
    """Load position-level PnL snapshot from IBKR.

    Subscribes to position PnL for a specific contract or all positions,
    waits for the first update, then returns position PnL data.

    Args:
        account: Account ID (empty string for default account)
        con_id: IBKR contract ID. Use 0 (default) for all positions
        model_code: Optional model/portfolio code
    """
    state = _state(ctx)
    try:
        pnl = await state.feed.load_position_pnl_snapshot(
            account=account,
            con_id=con_id,
            model_code=model_code,
        )
        return pnl.model_dump()
    except Exception as e:
        return _tool_error(e)


async def get_all_open_orders(
    ctx: Context,
    limit: int = 100,
) -> dict[str, Any]:
    """Get all open orders across ALL API clients and TWS (not just current session).

    Unlike load_open_orders which returns only current client's orders,
    this returns orders from all API sessions and manually-placed TWS orders.

    Args:
        limit: Max orders to return (default 100, max 1000)
    """
    state = _state(ctx)
    try:
        limit = _clamp(limit, low=1, high=1000)
        orders = await state.feed.get_all_open_orders()
        serialized = [o.model_dump(mode="json") if hasattr(o, "model_dump") else o for o in orders]
        return _truncate_items(serialized, limit, key="orders")
    except Exception as e:
        return _tool_error(e)


def register(mcp: FastMCP) -> None:
    for fn in (
        load_account_summary,
        load_live_positions,
        load_portfolio_items,
        load_account_pnl,
        load_position_pnl,
        get_all_open_orders,
    ):
        mcp.tool(annotations=_READ_TOOL)(fn)
