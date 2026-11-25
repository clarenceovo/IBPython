"""Trading MCP tools: order preview, modify, bracket, OCA, option exercise, and cancel-all."""

from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import Context, FastMCP

from src.mcp_app.common import (
    MCP_ORDERS_DISABLED_ERROR,
    MCP_ORDER_SUBMISSION_DISABLED_ERROR,
    _AssetClassParam,
    _DESTRUCTIVE_TOOL,
    _ExerciseActionParam,
    _ORDER_TOOL,
    _OcaTypeParam,
    _OrderTypeParam,
    _READ_TOOL,
    _RightParam,
    _TifOptParam,
    _TifParam,
    _error,
    _state,
    _tool_error,
)


def _require_mcp_submission_allowed(state: Any) -> dict[str, Any] | None:
    if not state.settings.ibkr_orders_enabled:
        return _error(MCP_ORDERS_DISABLED_ERROR, code="ORDERS_DISABLED")
    if state.settings.ibkr_disable_order_submission:
        return _error(MCP_ORDER_SUBMISSION_DISABLED_ERROR, code="ORDER_SUBMISSION_DISABLED")
    return None


async def cancel_all_orders(ctx: Context, confirm: bool = False) -> dict[str, Any]:
    """Cancel ALL open orders across all accounts. Use with extreme caution.

    Destructive and idempotent. Requires an explicit opt-in: pass
    confirm=true, otherwise the tool returns CONFIRMATION_REQUIRED and
    cancels nothing. Remains available when IBKR_DISABLE_ORDER_SUBMISSION=true
    so open risk can still be flattened.
    """
    state = _state(ctx)
    if not state.settings.ibkr_orders_enabled:
        return _error(MCP_ORDERS_DISABLED_ERROR, code="ORDERS_DISABLED")
    if not confirm:
        return _error(
            "pass confirm=true to cancel all open orders",
            code="CONFIRMATION_REQUIRED",
        )
    try:
        return await state.feed.cancel_all_orders()
    except Exception as e:
        return _tool_error(e)


async def exercise_option(
    ctx: Context,
    symbol: str,
    right: _RightParam,
    strike: float,
    expiry: str,
    exercise_action: _ExerciseActionParam,
    quantity: int,
    account: str,
    exchange: str = "SMART",
    currency: str = "USD",
    override: bool = False,
) -> dict[str, Any]:
    """Exercise or lapse an option position.

    Args:
        symbol: Underlying symbol
        right: C/CALL or P/PUT
        strike: Strike price
        expiry: Expiration date YYYYMMDD
        exercise_action: 1=exercise, 2=lapse
        quantity: Number of contracts
        account: IBKR account ID
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        override: Override exercise restrictions (default False)
    """
    state = _state(ctx)
    if err := _require_mcp_submission_allowed(state):
        return err
    try:
        return await state.feed.exercise_option(
            symbol=symbol,
            right=right,
            strike=strike,
            expiry=expiry,
            exercise_action=exercise_action,
            quantity=quantity,
            account=account,
            exchange=exchange,
            currency=currency,
            override=override,
        )
    except Exception as e:
        return _tool_error(e)


async def modify_order(
    ctx: Context,
    order_id: int,
    account: str,
    price: float | None = None,
    quantity: float | None = None,
    tif: _TifOptParam = None,
) -> dict[str, Any]:
    """Modify an existing live order. Only pass fields you want to change.

    Args:
        order_id: The IBKR order ID to modify
        account: IBKR account ID
        price: New limit price (optional)
        quantity: New order quantity (optional)
        tif: New time in force (optional, e.g. DAY, GTC, IOC)
    """
    from src.feeds.orders import ModifyOrderRequest, TIF as TIFEnum

    state = _state(ctx)
    if err := _require_mcp_submission_allowed(state):
        return err
    try:
        tif_enum = TIFEnum(tif) if tif is not None else None
        modifications = ModifyOrderRequest(price=price, quantity=quantity, tif=tif_enum)
        result = await state.feed.modify_order(
            account_id=account,
            order_id=order_id,
            modifications=modifications,
        )
        return result.model_dump(mode="json") if hasattr(result, "model_dump") else result
    except Exception as e:
        return _tool_error(e)


async def preview_order(
    ctx: Context,
    symbol: str,
    action: str = "BUY",
    order_type: _OrderTypeParam = "LMT",
    quantity: float = 1.0,
    price: float | None = None,
    aux_price: float | None = None,
    tif: _TifParam = "DAY",
    account: str | None = None,
    sec_type: str = "STK",
    exchange: str = "SMART",
    currency: str = "USD",
    outside_rth: bool = False,
) -> dict[str, Any]:
    """Preview an order without submitting (whatIfOrder). Returns margin and commission impact.

    Remains available when IBKR_DISABLE_ORDER_SUBMISSION=true as long as
    IBKR_ORDERS_ENABLED=true.

    Args:
        symbol: Ticker symbol
        action: BUY or SELL (default 'BUY')
        order_type: Order type — LMT, MKT, STP, STP_LMT, etc (default 'LMT')
        quantity: Order quantity (default 1.0)
        price: Limit price (required for LMT orders)
        aux_price: Auxiliary/stop price (for stop orders)
        tif: Time in force — DAY, GTC, IOC (default 'DAY')
        account: IBKR account ID (optional)
        sec_type: Security type — STK, OPT, FUT, CASH (default 'STK')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        outside_rth: Allow outside regular trading hours (default False)
    """
    from src.feeds.orders import OrderAction, OrderType as OrderTypeEnum, PlaceOrderRequest, TIF as TIFEnum

    state = _state(ctx)
    if not state.settings.ibkr_orders_enabled:
        return _error(MCP_ORDERS_DISABLED_ERROR, code="ORDERS_DISABLED")
    try:
        request = PlaceOrderRequest(
            symbol=symbol,
            action=OrderAction(action.upper()),
            order_type=OrderTypeEnum(order_type.upper()),
            quantity=quantity,
            price=price,
            aux_price=aux_price,
            tif=TIFEnum(tif.upper()),
            account_id=account,
            sec_type=sec_type,
            exchange=exchange,
            currency=currency,
            outside_rth=outside_rth,
        )
        result = await state.feed.preview_order(request)
        return result.model_dump(mode="json") if hasattr(result, "model_dump") else result
    except Exception as e:
        return _tool_error(e)


async def place_bracket_order(
    ctx: Context,
    symbol: str,
    action: str = "BUY",
    quantity: float = 1.0,
    limit_price: float | None = None,
    take_profit_price: float | None = None,
    stop_loss_price: float | None = None,
    order_type: _OrderTypeParam = "LMT",
    tif: _TifParam = "GTC",
    asset_class: _AssetClassParam = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    account: str = "",
) -> dict[str, Any]:
    """Place a bracket order (parent + take-profit + stop-loss).

    Creates three linked orders: a parent entry order, an attached take-profit,
    and an attached stop-loss. When one of the exit orders fills, the other is cancelled.

    Args:
        symbol: Ticker symbol
        action: BUY or SELL (default 'BUY')
        quantity: Order quantity (default 1.0)
        limit_price: Limit price for entry order (required for LMT orders)
        take_profit_price: Take-profit limit price (required)
        stop_loss_price: Stop-loss trigger price (required)
        order_type: MKT or LMT (default 'LMT')
        tif: Time in force: DAY, GTC, IOC, GTD (default 'GTC')
        asset_class: EQUITY, FUTURE, OPTION (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        account: IBKR account ID

    Validation and placement go through the same guarded order client as
    POST /api/v1/orders/bracket (master switch, submission kill-switch, typed responses).
    """
    state = _state(ctx)
    if err := _require_mcp_submission_allowed(state):
        return err
    try:
        from src.feeds.orders import BracketOrderRequest

        request = BracketOrderRequest.model_validate({
            "symbol": symbol,
            "action": action,
            "quantity": quantity,
            "limit_price": limit_price,
            "take_profit_price": take_profit_price,
            "stop_loss_price": stop_loss_price,
            "order_type": order_type,
            "tif": tif,
            "asset_class": asset_class or "EQUITY",
            "exchange": exchange,
            "currency": currency,
            "account": account,
        })
        response = await state.feed.place_bracket_order(request)
        return response.model_dump(mode="json")
    except Exception as e:
        return _tool_error(e)


async def place_oca_group(
    ctx: Context,
    orders: list[dict[str, Any]],
    oca_group: str = "",
    oca_type: _OcaTypeParam = 1,
    account: str = "",
) -> list[dict[str, Any]] | dict[str, Any]:
    """Place a One-Cancels-All group of orders.

    When any order in the group fills, all others are automatically cancelled.

    Args:
        orders: List of 2+ order dicts, each with: symbol, action, quantity, order_type, price (required for LMT), tif?, asset_class?, exchange?, currency?
        oca_group: OCA group name (auto-generated if empty)
        oca_type: 1=CancelAll, 2=ReduceRemaining, 3=ReducePosition (default 1)
        account: IBKR account ID

    Validation and placement go through the same guarded order client as
    POST /api/v1/orders/oca (master switch, submission kill-switch, typed responses).
    """
    state = _state(ctx)
    if err := _require_mcp_submission_allowed(state):
        return err
    try:
        from src.feeds.orders import OcaGroupRequest

        request = OcaGroupRequest.model_validate({
            "orders": orders,
            "oca_group": oca_group,
            "oca_type": oca_type,
            "account": account,
        })
        responses = await state.feed.place_oca_group(request)
        return [r.model_dump(mode="json") for r in responses]
    except Exception as e:
        return _tool_error(e)


def register(mcp: FastMCP) -> None:
    mcp.tool(annotations=_READ_TOOL)(preview_order)
    for fn in (modify_order, place_bracket_order, place_oca_group, exercise_option):
        mcp.tool(annotations=_ORDER_TOOL)(fn)
    mcp.tool(annotations=_DESTRUCTIVE_TOOL)(cancel_all_orders)
