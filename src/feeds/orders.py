"""Pydantic data models for IBKR order management, execution, and pre-trade risk.

These models are used by the order sub-client, REST router, and tests.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class OrderType(StrEnum):
    MARKET = "MKT"
    LIMIT = "LMT"
    STOP = "STP"
    STOP_LIMIT = "STP_LMT"
    TRAIL = "TRAIL"
    TRAIL_LIMIT = "TRAIL_LMT"
    MIDPRICE = "MIDPRICE"
    LIMIT_ON_CLOSE = "LOC"
    MARKET_ON_CLOSE = "MOC"


class TIF(StrEnum):
    DAY = "DAY"
    GTC = "GTC"
    OPG = "OPG"
    IOC = "IOC"
    GTD = "GTD"


class OrderAction(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(StrEnum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    FILLED = "filled"
    CANCELLED = "cancelled"
    PARTIAL = "partial"
    INACTIVE = "inactive"


# ---------------------------------------------------------------------------
# Order placement
# ---------------------------------------------------------------------------

class PlaceOrderRequest(BaseModel):
    """Request body for placing a new order."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    sec_type: str = "STK"
    exchange: str = "SMART"
    currency: str = "USD"
    action: OrderAction
    order_type: OrderType
    quantity: float = Field(gt=0)
    price: float | None = None
    aux_price: float | None = None
    tif: TIF = TIF.DAY
    account_id: str | None = None
    trailing_type: str | None = None
    trailing_amount: float | None = None
    outside_rth: bool = False

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: Any) -> str:
        if value is None:
            raise ValueError("symbol is required")
        return str(value).strip().upper()

    @field_validator("trailing_type", mode="after")
    @classmethod
    def validate_trailing_type(cls, value: str | None) -> str | None:
        if value is not None and value not in ("amt", "%"):
            raise ValueError("trailing_type must be 'amt' or '%'")
        return value

    @model_validator(mode="after")
    def validate_order_fields(self) -> "PlaceOrderRequest":
        if self.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.TRAIL_LIMIT, OrderType.LIMIT_ON_CLOSE):
            if self.price is None:
                raise ValueError(f"price is required for {self.order_type.value} orders")
        if self.order_type in (OrderType.STOP, OrderType.STOP_LIMIT):
            if self.aux_price is None:
                raise ValueError(f"aux_price is required for {self.order_type.value} orders")
        if self.order_type in (OrderType.TRAIL, OrderType.TRAIL_LIMIT):
            if self.trailing_type is not None and self.trailing_amount is None:
                raise ValueError("trailing_amount is required when trailing_type is set")
        return self


class OrderResponse(BaseModel):
    """Response from placing or modifying an order."""

    model_config = ConfigDict(extra="forbid")

    order_id: int
    status: OrderStatus
    message: str | None = None
    warnings: list[str] = []


class ModifyOrderRequest(BaseModel):
    """Request body for modifying an existing order."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    price: float | None = None
    quantity: float | None = None
    order_type: OrderType | None = None
    tif: TIF | None = None
    aux_price: float | None = None
    trailing_type: str | None = None
    trailing_amount: float | None = None

    @field_validator("trailing_type", mode="after")
    @classmethod
    def validate_trailing_type(cls, value: str | None) -> str | None:
        if value is not None and value not in ("amt", "%"):
            raise ValueError("trailing_type must be 'amt' or '%'")
        return value


class CancelOrderResponse(BaseModel):
    """Response from cancelling an order."""

    model_config = ConfigDict(extra="forbid")

    order_id: int
    status: str
    message: str | None = None


# ---------------------------------------------------------------------------
# Open orders
# ---------------------------------------------------------------------------

class OpenOrder(BaseModel):
    """A currently open (working) order."""

    model_config = ConfigDict(extra="forbid")

    order_id: int
    account_id: str = ""
    symbol: str
    sec_type: str
    action: str
    order_type: str
    quantity: float
    filled_quantity: float = 0.0
    price: float | None = None
    status: str
    tif: str = ""
    create_time: datetime | None = None


# ---------------------------------------------------------------------------
# Executions
# ---------------------------------------------------------------------------

class ExecutionRequest(BaseModel):
    """Request body for querying execution/fill details."""

    model_config = ConfigDict(extra="forbid")

    account_id: str | None = None
    symbol: str | None = None
    sec_type: str | None = None
    exchange: str | None = None
    side: str | None = None
    since: datetime | None = None


class ExecutionDetail(BaseModel):
    """A single execution/fill."""

    model_config = ConfigDict(extra="forbid")

    exec_id: str
    order_id: int
    symbol: str
    sec_type: str
    side: str
    quantity: float
    price: float
    commission: float | None = None
    realized_pnl: float | None = None
    exchange: str | None = None
    fill_time: datetime | None = None


class ExecutionResponse(BaseModel):
    """Response containing execution details."""

    model_config = ConfigDict(extra="forbid")

    executions: list[ExecutionDetail]
    total_count: int


# ---------------------------------------------------------------------------
# Pre-trade risk (what-if)
# ---------------------------------------------------------------------------

class WhatIfOrderResponse(BaseModel):
    """Pre-trade margin and commission preview from a what-if order."""

    model_config = ConfigDict(extra="forbid")

    initial_margin: float | None = None
    maintenance_margin: float | None = None
    commission: float | None = None
    equity_with_loan: float | None = None
    init_margin_before: float | None = None
    maint_margin_before: float | None = None
    warnings: list[str] = []


# ---------------------------------------------------------------------------
# Completed orders
# ---------------------------------------------------------------------------

class CompletedOrder(BaseModel):
    """A completed (filled / cancelled) order from order history."""

    model_config = ConfigDict(extra="forbid")

    order_id: int
    symbol: str
    sec_type: str
    action: str
    order_type: str
    quantity: float
    filled_quantity: float = 0.0
    avg_fill_price: float | None = None
    commission: float | None = None
    status: str
    fill_time: datetime | None = None


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def _ibkr_order_status_to_enum(status: Any) -> OrderStatus:
    """Map ib_insync OrderStatus or raw string to our OrderStatus enum."""
    raw = str(status).lower() if status is not None else ""
    mapping = {
        "pendingsubmit": OrderStatus.PENDING,
        "pendingcancel": OrderStatus.PENDING,
        "presubmitted": OrderStatus.PENDING,
        "submitted": OrderStatus.SUBMITTED,
        "filled": OrderStatus.FILLED,
        "cancelled": OrderStatus.CANCELLED,
        "cancelledorder": OrderStatus.CANCELLED,
        "partiallyfilled": OrderStatus.PARTIAL,
        "inactive": OrderStatus.INACTIVE,
        "api cancelled": OrderStatus.CANCELLED,
        "api cancelledorder": OrderStatus.CANCELLED,
    }
    return mapping.get(raw, OrderStatus.PENDING)


def normalize_open_order(trade: Any) -> OpenOrder:
    """Normalize an ib_insync Trade/OpenOrder into an OpenOrder DTO."""
    contract = getattr(trade, "contract", None) or SimpleNamespaceHelper()
    order = getattr(trade, "order", None) or SimpleNamespaceHelper()
    order_status = getattr(trade, "orderStatus", None) or SimpleNamespaceHelper()
    return OpenOrder(
        order_id=getattr(order, "orderId", 0),
        account_id=getattr(order, "acctCode", "") or getattr(order, "account", ""),
        symbol=getattr(contract, "symbol", ""),
        sec_type=getattr(contract, "secType", ""),
        action=getattr(order, "action", ""),
        order_type=getattr(order, "orderType", ""),
        quantity=float(getattr(order, "totalQuantity", 0)),
        filled_quantity=float(getattr(order_status, "filled", 0)),
        price=_safe_float(getattr(order, "lmtPrice", None) or getattr(order, "auxPrice", None)),
        status=str(getattr(order_status, "status", "")),
        tif=getattr(order, "tif", ""),
        create_time=getattr(order, "goodTillDate", None),
    )


def _parse_ibkr_fill_time(value: Any) -> datetime | None:
    """Parse IBKR fill time string (e.g. '20260101  12:00:00') into a datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    s = str(value).strip()
    if not s:
        return None
    # ib_insync returns 'YYYYMMDD  HH:MM:SS' format
    for fmt in ("%Y%m%d %H:%M:%S", "%Y%m%d  %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def normalize_execution(fill: Any) -> ExecutionDetail:
    """Normalize an ib_insync Fill into an ExecutionDetail DTO."""
    contract = getattr(fill, "contract", None) or SimpleNamespaceHelper()
    execution = getattr(fill, "execution", None) or SimpleNamespaceHelper()
    commission_report = getattr(fill, "commissionReport", None) or SimpleNamespaceHelper()
    return ExecutionDetail(
        exec_id=getattr(execution, "execId", ""),
        order_id=getattr(execution, "orderId", 0),
        symbol=getattr(contract, "symbol", ""),
        sec_type=getattr(contract, "secType", ""),
        side=getattr(execution, "side", ""),
        quantity=float(getattr(execution, "shares", 0)),
        price=float(getattr(execution, "price", 0)),
        commission=_safe_float(getattr(commission_report, "commission", None)),
        realized_pnl=_safe_float(getattr(commission_report, "realizedPNL", None)),
        exchange=getattr(execution, "exchange", ""),
        fill_time=_parse_ibkr_fill_time(getattr(execution, "time", None)),
    )


def normalize_completed_order(trade: Any) -> CompletedOrder:
    """Normalize a completed ib_insync Trade into a CompletedOrder DTO."""
    contract = getattr(trade, "contract", None) or SimpleNamespaceHelper()
    order = getattr(trade, "order", None) or SimpleNamespaceHelper()
    order_status = getattr(trade, "orderStatus", None) or SimpleNamespaceHelper()
    return CompletedOrder(
        order_id=getattr(order, "orderId", 0),
        symbol=getattr(contract, "symbol", ""),
        sec_type=getattr(contract, "secType", ""),
        action=getattr(order, "action", ""),
        order_type=getattr(order, "orderType", ""),
        quantity=float(getattr(order, "totalQuantity", 0)),
        filled_quantity=float(getattr(order_status, "filled", 0)),
        avg_fill_price=_safe_float(getattr(order_status, "avgFillPrice", None)),
        commission=_safe_float(getattr(order_status, "commission", None)),
        status=str(getattr(order_status, "status", "")),
        fill_time=getattr(order_status, "lastFillTime", None)
        or getattr(order_status, "completedTime", None),
    )


def normalize_what_if_response(trade: Any) -> WhatIfOrderResponse:
    """Normalize a what-if order result into WhatIfOrderResponse."""
    order_status = getattr(trade, "orderStatus", None) or SimpleNamespaceHelper()
    init_margin = _safe_float(getattr(order_status, "initMarginBefore", None))
    maint_margin = _safe_float(getattr(order_status, "maintMarginBefore", None))
    warnings: list[str] = []
    if init_margin is not None and init_margin > 0:
        pct = init_margin
        if pct > 0.8:
            warnings.append(f"Initial margin utilization after trade: {pct:.1%}")
    return WhatIfOrderResponse(
        initial_margin=_safe_float(getattr(order_status, "initMarginAfter", None)),
        maintenance_margin=_safe_float(getattr(order_status, "maintMarginAfter", None)),
        commission=_safe_float(getattr(order_status, "commission", None)),
        equity_with_loan=_safe_float(getattr(order_status, "equityWithLoanAfter", None)),
        init_margin_before=init_margin,
        maint_margin_before=maint_margin,
        warnings=warnings,
    )


def _safe_float(value: Any) -> float | None:
    """Convert a value to float, returning None if not possible."""
    if value is None:
        return None
    try:
        result = float(value)
        return result if result != 0.0 or str(value).strip() not in ("", "0", "0.0") else result
    except (TypeError, ValueError):
        return None


class SimpleNamespaceHelper:
    """Fallback namespace for safe getattr on possibly-None ib_insync objects."""
    pass
