"""Pydantic data models for IBKR order management, execution, and pre-trade risk.

These models are used by the order sub-client, REST router, and tests.
"""

from __future__ import annotations

import uuid as _uuid
from datetime import datetime, timezone
from enum import StrEnum
import math
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class OrderType(StrEnum):
    MARKET = "MKT"
    LIMIT = "LMT"
    STOP = "STP"
    STOP_LIMIT = "STP LMT"
    TRAIL = "TRAIL"
    TRAIL_LIMIT = "TRAIL LIMIT"
    MIDPRICE = "MIDPRICE"
    LIMIT_ON_CLOSE = "LOC"
    MARKET_ON_CLOSE = "MOC"


class TIF(StrEnum):
    DAY = "DAY"
    GTC = "GTC"
    OPG = "OPG"
    IOC = "IOC"
    FOK = "FOK"
    DTC = "DTC"
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
    primary_exchange: str | None = None
    last_trade_date_or_contract_month: str | None = None
    multiplier: str | None = None
    local_symbol: str | None = None
    sec_id_type: str | None = None
    sec_id: str | None = None
    con_id: int | None = Field(default=None, gt=0)
    # Option-specific contract fields
    strike: float | None = Field(default=None, gt=0)
    right: str | None = Field(default=None, description="Option right: C/CALL or P/PUT")
    option_expiry: str | None = Field(default=None, description="Option expiry YYYYMMDD for OPT/FOP contracts")
    underlying_symbol: str | None = Field(default=None, description="Underlying symbol for option contracts")
    action: OrderAction
    order_type: OrderType
    quantity: float = Field(gt=0)
    price: float | None = Field(default=None, gt=0)
    aux_price: float | None = Field(default=None, gt=0)
    tif: TIF = TIF.DAY
    account_id: str | None = Field(default=None, min_length=1)
    trailing_type: str | None = None
    trailing_amount: float | None = Field(default=None, gt=0)
    trail_stop_price: float | None = Field(default=None, gt=0)
    limit_price_offset: float | None = Field(default=None, gt=0)
    outside_rth: bool = False
    idempotency_key: str | None = Field(default=None, min_length=1)

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: Any) -> str:
        if value is None:
            raise ValueError("symbol is required")
        return str(value).strip().upper()

    @field_validator(
        "sec_type",
        "exchange",
        "currency",
        "primary_exchange",
        "last_trade_date_or_contract_month",
        "multiplier",
        "local_symbol",
        "sec_id_type",
        "sec_id",
        mode="before",
    )
    @classmethod
    def normalize_optional_upper_tokens(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None

    @field_validator("quantity", "price", "aux_price", "trailing_amount", "trail_stop_price", "limit_price_offset")
    @classmethod
    def validate_finite_number(cls, value: float | None) -> float | None:
        if value is not None and not math.isfinite(value):
            raise ValueError("order numeric fields must be finite")
        return value

    @field_validator("trailing_type", mode="after")
    @classmethod
    def validate_trailing_type(cls, value: str | None) -> str | None:
        if value is not None and value not in ("amt", "%"):
            raise ValueError("trailing_type must be 'amt' or '%'")
        return value

    @model_validator(mode="after")
    def validate_order_fields(self) -> "PlaceOrderRequest":
        if self.order_type == OrderType.TRAIL and self.price is not None:
            raise ValueError("TRAIL orders must use trail_stop_price, not price")
        if self.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.LIMIT_ON_CLOSE):
            if self.price is None:
                raise ValueError(f"price is required for {self.order_type.value} orders")
        if self.order_type in (OrderType.STOP, OrderType.STOP_LIMIT):
            if self.aux_price is None:
                raise ValueError(f"aux_price is required for {self.order_type.value} orders")
        if self.order_type in (OrderType.TRAIL, OrderType.TRAIL_LIMIT):
            if self.trailing_type is None:
                raise ValueError(f"trailing_type is required for {self.order_type.value} orders")
            if self.trailing_amount is None:
                raise ValueError(f"trailing_amount is required for {self.order_type.value} orders")
        if self.order_type == OrderType.TRAIL_LIMIT:
            if self.price is not None:
                raise ValueError("TRAIL LIMIT orders must use limit_price_offset, not price")
            if self.trail_stop_price is None:
                raise ValueError("trail_stop_price is required for TRAIL LIMIT orders")
            if self.limit_price_offset is None:
                raise ValueError("limit_price_offset is required for TRAIL LIMIT orders")
        if self.sec_type == "FUT" and not (
            self.last_trade_date_or_contract_month or self.local_symbol or self.con_id
        ):
            raise ValueError("future orders require last_trade_date_or_contract_month, local_symbol, or con_id")
        return self


class OrderResponse(BaseModel):
    """Response from placing or modifying an order."""

    model_config = ConfigDict(extra="forbid")

    order_id: int
    status: OrderStatus
    message: str | None = None
    warnings: list[str] = Field(default_factory=list)
    order_uuid: str | None = None


class OrderEnvelope(BaseModel):
    """UUID-tagged order payload for Redis caching.

    Every order action (place, cancel, modify) produces an envelope
    that wraps the original request with a UUID, timestamps, current
    status, and IBKR's order ID once acknowledged.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    order_uuid: str = Field(default_factory=lambda: str(_uuid.uuid4()))
    ibkr_order_id: int | None = None
    action: str  # "place", "cancel", "modify", "preview"
    request: dict[str, Any]  # The original request payload (serialized PlaceOrderRequest, etc.)
    response: dict[str, Any] | None = None  # IBKR response (serialized OrderResponse, etc.)
    status: OrderStatus = OrderStatus.PENDING
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    account_id: str | None = None
    parent_uuid: str | None = None  # Links modify/cancel to original place UUID
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("order_uuid", mode="before")
    @classmethod
    def coerce_uuid(cls, v: Any) -> str:
        if v is None:
            return str(_uuid.uuid4())
        return str(v)

    @field_validator("created_at", "updated_at", mode="before")
    @classmethod
    def normalize_timestamp_utc(cls, value: Any) -> datetime:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime):
            raise TypeError("timestamp must be a datetime")
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def touch(self) -> None:
        """Update the updated_at timestamp."""
        self.updated_at = datetime.now(timezone.utc)


class CachedOrderLookup(BaseModel):
    """Response for a cached order envelope lookup by UUID."""

    model_config = ConfigDict(extra="forbid")

    order_uuid: str
    found: bool
    envelope: OrderEnvelope | None = None


class ModifyOrderRequest(BaseModel):
    """Request body for modifying an existing order."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    price: float | None = Field(default=None, gt=0)
    quantity: float | None = Field(default=None, gt=0)
    tif: TIF | None = None

    @field_validator("price", "quantity")
    @classmethod
    def validate_finite_number(cls, value: float | None) -> float | None:
        if value is not None and not math.isfinite(value):
            raise ValueError("order numeric fields must be finite")
        return value

    @model_validator(mode="after")
    def validate_modification_fields(self) -> "ModifyOrderRequest":
        if self.price is None and self.quantity is None and self.tif is None:
            raise ValueError("at least one of price, quantity, or tif is required")
        return self


class CancelOrderResponse(BaseModel):
    """Response from cancelling an order."""

    model_config = ConfigDict(extra="forbid")

    order_id: int
    status: str
    message: str | None = None


# ---------------------------------------------------------------------------
# Multi-leg order helpers
# ---------------------------------------------------------------------------

class BracketOrderRequest(BaseModel):
    """Request body for placing a bracket order."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    action: OrderAction = OrderAction.BUY
    quantity: float = Field(default=1.0, gt=0)
    limit_price: float | None = Field(default=None, gt=0)
    take_profit_price: float = Field(gt=0)
    stop_loss_price: float = Field(gt=0)
    order_type: OrderType = OrderType.LIMIT
    tif: TIF = TIF.GTC
    asset_class: str = Field(default="EQUITY", min_length=1)
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    account: str = ""

    @field_validator("symbol", "asset_class", "exchange", "currency", "account", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip().upper()

    @model_validator(mode="after")
    def validate_bracket(self) -> "BracketOrderRequest":
        if self.order_type not in {OrderType.MARKET, OrderType.LIMIT}:
            raise ValueError("bracket order_type must be MKT or LMT")
        if self.order_type is OrderType.LIMIT and self.limit_price is None:
            raise ValueError("limit_price is required for LMT bracket orders")
        return self


class BracketOrderResponse(BaseModel):
    """Response from placing a bracket order."""

    model_config = ConfigDict(extra="forbid")

    symbol: str
    action: OrderAction
    quantity: float
    orders_placed: int
    parent_order_id: int | None = None
    take_profit_order_id: int | None = None
    stop_loss_order_id: int | None = None
    order_uuid: str | None = None


class OcaOrderItem(BaseModel):
    """Single order within an OCA group."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    action: OrderAction = OrderAction.BUY
    quantity: float = Field(gt=0)
    order_type: OrderType = OrderType.LIMIT
    tif: TIF = TIF.GTC
    price: float | None = Field(default=None, gt=0)
    asset_class: str = Field(default="EQUITY", min_length=1)
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)

    @field_validator("symbol", "asset_class", "exchange", "currency", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> str:
        if value is None:
            raise ValueError("value is required")
        normalized = str(value).strip().upper()
        if not normalized:
            raise ValueError("value cannot be empty")
        return normalized

    @model_validator(mode="after")
    def validate_oca_order(self) -> "OcaOrderItem":
        if self.order_type not in {OrderType.MARKET, OrderType.LIMIT}:
            raise ValueError("OCA order_type must be MKT or LMT")
        if self.order_type is OrderType.LIMIT and self.price is None:
            raise ValueError("price is required for LMT OCA orders")
        return self


class OcaGroupRequest(BaseModel):
    """Request body for placing an OCA (One-Cancels-All) order group."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    orders: list[OcaOrderItem] = Field(min_length=2)
    oca_group: str = Field(default="")
    oca_type: int = Field(default=1, ge=1, le=3)
    account: str = Field(default="")

    @field_validator("oca_group", "account", mode="before")
    @classmethod
    def normalize_optional_text(cls, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()


class OcaOrderResponse(BaseModel):
    """Single submitted OCA order response."""

    model_config = ConfigDict(extra="forbid")

    symbol: str
    action: OrderAction
    quantity: float
    order_id: int
    oca_group: str
    order_uuid: str | None = None


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
    warnings: list[str] = Field(default_factory=list)


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
        create_time=getattr(order_status, "lastFillTime", None)
        or getattr(order_status, "completedTime", None),
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
    order_state = getattr(trade, "orderState", None) or getattr(trade, "orderStatus", None) or SimpleNamespaceHelper()
    init_margin_before = _safe_float(getattr(order_state, "initMarginBefore", None))
    maint_margin_before = _safe_float(getattr(order_state, "maintMarginBefore", None))
    init_margin_after = _safe_float(getattr(order_state, "initMarginAfter", None))
    maint_margin_after = _safe_float(getattr(order_state, "maintMarginAfter", None))
    equity_with_loan = _safe_float(getattr(order_state, "equityWithLoanAfter", None))
    warnings: list[str] = []
    if init_margin_after is not None and equity_with_loan and equity_with_loan > 0:
        pct = init_margin_after / equity_with_loan
        if pct > 0.8:
            warnings.append(f"Initial margin utilization after trade: {pct:.1%}")
    warning_text = str(getattr(order_state, "warningText", "") or "").strip()
    if warning_text:
        warnings.append(warning_text)
    return WhatIfOrderResponse(
        initial_margin=init_margin_after,
        maintenance_margin=maint_margin_after,
        commission=_safe_float(
            getattr(order_state, "commission", None)
            if getattr(order_state, "commission", None) is not None
            else getattr(order_state, "commissionAndFees", None)
        ),
        equity_with_loan=equity_with_loan,
        init_margin_before=init_margin_before,
        maint_margin_before=maint_margin_before,
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
