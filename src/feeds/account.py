from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class AccountValueDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account: str = Field(description="IBKR account id that reported this value.")
    tag: str = Field(description="IBKR summary tag, e.g. NetLiquidation, TotalCashValue, AvailableFunds, ExcessLiquidity, Cushion, GrossPositionValue.")
    value: str = Field(description="Value as reported by IBKR (string; may contain thousands separators).")
    currency: str = Field(default="", description="ISO 4217 currency of the value; empty when IBKR reports none.")
    model_code: str = Field(default="", description="IBKR model code used for the value; empty for the base model.")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="UTC time the value snapshot was normalized.")


class AccountSummaryDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account: str = Field(description="IBKR account id.")
    values: dict[str, AccountValueDTO] = Field(description="Summary values keyed by IBKR tag (NetLiquidation, TotalCashValue, ...).")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="UTC time the summary was normalized.")


class LivePositionDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account: str = Field(description="IBKR account id holding the position.")
    con_id: int | None = Field(default=None, description="IBKR contract id; null when IBKR reports none.")
    symbol: str = Field(description="Contract symbol, e.g. AAPL or ESZ6.")
    sec_type: str = Field(description="IBKR security type, e.g. STK, OPT, FUT, CASH, IND.")
    exchange: str = Field(default="", description="Listing or routing exchange; empty when IBKR reports none.")
    currency: str = Field(default="", description="ISO 4217 position currency.")
    position: float = Field(description="Signed quantity; negative for short positions.")
    average_cost: float = Field(description="Average open cost per contract in the position currency.")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="UTC time the position snapshot was normalized.")
    # Option contract identity (None for non-option positions).
    strike: float | None = Field(default=None, description="Option strike; null for non-option positions.")
    expiry: str | None = Field(default=None, description="Option expiry as IBKR reports it (YYYYMMDD or YYYYMM); null for non-option positions.")
    right: str | None = Field(default=None, description="Option right: C or P; null for non-option positions.")
    multiplier: float | None = Field(default=None, description="Contract multiplier applied to price and Greeks.")
    local_symbol: str | None = Field(default=None, description="IBKR local symbol, e.g. AAPL 211120C00150000.")
    underlying_symbol: str | None = Field(default=None, description="Underlying symbol for option or futures positions; null otherwise.")


class PortfolioItemDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account: str = Field(description="IBKR account id holding the item.")
    con_id: int | None = Field(default=None, description="IBKR contract id; null when IBKR reports none.")
    symbol: str = Field(description="Contract symbol, e.g. AAPL or ESZ6.")
    sec_type: str = Field(description="IBKR security type, e.g. STK, OPT, FUT, CASH, IND.")
    exchange: str = Field(default="", description="Listing or routing exchange; empty when IBKR reports none.")
    currency: str = Field(default="", description="ISO 4217 item currency.")
    position: float = Field(description="Signed quantity; negative for short positions.")
    market_price: float | None = Field(default=None, description="Last mark price per contract; null when not reported.")
    market_value: float | None = Field(default=None, description="Mark-to-market value of the position; null when not reported.")
    average_cost: float | None = Field(default=None, description="Average open cost per contract; null when not reported.")
    unrealized_pnl: float | None = Field(default=None, description="Unrealized profit and loss in the item currency; null when not reported.")
    realized_pnl: float | None = Field(default=None, description="Realized profit and loss in the item currency; null when not reported.")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="UTC time the item was normalized.")
    # Option contract identity (None for non-option positions).
    strike: float | None = Field(default=None, description="Option strike; null for non-option positions.")
    expiry: str | None = Field(default=None, description="Option expiry as IBKR reports it (YYYYMMDD or YYYYMM); null for non-option positions.")
    right: str | None = Field(default=None, description="Option right: C or P; null for non-option positions.")
    multiplier: float | None = Field(default=None, description="Contract multiplier applied to price and Greeks.")
    local_symbol: str | None = Field(default=None, description="IBKR local symbol.")
    underlying_symbol: str | None = Field(default=None, description="Underlying symbol for option or futures positions; null otherwise.")

    @field_validator("market_price", "market_value", "average_cost", "unrealized_pnl", "realized_pnl", mode="before")
    @classmethod
    def normalize_optional_float(cls, value: Any) -> float | None:
        return _optional_float(value)


class AccountPnLDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account: str = Field(description="IBKR account id.")
    model_code: str = Field(default="", description="IBKR PnL model code; empty for the base model.")
    daily_pnl: float | None = Field(default=None, description="Day profit and loss in the account currency; null when not reported.")
    unrealized_pnl: float | None = Field(default=None, description="Unrealized profit and loss; null when not reported.")
    realized_pnl: float | None = Field(default=None, description="Realized profit and loss; null when not reported.")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="UTC time the PnL snapshot was normalized.")

    @field_validator("daily_pnl", "unrealized_pnl", "realized_pnl", mode="before")
    @classmethod
    def normalize_optional_float(cls, value: Any) -> float | None:
        return _optional_float(value)


class PositionPnLDTO(AccountPnLDTO):
    con_id: int = Field(gt=0, description="IBKR contract id of the position.")
    position: float | None = Field(default=None, description="Signed quantity of the position; null when not reported.")
    value: float | None = Field(default=None, description="Mark-to-market value of the position; null when not reported.")

    @field_validator("position", "value", mode="before")
    @classmethod
    def normalize_position_value(cls, value: Any) -> float | None:
        return _optional_float(value)


def normalize_account_values(values: list[Any]) -> list[AccountValueDTO]:
    return [
        AccountValueDTO(
            account=getattr(value, "account"),
            tag=getattr(value, "tag"),
            value=str(getattr(value, "value")),
            currency=getattr(value, "currency", "") or "",
            model_code=getattr(value, "modelCode", "") or "",
        )
        for value in values
    ]


def group_account_summary(values: list[AccountValueDTO]) -> list[AccountSummaryDTO]:
    grouped: dict[str, dict[str, AccountValueDTO]] = {}
    for value in values:
        grouped.setdefault(value.account, {})[value.tag] = value
    return [AccountSummaryDTO(account=account, values=items) for account, items in grouped.items()]


def _option_identity(contract: Any) -> dict[str, Any]:
    """Extract option identity fields from an IBKR contract; empty when absent."""
    strike = getattr(contract, "strike", None) or None
    expiry = str(getattr(contract, "lastTradeDateOrContractMonth", "") or "").strip() or None
    right = str(getattr(contract, "right", "") or "").strip().upper() or None
    raw_multiplier = getattr(contract, "multiplier", None)
    multiplier = float(raw_multiplier) if raw_multiplier not in (None, "") else None
    local_symbol = str(getattr(contract, "localSymbol", "") or "").strip() or None
    underlying = str(getattr(contract, "symbol", "") or "").strip().upper() or None
    return {
        "strike": float(strike) if strike is not None else None,
        "expiry": expiry,
        "right": right,
        "multiplier": multiplier,
        "local_symbol": local_symbol,
        "underlying_symbol": underlying,
    }


def normalize_positions(positions: list[Any]) -> list[LivePositionDTO]:
    normalized: list[LivePositionDTO] = []
    for item in positions:
        contract = getattr(item, "contract")
        normalized.append(
            LivePositionDTO(
                account=getattr(item, "account"),
                con_id=getattr(contract, "conId", None),
                symbol=getattr(contract, "symbol", ""),
                sec_type=getattr(contract, "secType", ""),
                exchange=getattr(contract, "exchange", ""),
                currency=getattr(contract, "currency", ""),
                position=float(getattr(item, "position")),
                average_cost=float(getattr(item, "avgCost")),
                **_option_identity(contract),
            )
        )
    return normalized


def normalize_portfolio_items(items: list[Any]) -> list[PortfolioItemDTO]:
    normalized: list[PortfolioItemDTO] = []
    for item in items:
        contract = getattr(item, "contract")
        normalized.append(
            PortfolioItemDTO(
                account=getattr(item, "account"),
                con_id=getattr(contract, "conId", None),
                symbol=getattr(contract, "symbol", ""),
                sec_type=getattr(contract, "secType", ""),
                exchange=getattr(contract, "exchange", ""),
                currency=getattr(contract, "currency", ""),
                position=float(getattr(item, "position")),
                market_price=getattr(item, "marketPrice", None),
                market_value=getattr(item, "marketValue", None),
                average_cost=getattr(item, "averageCost", None),
                unrealized_pnl=getattr(item, "unrealizedPNL", None),
                realized_pnl=getattr(item, "realizedPNL", None),
                **_option_identity(contract),
            )
        )
    return normalized


def normalize_account_pnl(value: Any, account: str, model_code: str = "") -> AccountPnLDTO:
    return AccountPnLDTO(
        account=account,
        model_code=model_code,
        daily_pnl=getattr(value, "dailyPnL", None),
        unrealized_pnl=getattr(value, "unrealizedPnL", None),
        realized_pnl=getattr(value, "realizedPnL", None),
    )


def normalize_position_pnl(value: Any, account: str, con_id: int, model_code: str = "") -> PositionPnLDTO:
    return PositionPnLDTO(
        account=account,
        model_code=model_code,
        con_id=con_id,
        position=getattr(value, "position", getattr(value, "pos", None)),
        daily_pnl=getattr(value, "dailyPnL", None),
        unrealized_pnl=getattr(value, "unrealizedPnL", None),
        realized_pnl=getattr(value, "realizedPnL", None),
        value=getattr(value, "value", None),
    )


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    numeric = float(value)
    if not math.isfinite(numeric) or abs(numeric) > 1e300:
        return None
    return numeric
