from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class AccountValueDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account: str
    tag: str
    value: str
    currency: str = ""
    model_code: str = ""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AccountSummaryDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account: str
    values: dict[str, AccountValueDTO]
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class LivePositionDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account: str
    con_id: int | None = None
    symbol: str
    sec_type: str
    exchange: str = ""
    currency: str = ""
    position: float
    average_cost: float
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class PortfolioItemDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account: str
    con_id: int | None = None
    symbol: str
    sec_type: str
    exchange: str = ""
    currency: str = ""
    position: float
    market_price: float | None = None
    market_value: float | None = None
    average_cost: float | None = None
    unrealized_pnl: float | None = None
    realized_pnl: float | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("market_price", "market_value", "average_cost", "unrealized_pnl", "realized_pnl", mode="before")
    @classmethod
    def normalize_optional_float(cls, value: Any) -> float | None:
        return _optional_float(value)


class AccountPnLDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account: str
    model_code: str = ""
    daily_pnl: float | None = None
    unrealized_pnl: float | None = None
    realized_pnl: float | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("daily_pnl", "unrealized_pnl", "realized_pnl", mode="before")
    @classmethod
    def normalize_optional_float(cls, value: Any) -> float | None:
        return _optional_float(value)


class PositionPnLDTO(AccountPnLDTO):
    con_id: int = Field(gt=0)
    position: float | None = None
    value: float | None = None

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
