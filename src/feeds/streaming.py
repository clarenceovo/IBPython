from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.feeds.models import AssetClass


class StreamingTickerSnapshot(BaseModel):
    """Real-time ticker snapshot for SSE delivery."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1)
    asset_class: AssetClass
    exchange: str = Field(min_length=1)
    currency: str = Field(min_length=1)
    timestamp: datetime = Field(default_factory=lambda: datetime.now())
    last: float | None = None
    bid: float | None = None
    ask: float | None = None
    bid_size: float | None = None
    ask_size: float | None = None
    volume: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    open: float | None = None
    vwap: float | None = None
    implied_volatility: float | None = None
    mark_price: float | None = None
    halted: bool | None = None

    @field_validator("symbol", "exchange", "currency", mode="before")
    @classmethod
    def normalize_upper(cls, v: Any) -> str:
        return str(v).strip().upper() if v else ""


class StreamSubscription(BaseModel):
    """Track an active SSE stream subscription."""

    model_config = ConfigDict(extra="forbid")

    subscription_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    asset_class: AssetClass
    exchange: str = Field(min_length=1)
    currency: str = Field(min_length=1)
    connected_at: datetime
    updates_sent: int = 0


class StreamRequest(BaseModel):
    """Request to start a real-time data stream."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, examples=["SPY", "AAPL", "EURUSD"])
    asset_class: AssetClass = Field(default=AssetClass.EQUITY)
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    primary_exchange: str | None = None
    update_interval_seconds: float = Field(
        default=0.5,
        ge=0.1,
        le=60.0,
        description="Minimum seconds between SSE updates",
    )
    max_updates: int = Field(
        default=0,
        ge=0,
        description="Max updates before auto-stop. 0 = unlimited.",
    )
    generic_tick_list: str = Field(
        default="",
        description="Comma-separated generic tick types (e.g. '100,101,104')",
    )
