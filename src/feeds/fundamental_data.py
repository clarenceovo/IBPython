from __future__ import annotations

import json
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.feeds.contracts import ContractSpec
from src.feeds.models import AssetClass


class FundamentalReportType(StrEnum):
    REPORT_SNAPSHOT = "ReportSnapshot"
    REPORTS_FIN_SUMMARY = "ReportsFinSummary"
    REPORT_RATIOS = "ReportRatios"
    REPORTS_FIN_STATEMENTS = "ReportsFinStatements"
    REPORTS_OWNERSHIP = "ReportsOwnership"
    CALENDAR_REPORT = "CalendarReport"
    RESC = "RESC"


class FundamentalDataRequest(BaseModel):
    """IBKR TWS fundamental report request."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    asset_class: AssetClass = AssetClass.EQUITY
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    primary_exchange: str | None = None
    report_type: FundamentalReportType = FundamentalReportType.REPORT_SNAPSHOT
    source: str = Field(default="ibkr", min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol", "exchange", "currency", mode="before")
    @classmethod
    def normalize_upper_tokens(cls, value: Any) -> str:
        if value is None:
            raise ValueError("value is required")
        return str(value).strip().upper()

    @model_validator(mode="after")
    def validate_supported_asset_class(self) -> Self:
        if self.asset_class is not AssetClass.EQUITY:
            raise ValueError("IBKR fundamental reports are supported for equity-style underlyings")
        return self

    def to_contract_spec(self) -> ContractSpec:
        return ContractSpec(
            symbol=self.symbol,
            asset_class=self.asset_class,
            exchange=self.exchange,
            currency=self.currency,
            primary_exchange=self.primary_exchange,
            metadata=self.metadata,
        )


class FundamentalDataReport(BaseModel):
    """Raw IBKR fundamental report response, usually XML."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1)
    asset_class: AssetClass = AssetClass.EQUITY
    con_id: int | None = Field(default=None, gt=0)
    report_type: FundamentalReportType
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str = Field(default="ibkr", min_length=1)
    raw_xml: str
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: Any) -> str:
        if value is None:
            raise ValueError("value is required")
        return str(value).strip().upper()

    @field_validator("received_at", mode="before")
    @classmethod
    def normalize_received_at(cls, value: Any) -> datetime:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime):
            raise TypeError("received_at must be a datetime")
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class WSHMetadataReport(BaseModel):
    """Wall Street Horizon metadata response from IBKR."""

    model_config = ConfigDict(extra="forbid")

    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str = Field(default="ibkr_wsh", min_length=1)
    raw_json: str
    payload: dict[str, Any] | list[Any]
    metadata: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_raw_json(cls, raw_json: str, **kwargs: Any) -> "WSHMetadataReport":
        return cls(raw_json=raw_json, payload=json.loads(raw_json), **kwargs)


class WSHEventDataRequest(BaseModel):
    """Wall Street Horizon event data request.

    This is corporate/event-calendar data, not a macroeconomic time-series feed.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    con_ids: tuple[int, ...] = Field(default_factory=tuple)
    country: str = "All"
    limit_region: int = Field(default=10, gt=0)
    limit: int = Field(default=10, gt=0)
    event_types: tuple[str, ...] = Field(default_factory=tuple)
    extra_filters: dict[str, Any] = Field(default_factory=dict)
    raw_filter_json: str | None = None

    @field_validator("con_ids", mode="before")
    @classmethod
    def normalize_con_ids(cls, value: Any) -> tuple[int, ...]:
        if value is None:
            return tuple()
        if not isinstance(value, (list, tuple, set)):
            raise TypeError("con_ids must be a sequence of contract ids")
        return tuple(int(item) for item in value)

    @field_validator("event_types", mode="before")
    @classmethod
    def normalize_event_types(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return tuple()
        if not isinstance(value, (list, tuple, set)):
            raise TypeError("event_types must be a sequence of WSH event type tags")
        return tuple(sorted(str(item).strip() for item in value if str(item).strip()))

    def to_filter_json(self) -> str:
        if self.raw_filter_json:
            json.loads(self.raw_filter_json)
            return self.raw_filter_json

        payload: dict[str, Any] = {
            "country": self.country,
            "limit_region": self.limit_region,
            "limit": self.limit,
            **self.extra_filters,
        }
        if self.con_ids:
            payload["watchlist"] = [str(con_id) for con_id in self.con_ids]
        for event_type in self.event_types:
            payload[event_type] = "true"
        return json.dumps(payload, sort_keys=True)


class WSHEventDataReport(BaseModel):
    """Wall Street Horizon event data response from IBKR."""

    model_config = ConfigDict(extra="forbid")

    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str = Field(default="ibkr_wsh", min_length=1)
    request_filter_json: str
    raw_json: str
    payload: dict[str, Any] | list[Any]
    metadata: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_raw_json(cls, *, raw_json: str, request_filter_json: str, **kwargs: Any) -> "WSHEventDataReport":
        return cls(
            raw_json=raw_json,
            request_filter_json=request_filter_json,
            payload=json.loads(raw_json),
            **kwargs,
        )


class ForecastEventContractCategory(BaseModel):
    """Client Portal Forecast/Event Contract category node."""

    model_config = ConfigDict(extra="allow")

    id: str | int | None = None
    name: str | None = None
    level: int | None = None
    children: list["ForecastEventContractCategory"] = Field(default_factory=list)
    raw: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "ForecastEventContractCategory":
        children_payload = payload.get("children") or payload.get("subcategories") or []
        return cls(
            id=payload.get("id") or payload.get("category_id") or payload.get("categoryId"),
            name=payload.get("name") or payload.get("label"),
            level=payload.get("level"),
            children=[cls.from_payload(child) for child in children_payload if isinstance(child, dict)],
            raw=payload,
        )
