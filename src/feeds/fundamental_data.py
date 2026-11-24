from __future__ import annotations

import json
from datetime import date, datetime, timezone
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
    """Legacy IBKR TWS fundamental report request retained for 410 responses."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1, description="Underlying symbol, e.g. AAPL or 0700.HK notation per asset class.")
    asset_class: AssetClass = Field(default=AssetClass.EQUITY, description="Underlying asset class; only equity is accepted by this legacy endpoint.")
    exchange: str = Field(default="SMART", min_length=1, description="Exchange or SMART for routing.")
    currency: str = Field(default="USD", min_length=1, description="ISO 4217 currency, e.g. USD.")
    primary_exchange: str | None = Field(default=None, description="Primary listing exchange for SMART-routed equities, e.g. NASDAQ.")
    report_type: FundamentalReportType = Field(default=FundamentalReportType.REPORT_SNAPSHOT, description="IBKR fundamental report type.")
    source: str = Field(default="ibkr", min_length=1, description="Data source identifier.")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Passthrough request metadata; preserved on the outgoing contract spec.")

    @field_validator("symbol", "exchange", "currency", mode="before")
    @classmethod
    def normalize_upper_tokens(cls, value: Any) -> str:
        if value is None:
            raise ValueError("value is required")
        return str(value).strip().upper()

    @model_validator(mode="after")
    def validate_supported_asset_class(self) -> Self:
        if self.asset_class is not AssetClass.EQUITY:
            raise ValueError("Legacy IBKR fundamental report requests accept only equity-style underlyings")
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
    """Legacy raw IBKR fundamental report response model, usually XML."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, description="Underlying symbol the report covers.")
    asset_class: AssetClass = Field(default=AssetClass.EQUITY, description="Underlying asset class (equity only).")
    con_id: int | None = Field(default=None, gt=0, description="IBKR contract id of the underlying; null when not qualified.")
    report_type: FundamentalReportType = Field(description="IBKR fundamental report type.")
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="UTC time the report was received.")
    source: str = Field(default="ibkr", min_length=1, description="Data source identifier.")
    raw_xml: str = Field(description="Raw IBKR fundamental report document (usually XML).")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Passthrough metadata about the request/session.")

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

    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="UTC time the report was received.")
    source: str = Field(default="ibkr_wsh", min_length=1, description="Data source identifier.")
    raw_json: str = Field(description="Raw WSH metadata JSON document exactly as IBKR returned it.")
    payload: dict[str, Any] | list[Any] = Field(description="Parsed WSH metadata payload (event-type tags, filters, countries); passthrough of IBKR JSON.")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Passthrough metadata about the request/session.")

    @classmethod
    def from_raw_json(cls, raw_json: str, **kwargs: Any) -> "WSHMetadataReport":
        return cls(raw_json=raw_json, payload=json.loads(raw_json), **kwargs)


class WSHEventDataRequest(BaseModel):
    """Wall Street Horizon event data request.

    This is corporate/event-calendar data, not a macroeconomic time-series feed.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    con_id: int | None = Field(default=None, gt=0, description="Single IBKR contract id for one-contract event lookups; uses WshEventData object mode with date bounds.")
    con_ids: tuple[int, ...] = Field(default_factory=tuple, description="IBKR contract ids for JSON filter mode; ignored when empty.")
    country: str = Field(default="All", description="Country filter for JSON filter mode, e.g. US, HK, or All.")
    limit_region: int = Field(default=10, gt=0, description="Per-region event limit for JSON filter mode.")
    limit: int = Field(default=10, gt=0, description="Per-contract event limit for JSON filter mode.")
    event_types: tuple[str, ...] = Field(default_factory=tuple, description="WSH event-type tags (e.g. wshe_ed for earnings); discover tags via /reference-data/wsh/metadata. IBKR accepts one tag per request.")
    extra_filters: dict[str, Any] = Field(default_factory=dict, description="Additional WSH JSON filter keys; passthrough to IBKR.")
    raw_filter_json: str | None = Field(default=None, description="Raw WSH filter JSON string; overrides the composed filter fields when set.")
    fill_watchlist: bool = Field(default=False, description="Include events for the account watchlist (object mode).")
    fill_portfolio: bool = Field(default=False, description="Include events for portfolio underlyings (object mode).")
    fill_competitors: bool = Field(default=False, description="Include events for portfolio competitors (object mode).")
    start_date: date | None = Field(default=None, description="Window start as YYYYMMDD (object mode; do not combine with JSON filter fields).")
    end_date: date | None = Field(default=None, description="Window end as YYYYMMDD (object mode; do not combine with JSON filter fields).")
    total_limit: int | None = Field(default=None, gt=0, le=100, description="Overall response cap across regions/contracts; IBKR allows at most 100.")

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

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def normalize_wsh_date(cls, value: Any) -> date | None:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        token = str(value).strip()
        if len(token) == 8 and token.isdigit():
            return datetime.strptime(token, "%Y%m%d").date()
        return date.fromisoformat(token)

    @model_validator(mode="after")
    def validate_wsh_request_mode(self) -> Self:
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be on or before end_date")
        if len(self.event_types) > 1:
            raise ValueError("IBKR WSH accepts only one event type tag per request")

        filter_fields = {
            "con_ids",
            "country",
            "limit_region",
            "limit",
            "event_types",
            "extra_filters",
            "raw_filter_json",
        }
        object_fields = {"con_id", "start_date", "end_date"}
        explicitly_set_filter_fields = filter_fields.intersection(self.model_fields_set)
        explicitly_set_object_fields = object_fields.intersection(self.model_fields_set)
        if explicitly_set_filter_fields and explicitly_set_object_fields:
            filter_field_list = ", ".join(sorted(explicitly_set_filter_fields))
            object_field_list = ", ".join(sorted(explicitly_set_object_fields))
            raise ValueError(
                "IBKR WshEventData filter mode cannot be combined with "
                f"object field(s) {object_field_list}; remove filter field(s): {filter_field_list}"
            )
        return self

    @property
    def uses_filter_json(self) -> bool:
        object_fields = {"con_id", "start_date", "end_date"}
        return not object_fields.intersection(self.model_fields_set)

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

    @staticmethod
    def _format_wsh_date(value: date) -> str:
        return value.strftime("%Y%m%d")

    def to_wsh_event_data_kwargs(self) -> dict[str, Any]:
        """Build kwargs for ib_async.WshEventData using documented IBKR fields."""
        kwargs: dict[str, Any] = {}
        if self.con_id is not None:
            kwargs["conId"] = self.con_id
        if self.uses_filter_json:
            kwargs["filter"] = self.to_filter_json()

        if self.fill_watchlist:
            kwargs["fillWatchlist"] = True
        if self.fill_portfolio:
            kwargs["fillPortfolio"] = True
        if self.fill_competitors:
            kwargs["fillCompetitors"] = True
        if self.start_date is not None:
            kwargs["startDate"] = self._format_wsh_date(self.start_date)
        if self.end_date is not None:
            kwargs["endDate"] = self._format_wsh_date(self.end_date)
        if self.total_limit is not None:
            kwargs["totalLimit"] = self.total_limit
        return kwargs


class WSHEventDataReport(BaseModel):
    """Wall Street Horizon event data response from IBKR."""

    model_config = ConfigDict(extra="forbid")

    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="UTC time the report was received.")
    source: str = Field(default="ibkr_wsh", min_length=1, description="Data source identifier.")
    request_filter_json: str = Field(description="The JSON filter IBKR applied for this request.")
    raw_json: str = Field(description="Raw WSH event JSON document exactly as IBKR returned it.")
    payload: dict[str, Any] | list[Any] = Field(description="Parsed WSH event payload; passthrough of IBKR JSON (contents depend on entitlement).")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Passthrough metadata; includes available event-type tags when ensure_metadata was set.")

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
