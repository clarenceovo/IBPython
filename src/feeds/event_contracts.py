"""IBKR Web API models and client for ForecastEx / CME Event Contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


DEFAULT_EVENT_CONTRACT_FIELDS: tuple[str, ...] = ("31", "84", "85", "86", "88", "7059")


class EventContractExchange(StrEnum):
    FORECASTX = "FORECASTX"
    CME = "CME"
    COMEX = "COMEX"
    NYMEX = "NYMEX"
    CBOT = "CBOT"


class EventContractSecType(StrEnum):
    INDEX = "IND"
    OPTION = "OPT"
    FUTURES_OPTION = "FOP"


class EventContractSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class EventContractOrderType(StrEnum):
    LIMIT = "LMT"
    MARKET = "MKT"


class EventContractTIF(StrEnum):
    DAY = "DAY"
    GTC = "GTC"
    IOC = "IOC"


class EventContractCategoryMarket(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    symbol: str
    exchange: str
    con_id: int = Field(gt=0)
    raw: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol", "exchange", mode="before")
    @classmethod
    def normalize_upper(cls, value: Any) -> str:
        return str(value).strip().upper()

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "EventContractCategoryMarket":
        return cls(
            name=str(payload.get("name") or ""),
            symbol=str(payload.get("symbol") or ""),
            exchange=str(payload.get("exchange") or ""),
            con_id=_required_int(payload, "conid", "con_id"),
            raw=dict(payload),
        )


class EventContractCategoryNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    category_id: str
    label: str
    parent_id: str | None = None
    markets: tuple[EventContractCategoryMarket, ...] = Field(default_factory=tuple)
    raw: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_payload(cls, category_id: str, payload: Mapping[str, Any]) -> "EventContractCategoryNode":
        markets = tuple(
            EventContractCategoryMarket.from_payload(item)
            for item in payload.get("markets", [])
            if isinstance(item, Mapping)
        )
        return cls(
            category_id=category_id,
            label=str(payload.get("label") or payload.get("name") or ""),
            parent_id=str(payload["parentId"]) if payload.get("parentId") is not None else None,
            markets=markets,
            raw=dict(payload),
        )


class EventContractSearchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1, examples=["FF", "NQ"])
    sec_type: EventContractSecType | None = Field(default=None, description="Use IND for CME underliers; omit for ForecastEx search.")
    exchange: str | None = Field(default=None, min_length=1)

    @field_validator("symbol", "exchange", mode="before")
    @classmethod
    def normalize_optional_upper(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None


class EventContractSearchResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    con_id: int = Field(gt=0)
    symbol: str
    description: str = ""
    company_name: str = ""
    company_header: str = ""
    opt_expirations: tuple[str, ...] = Field(default_factory=tuple)
    fop_expirations: tuple[str, ...] = Field(default_factory=tuple)
    sections: tuple[dict[str, Any], ...] = Field(default_factory=tuple)
    raw: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "EventContractSearchResult":
        return cls(
            con_id=_required_int(payload, "conid", "con_id"),
            symbol=str(payload.get("symbol") or "").upper(),
            description=str(payload.get("description") or ""),
            company_name=str(payload.get("companyName") or payload.get("company_name") or ""),
            company_header=str(payload.get("companyHeader") or payload.get("company_header") or ""),
            opt_expirations=_split_semicolon_dates(payload.get("opt")),
            fop_expirations=_split_semicolon_dates(payload.get("fop")),
            sections=tuple(dict(item) for item in payload.get("sections", []) if isinstance(item, Mapping)),
            raw=dict(payload),
        )


class EventContractStrikesRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    underlying_con_id: int = Field(gt=0)
    exchange: str = Field(default=EventContractExchange.FORECASTX.value, min_length=1)
    sec_type: EventContractSecType = EventContractSecType.OPTION
    month: str = Field(min_length=1, description="IBKR Web API option month such as SEP24.")

    @field_validator("exchange", "month", mode="before")
    @classmethod
    def normalize_upper(cls, value: Any) -> str:
        return str(value).strip().upper()


class EventContractStrikesResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    call: tuple[float, ...] = Field(default_factory=tuple)
    put: tuple[float, ...] = Field(default_factory=tuple)
    all_strikes: tuple[float, ...] = Field(default_factory=tuple)
    raw: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "EventContractStrikesResponse":
        calls = tuple(sorted(float(item) for item in payload.get("call", []) if item is not None))
        puts = tuple(sorted(float(item) for item in payload.get("put", []) if item is not None))
        return cls(call=calls, put=puts, all_strikes=tuple(sorted(set(calls) | set(puts))), raw=dict(payload))


class EventContractInfoRequest(EventContractStrikesRequest):
    strike: float | None = Field(default=None, gt=0)
    right: Literal["C", "P"] | None = None
    trading_class_prefix: str | None = Field(
        default=None,
        description="Optional filter, useful for CME Event Contracts whose trading classes often start with EC.",
    )

    @field_validator("right", "trading_class_prefix", mode="before")
    @classmethod
    def normalize_optional_upper(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None


class EventContractInstrument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    con_id: int = Field(gt=0)
    symbol: str
    sec_type: str
    exchange: str
    right: Literal["C", "P"] | str = ""
    yes_no: Literal["YES", "NO"] | None = None
    strike: float | None = None
    currency: str = "USD"
    desc1: str = ""
    desc2: str = ""
    maturity_date: str = ""
    multiplier: str = ""
    trading_class: str = ""
    valid_exchanges: str = ""
    raw: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "EventContractInstrument":
        right = str(payload.get("right") or "").upper()
        return cls(
            con_id=_required_int(payload, "conid", "con_id"),
            symbol=str(payload.get("symbol") or "").upper(),
            sec_type=str(payload.get("secType") or payload.get("sec_type") or "").upper(),
            exchange=str(payload.get("exchange") or "").upper(),
            right=right,
            yes_no="YES" if right == "C" else "NO" if right == "P" else None,
            strike=_optional_float(payload.get("strike")),
            currency=str(payload.get("currency") or "USD").upper(),
            desc1=str(payload.get("desc1") or ""),
            desc2=str(payload.get("desc2") or ""),
            maturity_date=str(payload.get("maturityDate") or payload.get("maturity_date") or ""),
            multiplier=str(payload.get("multiplier") or ""),
            trading_class=str(payload.get("tradingClass") or payload.get("trading_class") or "").upper(),
            valid_exchanges=str(payload.get("validExchanges") or payload.get("valid_exchanges") or ""),
            raw=dict(payload),
        )


class EventContractSnapshotRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    con_ids: tuple[int, ...] = Field(min_length=1, max_length=100)
    fields: tuple[str, ...] = Field(default=DEFAULT_EVENT_CONTRACT_FIELDS, min_length=1, max_length=50)

    @field_validator("con_ids", mode="before")
    @classmethod
    def normalize_con_ids(cls, value: Any) -> tuple[int, ...]:
        if not isinstance(value, (list, tuple, set)):
            raise TypeError("con_ids must be a sequence")
        return tuple(int(item) for item in value)

    @field_validator("fields", mode="before")
    @classmethod
    def normalize_fields(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return DEFAULT_EVENT_CONTRACT_FIELDS
        if not isinstance(value, (list, tuple, set)):
            raise TypeError("fields must be a sequence")
        return tuple(str(item).strip() for item in value if str(item).strip())


class EventContractMarketData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    con_id: int = Field(gt=0)
    con_id_ex: str = ""
    updated_at: datetime | None = None
    last: float | None = None
    bid: float | None = None
    bid_size: float | None = None
    ask: float | None = None
    ask_size: float | None = None
    last_size: float | None = None
    raw: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "EventContractMarketData":
        updated = _optional_float(payload.get("_updated"))
        return cls(
            con_id=_required_int(payload, "conid", "con_id"),
            con_id_ex=str(payload.get("conidEx") or payload.get("con_id_ex") or ""),
            updated_at=datetime.fromtimestamp(updated / 1000, tz=timezone.utc) if updated else None,
            last=_optional_float(payload.get("31")),
            bid=_optional_float(payload.get("84")),
            bid_size=_optional_float(payload.get("88")),
            ask=_optional_float(payload.get("86")),
            ask_size=_optional_float(payload.get("85")),
            last_size=_optional_float(payload.get("7059")),
            raw=dict(payload),
        )


class EventContractHistoryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    con_id: int = Field(gt=0)
    period: str = Field(default="2d", min_length=1)
    bar: str = Field(default="1h", min_length=1)
    start_time: datetime | str | None = None
    outside_rth: bool | None = None

    @field_validator("period", "bar", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> str:
        return str(value).strip()


class EventContractHistoryBar(BaseModel):
    model_config = ConfigDict(extra="forbid")

    timestamp: datetime
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float | None = None
    raw: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "EventContractHistoryBar":
        timestamp_value = payload.get("t")
        timestamp = (
            datetime.fromtimestamp(float(timestamp_value) / 1000, tz=timezone.utc)
            if timestamp_value is not None
            else datetime.now(timezone.utc)
        )
        return cls(
            timestamp=timestamp,
            open=_optional_float(payload.get("o")),
            high=_optional_float(payload.get("h")),
            low=_optional_float(payload.get("l")),
            close=_optional_float(payload.get("c")),
            volume=_optional_float(payload.get("v")),
            raw=dict(payload),
        )


class EventContractHistoryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    con_id: int
    symbol: str = ""
    text: str = ""
    period: str = ""
    bar_length_seconds: int | None = None
    bars: tuple[EventContractHistoryBar, ...] = Field(default_factory=tuple)
    raw: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_payload(cls, con_id: int, payload: Mapping[str, Any]) -> "EventContractHistoryResponse":
        return cls(
            con_id=con_id,
            symbol=str(payload.get("symbol") or ""),
            text=str(payload.get("text") or ""),
            period=str(payload.get("timePeriod") or ""),
            bar_length_seconds=int(payload["barLength"]) if payload.get("barLength") is not None else None,
            bars=tuple(EventContractHistoryBar.from_payload(item) for item in payload.get("data", []) if isinstance(item, Mapping)),
            raw=dict(payload),
        )


class EventContractStreamingMessageRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    con_id: int = Field(gt=0)
    fields: tuple[str, ...] = Field(default=DEFAULT_EVENT_CONTRACT_FIELDS, min_length=1, max_length=50)

    @field_validator("fields", mode="before")
    @classmethod
    def normalize_fields(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return DEFAULT_EVENT_CONTRACT_FIELDS
        if not isinstance(value, (list, tuple, set)):
            raise TypeError("fields must be a sequence")
        return tuple(str(item).strip() for item in value if str(item).strip())


class EventContractStreamingMessages(BaseModel):
    model_config = ConfigDict(extra="forbid")

    subscribe: str
    unsubscribe: str


class EventContractOrderTicket(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conid: int = Field(gt=0)
    side: EventContractSide
    orderType: EventContractOrderType
    quantity: float = Field(gt=0)
    tif: EventContractTIF = EventContractTIF.DAY
    price: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_price(self) -> "EventContractOrderTicket":
        if self.orderType is EventContractOrderType.LIMIT and self.price is None:
            raise ValueError("price is required for LMT event contract orders")
        return self


class EventContractOrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    account_id: str = Field(min_length=1)
    con_id: int = Field(gt=0)
    side: EventContractSide = EventContractSide.BUY
    order_type: EventContractOrderType = EventContractOrderType.LIMIT
    quantity: float = Field(gt=0)
    price: float | None = Field(default=None, gt=0)
    tif: EventContractTIF = EventContractTIF.DAY
    exchange: str = Field(default=EventContractExchange.FORECASTX.value, min_length=1)
    confirm_live_order: bool = False

    @field_validator("account_id", "exchange", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> str:
        return str(value).strip().upper()

    @model_validator(mode="after")
    def validate_event_contract_rules(self) -> "EventContractOrderRequest":
        if self.exchange == EventContractExchange.FORECASTX.value and self.side is EventContractSide.SELL:
            raise ValueError("ForecastEx event contracts cannot be sold; buy the opposing YES/NO contract to reduce or flip exposure")
        if self.order_type is EventContractOrderType.LIMIT and self.price is None:
            raise ValueError("price is required for LMT event contract orders")
        return self

    def to_ticket(self) -> EventContractOrderTicket:
        return EventContractOrderTicket(
            conid=self.con_id,
            side=self.side,
            orderType=self.order_type,
            quantity=self.quantity,
            tif=self.tif,
            price=self.price,
        )


class EventContractOrderBuildResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account_id: str
    live_order_enabled: bool
    ticket: EventContractOrderTicket
    warnings: tuple[str, ...] = Field(default_factory=tuple)


class EventContractOrderResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account_id: str
    submitted: bool
    response: dict[str, Any] | list[Any]
    warnings: tuple[str, ...] = Field(default_factory=tuple)


class IBKRWebAPIClient:
    """Small async client for the IBKR Web API endpoints used by Event Contracts."""

    def __init__(
        self,
        *,
        base_url: str,
        bearer_token: str = "",
        cookie: str = "",
        verify_ssl: bool = False,
        timeout_seconds: float = 20.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.bearer_token = bearer_token.strip()
        self.cookie = cookie.strip()
        self.verify_ssl = verify_ssl
        self.timeout_seconds = timeout_seconds
        self._transport = transport
        self._client: httpx.AsyncClient | None = None

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Accept": "application/json"}
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        if self.cookie:
            headers["Cookie"] = self.cookie
        return headers

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers=self._headers(),
                verify=self.verify_ssl,
                timeout=self.timeout_seconds,
                transport=self._transport,
            )
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def load_category_tree(self) -> list[EventContractCategoryNode]:
        payload = await self._request("GET", "/trsrv/event/category-tree")
        if not isinstance(payload, Mapping):
            raise ValueError("event category tree response must be an object")
        return [EventContractCategoryNode.from_payload(str(key), value) for key, value in payload.items() if isinstance(value, Mapping)]

    async def search(self, request: EventContractSearchRequest) -> list[EventContractSearchResult]:
        params: dict[str, Any] = {"symbol": request.symbol}
        if request.sec_type is not None:
            params["secType"] = request.sec_type.value
        if request.exchange:
            params["exchange"] = request.exchange
        payload = await self._request("GET", "/iserver/secdef/search", params=params)
        if not isinstance(payload, list):
            raise ValueError("event contract search response must be a list")
        return [EventContractSearchResult.from_payload(item) for item in payload if isinstance(item, Mapping)]

    async def strikes(self, request: EventContractStrikesRequest) -> EventContractStrikesResponse:
        payload = await self._request(
            "GET",
            "/iserver/secdef/strikes",
            params={
                "conid": request.underlying_con_id,
                "exchange": request.exchange,
                "sectype": request.sec_type.value,
                "month": request.month,
            },
        )
        if not isinstance(payload, Mapping):
            raise ValueError("event contract strikes response must be an object")
        return EventContractStrikesResponse.from_payload(payload)

    async def info(self, request: EventContractInfoRequest) -> list[EventContractInstrument]:
        params: dict[str, Any] = {
            "conid": request.underlying_con_id,
            "exchange": request.exchange,
            "sectype": request.sec_type.value,
            "month": request.month,
        }
        if request.strike is not None:
            params["strike"] = request.strike
        payload = await self._request("GET", "/iserver/secdef/info", params=params)
        if not isinstance(payload, list):
            raise ValueError("event contract info response must be a list")
        instruments = [EventContractInstrument.from_payload(item) for item in payload if isinstance(item, Mapping)]
        if request.right:
            instruments = [item for item in instruments if item.right == request.right]
        if request.trading_class_prefix:
            instruments = [item for item in instruments if item.trading_class.startswith(request.trading_class_prefix)]
        return instruments

    async def snapshot(self, request: EventContractSnapshotRequest) -> list[EventContractMarketData]:
        payload = await self._request(
            "GET",
            "/iserver/marketdata/snapshot",
            params={
                "conids": ",".join(str(item) for item in request.con_ids),
                "fields": ",".join(request.fields),
            },
        )
        if not isinstance(payload, list):
            raise ValueError("event contract market-data response must be a list")
        return [EventContractMarketData.from_payload(item) for item in payload if isinstance(item, Mapping) and item.get("conid")]

    async def history(self, request: EventContractHistoryRequest) -> EventContractHistoryResponse:
        params: dict[str, Any] = {"conid": request.con_id, "period": request.period, "bar": request.bar}
        if request.start_time is not None:
            params["startTime"] = _web_api_time(request.start_time)
        if request.outside_rth is not None:
            params["outsideRth"] = str(request.outside_rth).lower()
        payload = await self._request("GET", "/iserver/marketdata/history", params=params)
        if not isinstance(payload, Mapping):
            raise ValueError("event contract history response must be an object")
        return EventContractHistoryResponse.from_payload(request.con_id, payload)

    async def place_order(self, request: EventContractOrderRequest) -> EventContractOrderResponse:
        ticket = request.to_ticket()
        payload = await self._request(
            "POST",
            f"/iserver/account/{request.account_id}/orders",
            json=[ticket.model_dump(mode="json", exclude_none=True)],
        )
        if not isinstance(payload, (dict, list)):
            raise ValueError("event contract order response must be an object or list")
        return EventContractOrderResponse(
            account_id=request.account_id,
            submitted=True,
            response=payload,
            warnings=("IBKR order reply messages are returned raw; this client does not auto-confirm /iserver/reply prompts.",),
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
    ) -> Any:
        response = await self._get_client().request(method, path, params=params, json=json)
        response.raise_for_status()
        if not response.content:
            return {}
        return response.json()


def event_contract_streaming_messages(request: EventContractStreamingMessageRequest) -> EventContractStreamingMessages:
    fields = ",".join(f'"{field}"' for field in request.fields)
    return EventContractStreamingMessages(
        subscribe=f"smd+{request.con_id}+{{\"fields\":[{fields}]}}",
        unsubscribe=f"umd+{request.con_id}+{{}}",
    )


def _split_semicolon_dates(value: Any) -> tuple[str, ...]:
    if value is None:
        return tuple()
    if isinstance(value, Sequence) and not isinstance(value, str):
        return tuple(str(item).strip().upper() for item in value if str(item).strip())
    return tuple(item.strip().upper() for item in str(value).split(";") if item.strip())


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        value = value.replace(",", "")
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _required_int(payload: Mapping[str, Any], *names: str) -> int:
    for name in names:
        value = payload.get(name)
        if value is not None and value != "":
            return int(value)
    raise ValueError(f"missing required integer field: {'/'.join(names)}")


def _web_api_time(value: datetime | str) -> str:
    if isinstance(value, str):
        return value
    normalized = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return normalized.astimezone(timezone.utc).strftime("%Y%m%d-%H:%M:%S")
