from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from src.feeds.contracts import OptionChain, OptionChainRequest
from src.feeds.fundamental_data import (
    FundamentalDataReport,
    FundamentalDataRequest,
    WSHEventDataReport,
    WSHEventDataRequest,
    WSHMetadataReport,
)
from src.feeds.news import (
    HistoricalNewsHeadline,
    HistoricalNewsRequest,
    NewsArticle,
    NewsArticleRequest,
    NewsProvider,
)
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

router = APIRouter(prefix="/reference-data", tags=["reference-data"])


OPTION_CHAIN_REQUEST_EXAMPLES = {
    "tsla_equity_smart": {
        "summary": "TSLA equity option chain",
        "description": "SMART-routed US equities should include primary_exchange to remove IBKR contract ambiguity.",
        "value": {
            "request": {
                "symbol": "TSLA",
                "asset_class": "equity",
                "exchange": "SMART",
                "currency": "USD",
                "primary_exchange": "NASDAQ",
            },
            "use_ttl_cache": True,
            "cache_ttl_seconds": 300,
        },
    },
    "spx_index_cboe": {
        "summary": "SPX index option chain",
        "description": "Index option chains use asset_class=index and the exchange where the index option complex is listed.",
        "value": {
            "request": {
                "symbol": "SPX",
                "asset_class": "index",
                "exchange": "CBOE",
                "currency": "USD",
            },
            "use_ttl_cache": True,
            "cache_ttl_seconds": 300,
        },
    },
    "known_underlying_con_id": {
        "summary": "Known underlying conId",
        "description": "If you already know the IBKR underlying conId, pass it to skip underlying qualification. Verify conIds with your own IBKR session before relying on them operationally.",
        "value": {
            "request": {
                "symbol": "TSLA",
                "asset_class": "equity",
                "exchange": "SMART",
                "currency": "USD",
                "primary_exchange": "NASDAQ",
                "underlying_con_id": 76792991,
            },
            "use_ttl_cache": True,
            "cache_ttl_seconds": 300,
        },
    },
}


class CachedOptionChainRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: OptionChainRequest
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=300, ge=0)


class EconomicCalendarRequest(WSHEventDataRequest):
    """Convenience request for IBKR Wall Street Horizon calendar/event data."""

    limit_region: int = Field(default=50, gt=0, le=500)
    limit: int = Field(default=50, gt=0, le=500)
    total_limit: int | None = Field(default=100, gt=0, le=100)
    ensure_metadata: bool = True
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=300, ge=0)

    def to_wsh_request(self) -> WSHEventDataRequest:
        if not self.uses_filter_json:
            return WSHEventDataRequest(
                con_id=self.con_id,
                fill_watchlist=self.fill_watchlist,
                fill_portfolio=self.fill_portfolio,
                fill_competitors=self.fill_competitors,
                start_date=self.start_date,
                end_date=self.end_date,
                total_limit=self.total_limit,
            )
        return WSHEventDataRequest(
            con_ids=self.con_ids,
            country=self.country,
            limit_region=self.limit_region,
            limit=self.limit,
            event_types=self.event_types,
            extra_filters=self.extra_filters,
            raw_filter_json=self.raw_filter_json,
            fill_watchlist=self.fill_watchlist,
            fill_portfolio=self.fill_portfolio,
            fill_competitors=self.fill_competitors,
            start_date=self.start_date,
            end_date=self.end_date,
            total_limit=self.total_limit,
        )


class EconomicCalendarResponse(BaseModel):
    """IBKR WSH calendar response with explicit source caveats."""

    model_config = ConfigDict(extra="forbid")

    received_at: datetime
    source: str = "ibkr_wsh"
    calendar_type: str = "wall_street_horizon_event_calendar"
    dedicated_macro_calendar_endpoint_available: bool = False
    subscription_required: str = "Wall Street Horizon Corporate Event Data"
    caveats: tuple[str, ...] = (
        "IBKR exposes this through Wall Street Horizon event calendar data, not a dedicated macroeconomic-release calendar endpoint.",
        "Available filters and event-type tags are entitlement/session dependent; call /reference-data/wsh/metadata to inspect them.",
        "A Wall Street Horizon Corporate Event Data research subscription must be enabled for the account.",
    )
    request_filter_json: str
    raw_json: str
    payload: dict[str, Any] | list[Any]
    metadata: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_wsh_report(cls, report: WSHEventDataReport) -> "EconomicCalendarResponse":
        return cls(
            received_at=report.received_at,
            source=report.source,
            request_filter_json=report.request_filter_json,
            raw_json=report.raw_json,
            payload=report.payload,
            metadata=report.metadata,
        )


ECONOMIC_CALENDAR_EXAMPLES = {
    "all_us_wsh_events": {
        "summary": "US WSH events",
        "description": "Returns WSH calendar events using JSON filter mode when the account is entitled.",
        "value": {
            "country": "US",
            "limit": 50,
            "total_limit": 100,
            "use_ttl_cache": True,
        },
    },
    "watchlist_date_range": {
        "summary": "Watchlist date-bounded lookup",
        "description": "Date bounds use WshEventData object mode; do not combine them with JSON filter fields.",
        "value": {
            "fill_watchlist": True,
            "start_date": "20260601",
            "end_date": "20260630",
            "total_limit": 50,
            "use_ttl_cache": True,
        },
    },
    "single_contract_earnings": {
        "summary": "Single contract event lookup",
        "description": "Use con_id mode for date-bounded WSH event lookups on one IBKR contract.",
        "value": {
            "con_id": 8314,
            "start_date": "20260601",
            "end_date": "20260630",
            "total_limit": 50,
            "use_ttl_cache": True,
        },
    },
    "metadata_filter_tags": {
        "summary": "Filter by WSH tags",
        "description": "Event-type tags are returned by /reference-data/wsh/metadata and can change by entitlement/session.",
        "value": {
            "country": "All",
            "con_ids": [8314],
            "event_types": ["wshe_ed"],
            "limit": 10,
            "use_ttl_cache": False,
        },
    },
}


@router.post("/options/chains", response_model=list[OptionChain])
async def load_option_chains(
    payload: Annotated[CachedOptionChainRequest, Body(openapi_examples=OPTION_CHAIN_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OptionChain]:
    async def load() -> list[OptionChain]:
        return await state.feed.load_option_chains(payload.request)

    if payload.use_ttl_cache:
        key = stable_cache_key("option_chains", payload.request)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()


@router.post("/fundamentals", response_model=FundamentalDataReport)
async def load_fundamental_data(
    request: FundamentalDataRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> FundamentalDataReport:
    return await state.feed.load_fundamental_data(request)


@router.get("/wsh/metadata", response_model=WSHMetadataReport)
async def load_wsh_metadata(state: IBKRRestAppState = Depends(get_rest_state)) -> WSHMetadataReport:
    return await state.feed.load_wsh_metadata()


@router.post("/wsh/events", response_model=WSHEventDataReport)
async def load_wsh_event_data(
    request: WSHEventDataRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> WSHEventDataReport:
    return await state.feed.load_wsh_event_data(request)


@router.post("/economic-calendar", response_model=EconomicCalendarResponse)
async def load_economic_calendar(
    payload: Annotated[EconomicCalendarRequest, Body(openapi_examples=ECONOMIC_CALENDAR_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> EconomicCalendarResponse:
    request = payload.to_wsh_request()

    async def load() -> EconomicCalendarResponse:
        report = await state.feed.load_wsh_event_data(request, ensure_metadata=payload.ensure_metadata)
        return EconomicCalendarResponse.from_wsh_report(report)

    if payload.use_ttl_cache:
        key = stable_cache_key("economic_calendar", payload)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()


@router.get("/news/providers", response_model=list[NewsProvider])
async def load_news_providers(state: IBKRRestAppState = Depends(get_rest_state)) -> list[NewsProvider]:
    return await state.feed.load_news_providers()


@router.post("/news/historical", response_model=list[HistoricalNewsHeadline])
async def load_historical_news(
    request: HistoricalNewsRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[HistoricalNewsHeadline]:
    await _ensure_entitled_news_provider_codes(request.provider_codes, state)
    return await state.feed.load_historical_news(request)


@router.post("/news/article", response_model=NewsArticle)
async def load_news_article(
    request: NewsArticleRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> NewsArticle:
    return await state.feed.load_news_article(request)


async def _ensure_entitled_news_provider_codes(provider_codes: tuple[str, ...], state: IBKRRestAppState) -> None:
    providers = await state.feed.load_news_providers()
    entitled_provider_codes = tuple(provider.provider_code for provider in providers)
    if not entitled_provider_codes:
        raise HTTPException(status_code=503, detail="IBKR news providers are not available for this account")
    entitled_set = set(entitled_provider_codes)
    unsupported = tuple(code for code in provider_codes if code not in entitled_set)
    if unsupported:
        raise HTTPException(
            status_code=422,
            detail=(
                "IBKR news provider code(s) not entitled for this account: "
                f"{', '.join(unsupported)}. Entitled provider codes: {', '.join(entitled_provider_codes)}"
            ),
        )
