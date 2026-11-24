from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query
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
    """Cached option-chain request envelope."""

    model_config = ConfigDict(extra="forbid")

    request: OptionChainRequest = Field(description="The option chain request: symbol, asset_class (equity or index), exchange, currency, optional primary_exchange and underlying_con_id.")
    use_ttl_cache: bool = Field(default=True, description="Serve the response from the shared TTL cache when possible.")
    cache_ttl_seconds: float | None = Field(default=300, ge=0, description="Cache entry lifetime in seconds; null uses the cache default.")


class EconomicCalendarRequest(WSHEventDataRequest):
    """Convenience request for IBKR Wall Street Horizon calendar/event data."""

    limit_region: int = Field(default=50, gt=0, le=500, description="Per-region event limit for JSON filter mode.")
    limit: int = Field(default=50, gt=0, le=500, description="Per-contract event limit.")
    total_limit: int | None = Field(default=100, gt=0, le=100, description="Overall response cap; IBKR allows at most 100.")
    ensure_metadata: bool = Field(default=True, description="Refresh the available event-type tags into the response metadata.")
    use_ttl_cache: bool = Field(default=True, description="Serve the response from the shared TTL cache when possible.")
    cache_ttl_seconds: float | None = Field(default=300, ge=0, description="Cache entry lifetime in seconds; null uses the cache default.")

    def to_wsh_request(self) -> WSHEventDataRequest:
        if not self.uses_filter_json:
            kwargs: dict[str, Any] = {
                "fill_watchlist": self.fill_watchlist,
                "fill_portfolio": self.fill_portfolio,
                "fill_competitors": self.fill_competitors,
            }
            if self.con_id is not None:
                kwargs["con_id"] = self.con_id
            if self.start_date is not None:
                kwargs["start_date"] = self.start_date
            if self.end_date is not None:
                kwargs["end_date"] = self.end_date
            if self.total_limit is not None:
                kwargs["total_limit"] = self.total_limit
            return WSHEventDataRequest(**kwargs)
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
            total_limit=self.total_limit,
        )


class EconomicCalendarResponse(BaseModel):
    """IBKR WSH calendar response with explicit source caveats."""

    model_config = ConfigDict(extra="forbid")

    received_at: datetime = Field(description="UTC time the underlying WSH response was received.")
    source: str = Field(default="ibkr_wsh", description="Data source identifier.")
    calendar_type: str = Field(default="wall_street_horizon_event_calendar", description="Calendar flavor; IBKR has no dedicated macro-release calendar.")
    dedicated_macro_calendar_endpoint_available: bool = Field(default=False, description="Always false: this is WSH event data, not a macro calendar feed.")
    subscription_required: str = Field(default="Wall Street Horizon Corporate Event Data", description="Research subscription the account needs for this data.")
    caveats: tuple[str, ...] = Field(
        default=(
            "IBKR exposes this through Wall Street Horizon event calendar data, not a dedicated macroeconomic-release calendar endpoint.",
            "Available filters and event-type tags are entitlement/session dependent; call /reference-data/wsh/metadata to inspect them.",
            "IBKR WSH accepts one event-type tag per request; issue separate requests for multiple event categories.",
            "A Wall Street Horizon Corporate Event Data research subscription must be enabled for the account.",
        ),
        description="Source caveats every consumer of this calendar should read.",
    )
    request_filter_json: str = Field(description="The JSON filter IBKR applied for this request.")
    raw_json: str = Field(description="Raw WSH JSON document exactly as IBKR returned it.")
    payload: dict[str, Any] | list[Any] = Field(description="Parsed WSH event payload; passthrough of IBKR JSON (contents depend on entitlement).")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Passthrough metadata; includes available event-type tags when ensure_metadata was set.")

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
    limit: int | None = Query(default=None, ge=1, le=1000, description="Return at most the first N chains (one per exchange); omit for all."),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OptionChain]:
    """Return option chain metadata for one underlying across exchanges.

    Each chain lists the exchange, trading class, multiplier, expiries
    (YYYYMMDD, sorted), and strikes (sorted). Underlying qualification
    skips when underlying_con_id is supplied. Responses are cached for
    cache_ttl_seconds (default 300 s).
    """
    async def load() -> list[OptionChain]:
        return await state.feed.load_option_chains(payload.request)

    if payload.use_ttl_cache:
        chains = await state.market_data_cache.get_or_set(
            stable_cache_key("option_chains", payload.request), load, ttl_seconds=payload.cache_ttl_seconds
        )
    else:
        chains = await load()
    return chains[:limit] if isinstance(limit, int) else chains


@router.post("/fundamentals", response_model=FundamentalDataReport, deprecated=True,
             responses={410: {"description": "IBKR fundamental reports have been removed"}})
async def load_fundamental_data(
    request: FundamentalDataRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> FundamentalDataReport:
    """Deprecated: IBKR removed fundamental reports in TWS 10.32+.

    The endpoint is retained for contract discovery and always fails with
    410 in practice. Prefer /business/getEquityOverview for equity research
    context.
    """
    return await state.feed.load_fundamental_data(request)


@router.get("/wsh/metadata", response_model=WSHMetadataReport)
async def load_wsh_metadata(state: IBKRRestAppState = Depends(get_rest_state)) -> WSHMetadataReport:
    """Return Wall Street Horizon metadata: filters, tags, and countries.

    Call this before /wsh/events or /economic-calendar to discover the
    event-type tags and filter keys the current entitlement accepts. The
    payload is a passthrough of IBKR's JSON; raw_json carries the untouched
    document.
    """
    return await state.feed.load_wsh_metadata()


@router.post("/wsh/events", response_model=WSHEventDataReport)
async def load_wsh_event_data(
    request: WSHEventDataRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> WSHEventDataReport:
    """Query Wall Street Horizon corporate event data directly.

    Two modes: JSON filter mode (country, con_ids, event_types, limits) or
    object mode (con_id or fill_watchlist/portfolio/competitors with
    start_date/end_date as YYYYMMDD). One event-type tag per request. The
    response echoes the applied filter and carries the raw IBKR JSON.
    Requires a WSH research subscription.
    """
    return await state.feed.load_wsh_event_data(request)


@router.post("/economic-calendar", response_model=EconomicCalendarResponse)
async def load_economic_calendar(
    payload: Annotated[EconomicCalendarRequest, Body(openapi_examples=ECONOMIC_CALENDAR_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> EconomicCalendarResponse:
    """Return an event calendar built on IBKR Wall Street Horizon data.

    Convenience wrapper around /wsh/events with wider default limits,
    refreshed event-type metadata, and explicit caveats: IBKR serves
    corporate/event calendars, not a dedicated macroeconomic-release feed,
    and the payload shape depends on entitlement. Cached for
    cache_ttl_seconds (default 300 s).
    """
    request = payload.to_wsh_request()

    async def load() -> EconomicCalendarResponse:
        report = await state.feed.load_wsh_event_data(request, ensure_metadata=payload.ensure_metadata)
        return EconomicCalendarResponse.from_wsh_report(report)

    if payload.use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("economic_calendar", payload), load, ttl_seconds=payload.cache_ttl_seconds
        )
    return await load()


@router.get("/news/providers", response_model=list[NewsProvider])
async def load_news_providers(
    limit: int | None = Query(default=None, ge=1, le=1000, description="Return at most the first N providers; omit for all."),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[NewsProvider]:
    """Return the news providers this account is entitled to query.

    Each entry carries a provider_code and a display name. Use the codes in
    /reference-data/news/historical provider_codes. An empty list means no
    news entitlement.
    """
    providers = await state.feed.load_news_providers()
    return providers[:limit] if isinstance(limit, int) else providers


@router.post("/news/historical", response_model=list[HistoricalNewsHeadline])
async def load_historical_news(
    request: HistoricalNewsRequest,
    limit: int | None = Query(default=None, ge=1, le=1000, description="Return at most the first N headlines (already bounded by total_results); omit for all."),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[HistoricalNewsHeadline]:
    """Return historical news headlines for one contract.

    Query by con_id with one or more entitled provider codes, an optional
    UTC time window, and total_results as the IBKR-side bound. Provider
    codes that are not entitled fail with 422; no entitlement at all fails
    with 503.
    """
    await _ensure_entitled_news_provider_codes(request.provider_codes, state)
    headlines = await state.feed.load_historical_news(request)
    return headlines[:limit] if isinstance(limit, int) else headlines


@router.post("/news/article", response_model=NewsArticle)
async def load_news_article(
    request: NewsArticleRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> NewsArticle:
    """Return the full body of one IBKR news article.

    Identify the article by provider_code plus article_id exactly as a
    headline reports them. The response carries the article text, type, and
    UTC receive time.
    """
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
