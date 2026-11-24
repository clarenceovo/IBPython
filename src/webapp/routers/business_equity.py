"""A bounded, read-only equity overview assembled from existing feeds."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Annotated, Literal

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from src.feeds.equity_reference import DividendsResponse, EquityReferenceRequest, ShortabilityResponse
from src.feeds.exceptions import IBKRMarketDataUnavailableError
from src.feeds.ibkr_connection import capture_ibkr_session, require_ibkr_session
from src.feeds.snapshot_converters import ticker_to_snapshot
from src.feeds.snapshot_models import EquitySnapshot, EquitySnapshotQualityError, validate_equity_snapshot_quality
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.openapi_markdown import markdown_openapi_examples
from src.webapp.routers.business_news import SymbolNewsRequest, SymbolNewsResponse, get_symbol_news
from src.webapp.routers.business_shared import BusinessCacheControls

logger = logging.getLogger(__name__)
router = APIRouter()
SectionName = Literal["quote", "dividends", "shortability", "news"]
SectionStatus = Literal["available", "partial", "unavailable", "not_requested"]


class EquityOverviewRequest(EquityReferenceRequest, BusinessCacheControls):
    """Request body for the equity overview: quote plus optional research sections.

    Field overrides restate the inherited EquityReferenceRequest constraints
    with descriptions; validation behaviour is unchanged.
    """

    symbol: str = Field(min_length=1, description="Equity symbol, e.g. AAPL or 0700.HK.")
    exchange: str | None = Field(default=None, description="Exchange override, e.g. SMART or NASDAQ.")
    currency: str | None = Field(default=None, description="ISO 4217 currency override, e.g. USD.")
    primary_exchange: str | None = Field(default=None, description="Primary listing exchange for SMART-routed equities, e.g. NASDAQ.")
    con_id: int | None = Field(default=None, gt=0, description="IBKR contract id; skips symbol qualification when supplied.")
    timeout_seconds: float = Field(default=10, gt=0, le=20, description="Overall budget in seconds for qualification and all sections.")
    include_dividends: bool = Field(default=True, description="Include the dividend summary section.")
    include_shortability: bool = Field(default=True, description="Include the shortability section.")
    include_news: bool = Field(default=False, description="Include recent headlines via the symbol-news endpoint (bounded by news_limit).")
    news_provider_codes: tuple[str, ...] | None = Field(default=None, description="News provider codes to query; null queries all entitled providers. Codes are uppercased.")
    news_limit: int = Field(default=20, ge=1, le=300, description="Maximum number of headlines in the news section.")
    cache_ttl_seconds: float | None = Field(default=5, ge=0, description="Cache entry lifetime in seconds; the default is short because quotes go stale fast.")


class EquitySectionStatus(BaseModel):
    """Per-section outcome map for the equity overview (keys are the four sections)."""

    model_config = ConfigDict(extra="forbid")

    quote: SectionStatus = Field(description="Quote section status.")
    dividends: SectionStatus = Field(description="Dividend section status; not_requested when include_dividends is false.")
    shortability: SectionStatus = Field(description="Shortability section status; not_requested when include_shortability is false.")
    news: SectionStatus = Field(description="News section status; not_requested when include_news is false.")

    def __getitem__(self, key: str) -> SectionStatus:
        """Allow dict-style reads so callers written against the old map keep working."""
        return getattr(self, key)


class EquityOverviewResponse(BaseModel):
    """Response envelope for the equity overview."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(description="Qualified equity symbol, e.g. AAPL.")
    con_id: int = Field(description="IBKR contract id of the qualified stock.")
    exchange: str = Field(description="Qualified exchange, e.g. SMART or NASDAQ.")
    currency: str = Field(description="ISO 4217 quote currency.")
    primary_exchange: str = Field(description="Primary listing exchange; empty string when none is reported.")
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="UTC time the overview was assembled.")
    quote: EquitySnapshot | None = Field(default=None, description="Level-1 quote snapshot; null when the quote section failed.")
    quote_observed_at: datetime | None = Field(default=None, description="UTC time the quote was observed at the source; null when unknown.")
    quote_received_at: datetime | None = Field(default=None, description="UTC time this server received the quote; null when no quote was delivered.")
    quote_market_data_type: int | None = Field(default=None, description="IBKR market data type of the quote: 1 live, 2 frozen, 3 delayed, 4 delayed-frozen; null when unknown.")
    dividends: DividendsResponse | None = Field(default=None, description="Dividend summary; null when not requested or unavailable.")
    shortability: ShortabilityResponse | None = Field(default=None, description="Shortability info; null when not requested or unavailable.")
    news: SymbolNewsResponse | None = Field(default=None, description="Recent headlines; null when not requested or unavailable.")
    section_status: EquitySectionStatus = Field(description="Per-section outcome: available, partial, unavailable, or not_requested.")
    warnings: list[str] = Field(default_factory=list, description="Human-readable section warnings; empty when every section loaded cleanly.")
    connection_generation: int | None = Field(default=None, description="IBKR connection generation the quote belongs to; null when offline.")
    connection_status: str | None = Field(default=None, description="Feed connection status at response time; null when no connection object exists.")
    from_cache: bool = Field(default=False, description="True when the payload came from the shared TTL cache.")

    def cacheable(self) -> bool:
        return all(status in {"available", "not_requested"} for status in self.section_status.model_dump().values())


@router.post("/getEquityOverview", response_model=EquityOverviewResponse, summary="Get quotes and research context for one equity")
async def get_equity_overview(
    payload: Annotated[EquityOverviewRequest, Body(openapi_examples=markdown_openapi_examples("business.getEquityOverview"))],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> EquityOverviewResponse:
    """Return a quote, dividend, shortability, and news overview for one equity.

    Qualifies the stock contract, then loads the enabled sections
    concurrently under one timeout_seconds budget. A failing section does
    not fail the request: its field stays null, section_status records
    available/partial/unavailable/not_requested, and warnings carries the
    reason. Quote fields may be null when the market is closed or data is
    delayed; quote_market_data_type distinguishes live, frozen, and delayed
    data. Cached for cache_ttl_seconds (default 5 s) when every section is
    clean; from_cache reports a cache hit.
    """
    connection = getattr(state.feed, "_connection", None)
    generation = getattr(connection, "generation", None)
    if connection is not None:
        connection.require_connection_enabled()
    loaded = False

    async def load() -> EquityOverviewResponse:
        nonlocal loaded
        loaded = True
        return await _load_equity_overview(payload, state)

    def cacheable(value: EquityOverviewResponse) -> bool:
        return value.cacheable() and (
            connection is None or (connection.connection_enabled and connection.generation == generation)
        )

    if payload.use_ttl_cache:
        result = await state.market_data_cache.get_or_set(
            stable_cache_key("business_equity_overview", {"generation": generation, "request": payload.model_dump(mode="json")}), load,
            ttl_seconds=payload.cache_ttl_seconds, cache_if=cacheable,
        )
    else:
        result = await load()
    # Do not change cached observation timestamps or mutate a shared response object.
    return result.model_copy(update={
        "from_cache": not loaded,
        "connection_status": connection.control_status()["connection_status"] if connection is not None else None,
    })


async def _load_equity_overview(payload: EquityOverviewRequest, state: IBKRRestAppState) -> EquityOverviewResponse:
    deadline = asyncio.get_running_loop().time() + payload.timeout_seconds
    spec = payload.to_contract_spec()
    try:
        async with asyncio.timeout_at(deadline):
            contract = await state.feed.qualify_contract(spec)
    except TimeoutError as exc:
        raise IBKRMarketDataUnavailableError("Equity overview qualification exceeded the request deadline") from exc
    con_id = int(getattr(contract, "conId", 0) or 0)
    connection = getattr(state.feed, "_connection", None)
    session = capture_ibkr_session(connection) if connection is not None else None

    def require_session() -> None:
        if session is not None:
            require_ibkr_session(connection, session)
    if con_id <= 0 or getattr(contract, "secType", "STK") != "STK":
        raise HTTPException(status_code=422, detail="Equity overview requires a qualified stock contract")
    response = EquityOverviewResponse(
        symbol=getattr(contract, "symbol", None) or spec.symbol, con_id=con_id,
        exchange=getattr(contract, "exchange", None) or spec.exchange,
        currency=getattr(contract, "currency", None) or spec.currency,
        primary_exchange=getattr(contract, "primaryExchange", None) or spec.primary_exchange or "",
        section_status={"quote": "unavailable", "dividends": "not_requested", "shortability": "not_requested", "news": "not_requested"},
        connection_generation=session[0] if session is not None else None,
    )
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0:
        raise IBKRMarketDataUnavailableError("Equity overview qualification exceeded the request deadline")
    reference = EquityReferenceRequest(
        symbol=response.symbol, exchange=response.exchange, currency=response.currency,
        primary_exchange=response.primary_exchange or None, con_id=con_id, timeout_seconds=remaining,
    )

    async def load_quote() -> None:
        captures = []
        try:
            async with asyncio.timeout_at(deadline):
                captures = await state.feed.capture_equity_snapshots(
                    [(response.symbol, response.exchange, response.currency, response.primary_exchange, con_id)],
                    snapshot_wait_seconds=0,
                    lease_ttl_seconds=remaining + 10,
                )
                if not captures or not captures[0].success:
                    raise IBKRMarketDataUnavailableError(captures[0].error if captures else "No quote returned")
                capture = captures[0]
                # The normal 11.5-second snapshot wait exceeds this endpoint's budget.
                # Return the first usable quote, retaining nulls for fields not delivered yet.
                while True:
                    require_session()
                    snapshot = ticker_to_snapshot(
                        capture.ticker, symbol=response.symbol, exchange=response.exchange, currency=response.currency,
                        primary_exchange=response.primary_exchange, con_id=con_id,
                    )
                    try:
                        validate_equity_snapshot_quality(snapshot)
                        break
                    except EquitySnapshotQualityError:
                        updated = getattr(capture.ticker, "updateEvent", None)
                        if updated is None:
                            raise
                        await updated
                response.quote = snapshot
                response.quote_observed_at = snapshot.timestamp if getattr(capture.ticker, "time", None) is not None else None
                response.quote_received_at = datetime.now(timezone.utc)
                response.quote_market_data_type = getattr(capture.ticker, "marketDataType", None)
                response.section_status.quote = "available"
        finally:
            tickers = [capture.ticker for capture in captures if capture.ticker is not None]
            if tickers:
                # The shared feed owns bounded, cancellation-safe cleanup.
                failures = await state.feed.cancel_equity_tickers(tickers)
                if failures:
                    response.section_status.quote = "partial"
                    response.warnings.append("quote: subscription cleanup was incomplete")

    async def load_section(name: SectionName) -> None:
        setattr(response.section_status, name, "unavailable")
        try:
            if name == "quote":
                await load_quote()
            elif name in {"dividends", "shortability"}:
                loader = state.feed.load_equity_dividends if name == "dividends" else state.feed.load_equity_shortability
                # Reference feeds enforce their own deadline and allow up to five seconds for cleanup.
                value = await loader(reference)
                require_session()
                setattr(response, name, value)
                setattr(response.section_status, name, value.status)
                if value.status != "available":
                    response.warnings.append(f"{name}: {value.status}" + (" (request timed out)" if value.timed_out else ""))
            else:
                async with asyncio.timeout_at(deadline):
                    response.news = await get_symbol_news(SymbolNewsRequest(
                        symbol=response.symbol, exchange=response.exchange, currency=response.currency,
                        primary_exchange=response.primary_exchange or None, con_id=con_id,
                        provider_codes=payload.news_provider_codes, total_results=payload.news_limit,
                        include_articles=False, use_ttl_cache=False,
                    ), state)
                    require_session()
                    response.news.headlines = response.news.headlines[:payload.news_limit]
                    response.section_status.news = "available"
        except Exception as exc:
            if getattr(response.section_status, name) == "available":
                setattr(response.section_status, name, "partial")
            detail = exc.detail if isinstance(exc, HTTPException) else str(exc) or type(exc).__name__
            response.warnings.append(f"{name}: {detail}")
            logger.warning("Equity overview %s failed for %s: %s", name, response.symbol, detail)

    names: list[SectionName] = ["quote"]
    names.extend(name for name, enabled in (("dividends", payload.include_dividends), ("shortability", payload.include_shortability), ("news", payload.include_news)) if enabled)
    tasks = [asyncio.create_task(load_section(name)) for name in names]
    try:
        # Includes the existing reference-feed cleanup allowance; hung sections are cancelled below.
        _, pending = await asyncio.wait(tasks, timeout=max(0, deadline - asyncio.get_running_loop().time()) + 5)
        for name, task in zip(names, tasks, strict=True):
            if task in pending:
                task.cancel()
                setattr(response.section_status, name, "unavailable")
                response.warnings.append(f"{name}: request deadline and cleanup allowance exceeded")
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    response.generated_at = datetime.now(timezone.utc)
    return response
