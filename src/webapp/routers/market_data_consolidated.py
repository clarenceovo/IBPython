"""Consolidated market-data endpoints: one call, many sections, isolated failures."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from src.feeds.models import AssetClass, OHLCVRequest
from src.feeds.snapshotter import EquitySnapshot
from src.feeds.tick_data import MarketDepthSnapshot, TickByTickData
from src.webapp.consolidated import ConsolidatedSection, run_consolidated_sections, utc_now
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.routers.market_data_depth import UnifiedMarketDepthRequest
from src.webapp.routers.market_data_shared import session_to_dict

router = APIRouter(prefix="/market-data/consolidated", tags=["market-data"])

SNAPSHOT_SECTION = "snapshot"
TICKS_SECTION = "ticks"
DEPTH_SECTION = "depth"
TRADING_SCHEDULE_SECTION = "trading_schedule"

SECTION_NAMES = (SNAPSHOT_SECTION, TICKS_SECTION, DEPTH_SECTION, TRADING_SCHEDULE_SECTION)
_SECTION_NAME_SET = frozenset(SECTION_NAMES)

# Small tick count: the consolidated payload is a quick overview, not a full tick dump.
CONSOLIDATED_TICKS_COUNT = 10
# One trading week of schedule sessions keeps the section light.
CONSOLIDATED_SCHEDULE_DAYS = 7


class ConsolidatedSymbolMarketData(BaseModel):
    """One-symbol payload with one optional field per section plus status metadata."""

    symbol: str = Field(description="Requested path symbol.")
    exchange: str = Field(description="Requested exchange routing.")
    currency: str = Field(description="Requested currency.")
    snapshot: EquitySnapshot | None = Field(
        default=None,
        description="Latest cached equity snapshot. None when the section failed or was skipped.",
    )
    ticks: list[TickByTickData] | None = Field(
        default=None,
        description="Latest buffered ticks. None when the section failed or was skipped.",
    )
    depth: MarketDepthSnapshot | None = Field(
        default=None,
        description="Short-lived DOM/L2 snapshot. None when the section failed or was skipped.",
    )
    trading_schedule: list[dict[str, Any]] | None = Field(
        default=None,
        description="Trading schedule sessions. None when the section failed or was skipped.",
    )
    sections: dict[str, ConsolidatedSection] = Field(description="Per-section status: ok, error, or skipped.")
    requested_at: datetime = Field(description="UTC timestamp of the consolidated request.")


def _parse_section_whitelist(raw: str | None) -> frozenset[str] | None:
    """Parse the comma-separated section whitelist. None means 'all sections'."""
    if raw is None:
        return None
    names = [part.strip().lower() for part in raw.split(",")]
    names = [part for part in names if part]
    if not names:
        raise HTTPException(
            status_code=422,
            detail=f"sections must name at least one valid section: {', '.join(SECTION_NAMES)}",
        )
    unknown = [part for part in names if part not in _SECTION_NAME_SET]
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=f"unknown section(s): {', '.join(unknown)}; valid sections: {', '.join(SECTION_NAMES)}",
        )
    return frozenset(names)


@router.get(
    "/symbol/{symbol}",
    response_model=ConsolidatedSymbolMarketData,
    summary="Consolidated market data for one symbol",
    description=(
        "Returns the latest equity snapshot, latest ticks, a market-depth snapshot, and the "
        "trading schedule for one symbol in a single call. Sections run concurrently. "
        "One failing section does not fail the response; its field becomes null and its "
        "entry in 'sections' reports the error. Use the 'sections' query parameter to run "
        "only a subset, for example 'sections=snapshot,ticks'."
    ),
)
async def get_consolidated_symbol_market_data(
    symbol: str,
    exchange: str = Query(default="SMART", min_length=1, description="Exchange routing for the request."),
    currency: str = Query(default="USD", min_length=1, description="Currency for the request."),
    sections: str | None = Query(
        default=None,
        description=(
            "Comma-separated whitelist of sections to run: "
            f"{', '.join(SECTION_NAMES)}. Omit to run all sections."
        ),
    ),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> ConsolidatedSymbolMarketData:
    """Gather all requested market-data sections for one symbol concurrently."""
    whitelist = _parse_section_whitelist(sections)

    factories: dict[str, Callable[[], Awaitable[Any]]] = {}

    if whitelist is None or SNAPSHOT_SECTION in whitelist:

        async def load_snapshot() -> EquitySnapshot | None:
            cached = await state.redis.get_latest_equity_snapshots([symbol])
            return cached.get(symbol.strip().upper())

        factories[SNAPSHOT_SECTION] = load_snapshot

    if whitelist is None or TICKS_SECTION in whitelist:

        async def load_ticks() -> list[TickByTickData]:
            return state.feed.get_latest_ticks(symbol, "STK", exchange, CONSOLIDATED_TICKS_COUNT)

        factories[TICKS_SECTION] = load_ticks

    if whitelist is None or DEPTH_SECTION in whitelist:

        async def load_depth() -> MarketDepthSnapshot:
            request = UnifiedMarketDepthRequest(symbol=symbol, exchange=exchange, currency=currency)
            spec = request.to_contract_spec()
            return await state.feed.load_market_depth_snapshot(
                spec,
                num_rows=request.num_rows,
                is_smart_depth=request.resolved_smart_depth(spec),
                snapshot_wait_seconds=request.snapshot_wait_seconds,
                request_timeout_seconds=state.settings.ibkr_market_depth_request_timeout_seconds,
                lease_wait_seconds=state.settings.ibkr_market_depth_lease_wait_seconds,
            )

        factories[DEPTH_SECTION] = load_depth

    if whitelist is None or TRADING_SCHEDULE_SECTION in whitelist:

        async def load_schedule() -> list[dict[str, Any]]:
            request = OHLCVRequest(
                symbol=symbol,
                asset_class=AssetClass.EQUITY,
                exchange=exchange,
                currency=currency,
                bar_size="1 day",
                duration=f"{CONSOLIDATED_SCHEDULE_DAYS} D",
                what_to_show="SCHEDULE",
                use_rth=True,
            )
            sessions = await state.feed.load_trading_schedule(request, ref_date=date.today(), use_rth=True)
            return [session_to_dict(session) for session in sessions]

        factories[TRADING_SCHEDULE_SECTION] = load_schedule

    results, meta = await run_consolidated_sections(factories)
    for name in SECTION_NAMES:
        if name not in meta:
            meta[name] = ConsolidatedSection(status="skipped", latency_ms=0.0)

    return ConsolidatedSymbolMarketData(
        symbol=symbol,
        exchange=exchange,
        currency=currency,
        snapshot=results.get(SNAPSHOT_SECTION),
        ticks=results.get(TICKS_SECTION),
        depth=results.get(DEPTH_SECTION),
        trading_schedule=results.get(TRADING_SCHEDULE_SECTION),
        sections=meta,
        requested_at=utc_now(),
    )
