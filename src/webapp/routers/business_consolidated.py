"""Consolidated business-data endpoints: one call, many sections, isolated failures."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from src.feeds.account import AccountPnLDTO, AccountSummaryDTO, LivePositionDTO, PortfolioItemDTO
from src.feeds.orders import OpenOrder
from src.webapp.consolidated import ConsolidatedSection, run_consolidated_sections, utc_now
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

router = APIRouter(prefix="/business/consolidated", tags=["business"])


class ConsolidatedOverviewResponse(BaseModel):
    """Business overview envelope: one optional field per section plus per-section meta."""

    requested_at: datetime = Field(description="UTC timestamp captured after all sections resolved.")
    account_summary: list[AccountSummaryDTO] | None = Field(default=None, description="Account summary values grouped by account; null when the section failed.")
    account_pnl: AccountPnLDTO | None = Field(default=None, description="Account PnL snapshot; null when the section failed.")
    positions: list[LivePositionDTO] | None = Field(default=None, description="Live positions; null when the section failed.")
    portfolio: list[PortfolioItemDTO] | None = Field(default=None, description="Portfolio items; null when the section failed.")
    open_orders: list[OpenOrder] | None = Field(default=None, description="Currently open (working) orders; null when the section failed.")
    sections: dict[str, ConsolidatedSection] = Field(default_factory=dict, description="Per-section metadata: status, latency in milliseconds, and error text.")


@router.get("/overview", response_model=ConsolidatedOverviewResponse, summary="Get consolidated business overview")
async def consolidated_overview(
    account_id: str | None = Query(default=None, description="Account to scope summary, PnL, and portfolio queries; omit for all accounts."),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> ConsolidatedOverviewResponse:
    """Return account summary, PnL, positions, portfolio, and open orders in one payload.

    Sections run concurrently. A failing section yields a null field and an
    ``error`` entry in ``sections``; the remaining sections still load.
    Open orders are a read-only query and stay available even when order
    pathways are disabled.
    """
    account = account_id or ""
    results, meta = await run_consolidated_sections(
        {
            "account_summary": lambda: state.feed.load_account_summary(account),
            "account_pnl": lambda: state.feed.load_account_pnl_snapshot(account=account),
            "positions": state.feed.load_live_positions,
            "portfolio": lambda: state.feed.load_portfolio_items(account),
            "open_orders": state.feed.load_open_orders,
        }
    )
    return ConsolidatedOverviewResponse(
        requested_at=utc_now(),
        account_summary=results["account_summary"],
        account_pnl=results["account_pnl"],
        positions=results["positions"],
        portfolio=results["portfolio"],
        open_orders=results["open_orders"],
        sections=meta,
    )
