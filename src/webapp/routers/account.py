from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict, Field

from src.feeds.account import AccountPnLDTO, AccountSummaryDTO, LivePositionDTO, PortfolioItemDTO, PositionPnLDTO
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

router = APIRouter(prefix="/account", tags=["account"])


class AccountPnLSnapshotRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account: str = Field(min_length=1)
    model_code: str = ""
    wait_seconds: float = Field(default=1.2, ge=0)


class PositionPnLSnapshotRequest(AccountPnLSnapshotRequest):
    con_id: int = Field(gt=0)


@router.get("/summary", response_model=list[AccountSummaryDTO])
async def load_account_summary(
    account: str = Query(default=""),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[AccountSummaryDTO]:
    return await state.feed.load_account_summary(account)


@router.get("/positions", response_model=list[LivePositionDTO])
async def load_live_positions(state: IBKRRestAppState = Depends(get_rest_state)) -> list[LivePositionDTO]:
    return await state.feed.load_live_positions()


@router.get("/portfolio", response_model=list[PortfolioItemDTO])
async def load_portfolio_items(
    account: str = Query(default=""),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[PortfolioItemDTO]:
    return await state.feed.load_portfolio_items(account)


@router.post("/pnl/account", response_model=AccountPnLDTO)
async def load_account_pnl_snapshot(
    request: AccountPnLSnapshotRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> AccountPnLDTO:
    return await state.feed.load_account_pnl_snapshot(
        account=request.account,
        model_code=request.model_code,
        wait_seconds=request.wait_seconds,
    )


@router.post("/pnl/position", response_model=PositionPnLDTO)
async def load_position_pnl_snapshot(
    request: PositionPnLSnapshotRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> PositionPnLDTO:
    return await state.feed.load_position_pnl_snapshot(
        account=request.account,
        con_id=request.con_id,
        model_code=request.model_code,
        wait_seconds=request.wait_seconds,
    )
