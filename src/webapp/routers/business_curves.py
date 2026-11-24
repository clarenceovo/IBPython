"""Bond curve endpoints for the business domain."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

from src.feeds.bond_curve import BondCurveRequest, BondCurveResponse, build_standard_bond_curve

router = APIRouter()


@router.get(
    "/getBondCurve",
    response_model=BondCurveResponse,
    operation_id="getBondCurve",
    summary="Get a standard-tenor sovereign bond curve",
)
async def get_bond_curve(
    market: Annotated[str, Query(min_length=1, description="Sovereign market alias: UST, JGB, UK, UK_GILT, BUND, GERMAN_BUND, or KTB.")],
    valuation_date: date | None = Query(default=None, description="Curve valuation date (UTC); defaults to today."),
    coupon_frequency: int | None = Query(default=None, ge=1, description="Coupon payments per year used for bootstrapping; default follows market convention."),
) -> BondCurveResponse:
    try:
        request = BondCurveRequest(
            market=market,
            valuation_date=valuation_date or datetime.now(timezone.utc).date(),
            coupon_frequency=coupon_frequency,
        )
        return build_standard_bond_curve(request)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
