from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

from src.feeds.bond_curve import BondCurveResponse, build_standard_bond_curve, BondCurveRequest

router = APIRouter(prefix="/business", tags=["business"])


@router.get(
    "/getBondCurve",
    response_model=BondCurveResponse,
    operation_id="getBondCurve",
    summary="Get a standard-tenor sovereign bond curve",
    description=(
        "Returns a business-facing sovereign curve for UST, JGB, KTB, German Bund, or UK Gilt aliases. "
        "The built-in provider returns indicative standard-tenor CTD/benchmark placeholders and chart-ready render points. "
        "Production CTD selection should be wired to an exchange or vendor delivery-basket provider."
    ),
)
async def get_bond_curve(
    market: Annotated[
        str,
        Query(
            min_length=1,
            description="Sovereign market alias. Supported examples: UST, JGB, KTB, BUND, GERMAN_BUND, UK, UK_GILT, GILT.",
            examples=["UST"],
        ),
    ],
    valuation_date: Annotated[
        date | None,
        Query(
            description="Curve valuation date. Defaults to current UTC date.",
            examples=["2026-05-16"],
        ),
    ] = None,
    coupon_frequency: Annotated[
        int | None,
        Query(
            ge=1,
            description="Optional coupon frequency override used for the par-yield bootstrap.",
            examples=[2],
        ),
    ] = None,
) -> BondCurveResponse:
    request = BondCurveRequest(
        market=market,
        coupon_frequency=coupon_frequency,
        valuation_date=valuation_date or datetime.now(timezone.utc).date(),
    )
    try:
        return build_standard_bond_curve(request)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
