from __future__ import annotations

from datetime import date

import pytest

from src.feeds.bond_curve import BondCurveRequest, build_standard_bond_curve, resolve_bond_curve_market
from src.feeds.bonds import SovereignBondMarket


def test_standard_ust_curve_contains_ctd_inputs_and_render_points() -> None:
    response = build_standard_bond_curve(
        BondCurveRequest(market="UST", valuation_date=date(2026, 5, 16))
    )

    assert response.market is SovereignBondMarket.US_TREASURY
    assert response.currency == "USD"
    assert response.valuation_date == date(2026, 5, 16)
    assert tuple(point.tenor for point in response.standard_ctd_points) == ("2Y", "5Y", "10Y", "30Y")
    assert tuple(point.tenor for point in response.render_points) == ("2Y", "5Y", "10Y", "30Y")
    assert len(response.curve.points) == 4
    assert all(point.discount_factor > 0 for point in response.render_points)
    assert response.standard_ctd_points[0].ctd_status == "indicative_placeholder"
    assert "indicative placeholders" in response.caveats[0]


def test_bond_curve_aliases_support_uk_gilt_market() -> None:
    response = build_standard_bond_curve(
        BondCurveRequest(market="uk", valuation_date=date(2026, 5, 16))
    )

    assert response.market is SovereignBondMarket.UK_GILT
    assert response.currency == "GBP"
    assert response.render_points[-1].tenor == "30Y"


def test_bond_curve_rejects_unsupported_market_alias() -> None:
    with pytest.raises(ValueError, match="unsupported bond curve market"):
        resolve_bond_curve_market("XYZ")
