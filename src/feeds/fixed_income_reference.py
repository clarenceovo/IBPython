from __future__ import annotations

from dataclasses import dataclass

from src.feeds.bond_curve import BondCurveRequest, build_standard_bond_curve
from src.feeds.fixed_income import DeliverableBasketRequest, DeliverableBondInput


@dataclass(frozen=True)
class IndicativeFixedIncomeReferenceProvider:
    """Non-trading reference provider for local demos and API integration tests."""

    name: str = "indicative_fixed_income_reference_provider"

    async def get_deliverable_basket(self, request: DeliverableBasketRequest) -> tuple[DeliverableBondInput, ...]:
        curve = build_standard_bond_curve(
            BondCurveRequest(
                market=request.market,
                valuation_date=request.valuation_date,
            )
        )
        futures_symbol = request.futures_symbol.upper()
        return tuple(
            DeliverableBondInput(
                bond=point.bond.model_copy(
                    update={
                        "metadata": {
                            **point.bond.metadata,
                            "reference_provider": self.name,
                            "reference_warning": "indicative placeholder, not exchange-official CTD data",
                        }
                    }
                ),
                conversion_factor=1.0,
                clean_price=100.0,
                delivery_date=request.valuation_date,
                metadata={
                    "tenor": point.tenor,
                    "ctd_status": point.ctd_status,
                    "yield_source": point.yield_source,
                    "reference_warning": "indicative placeholder, not exchange-official CTD data",
                },
            )
            for point in curve.standard_ctd_points
            if point.future.futures_symbol.upper() == futures_symbol
        )


provider = IndicativeFixedIncomeReferenceProvider()

