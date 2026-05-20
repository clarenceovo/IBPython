from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from src.feeds.bonds import BondInstrument, CTDFutureDefinition, SovereignBondMarket
from src.feeds.fixed_income import (
    BondFutureContractSpec,
    DeliverableBasketRequest,
    DeliverableBondInput,
    build_futures_implied_curve,
    calculate_ctd_analytics,
    default_bond_future_specs,
    fixed_income_market_config,
    futures_implied_clean_price,
    yield_to_maturity_from_clean_price,
)
from src.feeds.fixed_income_reference import IndicativeFixedIncomeReferenceProvider


def test_default_bond_future_specs_map_ust_to_ibkr_futures_contract_fields() -> None:
    specs = default_bond_future_specs("UST", contract_month="202606")

    assert tuple(spec.futures_symbol for spec in specs) == ("ZT", "ZF", "ZN", "ZB")
    request = specs[2].to_ohlcv_request(duration="1 D", bar_size="1 min", what_to_show="TRADES", use_rth=True)
    assert request.symbol == "ZN"
    assert request.asset_class.value == "future"
    assert request.exchange == "CBOT"
    assert request.currency == "USD"
    assert request.last_trade_date_or_contract_month == "202606"


def test_fixed_income_market_config_supports_bund_and_gilt() -> None:
    bund = fixed_income_market_config("BUND")
    gilt = fixed_income_market_config("UK_GILT")

    assert bund.futures_exchange == "EUREX"
    assert "GBL" in bund.default_futures
    assert gilt.currency == "GBP"


def test_ctd_analytics_selects_lowest_net_basis_and_implied_clean_price() -> None:
    future = CTDFutureDefinition(
        market=SovereignBondMarket.US_TREASURY,
        futures_symbol="ZN",
        exchange="CBOT",
        currency="USD",
        description="10Y Treasury future",
    )
    cheap = DeliverableBondInput(
        bond=BondInstrument(symbol="91282CJL6", maturity_date=date(2034, 5, 15), coupon_rate=0.04),
        conversion_factor=0.80,
        clean_price=88,
        accrued_interest=0.5,
        carry=0.1,
    )
    rich = DeliverableBondInput(
        bond=BondInstrument(symbol="91282CHT1", maturity_date=date(2033, 5, 15), coupon_rate=0.04),
        conversion_factor=0.78,
        clean_price=90,
        accrued_interest=0.4,
    )

    result = calculate_ctd_analytics(
        future=future,
        contract_month="202606",
        valuation_date=date(2026, 5, 16),
        futures_price=110,
        basket=(rich, cheap),
        provider_name="test_provider",
    )

    assert result.selected.bond.symbol == "91282CJL6"
    assert futures_implied_clean_price(result.selected) == pytest.approx(88.0)
    assert result.diagnostics.provider == "test_provider"


def test_yield_to_maturity_from_clean_price_solves_par_bond_near_coupon() -> None:
    bond = BondInstrument(
        symbol="PAR",
        maturity_date=date(2031, 1, 1),
        coupon_rate=0.05,
        coupon_frequency=2,
    )

    solved = yield_to_maturity_from_clean_price(
        bond=bond,
        clean_price=100,
        valuation_date=date(2026, 1, 1),
    )

    assert solved == pytest.approx(0.05, abs=1e-3)


def test_build_futures_implied_curve_uses_selected_ctd_yields() -> None:
    future = CTDFutureDefinition(
        market=SovereignBondMarket.US_TREASURY,
        futures_symbol="ZF",
        exchange="CBOT",
        currency="USD",
        description="5Y Treasury future",
    )
    ctd = DeliverableBondInput(
        bond=BondInstrument(symbol="91282TEST", maturity_date=date(2031, 5, 15), coupon_rate=0.04),
        conversion_factor=0.90,
        clean_price=98,
    )
    analytics = calculate_ctd_analytics(
        future=future,
        contract_month="202606",
        valuation_date=date(2026, 5, 16),
        futures_price=108,
        basket=(ctd,),
        provider_name="test_provider",
    )

    curve = build_futures_implied_curve(
        market=SovereignBondMarket.US_TREASURY,
        currency="USD",
        valuation_date=date(2026, 5, 16),
        ctd_results=(analytics,),
    )

    assert curve.market is SovereignBondMarket.US_TREASURY
    assert curve.points[0].selected_ctd.symbol == "91282TEST"
    assert curve.curve.points[0].discount_factor > 0
    assert curve.diagnostics.mode == "futures_implied"


def test_bond_future_contract_requires_ibkr_identifier() -> None:
    with pytest.raises(ValueError, match="contract_month, local_symbol, or con_id"):
        BondFutureContractSpec(market="UST", futures_symbol="ZN", exchange="CBOT", currency="USD")


@pytest.mark.asyncio
async def test_indicative_fixed_income_reference_provider_returns_matching_standard_tenor() -> None:
    provider = IndicativeFixedIncomeReferenceProvider()

    basket = await provider.get_deliverable_basket(
        DeliverableBasketRequest(
            market="UST",
            futures_symbol="ZN",
            contract_month="202606",
            valuation_date=date(2026, 5, 16),
        )
    )

    assert len(basket) == 1
    assert basket[0].bond.symbol == "US_TREASURY_10Y_CTD"
    assert basket[0].conversion_factor == 1.0
    assert basket[0].metadata["ctd_status"] == "indicative_placeholder"
