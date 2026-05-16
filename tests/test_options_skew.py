from __future__ import annotations

import pytest

from src.feeds.contracts import OptionChain, OptionChainRequest
from src.feeds.models import AssetClass
from src.feeds.options import (
    OptionAnalyticsSnapshot,
    OptionContractSpec,
    OptionGreekSet,
    OptionMaturitySkew,
    OptionRight,
    OptionSkewSelectionMethod,
    OptionSkewSurfaceRequest,
    build_skew_option_contracts,
    calculate_maturity_skew,
    select_option_chain,
    select_skew_expirations,
    select_skew_strikes,
)


def _snapshot(
    *,
    expiry: str,
    strike: float,
    right: OptionRight,
    implied_vol: float,
    delta: float,
    open_interest: float,
    volume: float = 0,
) -> OptionAnalyticsSnapshot:
    contract = OptionContractSpec(
        underlying_symbol="TSLA",
        expiry=expiry,
        strike=strike,
        right=right,
        exchange="SMART",
        trading_class="TSLA",
    )
    return OptionAnalyticsSnapshot(
        contract=contract,
        model_greeks=OptionGreekSet(source="model", implied_vol=implied_vol, delta=delta),
        call_open_interest=open_interest if right is OptionRight.CALL else None,
        put_open_interest=open_interest if right is OptionRight.PUT else None,
        call_volume=volume if right is OptionRight.CALL else None,
        put_volume=volume if right is OptionRight.PUT else None,
    )


def test_calculate_maturity_skew_selects_delta_targets_and_max_oi() -> None:
    snapshots = [
        _snapshot(expiry="20260619", strike=260, right=OptionRight.CALL, implied_vol=0.42, delta=0.25, open_interest=900),
        _snapshot(expiry="20260619", strike=270, right=OptionRight.CALL, implied_vol=0.44, delta=0.18, open_interest=1200),
        _snapshot(expiry="20260619", strike=240, right=OptionRight.PUT, implied_vol=0.55, delta=-0.25, open_interest=1500),
        _snapshot(expiry="20260619", strike=230, right=OptionRight.PUT, implied_vol=0.58, delta=-0.18, open_interest=800),
    ]

    skew = calculate_maturity_skew(
        underlying_symbol="TSLA",
        expiry="20260619",
        spot_price=250,
        target_abs_delta=0.25,
        fallback_moneyness_pct=0.05,
        snapshots=snapshots,
    )

    assert isinstance(skew, OptionMaturitySkew)
    assert skew.selection_method is OptionSkewSelectionMethod.DELTA_TARGET
    assert skew.selected_call is not None
    assert skew.selected_call.contract.strike == 260
    assert skew.selected_put is not None
    assert skew.selected_put.contract.strike == 240
    assert skew.skew_put_minus_call_iv == pytest.approx(0.13)
    assert skew.risk_reversal_call_minus_put_iv == pytest.approx(-0.13)
    assert skew.max_call_open_interest is not None
    assert skew.max_call_open_interest.contract.strike == 270
    assert skew.max_put_open_interest is not None
    assert skew.max_put_open_interest.contract.strike == 240


def test_calculate_maturity_skew_falls_back_to_moneyness_without_delta() -> None:
    snapshots = [
        _snapshot(expiry="20260619", strike=260, right=OptionRight.CALL, implied_vol=0.40, delta=0.0, open_interest=100),
        _snapshot(expiry="20260619", strike=240, right=OptionRight.PUT, implied_vol=0.48, delta=0.0, open_interest=100),
    ]

    skew = calculate_maturity_skew(
        underlying_symbol="TSLA",
        expiry="20260619",
        spot_price=250,
        target_abs_delta=0.25,
        fallback_moneyness_pct=0.05,
        snapshots=snapshots,
    )

    assert skew.selection_method is OptionSkewSelectionMethod.MONEYNESS_FALLBACK
    assert skew.skew_put_minus_call_iv == pytest.approx(0.08)


def test_select_chain_expirations_strikes_and_contracts_for_skew() -> None:
    request = OptionSkewSurfaceRequest(
        chain_request=OptionChainRequest(symbol="TSLA", asset_class="equity", primary_exchange="NASDAQ"),
        max_expirations=2,
        max_strikes_per_expiry=5,
        spot_price=250,
    )
    chains = [
        OptionChain(
            underlying_symbol="TSLA",
            underlying_asset_class=AssetClass.EQUITY,
            underlying_con_id=76792991,
            exchange="BOX",
            trading_class="TSLA",
            multiplier="100",
            expirations=("20260619", "20260717", "20260821"),
            strikes=(100, 200, 230, 240, 250, 260, 270, 300, 400),
        ),
        OptionChain(
            underlying_symbol="TSLA",
            underlying_asset_class=AssetClass.EQUITY,
            underlying_con_id=76792991,
            exchange="SMART",
            trading_class="TSLA",
            multiplier="100",
            expirations=("20260619", "20260717", "20260821"),
            strikes=(100, 200, 230, 240, 250, 260, 270, 300, 400),
        ),
    ]

    chain = select_option_chain(chains, request)
    expirations = select_skew_expirations(chain, request)
    strikes = select_skew_strikes(chain.strikes, spot_price=250, window_pct=0.20, max_count=5)
    contracts = build_skew_option_contracts(chain=chain, request=request, expiry=expirations[0], strikes=strikes)

    assert chain.exchange == "SMART"
    assert expirations == ("20260619", "20260717")
    assert strikes == (230.0, 240.0, 250.0, 260.0, 270.0)
    assert len(contracts) == 10
    assert {contract.right for contract in contracts} == {OptionRight.CALL, OptionRight.PUT}


def test_skew_surface_request_budget_cap_reduces_strikes() -> None:
    """max_total_lines should automatically reduce max_strikes_per_expiry to fit."""
    request = OptionSkewSurfaceRequest(
        chain_request=OptionChainRequest(symbol="SPY", asset_class="equity", primary_exchange="ARCA"),
        max_expirations=6,
        max_strikes_per_expiry=50,
        max_total_lines=60,
        spot_price=500,
    )
    # 60 lines / 6 expirations = 10 strikes max (rounded to odd = 9)
    assert request.max_strikes_per_expiry <= 10


def test_skew_surface_request_default_budget_fits() -> None:
    """Default budget (60) with default max_expirations (6) and max_strikes (11) fits."""
    request = OptionSkewSurfaceRequest(
        chain_request=OptionChainRequest(symbol="SPY", asset_class="equity", primary_exchange="ARCA"),
        spot_price=500,
    )
    # 6 expirations * 11 strikes = 66 > 60, so max_strikes should be capped
    # 60 / 6 = 10, rounded to odd = 9
    assert request.max_strikes_per_expiry <= 10
    assert request.max_strikes_per_expiry % 2 == 1  # always odd
    assert request.max_total_lines == 60
