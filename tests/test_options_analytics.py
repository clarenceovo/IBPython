from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.feeds.options import (
    DEFAULT_OPTION_ANALYTICS_GENERIC_TICKS,
    OptionAnalyticsRequest,
    OptionContractSpec,
    OptionRight,
    normalize_option_analytics_from_ticker,
)


def test_option_analytics_request_uses_ibkr_generic_ticks() -> None:
    request = OptionAnalyticsRequest(
        contract=OptionContractSpec(
            underlying_symbol="SPY",
            expiry="20260619",
            strike=500,
            right=OptionRight.CALL,
        )
    )

    assert request.generic_ticks == DEFAULT_OPTION_ANALYTICS_GENERIC_TICKS
    assert request.generic_tick_list == "100,101,104,105,106"


def test_normalize_option_analytics_snapshot_derives_greeks_oi_and_volume() -> None:
    contract = OptionContractSpec(
        underlying_symbol="SPX",
        expiry="20260619",
        strike=5500,
        right=OptionRight.PUT,
        multiplier="100",
    )
    ticker = SimpleNamespace(
        modelGreeks=SimpleNamespace(
            impliedVol=0.18,
            delta=-0.42,
            gamma=0.001,
            theta=-1.25,
            vega=8.4,
            optPrice=125.0,
            pvDividend=0.0,
            undPrice=5525.0,
        ),
        impliedVolatility=0.19,
        histVolatility=0.16,
        callOpenInterest=100,
        putOpenInterest=175,
        callVolume=20,
        putVolume=35,
    )

    snapshot = normalize_option_analytics_from_ticker(ticker, contract)

    assert snapshot.model_greeks is not None
    assert snapshot.model_greeks.delta == pytest.approx(-0.42)
    assert snapshot.open_interest == pytest.approx(275)
    assert snapshot.option_volume == pytest.approx(55)
    assert snapshot.implied_volatility == pytest.approx(0.19)
