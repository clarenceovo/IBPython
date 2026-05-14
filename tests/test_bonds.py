from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from src.feeds.bonds import (
    BondInstrument,
    BondYieldField,
    BondYieldHistoryRequest,
    BondYieldQuote,
    CTDBondCandidate,
    CTDBondSnapshot,
    DEFAULT_CTD_FUTURE_DEFINITIONS,
    SovereignBondMarket,
    YieldCurveBootstrapInstrument,
    YieldCurveDTO,
    normalize_ibkr_bond_yield_bars,
)
from src.feeds.contracts import ibkr_contract_kwargs


def test_bond_instrument_maps_isin_into_ibkr_contract_kwargs() -> None:
    bond = BondInstrument(isin="us9128285m81", currency="usd")

    kwargs = ibkr_contract_kwargs(bond.to_contract_spec())

    assert kwargs["secType"] == "BOND"
    assert kwargs["secIdType"] == "ISIN"
    assert kwargs["secId"] == "US9128285M81"


def test_bond_instrument_can_use_con_id_without_symbol() -> None:
    bond = BondInstrument(con_id=123456, currency="USD")

    kwargs = ibkr_contract_kwargs(bond.to_contract_spec())

    assert kwargs["symbol"] == "123456"
    assert kwargs["conId"] == 123456


def test_bond_yield_quote_mid_and_decimal_conversion() -> None:
    quote = BondYieldQuote(
        bond=BondInstrument(symbol="91282CJL6"),
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        bid_yield=4.0,
        ask_yield=4.2,
    )

    assert quote.mid_yield == pytest.approx(4.1)
    assert quote.yield_as_decimal(quote.mid_yield) == pytest.approx(0.041)


def test_ctd_snapshot_selects_lowest_net_basis_candidate() -> None:
    future = DEFAULT_CTD_FUTURE_DEFINITIONS[0]
    cheap = CTDBondCandidate(
        future=future,
        bond=BondInstrument(symbol="91282CJL6"),
        futures_price=110,
        clean_price=108,
        conversion_factor=0.98,
        carry=0.1,
    )
    rich = CTDBondCandidate(
        future=future,
        bond=BondInstrument(symbol="91282CHT1"),
        futures_price=110,
        clean_price=109,
        conversion_factor=0.98,
        carry=0,
    )
    snapshot = CTDBondSnapshot(
        market=SovereignBondMarket.US_TREASURY,
        futures_symbol=future.futures_symbol,
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        candidates=(rich, cheap),
    )

    assert snapshot.selected_ctd().bond.symbol == "91282CJL6"


def test_yield_curve_bootstrap_and_bootscrape_alias() -> None:
    curve = YieldCurveDTO(
        curve_id="USD_TEST",
        currency="usd",
        valuation_date=date(2026, 1, 1),
        input_instruments=(
            YieldCurveBootstrapInstrument(
                instrument_id="1Y",
                maturity_date=date(2027, 1, 1),
                par_yield=0.05,
                coupon_frequency=1,
            ),
        ),
    )

    bootstrapped = curve.bootstrap()
    alias = curve.bootscrape()

    assert bootstrapped.points[0].discount_factor == pytest.approx(1 / 1.05)
    assert alias.points[0].zero_rate == pytest.approx(bootstrapped.points[0].zero_rate)


def test_normalize_ibkr_bond_yield_bars_accepts_daily_date() -> None:
    request = BondYieldHistoryRequest(
        bond=BondInstrument(symbol="91282CJL6"),
        bar_size="1 day",
        yield_fields=(BondYieldField.LAST,),
    )
    raw = [SimpleNamespace(date=date(2026, 1, 2), open=4.0, high=4.2, low=3.9, close=4.1)]

    bars = normalize_ibkr_bond_yield_bars(raw, request, BondYieldField.LAST)

    assert bars[0].timestamp == datetime(2026, 1, 2, tzinfo=timezone.utc)
    assert bars[0].yield_field is BondYieldField.LAST
