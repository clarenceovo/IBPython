"""Universal symbol resolver tests: every asset class, every supported notation."""

from __future__ import annotations

import pytest

from src.feeds.contracts import ContractSpec
from src.feeds.models import AssetClass
from src.feeds.symbol_resolver import (
    SymbolResolutionError,
    _two_digit_year,
    resolve_symbol,
)


class TestEquity:
    def test_us_equity_defaults_to_smart(self) -> None:
        resolved = resolve_symbol("AAPL")
        assert resolved.asset_class is AssetClass.EQUITY
        assert resolved.exchange == "SMART"
        assert resolved.currency == "USD"

    def test_hk_suffix_maps_to_sehk(self) -> None:
        resolved = resolve_symbol("0700.HK")
        assert resolved.canonical_symbol == "0700"
        assert resolved.exchange == "SEHK"
        assert resolved.currency == "HKD"

    def test_japan_suffix(self) -> None:
        resolved = resolve_symbol("7203.T")
        assert resolved.exchange == "TSEJ"
        assert resolved.currency == "JPY"

    def test_class_b_share_keeps_dot(self) -> None:
        resolved = resolve_symbol("BRK.B")
        assert resolved.canonical_symbol == "BRK.B"


class TestFx:
    @pytest.mark.parametrize("notation", ["EURUSD", "EUR.USD", "EUR-USD", "EUR/USD"])
    def test_notations_converge(self, notation: str) -> None:
        resolved = resolve_symbol(notation)
        assert resolved.asset_class is AssetClass.FX
        assert resolved.canonical_symbol == "EURUSD"
        assert resolved.exchange == "IDEALPRO"
        assert resolved.currency == "USD"

    def test_usd_jpy_quote_currency(self) -> None:
        resolved = resolve_symbol("USD/JPY")
        assert resolved.canonical_symbol == "USDJPY"
        assert resolved.currency == "JPY"


class TestCrypto:
    def test_bare_root(self) -> None:
        resolved = resolve_symbol("BTC")
        assert resolved.asset_class is AssetClass.CRYPTO
        assert resolved.exchange == "PAXOS"
        assert resolved.currency == "USD"

    @pytest.mark.parametrize("notation", ["ETHUSD", "BTCUSD"])
    def test_paired_notation(self, notation: str) -> None:
        resolved = resolve_symbol(notation)
        assert resolved.asset_class is AssetClass.CRYPTO
        assert resolved.canonical_symbol == notation[:-3]

    def test_crypto_beats_fx_detection(self) -> None:
        resolved = resolve_symbol("BTCUSD")
        assert resolved.asset_class is AssetClass.CRYPTO


class TestFutures:
    @pytest.mark.parametrize("notation", ["ES1!", "ES=F", "-ES"])
    def test_continuous_notations(self, notation: str) -> None:
        resolved = resolve_symbol(notation)
        assert resolved.asset_class is AssetClass.FUTURE
        assert resolved.canonical_symbol == "ES"
        assert resolved.continuous is True

    def test_dated_local_symbol_four_digit_year(self) -> None:
        resolved = resolve_symbol("ESZ2026")
        assert resolved.continuous is False
        assert resolved.last_trade_date_or_contract_month == "202612"
        assert resolved.local_symbol == "ESZ2026"

    def test_dated_two_digit_year(self) -> None:
        resolved = resolve_symbol("ESZ26")
        assert resolved.last_trade_date_or_contract_month == "202612"

    def test_dated_one_digit_year_rolls_decade(self) -> None:
        resolved = resolve_symbol("ESM6")
        assert resolved.last_trade_date_or_contract_month == "202606"

    def test_crude_maps_to_nymex(self) -> None:
        resolved = resolve_symbol("CLX26")
        assert resolved.exchange == "NYMEX"
        assert resolved.last_trade_date_or_contract_month == "202611"

    def test_month_code_table(self) -> None:
        resolved = resolve_symbol("ESH27")
        assert resolved.last_trade_date_or_contract_month == "202703"


class TestIndex:
    def test_known_index(self) -> None:
        resolved = resolve_symbol("SPX")
        assert resolved.asset_class is AssetClass.INDEX
        assert resolved.exchange == "CBOE"

    def test_yahoo_alias(self) -> None:
        resolved = resolve_symbol("^GSPC")
        assert resolved.canonical_symbol == "SPX"
        assert "alias" in resolved.matched_rule

    def test_hsi_hkfe(self) -> None:
        resolved = resolve_symbol("HSI")
        assert resolved.exchange == "HKFE"
        assert resolved.currency == "HKD"


class TestExplicitAndOverrides:
    def test_explicit_option_passthrough(self) -> None:
        resolved = resolve_symbol("SPXW", asset_class=AssetClass.OPTION)
        assert resolved.asset_class is AssetClass.OPTION

    def test_explicit_bond_passthrough(self) -> None:
        resolved = resolve_symbol("912828XX", asset_class=AssetClass.BOND)
        assert resolved.asset_class is AssetClass.BOND

    def test_exchange_override(self) -> None:
        resolved = resolve_symbol("AAPL", exchange="NASDAQ")
        assert resolved.exchange == "NASDAQ"

    def test_currency_override(self) -> None:
        resolved = resolve_symbol("0700.HK", currency="CNH")
        assert resolved.currency == "CNH"

    def test_explicit_fx_rejects_non_pair(self) -> None:
        with pytest.raises(SymbolResolutionError):
            resolve_symbol("XYZABC", asset_class=AssetClass.FX)


class TestOutputs:
    def test_contract_spec_round_trip(self) -> None:
        spec = resolve_symbol("ESZ26").to_contract_spec()
        assert isinstance(spec, ContractSpec)
        assert spec.asset_class is AssetClass.FUTURE
        assert spec.last_trade_date_or_contract_month == "202612"
        assert spec.continuous is False

    def test_describe_shape(self) -> None:
        described = resolve_symbol("ES1!").describe()
        assert described["asset_class"] == "future"
        assert described["continuous"] is True
        assert described["matched_rule"] == "future_pattern"

    def test_input_symbol_preserved(self) -> None:
        assert resolve_symbol(" eur.Usd ").input_symbol == " eur.Usd "

    def test_empty_symbol_rejected(self) -> None:
        with pytest.raises(SymbolResolutionError):
            resolve_symbol("   ")


class TestYearExpansion:
    def test_one_digit_current_decade(self) -> None:
        assert _two_digit_year("6") == 2026

    def test_two_digit(self) -> None:
        assert _two_digit_year("26") == 2026

    def test_four_digit_passthrough(self) -> None:
        assert _two_digit_year("2030") == 2030
