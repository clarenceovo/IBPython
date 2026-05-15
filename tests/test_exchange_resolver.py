"""Tests for exchange_resolver — symbol suffix → IBKR exchange mapping."""

import pytest

from src.feeds.exchange_resolver import resolve_equity


class TestResolveUSEquity:
    """US equities default to SMART/USD with primary_exchange lookup."""

    def test_tsla_resolves_nasdaq(self):
        r = resolve_equity("TSLA")
        assert r.symbol == "TSLA"
        assert r.exchange == "SMART"
        assert r.currency == "USD"
        assert r.primary_exchange == "NASDAQ"

    def test_spy_resolves_arca(self):
        r = resolve_equity("SPY")
        assert r.symbol == "SPY"
        assert r.primary_exchange == "ARCA"

    def test_jpm_resolves_nyse(self):
        r = resolve_equity("JPM")
        assert r.primary_exchange == "NYSE"

    def test_unknown_us_ticker_defaults_smart(self):
        r = resolve_equity("OBSCUREXYZ")
        assert r.exchange == "SMART"
        assert r.currency == "USD"
        assert r.primary_exchange == ""


class TestResolveHongKong:
    def test_0700_hk(self):
        r = resolve_equity("0700.HK")
        assert r.symbol == "0700"
        assert r.exchange == "SEHK"
        assert r.currency == "HKD"
        assert r.primary_exchange == "SEHK"

    def test_9988_hk(self):
        r = resolve_equity("9988.HK")
        assert r.symbol == "9988"
        assert r.exchange == "SEHK"

    def test_case_insensitive(self):
        r = resolve_equity("0700.hk")
        assert r.symbol == "0700"
        assert r.exchange == "SEHK"


class TestResolveJapan:
    def test_7203_t(self):
        r = resolve_equity("7203.T")
        assert r.symbol == "7203"
        assert r.exchange == "TSEJ"
        assert r.currency == "JPY"


class TestResolveEurope:
    def test_london_suffix(self):
        r = resolve_equity("HSBA.L")
        assert r.symbol == "HSBA"
        assert r.exchange == "LSE"
        assert r.currency == "GBP"

    def test_xetra_de_suffix(self):
        r = resolve_equity("SAP.DE")
        assert r.symbol == "SAP"
        assert r.exchange == "IBIS2"
        assert r.currency == "EUR"

    def test_xetra_f_suffix(self):
        r = resolve_equity("SAP.F")
        assert r.symbol == "SAP"
        assert r.exchange == "IBIS2"

    def test_paris_suffix(self):
        r = resolve_equity("MC.PA")
        assert r.symbol == "MC"
        assert r.exchange == "SBF"
        assert r.currency == "EUR"


class TestResolveOtherMarkets:
    def test_toronto_suffix(self):
        r = resolve_equity("RY.TO")
        assert r.symbol == "RY"
        assert r.exchange == "TSE"
        assert r.currency == "CAD"

    def test_australia_suffix(self):
        r = resolve_equity("BHP.AX")
        assert r.symbol == "BHP"
        assert r.exchange == "ASX"
        assert r.currency == "AUD"

    def test_singapore_suffix(self):
        r = resolve_equity("D05.SI")
        assert r.symbol == "D05"
        assert r.exchange == "SGX"
        assert r.currency == "SGD"

    def test_shanghai_suffix(self):
        r = resolve_equity("600519.SS")
        assert r.symbol == "600519"
        assert r.exchange == "SEHKNTL"
        assert r.currency == "CNH"

    def test_shenzhen_suffix(self):
        r = resolve_equity("000001.SZ")
        assert r.symbol == "000001"
        assert r.exchange == "SEHKSZSE"

    def test_korea_suffix(self):
        r = resolve_equity("005930.KS")
        assert r.symbol == "005930"
        assert r.exchange == "KSE"
        assert r.currency == "KRW"


class TestEdgeCases:
    def test_empty_raises(self):
        with pytest.raises(ValueError, match="empty"):
            resolve_equity("")

    def test_whitespace_stripped(self):
        r = resolve_equity("  TSLA  ")
        assert r.symbol == "TSLA"

    def test_suffix_dot_no_match(self):
        """A dot not matching any suffix should fall through to default."""
        r = resolve_equity("FOO.XX")
        assert r.exchange == "SMART"
        assert r.currency == "USD"
