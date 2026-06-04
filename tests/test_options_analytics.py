from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

import src.feeds.ibkr_feed as ibkr_feed_module
from src.feeds.contracts import OptionChain
from src.feeds.ibkr_feed import IBKRFeedClient
from src.feeds.ibkr_options_feed import format_unqualified_option_message
from src.feeds.models import AssetClass
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


def test_option_analytics_request_allows_empty_generic_ticks_for_true_snapshot() -> None:
    request = OptionAnalyticsRequest(
        contract=OptionContractSpec(
            underlying_symbol="SPY",
            expiry="20260619",
            strike=500,
            right=OptionRight.CALL,
        ),
        generic_ticks=[],
    )

    assert request.generic_ticks == ()
    assert request.generic_tick_list == ""


def test_option_contract_spec_supports_futures_options() -> None:
    contract = OptionContractSpec(
        sec_type="fop",
        underlying_symbol="cl",
        expiry="20260617",
        strike=80,
        right=OptionRight.CALL,
        exchange="nymex",
        multiplier="1000",
    )

    assert contract.sec_type == "FOP"
    assert contract.underlying_symbol == "CL"
    assert contract.exchange == "NYMEX"


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


def test_unqualified_option_message_points_to_listed_expirations() -> None:
    contract = OptionContractSpec(
        underlying_symbol="TSLA",
        expiry="20260608",
        strike=345,
        right=OptionRight.CALL,
    )
    chain = OptionChain(
        underlying_symbol="TSLA",
        underlying_asset_class=AssetClass.EQUITY,
        underlying_con_id=76792991,
        exchange="SMART",
        trading_class="TSLA",
        multiplier="100",
        expirations=("20260605", "20260612", "20260618"),
        strikes=(340, 345, 350),
    )

    message = format_unqualified_option_message(contract, chains=(chain,), last_ibkr_error=(200, "No security definition"))

    assert "expiry 20260608 is not listed" in message
    assert "20260605" in message
    assert "20260612" in message
    assert "Try trading_class='TSLA'" in message


def test_option_analytics_rejects_unqualified_contract_before_market_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_contract = SimpleNamespace(symbol="TSLA")
    chain = OptionChain(
        underlying_symbol="TSLA",
        underlying_asset_class=AssetClass.EQUITY,
        underlying_con_id=76792991,
        exchange="SMART",
        trading_class="TSLA",
        multiplier="100",
        expirations=("20260605", "20260612", "20260618"),
        strikes=(340, 345, 350),
    )

    def build_fake_option_contract(_contract_spec: OptionContractSpec) -> object:
        return fake_contract

    class FakeIB:
        def isConnected(self) -> bool:
            return True

        async def qualifyContractsAsync(self, _contract: object) -> list[object]:
            return []

        def reqMktData(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("unqualified options should not request market data")

    async def fake_load_option_chains(_request: object) -> list[OptionChain]:
        return [chain]

    monkeypatch.setattr(ibkr_feed_module, "build_ibkr_option_contract", build_fake_option_contract)
    client = IBKRFeedClient()
    client._ib = FakeIB()
    monkeypatch.setattr(client, "load_option_chains", fake_load_option_chains)
    request = OptionAnalyticsRequest(
        contract=OptionContractSpec(
            underlying_symbol="TSLA",
            expiry="20260608",
            strike=345,
            right=OptionRight.CALL,
        ),
        snapshot_wait_seconds=0.001,
    )

    with pytest.raises(Exception, match="expiry 20260608 is not listed"):
        asyncio.run(client.load_option_analytics(request))


def test_option_analytics_uses_streaming_subscription_when_generic_ticks_are_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    fake_contract = SimpleNamespace(conId=123, symbol="TSLA")

    def build_fake_option_contract(_contract_spec: OptionContractSpec) -> object:
        return fake_contract

    class FakeIB:
        def isConnected(self) -> bool:
            return True

        async def qualifyContractsAsync(self, contract: object) -> list[object]:
            return [contract]

        def reqMktData(
            self,
            contract: object,
            *,
            genericTickList: str,
            snapshot: bool,
            regulatorySnapshot: bool,
            mktDataOptions: list[object],
        ) -> object:
            captured["contract"] = contract
            captured["genericTickList"] = genericTickList
            captured["snapshot"] = snapshot
            captured["regulatorySnapshot"] = regulatorySnapshot
            captured["mktDataOptions"] = mktDataOptions
            return SimpleNamespace(
                modelGreeks=SimpleNamespace(impliedVol=0.42, delta=0.25),
                callOpenInterest=100,
                callVolume=10,
            )

        def cancelMktData(self, contract: object) -> None:
            captured["cancelled"] = contract

    monkeypatch.setattr(ibkr_feed_module, "build_ibkr_option_contract", build_fake_option_contract)
    client = IBKRFeedClient()
    client._ib = FakeIB()
    request = OptionAnalyticsRequest(
        contract=OptionContractSpec(
            underlying_symbol="TSLA",
            expiry="20260518",
            strike=270,
            right=OptionRight.PUT,
        ),
        snapshot_wait_seconds=0.001,
        regulatory_snapshot=True,
    )

    snapshot = asyncio.run(client.load_option_analytics(request))

    assert captured["genericTickList"] == "100,101,104,105,106"
    assert captured["snapshot"] is False
    assert captured["regulatorySnapshot"] is False
    assert captured["cancelled"] is fake_contract
    assert snapshot.model_greeks is not None
    assert snapshot.model_greeks.implied_vol == pytest.approx(0.42)


def test_option_analytics_uses_snapshot_subscription_without_generic_ticks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    fake_contract = SimpleNamespace(conId=123, symbol="SPY")

    def build_fake_option_contract(_contract_spec: OptionContractSpec) -> object:
        return fake_contract

    class FakeIB:
        def isConnected(self) -> bool:
            return True

        async def qualifyContractsAsync(self, contract: object) -> list[object]:
            return [contract]

        def reqMktData(
            self,
            contract: object,
            *,
            genericTickList: str,
            snapshot: bool,
            regulatorySnapshot: bool,
            mktDataOptions: list[object],
        ) -> object:
            captured["genericTickList"] = genericTickList
            captured["snapshot"] = snapshot
            captured["regulatorySnapshot"] = regulatorySnapshot
            return SimpleNamespace(modelGreeks=SimpleNamespace(impliedVol=0.2, delta=0.4))

        def cancelMktData(self, contract: object) -> None:
            captured["cancelled"] = contract

    monkeypatch.setattr(ibkr_feed_module, "build_ibkr_option_contract", build_fake_option_contract)
    client = IBKRFeedClient()
    client._ib = FakeIB()
    request = OptionAnalyticsRequest(
        contract=OptionContractSpec(
            underlying_symbol="SPY",
            expiry="20260619",
            strike=500,
            right=OptionRight.CALL,
        ),
        generic_ticks=[],
        snapshot_wait_seconds=0.001,
        regulatory_snapshot=True,
    )

    asyncio.run(client.load_option_analytics(request))

    assert captured["genericTickList"] == ""
    assert captured["snapshot"] is True
    assert captured["regulatorySnapshot"] is True
