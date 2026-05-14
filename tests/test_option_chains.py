import asyncio
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from src.feeds.contracts import OptionChainRequest
import src.feeds.ibkr_feed as ibkr_feed_module
from src.feeds.ibkr_feed import IBKRFeedClient, _ibkr_sec_type_for_option_underlying, normalize_ibkr_option_chains
from src.feeds.models import AssetClass


def test_option_chain_request_supports_equity_and_index() -> None:
    equity = OptionChainRequest(symbol="spy", asset_class="equity")
    index = OptionChainRequest(symbol="spx", asset_class="index", exchange="cboe")

    assert equity.to_contract_spec().asset_class is AssetClass.EQUITY
    assert index.to_contract_spec().asset_class is AssetClass.INDEX
    assert index.exchange == "CBOE"


def test_option_chain_request_can_carry_underlying_con_id() -> None:
    request = OptionChainRequest(symbol="tsla", asset_class="equity", underlying_con_id=76792991)

    assert request.underlying_con_id == 76792991
    assert request.to_contract_spec().con_id == 76792991


def test_option_chain_request_rejects_unsupported_underlying() -> None:
    with pytest.raises(ValidationError):
        OptionChainRequest(symbol="ES", asset_class="future")


def test_ibkr_option_underlying_sec_type_mapping() -> None:
    assert _ibkr_sec_type_for_option_underlying(AssetClass.EQUITY) == "STK"
    assert _ibkr_sec_type_for_option_underlying(AssetClass.INDEX) == "IND"


def test_normalize_ibkr_option_chains_sorts_expirations_and_strikes() -> None:
    request = OptionChainRequest(symbol="SPX", asset_class="index", exchange="CBOE")
    raw_chain = SimpleNamespace(
        exchange="CBOE",
        tradingClass="SPXW",
        multiplier="100",
        expirations={"20260619", "20260116"},
        strikes={5000.0, 4500.0},
    )

    chains = normalize_ibkr_option_chains([raw_chain], request, underlying_con_id=416904)

    assert len(chains) == 1
    assert chains[0].underlying_symbol == "SPX"
    assert chains[0].underlying_asset_class is AssetClass.INDEX
    assert chains[0].underlying_con_id == 416904
    assert chains[0].expirations == ("20260116", "20260619")
    assert chains[0].strikes == (4500.0, 5000.0)


def test_qualify_contract_falls_back_to_contract_details_for_smart_equity(monkeypatch: pytest.MonkeyPatch) -> None:
    def build_fake_contract(spec: object) -> SimpleNamespace:
        return SimpleNamespace(
            symbol=getattr(spec, "symbol"),
            secType="STK",
            exchange=getattr(spec, "exchange"),
            currency=getattr(spec, "currency"),
            primaryExchange=getattr(spec, "primary_exchange", None),
        )

    class FakeIB:
        def isConnected(self) -> bool:
            return True

        async def qualifyContractsAsync(self, _contract: object) -> list[object]:
            return []

        async def reqContractDetailsAsync(self, _contract: object) -> list[SimpleNamespace]:
            return [
                SimpleNamespace(
                    contract=SimpleNamespace(
                        symbol="TSLA",
                        secType="STK",
                        exchange="SMART",
                        currency="USD",
                        primaryExchange="ARCA",
                        conId=1,
                    )
                ),
                SimpleNamespace(
                    contract=SimpleNamespace(
                        symbol="TSLA",
                        secType="STK",
                        exchange="SMART",
                        currency="USD",
                        primaryExchange="NASDAQ",
                        conId=2,
                    )
                ),
            ]

    monkeypatch.setattr(ibkr_feed_module, "build_ibkr_contract", build_fake_contract)
    client = IBKRFeedClient()
    client._ib = FakeIB()

    selected = asyncio.run(client.qualify_contract(OptionChainRequest(symbol="TSLA", asset_class="equity").to_contract_spec()))

    assert selected.conId == 2
    assert selected.primaryExchange == "NASDAQ"
    assert selected.exchange == "SMART"


def test_load_option_chains_uses_provided_underlying_con_id_without_qualification() -> None:
    class FakeIB:
        seen_request: tuple[str, str, str, int] | None = None

        def isConnected(self) -> bool:
            return True

        async def qualifyContractsAsync(self, _contract: object) -> list[object]:
            raise AssertionError("underlying_con_id should skip qualification")

        async def reqSecDefOptParamsAsync(
            self,
            symbol: str,
            fut_fop_exchange: str,
            underlying_sec_type: str,
            underlying_con_id: int,
        ) -> list[SimpleNamespace]:
            self.seen_request = (symbol, fut_fop_exchange, underlying_sec_type, underlying_con_id)
            return [
                SimpleNamespace(
                    exchange="SMART",
                    tradingClass="TSLA",
                    multiplier="100",
                    expirations={"20260619"},
                    strikes={250.0},
                )
            ]

    fake_ib = FakeIB()
    client = IBKRFeedClient()
    client._ib = fake_ib
    request = OptionChainRequest(symbol="TSLA", asset_class="equity", underlying_con_id=76792991)

    chains = asyncio.run(client.load_option_chains(request))

    assert fake_ib.seen_request == ("TSLA", "", "STK", 76792991)
    assert chains[0].underlying_con_id == 76792991
