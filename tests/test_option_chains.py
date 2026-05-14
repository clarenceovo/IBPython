from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from src.feeds.contracts import OptionChainRequest
from src.feeds.ibkr_feed import _ibkr_sec_type_for_option_underlying, normalize_ibkr_option_chains
from src.feeds.models import AssetClass


def test_option_chain_request_supports_equity_and_index() -> None:
    equity = OptionChainRequest(symbol="spy", asset_class="equity")
    index = OptionChainRequest(symbol="spx", asset_class="index", exchange="cboe")

    assert equity.to_contract_spec().asset_class is AssetClass.EQUITY
    assert index.to_contract_spec().asset_class is AssetClass.INDEX
    assert index.exchange == "CBOE"


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
