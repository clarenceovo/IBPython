import pytest

from src.feeds.contracts import ContractSpec, ibkr_contract_kwargs


def test_equity_contract_mapping() -> None:
    spec = ContractSpec(symbol="SPY", asset_class="equity", exchange="SMART", currency="USD")

    assert ibkr_contract_kwargs(spec) == {
        "secType": "STK",
        "symbol": "SPY",
        "exchange": "SMART",
        "currency": "USD",
    }


def test_fx_contract_mapping_splits_pair() -> None:
    spec = ContractSpec(symbol="EURUSD", asset_class="fx", exchange="SMART", currency="USD")

    assert ibkr_contract_kwargs(spec) == {
        "secType": "CASH",
        "symbol": "EUR",
        "exchange": "IDEALPRO",
        "currency": "USD",
    }


def test_future_requires_expiry() -> None:
    with pytest.raises(ValueError):
        ContractSpec(symbol="ES", asset_class="future", exchange="CME", currency="USD")


def test_future_contract_mapping() -> None:
    spec = ContractSpec(
        symbol="ES",
        asset_class="future",
        exchange="CME",
        currency="USD",
        last_trade_date_or_contract_month="202606",
        multiplier="50",
    )

    assert ibkr_contract_kwargs(spec) == {
        "secType": "FUT",
        "symbol": "ES",
        "exchange": "CME",
        "currency": "USD",
        "lastTradeDateOrContractMonth": "202606",
        "multiplier": "50",
    }
