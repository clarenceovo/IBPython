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


def test_contract_spec_from_ohlcv_request_preserves_identifiers() -> None:
    from src.feeds.models import OHLCVRequest

    request = OHLCVRequest(
        symbol="ES",
        asset_class="future",
        exchange="CME",
        currency="USD",
        last_trade_date_or_contract_month="202606",
        multiplier="50",
        local_symbol="ESM6",
    )

    spec = ContractSpec.from_ohlcv_request(request)

    assert spec.last_trade_date_or_contract_month == "202606"
    assert spec.multiplier == "50"
    assert spec.local_symbol == "ESM6"


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


def test_hkfe_future_contract_mapping_derives_local_symbol() -> None:
    spec = ContractSpec(
        symbol="HTI",
        asset_class="future",
        exchange="HKFE",
        currency="HKD",
        last_trade_date_or_contract_month="202606",
    )

    assert ibkr_contract_kwargs(spec) == {
        "secType": "FUT",
        "symbol": "HTI",
        "exchange": "HKFE",
        "currency": "HKD",
        "lastTradeDateOrContractMonth": "202606",
        "localSymbol": "HTIM6",
    }


def test_future_contract_mapping_supports_local_symbol_without_expiry() -> None:
    spec = ContractSpec(
        symbol="ES",
        asset_class="future",
        exchange="CME",
        currency="USD",
        local_symbol="ESM6",
    )

    assert ibkr_contract_kwargs(spec) == {
        "secType": "FUT",
        "symbol": "ES",
        "exchange": "CME",
        "currency": "USD",
        "localSymbol": "ESM6",
    }


def test_futures_option_contract_mapping_uses_fop_sec_type() -> None:
    spec = ContractSpec(
        symbol="CL",
        asset_class="option",
        exchange="NYMEX",
        currency="USD",
        option_sec_type="FOP",
        underlying_symbol="CL",
        expiry="20260617",
        strike=80,
        right="call",
        multiplier="1000",
        trading_class="LO",
    )

    assert ibkr_contract_kwargs(spec) == {
        "secType": "FOP",
        "symbol": "CL",
        "exchange": "NYMEX",
        "currency": "USD",
        "lastTradeDateOrContractMonth": "20260617",
        "strike": 80.0,
        "right": "C",
        "multiplier": "1000",
        "tradingClass": "LO",
    }


def test_fx_option_contract_mapping_uses_opt_with_base_and_quote() -> None:
    spec = ContractSpec(
        symbol="EURUSD 20260619C1.1",
        asset_class="option",
        exchange="SMART",
        currency="USD",
        option_sec_type="OPT",
        underlying_symbol="EUR",
        expiry="20260619",
        strike=1.10,
        right="call",
        multiplier="100",
    )

    assert ibkr_contract_kwargs(spec) == {
        "secType": "OPT",
        "symbol": "EUR",
        "exchange": "SMART",
        "currency": "USD",
        "lastTradeDateOrContractMonth": "20260619",
        "strike": 1.10,
        "right": "C",
        "multiplier": "100",
    }
