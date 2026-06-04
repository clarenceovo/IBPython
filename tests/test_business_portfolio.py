from __future__ import annotations

from src.feeds.account import AccountSummaryDTO, AccountValueDTO
from src.webapp.routers.business_portfolio import _account_value_float, _summary_float


def test_account_value_float_parses_numeric_and_comma_values() -> None:
    value = AccountValueDTO(account="DU123", tag="NetLiquidation", value="1,234.56", currency="USD")

    assert _account_value_float(value) == 1234.56


def test_account_value_float_returns_none_for_missing_and_non_numeric_values() -> None:
    assert _account_value_float(None) is None
    assert _account_value_float(AccountValueDTO(account="DU123", tag="NetLiquidation", value="N/A", currency="USD")) is None


def test_summary_float_extracts_matching_tag_and_missing_tags() -> None:
    summary = AccountSummaryDTO(
        account="DU123",
        values={
            "NetLiquidation": AccountValueDTO(account="DU123", tag="NetLiquidation", value="100000", currency="USD"),
        },
    )

    assert _summary_float(summary, "NetLiquidation") == 100000
    assert _summary_float(summary, "AvailableFunds") is None
    assert _summary_float(None, "NetLiquidation") is None
