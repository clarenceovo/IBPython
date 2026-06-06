import json
from datetime import date

import pytest
from pydantic import ValidationError

from src.feeds.fundamental_data import (
    ForecastEventContractCategory,
    FundamentalDataRequest,
    FundamentalReportType,
    WSHEventDataReport,
    WSHEventDataRequest,
    WSHMetadataReport,
)
from src.feeds.models import AssetClass


def test_fundamental_data_request_maps_to_equity_contract_spec() -> None:
    request = FundamentalDataRequest(
        symbol="ibkr",
        exchange="smart",
        primary_exchange="nasdaq",
        report_type=FundamentalReportType.REPORTS_FIN_SUMMARY,
    )

    spec = request.to_contract_spec()

    assert request.symbol == "IBKR"
    assert spec.asset_class is AssetClass.EQUITY
    assert spec.primary_exchange == "nasdaq"


def test_fundamental_data_request_rejects_non_equity_asset_class() -> None:
    with pytest.raises(ValidationError):
        FundamentalDataRequest(symbol="SPX", asset_class="index")


def test_wsh_event_data_request_builds_filter_json() -> None:
    request = WSHEventDataRequest(con_ids=[8314], event_types=["wshe_ed"], limit=5)

    payload = json.loads(request.to_filter_json())

    assert payload["country"] == "All"
    assert payload["watchlist"] == ["8314"]
    assert payload["limit"] == 5
    assert payload["wshe_ed"] == "true"


def test_wsh_event_data_request_accepts_raw_filter_json() -> None:
    raw_filter = '{"country":"All","limit":1}'
    request = WSHEventDataRequest(raw_filter_json=raw_filter)

    assert request.to_filter_json() == raw_filter


def test_wsh_event_data_request_builds_ibkr_wsh_object_kwargs() -> None:
    request = WSHEventDataRequest(
        con_ids=[8314],
        event_types=["wshe_ed"],
        fill_watchlist=True,
        total_limit=25,
    )

    kwargs = request.to_wsh_event_data_kwargs()

    assert json.loads(kwargs["filter"])["watchlist"] == ["8314"]
    assert kwargs["fillWatchlist"] is True
    assert kwargs["totalLimit"] == 25


def test_wsh_event_data_request_supports_date_bounded_watchlist_mode() -> None:
    request = WSHEventDataRequest(
        fill_watchlist=True,
        start_date=date(2026, 6, 1),
        end_date="20260630",
        total_limit=25,
    )

    kwargs = request.to_wsh_event_data_kwargs()

    assert "filter" not in kwargs
    assert kwargs["startDate"] == "20260601"
    assert kwargs["endDate"] == "20260630"
    assert kwargs["fillWatchlist"] is True
    assert kwargs["totalLimit"] == 25


def test_wsh_event_data_request_supports_con_id_mode_without_filter_json() -> None:
    request = WSHEventDataRequest(con_id=8314, start_date="2026-06-01", end_date="2026-06-30", total_limit=10)

    assert request.uses_filter_json is False
    assert request.to_wsh_event_data_kwargs() == {
        "conId": 8314,
        "startDate": "20260601",
        "endDate": "20260630",
        "totalLimit": 10,
    }


def test_wsh_event_data_request_rejects_con_id_plus_filter_fields() -> None:
    with pytest.raises(ValidationError):
        WSHEventDataRequest(con_id=8314, event_types=["wshe_ed"])


def test_wsh_event_data_request_rejects_date_bounds_plus_filter_fields() -> None:
    with pytest.raises(ValidationError):
        WSHEventDataRequest(start_date="20260601", country="US")


def test_wsh_event_data_request_rejects_multiple_event_type_tags() -> None:
    with pytest.raises(ValidationError):
        WSHEventDataRequest(event_types=["wshe_ed", "wshe_bod"])


def test_wsh_event_data_request_rejects_inverted_date_range() -> None:
    with pytest.raises(ValidationError):
        WSHEventDataRequest(start_date="20260630", end_date="20260601")


def test_wsh_reports_parse_raw_json_payloads() -> None:
    metadata = WSHMetadataReport.from_raw_json('{"filters":["country"]}')
    events = WSHEventDataReport.from_raw_json(
        raw_json='[{"event_type":"wshe_ed"}]',
        request_filter_json='{"limit":1}',
    )

    assert metadata.payload == {"filters": ["country"]}
    assert events.payload == [{"event_type": "wshe_ed"}]


def test_forecast_event_contract_category_preserves_tree_payload() -> None:
    category = ForecastEventContractCategory.from_payload(
        {
            "category_id": "economics",
            "label": "Economic Indicators",
            "children": [{"id": "cpi", "name": "CPI"}],
        }
    )

    assert category.id == "economics"
    assert category.name == "Economic Indicators"
    assert category.children[0].id == "cpi"
    assert category.raw["category_id"] == "economics"
