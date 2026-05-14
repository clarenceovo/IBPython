import json

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
    request = WSHEventDataRequest(con_ids=[8314], event_types=["wshe_ed", "wshe_bod"], limit=5)

    payload = json.loads(request.to_filter_json())

    assert payload["country"] == "All"
    assert payload["watchlist"] == ["8314"]
    assert payload["limit"] == 5
    assert payload["wshe_bod"] == "true"
    assert payload["wshe_ed"] == "true"


def test_wsh_event_data_request_accepts_raw_filter_json() -> None:
    raw_filter = '{"country":"All","limit":1}'
    request = WSHEventDataRequest(raw_filter_json=raw_filter)

    assert request.to_filter_json() == raw_filter


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
