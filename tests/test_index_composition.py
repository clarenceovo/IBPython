from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.feeds.index_composition import (
    IndexCompositionScannerRequest,
    build_index_composition_from_scanner_rows,
    resolve_index_composition_scanner_request,
)
from src.feeds.scanner import MarketScannerRow


def test_hsi_index_composition_uses_hk_scanner_preset() -> None:
    request = IndexCompositionScannerRequest(index_symbol="hsi")

    scanner_request = resolve_index_composition_scanner_request(request)

    assert scanner_request.instrument == "STK"
    assert scanner_request.location_code == "STK.HK"
    assert scanner_request.scan_code == "HOT_BY_VOLUME"
    assert scanner_request.max_results == 50


def test_index_composition_scanner_rejects_results_above_ibkr_cap() -> None:
    with pytest.raises(ValidationError):
        IndexCompositionScannerRequest(index_symbol="HSI", max_results=51)


def test_scanner_rows_map_to_non_official_index_composition() -> None:
    request = IndexCompositionScannerRequest(index_symbol="HSI", max_results=2)
    scanner_request = resolve_index_composition_scanner_request(request)
    rows = [
        MarketScannerRow(
            rank=1,
            con_id=12345,
            symbol="0005",
            sec_type="STK",
            exchange="SEHK",
            currency="HKD",
            primary_exchange="SEHK",
            local_symbol="0005",
            long_name="HSBC Holdings PLC",
        )
    ]

    payload = build_index_composition_from_scanner_rows(request, scanner_request, rows)

    assert payload.index_symbol == "HSI"
    assert payload.provider == "ibkr_market_scanner"
    assert payload.is_official_composition is False
    assert payload.metadata["scanner"]["location_code"] == "STK.HK"
    assert payload.constituents[0].symbol == "0005"
    assert payload.constituents[0].name == "HSBC Holdings PLC"
    assert payload.constituents[0].weight is None
    assert payload.constituents[0].con_id == 12345
    assert payload.constituents[0].rank == 1
