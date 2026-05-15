import asyncio
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.config import config_constant as constants
from src.transport.scheduler import (
    IndexCompositionReloadJobHandler,
    IndexCompositionReloadParams,
    OHLCVSnapshotParams,
    SchedulerJobDefinition,
)


class FakeIndexCompositionService:
    def __init__(self) -> None:
        self.calls = []

    async def sync_many(self, index_symbols):
        self.calls.append(tuple(index_symbols))
        return []


def test_default_g10_index_symbols_are_expected() -> None:
    assert constants.DEFAULT_G10_INDEX_SYMBOLS == (
        "SPX",
        "TSX",
        "FTSE100",
        "DAX",
        "CAC40",
        "FTSEMIB",
        "NIKKEI225",
        "AEX",
        "BEL20",
        "OMXS30",
        "SMI",
    )


def test_reload_g10_schedulejob_json_parses() -> None:
    payload = json.loads(Path("schedulejob/reload_g10_index_composition.json").read_text())
    job = SchedulerJobDefinition.model_validate(payload)

    assert job.name == "reload_g10_index_composition"
    assert job.job_type == "index_composition_reload"
    assert job.interval_seconds == constants.DEFAULT_INDEX_SYNC_INTERVAL_SECONDS
    assert tuple(job.params["index_symbols"]) == constants.DEFAULT_G10_INDEX_SYMBOLS


def test_all_schedulejob_json_files_parse() -> None:
    for file_path in Path("schedulejob").glob("*.json"):
        payload = json.loads(file_path.read_text())
        job = SchedulerJobDefinition.model_validate(payload)
        if job.job_type == "ohlcv_snapshot":
            params = OHLCVSnapshotParams.model_validate(job.params)
            params.validate_interval(job)


def test_major_indices_schedulejob_contains_expected_symbols() -> None:
    payload = json.loads(Path("schedulejob/ohlcv_major_indices_5m.json").read_text())
    job = SchedulerJobDefinition.model_validate(payload)
    params = OHLCVSnapshotParams.model_validate(job.params)

    assert job.job_type == "ohlcv_snapshot"
    assert job.interval_seconds == 300
    assert {symbol.symbol for symbol in params.symbols} >= {"SPX", "NDX", "VIX", "HSI", "DAX", "FTSE100", "NIKKEI", "SMI"}


def test_index_composition_reload_handler_calls_sync_many() -> None:
    async def run() -> None:
        service = FakeIndexCompositionService()
        handler = IndexCompositionReloadJobHandler(service, provider_name="production_index_provider")
        job = SchedulerJobDefinition(
            name="reload_test",
            job_type="index_composition_reload",
            interval_seconds=60,
            params={"index_symbols": ["spx", "ndx"], "provider": "production_index_provider"},
        )

        await handler(job)

        assert service.calls == [("SPX", "NDX")]

    asyncio.run(run())


def test_index_composition_reload_handler_rejects_missing_symbols() -> None:
    with pytest.raises(ValidationError):
        IndexCompositionReloadParams.model_validate({"provider": "production_index_provider"})


def test_index_composition_reload_handler_rejects_empty_symbols() -> None:
    with pytest.raises(ValidationError):
        IndexCompositionReloadParams.model_validate(
            {"index_symbols": [], "provider": "production_index_provider"}
        )


def test_index_composition_reload_handler_rejects_placeholder_provider() -> None:
    async def run() -> None:
        service = FakeIndexCompositionService()
        handler = IndexCompositionReloadJobHandler(service, provider_name="configured_provider")
        job = SchedulerJobDefinition(
            name="reload_test",
            job_type="index_composition_reload",
            interval_seconds=60,
            params={"index_symbols": ["SPX"], "provider": "configured_provider"},
        )

        with pytest.raises(RuntimeError, match="requires a configured production provider"):
            await handler(job)

    asyncio.run(run())
