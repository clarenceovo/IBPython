from __future__ import annotations

import sys
from types import ModuleType

import pytest

import main
from src.feeds.index_composition import IndexCompositionPayload
from src.config.settings import load_settings
from src.transport.mysql_client import MySQLClient
from src.transport.questdb_client import QuestDBClient
from src.transport.scheduler import SchedulerJobDefinition


def test_build_index_composition_provider_returns_none_for_placeholder() -> None:
    assert main.build_index_composition_provider("") is None
    assert main.build_index_composition_provider("configured_provider") is None


def test_build_index_composition_provider_loads_dynamic_factory(monkeypatch: pytest.MonkeyPatch) -> None:
    class DynamicProvider:
        name = "dynamic_test_provider"

        async def fetch(self, index_symbol: str) -> IndexCompositionPayload:
            return IndexCompositionPayload(
                index_symbol=index_symbol,
                provider=self.name,
                constituents=[],
            )

    module = ModuleType("tests_dynamic_provider")
    module.build_provider = lambda: DynamicProvider()
    monkeypatch.setitem(sys.modules, "tests_dynamic_provider", module)

    provider = main.build_index_composition_provider("tests_dynamic_provider:build_provider")

    assert provider is not None
    assert provider.name == "dynamic_test_provider"


def test_build_index_composition_provider_loads_dynamic_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    class DynamicProvider:
        name = "dynamic_instance_provider"

        async def fetch(self, index_symbol: str) -> IndexCompositionPayload:
            return IndexCompositionPayload(
                index_symbol=index_symbol,
                provider=self.name,
                constituents=[],
            )

    module = ModuleType("tests_dynamic_provider_instance")
    module.provider = DynamicProvider()
    monkeypatch.setitem(sys.modules, "tests_dynamic_provider_instance", module)

    provider = main.build_index_composition_provider("tests_dynamic_provider_instance:provider")

    assert provider is module.provider


def test_build_index_composition_provider_returns_none_for_bad_import_path() -> None:
    assert main.build_index_composition_provider("not_an_import_path") is None


def test_build_market_data_store_selects_questdb_by_default() -> None:
    settings = load_settings(env_file=None, include_os_environ=False)

    assert isinstance(main.build_market_data_store(settings), QuestDBClient)


def test_build_market_data_store_selects_mysql() -> None:
    settings = load_settings(env_file=None, include_os_environ=False, market_data_db_backend="mysql")

    assert isinstance(main.build_market_data_store(settings), MySQLClient)


def test_dependency_plan_for_market_snapshot_jobs() -> None:
    persist_job = SchedulerJobDefinition(
        name="persisting_snapshot",
        job_type="market_snapshot",
        interval_seconds=60,
        params={
            "symbol": "SPY",
            "asset_class": "equity",
            "exchange": "SMART",
            "currency": "USD",
            "duration": "1 D",
            "bar_size": "1 min",
            "persist": True,
        },
    )
    cache_only_job = persist_job.model_copy(
        update={"name": "cache_only_snapshot", "params": {**persist_job.params, "persist": "false"}}
    )

    assert main._jobs_require_ibkr([persist_job]) is True
    assert main._jobs_require_market_store([persist_job]) is True
    assert main._jobs_require_questdb([persist_job]) is True
    assert main._jobs_require_market_store([cache_only_job]) is False
    assert main._jobs_require_questdb([cache_only_job]) is False


def test_dependency_plan_for_ohlcv_snapshot_jobs() -> None:
    persist_job = SchedulerJobDefinition(
        name="ohlcv_snapshot",
        job_type="ohlcv_snapshot",
        interval_seconds=60,
        params={
            "start_time": "09:30",
            "end_time": "16:00",
            "timezone": "UTC",
            "snap_interval_seconds": 60,
            "defaults": {
                "asset_class": "equity",
                "exchange": "SMART",
                "currency": "USD",
                "duration": "1 D",
                "bar_size": "1 min",
                "persist": True,
            },
            "symbols": [{"symbol": "SPY"}],
        },
    )
    cache_only_job = persist_job.model_copy(
        update={"params": {**persist_job.params, "defaults": {**persist_job.params["defaults"], "persist": "false"}}}
    )

    assert main._jobs_require_ibkr([persist_job]) is True
    assert main._jobs_require_market_store([persist_job]) is True
    assert main._jobs_require_questdb([persist_job]) is True
    assert main._jobs_require_market_store([cache_only_job]) is False
    assert main._jobs_require_questdb([cache_only_job]) is False


def test_dependency_plan_for_index_only_jobs_does_not_need_ibkr_or_questdb() -> None:
    job = SchedulerJobDefinition(
        name="reload_index",
        job_type="index_composition_reload",
        interval_seconds=60,
        params={"index_symbols": ["SPX"], "provider": "production_index_provider"},
    )

    assert main._jobs_require_ibkr([job]) is False
    assert main._jobs_require_market_store([job]) is False
    assert main._jobs_require_questdb([job]) is False
