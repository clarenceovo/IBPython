"""Tests for the async/perf fixes: FX snapshot batching, qualification cache, section timeouts."""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import pytest

import src.feeds.ibkr_historical as historical_module
import src.feeds.ibkr_options_feed as options_module
from src.feeds.contracts import ContractSpec
from src.feeds.ibkr_connection import QualificationCache
from src.feeds.ibkr_historical import IBKRHistoricalClient
from src.feeds.ibkr_options_feed import IBKROptionsFeedClient
from src.feeds.models import AssetClass
from src.feeds.options import OptionContractSpec, OptionRight
from src.webapp.consolidated import run_consolidated_sections


# ---------------------------------------------------------------------------
# FX option snapshot capture: batch qualification, one shared wait, cleanup
# ---------------------------------------------------------------------------


class _FakeLease:
    def __init__(self, tracker: list[str]) -> None:
        self._tracker = tracker

    async def release(self) -> None:
        self._tracker.append("release")


class _FakeFXConnection:
    def __init__(self) -> None:
        self.last_ibkr_error = None
        self.ib: object = None

    async def ensure_connected(self) -> None:
        return None

    async def with_retry(self, factory, operation: str = ""):
        return await factory()


class _FakeFXIB:
    def __init__(self) -> None:
        self.qualify_calls: list[tuple[object, ...]] = []
        self.subscriptions: list[object] = []
        self.cancellations: list[object] = []

    async def qualifyContractsAsync(self, *contracts: object) -> list[object | None]:
        self.qualify_calls.append(contracts)
        # ib_async semantics: result aligned with inputs, None on failure.
        for position, contract in enumerate(contracts):
            contract.conId = 1000 + position
        return list(contracts)

    def reqMktData(self, contract: object, **_kwargs: object) -> object:
        self.subscriptions.append(contract)
        return SimpleNamespace(last=1.0, bid=0.9, ask=1.1, time=None)

    def cancelMktData(self, contract: object) -> None:
        self.cancellations.append(contract)


def _fx_spec(strike: float) -> OptionContractSpec:
    return OptionContractSpec(
        underlying_symbol="EURUSD",
        expiry="20260619",
        strike=strike,
        right=OptionRight.PUT,
        exchange="SMART",
    )


def test_fx_option_capture_batches_qualification_and_waits_once(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _FakeFXConnection()
    fake_ib = _FakeFXIB()
    connection.ib = fake_ib
    events: list[str] = []
    lease_tracker: list[str] = []

    async def fake_acquire(*_args: object, **_kwargs: object) -> _FakeLease:
        events.append("lease")
        return _FakeLease(lease_tracker)

    async def fake_wait(*_args: object, **_kwargs: object) -> None:
        return None

    real_sleep = asyncio.sleep

    async def counting_sleep(seconds: float) -> None:
        events.append(f"sleep:{seconds}")
        await real_sleep(0)

    monkeypatch.setattr(options_module, "acquire_market_data_line", fake_acquire)
    monkeypatch.setattr(options_module, "wait_for_ibkr_request", fake_wait)
    monkeypatch.setattr(options_module.asyncio, "sleep", counting_sleep)
    client = IBKROptionsFeedClient(connection, SimpleNamespace())  # type: ignore[arg-type]

    specs = [_fx_spec(1.10), _fx_spec(1.15), _fx_spec(1.20)]
    snapshots = asyncio.run(
        client.capture_fx_option_snapshots(specs, symbols=["EURUSD"] * 3, snapshot_wait_seconds=2.0)
    )

    # One batched qualification call covering all contracts.
    assert len(fake_ib.qualify_calls) == 1
    assert len(fake_ib.qualify_calls[0]) == 3
    # One shared wait for the whole batch, not one per contract.
    assert events.count("sleep:2.0") == 1
    # All three subscribed, all three cancelled, all leases released.
    assert len(fake_ib.subscriptions) == 3
    assert len(fake_ib.cancellations) == 3
    assert lease_tracker.count("release") == 3
    assert [snapshot.strike for snapshot in snapshots] == [1.10, 1.15, 1.20]
    assert all(snapshot.con_id for snapshot in snapshots)


def test_fx_option_capture_releases_leases_when_subscription_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _FakeFXConnection()

    class _FailingIB(_FakeFXIB):
        def reqMktData(self, contract: object, **kwargs: object) -> object:
            self.subscriptions.append(contract)
            if len(self.subscriptions) == 2:
                raise RuntimeError("subscription rejected")
            return SimpleNamespace(last=1.0)

    fake_ib = _FailingIB()
    connection.ib = fake_ib
    lease_tracker: list[str] = []

    async def fake_acquire(*_args: object, **_kwargs: object) -> _FakeLease:
        return _FakeLease(lease_tracker)

    async def fake_wait(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(options_module, "acquire_market_data_line", fake_acquire)
    monkeypatch.setattr(options_module, "wait_for_ibkr_request", fake_wait)
    client = IBKROptionsFeedClient(connection, SimpleNamespace())  # type: ignore[arg-type]

    specs = [_fx_spec(1.10), _fx_spec(1.15)]
    with pytest.raises(RuntimeError, match="subscription rejected"):
        asyncio.run(client.capture_fx_option_snapshots(specs, symbols=["EURUSD"] * 2))

    # The failing lease releases inline; the subscribed one releases in cleanup.
    assert lease_tracker.count("release") == 2
    assert len(fake_ib.cancellations) == 1


def test_fx_option_capture_handles_unqualified_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _FakeFXConnection()

    class _PartialIB(_FakeFXIB):
        async def qualifyContractsAsync(self, *contracts: object) -> list[object | None]:
            self.qualify_calls.append(contracts)
            for position, contract in enumerate(contracts):
                contract.conId = 1000 + position
            return [contracts[0], None]

    fake_ib = _PartialIB()
    connection.ib = fake_ib
    lease_tracker: list[str] = []

    async def fake_acquire(*_args: object, **_kwargs: object) -> _FakeLease:
        return _FakeLease(lease_tracker)

    async def fake_wait(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(options_module, "acquire_market_data_line", fake_acquire)
    monkeypatch.setattr(options_module, "wait_for_ibkr_request", fake_wait)
    client = IBKROptionsFeedClient(connection, SimpleNamespace())  # type: ignore[arg-type]

    snapshots = asyncio.run(
        client.capture_fx_option_snapshots(
            [_fx_spec(1.10), _fx_spec(1.15)], symbols=["EURUSD"] * 2, snapshot_wait_seconds=0.01
        )
    )

    assert len(snapshots) == 2
    assert snapshots[0].con_id == 1000
    assert snapshots[1].con_id is None  # unqualified contract keeps spec con_id (None)
    assert len(fake_ib.cancellations) == 2


# ---------------------------------------------------------------------------
# Qualification cache
# ---------------------------------------------------------------------------


def _spec(symbol: str = "AAPL") -> ContractSpec:
    return ContractSpec(symbol=symbol, asset_class=AssetClass.EQUITY, exchange="SMART")


def test_qualification_cache_stores_and_returns_deep_copies() -> None:
    cache = QualificationCache(ttl_seconds=60)
    spec = _spec()
    contract = SimpleNamespace(conId=123)

    cache.set(spec, contract)
    first = cache.get(spec)
    second = cache.get(spec)

    assert first is not None and second is not None
    assert first is not contract and first is not second
    assert first.conId == 123
    assert cache.stats()["hits"] == 2
    assert cache.stats()["misses"] == 0


def test_qualification_cache_expires_and_clears() -> None:
    cache = QualificationCache(ttl_seconds=0.01)
    spec = _spec()

    cache.set(spec, SimpleNamespace(conId=1))
    assert cache.get(spec) is not None

    time.sleep(0.02)
    assert cache.get(spec) is None

    cache.set(spec, SimpleNamespace(conId=2))
    cache.clear()
    assert cache.get(spec) is None


def test_qualification_cache_evicts_oldest_at_capacity() -> None:
    cache = QualificationCache(ttl_seconds=60, max_size=2)
    cache.set(_spec("A"), SimpleNamespace(conId=1))
    cache.set(_spec("B"), SimpleNamespace(conId=2))
    cache.set(_spec("C"), SimpleNamespace(conId=3))

    assert cache.get(_spec("A")) is None
    assert cache.get(_spec("B")) is not None
    assert cache.get(_spec("C")) is not None


class _FakeQualConnection:
    def __init__(self) -> None:
        self.qualification_cache = QualificationCache()
        self.last_ibkr_error = None
        self.retry_calls = 0
        self.ib: object = None

    async def ensure_connected(self) -> None:
        return None

    async def with_retry(self, factory, operation: str = ""):
        self.retry_calls += 1
        return await factory()


def test_historical_client_qualify_contract_uses_session_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _FakeQualConnection()
    qualified = SimpleNamespace(conId=999, exchange="SMART")

    class _FakeQualIB:
        async def qualifyContractsAsync(self, contract: object) -> list[object]:
            return [contract]

    connection.ib = _FakeQualIB()
    monkeypatch.setattr(historical_module, "build_ibkr_contract", lambda spec: qualified)
    monkeypatch.setattr(historical_module, "_contract_int", lambda value, name: getattr(value, "conId", None))
    monkeypatch.setattr(historical_module, "_contract_text", lambda value, *names: None)
    client = IBKRHistoricalClient(connection)  # type: ignore[arg-type]

    first = asyncio.run(client.qualify_contract(_spec()))
    second = asyncio.run(client.qualify_contract(_spec()))

    assert connection.retry_calls == 1
    assert first is not second  # deep copies protect the cached master
    assert first.conId == 999 and second.conId == 999
    assert connection.qualification_cache.stats()["hits"] == 1


# ---------------------------------------------------------------------------
# Consolidated section timeout
# ---------------------------------------------------------------------------


def test_run_consolidated_sections_times_out_stuck_section() -> None:
    async def slow() -> object:
        await asyncio.sleep(0.5)
        return "never"

    async def fast() -> object:
        return "ok"

    results, meta = asyncio.run(
        run_consolidated_sections(
            {"slow": slow, "fast": fast},
            section_timeout_seconds=0.05,
        )
    )

    assert results["fast"] == "ok"
    assert meta["fast"].status == "ok"
    assert results["slow"] is None
    assert meta["slow"].status == "error"
    assert "section timed out after" in (meta["slow"].error or "")


def test_run_consolidated_sections_without_timeout_keeps_old_behavior() -> None:
    async def fine() -> object:
        return 42

    results, meta = asyncio.run(run_consolidated_sections({"fine": fine}))

    assert results == {"fine": 42}
    assert meta["fine"].status == "ok"
