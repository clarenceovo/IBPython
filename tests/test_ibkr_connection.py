from __future__ import annotations

import asyncio

from src.feeds.ibkr_connection import IBKRConnectionManager


def test_ibkr_disconnect_cancels_pending_reconnect_tasks() -> None:
    async def run() -> None:
        manager = IBKRConnectionManager()
        task_started = asyncio.Event()
        task_cancelled = asyncio.Event()

        async def reconnect_sleeper() -> None:
            task_started.set()
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                task_cancelled.set()
                raise

        task = asyncio.create_task(reconnect_sleeper())
        manager._background_tasks.add(task)
        await task_started.wait()

        await manager.disconnect()

        assert manager.shutting_down is True
        assert task_cancelled.is_set()
        assert task.cancelled()
        assert manager._background_tasks == set()

    asyncio.run(run())


def test_ibkr_disconnect_closes_connected_client() -> None:
    class FakeEvent:
        def __init__(self) -> None:
            self.disconnect_calls = 0

        def __isub__(self, handler: object) -> "FakeEvent":
            self.disconnect_calls += 1
            return self

    class FakeClient:
        def __init__(self) -> None:
            self.connected = True
            self.disconnect_calls = 0
            self.reset_calls = 0

        def isConnected(self) -> bool:
            return self.connected

        def disconnect(self) -> None:
            self.disconnect_calls += 1
            self.connected = False

        def reset(self) -> None:
            self.reset_calls += 1

    class FakeIB:
        def __init__(self) -> None:
            self.connected = True
            self.disconnect_calls = 0
            self.client = FakeClient()
            self.errorEvent = FakeEvent()
            self.disconnectedEvent = FakeEvent()
            self.timeoutEvent = FakeEvent()

        def isConnected(self) -> bool:
            return self.connected

        def disconnect(self) -> None:
            self.disconnect_calls += 1
            self.connected = False
            self.client.disconnect()

    async def run() -> None:
        manager = IBKRConnectionManager()
        fake_ib = FakeIB()
        manager._ib = fake_ib

        await manager.disconnect()

        assert manager.shutting_down is True
        assert fake_ib.disconnect_calls == 1
        assert fake_ib.connected is False
        assert fake_ib.client.disconnect_calls == 1
        assert fake_ib.client.reset_calls == 1
        assert fake_ib.errorEvent.disconnect_calls == 1
        assert fake_ib.disconnectedEvent.disconnect_calls == 1
        assert fake_ib.timeoutEvent.disconnect_calls == 1
        assert manager.ib is None

    asyncio.run(run())


def test_ibkr_disconnect_disposes_half_open_low_level_client() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.connected = True
            self.disconnect_calls = 0
            self.reset_calls = 0

        def isConnected(self) -> bool:
            return self.connected

        def disconnect(self) -> None:
            self.disconnect_calls += 1
            self.connected = False

        def reset(self) -> None:
            self.reset_calls += 1

    class FakeIB:
        def __init__(self) -> None:
            self.client = FakeClient()
            self.disconnect_calls = 0

        def isConnected(self) -> bool:
            return False

        def disconnect(self) -> None:
            self.disconnect_calls += 1

    async def run() -> None:
        manager = IBKRConnectionManager()
        fake_ib = FakeIB()
        manager._ib = fake_ib

        await manager.disconnect()

        assert fake_ib.disconnect_calls == 0
        assert fake_ib.client.disconnect_calls == 1
        assert fake_ib.client.reset_calls == 1
        assert fake_ib.client.connected is False
        assert manager.ib is None

    asyncio.run(run())
