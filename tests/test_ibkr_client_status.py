"""Fleet status uses Redis expiry, never wall-clock comparisons between hosts."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.transport.redis_client import MarketDataRedisClient
from src.webapp.dependencies import get_rest_state
from src.webapp.routers import system


def report(instance_id="local", **changes):
    return {
        "enabled": False, "desired_enabled": False, "connection_status": "disabled",
        "socket_connected": False, "lifecycle_state": "disabled", "generation": 4,
        "redis_key": "IBKR:ConnectionControl", "client_id": 101,
        "instance_id": instance_id, "role": "api", "last_checked_at": "2026-10-09T00:00:00Z",
        "last_error": None, "updated_at": "2026-10-09T00:00:00Z", "poll_seconds": 1.0,
        "ttl_seconds": 60, **changes,
    }


class RedisHeartbeats:
    def __init__(self, records):
        self.records = records
        self.pattern = None

    async def scan_iter(self, *, match, count):
        self.pattern = match
        for key in [*self.records, next(iter(self.records))]:
            yield key.encode()

    def pipeline(self, *, transaction):
        assert transaction is True
        records = self.records

        class Pipeline:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            def get(self, key):
                self.key = key.decode()
                return self

            def pttl(self, key):
                assert key.decode() == self.key
                return self

            async def execute(self):
                return records[self.key]

        return Pipeline()


def test_redis_heartbeat_age_ignores_clock_skew_and_marks_stale_unknown():
    control_key = "IBKR:[local]Control"
    prefix = f"{control_key}:clients:"
    records = {
        prefix + "fresh": (json.dumps(report("fresh", updated_at="1970-01-01T00:00:00Z")), 58_000),
        prefix + "stale": (json.dumps(report("stale", updated_at="2100-01-01T00:00:00Z", socket_connected=True, connection_status="connected")), 54_000),
        prefix + "no-expiry": (json.dumps(report("no-expiry")), -1),
        prefix + "expired": (None, -2),
        prefix + "broken": ("{", 59_000),
    }
    redis = RedisHeartbeats(records)
    statuses = asyncio.run(MarketDataRedisClient(client=redis).list_ibkr_client_status(control_key))
    clients = {status["instance_id"]: status for status in statuses}
    assert len(statuses) == 4  # SCAN duplicates are harmless; expired entries vanish.
    assert redis.pattern == r"IBKR:\[local\]Control:clients:*"
    assert clients["fresh"]["freshness"] == "fresh"
    assert clients["fresh"]["heartbeat_age_seconds"] == 2
    assert clients["stale"]["freshness"] == "stale"
    assert clients["stale"]["connection_status"] == "unknown"
    assert clients["stale"]["socket_connected"] is None
    assert clients["stale"]["heartbeat_age_seconds"] == 6
    assert clients["no-expiry"]["freshness"] == "stale"
    assert clients["no-expiry"]["heartbeat_age_seconds"] is None
    assert clients["broken"]["freshness"] == "unknown"


def connection_manager(records=()):
    current = report()
    redis = SimpleNamespace(list_ibkr_client_status=AsyncMock(return_value=list(records)))
    manager = SimpleNamespace(
        _control_redis=redis, connection_control_key=current["redis_key"],
        connection_control_error=None, refresh_connection_control=AsyncMock(),
        control_status=lambda: dict(current),
    )

    async def write(switch):
        current.update(enabled=switch.enabled, desired_enabled=switch.enabled)

    manager.write_connection_control = AsyncMock(side_effect=write)
    return manager, current


def api(manager):
    app = FastAPI()
    app.include_router(system.router, prefix="/api/v1")
    app.dependency_overrides[get_rest_state] = lambda: SimpleNamespace(feed=SimpleNamespace(_connection=manager))
    return TestClient(app)


PATH = "/api/v1/system/ibkr/connection"


def test_connection_status_preserves_local_fields_and_does_not_claim_remote_ack():
    remote = report("scheduler", role="scheduler", client_id=201, enabled=True,
                    desired_enabled=True, socket_connected=True, connection_status="connected", lifecycle_state="connected")
    manager, current = connection_manager([report("local", connection_status="unknown"), remote])
    with api(manager) as client:
        response = client.put(PATH, json={"enabled": False})
        assert response.status_code == 200
        data = response.json()
        for key in ("enabled", "connection_status", "redis_key", "client_id"):
            assert data[key] == current[key]
        assert data["clients_status"] == "available"
        assert len(data["clients"]) == 2
        assert data["clients"][0]["connection_status"] == "disabled"
        assert data["clients"][1]["socket_connected"] is True
        assert data["clients"][1]["desired_enabled"] is True
        assert client.put(PATH, json={"enabled": "false"}).status_code == 422


def test_unreadable_switch_returns_structured_disabled_state_even_when_fleet_is_down():
    manager, current = connection_manager()
    manager.connection_control_error = "ConnectionError"
    current.update(desired_enabled=None, last_error="ConnectionError")
    manager._control_redis.list_ibkr_client_status.side_effect = ConnectionError("private address")
    with api(manager) as client:
        response = client.get(PATH)
        assert response.status_code == 503
        detail = response.json()["detail"]
        assert "unreadable" in detail["message"]
        data = detail["connection"]
        assert data["desired_enabled"] is None and data["enabled"] is False
        assert data["clients_status"] == "unavailable"
        assert len(data["clients"]) == 1
        assert "private address" not in response.text


def test_slow_fleet_listing_is_bounded_and_local_status_remains_available(monkeypatch):
    manager, _ = connection_manager()

    async def wait_forever(key):
        await asyncio.Event().wait()

    manager._control_redis.list_ibkr_client_status.side_effect = wait_forever
    monkeypatch.setattr(system, "_READINESS_CHECK_TIMEOUT_SECONDS", .01)
    with api(manager) as client:
        response = client.get(PATH)
        assert response.status_code == 200
        data = response.json()
        assert data["clients_status"] == "unavailable"
        assert "TimeoutError" in data["clients_error"]
        assert data["clients"][0]["instance_id"] == "local"


def test_failed_control_write_reports_local_state_without_persistence_claim():
    manager, _ = connection_manager()
    manager.write_connection_control.side_effect = ConnectionError("down")
    with api(manager) as client:
        response = client.put(PATH, json={"enabled": True})
        assert response.status_code == 503
        detail = response.json()["detail"]
        assert "Could not persist" in detail["message"]
        assert detail["connection"]["enabled"] is False


def test_clients_without_listing_are_explicitly_local_only():
    manager, _ = connection_manager()
    manager._control_redis = SimpleNamespace()
    with api(manager) as client:
        data = client.get(PATH).json()
        assert data["clients_status"] == "unavailable"
        assert [item["instance_id"] for item in data["clients"]] == ["local"]
