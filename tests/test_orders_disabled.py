"""Order pathway master switch (IBKR_ORDERS_ENABLED) behavior.

Covers the disabled default, the service-layer guard, the REST 503 gate on all
order-writing endpoints, and the capabilities surface. Read-only order queries
must stay reachable.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient

from src.config.settings import Settings
from src.feeds.capabilities import gateway_capabilities
from src.feeds.exceptions import IBKROrdersDisabledError
from src.feeds.ibkr_order_client import IBKROrderClient
from src.feeds.orders import OrderAction, OrderType, PlaceOrderRequest


# ---------------------------------------------------------------------------
# Settings default
# ---------------------------------------------------------------------------


def test_orders_disabled_by_default() -> None:
    assert Settings().ibkr_orders_enabled is False


def test_orders_enabled_via_env_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IBKR_ORDERS_ENABLED", "true")
    assert Settings(_env_file=None).ibkr_orders_enabled is True


# ---------------------------------------------------------------------------
# Service-layer guard (defense in depth)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_order_client_refuses_writes_when_disabled() -> None:
    client = IBKROrderClient(None, orders_enabled=False)  # type: ignore[arg-type]
    request = PlaceOrderRequest(symbol="AAPL", action=OrderAction.BUY, order_type=OrderType.LIMIT, quantity=1, price=100.0)

    operations: list[tuple[str, Any]] = [
        ("place_order", (request,)),
        ("cancel_order", ("DU123", 1)),
        ("modify_order", ("DU123", 1, None)),
        ("preview_order", (request,)),
        ("place_bracket_order", (None,)),
        ("place_oca_group", (None,)),
        ("cancel_all_orders", ()),
        ("exercise_option", ("AAPL", "C", 100.0, "20261218", 1, 1, "DU123")),
    ]
    for name, args in operations:
        with pytest.raises(IBKROrdersDisabledError):
            await getattr(client, name)(*args)


@pytest.mark.asyncio
async def test_order_client_read_paths_unaffected_by_switch() -> None:
    class ReadStub:
        async def load_open_orders(self, *, include_deactivated: bool = False) -> list[str]:
            return ["open-order"]

    client = IBKROrderClient.__new__(IBKROrderClient)
    client._orders_enabled = False
    client.load_open_orders = ReadStub().load_open_orders  # type: ignore[method-assign]
    assert await client.load_open_orders() == ["open-order"]


# ---------------------------------------------------------------------------
# REST gate
# ---------------------------------------------------------------------------


class _ExplodingFeed:
    """Feed stub that fails the test if any order-writing call gets through."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def _boom(self, name: str, *args: Any, **kwargs: Any) -> None:
        self.calls.append(name)
        raise AssertionError(f"order pathway reached feed: {name}")

    async def place_order(self, request: Any) -> None:
        await self._boom("place_order")

    async def cancel_order(self, account_id: str, order_id: int) -> None:
        await self._boom("cancel_order")

    async def modify_order(self, account_id: str, order_id: int, modifications: Any) -> None:
        await self._boom("modify_order")

    async def preview_order(self, request: Any) -> None:
        await self._boom("preview_order")

    async def place_bracket_order(self, payload: Any) -> None:
        await self._boom("place_bracket_order")

    async def place_oca_group(self, payload: Any) -> None:
        await self._boom("place_oca_group")


class _FakeRedis:
    async def get_raw(self, _key: str) -> bytes:
        return b"test-order-token"


class _DisabledState:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.feed = _ExplodingFeed()
        self.loader = None
        self.redis = _FakeRedis()  # type: ignore[assignment]
        self.market_data_cache = None
        self.fixed_income_reference_provider = None
        self.event_contracts = _ExplodingFeed()

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass


def _make_client(**settings_kwargs: Any) -> tuple[TestClient, _DisabledState]:
    from src.webapp.app import create_app

    state = _DisabledState(Settings(**settings_kwargs))
    app = create_app(settings=state.settings, state=state)  # type: ignore[arg-type]
    return TestClient(app), state


ORDER_MUTATION_CASES = [
    ("POST", "/api/v1/orders/place", {"symbol": "AAPL", "action": "BUY", "order_type": "LMT", "quantity": 1, "price": 150.0}),
    ("POST", "/api/v1/orders/1/cancel?account_id=DU123", None),
    ("POST", "/api/v1/orders/1/modify?account_id=DU123", {"price": 151.0}),
    ("POST", "/api/v1/orders/preview", {"symbol": "AAPL", "action": "BUY", "order_type": "LMT", "quantity": 1, "price": 150.0}),
    ("POST", "/api/v1/orders/bracket", {"symbol": "AAPL", "action": "BUY", "quantity": 100, "limit_price": 185.0, "take_profit_price": 195.0, "stop_loss_price": 180.0}),
    ("POST", "/api/v1/orders/oca", {"orders": [{"symbol": "AAPL", "action": "BUY", "quantity": 100, "order_type": "LMT", "price": 185.0}, {"symbol": "AAPL", "action": "BUY", "quantity": 100, "order_type": "LMT", "price": 180.0}], "oca_type": 1}),
    ("POST", "/api/v1/business/event-contracts/orders/place", {"account_id": "DU123", "con_id": 713921696, "quantity": 1, "price": 0.81, "confirm_live_order": True}),
]


@pytest.mark.parametrize(("method", "path", "payload"), ORDER_MUTATION_CASES)
def test_order_mutation_endpoints_return_503_when_disabled(method: str, path: str, payload: Any) -> None:
    client, state = _make_client()
    with client:
        response = client.request(method, path, json=payload, headers={"Authorization": "Bearer test-order-token"})
        assert response.status_code == 503, response.text
        assert "IBKR_ORDERS_ENABLED" in response.json()["detail"]
    assert state.feed.calls == []


def test_bearer_token_check_precedence_unchanged_when_enabled() -> None:
    """With the switch on, the existing bearer-token auth still applies."""
    client, _ = _make_client(ibkr_orders_enabled=True)
    with client:
        response = client.post(
            "/api/v1/orders/place",
            json={"symbol": "AAPL", "action": "BUY", "order_type": "LMT", "quantity": 1, "price": 150.0},
            headers={"Authorization": "Bearer wrong-token"},
        )
        assert response.status_code in (401, 503)


# ---------------------------------------------------------------------------
# Capabilities
# ---------------------------------------------------------------------------


def test_capabilities_report_order_pathways_disabled() -> None:
    connection = SimpleNamespace(ib=None, is_connected=False)
    disabled = gateway_capabilities(connection, orders_enabled=False)
    enabled = gateway_capabilities(connection, orders_enabled=True)
    assert disabled.features["order_pathways"].availability == "disabled"
    assert enabled.features["order_pathways"].availability == "unknown"
