"""Agent-friendliness tests for the MCP server.

Covers the tool annotations, the shared error envelope, destructive-tool
confirmation, payload caps, and the typed order pathway.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import src.mcp_server as server
from src.config.settings import Settings
from src.feeds.exceptions import IBKRConnectionError, IBKRPacingError
from src.feeds.orders import BracketOrderRequest, BracketOrderResponse


def _ctx(**flags: bool) -> SimpleNamespace:
    """Fake MCP Context: request_context.lifespan_context is a plain dict."""
    request_context = SimpleNamespace(lifespan_context=flags)
    return SimpleNamespace(request_context=request_context)


def _state(feed: object, orders_enabled: bool = False) -> SimpleNamespace:
    return SimpleNamespace(settings=Settings(ibkr_orders_enabled=orders_enabled), feed=feed)


# ── annotations ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_every_tool_has_annotations() -> None:
    tools = await server.mcp.list_tools()
    assert tools, "no tools registered"
    by_name = {t.name: t for t in tools}
    assert by_name["cancel_all_orders"].annotations is not None
    assert by_name["cancel_all_orders"].annotations.destructiveHint is True
    assert by_name["query_historical_ohlcv"].annotations is not None
    assert by_name["query_historical_ohlcv"].annotations.readOnlyHint is True
    assert by_name["place_bracket_order"].annotations is not None
    assert by_name["place_bracket_order"].annotations.readOnlyHint is False
    for tool in tools:
        assert tool.annotations is not None, f"tool {tool.name} lacks annotations"


# ── error envelope ────────────────────────────────────────────────────────────


def test_tool_error_classifies_common_exceptions() -> None:
    result = server._tool_error(ValueError("bad input"))
    assert result == {"error": "bad input", "code": "BAD_REQUEST", "retryable": False}

    result = server._tool_error(IBKRConnectionError("socket closed"))
    assert result["code"] == "IBKR_UNAVAILABLE"
    assert result["retryable"] is True

    result = server._tool_error(IBKRPacingError("pacing violated"))
    assert result["code"] == "IBKR_PACING"
    assert result["retryable"] is True

    result = server._tool_error(RuntimeError("anything"))
    assert result["code"] == "TOOL_ERROR"
    assert result["retryable"] is False


def test_require_helpers_return_coded_errors() -> None:
    err = server._require_questdb(_ctx(questdb_connected=False))
    assert err == {"error": "QuestDB is not available", "code": "QUESTDB_UNAVAILABLE", "retryable": True}
    err = server._require_redis(_ctx(redis_connected=False))
    assert err["code"] == "REDIS_UNAVAILABLE"
    assert server._require_questdb(_ctx(questdb_connected=True)) is None


def test_load_fundamentals_returns_feature_removed() -> None:
    result = asyncio.run(server.load_fundamentals(_ctx(), "AAPL"))
    assert result["code"] == "FEATURE_REMOVED"
    assert "10.47" in result["error"]


# ── destructive-tool confirmation ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cancel_all_orders_requires_confirm(monkeypatch) -> None:
    feed = SimpleNamespace(cancel_all_orders=AsyncMock(return_value={"cancelled": 3}))
    monkeypatch.setattr(server, "_state", lambda _: _state(feed, orders_enabled=True))

    blocked = await server.cancel_all_orders(_ctx())
    assert blocked["code"] == "CONFIRMATION_REQUIRED"
    feed.cancel_all_orders.assert_not_called()

    result = await server.cancel_all_orders(_ctx(), confirm=True)
    assert result == {"cancelled": 3}
    feed.cancel_all_orders.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancel_all_orders_disabled_beats_confirm(monkeypatch) -> None:
    feed = SimpleNamespace(cancel_all_orders=AsyncMock())
    monkeypatch.setattr(server, "_state", lambda _: _state(feed, orders_enabled=False))
    result = await server.cancel_all_orders(_ctx(), confirm=True)
    assert result["code"] == "ORDERS_DISABLED"
    feed.cancel_all_orders.assert_not_called()


# ── payload caps ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_redis_key_truncates_long_values(monkeypatch) -> None:
    class _Redis:
        async def get_raw(self, _key: str) -> str:
            return "x" * 30_000

    monkeypatch.setattr(server, "_state", lambda _: SimpleNamespace(redis=_Redis()))
    result = await server.get_redis_key(_ctx(redis_connected=True), "OHLCV:big")
    assert len(result["value"]) == 20_000
    assert result["truncated"] is True


@pytest.mark.asyncio
async def test_query_raw_sql_caps_rows(monkeypatch) -> None:
    class _QuestDB:
        async def fetch_dicts(self, _sql: str) -> list[dict[str, int]]:
            return [{"n": i} for i in range(10)]

    loader = SimpleNamespace(questdb=_QuestDB())
    monkeypatch.setattr(server, "_state", lambda _: SimpleNamespace(loader=loader))
    rows = await server.query_raw_sql(_ctx(questdb_connected=True), "SELECT n FROM t", max_rows=3)
    assert rows == [{"n": 0}, {"n": 1}, {"n": 2}]


@pytest.mark.asyncio
async def test_get_all_open_orders_slices(monkeypatch) -> None:
    feed = SimpleNamespace(get_all_open_orders=AsyncMock(return_value=[{"id": i} for i in range(10)]))
    monkeypatch.setattr(server, "_state", lambda _: _state(feed))
    orders = await server.get_all_open_orders(_ctx(), limit=4)
    assert orders == [{"id": 0}, {"id": 1}, {"id": 2}, {"id": 3}]


# ── typed order pathway ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_place_bracket_order_uses_typed_client(monkeypatch) -> None:
    response = BracketOrderResponse(
        symbol="AAPL",
        action="BUY",
        quantity=1.0,
        orders_placed=3,
        parent_order_id=101,
        take_profit_order_id=102,
        stop_loss_order_id=103,
    )
    feed = SimpleNamespace(place_bracket_order=AsyncMock(return_value=response))
    monkeypatch.setattr(server, "_state", lambda _: _state(feed, orders_enabled=True))

    result = await server.place_bracket_order(
        _ctx(),
        symbol="aapl",
        action="BUY",
        quantity=1.0,
        limit_price=190.0,
        take_profit_price=200.0,
        stop_loss_price=180.0,
    )
    request = feed.place_bracket_order.call_args.args[0]
    assert isinstance(request, BracketOrderRequest)
    assert request.symbol == "AAPL"
    assert result["orders_placed"] == 3
    assert result["parent_order_id"] == 101


@pytest.mark.asyncio
async def test_place_bracket_order_rejects_missing_prices(monkeypatch) -> None:
    feed = SimpleNamespace(place_bracket_order=AsyncMock())
    monkeypatch.setattr(server, "_state", lambda _: _state(feed, orders_enabled=True))

    result = await server.place_bracket_order(_ctx(), symbol="AAPL", action="BUY", quantity=1.0)
    assert result["code"] == "BAD_REQUEST"
    assert result["retryable"] is False
    feed.place_bracket_order.assert_not_called()
