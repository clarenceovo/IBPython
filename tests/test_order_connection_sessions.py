"""Order mutations must never cross a disconnected IB session."""

from __future__ import annotations

import asyncio
from unittest.mock import Mock

import pytest

from src.feeds.exceptions import IBKRConnectionError
from src.feeds.ibkr_connection import IBKRConnectionManager
from src.feeds.ibkr_order_client import IBKROrderClient
from src.feeds.orders import (
    BracketOrderRequest, ModifyOrderRequest, OcaGroupRequest, OrderEnvelope, OrderStatus, PlaceOrderRequest,
)
from test_orders import FakeIB, FakeOrder, FakeOrderRedis, FakeTrade


def order_request(**kwargs):
    return PlaceOrderRequest(symbol="AAPL", action="BUY", order_type="LMT", quantity=10, price=150, **kwargs)


def bracket_request():
    return BracketOrderRequest(symbol="AAPL", quantity=10, limit_price=150, take_profit_price=160, stop_loss_price=140)


def oca_request():
    return OcaGroupRequest(orders=[
        {"symbol": symbol, "action": "BUY", "quantity": 10, "order_type": "LMT", "price": 150}
        for symbol in ("AAPL", "MSFT")
    ])


def make_client():
    original, replacement = FakeIB(), FakeIB()
    original.reqGlobalCancel = Mock()
    replacement.reqGlobalCancel = Mock()
    original._open_trades.append(FakeTrade(order=FakeOrder(orderId=42)))
    connection = IBKRConnectionManager()
    connection._ib = original
    redis = FakeOrderRedis()
    return IBKROrderClient(connection, redis), connection, original, replacement, redis


def change_session(connection, replacement, *, resume):
    connection._end_session()
    connection.connection_enabled = False
    connection._ib = None
    if resume:
        connection.connection_enabled = True
        connection._ib = replacement


async def envelopes(redis):
    return [OrderEnvelope.model_validate_json(await redis.get_raw(key)) for key in await redis.scan_order_envelopes()]


@pytest.mark.parametrize("resume", [False, True], ids=["disabled", "replacement"])
@pytest.mark.parametrize("action,operation", [
    ("place", "place_order:AAPL"),
    ("cancel", "cancel_order"),
    ("modify", "modify_order"),
    ("preview", "preview_order:place:AAPL"),
    ("bracket", "place_bracket_order:AAPL:0"),
    ("oca", "place_oca_order:AAPL"),
    ("exercise", "exercise_option:AAPL"),
    ("global", "global_cancel"),
])
def test_paced_mutation_is_rejected_after_session_changes(action, operation, resume):
    async def run():
        client, connection, original, replacement, redis = make_client()
        entered, release = asyncio.Event(), asyncio.Event()

        async def pace(*, operation: str, weight=1):
            if operation == target:
                entered.set()
                await release.wait()

        target = operation
        connection.wait_for_ibkr_request = pace
        calls = {
            "place": lambda: client.place_order(order_request()),
            "cancel": lambda: client.cancel_order("DU123", 42),
            "modify": lambda: client.modify_order("DU123", 42, ModifyOrderRequest(price=155)),
            "preview": lambda: client.preview_order(order_request()),
            "bracket": lambda: client.place_bracket_order(bracket_request()),
            "oca": lambda: client.place_oca_group(oca_request()),
            "exercise": lambda: client.exercise_option("AAPL", "C", 150, "20261218", 1, 1, "DU123"),
            "global": client.cancel_all_orders,
        }
        pending = asyncio.create_task(calls[action]())
        await asyncio.wait_for(entered.wait(), 1)
        change_session(connection, replacement, resume=resume)
        release.set()
        with pytest.raises(IBKRConnectionError):
            await pending
        for ib in (original, replacement):
            assert ib._placed_orders == []
            assert ib._what_if_orders == []
            assert ib._cancelled_orders == []
            assert ib._exercised_options == []
            ib.reqGlobalCancel.assert_not_called()
        assert original._open_trades[0].order.lmtPrice == 150

    asyncio.run(run())


@pytest.mark.parametrize("kind", ["bracket", "oca"])
@pytest.mark.parametrize("cancel_request", [False, True], ids=["replacement", "request_cancelled"])
def test_interrupted_group_preserves_submitted_leg_and_does_not_replay(kind, cancel_request):
    async def run():
        client, connection, original, replacement, redis = make_client()
        entered, release = asyncio.Event(), asyncio.Event()
        target = "place_bracket_order:AAPL:1" if kind == "bracket" else "place_oca_order:MSFT"

        async def pace(*, operation, weight=1):
            if operation == target:
                entered.set()
                await release.wait()

        connection.wait_for_ibkr_request = pace
        call = client.place_bracket_order(bracket_request()) if kind == "bracket" else client.place_oca_group(oca_request())
        pending = asyncio.create_task(call)
        await asyncio.wait_for(entered.wait(), 1)
        assert len(original._placed_orders) == 1
        if cancel_request:
            pending.cancel()
            expected = asyncio.CancelledError
        else:
            change_session(connection, replacement, resume=True)
            release.set()
            expected = IBKRConnectionError
        with pytest.raises(expected):
            await pending
        assert len(original._placed_orders) == 1
        assert replacement._placed_orders == []
        envelope, = await envelopes(redis)
        assert envelope.status == OrderStatus.PENDING
        assert envelope.ibkr_order_id == 1001
        assert envelope.metadata["reconciliation_required"] is True
        assert envelope.metadata["submitted_order_ids"] == [1001]
        assert envelope.metadata["last_known_orders"] == [{"order_id": 1001, "status": "submitted"}]
        assert envelope.response["orders"]

    asyncio.run(run())


def test_interrupted_ack_preserves_order_id_and_idempotency_reservation():
    async def run():
        client, connection, original, replacement, redis = make_client()
        entered, release = asyncio.Event(), asyncio.Event()

        async def acknowledgement(trade, **kwargs):
            entered.set()
            await release.wait()
            return OrderStatus.SUBMITTED

        client._wait_for_order_status = acknowledgement
        request = order_request(idempotency_key="once-only")
        pending = asyncio.create_task(client.place_order(request))
        await asyncio.wait_for(entered.wait(), 1)
        change_session(connection, replacement, resume=True)
        release.set()
        with pytest.raises(IBKRConnectionError):
            await pending
        envelope, = await envelopes(redis)
        assert envelope.status == OrderStatus.SUBMITTED
        assert envelope.ibkr_order_id == 1001
        assert envelope.metadata["reconciliation_required"] is True
        response = await client.place_order(request)
        assert response.order_id == 1001
        assert len(original._placed_orders) == 1
        assert replacement._placed_orders == []

    asyncio.run(run())


def test_preview_cleanup_never_cancels_order_on_replacement():
    async def run():
        client, connection, original, replacement, _ = make_client()

        async def pace(*, operation, weight=1):
            if operation == "preview_order:cancel:AAPL":
                change_session(connection, replacement, resume=True)

        connection.wait_for_ibkr_request = pace
        await client.preview_order(order_request())
        assert len(original._what_if_orders) == 1
        assert original._cancelled_orders == []
        assert replacement._cancelled_orders == []
        envelope, = await envelopes(redis)
        assert envelope.metadata["reconciliation_required"] is True

    asyncio.run(run())


def test_redis_cache_wait_cannot_move_submission_to_replacement():
    async def run():
        client, connection, original, replacement, redis = make_client()
        cache = redis.cache_order_envelope

        async def cache_and_replace(value, *, ttl=None):
            key = await cache(value, ttl=ttl)
            change_session(connection, replacement, resume=True)
            return key

        redis.cache_order_envelope = cache_and_replace
        with pytest.raises(IBKRConnectionError):
            await client.place_order(order_request())
        assert not original._placed_orders and not replacement._placed_orders
        envelope, = await envelopes(redis)
        assert envelope.metadata["reconciliation_required"] is False

    asyncio.run(run())
