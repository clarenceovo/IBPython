from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from src.feeds.event_contracts import (
    EventContractInfoRequest,
    EventContractHistoryRequest,
    EventContractOrderRequest,
    EventContractSearchRequest,
    EventContractSide,
    EventContractSnapshotRequest,
    EventContractStreamingMessageRequest,
    EventContractStrikesRequest,
    IBKRWebAPIClient,
    event_contract_streaming_messages,
)


def test_event_contract_order_blocks_forecastex_sell() -> None:
    with pytest.raises(ValueError, match="ForecastEx event contracts cannot be sold"):
        EventContractOrderRequest(
            account_id="DU123",
            con_id=713921701,
            side=EventContractSide.SELL,
            quantity=1,
            price=0.2,
            exchange="FORECASTX",
        )


def test_event_contract_streaming_message_format() -> None:
    messages = event_contract_streaming_messages(
        EventContractStreamingMessageRequest(con_id=721095500, fields=("31", "84", "86")),
    )

    assert messages.subscribe == 'smd+721095500+{"fields":["31","84","86"]}'
    assert messages.unsubscribe == "umd+721095500+{}"


def test_web_api_client_maps_event_contract_workflow() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/v1/api/trsrv/event/category-tree":
            return _json_response(
                {
                    "g17654": {
                        "label": "United States",
                        "parentId": "g17574",
                        "markets": [{"name": "US Fed Funds Target Rate", "symbol": "FF", "exchange": "FORECASTX", "conid": 658663572}],
                    }
                }
            )
        if request.url.path == "/v1/api/iserver/secdef/search":
            assert request.url.params["symbol"] == "FF"
            return _json_response(
                [
                    {
                        "conid": "658663572",
                        "symbol": "FF",
                        "description": "FORECASTX",
                        "companyName": "US Fed Funds Target Rate",
                        "opt": "20260616;20260728",
                    }
                ]
            )
        if request.url.path == "/v1/api/iserver/secdef/strikes":
            assert request.url.params["conid"] == "658663572"
            return _json_response({"call": [4.875, 5.125], "put": [4.875, 5.125]})
        if request.url.path == "/v1/api/iserver/secdef/info":
            assert request.url.params["strike"] == "4.875"
            return _json_response(
                [
                    {
                        "conid": 713921696,
                        "symbol": "FF",
                        "secType": "OPT",
                        "exchange": "FORECASTX",
                        "right": "C",
                        "strike": 4.875,
                        "currency": "USD",
                        "tradingClass": "FF",
                    }
                ]
            )
        if request.url.path == "/v1/api/iserver/marketdata/snapshot":
            assert request.url.params["conids"] == "713921696"
            return _json_response([{"conid": 713921696, "31": "0.81", "84": "0.79", "86": "0.82", "_updated": 1780000000000}])
        if request.url.path == "/v1/api/iserver/marketdata/history":
            return _json_response(
                {
                    "symbol": "FF",
                    "timePeriod": "2d",
                    "barLength": 3600,
                    "data": [{"t": 1780000000000, "o": 0.2, "h": 0.2, "l": 0.19, "c": 0.2, "v": 0}],
                }
            )
        if request.url.path == "/v1/api/iserver/account/DU123/orders":
            assert json.loads(request.content.decode("utf-8")) == [
                {"conid": 713921696, "side": "BUY", "orderType": "LMT", "quantity": 1.0, "tif": "DAY", "price": 0.81}
            ]
            return _json_response({"order_id": "987654", "order_status": "Submitted"})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    async def run() -> None:
        client = IBKRWebAPIClient(base_url="https://localhost:5000/v1/api", transport=httpx.MockTransport(handler))

        categories = await client.load_category_tree()
        assert categories[0].markets[0].con_id == 658663572

        search = await client.search(EventContractSearchRequest(symbol="ff"))
        assert search[0].opt_expirations == ("20260616", "20260728")

        strikes = await client.strikes(EventContractStrikesRequest(underlying_con_id=658663572, month="SEP24"))
        assert strikes.all_strikes == (4.875, 5.125)

        info = await client.info(EventContractInfoRequest(underlying_con_id=658663572, month="SEP24", strike=4.875, right="C"))
        assert info[0].yes_no == "YES"

        snapshot = await client.snapshot(EventContractSnapshotRequest(con_ids=(713921696,)))
        assert snapshot[0].last == 0.81

        history = await client.history(EventContractHistoryRequest(con_id=713921696))
        assert history.bars[0].close == 0.2

        order = await client.place_order(
            EventContractOrderRequest(
                account_id="DU123",
                con_id=713921696,
                quantity=1,
                price=0.81,
                confirm_live_order=True,
            )
        )
        assert order.response["order_status"] == "Submitted"

    asyncio.run(run())
    assert len(requests) == 7


def _json_response(payload: object) -> httpx.Response:
    return httpx.Response(200, json=payload)
