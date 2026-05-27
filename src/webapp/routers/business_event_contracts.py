"""Business endpoints for ForecastEx / CME Event Contracts via IBKR Web API."""

from __future__ import annotations

from typing import Annotated

import httpx
from fastapi import APIRouter, Body, Depends, HTTPException, status

from src.feeds.event_contracts import (
    EventContractCategoryNode,
    EventContractHistoryRequest,
    EventContractHistoryResponse,
    EventContractInfoRequest,
    EventContractInstrument,
    EventContractMarketData,
    EventContractOrderBuildResponse,
    EventContractOrderRequest,
    EventContractOrderResponse,
    EventContractSearchRequest,
    EventContractSearchResult,
    EventContractSnapshotRequest,
    EventContractStreamingMessageRequest,
    EventContractStreamingMessages,
    EventContractStrikesRequest,
    EventContractStrikesResponse,
    event_contract_streaming_messages,
)
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.routers.orders import require_order_bearer_token

router = APIRouter(prefix="/event-contracts")


@router.get(
    "/categories",
    response_model=list[EventContractCategoryNode],
    summary="Load ForecastEx Event Contract category tree",
)
async def get_event_contract_categories(
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[EventContractCategoryNode]:
    return await _call_web_api(state.event_contracts.load_category_tree)


@router.post(
    "/discover/search",
    response_model=list[EventContractSearchResult],
    summary="Search Event Contract underliers by product code",
)
async def search_event_contracts(
    payload: EventContractSearchRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[EventContractSearchResult]:
    return await _call_web_api(state.event_contracts.search, payload)


@router.post(
    "/discover/strikes",
    response_model=EventContractStrikesResponse,
    summary="Load valid Event Contract strikes for an underlier/month",
)
async def get_event_contract_strikes(
    payload: EventContractStrikesRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> EventContractStrikesResponse:
    return await _call_web_api(state.event_contracts.strikes, payload)


@router.post(
    "/discover/info",
    response_model=list[EventContractInstrument],
    summary="Resolve tradable Event Contract instruments and conIds",
)
async def get_event_contract_info(
    payload: EventContractInfoRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[EventContractInstrument]:
    return await _call_web_api(state.event_contracts.info, payload)


@router.post(
    "/market-data/snapshot",
    response_model=list[EventContractMarketData],
    summary="Load Event Contract top-of-book snapshot fields",
)
async def get_event_contract_snapshot(
    payload: EventContractSnapshotRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[EventContractMarketData]:
    return await _call_web_api(state.event_contracts.snapshot, payload)


@router.post(
    "/market-data/history",
    response_model=EventContractHistoryResponse,
    summary="Load Event Contract historical OHLC bars",
)
async def get_event_contract_history(
    payload: EventContractHistoryRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> EventContractHistoryResponse:
    return await _call_web_api(state.event_contracts.history, payload)


@router.post(
    "/market-data/streaming-messages",
    response_model=EventContractStreamingMessages,
    summary="Build IBKR Web API websocket subscribe/unsubscribe messages",
)
async def build_event_contract_streaming_messages(
    payload: EventContractStreamingMessageRequest,
) -> EventContractStreamingMessages:
    return event_contract_streaming_messages(payload)


@router.post(
    "/orders/build",
    response_model=EventContractOrderBuildResponse,
    summary="Validate and build an Event Contract order ticket without submitting",
)
async def build_event_contract_order(
    payload: Annotated[EventContractOrderRequest, Body()],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> EventContractOrderBuildResponse:
    warnings = (
        "ForecastEx positions are reduced by buying the opposing YES/NO contract; SELL is blocked for FORECASTX.",
        "This endpoint only builds the Web API ticket; it does not submit an order.",
    )
    return EventContractOrderBuildResponse(
        account_id=payload.account_id,
        live_order_enabled=state.settings.ibkr_event_contracts_live_orders_enabled,
        ticket=payload.to_ticket(),
        warnings=warnings,
    )


@router.post(
    "/orders/place",
    response_model=EventContractOrderResponse,
    summary="Submit a guarded Event Contract order through IBKR Web API",
)
async def place_event_contract_order(
    payload: Annotated[EventContractOrderRequest, Body()],
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> EventContractOrderResponse:
    if not state.settings.ibkr_event_contracts_live_orders_enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="live Event Contract orders are disabled; set IBKR_EVENT_CONTRACTS_LIVE_ORDERS_ENABLED=true to enable",
        )
    if not payload.confirm_live_order:
        raise HTTPException(
            status_code=422,
            detail="confirm_live_order=true is required for live Event Contract order submission",
        )
    return await _call_web_api(state.event_contracts.place_order, payload)


async def _call_web_api(callable_obj, *args):
    try:
        return await callable_obj(*args)
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text or exc.response.reason_phrase
        raise HTTPException(status_code=exc.response.status_code, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=f"IBKR Web API request failed: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
