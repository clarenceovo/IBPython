"""Agent-facing contract tests: error envelope and OpenAPI agent pass."""

from __future__ import annotations

import json

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from src.config.settings import Settings
from src.feeds.exceptions import (
    IBKRConnectionDisabledError,
    IBKRConnectionError,
    IBKRContractResolutionError,
    IBKROrdersDisabledError,
    IBKRPacingError,
    IBKRUnsupportedFeatureError,
)
from src.webapp.api_error import ApiError, classify_error, error_response, http_exception_response
from src.webapp.app import create_app
from src.webapp.openapi_agent import derive_operation_id

app = create_app(settings=Settings(ibkr_rest_app_name="AgentApiTest"), state=None)


@app.get("/api/v1/_probe/connection", include_in_schema=False)
async def _probe_connection() -> dict:
    raise IBKRConnectionError("IBKR connection unavailable: host=127.0.0.1 port=4001")


@app.get("/api/v1/_probe/disabled", include_in_schema=False)
async def _probe_disabled() -> dict:
    raise IBKRConnectionDisabledError("IBKR connections are disabled by the shared Redis switch")


@app.get("/api/v1/_probe/pacing", include_in_schema=False)
async def _probe_pacing() -> dict:
    raise IBKRPacingError("Historical data pacing violated: cooldown required")


@app.get("/api/v1/_probe/contract", include_in_schema=False)
async def _probe_contract() -> dict:
    raise IBKRContractResolutionError("Contract could not be qualified: ESZ26")


@app.get("/api/v1/_probe/feature-removed", include_in_schema=False)
async def _probe_feature_removed() -> dict:
    raise IBKRUnsupportedFeatureError("Fundamental reports were removed upstream")


@app.get("/api/v1/_probe/orders-disabled", include_in_schema=False)
async def _probe_orders_disabled() -> dict:
    raise IBKROrdersDisabledError("Order pathways are disabled")


@app.get("/api/v1/_probe/http-429", include_in_schema=False)
async def _probe_http_429() -> dict:
    raise HTTPException(status_code=429, detail="Historical data pacing violated: slow down")


@app.get("/api/v1/_probe/http-503", include_in_schema=False)
async def _probe_http_503() -> dict:
    raise HTTPException(status_code=503, detail="IBKR not available: reconnect in progress")


@app.get("/api/v1/_probe/unhandled", include_in_schema=False)
async def _probe_unhandled() -> dict:
    raise ValueError("boom")


client = TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize(
    ("path", "status", "code", "retryable"),
    [
        ("/api/v1/_probe/connection", 503, "IBKR_UNAVAILABLE", True),
        ("/api/v1/_probe/disabled", 503, "IBKR_DISABLED", True),
        ("/api/v1/_probe/pacing", 429, "IBKR_PACING", True),
        ("/api/v1/_probe/contract", 422, "CONTRACT_RESOLUTION_FAILED", False),
        ("/api/v1/_probe/feature-removed", 410, "FEATURE_REMOVED", False),
        ("/api/v1/_probe/orders-disabled", 403, "ORDERS_DISABLED", False),
        ("/api/v1/_probe/http-429", 429, "IBKR_PACING", True),
        ("/api/v1/_probe/http-503", 503, "IBKR_UNAVAILABLE", True),
        ("/api/v1/does-not-exist", 404, "NOT_FOUND", False),
    ],
)
def test_error_envelope(path: str, status: int, code: str, retryable: bool) -> None:
    response = client.get(path)

    assert response.status_code == status
    assert response.headers["content-type"].startswith("application/json")
    body = response.json()
    assert body["code"] == code
    assert body["retryable"] is retryable
    assert isinstance(body["detail"], str) and body["detail"]
    if status == 429:
        assert body["retry_after_seconds"] >= 1
        assert int(response.headers["Retry-After"]) >= 1


def test_unhandled_exception_returns_json_500() -> None:
    response = client.get("/api/v1/_probe/unhandled")

    assert response.status_code == 500
    assert response.json() == {
        "detail": "Internal error",
        "code": "INTERNAL_ERROR",
        "retryable": False,
    }


def test_pacing_response_matches_api_error_model() -> None:
    body = client.get("/api/v1/_probe/pacing").json()

    parsed = ApiError.model_validate(body)
    assert parsed.code == "IBKR_PACING"
    assert parsed.retryable is True


def test_classify_error_table() -> None:
    assert classify_error(429, "Historical data pacing violated") == "IBKR_PACING"
    assert classify_error(429, "too many requests") == "RATE_LIMITED"
    assert classify_error(503, "QuestDB connection failed") == "QUESTDB_UNAVAILABLE"
    assert classify_error(503, "Redis is not available") == "REDIS_UNAVAILABLE"
    assert classify_error(503, "IBKR connection unavailable") == "IBKR_UNAVAILABLE"
    assert classify_error(503, "something else") == "SERVICE_UNAVAILABLE"
    assert classify_error(422, "Contract could not be resolved") == "CONTRACT_RESOLUTION_FAILED"
    assert classify_error(422, "bad payload shape") == "UNPROCESSABLE_ENTITY"
    assert classify_error(403, "order pathways are disabled") == "ORDERS_DISABLED"
    assert classify_error(401, "token required") == "UNAUTHORIZED"
    assert classify_error(599, "unknown") == "HTTP_599"


def test_error_response_retry_header_only_for_429() -> None:
    pacing = error_response(429, "slow down", code="IBKR_PACING", retryable=True, retry_after_seconds=30)
    unavailable = error_response(503, "IBKR connection unavailable", code="IBKR_UNAVAILABLE")

    assert pacing.headers["retry-after"] == "30"
    assert "retry-after" not in (unavailable.headers or {})
    assert unavailable.status_code == 503


def test_http_exception_response_keeps_route_headers() -> None:
    exc = HTTPException(
        status_code=429,
        detail="pacing",
        headers={"Retry-After": "17", "X-Custom": "yes"},
    )

    response = http_exception_response(exc)

    assert response.status_code == 429
    assert response.headers["retry-after"] == "17"
    assert response.headers["x-custom"] == "yes"
    assert json.loads(response.body)["retry_after_seconds"] == 17


def test_derive_operation_id() -> None:
    assert derive_operation_id("/api/v1/account/summary", "get") == "accountSummaryGet"
    assert derive_operation_id("/api/v1/market-data/consolidated/resolve/{symbol}", "get") == (
        "marketDataConsolidatedResolveBySymbol"
    )
    assert derive_operation_id("/api/v1/orders/place", "post") == "ordersPlace"
    assert derive_operation_id("/api/v1/orders/{order_id}/cancel", "post") == "ordersByOrderIdCancel"
    assert derive_operation_id("/api/v1/market-data/options/iv-series", "post") == "marketDataOptionsIvSeriesPost"
    assert derive_operation_id("/api/v1/streaming/subscriptions", "delete") == "streamingSubscriptionsDelete"


def _operations(spec: dict) -> list[tuple[str, str, dict]]:
    operations = []
    for path, item in spec["paths"].items():
        for method, operation in item.items():
            if method in {"get", "post", "put", "delete", "patch"}:
                operations.append((path, method, operation))
    return operations


def test_openapi_agent_pass_invariants() -> None:
    spec = app.openapi()
    operations = _operations(spec)

    assert spec["x-agent-enhanced"] is True
    assert len(operations) >= 140

    # Every operation: unique, non-auto operationId, default error response,
    # side-effect annotation.
    ids = [operation["operationId"] for _, _, operation in operations]
    assert len(ids) == len(set(ids))
    for operation_id in ids:
        assert "_api_v1_" not in operation_id, operation_id
    for _, _, operation in operations:
        assert operation["responses"]["default"]["$ref"] == "#/components/responses/ApiError"
        assert operation["x-agent-side-effect"] in {"read", "mutate"}

    # Explicit camelCase id from the business router is preserved.
    assert spec["paths"]["/api/v1/business/getBondCurve"]["get"]["operationId"] == "getBondCurve"

    # Shared error components documented once.
    assert "ApiError" in spec["components"]["schemas"]
    assert spec["components"]["responses"]["ApiError"]["content"]["application/json"]["schema"]["$ref"] == (
        "#/components/schemas/ApiError"
    )

    # Every used tag is described.
    described = {tag["name"] for tag in spec["tags"]}
    used = {tag for _, _, operation in operations for tag in operation.get("tags", [])}
    assert used <= described

    # Agent guide present.
    assert "For AI agents" in spec["info"]["description"]

    # Side-effect classification spot checks.
    by_id = {operation["operationId"]: operation for _, _, operation in operations}
    assert by_id["ordersPlace"]["x-agent-side-effect"] == "mutate"
    assert by_id["marketDataOptionsIvSeriesPost"]["x-agent-side-effect"] == "read"


def test_openapi_agent_pass_is_idempotent() -> None:
    first = app.openapi()

    assert first["x-agent-enhanced"] is True
    default_refs = sum(
        1
        for _, _, operation in _operations(first)
        if operation["responses"].get("default", {}).get("$ref") == "#/components/responses/ApiError"
    )
    assert default_refs == len(_operations(first))
