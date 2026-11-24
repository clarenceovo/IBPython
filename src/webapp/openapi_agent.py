"""Post-process the generated OpenAPI document for AI-agent consumption.

Applied once on top of the standard FastAPI document:

* replaces FastAPI's auto-generated ``operationId`` values
  (``load_account_summary_api_v1_account_summary_get``) with short, stable,
  camelCase ids derived from the route path, the names agent frameworks use
  as tool names. Explicitly set operation ids are left untouched.
* declares the shared error envelope: an ``ApiError`` schema, a reusable
  ``responses.default`` reference on every operation, and code documentation.
* fills in descriptions for tags that lack one.
* classifies every operation as read-only vs mutating via ``x-agent-side-effect``.
* appends a "For AI agents" quick-start section to ``info.description``.

The pass is idempotent (guarded by ``x-agent-enhanced``) and deterministic.
"""

from __future__ import annotations

import logging
import re
from copy import deepcopy
from typing import Any

from src.webapp.api_error import ApiError

logger = logging.getLogger(__name__)

_HTTP_METHODS = ("get", "post", "put", "delete", "patch")

#: Path segments that already carry an action verb. No method suffix needed.
_VERB_PREFIXES = (
    "get",
    "load",
    "query",
    "fetch",
    "list",
    "find",
    "place",
    "cancel",
    "create",
    "update",
    "delete",
    "remove",
    "subscribe",
    "unsubscribe",
    "capture",
    "search",
    "scan",
    "resolve",
    "preview",
    "modify",
    "exercise",
    "flush",
    "clear",
    "stop",
    "start",
    "set",
)

#: Last-path-segment verbs that mutate broker/gateway state.
_MUTATING_SEGMENTS = frozenset({
    "place",
    "cancel",
    "modify",
    "exercise",
    "capture",
    "subscribe",
    "unsubscribe",
    "flush",
    "clear",
    "stop",
    "start",
    "set",
    "delete",
    "remove",
    "create",
    "update",
})

#: Path prefixes whose every operation mutates state.
_MUTATING_PREFIXES = (
    "/api/v1/business/event-contracts/orders",
    "/api/v1/snapshot/watchlists",
)

TAG_DESCRIPTIONS: dict[str, str] = {
    "account": "Account summary, positions, portfolio items, and P&L snapshots",
    "business": (
        "Research-friendly wrappers for curves, symbol news, market panels, returns, "
        "option skew, commodity futures, portfolio risk, and ForecastEx/CME Event Contracts"
    ),
    "event-risk": "Event-contract exposure aggregation across accounts and portfolios",
    "market-data": (
        "OHLCV bars, DOM/L2 snapshots, FX/commodity options, option analytics/skew, "
        "bond yields, and latest bar queries"
    ),
    "orders": (
        "Order management: live place/cancel/modify, execution details, and explicit "
        "what-if preview. Side effects: only this tag (plus event-contract orders) trades."
    ),
    "portfolio-risk": "Portfolio-level risk cockpit snapshots",
    "reference-data": "Option chains, fundamental data, Wall Street Horizon events, news, and contract search",
    "research": "As-of dataset joins, dataset catalog, and quality repairs for the QuestDB research store",
    "scanner": "Contract search and scanning across IBKR's security database",
    "streaming": "Real-time market data streaming via Server-Sent Events (SSE)",
    "system": "Health checks, rate-limit diagnostics, and cache management",
}

_AGENT_GUIDE = """

## For AI agents

- Start with `GET /system/capabilities` (feature and version matrix) and `GET /system/health`.
- Resolve any symbol once with `GET /market-data/consolidated/resolve/{symbol}`. It accepts
  `0700.HK`, `EUR.USD`, `ESZ26`, `CONID:12345` and returns the canonical contract fields
  (`symbol`, `asset_class`, `exchange`, `currency`, `con_id`) reused by every other endpoint.
- For a multi-view overview in one call use the consolidated endpoints
  (`POST /market-data/consolidated/universal/{symbol}`); each section reports its own
  `status` (`ok` | `error` | `skipped`), `latency_ms`, and `error`.
- Timestamps are ISO-8601 UTC. `bar_size` uses IBKR spelling (`5 mins`, `1 hour`, `1 day`);
  `duration` uses IBKR duration strings (`1 D`, `1 W`, `1 M`, `1 Y`).
- **Errors** share one envelope: `{"detail": str, "code": str, "retryable": bool,
  "retry_after_seconds": int|null}`. Retry only when `retryable` is true, after waiting
  `retry_after_seconds` (429 responses also carry a `Retry-After` header). Validation
  failures (422 from request parsing) keep FastAPI's `detail: [{loc, msg, type}]` format.
- **Side effects**: only `orders/*` writes, `business/event-contracts/orders/*`,
  `streaming/subscriptions*`, `tick-data/subscribe|unsubscribe`, `snapshot/*` writes,
  `scanner/subscribe`, and `system/*` writes (`PUT /system/ibkr/connection`,
  `DELETE /system/cache/*`, scheduler job endpoints) mutate state. Every other operation
  is a read. Each operation also carries `x-agent-side-effect: read | mutate`.
- Order placement supports `Idempotency-Key` replay protection; use `/orders/preview`
  (what-if) to check margin and commissions before `/orders/place`.
"""

_PACED_EXAMPLE = {
    "summary": "IBKR pacing violation",
    "value": {
        "detail": "Historical data pacing violated: retry after cooldown",
        "code": "IBKR_PACING",
        "retryable": True,
        "retry_after_seconds": 60,
    },
}

_UNAVAILABLE_EXAMPLE = {
    "summary": "IBKR unavailable",
    "value": {
        "detail": "IBKR connection unavailable",
        "code": "IBKR_UNAVAILABLE",
        "retryable": True,
        "retry_after_seconds": None,
    },
}

_ERROR_RESPONSE_DESCRIPTION = (
    "Error envelope shared by all 4xx/5xx responses. `detail` is the human-readable "
    "message; `code` is a stable machine-readable classification; `retryable` and "
    "`retry_after_seconds` drive retry decisions. Codes include: RATE_LIMITED, "
    "IBKR_PACING, IBKR_UNAVAILABLE, IBKR_DISABLED, IBKR_CIRCUIT_OPEN, "
    "MARKET_DATA_UNAVAILABLE, CONTRACT_RESOLUTION_FAILED, ORDER_REJECTED, "
    "ORDERS_DISABLED, FEATURE_REMOVED, REQUEST_TOO_LARGE, "
    "UNSUPPORTED_HISTORICAL_REQUEST, QUESTDB_UNAVAILABLE, REDIS_UNAVAILABLE, "
    "UNAUTHORIZED, FORBIDDEN, NOT_FOUND, UPSTREAM_ERROR, INTERNAL_ERROR. "
    "429 responses carry a `Retry-After` header; request-validation 422 bodies keep "
    "FastAPI's `detail: [{loc, msg, type}]` array."
)

_AUTO_ID_RE = re.compile(r"^[a-z][a-z0-9_]*_api_v1_[a-zA-Z0-9_{}\-]+_(get|post|put|delete|patch)$")


def _camel(text: str) -> str:
    words = re.split(r"[\s_\-]+", text.strip())
    return "".join(word[:1].upper() + word[1:] for word in words if word)


def derive_operation_id(path: str, method: str) -> str:
    """Derive a short camelCase operation id from the route path and method.

    ``GET /api/v1/market-data/consolidated/resolve/{symbol}`` becomes
    ``marketDataConsolidatedResolveBySymbol``; verb-less paths get the method
    appended (``GET /api/v1/account/summary`` -> ``accountSummaryGet``).
    """

    segments = [segment for segment in path.split("/") if segment][2:]  # drop api, v1
    names: list[str] = []
    has_verb = False
    for segment in segments:
        if segment.startswith("{"):
            names.append("By" + _camel(segment.strip("{}")))
            continue
        names.append(_camel(segment))
        if segment.replace("-", "").lower().startswith(_VERB_PREFIXES):
            has_verb = True
    base = "".join(names) or "api"
    if not has_verb:
        base += method.capitalize()
    return base[0].lower() + base[1:]


def _normalize_operation_ids(schema: dict[str, Any]) -> int:
    """Rewrite auto-generated operation ids; keep explicit ones. Returns count changed."""

    changed = 0
    used: set[str] = set()
    derived: list[tuple[dict[str, Any], str]] = []
    for path, item in schema.get("paths", {}).items():
        for method, operation in item.items():
            if method not in _HTTP_METHODS:
                continue
            current = operation.get("operationId", "")
            if current and not _AUTO_ID_RE.match(current):
                used.add(current)
                continue
            derived.append((operation, derive_operation_id(path, method)))
    for operation, candidate in derived:
        operation_id = candidate
        suffix = 2
        while operation_id in used:
            operation_id = f"{candidate}{suffix}"
            suffix += 1
        used.add(operation_id)
        operation["operationId"] = operation_id
        changed += 1
    return changed


def _classify_side_effect(path: str, method: str) -> str:
    if method in {"put", "delete", "patch"}:
        return "mutate"
    if path.startswith(_MUTATING_PREFIXES):
        return "mutate"
    last = path.rstrip("/").rsplit("/", 1)[-1].replace("-", "").lower()
    return "mutate" if last in _MUTATING_SEGMENTS else "read"


def _add_error_responses(schema: dict[str, Any]) -> None:
    components = schema.setdefault("components", {})
    schemas = components.setdefault("schemas", {})
    schemas.setdefault("ApiError", ApiError.model_json_schema())
    components.setdefault("responses", {})["ApiError"] = {
        "description": _ERROR_RESPONSE_DESCRIPTION,
        "content": {
            "application/json": {
                "schema": {"$ref": "#/components/schemas/ApiError"},
                "examples": {
                    "pacing": deepcopy(_PACED_EXAMPLE),
                    "unavailable": deepcopy(_UNAVAILABLE_EXAMPLE),
                },
            }
        },
    }
    reference = {"$ref": "#/components/responses/ApiError"}
    for path, item in schema.get("paths", {}).items():
        for method, operation in item.items():
            if method not in _HTTP_METHODS:
                continue
            responses = operation.setdefault("responses", {})
            if "default" not in responses:
                responses["default"] = deepcopy(reference)


def _describe_tags(schema: dict[str, Any]) -> None:
    tags = schema.setdefault("tags", [])
    known = {tag.get("name") for tag in tags}
    for name, description in TAG_DESCRIPTIONS.items():
        if name not in known:
            tags.append({"name": name, "description": description})


def _annotate_side_effects(schema: dict[str, Any]) -> None:
    for path, item in schema.get("paths", {}).items():
        for method, operation in item.items():
            if method in _HTTP_METHODS:
                operation.setdefault("x-agent-side-effect", _classify_side_effect(path, method))


def enhance_openapi_for_agents(schema: dict[str, Any]) -> dict[str, Any]:
    """Apply the agent pass in place (idempotent) and return the schema."""

    if schema.get("x-agent-enhanced"):
        return schema
    changed_ids = _normalize_operation_ids(schema)
    _add_error_responses(schema)
    _describe_tags(schema)
    _annotate_side_effects(schema)
    info = schema.setdefault("info", {})
    description = info.get("description", "")
    if "For AI agents" not in description:
        info["description"] = description.rstrip() + _AGENT_GUIDE
    schema["x-agent-enhanced"] = True
    logger.debug("OpenAPI agent pass: %d operation ids normalized", changed_ids)
    return schema
