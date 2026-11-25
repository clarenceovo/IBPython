"""Post-process the generated OpenAPI document and build ``/llms.txt`` for AI agents.

Applied once on top of the standard FastAPI document:

* replaces FastAPI's auto-generated ``operationId`` values
  (``load_account_summary_api_v1_account_summary_get``) with short, stable,
  camelCase ids derived from the route path, the names agent frameworks use
  as tool names. Explicitly set operation ids are left untouched.
* declares the shared error envelope: an ``ApiError`` schema, a reusable
  ``responses.default`` reference on every operation, and code documentation.
* fills in descriptions for tags that lack one (including ``fixed-income``).
* classifies every operation via ``x-agent-side-effect`` (``read`` | ``mutate``)
  and ``x-agent-streaming`` (``true`` on SSE streaming endpoints so HTTP tool
  runners do not block waiting for an infinite stream to close).
* appends an accurate "For AI agents" quick-start section to ``info.description``.

Also exposes ``render_llms_txt(schema)`` so agents can fetch a compact Markdown
route catalog from ``GET /llms.txt`` (~2 KB) instead of ingesting the entire
OpenAPI JSON document into context.
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
    "run",
    "build",
)

#: Last-path-segment verbs that mutate broker/gateway/archive state.
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
    "run",
})

#: Exact (method, path) overrides for routes whose path noun does not reflect side effects.
_SIDE_EFFECT_OVERRIDES: dict[tuple[str, str], str] = {
    # Live multi-leg order placement routes (noun segments, mutating)
    ("post", "/api/v1/orders/bracket"): "mutate",
    ("post", "/api/v1/orders/oca"): "mutate",
    # What-if margin/commission preview and dry-run ticket builder (non-mutating)
    ("post", "/api/v1/orders/preview"): "read",
    ("post", "/api/v1/business/event-contracts/orders/build"): "read",
    # Dry-run research capture (loads bars and returns manifest without persisting)
    ("post", "/api/v1/research/capture"): "read",
    # Immutable reference record append in research archive
    ("post", "/api/v1/research/references"): "mutate",
    # Opens live IBKR market-data subscription + SSE stream
    ("post", "/api/v1/streaming/ticker"): "mutate",
    # Mutates IBKR session market-data feed mode (live/frozen/delayed)
    ("post", "/api/v1/system/market-data-type"): "mutate",
    # Watchlist creation/replacement in Redis (GET /watchlists remains read)
    ("post", "/api/v1/snapshot/watchlists"): "mutate",
}

#: SSE endpoints that keep HTTP connections open until disconnect or max_updates.
_STREAMING_OPERATIONS: frozenset[tuple[str, str]] = frozenset({
    ("post", "/api/v1/streaming/ticker"),
    ("get", "/api/v1/account/pnl/account/stream"),
    ("get", "/api/v1/account/pnl/position/stream"),
    ("get", "/api/v1/realtime-bars/stream/{symbol}"),
})

TAG_DESCRIPTIONS: dict[str, str] = {
    "account": "Account summary, positions, portfolio items, and P&L snapshots",
    "business": (
        "Research-friendly wrappers for curves, symbol news, market panels, returns, "
        "option skew, commodity futures, portfolio risk, and ForecastEx/CME Event Contracts"
    ),
    "event-risk": "Event-contract exposure aggregation across accounts and portfolios",
    "fixed-income": "IBKR bond futures prices, CTD analytics, and futures-implied curves",
    "market-data": (
        "OHLCV bars, DOM/L2 snapshots, FX/commodity options, option analytics/skew, "
        "bond yields, and latest bar queries"
    ),
    "orders": (
        "Order management: live place/cancel/modify, bracket/OCA groups, execution details, "
        "TCA, and explicit what-if preview. Side effects: only this tag (plus event-contract orders) trades."
    ),
    "portfolio-risk": "Portfolio-level risk cockpit snapshots",
    "reference-data": "Option chains, Wall Street Horizon events, news, and contract search",
    "research": "As-of dataset joins, dataset catalog, and quality repairs for the research store",
    "scanner": "Contract search and scanning across IBKR's security database",
    "streaming": "Real-time market data streaming via Server-Sent Events (SSE)",
    "system": "Health checks, capabilities, rate-limit diagnostics, and cache management",
}

_AGENT_GUIDE = """

## For AI agents

- **Compact discovery**: fetch `GET /llms.txt` for a token-efficient Markdown index of all active endpoints, auth headers, and error codes before loading full schemas.
- **Capabilities & health**: start with `GET /api/v1/system/capabilities` (feature and version matrix) and `GET /api/v1/system/health`.
- **Universal symbol resolution**: resolve any ticker once with `GET /api/v1/market-data/consolidated/resolve/{symbol}`. It accepts `0700.HK`, `EUR.USD`, `ESZ26`, `ES1!`, `^GSPC`, `BTC` and returns canonical IBKR contract fields (`symbol`, `asset_class`, `exchange`, `currency`, `primary_exchange`, `contract_month`, `continuous`) reused across endpoints.
- **Single-call overviews**: use `GET /api/v1/market-data/consolidated/universal/{symbol}` (supports `?sections=ohlcv,ticks,depth,trading_schedule,snapshot`) or `GET /api/v1/business/consolidated/overview`; each section reports `status` (`ok` | `error` | `skipped`), `latency_ms`, `code`, `retryable`, and `error`.
- **Conventions**: timestamps are ISO-8601 UTC. `bar_size` uses IBKR spelling (`5 secs`, `1 min`, `5 mins`, `15 mins`, `1 hour`, `1 day`); `duration` uses IBKR strings (`1 D`, `1 W`, `1 M`, `1 Y`).
- **Authentication**:
  - When `IBKR_API_BEARER_TOKEN` is configured, pass `Authorization: Bearer <api-token>` on API calls.
  - Order and archive-mutation endpoints (`/api/v1/orders/*`, `/api/v1/business/event-contracts/orders/place`, `/api/v1/research/references`, `/api/v1/research/repairs/run`) require the Redis order token in `X-Order-Authorization: Bearer <order-token>` (or `Authorization: Bearer <order-token>` when API-wide auth is off).
- **Errors**: every 4xx/5xx response uses one envelope: `{"detail": str, "code": str, "retryable": bool, "retry_after_seconds": int|null, "errors": list|null}`. Retry only when `retryable` is `true` after waiting `retry_after_seconds` (429 responses also carry `Retry-After`). Request validation failures (`422`, `code="VALIDATION_ERROR"`) include structured field details in `errors`.
- **Side effects & streaming**: every operation carries `x-agent-side-effect: read | mutate`. SSE endpoints carry `x-agent-streaming: true` (pass `max_updates` where supported if calling from a non-streaming HTTP tool). Deprecated `410 Gone` endpoints are omitted from this schema.
- **Order switches & safe workflow**: `IBKR_ORDERS_ENABLED=false` disables all order pathways (`code="ORDERS_DISABLED"`); `IBKR_DISABLE_ORDER_SUBMISSION=true` blocks live order submission (`place`, `modify`, `bracket`, `oca`, `exercise`, and live Event Contracts) with `403` (`code="ORDER_SUBMISSION_DISABLED"`, `retryable=false`) while keeping `POST /api/v1/orders/preview` (`read`) and order cancellation available. Pass `Idempotency-Key` on `/api/v1/orders/place` for 24h replay protection.
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
    "Error envelope shared by all 4xx/5xx responses. `detail` is a human-readable "
    "message; `code` is a stable machine-readable classification; `retryable` and "
    "`retry_after_seconds` drive retry decisions; `errors` carries field-level "
    "Pydantic validation items on 422 `VALIDATION_ERROR` responses. Codes include: "
    "VALIDATION_ERROR, BAD_REQUEST, PAYLOAD_TOO_LARGE, RATE_LIMITED, IBKR_PACING, "
    "IBKR_UNAVAILABLE, IBKR_DISABLED, IBKR_CIRCUIT_OPEN, MARKET_DATA_UNAVAILABLE, "
    "CONTRACT_RESOLUTION_FAILED, ORDER_REJECTED, ORDERS_DISABLED, ORDER_SUBMISSION_DISABLED, FEATURE_REMOVED, "
    "REQUEST_TOO_LARGE, UNSUPPORTED_HISTORICAL_REQUEST, QUESTDB_UNAVAILABLE, "
    "REDIS_UNAVAILABLE, UNAUTHORIZED, FORBIDDEN, NOT_FOUND, UPSTREAM_ERROR, INTERNAL_ERROR."
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


def _prune_deprecated_410_operations(schema: dict[str, Any]) -> int:
    """Remove dead 410 Gone stub operations so agents do not waste tool calls on them."""

    pruned = 0
    paths = schema.get("paths", {})
    empty_paths: list[str] = []
    for path, item in list(paths.items()):
        for method in list(item.keys()):
            if method not in _HTTP_METHODS:
                continue
            operation = item[method]
            responses = operation.get("responses", {})
            if operation.get("deprecated") and "410" in responses:
                del item[method]
                pruned += 1
        if not any(m in item for m in _HTTP_METHODS):
            empty_paths.append(path)
    for path in empty_paths:
        del paths[path]
    return pruned


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
    method_lower = method.lower()
    override = _SIDE_EFFECT_OVERRIDES.get((method_lower, path))
    if override is not None:
        return override
    if method_lower in {"put", "delete", "patch"}:
        return "mutate"
    if method_lower == "get":
        return "read"
    last = path.rstrip("/").rsplit("/", 1)[-1].replace("-", "").lower()
    return "mutate" if last in _MUTATING_SEGMENTS else "read"


def _add_error_responses(schema: dict[str, Any]) -> None:
    components = schema.setdefault("components", {})
    schemas = components.setdefault("schemas", {})
    schemas["ApiError"] = ApiError.model_json_schema()
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
    known = {tag.get("name"): tag for tag in tags if tag.get("name")}
    for name, description in TAG_DESCRIPTIONS.items():
        if name not in known:
            tags.append({"name": name, "description": description})
        elif not known[name].get("description"):
            known[name]["description"] = description


def _annotate_agent_metadata(schema: dict[str, Any]) -> None:
    for path, item in schema.get("paths", {}).items():
        for method, operation in item.items():
            if method not in _HTTP_METHODS:
                continue
            method_lower = method.lower()
            operation["x-agent-side-effect"] = _classify_side_effect(path, method_lower)
            if (method_lower, path) in _STREAMING_OPERATIONS:
                operation["x-agent-streaming"] = True


def enhance_openapi_for_agents(schema: dict[str, Any]) -> dict[str, Any]:
    """Apply the agent pass in place (idempotent) and return the schema."""

    if schema.get("x-agent-enhanced"):
        return schema
    pruned = _prune_deprecated_410_operations(schema)
    changed_ids = _normalize_operation_ids(schema)
    _add_error_responses(schema)
    _describe_tags(schema)
    _annotate_agent_metadata(schema)
    info = schema.setdefault("info", {})
    description = info.get("description", "")
    if "For AI agents" not in description:
        info["description"] = description.rstrip() + _AGENT_GUIDE
    schema["x-agent-enhanced"] = True
    logger.debug("OpenAPI agent pass: %d operation ids normalized, %d dead 410 routes pruned", changed_ids, pruned)
    return schema


def render_llms_txt(schema: dict[str, Any]) -> str:
    """Render a compact, token-efficient ``/llms.txt`` Markdown guide from the enhanced OpenAPI schema."""

    info = schema.get("info", {})
    title = info.get("title", "IBKR REST API")
    version = info.get("version", "")
    lines: list[str] = [
        f"# {title} (v{version})" if version else f"# {title}",
        "",
        "> Async FastAPI bridge for Interactive Brokers TWS/Gateway, QuestDB historical store, and Redis cache.",
        "",
        "## Agent Quick Start",
        "- **Capabilities & health**: `GET /api/v1/system/capabilities`, `GET /api/v1/system/health`, `GET /api/v1/system/rate-limits`",
        "- **Resolve any ticker first**: `GET /api/v1/market-data/consolidated/resolve/{symbol}` (`0700.HK`, `EUR.USD`, `ESZ26`, `ES1!`, `^GSPC`, `BTC`)",
        "- **One-call symbol overview**: `GET /api/v1/market-data/consolidated/universal/{symbol}?sections=ohlcv,ticks,depth,trading_schedule,snapshot`",
        "- **One-call account overview**: `GET /api/v1/business/consolidated/overview`",
        "- **Full OpenAPI schema**: `GET /api/v1/openapi.json`",
        "- **Realtime WebSocket**: `WS /api/v1/ws/market-data` (JSON frames: `subscribe`, `unsubscribe`, `status`, `ping`)",
        "",
        "## Authentication & Safety",
        "- **API-wide auth** (when `IBKR_API_BEARER_TOKEN` is set): `Authorization: Bearer <api-token>`",
        "- **Order & archive-write auth**: `X-Order-Authorization: Bearer <order-token>` (falls back to `Authorization: Bearer <order-token>` when API-wide auth is off)",
        "- **Order switches**: `IBKR_ORDERS_ENABLED=false` disables all order pathways (`code=\"ORDERS_DISABLED\"`); `IBKR_DISABLE_ORDER_SUBMISSION=true` blocks live submission (`code=\"ORDER_SUBMISSION_DISABLED\"`) while keeping `preview` and `cancel` active",
        "- **Safe order submission**: preview with `POST /api/v1/orders/preview` (`read`), then submit `POST /api/v1/orders/place` with `Idempotency-Key`",
        "",
        "## Error Envelope (All 4xx/5xx)",
        "`{\"detail\": str, \"code\": str, \"retryable\": bool, \"retry_after_seconds\": int|null, \"errors\": list|null}`",
        "- Retry **only** when `retryable: true`, honoring `retry_after_seconds` / `Retry-After`.",
        "- `422` request validation errors return `code=\"VALIDATION_ERROR\"` with field-level `{loc, msg, type}` entries in `errors`.",
        "",
        "## Endpoints by Tag",
    ]

    by_tag: dict[str, list[tuple[str, str, str, str, bool, str]]] = {}
    for path, item in sorted(schema.get("paths", {}).items()):
        for method in _HTTP_METHODS:
            if method not in item:
                continue
            op = item[method]
            tags = op.get("tags") or ["other"]
            tag = tags[0]
            op_id = op.get("operationId", "")
            side_effect = op.get("x-agent-side-effect", _classify_side_effect(path, method))
            is_stream = bool(op.get("x-agent-streaming", False))
            summary = (op.get("summary") or "").strip()
            if not summary:
                desc = (op.get("description") or "").strip().splitlines()
                summary = desc[0].strip() if desc else op_id
            by_tag.setdefault(tag, []).append((method.upper(), path, op_id, side_effect, is_stream, summary))

    tag_desc_map = {t.get("name"): t.get("description", "") for t in schema.get("tags", []) if t.get("name")}
    for tag in sorted(by_tag.keys()):
        lines.append("")
        tag_desc = tag_desc_map.get(tag, TAG_DESCRIPTIONS.get(tag, ""))
        lines.append(f"### `{tag}`" + (f" — {tag_desc}" if tag_desc else ""))
        for method_up, path, op_id, side_effect, is_stream, summary in by_tag[tag]:
            badges = [side_effect]
            if is_stream:
                badges.append("sse-stream")
            badge_str = ", ".join(badges)
            lines.append(f"- `{method_up} {path}` (`{op_id}`, {badge_str}) — {summary}")

    lines.append("")
    return "\n".join(lines)
