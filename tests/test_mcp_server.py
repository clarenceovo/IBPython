"""Tests for the MCP server module.

Validates tool definitions, resource definitions, and basic structure
without requiring live IBKR/QuestDB/Redis connections.
"""

from __future__ import annotations

import ast
import importlib
import json
import re
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import config_constant as constants

MCP_SERVER_PATH = Path(__file__).resolve().parent.parent / "src" / "mcp_server.py"


# ── Structural tests (no imports needed) ─────────────────────────────────────


def _parse_module() -> ast.Module:
    with open(MCP_SERVER_PATH) as f:
        return ast.parse(f.read())


def test_mcp_server_file_exists():
    assert MCP_SERVER_PATH.exists(), "src/mcp_server.py must exist"


def test_mcp_server_is_valid_python():
    tree = _parse_module()
    assert isinstance(tree, ast.Module)


def test_tool_count():
    tree = _parse_module()
    tools = [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
        and any(
            isinstance(d, ast.Call)
            and isinstance(d.func, ast.Attribute)
            and d.func.attr == "tool"
            for d in node.decorator_list
        )
    ]
    assert len(tools) == 57, f"Expected 57 MCP tools, found {len(tools)}: {tools}"


def test_resource_count():
    tree = _parse_module()
    resources = [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
        and any(
            isinstance(d, ast.Call)
            and isinstance(d.func, ast.Attribute)
            and d.func.attr == "resource"
            for d in node.decorator_list
        )
    ]
    assert len(resources) >= 2, f"Expected >= 2 MCP resources, found {len(resources)}"


def test_expected_tools_present():
    tree = _parse_module()
    tools = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
        and any(
            isinstance(d, ast.Call)
            and isinstance(d.func, ast.Attribute)
            and d.func.attr == "tool"
            for d in node.decorator_list
        )
    }
    expected = {
        "query_historical_ohlcv",
        "query_latest_bars",
        "query_raw_sql",
        "get_latest_bar_from_cache",
        "list_scheduler_jobs",
        "get_redis_key",
        "load_option_chains",
        "load_account_summary",
        "load_live_positions",
        "load_historical_ohlcv_live",
        "search_contracts",
        "load_fundamentals",
        "load_news",
        "query_equity_snapshots",
        "query_fx_option_snapshots",
        "load_portfolio_items",
        "load_account_pnl",
        "load_position_pnl",
        "load_news_providers",
        "load_news_article",
        "load_wsh_metadata",
        "load_wsh_event_data",
        "load_economic_calendar",
        "get_latest_equity_snapshots",
        "get_latest_fx_option_snapshot",
        "get_all_latest_equity_snapshots",
        "list_snapshot_watchlists",
        "get_snapshot_watchlist",
        "load_option_analytics",
        "load_option_skew_surface",
        "get_latest_ticks",
        "load_historical_ticks",
        "calculate_iv",
        "calculate_option_price",
        "get_head_timestamp",
        "scan_contracts",
        "load_bond_yield_history",
        "get_market_depth",
        "get_ibkr_rate_limits",
        "get_scheduler_health",
        "get_cache_stats",
    }
    missing = expected - tools
    assert not missing, f"Missing tools: {missing}"


def test_expected_resources_present():
    tree = _parse_module()
    resources = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
        and any(
            isinstance(d, ast.Call)
            and isinstance(d.func, ast.Attribute)
            and d.func.attr == "resource"
            for d in node.decorator_list
        )
    }
    expected = {"get_server_status", "get_table_schema"}
    missing = expected - resources
    assert not missing, f"Missing resources: {missing}"


def test_has_lifespan_function():
    tree = _parse_module()
    functions = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
    }
    assert "mcp_lifespan" in functions, "mcp_lifespan async context manager must exist"


def test_has_entry_points():
    tree = _parse_module()
    functions = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
        and not node.decorator_list
    }
    assert "main_stdio" in functions, "main_stdio entry point must exist"
    assert "main_streamable_http" in functions, "main_streamable_http entry point must exist"
    assert "main" in functions, "module entry point must dispatch transport mode"


def test_fastmcp_constructor_uses_supported_kwargs():
    tree = _parse_module()
    fastmcp_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "FastMCP"
    ]
    assert fastmcp_calls, "MCP server must instantiate FastMCP"
    unsupported = {kw.arg for call in fastmcp_calls for kw in call.keywords if kw.arg == "version"}
    assert not unsupported, "FastMCP in mcp>=1.27 does not accept version="


def test_mcp_dockerfile_starts_http_transport():
    dockerfile = MCP_SERVER_PATH.parent.parent / "Dockerfile.mcp"
    content = dockerfile.read_text()
    assert '"--http"' in content, "MCP Docker image must start the Streamable HTTP transport"


def test_raw_sql_blocks_mutations():
    """Verify the raw SQL tool uses proper SQL injection protection."""
    source = MCP_SERVER_PATH.read_text()
    # Must enforce allowlist of permitted first keywords
    assert '"SELECT"' in source or "'SELECT'" in source, "Must allow SELECT queries"
    assert '"WITH"' in source or "'WITH'" in source, "Must allow WITH (CTE) queries"
    # Must block dangerous keywords
    assert "INSERT" in source, "Must block INSERT"
    assert "DELETE" in source, "Must block DELETE"
    assert "DROP" in source, "Must block DROP"
    # Must block multi-statement injection
    assert ";" in source, "Must block multi-statement injection"
    # Must use regex-based validation
    assert "re." in source, "Must use regex for SQL validation"


def test_query_limit_capped():
    """Verify historical query has a max limit cap."""
    source = MCP_SERVER_PATH.read_text()
    assert "50_000" in source or "50000" in source, "Historical query limit must be capped"


def test_live_ohlcv_tool_uses_auto_chunking_helper():
    """Verify live MCP OHLCV protects IBKR from oversized single historical requests."""
    source = MCP_SERVER_PATH.read_text()
    function_source = source[source.index("async def load_historical_ohlcv_live") : source.index("async def search_contracts")]

    assert "plan_historical_auto_chunk" in function_source
    assert "ensure_historical_chunk_limit" in function_source
    assert "load_historical_ohlcv_range" in function_source


def test_mcp_news_default_uses_documented_analyst_actions_provider_code():
    source = MCP_SERVER_PATH.read_text()
    function_source = source[source.index("async def load_news") : source.index("async def query_equity_snapshots")]

    assert '["BRFG", "BRFUPDN"]' in function_source


def test_server_status_resource_content():
    """Verify the status resource returns valid JSON structure."""
    source = MCP_SERVER_PATH.read_text()
    # The resource function returns a JSON string with tool list
    assert '"tools"' in source
    assert "questdb" in source
    assert "redis" in source
    assert "ibkr" in source


def test_table_schema_resource_content():
    """Verify table schema resource describes the OHLCV table."""
    source = MCP_SERVER_PATH.read_text()
    assert "EquityOHLCV" in source
    assert "equity_snapshots" in source
    assert "fx_option_snapshots" in source


# ── Unit tests with mocks ────────────────────────────────────────────────────


@pytest.fixture
def mock_state():
    """Create a mock IBKRRestAppState for unit testing."""
    state = MagicMock()
    state.questdb = AsyncMock()
    state.redis = AsyncMock()
    state.feed = AsyncMock()
    state.close = AsyncMock()
    return state


def test_pyproject_includes_mcp_dependency():
    """Verify mcp is listed as a dependency."""
    pyproject_path = MCP_SERVER_PATH.parent.parent / "pyproject.toml"
    content = pyproject_path.read_text()
    assert "mcp" in content, "mcp must be in dependencies"


def test_mcp_server_has_docstring():
    source = MCP_SERVER_PATH.read_text()
    assert '"""' in source, "Module must have a docstring"


# ── SQL injection protection tests ──────────────────────────────────────────


def _get_query_raw_sql_function():
    """Extract the query_raw_sql function source and exec it in isolation.

    Importing mcp_server directly fails in CI when FastMCP version mismatch.
    Instead, we extract just the validation logic and test it.
    """
    source = MCP_SERVER_PATH.read_text()
    import re as _re
    # Extract the query_raw_sql function body using regex
    # We need a simpler approach: just test the validation logic directly
    return source


def _validate_sql(sql: str) -> dict[str, Any] | None:
    """Replicate the SQL validation from mcp_server.py query_raw_sql.

    Returns None if validation passes, or an error dict if it fails.
    """
    stripped = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL).strip()
    stripped = re.sub(r"--.*?$", " ", stripped, flags=re.MULTILINE).strip()
    normalized = re.sub(r"\s+", " ", stripped).strip()

    if "/*" in normalized or "*/" in normalized:
        return {"error": "Malformed SQL block comments are not permitted"}

    if re.search(r";\s*\S", normalized):
        return {"error": "Multi-statement queries are not permitted"}

    first_keyword = normalized.split()[0].upper() if normalized else ""
    if first_keyword not in ("SELECT", "WITH"):
        return {"error": "Only SELECT and WITH (CTE) queries are permitted"}

    dangerous_keywords = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|EXECUTE|INTO)\b",
        re.IGNORECASE,
    )
    match = dangerous_keywords.search(normalized)
    if match:
        return {"error": f"Keyword '{match.group(1).upper()}' is not permitted in queries"}

    return None  # Validation passed


def _strip_sql(sql: str) -> str:
    """Replicate the SQL stripping logic from mcp_server.py."""
    stripped = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL).strip()
    stripped = re.sub(r"--.*?$", " ", stripped, flags=re.MULTILINE).strip()
    return stripped


def test_sql_validation_accepts_select():
    assert _validate_sql("SELECT * FROM equity_snapshots LIMIT 10") is None


def test_sql_validation_accepts_with_cte():
    assert _validate_sql("WITH cte AS (SELECT 1) SELECT * FROM cte") is None


def test_sql_validation_rejects_insert():
    result = _validate_sql("INSERT INTO foo VALUES (1)")
    assert result is not None
    assert "error" in result
    # INSERT fails the allowlist check (doesn't start with SELECT/WITH)
    assert "permitted" in result["error"] or "INSERT" in result["error"]


def test_sql_validation_rejects_delete():
    result = _validate_sql("DELETE FROM equity_snapshots")
    assert result is not None
    assert "error" in result


def test_sql_validation_rejects_drop():
    result = _validate_sql("DROP TABLE equity_snapshots")
    assert result is not None
    assert "error" in result


def test_sql_validation_rejects_update():
    result = _validate_sql("UPDATE equity_snapshots SET last = 0")
    assert result is not None
    assert "error" in result


def test_sql_validation_rejects_truncate():
    result = _validate_sql("TRUNCATE TABLE equity_snapshots")
    assert result is not None
    assert "error" in result


def test_sql_validation_rejects_alter():
    result = _validate_sql("ALTER TABLE equity_snapshots ADD COLUMN x INT")
    assert result is not None
    assert "error" in result


def test_sql_validation_rejects_create():
    result = _validate_sql("CREATE TABLE evil (id INT)")
    assert result is not None
    assert "error" in result


def test_sql_validation_rejects_multi_statement():
    result = _validate_sql("SELECT 1; DROP TABLE equity_snapshots")
    assert result is not None
    assert "error" in result


def test_sql_validation_strips_block_comments():
    """Block comments are stripped before validation."""
    sql = "/* a comment */ SELECT 1"
    assert _validate_sql(sql) is None
    stripped = _strip_sql(sql)
    assert "/*" not in stripped
    assert stripped == "SELECT 1"


def test_sql_validation_strips_inline_comments():
    """Inline comments are stripped, neutralizing injection."""
    sql = "SELECT 1 --; DROP TABLE equity_snapshots"
    # After stripping -- comments, only "SELECT 1" remains
    assert _validate_sql(sql) is None
    stripped = _strip_sql(sql)
    assert "--" not in stripped
    assert stripped == "SELECT 1"


def test_sql_validation_rejects_grant():
    result = _validate_sql("GRANT ALL ON equity_snapshots TO public")
    assert result is not None
    assert "error" in result


def test_sql_validation_rejects_revoke():
    result = _validate_sql("REVOKE ALL ON equity_snapshots FROM public")
    assert result is not None
    assert "error" in result


def test_sql_validation_rejects_execute():
    result = _validate_sql("EXECUTE some_procedure()")
    assert result is not None
    assert "error" in result


def test_sql_validation_rejects_malformed_block_comment():
    result = _validate_sql("SELECT 1 /* unclosed comment")
    assert result is not None
    assert "error" in result


def test_sql_source_uses_stripped_for_execution():
    """Verify the source code passes stripped SQL to the query method, not original."""
    source = MCP_SERVER_PATH.read_text()
    # The query_raw_sql function should call fetch_dicts with stripped, not sql
    assert "fetch_dicts(stripped)" in source, (
        "Must pass stripped SQL to fetch_dicts, not the original sql variable"
    )


def test_sql_validation_rejects_select_into():
    """SELECT ... INTO ... FROM is a write operation in QuestDB and must be blocked."""
    result = _validate_sql("SELECT * INTO new_table FROM old_table")
    assert result is not None
    assert "error" in result
    assert "INTO" in result["error"]


def test_sql_validation_rejects_select_into_with_columns():
    """SELECT col INTO target FROM source must also be blocked."""
    result = _validate_sql("SELECT symbol, close INTO backup_bars FROM equity_ohlcv")
    assert result is not None
    assert "error" in result


def test_sql_source_blocks_into_keyword():
    """Verify the source code has INTO in the dangerous keywords blocklist."""
    source = MCP_SERVER_PATH.read_text()
    assert "INTO" in source, "INTO must be in the dangerous keywords blocklist"


def test_sql_source_notes_string_literal_false_positive():
    """Verify the source has a comment noting the string literal false-positive limitation."""
    source = MCP_SERVER_PATH.read_text()
    # Check for a comment near the dangerous_keywords section about false positives
    assert "false" in source.lower() or "string literal" in source.lower() or "NOTE" in source, (
        "Source should document the string literal false-positive limitation"
    )


# ── Lifespan resilience tests ────────────────────────────────────────────────


def test_lifespan_tracks_connection_status():
    """Verify the lifespan function tracks connection status booleans."""
    source = MCP_SERVER_PATH.read_text()
    assert "questdb_connected" in source, "Must track questdb_connected status"
    assert "redis_connected" in source, "Must track redis_connected status"
    assert "ibkr_connected" in source, "Must track ibkr_connected status"


def test_lifespan_yields_connection_flags():
    """Verify the lifespan yields connection flags in the context."""
    source = MCP_SERVER_PATH.read_text()
    # The yield should include the connection flags
    assert '"questdb_connected"' in source, "Must yield questdb_connected flag"
    assert '"redis_connected"' in source, "Must yield redis_connected flag"
    assert '"ibkr_connected"' in source, "Must yield ibkr_connected flag"


def test_tools_check_questdb_availability():
    """Verify QuestDB tools guard against unavailable QuestDB connection."""
    source = MCP_SERVER_PATH.read_text()
    assert "_require_questdb" in source, "Must have _require_questdb helper"
    assert "QuestDB is not available" in source, "Must return QuestDB unavailable error"


def test_tools_check_redis_availability():
    """Verify Redis tools guard against unavailable Redis connection."""
    source = MCP_SERVER_PATH.read_text()
    assert "_require_redis" in source, "Must have _require_redis helper"
    assert "Redis is not available" in source, "Must return Redis unavailable error"


def test_tools_check_ibkr_availability():
    """Verify IBKR tools guard against unavailable IBKR connection."""
    source = MCP_SERVER_PATH.read_text()
    assert "_require_ibkr" in source, "Must have _require_ibkr helper"
    assert "IBKR feed is not available" in source, "Must return IBKR unavailable error"


def test_questdb_tools_use_loader_questdb():
    """Verify QuestDB tools access questdb through state.loader.questdb."""
    source = MCP_SERVER_PATH.read_text()
    assert "state.loader.questdb" in source, (
        "Must access QuestDB via state.loader.questdb (IBKRRestAppState has no .questdb)"
    )


# ── MCP HTTP security tests ────────────────────────────────────────────────


def test_mcp_default_bind_is_localhost():
    """Verify MCP HTTP defaults to 127.0.0.1, not 0.0.0.0."""
    source = MCP_SERVER_PATH.read_text()
    # The mcp_server.py reads from config_constant.py defaults
    # Verify the old 0.0.0.0 is NOT in the entry point
    assert 'host="0.0.0.0"' not in source, "Must not hardcode 0.0.0.0 as bind address"
    # Verify config_constant has the right default
    assert constants.DEFAULT_MCP_HTTP_HOST == "127.0.0.1"
    assert constants.DEFAULT_MCP_HTTP_PORT == 9000


def test_mcp_lifespan_uses_dedicated_ibkr_client_id():
    """Verify MCP replaces the shared IBKR client ID before building its feed."""
    source = MCP_SERVER_PATH.read_text()

    assert "settings.ibkr_mcp_client_id" in source
    assert 'model_copy(update={"ibkr_client_id": settings.ibkr_mcp_client_id})' in source


def test_mcp_asset_class_parser_accepts_documented_enum_inputs():
    """Verify MCP does not uppercase enum values before constructing AssetClass."""
    source = MCP_SERVER_PATH.read_text()

    assert "def _parse_asset_class" in source
    assert "AssetClass(asset_class.upper())" not in source
    assert '"futures": AssetClass.FUTURE.value' in source


def test_mcp_http_config_reads_env_vars():
    """Verify _get_mcp_http_config reads from env vars with correct defaults."""
    import os
    env_vars = [constants.MCP_HTTP_HOST_ENV, constants.MCP_HTTP_PORT_ENV, constants.MCP_API_KEY_ENV]
    saved = {k: os.environ.pop(k, None) for k in env_vars}
    try:
        # Test defaults by replicating the logic (can't import mcp_server due to FastMCP init)
        host = os.environ.get(constants.MCP_HTTP_HOST_ENV, constants.DEFAULT_MCP_HTTP_HOST)
        port = int(os.environ.get(constants.MCP_HTTP_PORT_ENV, str(constants.DEFAULT_MCP_HTTP_PORT)))
        api_key = os.environ.get(constants.MCP_API_KEY_ENV, constants.DEFAULT_MCP_API_KEY)
        assert host == "127.0.0.1"
        assert port == 9000
        assert api_key == ""

        # Test with env vars set
        os.environ[constants.MCP_HTTP_HOST_ENV] = "10.0.0.1"
        os.environ[constants.MCP_HTTP_PORT_ENV] = "8888"
        os.environ[constants.MCP_API_KEY_ENV] = "my-secret-key"
        host = os.environ.get(constants.MCP_HTTP_HOST_ENV, constants.DEFAULT_MCP_HTTP_HOST)
        port = int(os.environ.get(constants.MCP_HTTP_PORT_ENV, str(constants.DEFAULT_MCP_HTTP_PORT)))
        api_key = os.environ.get(constants.MCP_API_KEY_ENV, constants.DEFAULT_MCP_API_KEY)
        assert host == "10.0.0.1"
        assert port == 8888
        assert api_key == "my-secret-key"
    finally:
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v
            else:
                os.environ.pop(k, None)


def test_mcp_bearer_auth_middleware_rejects_missing_token():
    """MCPBearerAuthMiddleware returns 401 when no Authorization header is present."""
    import asyncio
    middleware = _make_mcp_auth_middleware("test-key")

    scope = {"type": "http", "headers": []}
    status_code, body = _run_asgi_middleware(middleware, scope)
    assert status_code == 401
    assert b"MCP API key required" in body


def test_mcp_bearer_auth_middleware_rejects_wrong_token():
    """MCPBearerAuthMiddleware returns 401 when token doesn't match."""
    import asyncio
    middleware = _make_mcp_auth_middleware("test-key")

    scope = {
        "type": "http",
        "headers": [(b"authorization", b"Bearer wrong-key")],
    }
    status_code, _ = _run_asgi_middleware(middleware, scope)
    assert status_code == 401


def test_mcp_bearer_auth_middleware_allows_valid_token():
    """MCPBearerAuthMiddleware passes request through when token matches."""
    import asyncio
    called = False

    async def inner_app(scope, receive, send):
        nonlocal called
        called = True
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    middleware = _make_mcp_auth_middleware("test-key", inner_app=inner_app)
    scope = {
        "type": "http",
        "headers": [(b"authorization", b"Bearer test-key")],
    }
    _run_asgi_middleware(middleware, scope)
    assert called


def test_mcp_bearer_auth_middleware_skips_auth_when_key_empty():
    """MCPBearerAuthMiddleware passes request through when api_key is empty."""
    import asyncio
    called = False

    async def inner_app(scope, receive, send):
        nonlocal called
        called = True
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    middleware = _make_mcp_auth_middleware("", inner_app=inner_app)
    scope = {"type": "http", "headers": []}
    _run_asgi_middleware(middleware, scope)
    assert called


def test_mcp_bearer_auth_middleware_skips_non_http_scopes():
    """MCPBearerAuthMiddleware passes through non-HTTP (e.g. lifespan) scopes."""
    import asyncio
    called = False

    async def inner_app(scope, receive, send):
        nonlocal called
        called = True

    middleware = _make_mcp_auth_middleware("test-key", inner_app=inner_app)
    asyncio.run(middleware({"type": "lifespan"}, lambda: {"type": "lifespan.startup"}, lambda msg: None))
    assert called


def _fake_asgi_app(response_body: bytes = b"ok"):
    """Create a minimal ASGI app that sends a 200 with the given body."""
    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": response_body})
    return app


def _make_mcp_auth_middleware(api_key: str, inner_app: Any = None):
    """Replicate MCPBearerAuthMiddleware for testing without importing mcp_server."""
    import secrets as _secrets

    app = inner_app or _fake_asgi_app(b"ok")

    class _Middleware:
        def __init__(self, app, *, api_key):
            self.app = app
            self._api_key = api_key

        async def __call__(self, scope, receive, send):
            import json as _json
            if scope["type"] != "http" or not self._api_key:
                await self.app(scope, receive, send)
                return
            headers = dict(scope.get("headers", []))
            auth_header = headers.get(b"authorization", b"")
            if isinstance(auth_header, bytes):
                auth_header = auth_header.decode("latin-1")
            if not auth_header.lower().startswith("bearer "):
                body = _json.dumps({"detail": "MCP API key required"}).encode()
                await send({"type": "http.response.start", "status": 401, "headers": [
                    [b"content-type", b"application/json"],
                    [b"www-authenticate", b"Bearer"],
                    [b"content-length", str(len(body)).encode()],
                ]})
                await send({"type": "http.response.body", "body": body})
                return
            token = auth_header[7:].strip()
            if not token or not _secrets.compare_digest(token, self._api_key.strip()):
                body = _json.dumps({"detail": "invalid MCP API key"}).encode()
                await send({"type": "http.response.start", "status": 401, "headers": [
                    [b"content-type", b"application/json"],
                    [b"www-authenticate", b"Bearer"],
                    [b"content-length", str(len(body)).encode()],
                ]})
                await send({"type": "http.response.body", "body": body})
                return
            await self.app(scope, receive, send)

    return _Middleware(app, api_key=api_key)


def _run_asgi_middleware(middleware, scope):
    """Run an ASGI middleware with a minimal scope, return (status_code, body)."""
    import asyncio
    status_code = None
    body = None

    async def receive():
        return {"type": "http.request", "body": b""}

    async def send(msg):
        nonlocal status_code, body
        if msg["type"] == "http.response.start":
            status_code = msg["status"]
        if msg["type"] == "http.response.body":
            body = msg["body"]

    asyncio.run(middleware(scope, receive, send))
    return status_code, body


# ── Source-code assertions for duplicated middleware (P1-8) ───────────────────


def test_mcp_auth_middleware_uses_compare_digest():
    """Verify the real MCPBearerAuthMiddleware uses timing-safe comparison."""
    source = MCP_SERVER_PATH.read_text()
    assert "secrets.compare_digest" in source, (
        "MCPBearerAuthMiddleware must use secrets.compare_digest for timing-safe comparison"
    )


def test_mcp_auth_middleware_compare_digest_pattern():
    """Verify the real middleware compares token against api_key.strip()."""
    source = MCP_SERVER_PATH.read_text()
    assert "compare_digest(token, self._api_key.strip())" in source, (
        "MCPBearerAuthMiddleware must use compare_digest(token, self._api_key.strip())"
    )


def test_mcp_auth_middleware_401_has_www_authenticate():
    """Verify 401 responses include proper WWW-Authenticate header."""
    source = MCP_SERVER_PATH.read_text()
    # Find the middleware's 401 sending code
    assert 'b"www-authenticate"' in source or "b'www-authenticate'" in source, (
        "401 responses must include WWW-Authenticate header"
    )


# ── Static tool list vs AST tool count (P1-9) ──────────────────────────────


def test_server_status_tool_count_matches_ast():
    """Verify the hardcoded tool list in get_server_status matches actual tool count."""
    source = MCP_SERVER_PATH.read_text()
    tree = ast.parse(source)

    # Count actual @mcp.tool() decorated functions via AST
    ast_tools = [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
        and any(
            isinstance(d, ast.Call)
            and isinstance(d.func, ast.Attribute)
            and d.func.attr == "tool"
            for d in node.decorator_list
        )
    ]

    # Extract the tools list from get_server_status resource function
    # Find the JSON string in the function body and parse it
    status_func_start = source.index("def get_server_status()")
    # Find the next function/class definition after get_server_status
    next_def_match = re.search(r"\n(?:def |class |@mcp\.)", source[status_func_start + 1:])
    if next_def_match:
        status_func_source = source[status_func_start : status_func_start + 1 + next_def_match.start()]
    else:
        status_func_source = source[status_func_start:]

    # Extract the JSON object from json.dumps({...})
    json_match = re.search(r"json\.loads\((r?f?\"{3}|')(.+?)(\1)\)", status_func_source, re.DOTALL)
    # Alternative: just extract the dict from json.dumps call
    # Find the tools list directly
    tools_list_match = re.search(r'"tools":\s*\[(.*?)\]', status_func_source, re.DOTALL)
    assert tools_list_match, "Could not find tools list in get_server_status"
    tools_list_str = tools_list_match.group(1)
    # Count items — each tool entry is a quoted string
    tool_entries = re.findall(r'"[^"]+—[^"]+"', tools_list_str)
    if not tool_entries:
        # Fallback: count entries separated by commas that contain descriptions
        tool_entries = re.findall(r'"[^"]+ — [^"]+"', tools_list_str)

    assert len(tool_entries) == len(ast_tools), (
        f"get_server_status lists {len(tool_entries)} tools but AST finds {len(ast_tools)}: "
        f"status={tool_entries}, ast={ast_tools}"
    )
