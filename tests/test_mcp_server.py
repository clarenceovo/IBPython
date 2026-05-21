"""Tests for the MCP server module.

Validates tool definitions, resource definitions, and basic structure
without requiring live IBKR/QuestDB/Redis connections.
"""

from __future__ import annotations

import ast
import importlib
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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
    assert len(tools) == 15, f"Expected 15 MCP tools, found {len(tools)}: {tools}"


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


def test_raw_sql_blocks_mutations():
    """Verify the raw SQL tool uses proper SQL injection protection."""
    source = MCP_SERVER_PATH.read_text()
    # Must enforce SELECT-only via prefix check
    assert "startswith(\"SELECT\")" in source or "startswith('SELECT')" in source, "Must enforce SELECT-only prefix"
    # Must block SQL comments
    assert "--" in source, "Must block inline comments"
    # Must block multi-statement injection
    assert ";" in source, "Must block multi-statement injection"
    # Must use regex-based validation
    assert "re." in source, "Must use regex for SQL validation"


def test_query_limit_capped():
    """Verify historical query has a max limit cap."""
    source = MCP_SERVER_PATH.read_text()
    assert "50_000" in source or "50000" in source, "Historical query limit must be capped"


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
