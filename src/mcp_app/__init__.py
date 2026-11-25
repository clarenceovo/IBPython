"""Modular MCP server package for IBPython."""

from __future__ import annotations

from src.mcp_app.runtime import mcp_lifespan
from src.mcp_app.tools import TOOLSET_NAMES, TOOLSET_PRESETS, TOOLSET_REGISTRY, register_toolsets, resolve_toolsets
from src.mcp_app.transport import (
    MCPBearerAuthMiddleware,
    create_mcp_server,
    get_http_app,
    main,
    main_stdio,
    main_streamable_http,
)

__all__ = [
    "MCPBearerAuthMiddleware",
    "TOOLSET_NAMES",
    "TOOLSET_PRESETS",
    "TOOLSET_REGISTRY",
    "create_mcp_server",
    "get_http_app",
    "main",
    "main_stdio",
    "main_streamable_http",
    "mcp_lifespan",
    "register_toolsets",
    "resolve_toolsets",
]
