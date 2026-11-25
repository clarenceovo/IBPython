"""MCP server factory, Bearer auth middleware, ASGI HTTP factory, and CLI entrypoints."""

from __future__ import annotations

import json
import logging
import os
import secrets
import sys
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from mcp.server.fastmcp import FastMCP

from src.config import config_constant as constants
from src.mcp_app.resources import register_resources
from src.mcp_app.runtime import make_mcp_lifespan
from src.mcp_app.tools import register_toolsets, resolve_toolsets

logger = logging.getLogger(__name__)


class MCPBearerAuthMiddleware:
    """Pure ASGI middleware for MCP HTTP bearer token auth.

    When ``api_key`` is empty (default for stdio transport), auth is skipped.
    When set, all HTTP requests must include ``Authorization: Bearer <token>``.
    """

    def __init__(self, app: Any, *, api_key: str) -> None:
        self.app = app
        self._api_key = api_key

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        if scope["type"] != "http" or not self._api_key:
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        auth_header = headers.get(b"authorization", b"")
        if isinstance(auth_header, bytes):
            auth_header = auth_header.decode("latin-1")

        if not auth_header.lower().startswith("bearer "):
            await self._send_401(send, "MCP API key required")
            return

        token = auth_header[7:].strip()
        if not token or not secrets.compare_digest(token, self._api_key.strip()):
            await self._send_401(send, "invalid MCP API key")
            return

        await self.app(scope, receive, send)

    @staticmethod
    async def _send_401(send: Callable[[dict[str, Any]], Awaitable[None]], detail: str) -> None:
        body = json.dumps({
            "detail": detail,
            "code": "UNAUTHORIZED",
            "retryable": False,
        }).encode("utf-8")
        await send({"type": "http.response.start", "status": 401, "headers": [
            [b"content-type", b"application/json"],
            [b"www-authenticate", b"Bearer"],
            [b"content-length", str(len(body)).encode()],
        ]})
        await send({"type": "http.response.body", "body": body})


def _get_mcp_http_config() -> tuple[str, int, str]:
    """Read MCP HTTP transport config from environment variables."""
    host = os.environ.get(constants.MCP_HTTP_HOST_ENV, constants.DEFAULT_MCP_HTTP_HOST)
    port = int(os.environ.get(constants.MCP_HTTP_PORT_ENV, str(constants.DEFAULT_MCP_HTTP_PORT)))
    api_key = os.environ.get(constants.MCP_API_KEY_ENV, constants.DEFAULT_MCP_API_KEY)
    return host, port, api_key


def _configured_toolsets(override: str | Iterable[str] | None = None) -> tuple[str, ...]:
    if override is not None:
        return resolve_toolsets(override)
    raw = os.environ.get(constants.MCP_TOOLSETS_ENV, constants.DEFAULT_MCP_TOOLSETS)
    return resolve_toolsets(raw)


def create_mcp_server(
    toolsets: str | Iterable[str] | None = None,
    *,
    name: str = "IBPython Market Data",
) -> FastMCP:
    """Build a FastMCP server registering only the requested toolsets."""
    enabled = _configured_toolsets(toolsets)
    server = FastMCP(
        name,
        lifespan=make_mcp_lifespan(enabled),
    )
    register_toolsets(server, enabled)
    register_resources(server, enabled)
    return server


def get_http_app(
    toolsets: str | Iterable[str] | None = None,
    *,
    api_key: str | None = None,
) -> Any:
    """ASGI factory for Streamable HTTP with Bearer auth enforced when ``MCP_API_KEY`` is set.

    Use with uvicorn::

        uvicorn src.mcp_server:get_http_app --factory --host 127.0.0.1 --port 9000
    """
    _, _, env_api_key = _get_mcp_http_config()
    resolved_key = env_api_key if api_key is None else api_key
    server = create_mcp_server(toolsets=toolsets)
    app = server.streamable_http_app()
    if resolved_key:
        return MCPBearerAuthMiddleware(app, api_key=resolved_key)
    return app


def _parse_toolsets_arg(argv: list[str]) -> str | None:
    for idx, arg in enumerate(argv):
        if arg == "--toolsets" and idx + 1 < len(argv):
            return argv[idx + 1]
        if arg.startswith("--toolsets="):
            return arg.split("=", 1)[1]
    return None


def main_stdio(toolsets: str | Iterable[str] | None = None) -> None:
    """Run MCP server over stdio (for Claude Desktop, Cursor, etc)."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    server = create_mcp_server(toolsets=toolsets)
    server.run(transport="stdio")


def main_streamable_http(toolsets: str | Iterable[str] | None = None) -> None:
    """Run MCP server over Streamable HTTP (for remote access).

    Binds to ``MCP_HTTP_HOST`` (default ``127.0.0.1``) on ``MCP_HTTP_PORT``
    (default ``9000``). When ``MCP_API_KEY`` is set, all requests must
    include ``Authorization: Bearer <key>``.
    """
    import uvicorn

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    host, port, api_key = _get_mcp_http_config()
    app = get_http_app(toolsets=toolsets, api_key=api_key)
    if api_key:
        logger.info("MCP HTTP: auth enabled, binding to %s:%s", host, port)
    else:
        logger.warning("MCP HTTP: auth disabled (MCP_API_KEY not set), binding to %s:%s", host, port)
    uvicorn.run(app, host=host, port=port)


def main(argv: list[str] | None = None) -> None:
    """Run stdio by default, or Streamable HTTP when ``--http`` is passed."""
    argv = sys.argv[1:] if argv is None else argv
    toolsets = _parse_toolsets_arg(argv)
    if "--http" in argv:
        main_streamable_http(toolsets=toolsets)
    else:
        main_stdio(toolsets=toolsets)
