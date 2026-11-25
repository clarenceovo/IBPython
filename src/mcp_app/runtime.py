"""Shared MCP connection runtime: refcounted state, idle-disconnect, and lifespan."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from mcp.server.fastmcp import FastMCP

from src.config.settings import Settings, load_settings
from src.feeds.exceptions import IBKRConnectionDisabledError
from src.webapp.dependencies import IBKRRestAppState, build_rest_app_state

logger = logging.getLogger(__name__)

_IBKR_TOOLSETS = frozenset({"market_data", "discovery", "options", "reference", "account", "trading"})


@dataclass
class _MCPSharedState:
    state: IBKRRestAppState
    redis_connected: bool
    questdb_connected: bool
    ibkr_connected: bool
    idle_disconnect_seconds: float
    ref_count: int = 0
    close_task: asyncio.Task[None] | None = None
    closing: bool = False
    cancel_idle_close_for_reuse: bool = False

    def context(self) -> dict[str, Any]:
        return {
            "ibkr_state": self.state,
            "redis_connected": self.redis_connected,
            "questdb_connected": self.questdb_connected,
            "ibkr_connected": self.ibkr_connected,
        }


_mcp_shared_state: _MCPSharedState | None = None
_mcp_shared_state_lock: asyncio.Lock | None = None
_mcp_shared_state_lock_loop: asyncio.AbstractEventLoop | None = None


def _get_mcp_shared_state_lock() -> asyncio.Lock:
    global _mcp_shared_state_lock
    global _mcp_shared_state_lock_loop
    loop = asyncio.get_running_loop()
    if _mcp_shared_state_lock is None or _mcp_shared_state_lock_loop is not loop:
        _mcp_shared_state_lock = asyncio.Lock()
        _mcp_shared_state_lock_loop = loop
    return _mcp_shared_state_lock


async def _build_mcp_shared_state(
    settings: Settings,
    *,
    connect_questdb: bool = True,
    connect_ibkr: bool = True,
) -> _MCPSharedState:
    mcp_settings = settings.model_copy(update={"ibkr_client_id": settings.ibkr_mcp_client_id})
    state = build_rest_app_state(mcp_settings)
    connection = getattr(state.feed, "_connection", None)
    if connection is not None:
        connection.client_role = "mcp"

    redis_connected = False
    questdb_connected = False
    ibkr_connected = False

    try:
        await state.redis.connect()
        redis_connected = True
        logger.info("MCP server: Redis connected")
    except Exception:
        logger.exception("MCP server: Redis connection failed")

    if connect_questdb:
        try:
            questdb = getattr(state.loader, "questdb", None)
            if questdb is None and mcp_settings.market_data_db_backend == "questdb":
                from src.transport.questdb_client import QuestDBClient

                questdb = QuestDBClient(
                    host=mcp_settings.questdb_host,
                    port=mcp_settings.questdb_port,
                    write_port=mcp_settings.questdb_write_port,
                    user=mcp_settings.questdb_user,
                    password=mcp_settings.questdb_password,
                    database=mcp_settings.questdb_database,
                    redis=state.redis,
                )
                state.loader.store = questdb
                state.loader.questdb = questdb
            if questdb is None:
                logger.warning("MCP server: QuestDB store is not configured")
            else:
                await questdb.connect()
                questdb_connected = True
                logger.info("MCP server: QuestDB connected")
        except Exception:
            logger.exception("MCP server: QuestDB connection failed")
    else:
        logger.info("MCP server: skipping QuestDB startup connect (storage toolset not enabled)")

    if connect_ibkr:
        try:
            await state.feed.connect()
            ibkr_connected = True
            logger.info("MCP server: IBKR feed connected")
        except IBKRConnectionDisabledError:
            logger.info("MCP server: IBKR connection disabled; Redis watcher remains active")
        except Exception:
            logger.exception("MCP server: IBKR feed connection failed (tools requiring live IBKR will reconnect lazily)")
    else:
        logger.info("MCP server: skipping IBKR startup connect (no live IBKR toolsets enabled)")

    return _MCPSharedState(
        state=state,
        redis_connected=redis_connected,
        questdb_connected=questdb_connected,
        ibkr_connected=ibkr_connected,
        idle_disconnect_seconds=settings.mcp_ibkr_idle_disconnect_seconds,
    )


async def _cancel_mcp_idle_close(shared: _MCPSharedState) -> None:
    task = shared.close_task
    shared.close_task = None
    if task is None or task.done():
        return
    shared.cancel_idle_close_for_reuse = True
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.debug("MCP server: cancelled idle IBKR disconnect")


async def _acquire_mcp_shared_state(
    settings: Settings,
    *,
    connect_questdb: bool = True,
    connect_ibkr: bool = True,
) -> _MCPSharedState:
    global _mcp_shared_state
    while True:
        close_task_to_wait: asyncio.Task[None] | None = None
        async with _get_mcp_shared_state_lock():
            if _mcp_shared_state is None:
                _mcp_shared_state = await _build_mcp_shared_state(
                    settings,
                    connect_questdb=connect_questdb,
                    connect_ibkr=connect_ibkr,
                )
                _mcp_shared_state.ref_count = 1
                return _mcp_shared_state
            if _mcp_shared_state.closing:
                close_task_to_wait = _mcp_shared_state.close_task
            else:
                _mcp_shared_state.ref_count += 1
                shared = _mcp_shared_state
                break
        if close_task_to_wait is not None:
            await close_task_to_wait
        else:
            await asyncio.sleep(0)

    await _cancel_mcp_idle_close(shared)
    return shared


async def _close_mcp_shared_state(shared: _MCPSharedState) -> None:
    try:
        await shared.state.close()
        questdb = getattr(shared.state.loader, "questdb", None)
        close = getattr(questdb, "close", None)
        if callable(close):
            await close()
    finally:
        logger.info("MCP server: all connections closed")


async def _close_mcp_shared_state_after_idle(shared: _MCPSharedState, delay_seconds: float) -> None:
    global _mcp_shared_state
    try:
        await asyncio.sleep(delay_seconds)
        async with _get_mcp_shared_state_lock():
            if _mcp_shared_state is not shared or shared.ref_count > 0:
                return
            shared.closing = True
        await _close_mcp_shared_state(shared)
        async with _get_mcp_shared_state_lock():
            if _mcp_shared_state is shared:
                _mcp_shared_state = None
    except asyncio.CancelledError:
        if shared.cancel_idle_close_for_reuse:
            shared.cancel_idle_close_for_reuse = False
            raise
        if shared.ref_count == 0:
            shared.closing = True
            await _close_mcp_shared_state(shared)
            async with _get_mcp_shared_state_lock():
                if _mcp_shared_state is shared:
                    _mcp_shared_state = None
        raise


async def _release_mcp_shared_state(shared: _MCPSharedState) -> None:
    global _mcp_shared_state
    close_now = False
    async with _get_mcp_shared_state_lock():
        if _mcp_shared_state is not shared:
            return
        shared.ref_count = max(0, shared.ref_count - 1)
        if shared.ref_count > 0:
            return
        delay_seconds = shared.idle_disconnect_seconds
        if delay_seconds <= 0:
            shared.closing = True
            _mcp_shared_state = None
            close_now = True
        else:
            logger.info("MCP server: idle; keeping IBKR clientId=%s for %.1fs", shared.state.feed.client_id, delay_seconds)
            shared.close_task = asyncio.create_task(_close_mcp_shared_state_after_idle(shared, delay_seconds))

    if close_now:
        await _close_mcp_shared_state(shared)


def make_mcp_lifespan(enabled_toolsets: Iterable[str] | None = None):
    """Build a FastMCP lifespan that skips unused backend connections."""
    toolset_set = frozenset(enabled_toolsets) if enabled_toolsets is not None else None
    connect_questdb = True if toolset_set is None else ("storage" in toolset_set)
    connect_ibkr = True if toolset_set is None else bool(toolset_set & _IBKR_TOOLSETS)

    @asynccontextmanager
    async def _lifespan(server: FastMCP) -> AsyncIterator[dict[str, Any]]:
        """Connect to Redis, QuestDB, and IBKR on startup; tear down on shutdown."""
        settings = load_settings()
        shared = await _acquire_mcp_shared_state(
            settings,
            connect_questdb=connect_questdb,
            connect_ibkr=connect_ibkr,
        )
        try:
            yield shared.context()
        finally:
            await _release_mcp_shared_state(shared)

    return _lifespan


mcp_lifespan = make_mcp_lifespan()
