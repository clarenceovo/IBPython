"""Account summary, positions, portfolio, PnL snapshots."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from src.feeds.account import (
    AccountPnLDTO,
    AccountSummaryDTO,
    LivePositionDTO,
    PortfolioItemDTO,
    PositionPnLDTO,
    group_account_summary,
    normalize_account_pnl,
    normalize_account_values,
    normalize_portfolio_items,
    normalize_position_pnl,
    normalize_positions,
)
from src.feeds.ibkr_connection import (
    IBKRConnectionManager,
    capture_ibkr_session,
    require_ibkr_session,
    wait_for_ibkr_request,
)

logger = logging.getLogger(__name__)


class IBKRAccountFeedClient:
    """Account summary, positions, portfolio, PnL snapshots."""

    def __init__(self, connection: IBKRConnectionManager) -> None:
        self._connection = connection

    @property
    def _ib(self) -> Any:
        return self._connection.ib

    async def load_account_summary(self, account: str = "") -> list[AccountSummaryDTO]:
        """Load account summary values grouped by account."""
        await self._connection.ensure_connected()
        session = capture_ibkr_session(self._connection)
        logger.info("load_account_summary: account=%s", account or "all")
        values = await self._connection.with_retry(
            lambda: session[1].accountSummaryAsync(account),
            operation=f"account_summary:{account or 'all'}",
        )
        require_ibkr_session(self._connection, session)
        result = group_account_summary(normalize_account_values(values))
        logger.info("load_account_summary: %d accounts for %s", len(result), account or "all")
        return result

    async def load_live_positions(self) -> list[LivePositionDTO]:
        """Load current live positions."""
        await self._connection.ensure_connected()
        session = capture_ibkr_session(self._connection)
        logger.info("load_live_positions: starting")
        positions = await self._connection.with_retry(
            lambda: session[1].reqPositionsAsync(),
            operation="positions",
        )
        require_ibkr_session(self._connection, session)
        result = normalize_positions(positions)
        logger.info("load_live_positions: %d positions loaded", len(result))
        return result

    async def load_portfolio_items(self, account: str = "") -> list[PortfolioItemDTO]:
        """Expose current portfolio items from ib_async's local account cache."""
        await self._connection.ensure_connected()
        logger.info("load_portfolio_items: account=%s", account or "all")
        result = normalize_portfolio_items(self._ib.portfolio(account))
        logger.info("load_portfolio_items: %d items for %s", len(result), account or "all")
        return result

    async def subscribe_account_pnl(self, account: str, model_code: str = "") -> object:
        """Start a live account PnL subscription and return the ib_async PnL object."""
        _, subscription = await self._subscribe_pnl(account, model_code)
        return subscription

    async def subscribe_position_pnl(self, account: str, con_id: int, model_code: str = "") -> object:
        """Start a live position PnL subscription and return the ib_async PnLSingle object."""
        _, subscription = await self._subscribe_pnl(account, model_code, con_id)
        return subscription

    async def _subscribe_pnl(
        self, account: str, model_code: str, con_id: int | None = None,
    ) -> tuple[tuple[int, Any], Any]:
        await self._connection.ensure_connected()
        session = capture_ibkr_session(self._connection)
        operation = f"account_pnl_subscribe:{account or 'all'}" if con_id is None else f"position_pnl_subscribe:{con_id}"
        await wait_for_ibkr_request(self._connection, operation=operation)
        require_ibkr_session(self._connection, session)
        ib = session[1]
        subscription = ib.reqPnL(account, model_code) if con_id is None else ib.reqPnLSingle(account, model_code, con_id)
        return session, subscription

    async def _wait_for_pnl(self, session: tuple[int, Any], wait_seconds: float) -> None:
        ended = getattr(self._connection, "session_ended", None)
        if wait_seconds:
            if isinstance(ended, asyncio.Event):
                try:
                    await asyncio.wait_for(ended.wait(), timeout=wait_seconds)
                except TimeoutError:
                    pass
            else:
                await asyncio.sleep(wait_seconds)
        require_ibkr_session(self._connection, session)

    async def _cancel_pnl(
        self, ib: Any, account: str, model_code: str, con_id: int | None = None,
    ) -> None:
        cancel = getattr(ib, "cancelPnL" if con_id is None else "cancelPnLSingle", None)
        if cancel is None:
            return
        operation = f"account_pnl_cancel:{account or 'all'}" if con_id is None else f"position_pnl_cancel:{con_id}"
        try:
            async with asyncio.timeout(5):
                try:
                    await wait_for_ibkr_request(self._connection, operation=operation)
                finally:
                    args = (account, model_code) if con_id is None else (account, model_code, con_id)
                    cancel(*args)
        except Exception:
            logger.debug("PnL subscription cleanup failed", exc_info=True)

    async def load_account_pnl_snapshot(
        self,
        account: str,
        model_code: str = "",
        *,
        wait_seconds: float = 1.2,
    ) -> AccountPnLDTO:
        """Open a short-lived account PnL subscription and return the latest values."""
        logger.info("load_account_pnl_snapshot: account=%s wait=%.1fs", account, wait_seconds)
        session, subscription = await self._subscribe_pnl(account, model_code)
        try:
            await self._wait_for_pnl(session, wait_seconds)
            return self.account_pnl_snapshot(subscription, account, model_code)
        finally:
            await self._cancel_pnl(session[1], account, model_code)

    async def load_position_pnl_snapshot(
        self,
        account: str,
        con_id: int,
        model_code: str = "",
        *,
        wait_seconds: float = 1.2,
    ) -> PositionPnLDTO:
        """Open a short-lived position PnL subscription and return the latest values."""
        logger.info("load_position_pnl_snapshot: account=%s con_id=%d wait=%.1fs", account, con_id, wait_seconds)
        session, subscription = await self._subscribe_pnl(account, model_code, con_id)
        try:
            await self._wait_for_pnl(session, wait_seconds)
            return self.position_pnl_snapshot(subscription, account, con_id, model_code)
        finally:
            await self._cancel_pnl(session[1], account, model_code, con_id)

    def account_pnl_snapshot(self, pnl_subscription: object, account: str, model_code: str = "") -> AccountPnLDTO:
        return normalize_account_pnl(pnl_subscription, account, model_code)

    def position_pnl_snapshot(
        self,
        pnl_subscription: object,
        account: str,
        con_id: int,
        model_code: str = "",
    ) -> PositionPnLDTO:
        return normalize_position_pnl(pnl_subscription, account, con_id, model_code)
