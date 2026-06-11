"""IBKR Feed Client — facade composing domain-specific sub-clients.

This module preserves the original public API of ``IBKRFeedClient`` while
delegating to focused domain clients:
- ``IBKRConnectionManager`` — connection lifecycle, retry infrastructure
- ``IBKRHistoricalClient`` — historical OHLCV, contract qualification
- ``IBKROptionsFeedClient`` — option chains, analytics, skew surfaces
- ``IBKRAccountFeedClient`` — account summary, positions, PnL
- ``IBKRReferenceFeedClient`` — news, fundamentals, WSH, scanner, bonds, streaming

All existing import paths remain backward-compatible:
    from src.feeds.ibkr_feed import IBKRFeedClient
"""

from __future__ import annotations

import asyncio
import logging
import time as monotonic_time
from collections.abc import Sequence
from datetime import date, datetime, time, timedelta, timezone
from typing import TYPE_CHECKING, Any, ClassVar

from src.config import config_constant as constants
from src.feeds.contracts import ContractSpec, OptionChain, OptionChainRequest, build_ibkr_contract
from src.feeds.fundamental_data import (
    FundamentalDataReport,
    FundamentalDataRequest,
    WSHEventDataReport,
    WSHEventDataRequest,
    WSHMetadataReport,
)
from src.feeds.models import AssetClass, FXOHLCVBar, FutureOHLCVBar, OHLCVBar, OHLCVRequest
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
from src.feeds.bonds import (
    BondYieldBar,
    BondYieldHistoryRequest,
    normalize_ibkr_bond_yield_bars,
)
from src.feeds.scanner import (
    ContractScanRequest,
    ContractSearchRequest,
    ContractSearchResult,
    MarketScannerRequest,
    MarketScannerRow,
)
from src.feeds.news import (
    HistoricalNewsHeadline,
    HistoricalNewsRequest,
    NewsArticle,
    NewsArticleRequest,
    NewsProvider,
    format_historical_news_datetime,
    normalize_historical_news,
    normalize_news_article,
    normalize_news_providers,
)
from src.feeds.options import (
    DEFAULT_OPTION_ANALYTICS_GENERIC_TICKS,
    OptionAnalyticsRequest,
    OptionAnalyticsSnapshot,
    OptionContractSpec,
    OptionSkewSurfaceRequest,
    OptionSkewSurfaceResponse,
    build_ibkr_option_contract,
    build_skew_option_contracts,
    calculate_maturity_skew,
    normalize_option_analytics_from_ticker,
    select_option_chain,
    select_skew_expirations,
    select_skew_strikes,
)
from src.feeds.snapshotter import FXOptionSnapshot

# Sub-client imports
from src.feeds.ibkr_connection import (
    IBKRConnectionManager,
    _patch_ib_insync_loop_getters,
    _maybe_apply_nest_asyncio,
    _root_cause_message,
    _last_ibkr_error_message,
    _contract_text,
    _contract_int,
    _qualification_hint,
    acquire_market_data_line,
    wait_for_ibkr_request,
)
from src.feeds.exceptions import (  # noqa: E402
    IBKRConnectionError,
    IBKRContractResolutionError,
    IBKRMarketDataUnavailableError,
    ConnectionStatus,
)
from src.feeds.ibkr_historical import (
    IBKRHistoricalClient,
    normalize_ibkr_bars,
    _format_ibkr_end_datetime,
    _parse_ibkr_timestamp,
    _historical_identical_key,
    _historical_same_contract_key,
    _ibkr_max_duration_for_bar_size,
    _ibkr_duration_to_seconds,
    _seconds_to_timedelta,
    _ibkr_duration_between,
    _parse_timestamp_string,
    _ohlcv_bar_model_for_request,
    _fx_base_currency,
    US_EQUITY_PRIMARY_EXCHANGE_PREFERENCE,
)
from src.feeds.ibkr_options_feed import (
    IBKROptionsFeedClient,
    _ticker_snapshot_price,
    _finite_positive,
    format_unqualified_option_message,
    normalize_ibkr_option_chains,
    option_chain_requests_for_diagnostics,
)
from src.feeds.ibkr_account_feed import IBKRAccountFeedClient
from src.feeds.ibkr_reference_feed import (
    IBKRReferenceFeedClient,
    _float_or_none,
    _build_wsh_event_data,
)
from src.feeds.ibkr_order_client import IBKROrderClient
from src.feeds.orders import (
    CachedOrderLookup,
    CancelOrderResponse,
    CompletedOrder,
    ExecutionRequest,
    ExecutionResponse,
    ModifyOrderRequest,
    OpenOrder,
    OrderEnvelope,
    OrderResponse,
    PlaceOrderRequest,
    WhatIfOrderResponse,
)
from src.feeds.ibkr_marketdata_ext import IBKRMarketDataExtClient
from src.feeds.circuit_breaker import CircuitBreaker, CircuitState  # noqa: F401
from src.feeds.pacing import IBKRHistoricalPacingGuard  # noqa: F401

logger = logging.getLogger(__name__)

DETECTION_TIMEOUT = 180


# Re-export _ibkr_sec_type_for_option_underlying from options module
from src.feeds.ibkr_options_feed import _ibkr_sec_type_for_option_underlying  # noqa: E402

if TYPE_CHECKING:
    from src.feeds.tick_data import TickType


def _ibkr_rate_limit_contract_key(contract: Any, fallback: str) -> str:
    con_id = getattr(contract, "conId", None)
    if con_id:
        return f"conId:{con_id}"
    local_symbol = getattr(contract, "localSymbol", None)
    if local_symbol:
        return f"localSymbol:{local_symbol}"
    symbol = getattr(contract, "symbol", fallback)
    sec_type = getattr(contract, "secType", "")
    exchange = getattr(contract, "exchange", "")
    currency = getattr(contract, "currency", "")
    return f"{sec_type}:{symbol}:{exchange}:{currency}:{fallback}"


class IBKRFeedClient:
    """Async IBKR market data adapter — facade composing domain sub-clients.

    All public methods delegate to the appropriate sub-client while preserving
    backward compatibility with the original monolithic interface.
    """

    def __init__(
        self,
        host: str = constants.DEFAULT_IBKR_HOST,
        port: int = constants.DEFAULT_IBKR_PORT,
        client_id: int = constants.DEFAULT_IBKR_CLIENT_ID,
        *,
        retry_attempts: int = 3,
        retry_base_delay_seconds: float = 0.5,
        pacing_guard: "IBKRHistoricalPacingGuard | None" = None,
        rate_limiter: Any | None = None,
        redis: Any | None = None,
    ) -> None:
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout_seconds=30.0,
        )
        if rate_limiter is None:
            try:
                from src.transport.ibkr_rate_limit import IBKRRateLimitController

                rate_limiter = IBKRRateLimitController(
                    redis_client=redis,
                    pacing_guard=pacing_guard or IBKRHistoricalPacingGuard(),
                )
                pacing_guard = rate_limiter.pacing_guard
            except Exception:
                logger.debug("failed to initialize IBKR rate limiter; using historical pacing only", exc_info=True)
        self._connection = IBKRConnectionManager(
            host=host,
            port=port,
            client_id=client_id,
            retry_attempts=retry_attempts,
            retry_base_delay_seconds=retry_base_delay_seconds,
            pacing_guard=pacing_guard or IBKRHistoricalPacingGuard(),
            rate_limiter=rate_limiter,
        )
        # Expose top-level attributes for backward compatibility
        self.host = self._connection.host
        self.port = self._connection.port
        self.client_id = self._connection.client_id
        self.retry_attempts = self._connection.retry_attempts
        self.retry_base_delay_seconds = self._connection.retry_base_delay_seconds

        # Compose domain clients
        self._historical = IBKRHistoricalClient(self._connection)
        self._options = IBKROptionsFeedClient(self._connection, self._historical)
        self._account = IBKRAccountFeedClient(self._connection)
        self._reference = IBKRReferenceFeedClient(self._connection, self._historical)
        self._order_client = IBKROrderClient(self._connection, redis=redis)
        self._marketdata_ext = IBKRMarketDataExtClient(self._connection)

    # Backward-compatible internal accessors
    @property
    def _ib(self) -> Any | None:
        return self._connection.ib

    @_ib.setter
    def _ib(self, value: Any) -> None:
        self._connection._ib = value

    @property
    def _pacing_guard(self) -> "IBKRHistoricalPacingGuard | None":
        return self._connection.pacing_guard

    @property
    def _shutting_down(self) -> bool:
        return self._connection.shutting_down

    @property
    def _connection_dead(self) -> bool:
        return getattr(self._connection, '_connection_dead', False)

    @property
    def _last_ibkr_error(self) -> tuple[int, str] | None:
        return self._connection.last_ibkr_error

    @property
    def _reconnect_lock(self) -> asyncio.Lock:
        return self._connection._reconnect_lock

    @property
    def _background_tasks(self) -> set[asyncio.Task[None]]:
        return self._connection._background_tasks

    # ------------------------------------------------------------------
    # Public connection status
    # ------------------------------------------------------------------

    def connection_status(self) -> str:
        """Return the current IBKR connection status as a string.

        Returns one of: ``ConnectionStatus.CONNECTED``,
        ``ConnectionStatus.DISCONNECTED``, or ``ConnectionStatus.DOWN``.
        """
        if getattr(self._connection, '_connection_dead', False):
            return ConnectionStatus.DOWN
        if self._connection.is_connected:
            return ConnectionStatus.CONNECTED
        return ConnectionStatus.DISCONNECTED

    # ------------------------------------------------------------------
    # Connection lifecycle — delegated to connection manager
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        await self._connection.connect()

    async def disconnect(self) -> None:
        await self._connection.disconnect()

    async def __aenter__(self) -> "IBKRFeedClient":
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.disconnect()

    async def _ensure_connected(self) -> None:
        """Ensure IB connection is alive, reconnecting if needed.

        Delegates through the facade so that test monkeypatches on
        ``client.connect`` are respected.
        """
        # Circuit breaker guard — fast-fail before any I/O
        await self._circuit_breaker.guard()
        try:
            if self._connection.ib is not None and hasattr(self._connection.ib, 'isConnected') and self._connection.ib.isConnected():
                return
            async with self._connection._reconnect_lock:
                if self._connection.ib is not None and hasattr(self._connection.ib, 'isConnected') and self._connection.ib.isConnected():
                    return
                try:
                    await self.connect()
                except Exception as exc:
                    await self._circuit_breaker.record_failure()
                    root_cause = _root_cause_message(exc)
                    logger.exception(
                        "IBKR connection unavailable: host=%s port=%d clientId=%d root_cause=%s last_ibkr_error=%s",
                        self.host,
                        self.port,
                        self.client_id,
                        root_cause,
                        self._last_ibkr_error,
                    )
                    raise IBKRConnectionError(
                        f"IBKR not available at {self.host}:{self.port} — "
                        f"ensure TWS or IB Gateway is running and API connections are enabled. "
                        f"clientId={self.client_id}. root_cause={root_cause}. "
                        f"{_last_ibkr_error_message(self._last_ibkr_error)}"
                    ) from exc
        except (IBKRConnectionError, RuntimeError):
            raise
        except Exception as exc:
            await self._circuit_breaker.record_failure()
            raise
        else:
            await self._circuit_breaker.record_success()

    # ------------------------------------------------------------------
    # Retry — delegated to connection manager
    # ------------------------------------------------------------------

    def _is_transient_error(self, exc: BaseException) -> bool:
        return self._connection.is_transient_error(exc)

    async def _with_retry(self, call: Any, *, operation: str) -> Any:
        try:
            result = await self._connection.with_retry(call, operation=operation)
            await self._circuit_breaker.record_success()
            return result
        except Exception:
            await self._circuit_breaker.record_failure()
            raise

    def circuit_breaker_state(self) -> dict[str, Any]:
        """Return the current circuit breaker state for health checks."""
        return self._circuit_breaker.get_state_dict()

    def _disconnect_stale_client(self) -> None:
        self._connection._disconnect_stale_client()

    # ------------------------------------------------------------------
    # Historical OHLCV — delegated to historical client
    # ------------------------------------------------------------------

    async def qualify_contract(self, spec: ContractSpec) -> Any:
        # NOTE: Intentionally not delegating to self._historical.qualify_contract()
        # because tests monkeypatch build_ibkr_contract in this module's namespace.
        await self._ensure_connected()
        logger.info(
            "qualify_contract: symbol=%s asset_class=%s exchange=%s primary_exchange=%s con_id=%s",
            spec.symbol,
            spec.asset_class,
            spec.exchange,
            spec.primary_exchange,
            spec.con_id,
        )
        t0 = monotonic_time.monotonic()
        contract = build_ibkr_contract(spec)
        qualified = await self._with_retry(
            lambda: self._ib.qualifyContractsAsync(contract),
            operation=f"qualify_contract:{spec.symbol}",
        )
        if qualified:
            selected = qualified[0]
            logger.debug(
                "qualify_contract completed in %.2fs for %s con_id=%s primary_exchange=%s",
                monotonic_time.monotonic() - t0,
                spec.symbol,
                _contract_int(selected, "conId"),
                _contract_text(selected, "primaryExchange", "primaryExch"),
            )
            return selected

        logger.warning(
            "qualifyContractsAsync returned no contract for %s; requesting contract details fallback",
            spec.symbol,
        )
        try:
            selected = await self._resolve_contract_from_details(contract, spec)
        except Exception as exc:
            raise IBKRContractResolutionError(
                f"IBKR could not qualify contract for {spec.symbol}.{_qualification_hint(spec)} "
                f"contract_details_root_cause={_root_cause_message(exc)} "
                f"{_last_ibkr_error_message(self._last_ibkr_error)}"
            ) from exc
        if selected is None:
            raise IBKRContractResolutionError(
                f"IBKR could not qualify contract for {spec.symbol}.{_qualification_hint(spec)} "
                f"{_last_ibkr_error_message(self._last_ibkr_error)}"
            )
        logger.info(
            "qualify_contract fallback selected %s con_id=%s exchange=%s primary_exchange=%s in %.2fs",
            spec.symbol,
            _contract_int(selected, "conId"),
            _contract_text(selected, "exchange"),
            _contract_text(selected, "primaryExchange", "primaryExch"),
            monotonic_time.monotonic() - t0,
        )
        return selected

    async def _resolve_contract_from_details(self, contract: Any, spec: ContractSpec) -> Any | None:
        details = await self._with_retry(
            lambda: self._ib.reqContractDetailsAsync(contract),
            operation=f"contract_details:{spec.symbol}",
        )
        from src.feeds.ibkr_historical import _select_contract_from_details
        selected = _select_contract_from_details(details, spec, contract)
        if (
            selected is not None
            and spec.asset_class is AssetClass.EQUITY
            and spec.exchange.upper() == "SMART"
            and _contract_text(selected, "exchange")
        ):
            setattr(selected, "exchange", "SMART")
        return selected

    async def load_historical_ohlcv_range(
        self,
        request: OHLCVRequest,
        *,
        start_datetime: datetime,
        end_datetime: datetime | None = None,
        max_chunks: int = constants.DEFAULT_IBKR_HISTORICAL_MAX_CHUNKS,
    ) -> list[OHLCVBar]:
        return await self._historical.load_historical_ohlcv_range(
            request, start_datetime=start_datetime, end_datetime=end_datetime, max_chunks=max_chunks,
        )

    async def load_historical_ohlcv(
        self,
        request: OHLCVRequest,
        *,
        max_chunks: int = constants.DEFAULT_IBKR_HISTORICAL_MAX_CHUNKS,
    ) -> list[OHLCVBar]:
        return await self._historical.load_historical_ohlcv(request, max_chunks=max_chunks)

    async def load_trading_schedule(
        self,
        request: OHLCVRequest,
        *,
        ref_date: date,
        use_rth: bool = True,
    ) -> tuple[Any, ...]:
        return await self._historical.load_trading_schedule(request, ref_date=ref_date, use_rth=use_rth)

    # ------------------------------------------------------------------
    # Options — delegated to options client
    # ------------------------------------------------------------------

    async def load_option_chains(self, request: OptionChainRequest) -> list[OptionChain]:
        return await self._options.load_option_chains(request)

    async def load_option_analytics(self, request: OptionAnalyticsRequest) -> OptionAnalyticsSnapshot:
        # NOTE: This method is intentionally not a simple delegation.
        # Tests monkeypatch ``ibkr_feed_module.build_ibkr_option_contract``, so
        # we resolve it from the facade's own namespace here.
        return await self._do_load_option_analytics(request)

    async def _do_load_option_analytics(self, request: OptionAnalyticsRequest) -> OptionAnalyticsSnapshot:
        """Internal: load option analytics using the facade's namespace for monkeypatch compat."""
        await self._ensure_connected()
        logger.info("load_option_analytics: underlying=%s expiry=%s", request.contract.underlying_symbol, request.contract.expiry)
        t0 = monotonic_time.monotonic()
        contract = build_ibkr_option_contract(request.contract)
        qualified = await self._with_retry(
            lambda: self._ib.qualifyContractsAsync(contract),
            operation=f"qualify_option:{request.contract.underlying_symbol}:{request.contract.expiry}",
        )
        if qualified:
            contract = qualified[0]
        else:
            error = await self._unqualified_option_error(request.contract)
            raise error
        generic_tick_list = request.generic_tick_list
        use_snapshot = not generic_tick_list
        if generic_tick_list and request.regulatory_snapshot:
            logger.warning(
                "load_option_analytics: regulatory_snapshot ignored because IBKR snapshot market data "
                "does not support generic ticks; using short-lived streaming subscription"
            )
        logger.debug(
            "load_option_analytics market data mode: snapshot=%s generic_ticks=%s",
            use_snapshot,
            generic_tick_list or "none",
        )
        operation = f"option_analytics:{request.contract.underlying_symbol}:{request.contract.expiry}"
        lease = await acquire_market_data_line(
            self._connection,
            contract_key=_ibkr_rate_limit_contract_key(contract, operation),
            operation=operation,
            ttl_seconds=max(30.0, request.snapshot_wait_seconds + 10.0),
        )
        await wait_for_ibkr_request(self._connection, operation=f"{operation}:reqMktData")
        try:
            ticker = self._ib.reqMktData(
                contract,
                genericTickList=generic_tick_list,
                snapshot=use_snapshot,
                regulatorySnapshot=request.regulatory_snapshot if use_snapshot else False,
                mktDataOptions=[],
            )
        except Exception:
            await lease.release()
            raise
        try:
            await asyncio.sleep(request.snapshot_wait_seconds)
            result = normalize_option_analytics_from_ticker(ticker, request.contract)
            logger.debug("load_option_analytics completed in %.2fs for %s", monotonic_time.monotonic() - t0, request.contract.underlying_symbol)
            return result
        finally:
            try:
                await wait_for_ibkr_request(self._connection, operation=f"{operation}:cancelMktData")
                self._ib.cancelMktData(contract)
            except Exception:
                logger.debug("Failed to cancel market data subscription for %s", request.contract.underlying_symbol, exc_info=True)
            await lease.release()

    async def _unqualified_option_error(self, contract: OptionContractSpec) -> IBKRContractResolutionError:
        chains: list[OptionChain] = []
        for chain_request in option_chain_requests_for_diagnostics(contract):
            try:
                chains = await self.load_option_chains(chain_request)
            except Exception:
                logger.debug(
                    "option diagnostic chain lookup failed for %s as %s",
                    contract.underlying_symbol,
                    chain_request.asset_class,
                    exc_info=True,
                )
                continue
            if chains:
                break
        return IBKRContractResolutionError(
            format_unqualified_option_message(
                contract,
                chains=chains,
                last_ibkr_error=self._last_ibkr_error,
            )
        )

    async def capture_fx_option_snapshots(
        self,
        contracts: Sequence[OptionContractSpec],
        *,
        symbols: Sequence[str],
        generic_ticks: tuple[str, ...] = DEFAULT_OPTION_ANALYTICS_GENERIC_TICKS,
        snapshot_wait_seconds: float = 2.0,
    ) -> list[FXOptionSnapshot]:
        return await self._options.capture_fx_option_snapshots(
            contracts,
            symbols=symbols,
            generic_ticks=generic_ticks,
            snapshot_wait_seconds=snapshot_wait_seconds,
        )

    async def load_option_skew_surface(self, request: OptionSkewSurfaceRequest) -> OptionSkewSurfaceResponse:
        return await self._options.load_option_skew_surface(request)

    # ------------------------------------------------------------------
    # Account — delegated to account client
    # ------------------------------------------------------------------

    async def load_account_summary(self, account: str = "") -> list[AccountSummaryDTO]:
        return await self._account.load_account_summary(account)

    async def load_live_positions(self) -> list[LivePositionDTO]:
        return await self._account.load_live_positions()

    async def load_portfolio_items(self, account: str = "") -> list[PortfolioItemDTO]:
        return await self._account.load_portfolio_items(account)

    async def subscribe_account_pnl(self, account: str, model_code: str = "") -> object:
        return await self._account.subscribe_account_pnl(account, model_code)

    async def subscribe_position_pnl(self, account: str, con_id: int, model_code: str = "") -> object:
        return await self._account.subscribe_position_pnl(account, con_id, model_code)

    async def load_account_pnl_snapshot(
        self,
        account: str,
        model_code: str = "",
        *,
        wait_seconds: float = 1.2,
    ) -> AccountPnLDTO:
        return await self._account.load_account_pnl_snapshot(account, model_code, wait_seconds=wait_seconds)

    async def load_position_pnl_snapshot(
        self,
        account: str,
        con_id: int,
        model_code: str = "",
        *,
        wait_seconds: float = 1.2,
    ) -> PositionPnLDTO:
        return await self._account.load_position_pnl_snapshot(account, con_id, model_code, wait_seconds=wait_seconds)

    def account_pnl_snapshot(self, pnl_subscription: object, account: str, model_code: str = "") -> AccountPnLDTO:
        return self._account.account_pnl_snapshot(pnl_subscription, account, model_code)

    def position_pnl_snapshot(
        self,
        pnl_subscription: object,
        account: str,
        con_id: int,
        model_code: str = "",
    ) -> PositionPnLDTO:
        return self._account.position_pnl_snapshot(pnl_subscription, account, con_id, model_code)

    # ------------------------------------------------------------------
    # Reference data — delegated to reference client
    # ------------------------------------------------------------------

    async def load_bond_yield_history(self, request: BondYieldHistoryRequest) -> list[BondYieldBar]:
        return await self._reference.load_bond_yield_history(request)

    async def load_fundamental_data(self, request: FundamentalDataRequest) -> FundamentalDataReport:
        return await self._reference.load_fundamental_data(request)

    async def load_wsh_metadata(self) -> WSHMetadataReport:
        return await self._reference.load_wsh_metadata()

    async def load_wsh_event_data(
        self,
        request: WSHEventDataRequest,
        *,
        ensure_metadata: bool = True,
    ) -> WSHEventDataReport:
        return await self._reference.load_wsh_event_data(request, ensure_metadata=ensure_metadata)

    async def load_news_providers(self) -> list[NewsProvider]:
        return await self._reference.load_news_providers()

    async def load_historical_news(self, request: HistoricalNewsRequest) -> list[HistoricalNewsHeadline]:
        return await self._reference.load_historical_news(request)

    async def load_news_article(self, request: NewsArticleRequest) -> NewsArticle:
        return await self._reference.load_news_article(request)

    async def search_contracts(self, request: ContractSearchRequest) -> list[ContractSearchResult]:
        return await self._reference.search_contracts(request)

    async def scan_contracts(self, request: ContractScanRequest) -> list[ContractSearchResult]:
        return await self._reference.scan_contracts(request)

    async def run_market_scanner(self, request: MarketScannerRequest) -> list[MarketScannerRow]:
        return await self._reference.run_market_scanner(request)

    async def subscribe_ticker(self, spec: ContractSpec) -> Any:
        return await self._reference.subscribe_ticker(spec)

    async def unsubscribe_ticker(self, ticker: Any) -> None:
        return await self._reference.unsubscribe_ticker(ticker)

    async def capture_equity_snapshots(
        self,
        symbols: Sequence[tuple[str, str, str, str, int]],
        *,
        snapshot_wait_seconds: float = constants.DEFAULT_IBKR_EQUITY_SNAPSHOT_WAIT_SECONDS,
        lease_ttl_seconds: float = constants.DEFAULT_IBKR_EQUITY_SNAPSHOT_LEASE_TTL_SECONDS,
    ) -> list[Any]:
        return await self._reference.capture_equity_snapshots(
            symbols,
            snapshot_wait_seconds=snapshot_wait_seconds,
            lease_ttl_seconds=lease_ttl_seconds,
        )

    async def cancel_equity_tickers(self, tickers: Sequence[Any]) -> int:
        return await self._reference.cancel_equity_tickers(tickers)

    # ------------------------------------------------------------------
    # Order management — delegated to order client
    # ------------------------------------------------------------------

    async def place_order(self, request: PlaceOrderRequest) -> OrderResponse:
        """Submit a new order to IBKR."""
        return await self._order_client.place_order(request)

    async def cancel_order(self, account_id: str, order_id: int) -> CancelOrderResponse:
        """Cancel an existing order."""
        return await self._order_client.cancel_order(account_id, order_id)

    async def modify_order(
        self,
        account_id: str,
        order_id: int,
        modifications: ModifyOrderRequest,
    ) -> OrderResponse:
        """Modify an existing order."""
        return await self._order_client.modify_order(account_id, order_id, modifications)

    async def load_open_orders(self) -> list[OpenOrder]:
        """Load all currently open (working) orders."""
        return await self._order_client.load_open_orders()

    async def load_executions(self, request: ExecutionRequest) -> ExecutionResponse:
        """Load execution/fill details with optional filtering."""
        return await self._order_client.load_executions(request)

    async def preview_order(self, request: PlaceOrderRequest) -> WhatIfOrderResponse:
        """Pre-trade margin & commission preview (what-if)."""
        return await self._order_client.preview_order(request)

    async def load_completed_orders(self) -> list[CompletedOrder]:
        """Load completed (filled/cancelled) order history."""
        return await self._order_client.load_completed_orders()

    async def get_cached_order(self, order_uuid: str) -> CachedOrderLookup:
        """Look up a cached order envelope by UUID."""
        return await self._order_client.get_cached_order(order_uuid)

    async def list_cached_orders(self) -> list[OrderEnvelope]:
        """List all cached order envelopes from Redis."""
        return await self._order_client.list_cached_orders()

    # ------------------------------------------------------------------
    # IBKR event handlers — delegated to connection manager
    # ------------------------------------------------------------------

    _DATA_FARM_CODES: ClassVar[frozenset[int]] = frozenset({
        2103, 2104, 2105, 2106, 2107, 2108, 2158,
    })

    def _on_ibkr_error(self, req_id: int, error_code: int, error_string: str, contract: Any = None) -> None:
        self._connection._on_ibkr_error(req_id, error_code, error_string, contract)

    def _on_ibkr_disconnected(self, *_: object) -> None:
        self._connection._on_ibkr_disconnected(*_)

    def _on_ibkr_timeout(self, *_: object) -> None:
        self._connection._on_ibkr_timeout(*_)

    async def _reconnect(self) -> None:
        await self._connection._reconnect()

    # ------------------------------------------------------------------
    # Market data extensions — delegated to marketdata_ext client
    # ------------------------------------------------------------------

    async def start_tick_by_tick(
        self,
        symbol: str,
        sec_type: str = "STK",
        exchange: str = "SMART",
        currency: str = "USD",
        tick_type: "TickType | None" = None,
        max_ticks: int = 10_000,
        on_tick: "Any | None" = None,
    ) -> Any:
        from src.feeds.tick_data import TickType
        tt = tick_type or TickType.ALL_LAST
        if isinstance(tt, str):
            tt = TickType(tt)
        return await self._marketdata_ext.start_tick_by_tick(
            symbol, sec_type, exchange, currency, tt, max_ticks, on_tick,
        )

    async def stop_tick_by_tick(
        self,
        symbol: str,
        sec_type: str = "STK",
        exchange: str = "SMART",
    ) -> None:
        return await self._marketdata_ext.stop_tick_by_tick(symbol, sec_type, exchange)

    def get_latest_ticks(
        self,
        symbol: str,
        sec_type: str = "STK",
        exchange: str = "SMART",
        n: int = 100,
    ) -> "list":
        return self._marketdata_ext.get_latest_ticks(symbol, sec_type, exchange, n)

    async def load_market_depth_snapshot(
        self,
        spec: ContractSpec,
        *,
        num_rows: int = 5,
        is_smart_depth: bool = False,
        snapshot_wait_seconds: float = 1.5,
        request_timeout_seconds: float = constants.DEFAULT_IBKR_MARKET_DEPTH_REQUEST_TIMEOUT_SECONDS,
        lease_wait_seconds: float = constants.DEFAULT_IBKR_MARKET_DEPTH_LEASE_WAIT_SECONDS,
    ) -> "Any":
        request_timeout_seconds = max(0.05, float(request_timeout_seconds))
        deadline = monotonic_time.monotonic() + request_timeout_seconds
        try:
            contract = await asyncio.wait_for(self.qualify_contract(spec), timeout=request_timeout_seconds)
        except TimeoutError as exc:
            raise IBKRMarketDataUnavailableError(
                f"market depth contract qualification timed out for {spec.symbol} after {request_timeout_seconds:.2f}s"
            ) from exc
        remaining_timeout = max(0.05, deadline - monotonic_time.monotonic())
        return await self._marketdata_ext.load_market_depth_snapshot(
            contract=contract,
            spec=spec,
            num_rows=num_rows,
            is_smart_depth=is_smart_depth,
            snapshot_wait_seconds=snapshot_wait_seconds,
            request_timeout_seconds=remaining_timeout,
            lease_wait_seconds=lease_wait_seconds,
        )

    async def load_historical_ticks(self, request: "Any") -> "Any":
        return await self._marketdata_ext.load_historical_ticks(request)

    async def load_histogram_data(self, request: "Any") -> "Any":
        return await self._marketdata_ext.load_histogram_data(request)

    async def load_market_rule(self, price_magnitude: int) -> "Any":
        return await self._marketdata_ext.load_market_rule(price_magnitude)

    async def load_smart_components(self, exchange: str) -> "list":
        return await self._marketdata_ext.load_smart_components(exchange)

    async def load_head_timestamp(self, request: "Any") -> "Any":
        return await self._marketdata_ext.load_head_timestamp(request)

    async def calculate_iv(self, contract: Any, option_price: float, under_price: float) -> float:
        return await self._marketdata_ext.calculate_iv(contract, option_price, under_price)

    async def calculate_option_price(self, contract: Any, volatility: float, under_price: float) -> float:
        return await self._marketdata_ext.calculate_option_price(contract, volatility, under_price)

    async def search_matching_symbols(self, pattern: str) -> "list":
        return await self._marketdata_ext.search_matching_symbols(pattern)

    # ------------------------------------------------------------------
    # Histogram, realtime bars, depth exchanges, market data type — delegated to marketdata_ext
    # ------------------------------------------------------------------

    async def request_histogram(
        self,
        symbol: str,
        asset_class: str = "EQUITY",
        exchange: str = "SMART",
        currency: str = "USD",
        use_rth: bool = True,
        time_period: str = "1 day",
    ) -> list[dict[str, Any]]:
        """Request histogram data for a contract."""
        return await self._marketdata_ext.request_histogram(
            symbol, asset_class, exchange, currency, use_rth, time_period,
        )

    async def subscribe_realtime_bars(
        self,
        symbol: str,
        asset_class: str = "EQUITY",
        exchange: str = "SMART",
        currency: str = "USD",
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ):
        """Subscribe to real-time 5-second bars."""
        return self._marketdata_ext.subscribe_realtime_bars(
            symbol, asset_class, exchange, currency, what_to_show, use_rth,
        )

    async def get_depth_exchanges(self) -> list[dict[str, Any]]:
        """Return exchanges supporting L2 market depth."""
        return await self._marketdata_ext.get_depth_exchanges()

    async def set_market_data_type(self, market_data_type: int) -> dict[str, Any]:
        """Switch the IBKR market data type."""
        return await self._marketdata_ext.set_market_data_type(market_data_type)

    # ------------------------------------------------------------------
    # Scanner — delegated to marketdata ext
    # ------------------------------------------------------------------

    async def get_scanner_parameters(self) -> dict[str, Any]:
        """Fetch available scanner parameters from IBKR."""
        return await self._marketdata_ext.get_scanner_parameters()

    async def scan_market(
        self,
        *,
        instrument: str = "STK",
        location: str = "STK.US",
        scan_code: str = "TOP_PERC_GAIN",
        above_price: float | None = None,
        below_price: float | None = None,
        above_volume: int | None = None,
        market_cap_above: float | None = None,
        market_cap_below: float | None = None,
        max_results: int = 50,
    ) -> list[dict[str, Any]]:
        """Run an IBKR market scanner."""
        return await self._marketdata_ext.scan_market(
            instrument=instrument,
            location=location,
            scan_code=scan_code,
            above_price=above_price,
            below_price=below_price,
            above_volume=above_volume,
            market_cap_above=market_cap_above,
            market_cap_below=market_cap_below,
            max_results=max_results,
        )

    # ------------------------------------------------------------------
    # News bulletins — delegated to marketdata ext
    # ------------------------------------------------------------------

    async def get_news_bulletins(self, *, all_messages: bool = True) -> list[dict[str, Any]]:
        """Get IBKR system news bulletins."""
        return await self._marketdata_ext.get_news_bulletins(all_messages=all_messages)

    # ------------------------------------------------------------------
    # Server time — delegated to connection manager
    # ------------------------------------------------------------------

    async def get_server_time(self) -> dict[str, Any]:
        """Request the current IBKR server time."""
        return await self._connection.get_server_time()

    # ------------------------------------------------------------------
    # Additional order management — delegated to order client
    # ------------------------------------------------------------------

    async def cancel_all_orders(self) -> dict[str, Any]:
        """Cancel all open orders globally."""
        return await self._order_client.cancel_all_orders()

    async def get_all_open_orders(self) -> "list":
        """Load all open orders across all accounts."""
        return await self._order_client.get_all_open_orders()

    async def exercise_option(
        self,
        symbol: str,
        right: str,
        strike: float,
        expiry: str,
        exercise_action: int,
        quantity: int,
        account: str,
        exchange: str = "SMART",
        currency: str = "USD",
        override: bool = False,
        manual_order_time: str = "",
    ) -> dict[str, Any]:
        """Exercise or lapse an option position."""
        return await self._order_client.exercise_option(
            symbol, right, strike, expiry, exercise_action, quantity,
            account, exchange, currency, override, manual_order_time,
        )
