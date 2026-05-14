from __future__ import annotations

import asyncio
import logging
import time as monotonic_time
from collections import defaultdict, deque
from collections.abc import Sequence
from datetime import date, datetime, time, timezone
from typing import Any

from src.config import config_constant as constants
from src.feeds.contracts import ContractSpec, OptionChain, OptionChainRequest, build_ibkr_contract
from src.feeds.fundamental_data import (
    FundamentalDataReport,
    FundamentalDataRequest,
    WSHEventDataReport,
    WSHEventDataRequest,
    WSHMetadataReport,
)
from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest
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
    OptionAnalyticsRequest,
    OptionAnalyticsSnapshot,
    build_ibkr_option_contract,
    normalize_option_analytics_from_ticker,
)

logger = logging.getLogger(__name__)


class IBKRFeedClient:
    """Async IBKR market data adapter backed by ib_insync."""

    def __init__(
        self,
        host: str = constants.DEFAULT_IBKR_HOST,
        port: int = constants.DEFAULT_IBKR_PORT,
        client_id: int = constants.DEFAULT_IBKR_CLIENT_ID,
        *,
        retry_attempts: int = 3,
        retry_base_delay_seconds: float = 0.5,
        pacing_guard: "IBKRHistoricalPacingGuard | None" = None,
    ) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self.retry_attempts = retry_attempts
        self.retry_base_delay_seconds = retry_base_delay_seconds
        self._ib: Any | None = None
        self._pacing_guard = pacing_guard or IBKRHistoricalPacingGuard()
        self._wsh_metadata_loaded = False

    async def connect(self) -> None:
        if self._ib is not None and self._ib.isConnected():
            return

        try:
            from ib_insync import IB
        except ImportError as exc:
            raise RuntimeError("ib_insync is required to connect to IBKR") from exc

        self._ib = IB()
        await self._with_retry(
            lambda: self._ib.connectAsync(self.host, self.port, clientId=self.client_id),
            operation="connect",
        )

    async def disconnect(self) -> None:
        if self._ib is not None and self._ib.isConnected():
            self._ib.disconnect()

    async def __aenter__(self) -> "IBKRFeedClient":
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.disconnect()

    async def qualify_contract(self, spec: ContractSpec) -> Any:
        await self._ensure_connected()
        contract = build_ibkr_contract(spec)
        qualified = await self._with_retry(
            lambda: self._ib.qualifyContractsAsync(contract),
            operation=f"qualify_contract:{spec.symbol}",
        )
        if not qualified:
            raise RuntimeError(f"IBKR could not qualify contract for {spec.symbol}")
        return qualified[0]

    async def load_historical_ohlcv(self, request: OHLCVRequest) -> list[OHLCVBar]:
        await self._ensure_connected()
        contract = await self.qualify_contract(ContractSpec.from_ohlcv_request(request))
        end_datetime = _format_ibkr_end_datetime(request.end_datetime)

        await self._pacing_guard.acquire(request)
        try:
            bars = await self._with_retry(
                lambda: self._ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime=end_datetime,
                    durationStr=request.duration,
                    barSizeSetting=request.bar_size,
                    whatToShow=request.what_to_show,
                    useRTH=request.use_rth,
                    formatDate=2,
                    keepUpToDate=False,
                ),
                operation=f"historical_ohlcv:{request.symbol}:{request.bar_size}",
            )
        finally:
            self._pacing_guard.release()
        return normalize_ibkr_bars(bars, request)

    async def load_option_chains(self, request: OptionChainRequest) -> list[OptionChain]:
        """Load option chain metadata for stock or index underlyings via reqSecDefOptParams."""

        await self._ensure_connected()
        underlying_contract = await self.qualify_contract(request.to_contract_spec())
        underlying_con_id = int(getattr(underlying_contract, "conId"))
        chains = await self._with_retry(
            lambda: self._ib.reqSecDefOptParamsAsync(
                request.symbol,
                "",
                _ibkr_sec_type_for_option_underlying(request.asset_class),
                underlying_con_id,
            ),
            operation=f"option_chain:{request.symbol}",
        )
        return normalize_ibkr_option_chains(chains, request, underlying_con_id)

    async def load_option_analytics(self, request: OptionAnalyticsRequest) -> OptionAnalyticsSnapshot:
        """Load a market-data snapshot with option Greeks, volume, OI, and volatility fields."""

        await self._ensure_connected()
        contract = build_ibkr_option_contract(request.contract)
        qualified = await self._with_retry(
            lambda: self._ib.qualifyContractsAsync(contract),
            operation=f"qualify_option:{request.contract.underlying_symbol}:{request.contract.expiry}",
        )
        if qualified:
            contract = qualified[0]
        ticker = self._ib.reqMktData(
            contract,
            genericTickList=request.generic_tick_list,
            snapshot=True,
            regulatorySnapshot=request.regulatory_snapshot,
            mktDataOptions=[],
        )
        await asyncio.sleep(request.snapshot_wait_seconds)
        return normalize_option_analytics_from_ticker(ticker, request.contract)

    async def load_bond_yield_history(self, request: BondYieldHistoryRequest) -> list[BondYieldBar]:
        """Load historical bond yield bars for bid, ask, and/or last yield fields."""

        await self._ensure_connected()
        contract = await self.qualify_contract(request.bond.to_contract_spec())
        normalized: list[BondYieldBar] = []
        for yield_field in request.yield_fields:
            pacing_request = request.to_pacing_request(yield_field)
            await self._pacing_guard.acquire(pacing_request)
            try:
                bars = await self._with_retry(
                    lambda: self._ib.reqHistoricalDataAsync(
                        contract,
                        endDateTime=_format_ibkr_end_datetime(request.end_datetime),
                        durationStr=request.duration,
                        barSizeSetting=request.bar_size,
                        whatToShow=yield_field.value,
                        useRTH=request.use_rth,
                        formatDate=2,
                        keepUpToDate=False,
                    ),
                    operation=f"bond_yield:{request.bond.symbol}:{yield_field.value}",
                )
            finally:
                self._pacing_guard.release()
            normalized.extend(normalize_ibkr_bond_yield_bars(bars, request, yield_field))
        return normalized

    async def load_fundamental_data(self, request: FundamentalDataRequest) -> FundamentalDataReport:
        """Load an IBKR fundamental report as raw XML."""

        await self._ensure_connected()
        contract = await self.qualify_contract(request.to_contract_spec())
        raw_xml = await self._with_retry(
            lambda: self._ib.reqFundamentalDataAsync(contract, request.report_type.value, []),
            operation=f"fundamental_data:{request.symbol}:{request.report_type.value}",
        )
        return FundamentalDataReport(
            symbol=request.symbol,
            asset_class=request.asset_class,
            con_id=getattr(contract, "conId", None),
            report_type=request.report_type,
            raw_xml=raw_xml,
            source=request.source,
            metadata=request.metadata,
        )

    async def load_wsh_metadata(self) -> WSHMetadataReport:
        """Load Wall Street Horizon metadata as raw JSON."""

        await self._ensure_connected()
        raw_json = await self._with_retry(
            lambda: self._ib.getWshMetaDataAsync(),
            operation="wsh_metadata",
        )
        self._wsh_metadata_loaded = True
        return WSHMetadataReport.from_raw_json(raw_json)

    async def load_wsh_event_data(
        self,
        request: WSHEventDataRequest,
        *,
        ensure_metadata: bool = True,
    ) -> WSHEventDataReport:
        """Load Wall Street Horizon event data as raw JSON."""

        await self._ensure_connected()
        if ensure_metadata and not self._wsh_metadata_loaded:
            await self.load_wsh_metadata()
        request_filter_json = request.to_filter_json()
        wsh_event_data = _build_wsh_event_data(request_filter_json)
        raw_json = await self._with_retry(
            lambda: self._ib.getWshEventDataAsync(wsh_event_data),
            operation="wsh_event_data",
        )
        return WSHEventDataReport.from_raw_json(
            raw_json=raw_json,
            request_filter_json=request_filter_json,
        )

    async def load_news_providers(self) -> list[NewsProvider]:
        """Load API-entitled IBKR news providers."""

        await self._ensure_connected()
        providers = await self._with_retry(
            lambda: self._ib.reqNewsProvidersAsync(),
            operation="news_providers",
        )
        return normalize_news_providers(providers)

    async def load_historical_news(self, request: HistoricalNewsRequest) -> list[HistoricalNewsHeadline]:
        """Load historical IBKR news headlines for a contract id."""

        await self._ensure_connected()
        headlines = await self._with_retry(
            lambda: self._ib.reqHistoricalNewsAsync(
                request.con_id,
                request.provider_codes_param,
                format_historical_news_datetime(request.start_datetime),
                format_historical_news_datetime(request.end_datetime),
                request.total_results,
                [],
            ),
            operation=f"historical_news:{request.con_id}:{request.provider_codes_param}",
        )
        return normalize_historical_news(headlines)

    async def load_news_article(self, request: NewsArticleRequest) -> NewsArticle:
        """Load the body of an IBKR news article by provider and article id."""

        await self._ensure_connected()
        article = await self._with_retry(
            lambda: self._ib.reqNewsArticleAsync(request.provider_code, request.article_id, []),
            operation=f"news_article:{request.provider_code}:{request.article_id}",
        )
        return normalize_news_article(article, request)

    async def load_account_summary(self, account: str = "") -> list[AccountSummaryDTO]:
        """Load account summary values grouped by account."""

        await self._ensure_connected()
        values = await self._with_retry(
            lambda: self._ib.accountSummaryAsync(account),
            operation=f"account_summary:{account or 'all'}",
        )
        return group_account_summary(normalize_account_values(values))

    async def load_live_positions(self) -> list[LivePositionDTO]:
        """Load current live positions."""

        await self._ensure_connected()
        positions = await self._with_retry(
            lambda: self._ib.reqPositionsAsync(),
            operation="positions",
        )
        return normalize_positions(positions)

    async def load_portfolio_items(self, account: str = "") -> list[PortfolioItemDTO]:
        """Expose current portfolio items from ib_insync's local account cache."""

        await self._ensure_connected()
        return normalize_portfolio_items(self._ib.portfolio(account))

    async def subscribe_account_pnl(self, account: str, model_code: str = "") -> object:
        """Start a live account PnL subscription and return the ib_insync PnL object."""

        await self._ensure_connected()
        return self._ib.reqPnL(account, model_code)

    async def subscribe_position_pnl(self, account: str, con_id: int, model_code: str = "") -> object:
        """Start a live position PnL subscription and return the ib_insync PnLSingle object."""

        await self._ensure_connected()
        return self._ib.reqPnLSingle(account, model_code, con_id)

    async def load_account_pnl_snapshot(
        self,
        account: str,
        model_code: str = "",
        *,
        wait_seconds: float = 1.2,
    ) -> AccountPnLDTO:
        """Open a short-lived account PnL subscription and return the latest values."""

        subscription = await self.subscribe_account_pnl(account, model_code)
        try:
            if wait_seconds:
                await asyncio.sleep(wait_seconds)
            return self.account_pnl_snapshot(subscription, account, model_code)
        finally:
            cancel = getattr(self._ib, "cancelPnL", None)
            if cancel is not None:
                cancel(account, model_code)

    async def load_position_pnl_snapshot(
        self,
        account: str,
        con_id: int,
        model_code: str = "",
        *,
        wait_seconds: float = 1.2,
    ) -> PositionPnLDTO:
        """Open a short-lived position PnL subscription and return the latest values."""

        subscription = await self.subscribe_position_pnl(account, con_id, model_code)
        try:
            if wait_seconds:
                await asyncio.sleep(wait_seconds)
            return self.position_pnl_snapshot(subscription, account, con_id, model_code)
        finally:
            cancel = getattr(self._ib, "cancelPnLSingle", None)
            if cancel is not None:
                cancel(account, model_code, con_id)

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

    async def _ensure_connected(self) -> None:
        if self._ib is None or not self._ib.isConnected():
            await self.connect()

    async def _with_retry(self, call: Any, *, operation: str) -> Any:
        last_error: BaseException | None = None
        for attempt in range(1, self.retry_attempts + 1):
            try:
                return await call()
            except Exception as exc:  # pragma: no cover - exercised with live gateways.
                last_error = exc
                if attempt >= self.retry_attempts:
                    break
                delay = self.retry_base_delay_seconds * (2 ** (attempt - 1))
                logger.warning("IBKR %s failed on attempt %s; retrying in %.2fs", operation, attempt, delay)
                await asyncio.sleep(delay)
        raise RuntimeError(f"IBKR operation failed after retries: {operation}") from last_error


class IBKRHistoricalPacingGuard:
    """Conservative in-process pacing guard for IBKR historical data requests."""

    def __init__(
        self,
        *,
        max_requests_per_window: int = constants.IBKR_HISTORICAL_MAX_REQUESTS_PER_WINDOW,
        request_window_seconds: float = constants.IBKR_HISTORICAL_REQUEST_WINDOW_SECONDS,
        identical_cooldown_seconds: float = constants.IBKR_HISTORICAL_IDENTICAL_REQUEST_COOLDOWN_SECONDS,
        same_contract_window_seconds: float = constants.IBKR_HISTORICAL_SAME_CONTRACT_WINDOW_SECONDS,
        same_contract_max_requests: int = constants.IBKR_HISTORICAL_SAME_CONTRACT_MAX_REQUESTS,
        max_concurrent_requests: int = constants.IBKR_CONSERVATIVE_HISTORICAL_CONCURRENCY,
    ) -> None:
        self.max_requests_per_window = max_requests_per_window
        self.request_window_seconds = request_window_seconds
        self.identical_cooldown_seconds = identical_cooldown_seconds
        self.same_contract_window_seconds = same_contract_window_seconds
        self.same_contract_max_requests = same_contract_max_requests
        self._request_times: deque[float] = deque()
        self._identical_last_seen: dict[tuple[Any, ...], float] = {}
        self._same_contract_times: dict[tuple[Any, ...], deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()
        self._concurrency = asyncio.Semaphore(max_concurrent_requests)

    async def acquire(self, request: OHLCVRequest) -> None:
        await self._concurrency.acquire()
        try:
            await self._wait_for_slot(request)
        except Exception:
            self._concurrency.release()
            raise

    def release(self) -> None:
        self._concurrency.release()

    async def _wait_for_slot(self, request: OHLCVRequest) -> None:
        weight = 2 if request.what_to_show.upper() == "BID_ASK" else 1
        identical_key = _historical_identical_key(request)
        same_contract_key = _historical_same_contract_key(request)

        while True:
            async with self._lock:
                now = monotonic_time.monotonic()
                self._prune(now)
                wait_seconds = self._required_wait_seconds(now, identical_key, same_contract_key, weight)
                if wait_seconds <= 0:
                    for _ in range(weight):
                        self._request_times.append(now)
                    self._identical_last_seen[identical_key] = now
                    self._same_contract_times[same_contract_key].append(now)
                    return
            await asyncio.sleep(wait_seconds)

    def _prune(self, now: float) -> None:
        while self._request_times and now - self._request_times[0] >= self.request_window_seconds:
            self._request_times.popleft()
        for key, timestamps in list(self._same_contract_times.items()):
            while timestamps and now - timestamps[0] >= self.same_contract_window_seconds:
                timestamps.popleft()
            if not timestamps:
                del self._same_contract_times[key]
        for key, timestamp in list(self._identical_last_seen.items()):
            if now - timestamp >= self.identical_cooldown_seconds:
                del self._identical_last_seen[key]

    def _required_wait_seconds(
        self,
        now: float,
        identical_key: tuple[Any, ...],
        same_contract_key: tuple[Any, ...],
        weight: int,
    ) -> float:
        waits: list[float] = []

        if len(self._request_times) + weight > self.max_requests_per_window:
            waits.append(self.request_window_seconds - (now - self._request_times[0]))

        identical_seen = self._identical_last_seen.get(identical_key)
        if identical_seen is not None:
            waits.append(self.identical_cooldown_seconds - (now - identical_seen))

        same_contract_times = self._same_contract_times.get(same_contract_key)
        if same_contract_times and len(same_contract_times) >= self.same_contract_max_requests:
            waits.append(self.same_contract_window_seconds - (now - same_contract_times[0]))

        return max([0.0, *waits])


def normalize_ibkr_bars(bars: Sequence[Any], request: OHLCVRequest) -> list[OHLCVBar]:
    normalized: list[OHLCVBar] = []
    for bar in bars:
        normalized.append(
            OHLCVBar(
                symbol=request.symbol,
                asset_class=request.asset_class,
                exchange=request.exchange,
                currency=request.currency,
                timestamp=_parse_ibkr_timestamp(getattr(bar, "date")),
                open=float(getattr(bar, "open")),
                high=float(getattr(bar, "high")),
                low=float(getattr(bar, "low")),
                close=float(getattr(bar, "close")),
                volume=float(getattr(bar, "volume", 0) or 0),
                bar_size=request.bar_size,
                source=request.source,
                metadata={
                    **request.metadata,
                    "what_to_show": request.what_to_show,
                    "use_rth": request.use_rth,
                },
            )
        )
    return normalized


def normalize_ibkr_option_chains(
    chains: Sequence[Any],
    request: OptionChainRequest,
    underlying_con_id: int,
) -> list[OptionChain]:
    normalized: list[OptionChain] = []
    for chain in chains:
        expirations = tuple(getattr(chain, "expirations", ()) or ())
        strikes = tuple(getattr(chain, "strikes", ()) or ())
        if not expirations or not strikes:
            continue
        normalized.append(
            OptionChain(
                underlying_symbol=request.symbol,
                underlying_asset_class=request.asset_class,
                underlying_con_id=underlying_con_id,
                exchange=getattr(chain, "exchange", ""),
                trading_class=getattr(chain, "tradingClass", ""),
                multiplier=str(getattr(chain, "multiplier", "")),
                expirations=expirations,
                strikes=strikes,
            )
        )
    return normalized


def _format_ibkr_end_datetime(value: datetime | None) -> str:
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y%m%d %H:%M:%S UTC")


def _parse_ibkr_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    elif isinstance(value, str):
        parsed = _parse_timestamp_string(value)
    else:
        raise TypeError(f"unsupported IBKR timestamp type: {type(value)!r}")

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_timestamp_string(value: str) -> datetime:
    text = value.strip().replace("Z", "+00:00")
    for fmt in ("%Y%m%d %H:%M:%S %Z", "%Y%m%d %H:%M:%S", "%Y%m%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt.endswith("%Z") and text.endswith("UTC"):
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            continue
    return datetime.fromisoformat(text)


def _historical_identical_key(request: OHLCVRequest) -> tuple[Any, ...]:
    return (
        request.symbol,
        request.asset_class,
        request.exchange,
        request.currency,
        request.end_datetime,
        request.duration,
        request.bar_size,
        request.what_to_show.upper(),
        request.use_rth,
    )


def _ibkr_sec_type_for_option_underlying(asset_class: AssetClass) -> str:
    if asset_class is AssetClass.EQUITY:
        return "STK"
    if asset_class is AssetClass.INDEX:
        return "IND"
    raise ValueError(f"unsupported option underlying asset class: {asset_class}")


def _build_wsh_event_data(filter_json: str) -> Any:
    try:
        from ib_insync import WshEventData
    except ImportError as exc:
        raise RuntimeError("ib_insync is required for WSH event data requests") from exc
    return WshEventData(filter=filter_json)


def _historical_same_contract_key(request: OHLCVRequest) -> tuple[Any, ...]:
    return (
        request.symbol,
        request.asset_class,
        request.exchange,
        request.what_to_show.upper(),
    )
