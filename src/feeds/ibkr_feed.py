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
from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest

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


def _historical_same_contract_key(request: OHLCVRequest) -> tuple[Any, ...]:
    return (
        request.symbol,
        request.asset_class,
        request.exchange,
        request.what_to_show.upper(),
    )
