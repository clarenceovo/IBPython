"""Market data extensions — tick-by-tick, historical ticks, market rules, IV/price calc, symbol search.

Sub-client following the pattern of ``ibkr_historical.py`` / ``ibkr_options_feed.py``.
Composes ``IBKRConnectionManager`` for connection lifecycle, retry, and pacing.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time as monotonic_time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Callable

from src.feeds.contracts import build_ibkr_contract, ContractSpec
from src.feeds.exceptions import IBKRMarketDataLeaseTimeoutError, IBKRMarketDataUnavailableError
from src.feeds.ibkr_connection import (
    IBKRConnectionManager,
    _contract_int,
    _contract_text,
    acquire_market_data_line,
    wait_for_ibkr_request,
)
from src.feeds.models import AssetClass
from src.feeds.tick_data import (
    HeadTimestampRequest,
    HistoricalTickRequest,
    HistoricalTickResponse,
    IVCalcRequest,
    MarketDepthLevel,
    MarketDepthSnapshot,
    MarketRule,
    OptionPriceCalcRequest,
    PriceIncrement,
    SmartComponent,
    SymbolDescription,
    TickByTickData,
    TickType,
)
from src.transport.metrics import metrics

logger = logging.getLogger(__name__)


def _build_contract(
    symbol: str,
    sec_type: str = "STK",
    exchange: str = "SMART",
    currency: str = "USD",
    **extra: Any,
) -> Any:
    """Build an IBKR contract via the standard ContractSpec → build_ibkr_contract path."""
    spec = ContractSpec(
        symbol=symbol,
        asset_class=_asset_class_for_sec_type(sec_type),
        exchange=exchange,
        currency=currency,
        **{k: v for k, v in extra.items() if v is not None},
    )
    return build_ibkr_contract(spec)


def _asset_class_for_sec_type(sec_type: str) -> AssetClass:
    """Map IBKR secType string to our AssetClass enum."""
    mapping = {
        "STK": AssetClass.EQUITY,
        "OPT": AssetClass.OPTION,
        "FOP": AssetClass.OPTION,
        "FUT": AssetClass.FUTURE,
        "CASH": AssetClass.FX,
        "BOND": AssetClass.BOND,
        "IND": AssetClass.INDEX,
    }
    return mapping.get(sec_type.upper(), AssetClass.EQUITY)


def _asset_class_to_sec_type(asset_class: str) -> str:
    """Map an asset class string (or AssetClass enum value) to an IBKR secType."""
    mapping = {
        "EQUITY": "STK",
        "equity": "STK",
        AssetClass.EQUITY: "STK",
        "OPTION": "OPT",
        "option": "OPT",
        AssetClass.OPTION: "OPT",
        "FUTURE": "FUT",
        "future": "FUT",
        AssetClass.FUTURE: "FUT",
        "FX": "CASH",
        "fx": "CASH",
        AssetClass.FX: "CASH",
        "BOND": "BOND",
        "bond": "BOND",
        AssetClass.BOND: "BOND",
        "INDEX": "IND",
        "index": "IND",
        AssetClass.INDEX: "IND",
        "CRYPTO": "CRYPTO",
        "crypto": "CRYPTO",
        AssetClass.CRYPTO: "CRYPTO",
    }
    return mapping.get(asset_class, mapping.get(str(asset_class).upper(), "STK"))


def _parse_tick_timestamp(value: Any) -> datetime:
    """Parse IBKR tick timestamp to UTC datetime."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        parsed = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            parsed = datetime.strptime(text, "%Y%m%d %H:%M:%S").replace(tzinfo=timezone.utc)
    else:
        parsed = datetime.now(tz=timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_ibkr_tick(ibkr_tick: Any, tick_type: TickType) -> TickByTickData:
    """Convert an ib_insync HistoricalTick / HistoricalTickBidAsk to our model."""
    if tick_type == TickType.BID_ASK:
        return TickByTickData(
            timestamp=_parse_tick_timestamp(getattr(ibkr_tick, "time", None)),
            tick_type=tick_type,
            price=None,
            size=None,
            bid=getattr(ibkr_tick, "priceBid", None) or getattr(ibkr_tick, "bid", None),
            ask=getattr(ibkr_tick, "priceAsk", None) or getattr(ibkr_tick, "ask", None),
            size_bid=getattr(ibkr_tick, "sizeBid", None),
            size_ask=getattr(ibkr_tick, "sizeAsk", None),
            exchange=getattr(ibkr_tick, "exchange", None),
            special_conditions=getattr(ibkr_tick, "specialConditions", None),
        )
    return TickByTickData(
        timestamp=_parse_tick_timestamp(getattr(ibkr_tick, "time", None)),
        tick_type=tick_type,
        price=getattr(ibkr_tick, "price", None),
        size=getattr(ibkr_tick, "size", None),
        bid=None,
        ask=None,
        exchange=getattr(ibkr_tick, "exchange", None),
        special_conditions=getattr(ibkr_tick, "specialConditions", None),
    )


def _what_to_show_to_tick_type(what_to_show: str) -> TickType:
    """Map historical tick whatToShow to the closest TickType."""
    mapping = {
        "TRADES": TickType.ALL_LAST,
        "BID_ASK": TickType.BID_ASK,
        "MIDPOINT": TickType.MIDPOINT,
    }
    return mapping.get(what_to_show.upper(), TickType.ALL_LAST)


def _finite_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _normalize_dom_levels(levels: list[Any], *, limit: int) -> list[MarketDepthLevel]:
    normalized: list[MarketDepthLevel] = []
    for position, level in enumerate(levels[:limit]):
        price = _finite_float(getattr(level, "price", None))
        size = _finite_float(getattr(level, "size", None))
        if price is None or size is None:
            continue
        normalized.append(
            MarketDepthLevel(
                position=position,
                price=max(price, 0.0),
                size=max(size, 0.0),
                market_maker=getattr(level, "marketMaker", None),
            )
        )
    return normalized


def _market_depth_contract_key(contract: Any, spec: ContractSpec) -> str:
    con_id = _contract_int(contract, "conId")
    if con_id:
        return f"market_depth:conId:{con_id}"
    local_symbol = _contract_text(contract, "localSymbol", "local_symbol")
    if local_symbol:
        return f"market_depth:localSymbol:{local_symbol}"
    sec_type = _contract_text(contract, "secType") or ""
    exchange = _contract_text(contract, "exchange") or spec.exchange
    currency = _contract_text(contract, "currency") or spec.currency
    return f"market_depth:{sec_type}:{spec.symbol}:{exchange}:{currency}"


class IBKRMarketDataExtClient:
    """Extended market data sub-client — tick-by-tick, historical ticks, market rules, IV calc, symbol search.

    Follows the sub-client pattern:
    - Constructor takes ``IBKRConnectionManager``
    - All methods are async
    - Uses ``self._ib`` for the ib_async IB instance
    - Uses ``self._connection`` for retry, pacing, and error checks
    """

    def __init__(self, connection: IBKRConnectionManager) -> None:
        self._connection = connection
        # Tick-by-tick subscription state: key = (symbol, sec_type, exchange) → deque
        self._tick_buffers: dict[tuple[str, str, str], deque[TickByTickData]] = {}
        # Active subscription handles: key = (symbol, sec_type, exchange) → ib_insync ticker handle
        self._tick_subscriptions: dict[tuple[str, str, str], Any] = {}
        self._tick_subscription_leases: dict[tuple[str, str, str], Any] = {}
        # Optional callback per symbol key
        self._on_tick_callbacks: dict[tuple[str, str, str], Callable[[TickByTickData], None] | None] = {}

    @property
    def _ib(self) -> Any:
        return self._connection.ib

    # ------------------------------------------------------------------
    # 1. Tick-by-tick streaming
    # ------------------------------------------------------------------

    async def start_tick_by_tick(
        self,
        symbol: str,
        sec_type: str = "STK",
        exchange: str = "SMART",
        currency: str = "USD",
        tick_type: TickType = TickType.ALL_LAST,
        max_ticks: int = 10_000,
        on_tick: Callable[[TickByTickData], None] | None = None,
    ) -> Any:
        """Subscribe to tick-by-tick trade/quote/midpoint data.

        Stores ticks in a bounded deque per symbol. Returns the subscription handle.
        """
        await self._connection.ensure_connected()
        key = (symbol.upper(), sec_type.upper(), exchange.upper())

        # Cancel existing subscription if any
        if key in self._tick_subscriptions:
            await self.stop_tick_by_tick(symbol, sec_type, exchange)

        contract = _build_contract(symbol, sec_type, exchange, currency)
        lease = await acquire_market_data_line(
            self._connection,
            contract_key=f"tick_by_tick:{sec_type.upper()}:{symbol.upper()}:{exchange.upper()}:{currency.upper()}",
            operation=f"tick_by_tick:{symbol}",
        )

        # Store buffer and callback
        self._tick_buffers[key] = deque(maxlen=max_ticks)
        self._on_tick_callbacks[key] = on_tick

        # Build the callback wrapper
        def _on_tick_by_tick(ticker: Any, tick_type_str: str = tick_type.value) -> None:
            try:
                tick_data = TickByTickData(
                    timestamp=_parse_tick_timestamp(getattr(ticker, "time", None)),
                    tick_type=TickType(tick_type_str),
                    price=getattr(ticker, "price", None),
                    size=getattr(ticker, "size", None),
                    bid=getattr(ticker, "bid", None) if tick_type == TickType.BID_ASK else None,
                    ask=getattr(ticker, "ask", None) if tick_type == TickType.BID_ASK else None,
                    exchange=getattr(ticker, "exchange", None),
                    special_conditions=getattr(ticker, "specialConditions", None),
                )
                buffer = self._tick_buffers.get(key)
                if buffer is not None:
                    buffer.append(tick_data)
                callback = self._on_tick_callbacks.get(key)
                if callback is not None:
                    try:
                        callback(tick_data)
                    except Exception:
                        logger.debug("on_tick callback error for %s", symbol, exc_info=True)
            except Exception:
                logger.debug("error processing tick-by-tick data for %s", symbol, exc_info=True)

        try:
            handle = await self._connection.with_retry(
                lambda: self._ib.reqTickByTickDataAsync(
                    contract,
                    tick_type.value,
                    numberOfTicks=0,
                    ignoreSize=True,
                ),
                operation=f"tick_by_tick:{symbol}",
            )
        except Exception:
            await lease.release()
            raise

        self._tick_subscriptions[key] = handle
        self._tick_subscription_leases[key] = lease
        logger.info(
            "started tick-by-tick subscription: symbol=%s sec_type=%s exchange=%s tick_type=%s max_ticks=%d",
            symbol, sec_type, exchange, tick_type.value, max_ticks,
        )
        return handle

    async def stop_tick_by_tick(
        self,
        symbol: str,
        sec_type: str = "STK",
        exchange: str = "SMART",
    ) -> None:
        """Cancel a tick-by-tick subscription."""
        key = (symbol.upper(), sec_type.upper(), exchange.upper())
        handle = self._tick_subscriptions.pop(key, None)
        if handle is not None and self._ib is not None:
            try:
                await wait_for_ibkr_request(self._connection, operation=f"tick_by_tick_cancel:{symbol}")
                self._ib.cancelTickByTickData(handle)
            except Exception:
                logger.debug("error cancelling tick-by-tick for %s", symbol, exc_info=True)
        lease = self._tick_subscription_leases.pop(key, None)
        if lease is not None:
            await lease.release()
        self._on_tick_callbacks.pop(key, None)
        logger.info("stopped tick-by-tick subscription: symbol=%s sec_type=%s exchange=%s", symbol, sec_type, exchange)

    def get_latest_ticks(
        self,
        symbol: str,
        sec_type: str = "STK",
        exchange: str = "SMART",
        n: int = 100,
    ) -> list[TickByTickData]:
        """Get the latest N ticks from the in-memory buffer."""
        key = (symbol.upper(), sec_type.upper(), exchange.upper())
        buffer = self._tick_buffers.get(key)
        if buffer is None:
            return []
        ticks = list(buffer)
        return ticks[-n:]

    def list_active_subscriptions(self) -> list[dict[str, str]]:
        """List active tick-by-tick subscription keys."""
        return [
            {"symbol": k[0], "sec_type": k[1], "exchange": k[2]}
            for k in self._tick_subscriptions
        ]

    # ------------------------------------------------------------------
    # 2. Market depth / DOM snapshots
    # ------------------------------------------------------------------

    async def load_market_depth_snapshot(
        self,
        *,
        contract: Any,
        spec: ContractSpec,
        num_rows: int = 5,
        is_smart_depth: bool = False,
        snapshot_wait_seconds: float = 1.5,
        request_timeout_seconds: float = 5.0,
        lease_wait_seconds: float = 2.0,
    ) -> MarketDepthSnapshot:
        """Capture a short-lived live DOM snapshot using IBKR ``reqMktDepth``.

        IBKR market depth is a subscription, so this method starts it, waits
        briefly for depth callbacks to populate ``Ticker.domBids/domAsks``, and
        always cancels the subscription before returning.
        """

        start = monotonic_time.monotonic()
        status = "error"
        try:
            snapshot = await asyncio.wait_for(
                self._capture_market_depth_snapshot(
                    contract=contract,
                    spec=spec,
                    num_rows=num_rows,
                    is_smart_depth=is_smart_depth,
                    snapshot_wait_seconds=snapshot_wait_seconds,
                    lease_wait_seconds=lease_wait_seconds,
                ),
                timeout=max(0.05, float(request_timeout_seconds)),
            )
            status = "ok"
            return snapshot
        except TimeoutError as exc:
            status = "timeout"
            raise IBKRMarketDataUnavailableError(
                f"market depth request timed out for {spec.symbol} after {request_timeout_seconds:.2f}s"
            ) from exc
        except IBKRMarketDataLeaseTimeoutError:
            status = "lease_timeout"
            raise
        except IBKRMarketDataUnavailableError:
            status = "unavailable"
            raise
        except Exception as exc:
            status = "error"
            raise IBKRMarketDataUnavailableError(f"market depth unavailable for {spec.symbol}: {exc}") from exc
        finally:
            elapsed = monotonic_time.monotonic() - start
            metrics.market_depth_request_total.inc({"status": status})
            metrics.market_depth_request_duration.observe(elapsed, {"status": status})

    async def _capture_market_depth_snapshot(
        self,
        *,
        contract: Any,
        spec: ContractSpec,
        num_rows: int,
        is_smart_depth: bool,
        snapshot_wait_seconds: float,
        lease_wait_seconds: float,
    ) -> MarketDepthSnapshot:
        await self._connection.ensure_connected()
        bounded_rows = max(1, min(int(num_rows), 5))
        bounded_wait = max(0.05, float(snapshot_wait_seconds))
        operation = f"market_depth:{spec.symbol}"
        try:
            lease = await asyncio.wait_for(
                acquire_market_data_line(
                    self._connection,
                    contract_key=_market_depth_contract_key(contract, spec),
                    operation=operation,
                    ttl_seconds=max(10.0, bounded_wait + 5.0),
                ),
                timeout=max(0.05, float(lease_wait_seconds)),
            )
        except TimeoutError as exc:
            raise IBKRMarketDataLeaseTimeoutError(
                f"market depth line unavailable for {spec.symbol} after {lease_wait_seconds:.2f}s"
            ) from exc

        subscribed = False
        last_error_before = getattr(self._connection, "last_ibkr_error", None)
        try:
            await wait_for_ibkr_request(self._connection, operation=f"{operation}:reqMktDepth")
            ticker = self._ib.reqMktDepth(
                contract,
                numRows=bounded_rows,
                isSmartDepth=is_smart_depth,
                mktDepthOptions=[],
            )
            subscribed = True
            await asyncio.sleep(bounded_wait)
            bids = _normalize_dom_levels(list(getattr(ticker, "domBids", []) or []), limit=bounded_rows)
            asks = _normalize_dom_levels(list(getattr(ticker, "domAsks", []) or []), limit=bounded_rows)
            if not bids and not asks:
                metrics.market_depth_empty_book_total.inc({"asset_class": spec.asset_class.value})
                raise IBKRMarketDataUnavailableError(
                    "No market depth levels were received; check market depth subscriptions, "
                    "venue routing, market hours, and IBKR depth-request limits."
                )
            dom_ticks = list(getattr(ticker, "domTicks", []) or [])
            metadata: dict[str, Any] = {
                "dom_tick_count": len(dom_ticks),
                "qualified_symbol": _contract_text(contract, "symbol"),
            }
            local_symbol = _contract_text(contract, "localSymbol", "local_symbol")
            if local_symbol:
                metadata["qualified_local_symbol"] = local_symbol
            last_error_after = getattr(self._connection, "last_ibkr_error", None)
            if last_error_after is not None and last_error_after != last_error_before:
                code, message = last_error_after
                if code in {200, 309, 316, 317, 354, 10167}:
                    metadata["last_ibkr_error"] = {"code": code, "message": message}
            return MarketDepthSnapshot(
                symbol=spec.symbol,
                asset_class=spec.asset_class.value,
                exchange=_contract_text(contract, "exchange") or spec.exchange,
                currency=_contract_text(contract, "currency") or spec.currency,
                primary_exchange=_contract_text(contract, "primaryExchange", "primaryExch") or spec.primary_exchange,
                con_id=_contract_int(contract, "conId"),
                local_symbol=local_symbol,
                sec_type=_contract_text(contract, "secType"),
                num_rows=bounded_rows,
                is_smart_depth=is_smart_depth,
                snapshot_wait_seconds=bounded_wait,
                received_at=datetime.now(tz=timezone.utc),
                bids=bids,
                asks=asks,
                metadata=metadata,
            )
        finally:
            if subscribed:
                try:
                    await asyncio.wait_for(
                        self._cancel_market_depth(contract, spec.symbol, is_smart_depth=is_smart_depth),
                        timeout=1.0,
                    )
                except Exception:
                    metrics.market_depth_cleanup_failures_total.inc({"operation": "cancelMktDepth"})
                    logger.debug("error cancelling market depth for %s", spec.symbol, exc_info=True)
            try:
                await lease.release()
            except Exception:
                metrics.market_depth_cleanup_failures_total.inc({"operation": "release_lease"})
                logger.debug("error releasing market depth lease for %s", spec.symbol, exc_info=True)

    async def _cancel_market_depth(self, contract: Any, symbol: str, *, is_smart_depth: bool) -> None:
        await wait_for_ibkr_request(self._connection, operation=f"market_depth:{symbol}:cancelMktDepth")
        self._ib.cancelMktDepth(contract, isSmartDepth=is_smart_depth)

    # ------------------------------------------------------------------
    # 3. Historical ticks
    # ------------------------------------------------------------------

    async def load_historical_ticks(self, request: HistoricalTickRequest) -> HistoricalTickResponse:
        """Load historical tick-level data with automatic pagination.

        IBKR returns max 1000 ticks per call. This method paginates
        until the date range is covered or max_ticks is reached.
        """
        await self._connection.ensure_connected()
        logger.info(
            "load_historical_ticks: symbol=%s what_to_show=%s range=%s → %s",
            request.symbol, request.what_to_show,
            request.start_date.isoformat(), request.end_date.isoformat(),
        )
        t0 = monotonic_time.monotonic()

        contract = _build_contract(
            request.symbol, request.sec_type, request.exchange, request.currency,
        )
        tick_type = _what_to_show_to_tick_type(request.what_to_show)

        all_ticks: list[TickByTickData] = []
        start_date = request.start_date
        end_date = request.end_date
        max_per_call = 1000
        truncated = False

        while True:
            try:
                await self._connection.pacing_guard.acquire(
                    # Minimal OHLCVRequest-like object for pacing — only uses symbol/what_to_show
                    _PacingProxy(request.symbol, request.what_to_show),
                )
                try:
                    raw_ticks = await self._connection.with_retry(
                        lambda: self._ib.reqHistoricalTicksAsync(
                            contract,
                            startDateTime=start_date.strftime("%Y%m%d %H:%M:%S UTC"),
                            endDateTime=end_date.strftime("%Y%m%d %H:%M:%S UTC"),
                            numberOfTicks=max_per_call,
                            whatToShow=request.what_to_show,
                            useRth=request.use_rth,
                            ignoreSize=True,
                        ),
                        operation=f"historical_ticks:{request.symbol}",
                    )
                finally:
                    self._connection.pacing_guard.release()
            except AttributeError:
                # ib_insync might not have reqHistoricalTicksAsync; fallback gracefully
                logger.warning("reqHistoricalTicksAsync not available in ib_insync; returning empty")
                break

            if not raw_ticks:
                break

            for tick in raw_ticks:
                all_ticks.append(_normalize_ibkr_tick(tick, tick_type))

            if len(all_ticks) >= request.max_ticks:
                all_ticks = all_ticks[:request.max_ticks]
                truncated = True
                break

            # Move end_date to before the earliest tick returned
            earliest = _parse_tick_timestamp(getattr(raw_ticks[0], "time", None))
            if earliest <= start_date:
                break
            end_date = earliest

        elapsed = monotonic_time.monotonic() - t0
        logger.info(
            "load_historical_ticks: %d ticks for %s in %.2fs (truncated=%s)",
            len(all_ticks), request.symbol, elapsed, truncated,
        )
        return HistoricalTickResponse(
            symbol=request.symbol,
            ticks=all_ticks,
            total_count=len(all_ticks),
            truncated=truncated,
        )

    # ------------------------------------------------------------------
    # 4. Market rules
    # ------------------------------------------------------------------

    async def load_market_rule(self, price_magnitude: int) -> MarketRule:
        """Load exchange-specific market rules (min tick, price increments).

        Args:
            price_magnitude: The price magnitude identifier from contract details.
        """
        await self._connection.ensure_connected()
        logger.info("load_market_rule: price_magnitude=%d", price_magnitude)

        rule = await self._connection.with_retry(
            lambda: self._ib.reqMarketRuleAsync(price_magnitude),
            operation=f"market_rule:{price_magnitude}",
        )

        increments = []
        if rule is not None:
            increments_data = getattr(rule, "priceIncrements", []) or []
            for inc in increments_data:
                increments.append(PriceIncrement(
                    low_edge=float(getattr(inc, "lowEdge", 0)),
                    increment=float(getattr(inc, "increment", 0.01)),
                ))

        return MarketRule(
            price_magnitude=price_magnitude,
            increments=increments,
        )

    # ------------------------------------------------------------------
    # 5. Smart components
    # ------------------------------------------------------------------

    async def load_smart_components(self, exchange: str) -> list[SmartComponent]:
        """Load smart-routing component exchanges for a given exchange."""
        await self._connection.ensure_connected()
        logger.info("load_smart_components: exchange=%s", exchange)

        components = await self._connection.with_retry(
            lambda: self._ib.reqSmartComponentsAsync(exchange),
            operation=f"smart_components:{exchange}",
        )

        result = []
        if components:
            for comp in components:
                result.append(SmartComponent(
                    exchange=getattr(comp, "exchange", ""),
                    con_id=getattr(comp, "conId", None),
                    description=getattr(comp, "description", None),
                ))
        return result

    # ------------------------------------------------------------------
    # 6. Head timestamp
    # ------------------------------------------------------------------

    async def load_head_timestamp(self, request: HeadTimestampRequest) -> datetime | None:
        """Get the earliest available data date for a contract.

        Returns None if the data is not available.
        """
        await self._connection.ensure_connected()
        logger.info("load_head_timestamp: symbol=%s what_to_show=%s", request.symbol, request.what_to_show)

        contract = _build_contract(
            request.symbol, request.sec_type, request.exchange, request.currency,
        )

        try:
            ts = await self._connection.with_retry(
                lambda: self._ib.reqHeadTimeStampAsync(
                    contract,
                    whatToShow=request.what_to_show,
                    useRTH=request.use_rth,
                    formatDate=2,
                ),
                operation=f"head_timestamp:{request.symbol}",
            )
        except Exception:
            logger.warning("head_timestamp failed for %s; returning None", request.symbol, exc_info=True)
            return None

        if ts is None or ts == "":
            return None

        return _parse_tick_timestamp(ts)

    # ------------------------------------------------------------------
    # 7. Implied volatility calculation
    # ------------------------------------------------------------------

    async def calculate_iv(
        self,
        contract: Any,
        option_price: float,
        under_price: float,
    ) -> float:
        """Calculate implied volatility from the IBKR engine.

        Args:
            contract: A qualified IBKR option contract.
            option_price: The observed option price.
            under_price: The underlying price.

        Returns:
            Implied volatility as a float (e.g. 0.25 = 25%).
        """
        await self._connection.ensure_connected()
        logger.info("calculate_iv: option_price=%.4f under_price=%.4f", option_price, under_price)

        iv = await self._connection.with_retry(
            lambda: self._ib.calculateImpliedVolatilityAsync(
                contract,
                optionPrice=option_price,
                underPrice=under_price,
            ),
            operation="calculate_iv",
        )
        # ib_insync returns (vol, err) tuple or just vol depending on version
        if isinstance(iv, tuple):
            vol = float(iv[0]) if iv[0] is not None else 0.0
        else:
            vol = float(iv) if iv is not None else 0.0
        return vol

    # ------------------------------------------------------------------
    # 8. Option price calculation
    # ------------------------------------------------------------------

    async def calculate_option_price(
        self,
        contract: Any,
        volatility: float,
        under_price: float,
    ) -> float:
        """Calculate option price from the IBKR engine.

        Args:
            contract: A qualified IBKR option contract.
            volatility: Implied volatility (e.g. 0.25 = 25%).
            under_price: The underlying price.

        Returns:
            Option price as a float.
        """
        await self._connection.ensure_connected()
        logger.info("calculate_option_price: volatility=%.4f under_price=%.4f", volatility, under_price)

        price = await self._connection.with_retry(
            lambda: self._ib.calculateOptionPriceAsync(
                contract,
                volatility=volatility,
                underPrice=under_price,
            ),
            operation="calculate_option_price",
        )
        if isinstance(price, tuple):
            opt_price = float(price[0]) if price[0] is not None else 0.0
        else:
            opt_price = float(price) if price is not None else 0.0
        return opt_price

    # ------------------------------------------------------------------
    # 9. Symbol search (fuzzy matching)
    # ------------------------------------------------------------------

    async def search_matching_symbols(self, pattern: str) -> list[SymbolDescription]:
        """Search for matching symbols using IBKR's fuzzy symbol search.

        Args:
            pattern: Search pattern (e.g. "AAPL", "Apple Inc").
        """
        await self._connection.ensure_connected()
        logger.info("search_matching_symbols: pattern=%s", pattern)

        results = await self._connection.with_retry(
            lambda: self._ib.reqMatchingSymbolsAsync(pattern),
            operation=f"symbol_search:{pattern}",
        )

        descriptions = []
        if results:
            for desc in results:
                try:
                    descriptions.append(SymbolDescription(
                        con_id=int(getattr(desc, "conId", 0)),
                        symbol=getattr(desc, "symbol", "") or "",
                        name=getattr(desc, "name", "") or "",
                        sec_type=getattr(desc, "secType", "") or "",
                        exchange=getattr(desc, "primaryExchange", None) or getattr(desc, "exchange", None),
                        listing_exchange=getattr(desc, "listingExchange", None),
                        industry=getattr(desc, "industry", None),
                        category=getattr(desc, "category", None),
                        subcategory=getattr(desc, "subcategory", None),
                    ))
                except Exception:
                    logger.debug("skipping malformed symbol description", exc_info=True)
        return descriptions

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    async def cancel_all_subscriptions(self) -> None:
        """Cancel all active tick-by-tick subscriptions."""
        keys = list(self._tick_subscriptions.keys())
        for key in keys:
            symbol, sec_type, exchange = key
            try:
                await self.stop_tick_by_tick(symbol, sec_type, exchange)
            except Exception:
                logger.debug("error cancelling subscription for %s", symbol, exc_info=True)
        self._tick_buffers.clear()
        self._on_tick_callbacks.clear()

    # ------------------------------------------------------------------
    # 10. Histogram data
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
        """Request histogram data for a contract.

        Returns a list of ``{"price": float, "count": int}`` buckets.
        """
        await self._connection.ensure_connected()
        logger.info(
            "request_histogram: symbol=%s asset_class=%s time_period=%s use_rth=%s",
            symbol, asset_class, time_period, use_rth,
        )

        sec_type = _asset_class_to_sec_type(asset_class)
        contract = _build_contract(symbol, sec_type, exchange, currency)

        result = await self._connection.with_retry(
            lambda: self._ib.reqHistogramDataAsync(
                contract,
                useRTH=use_rth,
                period=time_period,
            ),
            operation=f"histogram:{symbol}",
        )

        buckets: list[dict[str, Any]] = []
        if result:
            for item in result:
                buckets.append({
                    "price": float(getattr(item, "price", 0)),
                    "count": int(getattr(item, "count", 0)),
                })

        logger.info("request_histogram: %d buckets for %s", len(buckets), symbol)
        return buckets

    # ------------------------------------------------------------------
    # 11. Real-time 5-second bars
    # ------------------------------------------------------------------

    async def subscribe_realtime_bars(
        self,
        symbol: str,
        asset_class: str = "EQUITY",
        exchange: str = "SMART",
        currency: str = "USD",
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ):
        """Subscribe to real-time 5-second bars for a contract.

        Yields ``dict`` with keys: time, open, high, low, close, volume, wap, count.
        """
        await self._connection.ensure_connected()
        logger.info(
            "subscribe_realtime_bars: symbol=%s asset_class=%s what_to_show=%s",
            symbol, asset_class, what_to_show,
        )

        sec_type = _asset_class_to_sec_type(asset_class)
        contract = _build_contract(symbol, sec_type, exchange, currency)

        async for bar in await self._connection.with_retry(
            lambda: self._ib.reqRealTimeBarsAsync(
                contract,
                5,
                whatToShow=what_to_show,
                useRTH=use_rth,
            ),
            operation=f"realtime_bars:{symbol}",
        ):
            yield {
                "time": getattr(bar, "time", None),
                "open": float(getattr(bar, "open_", 0) or getattr(bar, "open", 0)),
                "high": float(getattr(bar, "high", 0)),
                "low": float(getattr(bar, "low", 0)),
                "close": float(getattr(bar, "close", 0)),
                "volume": float(getattr(bar, "volume", 0)),
                "wap": float(getattr(bar, "wap", 0)),
                "count": int(getattr(bar, "count", 0)),
            }

    # ------------------------------------------------------------------
    # 12. Market depth exchanges
    # ------------------------------------------------------------------

    async def get_depth_exchanges(self) -> list[dict[str, Any]]:
        """Return exchanges supporting L2 market depth.

        Uses ``reqMktDepthExchangesAsync()``.
        """
        await self._connection.ensure_connected()
        logger.info("get_depth_exchanges: starting")

        result = await self._connection.with_retry(
            lambda: self._ib.reqMktDepthExchangesAsync(),
            operation="depth_exchanges",
        )

        exchanges: list[dict[str, Any]] = []
        if result:
            for item in result:
                exchanges.append({
                    "exchange": getattr(item, "exchange", "") or "",
                    "sec_type": getattr(item, "secType", "") or "",
                    "listing_exchange": getattr(item, "listingExchange", "") or "",
                })

        logger.info("get_depth_exchanges: %d exchanges", len(exchanges))
        return exchanges

    # ------------------------------------------------------------------
    # 13. Market data type switching
    # ------------------------------------------------------------------

    async def set_market_data_type(self, market_data_type: int) -> dict[str, Any]:
        """Switch the IBKR market data type.

        Values: 1=Live, 2=Delayed, 3=Delayed Frozen, 4=Delayed Off.
        """
        await self._connection.ensure_connected()
        logger.info("set_market_data_type: type=%d", market_data_type)

        self._ib.reqMarketDataType(market_data_type)

        return {"market_data_type": market_data_type}


    # ------------------------------------------------------------------
    # 14. Scanner parameters
    # ------------------------------------------------------------------

    async def get_scanner_parameters(self) -> dict[str, Any]:
        """Fetch available scanner parameters from IBKR (instruments, filters, locations).

        Returns an XML document describing all valid scanner parameter values.
        Required before using :meth:`scan_market` to discover valid filter values.
        """
        await self._connection.ensure_connected()
        logger.info("get_scanner_parameters: fetching scanner parameter XML")

        xml = await self._connection.with_retry(
            lambda: self._ib.reqScannerParametersAsync(),
            operation="scanner_parameters",
        )
        return {"xml": xml}

    # ------------------------------------------------------------------
    # 15. Market scanner
    # ------------------------------------------------------------------

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
        """Run an IBKR market scanner with the given parameters.

        Uses IBKR's built-in scanner to find instruments matching criteria.
        Call :meth:`get_scanner_parameters` first to discover valid values for
        *instrument*, *location*, and *scan_code*.

        Returns a list of dicts with keys: symbol, sec_type, exchange, currency,
        con_id, local_symbol.
        """
        await self._connection.ensure_connected()
        logger.info(
            "scan_market: instrument=%s location=%s scan_code=%s max_results=%d",
            instrument, location, scan_code, max_results,
        )

        from ib_insync import ScannerSubscription

        sub = ScannerSubscription()
        sub.instrument = instrument
        sub.locationCode = location
        sub.scanCode = scan_code
        sub.numberOfRows = max_results

        tags: list[tuple[str, str]] = []
        if above_price is not None:
            tags.append(("priceAbove", str(above_price)))
        if below_price is not None:
            tags.append(("priceBelow", str(below_price)))
        if above_volume is not None:
            tags.append(("volumeAbove", str(above_volume)))
        if market_cap_above is not None:
            tags.append(("marketCapAbove", str(market_cap_above)))
        if market_cap_below is not None:
            tags.append(("marketCapBelow", str(market_cap_below)))

        tag_values = [tuple(t) for t in tags] if tags else []  # type: ignore[arg-type]

        data = await self._connection.with_retry(
            lambda: self._ib.reqScannerSubscriptionAsync(sub, tagValues=tag_values),
            operation=f"scan_market:{instrument}:{location}:{scan_code}",
        )

        results: list[dict[str, Any]] = []
        for item in data:
            contract = item.contractDetails.contract if hasattr(item, "contractDetails") else None
            if contract is None:
                continue
            results.append({
                "symbol": getattr(contract, "symbol", ""),
                "sec_type": getattr(contract, "secType", ""),
                "exchange": getattr(contract, "exchange", ""),
                "currency": getattr(contract, "currency", ""),
                "con_id": getattr(contract, "conId", None),
                "local_symbol": getattr(contract, "localSymbol", ""),
            })

        logger.info("scan_market: %d results for %s/%s/%s", len(results), instrument, location, scan_code)
        return results

    # ------------------------------------------------------------------
    # 16. News bulletins
    # ------------------------------------------------------------------

    async def get_news_bulletins(self, *, all_messages: bool = True) -> list[dict[str, Any]]:
        """Get IBKR system news bulletins (exchange halts, margin changes, etc).

        Args:
            all_messages: Return all historical bulletins (default True).
        """
        await self._connection.ensure_connected()
        logger.info("get_news_bulletins: all_messages=%s", all_messages)

        bulletins = await self._connection.with_retry(
            lambda: self._ib.reqNewsBulletinsAsync(allMessages=all_messages),
            operation="news_bulletins",
        )

        results: list[dict[str, Any]] = []
        for b in bulletins:
            results.append({
                "msg_id": getattr(b, "msgId", None),
                "message": getattr(b, "message", ""),
                "exchange": getattr(b, "exchange", ""),
                "time": str(getattr(b, "time", "")),
            })
        return results


class _PacingProxy:
    """Minimal OHLCVRequest-like proxy for pacing guard compatibility.

    The pacing guard only accesses ``symbol`` and ``what_to_show`` attributes,
    so we provide just those.
    """

    __slots__ = ("symbol", "what_to_show")

    def __init__(self, symbol: str, what_to_show: str) -> None:
        self.symbol = symbol
        self.what_to_show = what_to_show
