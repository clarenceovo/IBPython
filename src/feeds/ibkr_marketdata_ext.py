"""Market data extensions — tick-by-tick, historical ticks, market rules, IV/price calc, symbol search.

Sub-client following the pattern of ``ibkr_historical.py`` / ``ibkr_options_feed.py``.
Composes ``IBKRConnectionManager`` for connection lifecycle, retry, and pacing.
"""

from __future__ import annotations

import logging
import time as monotonic_time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Callable

from src.feeds.contracts import build_ibkr_contract, ContractSpec
from src.feeds.ibkr_connection import IBKRConnectionManager
from src.feeds.models import AssetClass
from src.feeds.tick_data import (
    HeadTimestampRequest,
    HistoricalTickRequest,
    HistoricalTickResponse,
    IVCalcRequest,
    MarketRule,
    OptionPriceCalcRequest,
    PriceIncrement,
    SmartComponent,
    SymbolDescription,
    TickByTickData,
    TickType,
)

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

        handle = await self._connection.with_retry(
            lambda: self._ib.reqTickByTickDataAsync(
                contract,
                tick_type.value,
                numberOfTicks=0,
                ignoreSize=True,
            ),
            operation=f"tick_by_tick:{symbol}",
        )

        self._tick_subscriptions[key] = handle
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
                self._ib.cancelTickByTickData(handle)
            except Exception:
                logger.debug("error cancelling tick-by-tick for %s", symbol, exc_info=True)
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
    # 2. Historical ticks
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
    # 3. Market rules
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
    # 4. Smart components
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
    # 5. Head timestamp
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
    # 6. Implied volatility calculation
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
    # 7. Option price calculation
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
    # 8. Symbol search (fuzzy matching)
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


class _PacingProxy:
    """Minimal OHLCVRequest-like proxy for pacing guard compatibility.

    The pacing guard only accesses ``symbol`` and ``what_to_show`` attributes,
    so we provide just those.
    """

    __slots__ = ("symbol", "what_to_show")

    def __init__(self, symbol: str, what_to_show: str) -> None:
        self.symbol = symbol
        self.what_to_show = what_to_show
