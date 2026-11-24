"""Consolidated market-data endpoints: one call, many sections, isolated failures."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, ConfigDict, Field

from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest
from src.feeds.snapshotter import EquitySnapshot
from src.feeds.symbol_resolver import ResolvedSymbol, SymbolResolutionError, resolve_symbol
from src.feeds.tick_data import MarketDepthSnapshot, TickByTickData
from src.webapp.consolidated import ConsolidatedSection, run_consolidated_sections, utc_now
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.routers.market_data_depth import UnifiedMarketDepthRequest
from src.webapp.routers.market_data_shared import load_ohlcv_with_controls, session_to_dict

router = APIRouter(prefix="/market-data/consolidated", tags=["market-data"])

SNAPSHOT_SECTION = "snapshot"
TICKS_SECTION = "ticks"
DEPTH_SECTION = "depth"
TRADING_SCHEDULE_SECTION = "trading_schedule"

SECTION_NAMES = (SNAPSHOT_SECTION, TICKS_SECTION, DEPTH_SECTION, TRADING_SCHEDULE_SECTION)
_SECTION_NAME_SET = frozenset(SECTION_NAMES)

# Small tick count: the consolidated payload is a quick overview, not a full tick dump.
CONSOLIDATED_TICKS_COUNT = 10
# One trading week of schedule sessions keeps the section light.
CONSOLIDATED_SCHEDULE_DAYS = 7

# Universal (all-asset-class) endpoint sections and conventions.
OHLCV_SECTION = "ohlcv"

UNIVERSAL_SECTION_NAMES = (
    OHLCV_SECTION,
    TICKS_SECTION,
    DEPTH_SECTION,
    TRADING_SCHEDULE_SECTION,
    SNAPSHOT_SECTION,
)
_UNIVERSAL_SECTION_NAME_SET = frozenset(UNIVERSAL_SECTION_NAMES)

# Universal OHLCV overview: two trading days of hourly bars.
UNIVERSAL_DURATION = "2 D"
UNIVERSAL_BAR_SIZE = "1 hour"

# AssetClass -> IBKR secType used by the in-memory tick-by-tick buffers.
SEC_TYPE_BY_ASSET_CLASS: dict[AssetClass, str] = {
    AssetClass.EQUITY: "STK",
    AssetClass.FUTURE: "FUT",
    AssetClass.FX: "CASH",
    AssetClass.CRYPTO: "CRYPTO",
    AssetClass.INDEX: "IND",
    AssetClass.OPTION: "OPT",
    AssetClass.BOND: "BOND",
}

# Order-book DOM snapshots only make sense for centrally listed order books.
DOM_ASSET_CLASSES = frozenset({AssetClass.EQUITY, AssetClass.FUTURE, AssetClass.INDEX, AssetClass.OPTION})
# The unified DOM request model only accepts equity, fx, index, and concrete future classes.
_DOM_REQUEST_ASSET_CLASSES = frozenset({AssetClass.EQUITY, AssetClass.FUTURE, AssetClass.INDEX})

# what_to_show / use_rth defaults per asset class, matching market_data_shared conventions.
UNIVERSAL_OHLCV_DEFAULTS: dict[AssetClass, tuple[str, bool]] = {
    AssetClass.EQUITY: ("TRADES", True),
    AssetClass.FX: ("MIDPOINT", False),
    AssetClass.FUTURE: ("TRADES", False),
    AssetClass.INDEX: ("TRADES", True),
    AssetClass.CRYPTO: ("AGGTRADES", False),
    AssetClass.OPTION: ("TRADES", True),
    AssetClass.BOND: ("TRADES", True),
}


class UniversalMarketDataResponse(BaseModel):
    """Universal one-symbol payload for any asset class, one optional field per section."""

    symbol: str = Field(description="Canonical resolved IBKR symbol.")
    asset_class: str = Field(description="Resolved asset class.")
    exchange: str = Field(description="Resolved exchange routing.")
    currency: str = Field(description="Resolved currency.")
    local_symbol: str | None = Field(default=None, description="Resolved IBKR local symbol when present.")
    contract_month: str | None = Field(
        default=None,
        description="Resolved lastTradeDateOrContractMonth (YYYYMM) for dated futures.",
    )
    continuous: bool = Field(default=False, description="Whether the resolution is a continuous future.")
    matched_rule: str = Field(default="", description="Resolver rule that produced this resolution.")
    ohlcv: list[OHLCVBar] | None = Field(
        default=None,
        description="Two days of hourly OHLCV bars. None when the section failed or was skipped.",
    )
    ticks: list[TickByTickData] | None = Field(
        default=None,
        description="Latest buffered ticks. None when the section failed or was skipped.",
    )
    depth: MarketDepthSnapshot | None = Field(
        default=None,
        description="Short-lived DOM/L2 snapshot. None when the section failed or was skipped.",
    )
    trading_schedule: list[dict[str, Any]] | None = Field(
        default=None,
        description="Trading schedule sessions. None when the section failed or was skipped.",
    )
    snapshot: EquitySnapshot | None = Field(
        default=None,
        description="Latest cached equity snapshot (equity only). None when unavailable or skipped.",
    )
    sections: dict[str, ConsolidatedSection] = Field(description="Per-section status: ok, error, or skipped.")
    requested_at: datetime = Field(description="UTC timestamp of the consolidated request.")


class ResolvedSymbolResponse(BaseModel):
    """Normalized IBKR contract fields returned by the universal symbol resolver.

    Field names mirror ``ResolvedSymbol.describe()`` exactly.
    """

    model_config = ConfigDict(extra="forbid")

    input: str = Field(description="Ticker exactly as submitted.")
    symbol: str = Field(description="Canonical IBKR symbol, e.g. 0700, ES, EURUSD.")
    asset_class: str = Field(description="Resolved asset class: equity, fx, future, crypto, index, option, or bond.")
    exchange: str = Field(description="Resolved IBKR exchange routing, e.g. SEHK, CME, IDEALPRO, SMART.")
    currency: str = Field(description="Resolved contract currency, e.g. HKD, USD.")
    primary_exchange: str = Field(description="Primary exchange when relevant; empty string otherwise.")
    local_symbol: str | None = Field(default=None, description="IBKR local symbol when the rule produced one, e.g. ESZ6.")
    contract_month: str | None = Field(
        default=None,
        description="Futures contract month (YYYYMM) for dated contracts; null otherwise.",
    )
    continuous: bool = Field(description="True when the resolution is a continuous future (CONTFUT).")
    con_id: int | None = Field(default=None, description="IBKR contract id when the rule knows it; null otherwise.")
    matched_rule: str = Field(description="Resolver rule that matched, e.g. future_pattern, fx_pair, equity_fallback.")
    notes: list[str] = Field(default_factory=list, description="Resolver notes and disambiguation hints.")


def _parse_universal_section_whitelist(raw: str | None) -> frozenset[str] | None:
    """Parse the universal-endpoint section whitelist. None means 'all sections'."""
    if raw is None:
        return None
    names = [part.strip().lower() for part in raw.split(",")]
    names = [part for part in names if part]
    if not names:
        raise HTTPException(
            status_code=422,
            detail=f"sections must name at least one valid section: {', '.join(UNIVERSAL_SECTION_NAMES)}",
        )
    unknown = [part for part in names if part not in _UNIVERSAL_SECTION_NAME_SET]
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=f"unknown section(s): {', '.join(unknown)}; valid sections: {', '.join(UNIVERSAL_SECTION_NAMES)}",
        )
    return frozenset(names)


def _universal_ohlcv_request(resolved: ResolvedSymbol) -> OHLCVRequest:
    """Build the universal two-day hourly OHLCV request from a resolved symbol."""
    what_to_show, use_rth = UNIVERSAL_OHLCV_DEFAULTS[resolved.asset_class]
    contract_fields: dict[str, Any] = {}
    if resolved.asset_class is AssetClass.FUTURE:
        contract_fields = {
            "last_trade_date_or_contract_month": resolved.last_trade_date_or_contract_month,
            "local_symbol": resolved.local_symbol,
            "continuous": resolved.continuous,
        }
    return OHLCVRequest(
        symbol=resolved.canonical_symbol,
        asset_class=resolved.asset_class,
        exchange=resolved.exchange,
        currency=resolved.currency,
        primary_exchange=resolved.primary_exchange or None,
        duration=UNIVERSAL_DURATION,
        bar_size=UNIVERSAL_BAR_SIZE,
        what_to_show=what_to_show,
        use_rth=use_rth,
        **contract_fields,
    )


def _universal_schedule_request(resolved: ResolvedSymbol) -> OHLCVRequest:
    """Build the one-week SCHEDULE request from a resolved symbol."""
    return _universal_ohlcv_request(resolved).model_copy(
        update={
            "bar_size": "1 day",
            "duration": f"{CONSOLIDATED_SCHEDULE_DAYS} D",
            "what_to_show": "SCHEDULE",
            "use_rth": True,
        }
    )


def _universal_depth_request(resolved: ResolvedSymbol) -> UnifiedMarketDepthRequest:
    """Build the unified DOM request for DOM-capable asset classes."""
    dated_future = resolved.asset_class is AssetClass.FUTURE and not resolved.continuous
    return UnifiedMarketDepthRequest(
        symbol=resolved.canonical_symbol,
        asset_class=resolved.asset_class if resolved.asset_class in _DOM_REQUEST_ASSET_CLASSES else None,
        exchange=resolved.exchange,
        currency=resolved.currency,
        primary_exchange=resolved.primary_exchange or None,
        contract_month=resolved.last_trade_date_or_contract_month if dated_future else None,
        local_symbol=resolved.local_symbol if dated_future else None,
    )


def _resolve_or_422(
    symbol: str,
    asset_class: AssetClass | None,
    exchange: str | None,
    currency: str | None,
) -> ResolvedSymbol:
    """Resolve the symbol or raise a 422 carrying the resolver message."""
    try:
        return resolve_symbol(symbol, asset_class=asset_class, exchange=exchange, currency=currency)
    except SymbolResolutionError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get(
    "/resolve/{symbol}",
    summary="Resolve any ticker notation to IBKR contract fields",
    response_model=ResolvedSymbolResponse,
    description=(
        "Runs the universal symbol resolver on one ticker and returns the canonical IBKR symbol, "
        "asset class, exchange, currency, primary exchange, futures contract fields, and the "
        "matched rule. Accepts equity suffix notations (0700.HK, BRK.B), FX pairs (EURUSD, "
        "EUR.USD), crypto roots (BTC), futures notations (ESZ26, ES1!, ES=F), and index aliases "
        "(^GSPC). Pin the asset class with the query parameter when auto-detection is ambiguous. "
        "Unresolvable tickers return 422 with the resolver message."
    ),
)
async def resolve_market_data_symbol(
    symbol: str = Path(
        description="Ticker in any supported notation: 0700.HK, EUR.USD, ESZ26, ES1!, ^GSPC, BTC, CONID:12345-style is not needed here.",
    ),
    asset_class: AssetClass | None = Query(
        default=None,
        description="Optional asset class pin: equity, fx, future, crypto, index, option, or bond.",
    ),
    exchange: str | None = Query(default=None, min_length=1, description="Optional exchange override."),
    currency: str | None = Query(default=None, min_length=1, description="Optional currency override."),
) -> ResolvedSymbolResponse:
    """Resolve one ticker in any supported notation into normalized IBKR contract fields.

    Runs the same universal resolver that the auto and universal endpoints
    use and returns the canonical symbol, venue, currency, futures fields,
    matched rule, and notes. Unambiguous input needs no query parameters;
    unresolvable tickers return 422 with the resolver message.
    """
    resolved = _resolve_or_422(symbol, asset_class, exchange, currency)
    return ResolvedSymbolResponse.model_validate(resolved.describe())


@router.get(
    "/universal/{symbol}",
    response_model=UniversalMarketDataResponse,
    summary="Consolidated market data for any symbol in any asset class",
    description=(
        "Resolves the ticker with the universal symbol resolver, then returns hourly OHLCV bars, "
        "latest ticks, a DOM depth snapshot, the trading schedule, and (for equities) the cached "
        "snapshot in one call. Sections run concurrently and failures are isolated per section. "
        "Sections that do not apply to the resolved asset class (DOM for fx/crypto/bond, snapshot "
        "for non-equity) are reported as skipped with a note. Use the 'sections' query parameter "
        "to run only a subset, for example 'sections=ohlcv,ticks'."
    ),
)
async def get_universal_market_data(
    symbol: str = Path(
        description="Ticker in any supported notation: e.g. TSLA, 0700.HK, EURUSD, ESZ26, ^GSPC, BTC.",
    ),
    asset_class: AssetClass | None = Query(
        default=None,
        description="Optional asset class pin: equity, fx, future, crypto, index, option, or bond.",
    ),
    exchange: str | None = Query(default=None, min_length=1, description="Optional exchange override."),
    currency: str | None = Query(default=None, min_length=1, description="Optional currency override."),
    sections: str | None = Query(
        default=None,
        description=(
            "Comma-separated whitelist of sections to run: "
            f"{', '.join(UNIVERSAL_SECTION_NAMES)}. Omit to run all sections."
        ),
    ),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> UniversalMarketDataResponse:
    """Resolve any ticker, then gather every applicable market-data section concurrently.

    Each section (ohlcv, ticks, depth, trading_schedule, snapshot) runs
    with its own timeout and its status is reported separately, so one
    failing section never fails the call. Inapplicable sections are
    reported as skipped with a note. Use the sections query parameter to
    request a subset. Unresolvable tickers return 422.
    """
    resolved = _resolve_or_422(symbol, asset_class, exchange, currency)
    whitelist = _parse_universal_section_whitelist(sections)

    factories: dict[str, Callable[[], Awaitable[Any]]] = {}
    skip_notes: dict[str, str] = {}

    if whitelist is None or OHLCV_SECTION in whitelist:

        async def load_universal_ohlcv() -> list[OHLCVBar]:
            request = _universal_ohlcv_request(resolved)
            # Route through the shared cached loader so repeated universal
            # calls reuse the TTL cache and single-flight like /market-data/ohlcv.
            return await load_ohlcv_with_controls(
                request=request,
                persist=False,
                cache_latest=False,
                use_ttl_cache=True,
                cache_ttl_seconds=state.settings.ibkr_rest_market_data_ttl_seconds,
                cache_namespace="universal_ohlcv",
                state=state,
            )

        factories[OHLCV_SECTION] = load_universal_ohlcv

    if whitelist is None or TICKS_SECTION in whitelist:

        async def load_universal_ticks() -> list[TickByTickData]:
            return state.feed.get_latest_ticks(
                resolved.canonical_symbol,
                SEC_TYPE_BY_ASSET_CLASS[resolved.asset_class],
                resolved.exchange,
                CONSOLIDATED_TICKS_COUNT,
            )

        factories[TICKS_SECTION] = load_universal_ticks

    if whitelist is None or DEPTH_SECTION in whitelist:
        if resolved.asset_class in DOM_ASSET_CLASSES:

            async def load_universal_depth() -> MarketDepthSnapshot:
                request = _universal_depth_request(resolved)
                spec = request.to_contract_spec()
                return await state.feed.load_market_depth_snapshot(
                    spec,
                    num_rows=request.num_rows,
                    is_smart_depth=request.resolved_smart_depth(spec),
                    snapshot_wait_seconds=request.snapshot_wait_seconds,
                    request_timeout_seconds=state.settings.ibkr_market_depth_request_timeout_seconds,
                    lease_wait_seconds=state.settings.ibkr_market_depth_lease_wait_seconds,
                )

            factories[DEPTH_SECTION] = load_universal_depth
        else:
            skip_notes[DEPTH_SECTION] = (
                f"{resolved.asset_class.value} has no aggregated order book; "
                "DOM depth only applies to equity, future, index, and option contracts"
            )

    if whitelist is None or TRADING_SCHEDULE_SECTION in whitelist:

        async def load_universal_schedule() -> list[dict[str, Any]]:
            request = _universal_schedule_request(resolved)
            sessions = await state.feed.load_trading_schedule(request, ref_date=date.today(), use_rth=True)
            return [session_to_dict(session) for session in sessions]

        factories[TRADING_SCHEDULE_SECTION] = load_universal_schedule

    if whitelist is None or SNAPSHOT_SECTION in whitelist:
        if resolved.asset_class is AssetClass.EQUITY:

            async def load_universal_snapshot() -> EquitySnapshot | None:
                cached = await state.redis.get_latest_equity_snapshots([resolved.canonical_symbol])
                return cached.get(resolved.canonical_symbol.strip().upper())

            factories[SNAPSHOT_SECTION] = load_universal_snapshot
        else:
            skip_notes[SNAPSHOT_SECTION] = (
                f"cached snapshots are equity-only; skipped for {resolved.asset_class.value}"
            )

    results, meta = await run_consolidated_sections(
        factories,
        section_timeout_seconds=state.settings.ibkr_rest_consolidated_section_timeout_seconds,
    )
    for name in UNIVERSAL_SECTION_NAMES:
        if name not in meta:
            meta[name] = ConsolidatedSection(status="skipped", latency_ms=0.0, error=skip_notes.get(name))

    return UniversalMarketDataResponse(
        symbol=resolved.canonical_symbol,
        asset_class=resolved.asset_class.value,
        exchange=resolved.exchange,
        currency=resolved.currency,
        local_symbol=resolved.local_symbol,
        contract_month=resolved.last_trade_date_or_contract_month,
        continuous=resolved.continuous,
        matched_rule=resolved.matched_rule,
        ohlcv=results.get(OHLCV_SECTION),
        ticks=results.get(TICKS_SECTION),
        depth=results.get(DEPTH_SECTION),
        trading_schedule=results.get(TRADING_SCHEDULE_SECTION),
        snapshot=results.get(SNAPSHOT_SECTION),
        sections=meta,
        requested_at=utc_now(),
    )
