"""Equity OHLCV endpoints and models."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, Depends, Query
from pydantic import Field

from src.config.reference_data import resolve_index as _resolve_index
from src.feeds.exchange_resolver import resolve_equity
from src.feeds.models import AssetClass, OHLCVBar
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.routers.market_data_shared import (
    HistoricalOHLCVLoadRequest,
    MinimalOHLCVLoadControls,
    load_ohlcv_with_controls,
)

router = APIRouter(prefix="/market-data", tags=["market-data"])


class EquityOHLCVLoadRequest(MinimalOHLCVLoadControls):
    symbol: str = Field(
        min_length=1,
        examples=["SPY", "0700.HK", "7203.T", "TSLA"],
        description=(
            "Ticker symbol with optional exchange suffix. "
            "Supported suffixes: .HK (HKEX), .T (TSE), .L (LSE), .F/.DE (Xetra), "
            ".PA (Euronext Paris), .AS (Amsterdam), .MC (Madrid), .MI (Milan), "
            ".SW (SIX), .TO (TSX), .AX (ASX), .SI (SGX), .NS/.BO (India), "
            ".KS (Korea), .SS (Shanghai), .SZ (Shenzhen), .MX (Mexico), .SA (Brazil). "
            "No suffix defaults to SMART/USD (US equity)."
        ),
    )
    exchange: str | None = Field(
        default=None,
        min_length=1,
        description="Override auto-detected exchange. Leave empty to auto-resolve from symbol suffix.",
    )
    currency: str | None = Field(
        default=None,
        min_length=1,
        description="Override auto-detected currency. Leave empty to auto-resolve from symbol suffix.",
    )
    primary_exchange: str | None = Field(
        default=None,
        examples=["ARCA"],
        description="Override auto-detected primary exchange. Leave empty to auto-resolve.",
    )

    def to_request(self):
        from src.feeds.models import OHLCVRequest
        resolved = resolve_equity(self.symbol)
        exchange = self.exchange or resolved.exchange
        currency = self.currency or resolved.currency
        primary = self.primary_exchange or resolved.primary_exchange or None
        return self.to_ohlcv_request(
            AssetClass.EQUITY,
            symbol=resolved.symbol,
            exchange=exchange,
            currency=currency,
            primary_exchange=primary,
        )


class IndexOHLCVLoadRequest(MinimalOHLCVLoadControls):
    """Wrapper for index OHLCV — SPX, NDX, DAX, HSI, etc."""

    symbol: str = Field(
        min_length=1,
        examples=["SPX", "NDX", "HSI", "DAX"],
        description=(
            "Index symbol as recognized by IBKR. "
            "SPX → CBOE/USD, NDX → CBOE/USD, HSI → SEHK/HKD, DAX → EUREX/EUR. "
            "Use the exchange/currency overrides for less common indices."
        ),
    )
    exchange: str | None = Field(
        default=None,
        min_length=1,
        description="Override auto-detected exchange. Leave empty to auto-resolve from symbol.",
    )
    currency: str | None = Field(
        default=None,
        min_length=1,
        description="Override auto-detected currency. Leave empty to auto-resolve.",
    )

    def to_request(self):
        resolved = _resolve_index(self.symbol)
        return self.to_ohlcv_request(
            AssetClass.INDEX,
            symbol=resolved["symbol"],
            exchange=self.exchange or resolved["exchange"],
            currency=self.currency or resolved["currency"],
        )


GENERIC_OHLCV_REQUEST_EXAMPLES = {
    "spy_equity_full_request": {
        "summary": "Generic SPY equity OHLCV",
        "description": "Advanced endpoint that accepts the full OHLCVRequest payload.",
        "value": {
            "request": {
                "symbol": "SPY",
                "asset_class": "equity",
                "exchange": "SMART",
                "currency": "USD",
                "start_datetime": "2026-05-01T13:30:00Z",
                "end_datetime": "2026-05-01T20:00:00Z",
                "duration": "1 D",
                "bar_size": "1 min",
                "what_to_show": "TRADES",
                "use_rth": True,
            },
            "persist": False,
            "cache_latest": True,
            "use_ttl_cache": True,
        },
    }
}

EQUITY_OHLCV_REQUEST_EXAMPLES = {
    "minimal_spy": {
        "summary": "Minimal SPY equity bars",
        "description": "Only symbol is required. Auto-resolves to SMART/USD/ARCA.",
        "value": {"symbol": "SPY"},
    },
    "tsla_nasdaq_auto": {
        "summary": "TSLA auto-resolved",
        "description": "TSLA auto-resolves to SMART/USD/NASDAQ via exchange resolver.",
        "value": {"symbol": "TSLA"},
    },
    "hk_stock_0700": {
        "summary": "0700.HK (Tencent)",
        "description": ".HK suffix auto-resolves to SEHK/HKD.",
        "value": {"symbol": "0700.HK"},
    },
    "jp_stock_toyota": {
        "summary": "7203.T (Toyota)",
        "description": ".T suffix auto-resolves to TSEJ/JPY.",
        "value": {"symbol": "7203.T"},
    },
    "nasdaq_equity_with_primaryExchange": {
        "summary": "NASDAQ-listed equity (explicit override)",
        "description": "Use primary_exchange when the auto-resolver doesn't match and SMART needs disambiguation.",
        "value": {
            "symbol": "TSLA",
            "primary_exchange": "NASDAQ",
            "start_datetime": "2026-05-01T13:30:00Z",
            "end_datetime": "2026-05-01T20:00:00Z",
            "duration": "1 D",
            "bar_size": "5 mins",
            "cache_latest": True,
        },
    },
    "london_stock": {
        "summary": "HSBA.L (HSBC London)",
        "description": ".L suffix auto-resolves to LSE/GBP.",
        "value": {"symbol": "HSBA.L"},
    },
    "shanghai_connect": {
        "summary": "600519.SS (Kweichow Moutai)",
        "description": ".SS suffix auto-resolves to SEHKNTL/CNH (Stock Connect).",
        "value": {"symbol": "600519.SS"},
    },
}

INDEX_OHLCV_REQUEST_EXAMPLES = {
    "spx_us": {"summary": "S&P 500 (SPX)", "description": "Auto-resolves to CBOE/USD.", "value": {"symbol": "SPX"}},
    "ndx_us": {"summary": "Nasdaq-100 (NDX)", "description": "Auto-resolves to CBOE/USD.", "value": {"symbol": "NDX"}},
    "vix_us": {"summary": "CBOE Volatility Index (VIX)", "description": "Auto-resolves to CBOE/USD.", "value": {"symbol": "VIX"}},
    "rut_us": {"summary": "Russell 2000 (RUT)", "description": "Auto-resolves to ICE/USD.", "value": {"symbol": "RUT"}},
    "dji_us": {"summary": "Dow Jones Industrial Average (DJI)", "description": "Auto-resolves to CBOE/USD.", "value": {"symbol": "DJI"}},
    "hsi_hk": {"summary": "Hang Seng Index (HSI)", "description": "Auto-resolves to SEHK/HKD.", "value": {"symbol": "HSI"}},
    "dax_de": {"summary": "DAX (DAX)", "description": "Auto-resolves to EUREX/EUR.", "value": {"symbol": "DAX"}},
    "nikkei_jp": {"summary": "Nikkei 225 (NIKKEI)", "description": "Auto-resolves to TSEJ/JPY.", "value": {"symbol": "NIKKEI"}},
    "ftse100_uk": {"summary": "FTSE 100 (UK)", "description": "Auto-resolves to LSE/GBP.", "value": {"symbol": "FTSE100"}},
    "cac40_fr": {"summary": "CAC 40 (France)", "description": "Auto-resolves to SBF/EUR.", "value": {"symbol": "CAC40"}},
    "taiex_tw": {"summary": "TAIEX (Taiwan)", "description": "Auto-resolves to TWSE/TWD.", "value": {"symbol": "TAIEX"}},
    "kospi200_kr": {"summary": "KOSPI 200 (Korea)", "description": "Auto-resolves to KSE/KRW.", "value": {"symbol": "KOSPI200"}},
    "smi_ch": {"summary": "SMI (Switzerland)", "description": "Auto-resolves to EBS/CHF.", "value": {"symbol": "SMI"}},
    "estx50_eu": {"summary": "Euro Stoxx 50", "description": "Auto-resolves to EUREX/EUR.", "value": {"symbol": "ESTX50"}},
}


@router.post("/ohlcv", response_model=list[OHLCVBar])
async def load_ohlcv(
    payload: Annotated[HistoricalOHLCVLoadRequest, Body(openapi_examples=GENERIC_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OHLCVBar]:
    return await load_ohlcv_with_controls(
        request=payload.request,
        start_datetime=payload.request.start_datetime,
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv",
        state=state,
    )


@router.post(
    "/ohlcv/equity",
    response_model=list[OHLCVBar],
    summary="Load equity OHLCV with preset asset_class",
)
async def load_equity_ohlcv(
    payload: Annotated[EquityOHLCVLoadRequest, Body(openapi_examples=EQUITY_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OHLCVBar]:
    return await load_ohlcv_with_controls(
        request=payload.to_request(),
        start_datetime=payload.start_datetime,
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv_equity",
        state=state,
    )


@router.post(
    "/ohlcv/index",
    response_model=list[OHLCVBar],
    summary="Load index OHLCV with auto-resolved exchange",
)
async def load_index_ohlcv(
    payload: Annotated[IndexOHLCVLoadRequest, Body(openapi_examples=INDEX_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OHLCVBar]:
    return await load_ohlcv_with_controls(
        request=payload.to_request(),
        start_datetime=payload.start_datetime,
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv_index",
        state=state,
    )


@router.get(
    "/latest-bar",
    response_model=OHLCVBar | None,
    summary="Read the latest cached OHLCV bar",
    description=(
        "Reads Redis only; it does not call IBKR or QuestDB. "
        "Populate this cache by loading OHLCV with cache_latest=true or by running a scheduler snapshot job. "
        "Use symbol for the production symbol-scoped key. If symbol is omitted, the legacy asset-class latest key is used."
    ),
)
async def get_latest_bar(
    asset_class: Annotated[
        AssetClass,
        Query(
            description="Asset class namespace used in the Redis latest-bar key.",
            examples=["equity"],
        ),
    ],
    bar_size: Annotated[
        str,
        Query(
            min_length=1,
            description="Bar size exactly as used by OHLCV loading, for example '1 min'. Spaces are normalized to underscores in Redis keys.",
            examples=["1 min"],
        ),
    ],
    symbol: Annotated[
        str | None,
        Query(
            min_length=1,
            description="Optional symbol for the symbol-scoped latest-bar key. Omit only for the legacy asset-class latest key.",
            examples=["SPY"],
        ),
    ] = None,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> OHLCVBar | None:
    return await state.redis.get_latest_bar(asset_class, bar_size, symbol=symbol)
