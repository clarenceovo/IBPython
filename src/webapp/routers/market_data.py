from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.feeds.bonds import BondYieldBar, BondYieldHistoryRequest
from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest
from src.feeds.options import OptionAnalyticsRequest, OptionAnalyticsSnapshot, OptionSkewSurfaceRequest, OptionSkewSurfaceResponse
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

router = APIRouter(prefix="/market-data", tags=["market-data"])


class HistoricalOHLCVLoadRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: OHLCVRequest
    persist: bool = False
    cache_latest: bool = True
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=None, ge=0)


class MinimalOHLCVLoadControls(BaseModel):
    """Common wrapper controls with production defaults."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    duration: str = Field(default="1 D", min_length=1)
    bar_size: str = Field(default="1 min", min_length=1)
    end_datetime: datetime | None = None
    what_to_show: str = Field(default="TRADES", min_length=1)
    use_rth: bool = True
    persist: bool = False
    cache_latest: bool = True
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=None, ge=0)
    metadata: dict[str, object] = Field(default_factory=dict)

    @field_validator("duration", "bar_size", "what_to_show", mode="before")
    @classmethod
    def normalize_required_text(cls, value: object) -> str:
        if value is None:
            raise ValueError("value is required")
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("value cannot be empty")
        return normalized

    def to_ohlcv_request(self, asset_class: AssetClass, **overrides: object) -> OHLCVRequest:
        return OHLCVRequest(
            asset_class=asset_class,
            duration=self.duration,
            bar_size=self.bar_size,
            end_datetime=self.end_datetime,
            what_to_show=self.what_to_show,
            use_rth=self.use_rth,
            metadata=self.metadata,
            **overrides,
        )


class EquityOHLCVLoadRequest(MinimalOHLCVLoadControls):
    symbol: str = Field(min_length=1, examples=["SPY"])
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    primary_exchange: str | None = Field(default=None, examples=["ARCA"])

    def to_request(self) -> OHLCVRequest:
        return self.to_ohlcv_request(
            AssetClass.EQUITY,
            symbol=self.symbol,
            exchange=self.exchange,
            currency=self.currency,
            primary_exchange=self.primary_exchange,
        )


class FutureOHLCVLoadRequest(MinimalOHLCVLoadControls):
    symbol: str = Field(min_length=1, examples=["ES"])
    exchange: str = Field(default="CME", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    last_trade_date_or_contract_month: str | None = Field(default=None, examples=["202606"])
    multiplier: str | None = Field(default=None, examples=["50"])
    local_symbol: str | None = Field(default=None, examples=["ESM6"])
    con_id: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_future_identifier(self) -> "FutureOHLCVLoadRequest":
        if not (self.last_trade_date_or_contract_month or self.local_symbol or self.con_id):
            raise ValueError("futures wrapper requires last_trade_date_or_contract_month, local_symbol, or con_id")
        return self

    def to_request(self) -> OHLCVRequest:
        return self.to_ohlcv_request(
            AssetClass.FUTURE,
            symbol=self.symbol,
            exchange=self.exchange,
            currency=self.currency,
            last_trade_date_or_contract_month=self.last_trade_date_or_contract_month,
            multiplier=self.multiplier,
            local_symbol=self.local_symbol,
            con_id=self.con_id,
        )


class FXOHLCVLoadRequest(MinimalOHLCVLoadControls):
    symbol: str = Field(min_length=1, examples=["EURUSD"])
    exchange: str = Field(default="IDEALPRO", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    what_to_show: str = Field(default="MIDPOINT", min_length=1)
    use_rth: bool = False

    def to_request(self) -> OHLCVRequest:
        return self.to_ohlcv_request(
            AssetClass.FX,
            symbol=self.symbol,
            exchange=self.exchange,
            currency=self.currency,
        )


class BondOHLCVLoadRequest(MinimalOHLCVLoadControls):
    symbol: str | None = Field(default=None, min_length=1, examples=["91282CJN2"])
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    sec_id_type: str | None = Field(default=None, examples=["CUSIP"])
    sec_id: str | None = Field(default=None, examples=["91282CJN2"])
    con_id: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_bond_identifier(self) -> "BondOHLCVLoadRequest":
        if not (self.symbol or self.sec_id or self.con_id):
            raise ValueError("bond wrapper requires symbol, sec_id, or con_id")
        return self

    def to_request(self) -> OHLCVRequest:
        symbol = self.symbol or self.sec_id or str(self.con_id)
        return self.to_ohlcv_request(
            AssetClass.BOND,
            symbol=symbol,
            exchange=self.exchange,
            currency=self.currency,
            sec_id_type=self.sec_id_type,
            sec_id=self.sec_id,
            con_id=self.con_id,
        )


class CachedOptionAnalyticsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: OptionAnalyticsRequest
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=None, ge=0)


class CachedOptionSkewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: OptionSkewSurfaceRequest
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=60, ge=0)


class CachedBondYieldHistoryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: BondYieldHistoryRequest
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=None, ge=0)


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
        "description": "Only symbol is required. The wrapper presets asset_class=equity, exchange=SMART, currency=USD, and TRADES.",
        "value": {"symbol": "SPY"},
    },
    "nasdaq_equity_with_primary_exchange": {
        "summary": "NASDAQ-listed equity",
        "description": "Use primary_exchange when the SMART-routed stock needs IBKR disambiguation.",
        "value": {
            "symbol": "TSLA",
            "primary_exchange": "NASDAQ",
            "duration": "1 D",
            "bar_size": "5 mins",
            "cache_latest": True,
        },
    },
}


FUTURES_OHLCV_REQUEST_EXAMPLES = {
    "es_by_contract_month": {
        "summary": "ES future by contract month",
        "description": "Futures require an expiry/contract month, local_symbol, or con_id.",
        "value": {
            "symbol": "ES",
            "exchange": "CME",
            "currency": "USD",
            "last_trade_date_or_contract_month": "202606",
            "duration": "1 D",
            "bar_size": "1 min",
        },
    },
    "es_by_local_symbol": {
        "summary": "ES future by local symbol",
        "description": "Use local_symbol when that is how the contract is represented in TWS.",
        "value": {
            "symbol": "ES",
            "exchange": "CME",
            "local_symbol": "ESM6",
            "multiplier": "50",
        },
    },
}


FX_OHLCV_REQUEST_EXAMPLES = {
    "eurusd_minimal": {
        "summary": "EURUSD midpoint bars",
        "description": "FX wrapper presets asset_class=fx, exchange=IDEALPRO, what_to_show=MIDPOINT, and use_rth=false.",
        "value": {"symbol": "EURUSD"},
    },
    "usdjpy_hourly": {
        "summary": "USDJPY hourly midpoint bars",
        "value": {
            "symbol": "USDJPY",
            "currency": "JPY",
            "duration": "5 D",
            "bar_size": "1 hour",
        },
    },
}


BOND_OHLCV_REQUEST_EXAMPLES = {
    "treasury_by_cusip": {
        "summary": "Treasury bond by CUSIP",
        "description": "Bond wrapper presets asset_class=bond. Use sec_id_type/sec_id or con_id when available.",
        "value": {
            "sec_id_type": "CUSIP",
            "sec_id": "91282CJN2",
            "duration": "1 M",
            "bar_size": "1 day",
        },
    },
    "bond_by_con_id": {
        "summary": "Bond by IBKR conId",
        "description": "Use con_id when your upstream security master already stores IBKR identifiers.",
        "value": {
            "con_id": 123456789,
            "currency": "USD",
            "duration": "1 M",
            "bar_size": "1 day",
        },
    },
}


OPTION_SKEW_REQUEST_EXAMPLES = {
    "tsla_bounded_skew": {
        "summary": "TSLA per-maturity skew",
        "description": "Samples a bounded strike window around spot, computes put-minus-call IV skew, and reports max call/put OI per expiry.",
        "value": {
            "request": {
                "chain_request": {
                    "symbol": "TSLA",
                    "asset_class": "equity",
                    "exchange": "SMART",
                    "currency": "USD",
                    "primary_exchange": "NASDAQ",
                },
                "spot_price": 250.0,
                "strike_window_pct": 0.30,
                "max_expirations": 4,
                "max_strikes_per_expiry": 11,
                "target_abs_delta": 0.25,
                "max_concurrent_requests": 4,
            },
            "use_ttl_cache": True,
            "cache_ttl_seconds": 60,
        },
    },
    "spx_bounded_skew": {
        "summary": "SPX index skew",
        "description": "For index options, specify the index exchange and optionally a trading class such as SPX or SPXW.",
        "value": {
            "request": {
                "chain_request": {
                    "symbol": "SPX",
                    "asset_class": "index",
                    "exchange": "CBOE",
                    "currency": "USD",
                },
                "chain_exchange": "CBOE",
                "trading_class": "SPX",
                "spot_price": 5200.0,
                "strike_window_pct": 0.20,
                "max_expirations": 4,
                "max_strikes_per_expiry": 11,
            },
            "use_ttl_cache": True,
            "cache_ttl_seconds": 60,
        },
    },
}


async def _load_ohlcv_with_controls(
    *,
    request: OHLCVRequest,
    persist: bool,
    cache_latest: bool,
    use_ttl_cache: bool,
    cache_ttl_seconds: float | None,
    cache_namespace: str,
    state: IBKRRestAppState,
) -> list[OHLCVBar]:
    async def load() -> list[OHLCVBar]:
        return await state.loader.load(
            request,
            persist=persist,
            cache_latest=cache_latest,
        )

    if use_ttl_cache and not persist:
        key = stable_cache_key(
            cache_namespace,
            {
                "request": request.model_dump(mode="json"),
                "cache_latest": cache_latest,
            },
        )
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=cache_ttl_seconds)
    return await load()


@router.post("/ohlcv", response_model=list[OHLCVBar])
async def load_ohlcv(
    payload: Annotated[HistoricalOHLCVLoadRequest, Body(openapi_examples=GENERIC_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OHLCVBar]:
    return await _load_ohlcv_with_controls(
        request=payload.request,
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
    return await _load_ohlcv_with_controls(
        request=payload.to_request(),
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv_equity",
        state=state,
    )


@router.post(
    "/ohlcv/futures",
    response_model=list[OHLCVBar],
    summary="Load futures OHLCV with preset asset_class",
)
async def load_futures_ohlcv(
    payload: Annotated[FutureOHLCVLoadRequest, Body(openapi_examples=FUTURES_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OHLCVBar]:
    return await _load_ohlcv_with_controls(
        request=payload.to_request(),
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv_futures",
        state=state,
    )


@router.post(
    "/ohlcv/fx",
    response_model=list[OHLCVBar],
    summary="Load FX OHLCV with preset asset_class",
)
async def load_fx_ohlcv(
    payload: Annotated[FXOHLCVLoadRequest, Body(openapi_examples=FX_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OHLCVBar]:
    return await _load_ohlcv_with_controls(
        request=payload.to_request(),
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv_fx",
        state=state,
    )


@router.post(
    "/ohlcv/bond",
    response_model=list[OHLCVBar],
    summary="Load bond OHLCV with preset asset_class",
)
async def load_bond_ohlcv(
    payload: Annotated[BondOHLCVLoadRequest, Body(openapi_examples=BOND_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OHLCVBar]:
    return await _load_ohlcv_with_controls(
        request=payload.to_request(),
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv_bond",
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


@router.post("/options/analytics", response_model=OptionAnalyticsSnapshot)
async def load_option_analytics(
    payload: CachedOptionAnalyticsRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> OptionAnalyticsSnapshot:
    async def load() -> OptionAnalyticsSnapshot:
        return await state.feed.load_option_analytics(payload.request)

    if payload.use_ttl_cache:
        key = stable_cache_key("option_analytics", payload.request)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()


@router.post("/options/skew", response_model=OptionSkewSurfaceResponse)
async def load_option_skew_surface(
    payload: Annotated[CachedOptionSkewRequest, Body(openapi_examples=OPTION_SKEW_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> OptionSkewSurfaceResponse:
    async def load() -> OptionSkewSurfaceResponse:
        return await state.feed.load_option_skew_surface(payload.request)

    if payload.use_ttl_cache:
        key = stable_cache_key("option_skew", payload.request)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()


@router.post("/bonds/yields/history", response_model=list[BondYieldBar])
async def load_bond_yield_history(
    payload: CachedBondYieldHistoryRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[BondYieldBar]:
    async def load() -> list[BondYieldBar]:
        return await state.feed.load_bond_yield_history(payload.request)

    if payload.use_ttl_cache:
        key = stable_cache_key("bond_yield_history", payload.request)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()
