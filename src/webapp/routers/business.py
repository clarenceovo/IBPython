"""Business domain router — thin re-export for backward compatibility.

All endpoints are now split into domain-focused sub-modules:
- business_curves.py   — bond curve endpoints
- business_news.py     — news endpoints
- business_returns.py  — market panel, universe bars, returns
- business_skew.py     — option skew surface
- business_futures.py  — commodity futures
- business_portfolio.py — portfolio risk snapshots
- business_event_contracts.py — ForecastEx / CME Event Contracts
- business_shared.py   — shared models, helpers, presets, Easter calculation
"""

from __future__ import annotations

from fastapi import APIRouter

from src.webapp.routers.business_curves import router as curves_router  # noqa: F401
from src.webapp.routers.business_curves import get_bond_curve  # noqa: F401
from src.webapp.routers.business_news import router as news_router  # noqa: F401
from src.webapp.routers.business_news import (  # noqa: F401
    SymbolNewsRequest,
    BusinessNewsHeadline,
    SymbolNewsResponse,
    CachedNewsArticleRequest,
    get_news_providers,
    get_symbol_news,
    get_news_article,
)
from src.webapp.routers.business_returns import router as returns_router  # noqa: F401
from src.webapp.routers.business_returns import (  # noqa: F401
    ReturnPoint,
    SymbolReturnSummary,
    ReturnsResponse,
    get_market_panel,
    get_universe_bars,
    get_returns,
)
from src.webapp.routers.business_skew import router as skew_router  # noqa: F401
from src.webapp.routers.business_skew import (  # noqa: F401
    BusinessOptionSkewRequest,
    get_option_skew,
)
from src.webapp.routers.business_futures import router as futures_router  # noqa: F401
from src.webapp.routers.business_futures import (  # noqa: F401
    CommodityFuturesRequest,
    CommodityFuturePoint,
    CommodityFuturesResponse,
    get_commodity_futures,
)
from src.webapp.routers.business_portfolio import router as portfolio_router  # noqa: F401
from src.webapp.routers.business_portfolio import (  # noqa: F401
    BusinessPortfolioExposure,
    BusinessPortfolioPosition,
    BusinessPortfolioRiskRequest,
    BusinessPortfolioRiskResponse,
    get_portfolio_risk_snapshot,
)
from src.webapp.routers.business_event_contracts import router as event_contracts_router  # noqa: F401
from src.webapp.routers.business_shared import (  # noqa: F401
    BusinessCacheControls,
    BusinessDateRangeControls,
    BusinessOHLCVSymbol,
    MarketPanelRequest,
    UniverseBarsRequest,
    commodity_contract_months,
    nymex_crude_oil_last_trade_date,
    resolve_business_symbol,
    symbol_to_ohlcv_request,
    load_many_ohlcv,
)

# Backward compat aliases — tests import the underscore-prefixed names
_nymex_crude_oil_last_trade_date = nymex_crude_oil_last_trade_date  # noqa: F401
_commodity_contract_months = commodity_contract_months  # noqa: F401

router = APIRouter(prefix="/business", tags=["business"])

# Include all sub-routers so they register their routes under /business prefix
router.include_router(curves_router)
router.include_router(news_router)
router.include_router(returns_router)
router.include_router(skew_router)
router.include_router(futures_router)
router.include_router(portfolio_router)
router.include_router(event_contracts_router)
