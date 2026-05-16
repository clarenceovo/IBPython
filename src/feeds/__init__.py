"""Feed adapters and market data domain models."""

from src.feeds.account import (
    AccountPnLDTO,
    AccountSummaryDTO,
    AccountValueDTO,
    LivePositionDTO,
    PortfolioItemDTO,
    PositionPnLDTO,
)
try:
    from src.feeds.bond_curve import (
        BondCurveRenderPoint,
        BondCurveRequest,
        BondCurveResponse,
        StandardTenorCTDPoint,
        build_standard_bond_curve,
        resolve_bond_curve_market,
    )
except ImportError:
    # bond_curve module not yet available; symbols will be missing from __all__
    BondCurveRenderPoint = None  # type: ignore[assignment,misc]
    BondCurveRequest = None  # type: ignore[assignment,misc]
    BondCurveResponse = None  # type: ignore[assignment,misc]
    StandardTenorCTDPoint = None  # type: ignore[assignment,misc]
    build_standard_bond_curve = None  # type: ignore[assignment,misc]
    resolve_bond_curve_market = None  # type: ignore[assignment,misc]
from src.feeds.bonds import (
    BondInstrument,
    BondYieldBar,
    BondYieldField,
    BondYieldHistoryRequest,
    BondYieldQuote,
    CTDBondCandidate,
    CTDBondSnapshot,
    CTDFutureDefinition,
    DEFAULT_CTD_FUTURE_DEFINITIONS,
    SovereignBondMarket,
    YieldCurveBootstrapInstrument,
    YieldCurveDTO,
    YieldCurvePoint,
    YieldUnit,
)
from src.feeds.fundamental_data import (
    ForecastEventContractCategory,
    FundamentalDataReport,
    FundamentalDataRequest,
    FundamentalReportType,
    WSHEventDataReport,
    WSHEventDataRequest,
    WSHMetadataReport,
)
from src.feeds.models import AssetClass, BaseOHLCVBar, FXOHLCVBar, FutureOHLCVBar, OHLCVBar, OHLCVRequest, OptionOHLCVBar
from src.feeds.news import (
    HistoricalNewsHeadline,
    HistoricalNewsRequest,
    NewsArticle,
    NewsArticleRequest,
    NewsBulletin,
    NewsProvider,
    NewsTick,
)
from src.feeds.options import (
    DEFAULT_OPTION_ANALYTICS_GENERIC_TICKS,
    OptionAnalyticsRequest,
    OptionAnalyticsSnapshot,
    OptionContractSpec,
    OptionGreekSource,
    OptionGreekSet,
    OptionMaturitySkew,
    OptionRight,
    OptionSkewPoint,
    OptionSkewSelectionMethod,
    OptionSkewSurfaceRequest,
    OptionSkewSurfaceResponse,
)

__all__ = [
    "AccountPnLDTO",
    "AccountSummaryDTO",
    "AccountValueDTO",
    "AssetClass",
    "BaseOHLCVBar",
    "BondCurveRenderPoint",
    "BondCurveRequest",
    "BondCurveResponse",
    "BondInstrument",
    "BondYieldBar",
    "BondYieldField",
    "BondYieldHistoryRequest",
    "BondYieldQuote",
    "CTDBondCandidate",
    "CTDBondSnapshot",
    "CTDFutureDefinition",
    "DEFAULT_CTD_FUTURE_DEFINITIONS",
    "DEFAULT_OPTION_ANALYTICS_GENERIC_TICKS",
    "FXOHLCVBar",
    "ForecastEventContractCategory",
    "FundamentalDataReport",
    "FundamentalDataRequest",
    "FundamentalReportType",
    "FutureOHLCVBar",
    "LivePositionDTO",
    "OHLCVBar",
    "OHLCVRequest",
    "OptionOHLCVBar",
    "OptionAnalyticsRequest",
    "OptionAnalyticsSnapshot",
    "OptionContractSpec",
    "OptionGreekSource",
    "OptionGreekSet",
    "OptionMaturitySkew",
    "OptionRight",
    "OptionSkewPoint",
    "OptionSkewSelectionMethod",
    "OptionSkewSurfaceRequest",
    "OptionSkewSurfaceResponse",
    "PortfolioItemDTO",
    "PositionPnLDTO",
    "StandardTenorCTDPoint",
    "SovereignBondMarket",
    "YieldCurveBootstrapInstrument",
    "YieldCurveDTO",
    "YieldCurvePoint",
    "YieldUnit",
    "HistoricalNewsHeadline",
    "HistoricalNewsRequest",
    "NewsArticle",
    "NewsArticleRequest",
    "NewsBulletin",
    "NewsProvider",
    "NewsTick",
    "WSHEventDataReport",
    "WSHEventDataRequest",
    "WSHMetadataReport",
    "build_standard_bond_curve",
    "resolve_bond_curve_market",
]
