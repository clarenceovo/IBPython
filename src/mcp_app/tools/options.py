"""Options MCP tools: chains, real-time analytics, skew surface, IV, and option pricing."""

from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import Context, FastMCP

from src.mcp_app.common import (
    _AssetClassParam,
    _READ_TOOL,
    _RightParam,
    _parse_asset_class,
    _state,
    _tool_error,
)


async def load_option_chains(
    ctx: Context,
    symbol: str,
    asset_class: _AssetClassParam = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
) -> list[dict[str, Any]] | dict[str, Any]:
    """Load option chain for a symbol from IBKR.

    Returns all available expirations and strikes for the given underlying.

    Args:
        symbol: Underlying symbol (e.g. 'AAPL', 'SPY', 'SPX')
        asset_class: EQUITY or INDEX (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
    """
    state = _state(ctx)
    from src.feeds.contracts import OptionChainRequest

    try:
        request = OptionChainRequest(
            symbol=symbol,
            asset_class=_parse_asset_class(asset_class),
            exchange=exchange,
            currency=currency,
        )
        chains = await state.feed.load_option_chains(request)
        return [
            {
                "symbol": chain.symbol,
                "exchange": chain.exchange,
                "expirations": chain.expirations,
                "strikes": chain.strikes,
                "trading_class": getattr(chain, "trading_class", None),
                "multiplier": getattr(chain, "multiplier", None),
            }
            for chain in chains
        ]
    except Exception as e:
        return _tool_error(e)


async def load_option_analytics(
    ctx: Context,
    symbol: str,
    expiry: str,
    strike: float,
    right: _RightParam = "C",
    asset_class: _AssetClassParam = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    multiplier: str = "100",
) -> dict[str, Any]:
    """Load real-time option greeks/analytics from IBKR.

    Returns delta, gamma, theta, vega, implied volatility, OI, volume, etc.

    Args:
        symbol: Underlying symbol (e.g. 'AAPL', 'SPY')
        expiry: Option expiry (e.g. '20260619')
        strike: Strike price
        right: 'C' for Call or 'P' for Put (default 'C')
        asset_class: EQUITY or INDEX (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        multiplier: Option multiplier (default '100')
    """
    state = _state(ctx)
    from src.feeds.options_models import OptionAnalyticsRequest, OptionContractSpec, OptionRight

    try:
        spec = OptionContractSpec(
            underlying_symbol=symbol.upper(),
            expiry=expiry,
            strike=strike,
            right=OptionRight(right.upper()),
            exchange=exchange,
            currency=currency,
            multiplier=multiplier,
        )
        request = OptionAnalyticsRequest(contract=spec)
        snapshot = await state.feed.load_option_analytics(request)
        return snapshot.model_dump(mode="json")
    except Exception as e:
        return _tool_error(e)


async def load_option_skew_surface(
    ctx: Context,
    symbol: str,
    asset_class: _AssetClassParam = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    max_expirations: int = 6,
    strike_window_pct: float = 0.30,
) -> dict[str, Any]:
    """Load option skew surface across expirations.

    Returns put-minus-call IV skew per expiry, max call/put OI, and per-strike greeks.

    Args:
        symbol: Underlying symbol (e.g. 'AAPL', 'SPY')
        asset_class: EQUITY or INDEX (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        max_expirations: Max expirations to sample (default 6)
        strike_window_pct: Strike window around spot as percentage (default 0.30)
    """
    state = _state(ctx)
    from src.feeds.contracts import OptionChainRequest
    from src.feeds.options_models import OptionSkewSurfaceRequest

    try:
        chain_request = OptionChainRequest(
            symbol=symbol,
            asset_class=_parse_asset_class(asset_class),
            exchange=exchange,
            currency=currency,
        )
        request = OptionSkewSurfaceRequest(
            chain_request=chain_request,
            max_expirations=max_expirations,
            strike_window_pct=strike_window_pct,
        )
        response = await state.feed.load_option_skew_surface(request)
        return response.model_dump(mode="json")
    except Exception as e:
        return _tool_error(e)


async def calculate_iv(
    ctx: Context,
    symbol: str,
    strike: float,
    expiry: str,
    option_price: float,
    under_price: float,
    right: _RightParam = "C",
    exchange: str = "SMART",
    currency: str = "USD",
    multiplier: str = "100",
) -> dict[str, Any]:
    """Calculate implied volatility using IBKR's option pricing engine.

    Args:
        symbol: Underlying symbol
        strike: Strike price
        expiry: Option expiry (e.g. '20260619')
        option_price: Observed option price
        under_price: Current underlying price
        right: 'C' for Call or 'P' for Put (default 'C')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        multiplier: Option multiplier (default '100')
    """
    state = _state(ctx)
    from src.feeds.contracts import ContractSpec
    from src.feeds.models import AssetClass

    try:
        spec = ContractSpec(
            symbol=symbol,
            asset_class=AssetClass.OPTION,
            exchange=exchange,
            currency=currency,
            option_sec_type="OPT",
            underlying_symbol=symbol,
            expiry=expiry,
            last_trade_date_or_contract_month=expiry,
            strike=strike,
            right=right,
            multiplier=multiplier,
        )
        contract = await state.feed.qualify_contract(spec)
        iv = await state.feed.calculate_iv(contract, option_price, under_price)
        return {
            "symbol": symbol,
            "strike": strike,
            "right": right,
            "expiry": expiry,
            "option_price": option_price,
            "under_price": under_price,
            "implied_volatility": iv,
        }
    except Exception as e:
        return _tool_error(e)


async def calculate_option_price(
    ctx: Context,
    symbol: str,
    strike: float,
    expiry: str,
    implied_vol: float,
    under_price: float,
    right: _RightParam = "C",
    exchange: str = "SMART",
    currency: str = "USD",
    multiplier: str = "100",
) -> dict[str, Any]:
    """Calculate option price from implied volatility using IBKR's pricing engine.

    Args:
        symbol: Underlying symbol
        strike: Strike price
        expiry: Option expiry (e.g. '20260619')
        implied_vol: Implied volatility (e.g. 0.25 for 25%)
        under_price: Current underlying price
        right: 'C' for Call or 'P' for Put (default 'C')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        multiplier: Option multiplier (default '100')
    """
    state = _state(ctx)
    from src.feeds.contracts import ContractSpec
    from src.feeds.models import AssetClass

    try:
        spec = ContractSpec(
            symbol=symbol,
            asset_class=AssetClass.OPTION,
            exchange=exchange,
            currency=currency,
            option_sec_type="OPT",
            underlying_symbol=symbol,
            expiry=expiry,
            last_trade_date_or_contract_month=expiry,
            strike=strike,
            right=right,
            multiplier=multiplier,
        )
        contract = await state.feed.qualify_contract(spec)
        price = await state.feed.calculate_option_price(contract, implied_vol, under_price)
        return {
            "symbol": symbol,
            "strike": strike,
            "right": right,
            "expiry": expiry,
            "implied_vol": implied_vol,
            "under_price": under_price,
            "option_price": price,
        }
    except Exception as e:
        return _tool_error(e)


def register(mcp: FastMCP) -> None:
    for fn in (
        load_option_chains,
        load_option_analytics,
        load_option_skew_surface,
        calculate_iv,
        calculate_option_price,
    ):
        mcp.tool(annotations=_READ_TOOL)(fn)
