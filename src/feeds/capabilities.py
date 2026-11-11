"""Local implementation capabilities; no network calls or entitlement guesses."""
from __future__ import annotations

from importlib.metadata import version
from typing import Any, Literal

from pydantic import BaseModel

FUNDAMENTALS_REMOVED = (
    "IBKR removed fundamental report requests in API 10.47. "
    "This gateway no longer requests fundamental reports; WSH events and dividend ticks are separate data products."
)


class FeatureCapability(BaseModel):
    implemented: bool
    client_supported: bool | None
    availability: Literal["unknown", "unsupported", "disabled"]
    reason: str


class GatewayCapabilities(BaseModel):
    client_package: str = "ib-async"
    client_version: str | None
    connected: bool
    negotiated_server_protocol: int | None
    features: dict[str, FeatureCapability]


def gateway_capabilities(connection: Any, *, orders_enabled: bool = True) -> GatewayCapabilities:
    ib = connection.ib
    connected = connection.is_connected
    protocol = ib.client.serverVersion() if connected else None
    features: dict[str, FeatureCapability] = {}
    for name, method, decoder_method in (
        ("shortability", "reqMktData", "tickSize"),
        ("dividends", "reqMktData", "tickString"),
        ("all_open_orders", "reqAllOpenOrdersAsync", None),
    ):
        supported = None
        if ib is not None:
            supported = callable(getattr(ib, method, None))
            if decoder_method is not None:
                supported = supported and callable(getattr(getattr(ib, "wrapper", None), decoder_method, None))
        features[name] = FeatureCapability(
            implemented=True, client_supported=supported,
            availability="unsupported" if supported is False else "unknown",
            reason="Local implementation exists; live server support, permissions and data availability require a request.",
        )
    features["fundamental_reports"] = FeatureCapability(
        implemented=False, client_supported=False, availability="unsupported", reason=FUNDAMENTALS_REMOVED,
    )
    features["order_pathways"] = FeatureCapability(
        implemented=True,
        client_supported=True,
        availability="unknown" if orders_enabled else "disabled",
        reason=(
            "Order-writing pathways (place/modify/cancel/bracket/OCA/preview/exercise) are gated by the "
            "IBKR_ORDERS_ENABLED master switch; read-only order queries stay available."
        ),
    )
    features.update(_tws_1047_features())
    return GatewayCapabilities(
        client_version=version("ib-async"), connected=connected,
        negotiated_server_protocol=protocol, features=features,
    )


def _tws_1047_features() -> dict[str, FeatureCapability]:
    """Feature surface introduced in the TWS API 10.4x production line.

    Fields that the installed ib_async does not model yet are reported as
    client-unsupported so callers can degrade instead of sending dead fields.
    """
    order_fields = {"hedgeMaxSize", "conditionsIncludeOvernight"}
    modeled: set[str] = set()
    try:  # import guarded: capabilities must stay import-safe without ib_async
        from ib_async import Order as _IbOrder

        modeled = {name for name in order_fields if hasattr(_IbOrder(), name)}
    except Exception:  # noqa: BLE001 - missing/older client counts as unmodeled
        pass
    features: dict[str, FeatureCapability] = {}
    features["hedge_max_size"] = FeatureCapability(
        implemented=True,
        client_supported="hedgeMaxSize" in modeled,
        availability="unknown",
        reason="Order field hedgeMaxSize (TWS API 10.47); REST surface ready, "
        "wire support arrives with an ib_async release that models the field.",
    )
    features["overnight_conditions"] = FeatureCapability(
        implemented=True,
        client_supported="conditionsIncludeOvernight" in modeled,
        availability="unknown",
        reason="Order field conditionsIncludeOvernight (TWS API 10.47); REST "
        "surface ready, wire support arrives with an ib_async release that "
        "models the field.",
    )
    features["deactivated_orders_in_open_orders"] = FeatureCapability(
        implemented=True,
        client_supported=None,
        availability="unknown",
        reason="TWS API 10.47: reqOpenOrders responses include de-activated "
        "orders; load_open_orders filters them unless include_deactivated is set.",
    )
    features["odd_lot_ticks"] = FeatureCapability(
        implemented=False,
        client_supported=False,
        availability="unsupported",
        reason="Tick types 105-110 (oddLotBid/Ask and sizes) need an ib_async "
        "decoder release; no local mapping yet.",
    )
    features["fractional_last_sizes"] = FeatureCapability(
        implemented=False,
        client_supported=False,
        availability="unsupported",
        reason="Fractional last sizes in tickSize responses need an ib_async "
        "decoder release; no local mapping yet.",
    )
    return features
