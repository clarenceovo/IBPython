"""Local implementation capabilities; no network calls or entitlement guesses."""
from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from typing import Any, Literal

from pydantic import BaseModel

FUNDAMENTALS_REMOVED = (
    "IBKR removed fundamental report requests in API 10.47. "
    "This gateway no longer requests fundamental reports; WSH events and dividend ticks are separate data products."
)


class FeatureCapability(BaseModel):
    implemented: bool
    client_supported: bool | None
    availability: Literal["unknown", "unsupported"]
    reason: str


class GatewayCapabilities(BaseModel):
    client_package: str = "ib-insync"
    client_version: str | None
    connected: bool
    negotiated_server_protocol: int | None
    features: dict[str, FeatureCapability]


def gateway_capabilities(connection: Any) -> GatewayCapabilities:
    try:
        client_version = version("ib-insync")
    except PackageNotFoundError:
        client_version = None
    ib = connection.ib
    connected = connection.is_connected
    protocol = ib.client.serverVersion() if connected else None
    features = {}
    for name, method in (("shortability", "reqMktData"), ("dividends", "reqMktData"), ("all_open_orders", "reqAllOpenOrdersAsync")):
        supported = callable(getattr(ib, method, None)) if ib is not None else None
        features[name] = FeatureCapability(
            implemented=True, client_supported=supported,
            availability="unsupported" if supported is False else "unknown",
            reason="Local implementation exists; live server support, permissions and data availability require a request.",
        )
    features["fundamental_reports"] = FeatureCapability(
        implemented=False, client_supported=False, availability="unsupported", reason=FUNDAMENTALS_REMOVED,
    )
    for name in ("odd_lot_quotes", "settlement_type", "overnight_conditions"):
        features[name] = FeatureCapability(
            implemented=False, client_supported=None, availability="unsupported",
            reason="Not implemented by this gateway; requires verified encoder/decoder support before exposure.",
        )
    return GatewayCapabilities(
        client_version=client_version, connected=connected,
        negotiated_server_protocol=protocol, features=features,
    )
