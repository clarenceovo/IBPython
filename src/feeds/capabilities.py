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
    availability: Literal["unknown", "unsupported"]
    reason: str


class GatewayCapabilities(BaseModel):
    client_package: str = "ib-insync"
    client_version: str | None
    connected: bool
    negotiated_server_protocol: int | None
    features: dict[str, FeatureCapability]


def gateway_capabilities(connection: Any) -> GatewayCapabilities:
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
    return GatewayCapabilities(
        client_version=version("ib-insync"), connected=connected,
        negotiated_server_protocol=protocol, features=features,
    )
