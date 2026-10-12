"""Optional OpenTelemetry tracing for the REST API.

Telemetry is opt-in: it stays off unless ``OTEL_ENABLED`` is set. When off,
no OpenTelemetry module is imported and the app behaves exactly as before.
When on, the app exports traces over OTLP/HTTP (no gRPC dependency, so the
image stays slim) and instruments:

- the FastAPI/ASGI request path (server spans),
- outbound ``httpx`` calls (IBKR REST),
- ``redis`` commands (connection control, latest-bar cache).
"""
from __future__ import annotations

import logging

from fastapi import FastAPI

from src.config.settings import Settings

logger = logging.getLogger(__name__)

_libraries_instrumented = False


def _traces_url(endpoint: str) -> str:
    """Normalize an OTLP endpoint to a full traces URL.

    Accepts either a base endpoint (``http://collector:4318``) or a full
    traces URL. Per the OTel spec, base endpoints get ``/v1/traces`` appended.
    """
    base = endpoint.rstrip("/")
    if base.endswith("/traces"):
        return base
    return f"{base}/v1/traces"


def configure_opentelemetry(settings: Settings, app: FastAPI | None = None) -> bool:
    """Enable tracing when ``OTEL_ENABLED`` is set; return True when active.

    Imports live inside this function so module import stays cheap and the
    dependency graph is only touched when telemetry is actually on.
    """
    global _libraries_instrumented

    if not settings.otel_enabled:
        return False
    if not settings.otel_exporter_otlp_endpoint:
        logger.warning(
            "OTEL_ENABLED is set but OTEL_EXPORTER_OTLP_ENDPOINT is empty; telemetry stays off"
        )
        return False

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.instrumentation.redis import RedisInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(
        resource=Resource.create({"service.name": settings.otel_service_name}),
    )
    provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=_traces_url(settings.otel_exporter_otlp_endpoint))
        ),
    )
    trace.set_tracer_provider(provider)

    if not _libraries_instrumented:
        HTTPXClientInstrumentor().instrument()
        RedisInstrumentor().instrument()
        _libraries_instrumented = True

    if app is not None:
        FastAPIInstrumentor.instrument_app(app)

    logger.info(
        "OpenTelemetry tracing enabled (service=%s, endpoint=%s)",
        settings.otel_service_name,
        settings.otel_exporter_otlp_endpoint,
    )
    return True
