"""OpenTelemetry opt-in wiring: off by default, active when configured.

Runs a stub OTLP/HTTP collector so the real export path is exercised without
network noise, and resets the global tracer provider after each test.
"""
from __future__ import annotations

import contextlib
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from opentelemetry import trace

from src.config.settings import Settings
from src.webapp.app import create_app
from src.webapp.telemetry import _traces_url, configure_opentelemetry


def _otel_settings(endpoint: str, **overrides) -> Settings:
    return Settings(
        _env_file=None,
        otel_enabled=True,
        otel_exporter_otlp_endpoint=endpoint,
        otel_service_name="test-otel",
        **overrides,
    )


@pytest.fixture()
def otel_env():
    """Stub OTLP collector plus global-provider reset for test isolation."""
    received: list[bytes] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            received.append(self.rfile.read(length))
            self.send_response(200)
            self.end_headers()

        def log_message(self, format, *args):  # noqa: A002
            del format, args

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        yield endpoint, received
    finally:
        # Flush and reset the global provider while the stub still runs, so
        # pending batches export cleanly instead of retrying at process exit.
        provider = trace.get_tracer_provider()
        if hasattr(provider, "shutdown"):
            with contextlib.suppress(Exception):
                provider.shutdown()
        trace._TRACER_PROVIDER = None  # noqa: SLF001 — test isolation
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def test_disabled_by_default_returns_false():
    settings = Settings(_env_file=None)
    assert settings.otel_enabled is False
    assert configure_opentelemetry(settings) is False


def test_enabled_without_endpoint_stays_off():
    settings = _otel_settings("")
    assert configure_opentelemetry(settings) is False


def test_enabled_exports_spans_to_collector(otel_env):
    endpoint, received = otel_env
    assert configure_opentelemetry(_otel_settings(endpoint)) is True
    provider = trace.get_tracer_provider()
    assert type(provider).__name__ == "TracerProvider"

    with trace.get_tracer("test").start_as_current_span("probe-span"):
        pass

    assert provider.force_flush() is True
    assert received, "collector received no export payload"
    assert b"probe-span" in received[0]
    assert b"test-otel" in received[0]


def test_traces_url_appends_v1_traces_to_base_endpoint():
    assert _traces_url("http://collector:4318") == "http://collector:4318/v1/traces"
    assert _traces_url("http://collector:4318/") == "http://collector:4318/v1/traces"
    assert (
        _traces_url("http://collector:4318/v1/traces")
        == "http://collector:4318/v1/traces"
    )


def test_create_app_enabled_adds_otel_middleware(otel_env):
    endpoint, _received = otel_env
    app = create_app(settings=_otel_settings(endpoint))
    # 0.66b1+ wraps build_middleware_stack instead of touching
    # user_middleware; the official marker records instrumentation.
    assert getattr(app, "_is_instrumented_by_opentelemetry", False) is True


def test_create_app_default_has_no_otel_middleware():
    app = create_app(settings=Settings(_env_file=None))
    assert getattr(app, "_is_instrumented_by_opentelemetry", False) is False
