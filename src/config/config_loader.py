from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from src.config import config_constant as constants


ValueParser = Callable[[Any], Any]


@dataclass(frozen=True)
class ConfigValueSpec:
    field_name: str
    env_name: str
    default: Any
    parser: ValueParser


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def parse_str(value: Any) -> str:
    return str(value).strip()


def parse_market_data_db_backend(value: Any) -> str:
    return parse_str(value).lower()


CONFIG_VALUE_SPECS: tuple[ConfigValueSpec, ...] = (
    ConfigValueSpec("ibkr_host", constants.IBKR_HOST_ENV, constants.DEFAULT_IBKR_HOST, parse_str),
    ConfigValueSpec("ibkr_port", constants.IBKR_PORT_ENV, constants.DEFAULT_IBKR_PORT, int),
    ConfigValueSpec("ibkr_client_id", constants.IBKR_CLIENT_ID_ENV, constants.DEFAULT_IBKR_CLIENT_ID, int),
    ConfigValueSpec("redis_url", constants.REDIS_URL_ENV, constants.DEFAULT_REDIS_URL, parse_str),
    ConfigValueSpec("redis_password", constants.REDIS_PASSWORD_ENV, constants.DEFAULT_REDIS_PASSWORD, parse_str),
    ConfigValueSpec("ibkr_rest_base_url", constants.IBKR_REST_BASE_URL_ENV, constants.DEFAULT_IBKR_REST_BASE_URL, parse_str),
    ConfigValueSpec("questdb_host", constants.QUESTDB_HOST_ENV, constants.DEFAULT_QUESTDB_HOST, parse_str),
    ConfigValueSpec("questdb_port", constants.QUESTDB_PORT_ENV, constants.DEFAULT_QUESTDB_PORT, int),
    ConfigValueSpec("questdb_write_port", constants.QUESTDB_WRITE_PORT_ENV, constants.DEFAULT_QUESTDB_WRITE_PORT, int),
    ConfigValueSpec("questdb_user", constants.QUESTDB_USER_ENV, constants.DEFAULT_QUESTDB_USER, parse_str),
    ConfigValueSpec("questdb_password", constants.QUESTDB_PASSWORD_ENV, constants.DEFAULT_QUESTDB_PASSWORD, parse_str),
    ConfigValueSpec("questdb_database", constants.QUESTDB_DATABASE_ENV, constants.DEFAULT_QUESTDB_DATABASE, parse_str),
    ConfigValueSpec("mysql_host", constants.MYSQL_HOST_ENV, constants.DEFAULT_MYSQL_HOST, parse_str),
    ConfigValueSpec("mysql_port", constants.MYSQL_PORT_ENV, constants.DEFAULT_MYSQL_PORT, int),
    ConfigValueSpec("mysql_user", constants.MYSQL_USER_ENV, constants.DEFAULT_MYSQL_USER, parse_str),
    ConfigValueSpec("mysql_password", constants.MYSQL_PASSWORD_ENV, constants.DEFAULT_MYSQL_PASSWORD, parse_str),
    ConfigValueSpec("mysql_database", constants.MYSQL_DATABASE_ENV, constants.DEFAULT_MYSQL_DATABASE, parse_str),
    ConfigValueSpec(
        "market_data_db_backend",
        constants.MARKET_DATA_DB_BACKEND_ENV,
        constants.DEFAULT_MARKET_DATA_DB_BACKEND,
        parse_market_data_db_backend,
    ),
    ConfigValueSpec(
        "index_sync_interval_seconds",
        constants.INDEX_SYNC_INTERVAL_SECONDS_ENV,
        constants.DEFAULT_INDEX_SYNC_INTERVAL_SECONDS,
        int,
    ),
    ConfigValueSpec(
        "ibkr_market_data_lines",
        constants.IBKR_MARKET_DATA_LINES_ENV,
        constants.DEFAULT_IBKR_MARKET_DATA_LINES,
        int,
    ),
    ConfigValueSpec(
        "index_composition_provider",
        constants.INDEX_COMPOSITION_PROVIDER_ENV,
        constants.DEFAULT_INDEX_COMPOSITION_PROVIDER,
        parse_str,
    ),
    ConfigValueSpec(
        "fixed_income_reference_provider",
        constants.FIXED_INCOME_REFERENCE_PROVIDER_ENV,
        constants.DEFAULT_FIXED_INCOME_REFERENCE_PROVIDER,
        parse_str,
    ),
    ConfigValueSpec(
        "ibkr_rest_app_name",
        constants.IBKR_REST_APP_NAME_ENV,
        constants.DEFAULT_IBKR_REST_APP_NAME,
        parse_str,
    ),
    ConfigValueSpec(
        "ibkr_rest_connect_on_startup",
        constants.IBKR_REST_CONNECT_ON_STARTUP_ENV,
        constants.DEFAULT_IBKR_REST_CONNECT_ON_STARTUP,
        parse_bool,
    ),
    ConfigValueSpec(
        "ibkr_rest_market_data_ttl_seconds",
        constants.IBKR_REST_MARKET_DATA_TTL_SECONDS_ENV,
        constants.DEFAULT_IBKR_REST_MARKET_DATA_TTL_SECONDS,
        float,
    ),
    ConfigValueSpec(
        "ibkr_rest_market_data_cache_maxsize",
        constants.IBKR_REST_MARKET_DATA_CACHE_MAXSIZE_ENV,
        constants.DEFAULT_IBKR_REST_MARKET_DATA_CACHE_MAXSIZE,
        int,
    ),
    ConfigValueSpec(
        "ibkr_market_depth_request_timeout_seconds",
        constants.IBKR_MARKET_DEPTH_REQUEST_TIMEOUT_SECONDS_ENV,
        constants.DEFAULT_IBKR_MARKET_DEPTH_REQUEST_TIMEOUT_SECONDS,
        float,
    ),
    ConfigValueSpec(
        "ibkr_market_depth_lease_wait_seconds",
        constants.IBKR_MARKET_DEPTH_LEASE_WAIT_SECONDS_ENV,
        constants.DEFAULT_IBKR_MARKET_DEPTH_LEASE_WAIT_SECONDS,
        float,
    ),
    ConfigValueSpec(
        "ibkr_market_depth_cache_ttl_seconds",
        constants.IBKR_MARKET_DEPTH_CACHE_TTL_SECONDS_ENV,
        constants.DEFAULT_IBKR_MARKET_DEPTH_CACHE_TTL_SECONDS,
        float,
    ),
    ConfigValueSpec(
        "ibkr_rest_ohlcv_rate_limit_retry_delay_seconds",
        constants.IBKR_REST_OHLCV_RATE_LIMIT_RETRY_DELAY_SECONDS_ENV,
        constants.DEFAULT_IBKR_REST_OHLCV_RATE_LIMIT_RETRY_DELAY_SECONDS,
        float,
    ),
    ConfigValueSpec(
        "ibkr_rest_ohlcv_rate_limit_retry_count",
        constants.IBKR_REST_OHLCV_RATE_LIMIT_RETRY_COUNT_ENV,
        constants.DEFAULT_IBKR_REST_OHLCV_RATE_LIMIT_RETRY_COUNT,
        int,
    ),
    ConfigValueSpec(
        "ibkr_equity_snapshot_wait_seconds",
        constants.IBKR_EQUITY_SNAPSHOT_WAIT_SECONDS_ENV,
        constants.DEFAULT_IBKR_EQUITY_SNAPSHOT_WAIT_SECONDS,
        float,
    ),
    ConfigValueSpec(
        "ibkr_equity_snapshot_lease_ttl_seconds",
        constants.IBKR_EQUITY_SNAPSHOT_LEASE_TTL_SECONDS_ENV,
        constants.DEFAULT_IBKR_EQUITY_SNAPSHOT_LEASE_TTL_SECONDS,
        float,
    ),
    ConfigValueSpec(
        "ibkr_historical_max_chunks",
        constants.IBKR_HISTORICAL_MAX_CHUNKS_ENV,
        constants.DEFAULT_IBKR_HISTORICAL_MAX_CHUNKS,
        int,
    ),
    ConfigValueSpec(
        "ibpython_live_smoke",
        constants.IBPYTHON_LIVE_SMOKE_ENV,
        constants.DEFAULT_IBPYTHON_LIVE_SMOKE,
        parse_bool,
    ),
    ConfigValueSpec(
        "ibkr_api_bearer_token",
        constants.IBKR_API_BEARER_TOKEN_ENV,
        constants.DEFAULT_IBKR_API_BEARER_TOKEN,
        parse_str,
    ),
    ConfigValueSpec("ibkr_web_api_base_url", constants.IBKR_WEB_API_BASE_URL_ENV, constants.DEFAULT_IBKR_WEB_API_BASE_URL, parse_str),
    ConfigValueSpec("ibkr_web_api_bearer_token", constants.IBKR_WEB_API_BEARER_TOKEN_ENV, constants.DEFAULT_IBKR_WEB_API_BEARER_TOKEN, parse_str),
    ConfigValueSpec("ibkr_web_api_cookie", constants.IBKR_WEB_API_COOKIE_ENV, constants.DEFAULT_IBKR_WEB_API_COOKIE, parse_str),
    ConfigValueSpec("ibkr_web_api_verify_ssl", constants.IBKR_WEB_API_VERIFY_SSL_ENV, constants.DEFAULT_IBKR_WEB_API_VERIFY_SSL, parse_bool),
    ConfigValueSpec(
        "ibkr_event_contracts_live_orders_enabled",
        constants.IBKR_EVENT_CONTRACTS_LIVE_ORDERS_ENABLED_ENV,
        constants.DEFAULT_IBKR_EVENT_CONTRACTS_LIVE_ORDERS_ENABLED,
        parse_bool,
    ),
)

_SPECS_BY_FIELD_NAME = {spec.field_name: spec for spec in CONFIG_VALUE_SPECS}
_FIELD_NAME_BY_ENV_NAME = {spec.env_name: spec.field_name for spec in CONFIG_VALUE_SPECS}
_CANONICAL_FIELD_NAME_BY_ENV_NAME = {
    "IBKR_HOST": "ibkr_host",
    "IBKR_PORT": "ibkr_port",
    "IBKR_CLIENT_ID": "ibkr_client_id",
    "REDIS_URL": "redis_url",
    "REDIS_PASSWORD": "redis_password",
    "IBKR_REST_BASE_URL": "ibkr_rest_base_url",
    "QUESTDB_HOST": "questdb_host",
    "QUESTDB_PORT": "questdb_port",
    "QUESTDB_WRITE_PORT": "questdb_write_port",
    "QUESTDB_USER": "questdb_user",
    "QUESTDB_PASSWORD": "questdb_password",
    "QUESTDB_DATABASE": "questdb_database",
    "MYSQL_HOST": "mysql_host",
    "MYSQL_PORT": "mysql_port",
    "MYSQL_USER": "mysql_user",
    "MYSQL_PASSWORD": "mysql_password",
    "MYSQL_DATABASE": "mysql_database",
    "MARKET_DATA_DB_BACKEND": "market_data_db_backend",
    "INDEX_SYNC_INTERVAL_SECONDS": "index_sync_interval_seconds",
    "IBKR_MARKET_DATA_LINES": "ibkr_market_data_lines",
    "INDEX_COMPOSITION_PROVIDER": "index_composition_provider",
    "FIXED_INCOME_REFERENCE_PROVIDER": "fixed_income_reference_provider",
    "IBKR_REST_APP_NAME": "ibkr_rest_app_name",
    "IBKR_REST_CONNECT_ON_STARTUP": "ibkr_rest_connect_on_startup",
    "IBKR_REST_MARKET_DATA_TTL_SECONDS": "ibkr_rest_market_data_ttl_seconds",
    "IBKR_REST_MARKET_DATA_CACHE_MAXSIZE": "ibkr_rest_market_data_cache_maxsize",
    "IBKR_MARKET_DEPTH_REQUEST_TIMEOUT_SECONDS": "ibkr_market_depth_request_timeout_seconds",
    "IBKR_MARKET_DEPTH_LEASE_WAIT_SECONDS": "ibkr_market_depth_lease_wait_seconds",
    "IBKR_MARKET_DEPTH_CACHE_TTL_SECONDS": "ibkr_market_depth_cache_ttl_seconds",
    "IBKR_REST_OHLCV_RATE_LIMIT_RETRY_DELAY_SECONDS": "ibkr_rest_ohlcv_rate_limit_retry_delay_seconds",
    "IBKR_REST_OHLCV_RATE_LIMIT_RETRY_COUNT": "ibkr_rest_ohlcv_rate_limit_retry_count",
    "IBKR_EQUITY_SNAPSHOT_WAIT_SECONDS": "ibkr_equity_snapshot_wait_seconds",
    "IBKR_EQUITY_SNAPSHOT_LEASE_TTL_SECONDS": "ibkr_equity_snapshot_lease_ttl_seconds",
    "IBKR_HISTORICAL_MAX_CHUNKS": "ibkr_historical_max_chunks",
    "IBPYTHON_LIVE_SMOKE": "ibpython_live_smoke",
    "IBKR_API_BEARER_TOKEN": "ibkr_api_bearer_token",
    "IBKR_WEB_API_BASE_URL": "ibkr_web_api_base_url",
    "IBKR_WEB_API_BEARER_TOKEN": "ibkr_web_api_bearer_token",
    "IBKR_WEB_API_COOKIE": "ibkr_web_api_cookie",
    "IBKR_WEB_API_VERIFY_SSL": "ibkr_web_api_verify_ssl",
    "IBKR_EVENT_CONTRACTS_LIVE_ORDERS_ENABLED": "ibkr_event_contracts_live_orders_enabled",
}


class ConfigLoader:
    """Load app configuration from defaults, `.env`, environment variables, and explicit overrides."""

    def __init__(
        self,
        *,
        env_file: str | Path | None = ".env",
        include_os_environ: bool = True,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        self.env_file = Path(env_file) if env_file is not None else None
        self.include_os_environ = include_os_environ
        self.environ = environ if environ is not None else os.environ

    def load(self, overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
        values = default_config_values()
        self._apply_source(values, _load_dotenv_values(self.env_file))
        if self.include_os_environ:
            self._apply_source(values, self.environ)
        if overrides:
            self._apply_source(values, overrides)
        return values

    @staticmethod
    def _apply_source(values: dict[str, Any], source: Mapping[str, Any]) -> None:
        for raw_key, raw_value in source.items():
            field_name = _FIELD_NAME_BY_ENV_NAME.get(raw_key) or _CANONICAL_FIELD_NAME_BY_ENV_NAME.get(raw_key) or raw_key
            spec = _SPECS_BY_FIELD_NAME.get(field_name)
            if spec is None or not _has_value(raw_value):
                continue
            values[field_name] = spec.parser(raw_value)


def default_config_values() -> dict[str, Any]:
    return {spec.field_name: spec.default for spec in CONFIG_VALUE_SPECS}


def load_config_values(
    *,
    env_file: str | Path | None = ".env",
    include_os_environ: bool = True,
    environ: Mapping[str, str] | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return ConfigLoader(
        env_file=env_file,
        include_os_environ=include_os_environ,
        environ=environ,
    ).load(overrides=overrides)


def load_settings(**overrides: Any) -> Any:
    from src.config.settings import Settings

    return Settings(**overrides)


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    return True


def _load_dotenv_values(env_file: Path | None) -> dict[str, str]:
    if env_file is None or not env_file.exists():
        return {}
    try:
        from dotenv import dotenv_values
    except ImportError:
        return _load_dotenv_values_without_dependency(env_file)
    return {key: value for key, value in dotenv_values(env_file).items() if value is not None}


def _load_dotenv_values_without_dependency(env_file: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in env_file.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if not key:
            continue
        values[key] = _strip_quotes(value.strip())
    return values


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value
