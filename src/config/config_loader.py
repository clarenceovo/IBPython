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


CONFIG_VALUE_SPECS: tuple[ConfigValueSpec, ...] = (
    ConfigValueSpec("ibkr_host", constants.IBKR_HOST_ENV, constants.DEFAULT_IBKR_HOST, parse_str),
    ConfigValueSpec("ibkr_port", constants.IBKR_PORT_ENV, constants.DEFAULT_IBKR_PORT, int),
    ConfigValueSpec("ibkr_client_id", constants.IBKR_CLIENT_ID_ENV, constants.DEFAULT_IBKR_CLIENT_ID, int),
    ConfigValueSpec("redis_url", constants.REDIS_URL_ENV, constants.DEFAULT_REDIS_URL, parse_str),
    ConfigValueSpec("redis_password", constants.REDIS_PASSWORD_ENV, constants.DEFAULT_REDIS_PASSWORD, parse_str),
    ConfigValueSpec("questdb_host", constants.QUESTDB_HOST_ENV, constants.DEFAULT_QUESTDB_HOST, parse_str),
    ConfigValueSpec("questdb_port", constants.QUESTDB_PORT_ENV, constants.DEFAULT_QUESTDB_PORT, int),
    ConfigValueSpec("questdb_user", constants.QUESTDB_USER_ENV, constants.DEFAULT_QUESTDB_USER, parse_str),
    ConfigValueSpec("questdb_password", constants.QUESTDB_PASSWORD_ENV, constants.DEFAULT_QUESTDB_PASSWORD, parse_str),
    ConfigValueSpec("questdb_database", constants.QUESTDB_DATABASE_ENV, constants.DEFAULT_QUESTDB_DATABASE, parse_str),
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
)

_SPECS_BY_FIELD_NAME = {spec.field_name: spec for spec in CONFIG_VALUE_SPECS}
_FIELD_NAME_BY_ENV_NAME = {spec.env_name: spec.field_name for spec in CONFIG_VALUE_SPECS}
_CANONICAL_FIELD_NAME_BY_ENV_NAME = {
    "IBKR_HOST": "ibkr_host",
    "IBKR_PORT": "ibkr_port",
    "IBKR_CLIENT_ID": "ibkr_client_id",
    "REDIS_URL": "redis_url",
    "REDIS_PASSWORD": "redis_password",
    "QUESTDB_HOST": "questdb_host",
    "QUESTDB_PORT": "questdb_port",
    "QUESTDB_USER": "questdb_user",
    "QUESTDB_PASSWORD": "questdb_password",
    "QUESTDB_DATABASE": "questdb_database",
    "INDEX_SYNC_INTERVAL_SECONDS": "index_sync_interval_seconds",
    "IBKR_MARKET_DATA_LINES": "ibkr_market_data_lines",
    "INDEX_COMPOSITION_PROVIDER": "index_composition_provider",
    "IBKR_REST_APP_NAME": "ibkr_rest_app_name",
    "IBKR_REST_CONNECT_ON_STARTUP": "ibkr_rest_connect_on_startup",
    "IBKR_REST_MARKET_DATA_TTL_SECONDS": "ibkr_rest_market_data_ttl_seconds",
    "IBKR_REST_MARKET_DATA_CACHE_MAXSIZE": "ibkr_rest_market_data_cache_maxsize",
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
