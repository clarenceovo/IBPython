from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from src.config import config_constant as constants
from src.config.config_loader import ConfigLoader


class Settings(BaseModel):
    """Validated application settings loaded through ConfigLoader."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    ibkr_host: str = Field(default=constants.DEFAULT_IBKR_HOST, alias=constants.IBKR_HOST_ENV)
    ibkr_port: int = Field(default=constants.DEFAULT_IBKR_PORT, alias=constants.IBKR_PORT_ENV)
    ibkr_client_id: int = Field(default=constants.DEFAULT_IBKR_CLIENT_ID, alias=constants.IBKR_CLIENT_ID_ENV)

    redis_url: str = Field(default=constants.DEFAULT_REDIS_URL, alias=constants.REDIS_URL_ENV)
    redis_password: str = Field(default=constants.DEFAULT_REDIS_PASSWORD, alias=constants.REDIS_PASSWORD_ENV)
    ibkr_rest_base_url: str = Field(default=constants.DEFAULT_IBKR_REST_BASE_URL, alias=constants.IBKR_REST_BASE_URL_ENV)

    questdb_host: str = Field(default=constants.DEFAULT_QUESTDB_HOST, alias=constants.QUESTDB_HOST_ENV)
    questdb_port: int = Field(default=constants.DEFAULT_QUESTDB_PORT, alias=constants.QUESTDB_PORT_ENV)
    questdb_user: str = Field(default=constants.DEFAULT_QUESTDB_USER, alias=constants.QUESTDB_USER_ENV)
    questdb_password: str = Field(default=constants.DEFAULT_QUESTDB_PASSWORD, alias=constants.QUESTDB_PASSWORD_ENV)
    questdb_database: str = Field(default=constants.DEFAULT_QUESTDB_DATABASE, alias=constants.QUESTDB_DATABASE_ENV)

    mysql_host: str = Field(default=constants.DEFAULT_MYSQL_HOST, alias=constants.MYSQL_HOST_ENV)
    mysql_port: int = Field(default=constants.DEFAULT_MYSQL_PORT, alias=constants.MYSQL_PORT_ENV)
    mysql_user: str = Field(default=constants.DEFAULT_MYSQL_USER, alias=constants.MYSQL_USER_ENV)
    mysql_password: str = Field(default=constants.DEFAULT_MYSQL_PASSWORD, alias=constants.MYSQL_PASSWORD_ENV)
    mysql_database: str = Field(default=constants.DEFAULT_MYSQL_DATABASE, alias=constants.MYSQL_DATABASE_ENV)
    market_data_db_backend: str = Field(
        default=constants.DEFAULT_MARKET_DATA_DB_BACKEND,
        alias=constants.MARKET_DATA_DB_BACKEND_ENV,
        pattern="^(questdb|mysql)$",
    )

    index_sync_interval_seconds: int = Field(
        default=constants.DEFAULT_INDEX_SYNC_INTERVAL_SECONDS,
        alias=constants.INDEX_SYNC_INTERVAL_SECONDS_ENV,
        gt=0,
    )
    ibkr_market_data_lines: int = Field(
        default=constants.DEFAULT_IBKR_MARKET_DATA_LINES,
        alias=constants.IBKR_MARKET_DATA_LINES_ENV,
        gt=0,
    )
    index_composition_provider: str = Field(
        default=constants.DEFAULT_INDEX_COMPOSITION_PROVIDER,
        alias=constants.INDEX_COMPOSITION_PROVIDER_ENV,
    )
    fixed_income_reference_provider: str = Field(
        default=constants.DEFAULT_FIXED_INCOME_REFERENCE_PROVIDER,
        alias=constants.FIXED_INCOME_REFERENCE_PROVIDER_ENV,
    )
    ibkr_rest_app_name: str = Field(
        default=constants.DEFAULT_IBKR_REST_APP_NAME,
        alias=constants.IBKR_REST_APP_NAME_ENV,
        min_length=1,
    )
    ibkr_rest_connect_on_startup: bool = Field(
        default=constants.DEFAULT_IBKR_REST_CONNECT_ON_STARTUP,
        alias=constants.IBKR_REST_CONNECT_ON_STARTUP_ENV,
    )
    ibkr_rest_market_data_ttl_seconds: float = Field(
        default=constants.DEFAULT_IBKR_REST_MARKET_DATA_TTL_SECONDS,
        alias=constants.IBKR_REST_MARKET_DATA_TTL_SECONDS_ENV,
        ge=0,
    )
    ibkr_rest_market_data_cache_maxsize: int = Field(
        default=constants.DEFAULT_IBKR_REST_MARKET_DATA_CACHE_MAXSIZE,
        alias=constants.IBKR_REST_MARKET_DATA_CACHE_MAXSIZE_ENV,
        gt=0,
    )
    ibkr_equity_snapshot_wait_seconds: float = Field(
        default=constants.DEFAULT_IBKR_EQUITY_SNAPSHOT_WAIT_SECONDS,
        alias=constants.IBKR_EQUITY_SNAPSHOT_WAIT_SECONDS_ENV,
        gt=0,
    )
    ibkr_equity_snapshot_lease_ttl_seconds: float = Field(
        default=constants.DEFAULT_IBKR_EQUITY_SNAPSHOT_LEASE_TTL_SECONDS,
        alias=constants.IBKR_EQUITY_SNAPSHOT_LEASE_TTL_SECONDS_ENV,
        gt=0,
    )
    ibkr_historical_max_chunks: int = Field(
        default=constants.DEFAULT_IBKR_HISTORICAL_MAX_CHUNKS,
        alias=constants.IBKR_HISTORICAL_MAX_CHUNKS_ENV,
        gt=0,
    )
    ibpython_live_smoke: bool = Field(
        default=constants.DEFAULT_IBPYTHON_LIVE_SMOKE,
        alias=constants.IBPYTHON_LIVE_SMOKE_ENV,
    )
    ibkr_order_auth_redis_key: str = Field(
        default=constants.DEFAULT_IBKR_ORDER_AUTH_REDIS_KEY,
        alias=constants.IBKR_ORDER_AUTH_REDIS_KEY_ENV,
        min_length=1,
    )
    ibkr_rate_limit_enabled: bool = Field(
        default=constants.DEFAULT_IBKR_RATE_LIMIT_ENABLED,
        alias=constants.IBKR_RATE_LIMIT_ENABLED_ENV,
    )
    ibkr_rate_limit_global_messages_per_second: int = Field(
        default=constants.DEFAULT_IBKR_RATE_LIMIT_GLOBAL_MESSAGES_PER_SECOND,
        alias=constants.IBKR_RATE_LIMIT_GLOBAL_MESSAGES_PER_SECOND_ENV,
        gt=0,
    )
    ibkr_rate_limit_market_data_reserve: int | None = Field(
        default=None,
        alias=constants.IBKR_RATE_LIMIT_MARKET_DATA_RESERVE_ENV,
        ge=0,
    )
    ibkr_rate_limit_market_data_lease_ttl_seconds: float = Field(
        default=constants.DEFAULT_IBKR_RATE_LIMIT_MARKET_DATA_LEASE_TTL_SECONDS,
        alias=constants.IBKR_RATE_LIMIT_MARKET_DATA_LEASE_TTL_SECONDS_ENV,
        gt=0,
    )

    ibkr_api_bearer_token: str = Field(
        default=constants.DEFAULT_IBKR_API_BEARER_TOKEN,
        alias=constants.IBKR_API_BEARER_TOKEN_ENV,
    )
    ibkr_web_api_base_url: str = Field(
        default=constants.DEFAULT_IBKR_WEB_API_BASE_URL,
        alias=constants.IBKR_WEB_API_BASE_URL_ENV,
        min_length=1,
    )
    ibkr_web_api_bearer_token: str = Field(
        default=constants.DEFAULT_IBKR_WEB_API_BEARER_TOKEN,
        alias=constants.IBKR_WEB_API_BEARER_TOKEN_ENV,
    )
    ibkr_web_api_cookie: str = Field(
        default=constants.DEFAULT_IBKR_WEB_API_COOKIE,
        alias=constants.IBKR_WEB_API_COOKIE_ENV,
    )
    ibkr_web_api_verify_ssl: bool = Field(
        default=constants.DEFAULT_IBKR_WEB_API_VERIFY_SSL,
        alias=constants.IBKR_WEB_API_VERIFY_SSL_ENV,
    )
    ibkr_event_contracts_live_orders_enabled: bool = Field(
        default=constants.DEFAULT_IBKR_EVENT_CONTRACTS_LIVE_ORDERS_ENABLED,
        alias=constants.IBKR_EVENT_CONTRACTS_LIVE_ORDERS_ENABLED_ENV,
    )

    # Telegram alerting
    telegram_bot_token: str = Field(
        default=constants.DEFAULT_TELEGRAM_BOT_TOKEN,
        alias=constants.TELEGRAM_BOT_TOKEN_ENV,
    )
    telegram_chat_id: str = Field(
        default=constants.DEFAULT_TELEGRAM_CHAT_ID,
        alias=constants.TELEGRAM_CHAT_ID_ENV,
    )
    telegram_log_level: str = Field(
        default=constants.DEFAULT_TELEGRAM_LOG_LEVEL,
        alias=constants.TELEGRAM_LOG_LEVEL_ENV,
    )

    def __init__(self, **data: Any) -> None:
        env_file = data.pop("_env_file", ".env")
        include_os_environ = data.pop("_include_os_environ", True)
        environ = data.pop("_environ", None)
        loaded_values = ConfigLoader(
            env_file=env_file,
            include_os_environ=include_os_environ,
            environ=environ,
        ).load(overrides=data)
        super().__init__(**loaded_values)

    @property
    def questdb_dsn(self) -> str:
        return (
            f"host={self.questdb_host} "
            f"port={self.questdb_port} "
            f"user={self.questdb_user} "
            f"password={self.questdb_password} "
            f"dbname={self.questdb_database}"
        )


def load_settings(
    *,
    env_file: str | Path | None = ".env",
    include_os_environ: bool = True,
    environ: dict[str, str] | None = None,
    **overrides: Any,
) -> Settings:
    return Settings(
        _env_file=env_file,
        _include_os_environ=include_os_environ,
        _environ=environ,
        **overrides,
    )
