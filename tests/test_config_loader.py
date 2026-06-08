from __future__ import annotations

from pathlib import Path

from src.config import config_constant as constants
from src.config.config_loader import ConfigLoader, default_config_values, load_config_values
from src.config.settings import Settings, load_settings


def test_default_config_values_come_from_config_constants() -> None:
    values = default_config_values()

    assert values["ibkr_host"] == constants.DEFAULT_IBKR_HOST
    assert values["ibkr_port"] == constants.DEFAULT_IBKR_PORT
    assert values["questdb_database"] == constants.DEFAULT_QUESTDB_DATABASE
    assert values["questdb_write_port"] == constants.DEFAULT_QUESTDB_WRITE_PORT
    assert values["redis_password"] == constants.DEFAULT_REDIS_PASSWORD
    assert values["market_data_db_backend"] == constants.DEFAULT_MARKET_DATA_DB_BACKEND
    assert values["ibkr_rest_market_data_cache_maxsize"] == constants.DEFAULT_IBKR_REST_MARKET_DATA_CACHE_MAXSIZE
    assert values["ibkr_rest_ohlcv_rate_limit_retry_delay_seconds"] == constants.DEFAULT_IBKR_REST_OHLCV_RATE_LIMIT_RETRY_DELAY_SECONDS
    assert values["ibkr_rest_ohlcv_rate_limit_retry_count"] == constants.DEFAULT_IBKR_REST_OHLCV_RATE_LIMIT_RETRY_COUNT
    assert values["ibkr_market_depth_request_timeout_seconds"] == constants.DEFAULT_IBKR_MARKET_DEPTH_REQUEST_TIMEOUT_SECONDS
    assert values["ibkr_market_depth_lease_wait_seconds"] == constants.DEFAULT_IBKR_MARKET_DEPTH_LEASE_WAIT_SECONDS
    assert values["ibkr_market_depth_cache_ttl_seconds"] == constants.DEFAULT_IBKR_MARKET_DEPTH_CACHE_TTL_SECONDS
    assert values["ibkr_equity_snapshot_wait_seconds"] == constants.DEFAULT_IBKR_EQUITY_SNAPSHOT_WAIT_SECONDS
    assert values["ibkr_historical_max_chunks"] == constants.DEFAULT_IBKR_HISTORICAL_MAX_CHUNKS


def test_env_name_constants_are_canonical_names() -> None:
    assert constants.IBKR_HOST_ENV == "IBKR_HOST"
    assert constants.IBKR_PORT_ENV == "IBKR_PORT"
    assert constants.IBKR_CLIENT_ID_ENV == "IBKR_CLIENT_ID"
    assert constants.IBKR_MCP_CLIENT_ID_ENV == "IBKR_MCP_CLIENT_ID"
    assert constants.REDIS_URL_ENV == "REDIS_URL"
    assert constants.REDIS_PASSWORD_ENV == "REDIS_PASSWORD"
    assert constants.QUESTDB_HOST_ENV == "QUESTDB_HOST"
    assert constants.QUESTDB_WRITE_PORT_ENV == "QUESTDB_WRITE_PORT"
    assert constants.MARKET_DATA_DB_BACKEND_ENV == "MARKET_DATA_DB_BACKEND"
    assert constants.IBKR_REST_OHLCV_RATE_LIMIT_RETRY_DELAY_SECONDS_ENV == "IBKR_REST_OHLCV_RATE_LIMIT_RETRY_DELAY_SECONDS"
    assert constants.IBKR_REST_OHLCV_RATE_LIMIT_RETRY_COUNT_ENV == "IBKR_REST_OHLCV_RATE_LIMIT_RETRY_COUNT"
    assert constants.IBKR_MARKET_DEPTH_REQUEST_TIMEOUT_SECONDS_ENV == "IBKR_MARKET_DEPTH_REQUEST_TIMEOUT_SECONDS"
    assert constants.IBKR_MARKET_DEPTH_LEASE_WAIT_SECONDS_ENV == "IBKR_MARKET_DEPTH_LEASE_WAIT_SECONDS"
    assert constants.IBKR_MARKET_DEPTH_CACHE_TTL_SECONDS_ENV == "IBKR_MARKET_DEPTH_CACHE_TTL_SECONDS"


def test_config_loader_uses_defaults_when_env_file_is_missing() -> None:
    values = ConfigLoader(env_file=Path("/tmp/does-not-exist.env"), include_os_environ=False).load()

    assert values["ibkr_host"] == constants.DEFAULT_IBKR_HOST
    assert values["ibkr_mcp_client_id"] == constants.DEFAULT_IBKR_MCP_CLIENT_ID
    assert values["redis_url"] == constants.DEFAULT_REDIS_URL
    assert values["ibkr_rest_base_url"] == constants.DEFAULT_IBKR_REST_BASE_URL


def test_mcp_client_id_is_independent_from_default_ibkr_client_id(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("IBKR_CLIENT_ID=101\nIBKR_MCP_CLIENT_ID=301\n", encoding="utf-8")

    settings = load_settings(env_file=env_file, include_os_environ=False)

    assert settings.ibkr_client_id == 101
    assert settings.ibkr_mcp_client_id == 301


def test_config_loader_ignores_blank_dotenv_values_and_parses_types(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "IBKR_HOST=",
                "IBKR_PORT=4002",
                "MARKET_DATA_DB_BACKEND=MYSQL",
                "IBKR_REST_CONNECT_ON_STARTUP=true",
                "IBKR_REST_MARKET_DATA_TTL_SECONDS=12.5",
                "IBKR_MARKET_DEPTH_REQUEST_TIMEOUT_SECONDS=4",
                "IBKR_MARKET_DEPTH_LEASE_WAIT_SECONDS=0.5",
                "IBKR_MARKET_DEPTH_CACHE_TTL_SECONDS=0.1",
                "IBKR_REST_OHLCV_RATE_LIMIT_RETRY_DELAY_SECONDS=45",
                "IBKR_REST_OHLCV_RATE_LIMIT_RETRY_COUNT=2",
                "IBKR_EQUITY_SNAPSHOT_WAIT_SECONDS=9.5",
                "IBKR_HISTORICAL_MAX_CHUNKS=12",
                "IBPYTHON_LIVE_SMOKE=true",
            ]
        ),
        encoding="utf-8",
    )

    values = ConfigLoader(env_file=env_file, include_os_environ=False).load()

    assert values["ibkr_host"] == constants.DEFAULT_IBKR_HOST
    assert values["ibkr_port"] == 4002
    assert values["market_data_db_backend"] == "mysql"
    assert values["ibkr_rest_connect_on_startup"] is True
    assert values["ibkr_rest_market_data_ttl_seconds"] == 12.5
    assert values["ibkr_market_depth_request_timeout_seconds"] == 4.0
    assert values["ibkr_market_depth_lease_wait_seconds"] == 0.5
    assert values["ibkr_market_depth_cache_ttl_seconds"] == 0.1
    assert values["ibkr_rest_ohlcv_rate_limit_retry_delay_seconds"] == 45.0
    assert values["ibkr_rest_ohlcv_rate_limit_retry_count"] == 2
    assert values["ibkr_equity_snapshot_wait_seconds"] == 9.5
    assert values["ibkr_historical_max_chunks"] == 12
    assert values["ibpython_live_smoke"] is True


def test_environment_overrides_dotenv_values(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("IBKR_HOST=dotenv-host\nIBKR_PORT=4002\nREDIS_PASSWORD=dotenv-secret\n", encoding="utf-8")

    values = ConfigLoader(
        env_file=env_file,
        include_os_environ=True,
        environ={"IBKR_HOST": "env-host"},
    ).load()

    assert values["ibkr_host"] == "env-host"
    assert values["ibkr_port"] == 4002
    assert values["redis_password"] == "dotenv-secret"


def test_settings_and_load_settings_use_config_loader(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "QUESTDB_HOST=questdb.local\nQUESTDB_WRITE_PORT=9009\nIBKR_REST_APP_NAME=\nREDIS_PASSWORD=redis-secret\n",
        encoding="utf-8",
    )

    settings = Settings(_env_file=env_file, _include_os_environ=False)
    loaded = load_settings(env_file=env_file, include_os_environ=False)

    assert settings.questdb_host == "questdb.local"
    assert settings.questdb_write_port == 9009
    assert settings.ibkr_rest_app_name == constants.DEFAULT_IBKR_REST_APP_NAME
    assert settings.ibkr_rest_ohlcv_rate_limit_retry_delay_seconds == constants.DEFAULT_IBKR_REST_OHLCV_RATE_LIMIT_RETRY_DELAY_SECONDS
    assert settings.ibkr_rest_ohlcv_rate_limit_retry_count == constants.DEFAULT_IBKR_REST_OHLCV_RATE_LIMIT_RETRY_COUNT
    assert settings.ibkr_equity_snapshot_wait_seconds == constants.DEFAULT_IBKR_EQUITY_SNAPSHOT_WAIT_SECONDS
    assert settings.ibkr_historical_max_chunks == constants.DEFAULT_IBKR_HISTORICAL_MAX_CHUNKS
    assert settings.redis_password == "redis-secret"
    assert loaded.questdb_host == settings.questdb_host


def test_load_config_values_allows_field_name_and_env_name_overrides() -> None:
    values = load_config_values(
        env_file=None,
        include_os_environ=False,
        environ={},
        overrides={
            "ibkr_host": "field-host",
            "IBKR_PORT": "5000",
            "REDIS_URL": "",
        },
    )

    assert values["ibkr_host"] == "field-host"
    assert values["ibkr_port"] == 5000
    assert values["redis_url"] == constants.DEFAULT_REDIS_URL
