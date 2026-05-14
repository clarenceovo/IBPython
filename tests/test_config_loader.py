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
    assert values["redis_password"] == constants.DEFAULT_REDIS_PASSWORD
    assert values["ibkr_rest_market_data_cache_maxsize"] == constants.DEFAULT_IBKR_REST_MARKET_DATA_CACHE_MAXSIZE


def test_config_loader_uses_defaults_when_env_file_is_missing() -> None:
    values = ConfigLoader(env_file=Path("/tmp/does-not-exist.env"), include_os_environ=False).load()

    assert values["ibkr_host"] == constants.DEFAULT_IBKR_HOST
    assert values["redis_url"] == constants.DEFAULT_REDIS_URL


def test_config_loader_ignores_blank_dotenv_values_and_parses_types(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "IBKR_HOST=",
                "IBKR_PORT=4002",
                "IBKR_REST_CONNECT_ON_STARTUP=true",
                "IBKR_REST_MARKET_DATA_TTL_SECONDS=12.5",
            ]
        ),
        encoding="utf-8",
    )

    values = ConfigLoader(env_file=env_file, include_os_environ=False).load()

    assert values["ibkr_host"] == constants.DEFAULT_IBKR_HOST
    assert values["ibkr_port"] == 4002
    assert values["ibkr_rest_connect_on_startup"] is True
    assert values["ibkr_rest_market_data_ttl_seconds"] == 12.5


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
    env_file.write_text("QUESTDB_HOST=questdb.local\nIBKR_REST_APP_NAME=\nREDIS_PASSWORD=redis-secret\n", encoding="utf-8")

    settings = Settings(_env_file=env_file, _include_os_environ=False)
    loaded = load_settings(env_file=env_file, include_os_environ=False)

    assert settings.questdb_host == "questdb.local"
    assert settings.ibkr_rest_app_name == constants.DEFAULT_IBKR_REST_APP_NAME
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
