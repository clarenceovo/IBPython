from __future__ import annotations

import os

from pydantic import Field

from src.config import config_constant as constants

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError:  # pragma: no cover - fallback keeps tests importable before deps are installed.
    from pydantic import BaseModel

    class BaseSettings(BaseModel):  # type: ignore[no-redef]
        def __init__(self, **data: object) -> None:
            env_data = {
                "ibkr_host": os.getenv(constants.IBKR_HOST_ENV, constants.DEFAULT_IBKR_HOST),
                "ibkr_port": int(os.getenv(constants.IBKR_PORT_ENV, constants.DEFAULT_IBKR_PORT)),
                "ibkr_client_id": int(os.getenv(constants.IBKR_CLIENT_ID_ENV, constants.DEFAULT_IBKR_CLIENT_ID)),
                "redis_url": os.getenv(constants.REDIS_URL_ENV, constants.DEFAULT_REDIS_URL),
                "questdb_host": os.getenv(constants.QUESTDB_HOST_ENV, constants.DEFAULT_QUESTDB_HOST),
                "questdb_port": int(os.getenv(constants.QUESTDB_PORT_ENV, constants.DEFAULT_QUESTDB_PORT)),
                "questdb_user": os.getenv(constants.QUESTDB_USER_ENV, constants.DEFAULT_QUESTDB_USER),
                "questdb_password": os.getenv(constants.QUESTDB_PASSWORD_ENV, constants.DEFAULT_QUESTDB_PASSWORD),
                "questdb_database": os.getenv(constants.QUESTDB_DATABASE_ENV, constants.DEFAULT_QUESTDB_DATABASE),
                "index_sync_interval_seconds": int(
                    os.getenv(
                        constants.INDEX_SYNC_INTERVAL_SECONDS_ENV,
                        constants.DEFAULT_INDEX_SYNC_INTERVAL_SECONDS,
                    )
                ),
                "ibkr_market_data_lines": int(
                    os.getenv(constants.IBKR_MARKET_DATA_LINES_ENV, constants.DEFAULT_IBKR_MARKET_DATA_LINES)
                ),
                "index_composition_provider": os.getenv(
                    constants.INDEX_COMPOSITION_PROVIDER_ENV,
                    constants.DEFAULT_INDEX_COMPOSITION_PROVIDER,
                ),
            }
            env_data.update(data)
            super().__init__(**env_data)

    SettingsConfigDict = dict  # type: ignore[assignment,misc]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        validate_by_name=True,
        populate_by_name=True,
    )

    ibkr_host: str = Field(default=constants.DEFAULT_IBKR_HOST, alias=constants.IBKR_HOST_ENV)
    ibkr_port: int = Field(default=constants.DEFAULT_IBKR_PORT, alias=constants.IBKR_PORT_ENV)
    ibkr_client_id: int = Field(default=constants.DEFAULT_IBKR_CLIENT_ID, alias=constants.IBKR_CLIENT_ID_ENV)

    redis_url: str = Field(default=constants.DEFAULT_REDIS_URL, alias=constants.REDIS_URL_ENV)

    questdb_host: str = Field(default=constants.DEFAULT_QUESTDB_HOST, alias=constants.QUESTDB_HOST_ENV)
    questdb_port: int = Field(default=constants.DEFAULT_QUESTDB_PORT, alias=constants.QUESTDB_PORT_ENV)
    questdb_user: str = Field(default=constants.DEFAULT_QUESTDB_USER, alias=constants.QUESTDB_USER_ENV)
    questdb_password: str = Field(default=constants.DEFAULT_QUESTDB_PASSWORD, alias=constants.QUESTDB_PASSWORD_ENV)
    questdb_database: str = Field(default=constants.DEFAULT_QUESTDB_DATABASE, alias=constants.QUESTDB_DATABASE_ENV)

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

    @property
    def questdb_dsn(self) -> str:
        return (
            f"host={self.questdb_host} "
            f"port={self.questdb_port} "
            f"user={self.questdb_user} "
            f"password={self.questdb_password} "
            f"dbname={self.questdb_database}"
        )
