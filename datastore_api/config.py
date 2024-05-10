from functools import lru_cache
import logging
from typing import Any

from pydantic import BaseModel, BaseSettings

from datastore_api.utils import load_yaml


LOGGER = logging.getLogger(__name__)


def yaml_config_settings_source(settings: BaseSettings) -> dict[str, Any]:
    return load_yaml("config.yaml", settings.__config__.env_file_encoding)


class IcatUser(BaseModel):
    auth: str
    username: str


class IcatSettings(BaseModel):
    url: str
    check_cert: bool = True
    admin_users: list[IcatUser] = []
    embargo_period_years: int = 2


class Fts3Settings(BaseModel):
    endpoint: str
    instrument_data_cache: str
    user_data_cache: str
    tape_archive: str
    bring_online: int = 28800  # 8 hours
    copy_pin_lifetime: int = 28800  # 8 hours


class Settings(BaseSettings):
    icat: IcatSettings
    fts3: Fts3Settings

    class Config:
        @classmethod
        def customise_sources(cls, init_settings, env_settings, file_secret_settings):
            return (
                init_settings,
                env_settings,
                yaml_config_settings_source,
                file_secret_settings,
            )


@lru_cache
def get_settings() -> Settings:
    """Get and cache the API settings to prevent overhead from reading from file.

    Returns:
        Settings: The configurations settings for the API.
    """
    settings = Settings()
    LOGGER.info("Initialised and cached Settings: %s", settings)
    return settings
