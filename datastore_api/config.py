from functools import lru_cache
import logging
import os
from typing import Any

from pydantic import BaseModel, BaseSettings, validator

from datastore_api.utils import load_yaml


LOGGER = logging.getLogger(__name__)


def yaml_config_settings_source(settings: BaseSettings) -> dict[str, Any]:
    return load_yaml("config.yaml", settings.__config__.env_file_encoding)


class IcatUser(BaseModel):
    auth: str
    username: str


class FunctionalUser(IcatUser):
    password: str


class IcatSettings(BaseModel):
    url: str
    check_cert: bool = True
    admin_users: list[IcatUser] = []
    functional_user: FunctionalUser
    embargo_period_years: int = 2
    parameter_type_job_ids: str = "Archival ids"
    parameter_type_job_state: str = "Archival state"
    embargo_types: list[str] = []


class Fts3Settings(BaseModel):
    endpoint: str
    instrument_data_cache: str
    user_data_cache: str
    tape_archive: str
    x509_user_proxy: str = None
    x509_user_key: str = None
    x509_user_cert: str = None
    bring_online: int = 28800  # 8 hours
    copy_pin_lifetime: int = 28800  # 8 hours

    @validator("x509_user_cert", always=True)
    def _validate_x509(cls, v: str, values: dict) -> str:
        if v is not None:
            x509_user_key = values.get("x509_user_key", None)
            return Fts3Settings._validate_x509_cert(v, x509_user_key)
        else:
            values["x509_user_key"] = None
            x509_user_proxy = values.get("x509_user_proxy", None)
            return Fts3Settings._validate_x509_proxy(x509_user_proxy)

    @staticmethod
    def _validate_x509_cert(x509_user_cert: str, x509_user_key: str | None) -> str:
        if x509_user_key is None:
            raise ValueError("x509_user_key not set")
        elif not os.path.exists(x509_user_cert):
            raise ValueError("x509_user_cert set but doesn't exist")
        elif not os.access(x509_user_cert, os.R_OK):
            raise ValueError("x509_user_cert exists but is not readable")
        elif not os.path.exists(x509_user_key):
            raise ValueError("x509_user_key set but doesn't exist")
        elif not os.access(x509_user_key, os.R_OK):
            raise ValueError("x509_user_key exists but is not readable")

        return x509_user_cert

    @staticmethod
    def _validate_x509_proxy(x509_user_proxy: str | None) -> str:
        if x509_user_proxy is None:
            raise ValueError("Neither x509_user_cert nor x509_user_proxy set")
        elif not os.path.exists(x509_user_proxy):
            raise ValueError("x509_user_proxy set but doesn't exist")
        elif not os.access(x509_user_proxy, os.R_OK):
            raise ValueError("x509_user_proxy exists but is not readable")

        return x509_user_proxy


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
