from enum import StrEnum
from functools import lru_cache
import logging
import os
from typing import Any

from pydantic import BaseModel, BaseSettings, HttpUrl, validator

from datastore_api.utils import load_yaml


LOGGER = logging.getLogger(__name__)


def yaml_config_settings_source(settings: BaseSettings) -> dict[str, Any]:
    return load_yaml("config.yaml", settings.__config__.env_file_encoding)


def validate_endpoint(v: str) -> str:
    double_slash_count = v.count("//")
    message = f"Endpoint {v} did contain second '//', appending"
    error_message = (
        f"Endpoint {v} did not contain '//' twice in the form:\n"
        "protocol://hostname//path/to/root/dir/"
    )
    if double_slash_count == 2:
        if v.endswith("/"):
            return v
        else:
            message = f"Endpoint {v} did not end with trailing '/', appending"
            LOGGER.warn(message)
            return f"{v}/"
    elif double_slash_count == 1:
        if v.endswith("//"):
            raise ValueError(error_message)
        elif v.endswith("/"):
            LOGGER.warn(message)
            return f"{v}/"
        else:
            LOGGER.warn(message)
            return f"{v}//"
    else:
        raise ValueError(error_message)


class IcatUser(BaseModel):
    auth: str
    username: str


class FunctionalUser(IcatUser):
    password: str


class IcatSettings(BaseModel):
    url: HttpUrl
    check_cert: bool = True
    admin_users: list[IcatUser] = []
    functional_user: FunctionalUser
    embargo_period_years: int = 2
    parameter_type_job_ids: str = "Archival ids"
    parameter_type_job_state: str = "Archival state"
    embargo_types: list[str] = []


class VerifyChecksum(StrEnum):
    NONE = "none"
    SOURCE = "source"
    DESTINATION = "destination"
    BOTH = "both"


class Fts3Settings(BaseModel):
    endpoint: HttpUrl
    instrument_data_cache: str
    user_data_cache: str
    tape_archive: str
    x509_user_proxy: str = None
    x509_user_key: str = None
    x509_user_cert: str = None
    retry: int = -1
    verify_checksum: VerifyChecksum = VerifyChecksum.NONE
    bring_online: int = 28800  # 8 hours
    archive_timeout: int = 28800  # 8 hours

    @validator("x509_user_cert", always=True)
    def _validate_x509(cls, v: str, values: dict) -> str:
        if v is not None:
            x509_user_key = values.get("x509_user_key", None)
            return Fts3Settings._validate_x509_cert(v, x509_user_key)
        else:
            values["x509_user_key"] = None
            x509_user_proxy = values.get("x509_user_proxy", None)
            return Fts3Settings._validate_x509_proxy(x509_user_proxy)

    @validator(
        "instrument_data_cache",
        "user_data_cache",
        "tape_archive",
    )
    def _validate_endpoint(cls, v: str) -> str:
        return validate_endpoint(v)

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


class S3Settings(BaseModel):
    endpoint: HttpUrl
    access_key: str
    secret_key: str

    @validator("endpoint")
    def _validate_s3_endpoint(cls, v: str) -> str:
        if v.endswith("//"):
            LOGGER.warn(f"S3 Endpoint {v} did contain second '//', removing")
            return v[:-2]
        elif v.endswith("/"):
            LOGGER.warn(f"S3 Endpoint {v} did end with trailing '/', removing")
            return v[:-1]
        else:
            return v


class Settings(BaseSettings):
    icat: IcatSettings
    fts3: Fts3Settings
    s3: S3Settings

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
