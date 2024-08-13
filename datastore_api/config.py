from enum import StrEnum
from functools import lru_cache
import logging
import os
from typing import Any

from pydantic import (
    AnyHttpUrl,
    BaseModel,
    BaseSettings,
    Field,
    parse_obj_as,
    stricturl,
    validator,
)

from datastore_api.utils import load_yaml


LOGGER = logging.getLogger(__name__)
RootUrl = stricturl(allowed_schemes={"root"})


def yaml_config_settings_source(settings: BaseSettings) -> dict[str, Any]:
    return load_yaml("config.yaml", settings.__config__.env_file_encoding)


class IcatUser(BaseModel):
    auth: str = Field(description="ICAT authentication mechanism.", example="simple")
    username: str = Field(description="ICAT username.", example="root")


class FunctionalUser(IcatUser):
    password: str = Field(description="ICAT password.", example="pw")


class IcatSettings(BaseModel):
    url: AnyHttpUrl = Field(
        description="Url to use for the ICAT server",
        example="https://localhost:8181",
    )
    check_cert: bool = Field(
        description=(
            "Whether the server's SSL certificate should be verified if connecting to "
            "ICAT with HTTPS."
        ),
        example="https://localhost:8181",
    )
    admin_users: list[IcatUser] = Field(
        default=[],
        description=(
            "List of ICAT users who should be allowed to perform admin actions."
        ),
    )
    functional_user: FunctionalUser = Field(
        description=(
            "ICAT user to use when performing functional actions not associated with a "
            "normal user, such as regular polling of the catalogue."
        ),
    )
    embargo_period_years: int = Field(
        default=2,
        description=(
            "Number of years to apply to the `releaseDate` if Investigations are "
            "created without one set."
        ),
    )
    parameter_type_job_ids: str = Field(
        default="Archival ids",
        description=(
            "ICAT ParameterType.name to identify how to record FTS archival ids."
        ),
    )
    parameter_type_job_state: str = Field(
        default="Archival state",
        description=(
            "ICAT ParameterType.name to identify how to record FTS archival state."
        ),
    )
    embargo_types: list[str] = Field(
        default=[],
        description=(
            "List of ICAT InvestigationType.name that indicate the release date should "
            "not be set."
        ),
    )


class VerifyChecksum(StrEnum):
    NONE = "none"
    SOURCE = "source"
    DESTINATION = "destination"
    BOTH = "both"


class Fts3Settings(BaseModel):
    endpoint: AnyHttpUrl = Field(
        description="Url to use for the FTS server",
        example="https://localhost:8446",
    )
    instrument_data_cache: RootUrl = Field(
        description="Url for the destination of raw, instrument data pre-archival",
        example="root://localhost:1094//",
    )
    tape_archive: RootUrl = Field(
        description="Url for the destination of archived data",
        example="root://localhost:1094//",
    )
    restored_data_cache: RootUrl = Field(
        description="Url for the destination of restored data post-archival",
        example="root://localhost:1094//",
    )
    x509_user_proxy: str = Field(
        default=None,
        description=(
            "Filepath to X509 user proxy. Not required if `x509_user_cert` and "
            "`x509_user_key` are both set."
        ),
        example="/tmp/x509up_u00000",
    )
    x509_user_key: str = Field(
        default=None,
        description=(
            "Filepath to X509 user key. Not required if `x509_user_proxy` is set."
        ),
        example="hostkey.pem",
    )
    x509_user_cert: str = Field(
        default=None,
        description=(
            "Filepath to X509 user cert. Not required if `x509_user_proxy` is set."
        ),
        example="hostcert.pem",
    )
    retry: int = Field(
        default=-1,
        description=(
            "Number of retries for transfers where <0 is no retries and 0 is server "
            "default."
        ),
    )
    verify_checksum: VerifyChecksum = Field(
        default=VerifyChecksum.NONE,
        description=(
            "Whether to verify checksums at 'source', 'destination', 'both' or 'none'. "
            "If 'both', then only the checksum mechanism needs to be provided with the "
            "files and not the value."
        ),
    )
    bring_online: int = Field(
        default=28800,
        description=(
            "Number of seconds to wait for an archived file to be staged for "
            "restoration. The default (28800 seconds) is 8 hours."
        ),
    )
    archive_timeout: int = Field(
        default=28800,
        description=(
            "Number of seconds to wait for an file to be archived. "
            "The default (28800 seconds) is 8 hours."
        ),
    )

    @validator("x509_user_cert", always=True)
    def _validate_x509(cls, v: str, values: dict) -> str:
        if v is not None:
            x509_user_key = values.get("x509_user_key", None)
            return Fts3Settings._validate_x509_cert(v, x509_user_key)
        else:
            values["x509_user_key"] = None
            x509_user_proxy = values.get("x509_user_proxy", None)
            return Fts3Settings._validate_x509_proxy(x509_user_proxy)

    @validator("instrument_data_cache", "restored_data_cache", "tape_archive")
    def _validate_storage_endpoint(cls, v: str) -> RootUrl:
        url = parse_obj_as(RootUrl, v)
        if url.query is not None:
            raise ValueError("Url query not supported for FTS endpoint")
        if url.fragment is not None:
            raise ValueError("Url fragment not supported for FTS endpoint")

        path = url.path
        if path is None:
            LOGGER.warn("FTS endpoint '%s' missing path, setting to '//'", v)
            path = "//"
        else:
            if not path.startswith("//"):
                LOGGER.warn(
                    "FTS endpoint '%s' path did not start with '//', appending",
                    v,
                )
                path = f"/{path}"

            if not path.endswith("/"):
                LOGGER.warn("FTS endpoint '%s' path did not end with '/', appending", v)
                path = f"{path}/"

        return RootUrl.build(
            scheme=url.scheme,
            user=url.user,
            password=url.password,
            host=url.host,
            port=url.port,
            path=path,
        )

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
    icat: IcatSettings = Field(description="Settings to connect to an ICAT instance")
    fts3: Fts3Settings = Field(description="Settings to connect to an FTS3 instance")

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
