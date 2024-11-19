from enum import StrEnum
from functools import lru_cache
import logging
import os
from typing import Annotated, Tuple, Type

from pydantic import (
    AfterValidator,
    BaseModel,
    Field,
    HttpUrl,
    model_validator,
    TypeAdapter,
    UrlConstraints,
)
from pydantic_core import Url
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)


LOGGER = logging.getLogger(__name__)
EndpointUrl = Annotated[Url, UrlConstraints(allowed_schemes={"root", "https", "davs"})]


def validate_endpoint_url_str(url_str: str):
    type_adapter = TypeAdapter(EndpointUrl)
    type_adapter.validate_python(url_str)

    url = EndpointUrl(url_str)
    if url.query is not None:
        raise ValueError("Url query not supported for FTS endpoint")
    if url.fragment is not None:
        raise ValueError("Url fragment not supported for FTS endpoint")

    if url.path is None:
        msg = f"FTS endpoint {url} path not set"
        raise ValueError(msg)

    if url.scheme == "root" and not url.path.startswith("//"):
        msg = f"FTS endpoint {url} path did not start with '//'"
        raise ValueError(msg)

    if not url.path.endswith("/"):
        msg = f"FTS endpoint {url} path did not end with '/'"
        raise ValueError(msg)

    return url_str


def validate_url_str(url_str: str, url_type: type = HttpUrl) -> str:
    type_adapter = TypeAdapter(url_type)
    type_adapter.validate_python(url_str)
    return url_str


HttpUrlStr = Annotated[str, AfterValidator(validate_url_str)]
EndpointUrlStr = Annotated[str, AfterValidator(validate_endpoint_url_str)]


class IcatUser(BaseModel):
    auth: str = Field(description="ICAT authentication mechanism.", examples=["simple"])
    username: str = Field(description="ICAT username.", examples=["root"])


class FunctionalUser(IcatUser):
    password: str = Field(description="ICAT password.", examples=["pw"])


class IcatSettings(BaseModel):
    url: HttpUrlStr = Field(
        description="Url to use for the ICAT server",
        examples=["https://localhost:8181"],
    )
    check_cert: bool = Field(
        description=(
            "Whether the server's SSL certificate should be verified if connecting to "
            "ICAT with HTTPS."
        ),
        examples=["https://localhost:8181"],
    )
    facility_name: str = Field(description="ICAT Facility.name")
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
    create_parameter_types: bool = Field(
        default=False,
        description=(
            "Whether to dynamically create the ParameterTypes used for archival state "
            "if they are not set."
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
    parameter_type_deletion_date: str = Field(
        default="Deletion date",
        description=(
            "ICAT ParameterType.name to identify how to record deletion date."
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
    endpoint: HttpUrlStr = Field(
        description="Url to use for the FTS server",
        examples=["https://localhost:8446"],
    )
    instrument_data_cache: EndpointUrlStr = Field(
        description="Url for the destination of raw, instrument data pre-archival",
        examples=["root://localhost:1094//"],
    )
    tape_archive: EndpointUrlStr = Field(
        description="Url for the destination of archived data",
        examples=["root://localhost:1094//"],
    )
    restored_data_cache: EndpointUrlStr = Field(
        description="Url for the destination of restored data post-archival",
        examples=["root://localhost:1094//"],
    )
    x509_user_proxy: str = Field(
        default=None,
        description=(
            "Filepath to X509 user proxy. Not required if `x509_user_cert` and "
            "`x509_user_key` are both set."
        ),
        examples=["/tmp/x509up_u00000"],
    )
    x509_user_key: str = Field(
        default=None,
        description=(
            "Filepath to X509 user key. Not required if `x509_user_proxy` is set."
        ),
        examples=["hostkey.pem"],
    )
    x509_user_cert: str = Field(
        default=None,
        description=(
            "Filepath to X509 user cert. Not required if `x509_user_proxy` is set."
        ),
        examples=["hostcert.pem"],
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
    supported_checksums: list[str] = Field(
        default=[],
        description=(
            "List of checksum mechanisms supported by the storage endpoints. If "
            "`verify_checksum` is not 'none', then this must include at least one "
            "mechanism."
        ),
        examples=[["ADLER32"]],
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

    @staticmethod
    def _validate_x509_file(setting: str, x509_file: str) -> None:
        if not os.path.exists(x509_file):
            raise ValueError(f"{setting} set but doesn't exist")
        elif not os.access(x509_file, os.R_OK):
            raise ValueError(f"{setting} exists but is not readable")

    @model_validator(mode="after")
    def _validate_model(self) -> "Fts3Settings":
        if self.x509_user_cert is not None:
            if self.x509_user_key is not None:
                Fts3Settings._validate_x509_file("x509_user_cert", self.x509_user_cert)
                Fts3Settings._validate_x509_file("x509_user_key", self.x509_user_key)
            else:
                raise ValueError("x509_user_key not set")
        else:
            if self.x509_user_proxy is not None:
                Fts3Settings._validate_x509_file(
                    setting="x509_user_proxy",
                    x509_file=self.x509_user_proxy,
                )
                self.x509_user_key = None
                self.x509_user_cert = self.x509_user_proxy
            else:
                raise ValueError("Neither x509_user_cert nor x509_user_proxy set")

        if self.verify_checksum != VerifyChecksum.NONE and not self.supported_checksums:
            raise ValueError(
                "At least one checksum mechanism needs to be provided if "
                "`verify_checksum` is not 'none'",
            )

        return self


class S3Settings(BaseModel):
    endpoint: HttpUrlStr = Field(description="Url to use for the S3 storage")
    access_key: str = Field(description="The ID for this access key")
    secret_key: str = Field(description="The secret key used to sign requests")
    cache_bucket: str = Field(
        description="Private bucket used to cache files before copy to download bucket",
    )


class Settings(BaseSettings):
    icat: IcatSettings = Field(description="Settings to connect to an ICAT instance")
    fts3: Fts3Settings = Field(description="Settings to connect to an FTS3 instance")
    s3: S3Settings = Field(description="Settings to connect to an S3 instance")

    model_config = SettingsConfigDict(yaml_file="config.yaml")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls),
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
