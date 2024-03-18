from pathlib import Path
from typing import Any

from pydantic import BaseModel, BaseSettings
import yaml


def yaml_config_settings_source(settings: BaseSettings) -> dict[str, Any]:
    with open(Path("config.yaml"), encoding=settings.__config__.env_file_encoding) as f:
        yaml_config = yaml.safe_load(f)

    return yaml_config


class IcatUser(BaseModel):
    auth: str
    username: str


class IcatSettings(BaseModel):
    url: str
    check_cert: bool = True
    admin_users: list[IcatUser] = []


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
