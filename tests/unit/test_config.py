import os
from pathlib import Path

import pytest

from datastore_api.config import Fts3Settings, Settings, Storage, StorageType
from tests.fixtures import mock_fts3_settings, submit


class TestFts3Settings:
    def test_x509_cert_key(self, tmp_path: Path):
        x509_user_cert_path = tmp_path / "cert"
        x509_user_cert_path.write_text("cert")
        x509_user_cert = x509_user_cert_path.as_posix()

        x509_user_key_path = tmp_path / "key"
        x509_user_key_path.write_text("key")
        x509_user_key = x509_user_key_path.as_posix()

        settings = Fts3Settings(
            endpoint="https://fts.ac.uk:8446",
            instrument_data_cache="root://idc.ac.uk:1094//",
            restored_data_cache="root://rdc.ac.uk:1094//",
            tape_archive="root://archive.ac.uk:1094//",
            x509_user_cert=x509_user_cert,
            x509_user_key=x509_user_key,
        )
        assert settings.x509_user_cert == x509_user_cert
        assert settings.x509_user_key == x509_user_key
        assert settings.x509_user_proxy is None

    def test_x509_proxy(self, tmp_path: Path):
        x509_user_proxy_path = tmp_path / "proxy"
        x509_user_proxy_path.write_text("proxy")
        x509_user_proxy = x509_user_proxy_path.as_posix()
        settings = Fts3Settings(
            endpoint="https://fts.ac.uk:8446",
            instrument_data_cache="root://idc.ac.uk:1094//",
            restored_data_cache="root://rdc.ac.uk:1094//",
            tape_archive="root://archive.ac.uk:1094//",
            x509_user_proxy=x509_user_proxy,
        )
        assert settings.x509_user_cert == x509_user_proxy
        assert settings.x509_user_key is None
        assert settings.x509_user_proxy == x509_user_proxy

    @pytest.mark.parametrize(
        ["x509_user_cert", "x509_user_key", "expected_error"],
        [
            pytest.param(None, None, "Neither x509_user_cert nor x509_user_proxy set"),
            pytest.param("cert", None, "x509_user_key not set"),
            pytest.param("cert", "key", "x509_user_cert set but doesn't exist"),
            pytest.param(__file__, "key", "x509_user_key set but doesn't exist"),
            pytest.param(
                "not_readable",
                "not_readable",
                "x509_user_cert exists but is not readable",
            ),
            pytest.param(
                __file__,
                "not_readable",
                "x509_user_key exists but is not readable",
            ),
        ],
    )
    def test_x509_cert_failure(
        self,
        x509_user_cert: str | None,
        x509_user_key: str | None,
        expected_error: str,
        tmp_path: Path,
        mock_fts3_settings: Settings,
    ):

        if x509_user_cert == "not_readable":
            x509_user_cert_path = tmp_path / "cert"
            x509_user_cert_path.write_text("cert")
            os.chmod(x509_user_cert_path, 0o000)
            x509_user_cert = x509_user_cert_path.as_posix()

        if x509_user_key == "not_readable":
            x509_user_key_path = tmp_path / "key"
            x509_user_key_path.write_text("key")
            os.chmod(x509_user_key_path, 0o000)
            x509_user_key = x509_user_key_path.as_posix()

        fts3_settings_dict = mock_fts3_settings.fts3.model_dump(
            exclude_none=True,
            exclude={"x509_user_cert", "x509_user_key"},
        )

        if x509_user_cert is not None:
            fts3_settings_dict["x509_user_cert"] = x509_user_cert

        if x509_user_key is not None:
            fts3_settings_dict["x509_user_key"] = x509_user_key

        with pytest.raises(ValueError) as e:
            Fts3Settings(**fts3_settings_dict)

        assert expected_error in e.exconly()

    @pytest.mark.parametrize(
        ["x509_user_proxy", "expected_error"],
        [
            pytest.param(None, "Neither x509_user_cert nor x509_user_proxy set"),
            pytest.param("proxy", "x509_user_proxy set but doesn't exist"),
            pytest.param("not_readable", "x509_user_proxy exists but is not readable"),
        ],
    )
    def test_x509_proxy_failure(
        self,
        x509_user_proxy: str | None,
        expected_error: str,
        tmp_path: Path,
        mock_fts3_settings: Settings,
    ):

        if x509_user_proxy == "not_readable":
            x509_user_proxy_path = tmp_path / "proxy"
            x509_user_proxy_path.write_text("proxy")
            os.chmod(x509_user_proxy_path, 0o000)
            x509_user_proxy = x509_user_proxy_path.as_posix()

        fts3_settings_dict = mock_fts3_settings.fts3.model_dump(
            exclude_none=True,
            exclude={"x509_user_cert", "x509_user_key"},
        )
        if x509_user_proxy is not None:
            fts3_settings_dict["x509_user_proxy"] = x509_user_proxy

        with pytest.raises(ValueError) as e:
            Fts3Settings(**fts3_settings_dict)

        assert expected_error in e.exconly()

    @pytest.mark.parametrize(
        ["endpoint", "error"],
        [
            pytest.param("root://domain.ac.uk:1094", "path not set"),
            pytest.param("root://domain.ac.uk:1094/", "path did not start with '//'"),
            pytest.param("root://domain.ac.uk:1094//path", "path did not end with '/'"),
            pytest.param("s3s://domain.ac.uk:1094", "URL scheme should be "),
            pytest.param(
                "root://domain.ac.uk:1094?query=query",
                "Url query not supported for FTS endpoint",
            ),
            pytest.param(
                "root://domain.ac.uk:1094#fragment",
                "Url fragment not supported for FTS endpoint",
            ),
        ],
    )
    def test_validate_endpoint(self, endpoint: str, error: str):
        with pytest.raises(ValueError) as e:
            Storage(url=endpoint, storage_type=StorageType.DISK)

        assert error in e.exconly()

    def test_validate_supported_checksums(self, mock_fts3_settings: Settings):
        fts3_settings_dict = mock_fts3_settings.fts3.model_dump(exclude_none=True)
        fts3_settings_dict["verify_checksum"] = "both"
        fts3_settings_dict["supported_checksums"] = []
        with pytest.raises(ValueError) as e:
            Fts3Settings(**fts3_settings_dict)

        assert (
            "At least one checksum mechanism needs to be provided if `verify_checksum`"
            " is not 'none'"
        ) in e.exconly()
