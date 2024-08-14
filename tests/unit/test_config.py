import os
from pathlib import Path

import pytest

from datastore_api.config import Fts3Settings


class TestFts3Settings:
    def test_x509_cert_key(self, tmp_path: Path):
        x509_user_cert_path = tmp_path / "cert"
        x509_user_cert_path.write_text("cert")
        x509_user_cert = x509_user_cert_path.as_posix()

        x509_user_key_path = tmp_path / "key"
        x509_user_key_path.write_text("key")
        x509_user_key = x509_user_key_path.as_posix()

        settings = Fts3Settings(
            endpoint="https://localhost:8446",
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
            endpoint="https://localhost:8446",
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
            pytest.param(None, None, "x509_user_key not set"),
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
        x509_user_cert: str,
        x509_user_key: str,
        expected_error: str,
        tmp_path: Path,
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

        with pytest.raises(ValueError) as e:
            Fts3Settings._validate_x509_cert(x509_user_cert, x509_user_key)

        assert e.exconly() == f"ValueError: {expected_error}"

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
        x509_user_proxy: str,
        expected_error: str,
        tmp_path: Path,
    ):

        if x509_user_proxy == "not_readable":
            x509_user_proxy_path = tmp_path / "proxy"
            x509_user_proxy_path.write_text("proxy")
            os.chmod(x509_user_proxy_path, 0o000)
            x509_user_proxy = x509_user_proxy_path.as_posix()

        with pytest.raises(ValueError) as e:
            Fts3Settings._validate_x509_proxy(x509_user_proxy)

        assert e.exconly() == f"ValueError: {expected_error}"

    @pytest.mark.parametrize(
        ["endpoint", "expected_endpoint"],
        [
            pytest.param("root://domain.ac.uk:1094", "root://domain.ac.uk:1094//"),
            pytest.param("root://domain.ac.uk:1094/", "root://domain.ac.uk:1094//"),
            pytest.param(
                "root://domain.ac.uk:1094//path",
                "root://domain.ac.uk:1094//path/",
            ),
        ],
    )
    def test_validate_endpoint(self, endpoint: str, expected_endpoint: str):
        validated_endpoint = Fts3Settings._validate_storage_endpoint(endpoint)

        assert validated_endpoint == expected_endpoint

    @pytest.mark.parametrize(
        ["endpoint", "error"],
        [
            pytest.param("http://domain.ac.uk:1094", "URL scheme not permitted"),
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
    def test_validate_endpoint_error(self, endpoint: str, error: str):
        with pytest.raises(ValueError) as e:
            Fts3Settings._validate_storage_endpoint(endpoint)

        assert error in e.exconly()

    def test_validate_supported_checksums(self):
        with pytest.raises(ValueError) as e:
            Fts3Settings._validate_supported_checksums([], {"verify_checksum": "both"})

        assert e.exconly() == (
            "ValueError: At least one checksum mechanism needs to be provided if "
            "`verify_checksum` is not 'none'"
        )
