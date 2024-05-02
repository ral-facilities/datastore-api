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
            endpoint="",
            instrument_data_cache="",
            user_data_cache="",
            tape_archive="",
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
            endpoint="",
            instrument_data_cache="",
            user_data_cache="",
            tape_archive="",
            x509_user_proxy=x509_user_proxy,
        )
        assert settings.x509_user_cert == x509_user_proxy
        assert settings.x509_user_key is None
        assert settings.x509_user_proxy == x509_user_proxy

    @pytest.mark.parametrize(
        ["x509_user_cert", "x509_user_key", "expected_error"],
        [
            pytest.param(None, None, "x509_user_key not set"),
            pytest.param("cert", "key", "x509_user_cert not readable"),
            pytest.param(__file__, "key", "x509_user_key not readable"),
        ],
    )
    def test_x509_cert_failure(
        self,
        x509_user_cert: str,
        x509_user_key: str,
        expected_error: str,
    ):
        with pytest.raises(ValueError) as e:
            Fts3Settings._validate_x509_cert(x509_user_cert, x509_user_key)

        assert e.exconly() == f"ValueError: {expected_error}"

    @pytest.mark.parametrize(
        ["x509_user_proxy", "expected_error"],
        [
            pytest.param(None, "Neither x509_user_cert nor x509_user_proxy set"),
            pytest.param("proxy", "x509_user_proxy not readable"),
        ],
    )
    def test_x509_proxy_failure(self, x509_user_proxy: str, expected_error: str):
        with pytest.raises(ValueError) as e:
            Fts3Settings._validate_x509_proxy(x509_user_proxy)

        assert e.exconly() == f"ValueError: {expected_error}"
