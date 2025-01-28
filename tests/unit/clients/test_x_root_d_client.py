from pydantic_core import Url
import pytest

from datastore_api.clients.x_root_d_client import XRootDClient


class TestXRootDClient:
    @pytest.mark.parametrize(
        ["url", "expected_path"],
        [
            pytest.param("https://localhost:1095/", "/"),
            pytest.param("https://localhost:1095/path/", "/path/"),
            pytest.param("root://localhost:1094//", "/"),
            pytest.param("root://localhost:1094//path/", "/path/"),
        ],
    )
    def test_init(self, url: str, expected_path: str):
        x_root_d_client = XRootDClient(url=url)
        assert x_root_d_client.url_path == expected_path

    def test_validate_url(self):
        url = XRootDClient._validate_url(Url("https://localhost:1095/path"))
        assert url == "root://localhost:1094"
