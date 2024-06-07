from pathlib import Path

import pytest

from datastore_api.utils import load_yaml


class TestUtils:
    @pytest.mark.parametrize(
        ["text", "expected_dict"],
        [pytest.param(None, {}), pytest.param("key: value", {"key": "value"})],
    )
    def test_load_yaml(self, text: str, expected_dict: dict, tmp_path: Path):
        path = tmp_path / "config.yaml"
        if text is not None:
            path.write_text(text)

        yaml_dict = load_yaml(path)
        assert yaml_dict == expected_dict
