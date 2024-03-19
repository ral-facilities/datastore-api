from pathlib import Path

from datastore_api.utils import load_yaml


class TestUtils:
    def test_load_yaml(self, tmp_path: Path):
        path = tmp_path / "config.yaml"
        path.write_text("key: value")

        yaml_dict = load_yaml(path)
        assert yaml_dict == {"key": "value"}
