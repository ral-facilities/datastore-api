from pathlib import Path
from typing import Any

import yaml


def load_yaml(filepath: str, encoding: str = None) -> dict[str, Any]:
    """Loads the contents of a YAML file, or returns an empty dict if not found.

    Args:
        filepath (str): YAML filepath.
        encoding (str, optional): File encoding. Defaults to None.

    Returns:
        dict[str, Any]: Contents of filepath, or an empty dict if not found.
    """
    try:
        with open(Path(filepath), encoding=encoding) as f:
            return yaml.safe_load(f)

    except FileNotFoundError:
        return {}
