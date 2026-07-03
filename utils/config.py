from __future__ import annotations

from pathlib import Path

import yaml


def load_config(config_file: str | Path) -> dict:
    path = Path(config_file)
    if not path.exists():
        raise FileNotFoundError(path)
    with open(path) as f:
        return yaml.safe_load(f)
