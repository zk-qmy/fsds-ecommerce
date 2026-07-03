import yaml
from pathlib import Path


def load_config(config_file: str) -> dict:
    path = Path(config_file)
    if not path.exists():
        raise FileNotFoundError(path)
    with open(path) as f:
        return yaml.safe_load(f)
