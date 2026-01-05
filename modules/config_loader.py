
from pathlib import Path
import yaml


def load_config(path: str | None):
    if path is None:
        return {}
    p = Path(path)
    if not p.exists():
        print(f"Config file not found: {p}, using empty config.")
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_schema(path: str | None):
    if path is None:
        return {}
    p = Path(path)
    if not p.exists():
        print(f"Schema file not found: {p}, no schema will be applied.")
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}
