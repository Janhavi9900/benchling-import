import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict


CONFIG_PATH = Path(__file__).with_name("config.json")


@lru_cache()
def load_config() -> Dict[str, Any]:
    """
    Load the application configuration from config.json in the project root.

    The result is cached for the lifetime of the process.
    """
    with CONFIG_PATH.open(encoding="utf-8") as f:
        return json.load(f)

