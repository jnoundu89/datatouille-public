import json
import logging

logger = logging.getLogger(__name__)


def load_json_file(file_path: str) -> dict | list | None:
    """Load JSON file from local filesystem."""
    try:
        with open(file_path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error("Failed to load %s: %s", file_path, e)
        return None
