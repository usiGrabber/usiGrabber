import logging
import os
import sys
from collections.abc import Generator
from datetime import date, datetime
from pathlib import Path
from typing import Any

import ijson
from dotenv import load_dotenv

from usigrabber.utils.logging_helpers.formatter import CustomColorFormatter, JsonFormatter

load_dotenv()

logging_dir = Path(os.environ.get("LOGGING_DIR", "logs"))

logging_dir.mkdir(exist_ok=True)

# TODO: Remove this later: https://rednafi.com/python/no-hijack-root-logger/
logger = logging.getLogger("")

logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())
logger.propagate = False  # Chatty variable. TODO: check
if logger.hasHandlers():
    logger.handlers.clear()

terminal_handler = logging.StreamHandler(sys.stdout)
terminal_handler.setLevel(logging.INFO)
terminal_handler.setFormatter(CustomColorFormatter(use_colors=True))

# Handler for plain text file output (without colors)
file_handler = logging.FileHandler(logging_dir / "application.log", mode="w")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(CustomColorFormatter(use_colors=False))

# Handler for JSON file output (remains unchanged)
json_handler = logging.FileHandler(logging_dir / "application.jsonl", mode="w")
json_handler.setLevel(logging.DEBUG)
json_handler.setFormatter(JsonFormatter())

logger.addHandler(terminal_handler)
logger.addHandler(file_handler)
logger.addHandler(json_handler)


def get_cache_dir() -> Path:
    return Path(os.getenv("CACHE_DIR", ".cache"))


def iter_json(json_path: Path) -> Generator[dict[Any, Any], None, None]:
    """Yield items from a JSON file using ijson for efficient parsing."""
    with open(json_path, "rb") as in_f:
        # for item in tqdm(
        # , desc=f"Parsing {json_path.name}", unit="item"
        # ):
        for item in ijson.items(in_f, "item"):
            yield from item


unimod_db = None


def get_unimod_db():
    """Lazy load the Unimod database."""
    global unimod_db
    if unimod_db is None:
        from pyteomics.mass.unimod import Unimod

        cache_dir = get_cache_dir()
        cache_dir.mkdir(parents=True, exist_ok=True)
        db_path = cache_dir / "unimod.db"

        unimod_db = Unimod("sqlite:///" + db_path.as_posix())

    return unimod_db


def parse_date(date_str: str | None) -> date | None:
    """Parse date string in YYYY-MM-DD format."""
    if not date_str:
        return None
    try:
        # Handle both date-only and datetime formats
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.date()
    except (ValueError, AttributeError):
        return None


if __name__ == "__main__":
    logger.debug("This is a debug message for the file and json logs.")
    logger.info("Starting the application with the new formatter.", extra={"project_id": "Hi"})
    logger.warning("A non-critical warning has occurred.")
