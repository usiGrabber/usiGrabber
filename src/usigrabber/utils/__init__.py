import logging
import os
from collections.abc import Generator
from datetime import date, datetime
from pathlib import Path
from typing import Any

import ijson
from dotenv import load_dotenv
from rich.console import Console

load_dotenv()

# Create one shared console for everything
console = Console()
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO").upper(),
)
logger = logging.getLogger(__name__)
# Suppress overly verbose logs from dependencies
for name in ["sqlalchemy", "urllib3"]:
    logging.getLogger(name).setLevel(logging.WARNING)


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

        db_path = get_cache_dir() / "unimod.db"
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
