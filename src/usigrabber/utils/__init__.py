import logging
import os
from collections.abc import Generator
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

import ijson

logger = logging.getLogger(__name__)


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


@lru_cache(maxsize=420)
def lookup_unimod_id_by_name(mod_name: str) -> int | None:
    """
    Lookup UNIMOD ID by modification name with caching.

    Args:
            mod_name: Name of the modification

    Returns:
            UNIMOD ID as integer, or None if not found
    """
    try:
        mod = get_unimod_db().get(mod_name, False)
        if mod is not None:
            return int(cast(int, mod.id))
    except KeyError:
        pass

    return None


def get_unimod_db():
    """Lazy load the Unimod database."""
    global unimod_db
    if unimod_db is None:
        from pyteomics.mass.unimod import Unimod

        # Load the Unimod database in memory
        unimod_db = Unimod()

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
