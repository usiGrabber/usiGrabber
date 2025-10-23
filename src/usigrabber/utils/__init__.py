import csv
import logging
import os
from collections.abc import Generator
from pathlib import Path

import ijson

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def data_directory_path() -> Path:
    """Return the root directory of the project."""
    data_dir = os.getenv("UG_DATA_DIR", default=".")
    if data_dir == ".":
        logger.warning(
            "UG_DATA_DIR environment variable not set. Using current directory as root."
        )
    if data_dir.startswith("~"):
        data_dir = os.path.expanduser(data_dir)
    return Path(data_dir)


def iter_json(json_path: Path) -> Generator[dict, None, None]:
    """Yield items from a JSON file using ijson for efficient parsing."""
    with open(json_path, encoding="utf-8") as in_f:
        for item in ijson.items(in_f, "item"):
            yield item


UNIMOD_DB = None


def get_unimod_db():
    """Lazy load the Unimod database."""
    global UNIMOD_DB
    if UNIMOD_DB is None:
        from pyteomics.mass.unimod import Unimod

        UNIMOD_DB = Unimod(
            "sqlite:///" + (data_directory_path() / "unimod.db").as_posix()
        )
    return UNIMOD_DB
