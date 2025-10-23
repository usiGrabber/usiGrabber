import csv
import logging
from collections.abc import Generator
from pathlib import Path

import ijson

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def project_root_path() -> Path:
    """Return the root directory of the project."""
    return Path(__file__).parent.parent.parent


def iter_json(json_path: Path) -> Generator[dict, None, None]:
    """Yield items from a JSON file using ijson for efficient parsing."""
    with open(json_path, encoding="utf-8") as in_f:
        for item in ijson.items(in_f, "item"):
            yield item


def parse_unimod() -> dict[str, int]:
    """
    Parse a Unimod CSV file and return a mapping from modification name to unimod_id.

    :returns: Dictionary mapping modification name (str) to unimod_id (int).
    """
    unimod_map = {}

    csv_path = project_root_path() / "data" / "files" / "unimod.csv"

    with open(csv_path, encoding="utf-8", newline="") as in_f:
        reader = csv.DictReader(in_f)
        if not reader.fieldnames:
            return unimod_map

        # Find header keys for title and record id in a case-insensitive manner
        title_key = next(
            (k for k in reader.fieldnames if k and k.lower() == "title"), None
        )
        record_id_key = next(
            (
                k
                for k in reader.fieldnames
                if k
                and k.lower()
                in {
                    "record_id",
                    "recordid",
                    "record id",
                    "id",
                    "unimod_id",
                    "unimod id",
                }
            ),
            None,
        )

        for row in reader:
            if not row:
                continue
            title = row.get(title_key) if title_key else None
            record_id_val = row.get(record_id_key) if record_id_key else None

            try:
                unimod_map[title] = int(record_id_val)
            except (ValueError, TypeError):
                # skip rows with non-integer record ids
                logger.warning(
                    "Skipping Unimod entry with invalid record id: %d", record_id_val
                )
                continue

    return unimod_map


UNIMOD_LOOKUP = parse_unimod()
