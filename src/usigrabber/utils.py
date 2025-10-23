from collections.abc import Generator
from pathlib import Path

import ijson


def project_root_path() -> Path:
    """Return the root directory of the project."""
    return Path(__file__).parent.parent.parent


def iter_json(json_path: Path) -> Generator[dict, None, None]:
    """Yield items from a JSON file using ijson for efficient parsing."""
    with open(json_path, encoding="utf-8") as in_f:
        for item in ijson.items(in_f, "item"):
            yield item
