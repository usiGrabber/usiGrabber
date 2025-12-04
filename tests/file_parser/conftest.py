from pathlib import Path

import pytest


@pytest.fixture
def mzid_fixtures_dir() -> Path:
    """Return path to the mzID fixtures directory."""
    return Path("tests/file_parser/mzid/fixtures")


@pytest.fixture
def full_mzid_path(mzid_fixtures_dir: Path) -> Path:
    """Return path to a full example mzID file."""
    return mzid_fixtures_dir / "full_small.mzid"
