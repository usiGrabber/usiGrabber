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


@pytest.fixture(autouse=True)
def mock_lookup_unimod_id_by_name(monkeypatch):
    """Stub network-backed lookup so CI stays offline."""

    def _lookup(name: str | None) -> int | None:
        mappings = {
            "Oxidation": 35,
            "fragment neutral loss": None,
            "": None,
        }
        return mappings.get(name or "")

    monkeypatch.setattr("usigrabber.file_parser.helpers.lookup_unimod_id_by_name", _lookup)
