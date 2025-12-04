"""
Shared test fixtures and utilities for mzID parsing tests.
"""

from pathlib import Path

import pytest
from pyteomics import mzid


def create_mzid_reader(mzid_path: Path) -> mzid.MzIdentML:
    """
    Create an MzIdentML reader from a file path.

    Args:
            mzid_path: Path to the mzIdentML file

    Returns:
            MzIdentML reader instance with retrieve_refs=False
    """
    return mzid.MzIdentML(str(mzid_path), retrieve_refs=False)


# ============================================================================
# Fixtures for threshold parsing tests
# ============================================================================


@pytest.fixture
def fdr_reader(mzid_fixtures_dir: Path):
    """Return mzID reader for file with FDR threshold.

    Contains: "pep:FDR threshold" = "0.01"
    """
    path = mzid_fixtures_dir / "threshold_fdr.mzid"
    return create_mzid_reader(path)


@pytest.fixture
def mascot_reader(mzid_fixtures_dir: Path):
    """Return mzID reader for file with Mascot threshold.

    Contains: "Mascot:SigThreshold" = "0.05" (and other Mascot params)
    """
    path = mzid_fixtures_dir / "threshold_mascot.mzid"
    return create_mzid_reader(path)


@pytest.fixture
def invalid_value_reader(mzid_fixtures_dir: Path):
    """Return mzID reader for file with invalid threshold value.

    Contains: "pep:FDR threshold" = "not_a_number"
    """
    path = mzid_fixtures_dir / "threshold_invalid_value.mzid"
    return create_mzid_reader(path)


@pytest.fixture
def empty_value_reader(mzid_fixtures_dir: Path):
    """Return mzID reader for file with empty threshold value.

    Contains: "pep:FDR threshold" = ""
    """
    path = mzid_fixtures_dir / "threshold_empty_value.mzid"
    return create_mzid_reader(path)


@pytest.fixture
def no_threshold_reader(mzid_fixtures_dir: Path):
    """Return mzID reader for file with no Threshold element."""
    path = mzid_fixtures_dir / "threshold_no_threshold.mzid"
    return create_mzid_reader(path)


@pytest.fixture
def full_mzid_reader(full_mzid_path):
    """Return mzID reader for full example file.

    Contains complete mzID data including software, thresholds, peptides,
    modifications, peptide evidence, and PSMs.
    """
    return create_mzid_reader(full_mzid_path)
