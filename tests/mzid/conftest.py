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
def fdr_reader():
    """Return mzID reader for file with FDR threshold.

    Contains: "pep:FDR threshold" = "0.01"
    """
    path = Path("tests/mzid/fixtures/threshold_fdr.mzid")
    return create_mzid_reader(path)


@pytest.fixture
def mascot_reader():
    """Return mzID reader for file with Mascot threshold.

    Contains: "Mascot:SigThreshold" = "0.05" (and other Mascot params)
    """
    path = Path("tests/mzid/fixtures/threshold_mascot.mzid")
    return create_mzid_reader(path)


@pytest.fixture
def invalid_value_reader():
    """Return mzID reader for file with invalid threshold value.

    Contains: "pep:FDR threshold" = "not_a_number"
    """
    path = Path("tests/mzid/fixtures/threshold_invalid_value.mzid")
    return create_mzid_reader(path)


@pytest.fixture
def empty_value_reader():
    """Return mzID reader for file with empty threshold value.

    Contains: "pep:FDR threshold" = ""
    """
    path = Path("tests/mzid/fixtures/threshold_empty_value.mzid")
    return create_mzid_reader(path)


@pytest.fixture
def no_threshold_reader():
    """Return mzID reader for file with no Threshold element."""
    path = Path("tests/mzid/fixtures/threshold_no_threshold.mzid")
    return create_mzid_reader(path)


@pytest.fixture
def full_mzid_path():
    """Return path to a full example mzID file."""
    return Path("tests/mzid/fixtures/full_small.mzid")


@pytest.fixture
def full_mzid_reader():
    """Return mzID reader for full example file.

    Contains complete mzID data including software, thresholds, peptides,
    modifications, peptide evidence, and PSMs.
    """
    path = Path("tests/mzid/fixtures/full_small.mzid")
    return create_mzid_reader(path)
