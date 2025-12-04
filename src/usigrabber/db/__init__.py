"""Database module for PRIDE project data management."""

from usigrabber.db.engine import load_db_engine
from usigrabber.db.schema import (
    CvParam,
    Modification,
    ModifiedPeptide,
    MzidFile,
    PeptideEvidence,
    PeptideSpectrumMatch,
    Project,
    ProjectCountry,
    ProjectKeyword,
    ProjectTag,
    PSMPeptideEvidence,
    Reference,
    create_db_and_tables,
)
from usigrabber.db.seed import seed_minimal_data

__all__ = [
    # Engine
    "load_db_engine",
    # Schema - Project tables
    "create_db_and_tables",
    "Project",
    # Schema
    "create_db_and_tables",
    "Project",
    "CvParam",
    "Reference",
    "ProjectKeyword",
    "ProjectTag",
    "ProjectCountry",
    # Schema - mzID/PSM tables
    "MzidFile",
    "PeptideSpectrumMatch",
    "Modification",
    "PeptideEvidence",
    "ModifiedPeptide",
    "PSMPeptideEvidence",
    # Seeding
    "seed_minimal_data",
]
