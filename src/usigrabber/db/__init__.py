"""Database module for PRIDE project data management."""

from usigrabber.db.engine import load_db_engine
from usigrabber.db.schema import (
	MzidFile,
	Peptide,
	PeptideEvidence,
	PeptideModification,
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
	"Reference",
	"ProjectKeyword",
	"ProjectTag",
	"ProjectCountry",
	# Schema - mzID/PSM tables
	"MzidFile",
	"PeptideSpectrumMatch",
	"Peptide",
	"PeptideEvidence",
	"PeptideModification",
	"PSMPeptideEvidence",
	# Seeding
	"seed_minimal_data",
]
