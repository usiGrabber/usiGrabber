"""
File Parser Models

Data models for tracking import statistics and results.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, NamedTuple, TypedDict
from uuid import UUID

from usigrabber.db.schema import IndexType, MzidFile

# ============================================================================
# TypedDicts for parsed data. This should always be aligned with the database schema.
# ============================================================================


class ModifiedPeptideDict(TypedDict):
    """Dictionary representation of a ModifiedPeptide record for bulk insertion."""

    id: UUID
    peptide_sequence: str


class ModificationDict(TypedDict):
    """Dictionary representation of a Modification record for bulk insertion."""

    id: UUID
    unimod_id: int | None
    name: str | None
    location: int | None
    modified_residue: str | None


class ModifiedPeptideModificationJunctionDict(TypedDict):
    """Dictionary representation of junction table linking modified peptides to modifications."""

    modified_peptide_id: UUID
    modification_id: UUID


class PeptideEvidenceDict(TypedDict):
    """Dictionary representation of a PeptideEvidence record for bulk insertion."""

    id: UUID
    protein_accession: str | None
    is_decoy: bool | None
    start_position: int | None
    end_position: int | None
    pre_residue: str | None
    post_residue: str | None


class PeptideSpectrumMatchDict(TypedDict):
    """Dictionary representation of a PeptideSpectrumMatch (PSM) record for bulk insertion.

    Note: All fields are required in the dict, but many can be None.
    USI fields (index_type, index_number, ms_run) are always present but may be None.
    """

    id: UUID
    project_accession: str
    mzid_file_id: UUID | None
    modified_peptide_id: UUID
    spectrum_id: str | None
    charge_state: int | None
    experimental_mz: float | None
    calculated_mz: float | None
    score_values: dict[str, float] | None
    rank: int | None
    pass_threshold: bool | None
    index_type: IndexType | None
    index_number: int | None
    ms_run: str | None


class PSMPeptideEvidenceDict(TypedDict):
    """Dictionary representation of junction table linking PSMs to peptide evidence."""

    id: UUID
    psm_id: UUID
    peptide_evidence_id: UUID


# ============================================================================
# NamedTuples for parsed data containers
# ============================================================================


class ParsedMzidData(NamedTuple):
    """Container for all parsed data from an mzIdentML file."""

    mzid_file: MzidFile
    modified_peptides: list[ModifiedPeptideDict]
    modifications: list[ModificationDict]
    modified_peptide_modification_junctions: list[ModifiedPeptideModificationJunctionDict]
    peptide_evidence: list[PeptideEvidenceDict]
    psms: list[PeptideSpectrumMatchDict]
    psm_peptide_evidence_junctions: list[PSMPeptideEvidenceDict]


class ParsedMztabData(NamedTuple):
    """Container for all parsed data from an mzTab file."""

    modified_peptides: list[ModifiedPeptideDict]
    psms: list[PeptideSpectrumMatchDict]


class ParsedTxtZipData(NamedTuple):
    """Container for all parsed data from an evidence.txt file."""

    peptides: list[dict]
    peptide_modifications: list[dict]
    peptide_evidence: list[dict]
    psms: list[dict]
    psm_peptide_evidence_junctions: list[dict]
    search_modifications: list[dict]


@dataclass
class ImportStats:
    """Statistics from importing a file into the database."""

    file_name: str
    project_accession: str
    peptide_count: int = 0
    modification_count: int = 0
    peptide_evidence_count: int = 0
    psm_count: int = 0
    start_time: datetime = field(default_factory=datetime.utcnow)
    parsing_complete_time: datetime | None = None
    end_time: datetime | None = None
    success: bool = False
    error_message: str | None = None

    def mark_parsing_complete(self) -> None:
        """Mark parsing as successfully completed."""
        self.parsing_complete_time = datetime.utcnow()

    def mark_complete(self) -> None:
        """Mark import as successfully completed."""
        self.end_time = datetime.utcnow()
        self.success = True

    def mark_failed(self, error_message: str) -> None:
        """Mark import as failed with error message."""
        self.end_time = datetime.utcnow()
        self.success = False
        self.error_message = error_message

    @property
    def duration_seconds(self) -> float | None:
        """Calculate import duration in seconds."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    @property
    def parsing_duration_seconds(self) -> float | None:
        """Calculate parsing duration in seconds."""
        if self.parsing_complete_time:
            return (self.parsing_complete_time - self.start_time).total_seconds()
        return None

    def summary(self) -> str:
        """Generate human-readable summary."""
        if not self.success:
            return f"Import failed: {self.error_message}"

        duration = f"{self.duration_seconds:.1f}s" if self.duration_seconds else ""
        parsing_duration = (
            f"{self.parsing_duration_seconds:.1f}s" if self.parsing_duration_seconds else ""
        )
        return (
            f"Successfully imported ({duration}):\n"
            f"  - {self.peptide_count:,} peptides\n"
            f"  - {self.modification_count:,} modifications\n"
            f"  - {self.peptide_evidence_count:,} protein mappings\n"
            f"  - {self.psm_count:,} PSMs\n"
            f"Parsing took {parsing_duration}/{duration}."
        )

    def dict_summary(self) -> dict[str, Any]:
        """Generate dictionary summary for structured logging."""
        return {
            "file_name": self.file_name,
            "project_accession": self.project_accession,
            "peptide_count": self.peptide_count,
            "modification_count": self.modification_count,
            "peptide_evidence_count": self.peptide_evidence_count,
            "psm_count": self.psm_count,
            "start_time": self.start_time.isoformat(),
            "parsing_complete_time": self.parsing_complete_time.isoformat()
            if self.parsing_complete_time
            else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "success": self.success,
            "error_message": self.error_message,
            "duration_seconds": self.duration_seconds,
            "parsing_duration_seconds": self.parsing_duration_seconds,
        }
