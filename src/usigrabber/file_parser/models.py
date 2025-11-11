"""
File Parser Models

Data models for tracking import statistics and results.
"""

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class ImportStats:
    """Statistics from importing a file into the database."""

    file_name: str
    project_accession: str
    peptide_count: int = 0
    modification_count: int = 0
    peptide_evidence_count: int = 0
    psm_count: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    end_time: datetime | None = None
    success: bool = False
    error_message: str | None = None

    def mark_complete(self) -> None:
        """Mark import as successfully completed."""
        self.end_time = datetime.now()
        self.success = True

    def mark_failed(self, error_message: str) -> None:
        """Mark import as failed with error message."""
        self.end_time = datetime.now()
        self.success = False
        self.error_message = error_message

    @property
    def duration_seconds(self) -> float | None:
        """Calculate import duration in seconds."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    def summary(self) -> str:
        """Generate human-readable summary."""
        if not self.success:
            return f"❌ Import failed: {self.error_message}"

        duration = f" ({self.duration_seconds:.1f}s)" if self.duration_seconds else ""
        return (
            f"✅ Successfully imported{duration}:\n"
            f"  • {self.peptide_count:,} peptides\n"
            f"  • {self.modification_count:,} modifications\n"
            f"  • {self.peptide_evidence_count:,} protein mappings\n"
            f"  • {self.psm_count:,} PSMs"
        )
