"""
txt.zip Parser Data Models
"""

from typing import NamedTuple

from usigrabber.db.schema import (
    Peptide,
    PeptideEvidence,
    PeptideModification,
    PeptideSpectrumMatch,
    PSMPeptideEvidence,
    SearchModification,
)


class ParsedTxtZipData(NamedTuple):
    """Container for all parsed data from an evidence.txt file."""

    peptides: list[Peptide]
    peptide_modifications: list[PeptideModification]
    peptide_evidence: list[PeptideEvidence]
    psms: list[PeptideSpectrumMatch]
    psm_peptide_evidence_junctions: list[PSMPeptideEvidence]
    search_modifications: list[SearchModification]
