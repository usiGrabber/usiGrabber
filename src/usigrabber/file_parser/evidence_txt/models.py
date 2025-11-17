"""
evidence.txt Parser Data Models
"""

from typing import NamedTuple

from usigrabber.db.schema import (
    # EvidenceTxtFile,
    Peptide,
    PeptideEvidence,
    PeptideModification,
    PeptideSpectrumMatch,
    PSMPeptideEvidence,
)


class ParsedEvidenceTxtData(NamedTuple):
    """Container for all parsed data from an evidence.txt file."""

    # evidence_txt_file: EvidenceTxtFile
    peptides: list[Peptide]
    peptide_modifications: list[PeptideModification]
    peptide_evidence: list[PeptideEvidence]
    psms: list[PeptideSpectrumMatch]
    psm_peptide_evidence_junctions: list[PSMPeptideEvidence]
