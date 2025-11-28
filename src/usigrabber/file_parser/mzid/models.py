"""
mzID Parser Data Models

Data structures for holding parsed mzIdentML data.
"""

from typing import NamedTuple

from usigrabber.db.schema import (
    MzidFile,
    Peptide,
    PeptideEvidence,
    PeptideModification,
    PeptideSpectrumMatch,
    PSMPeptideEvidence,
)


class ParsedMzidData(NamedTuple):
    """Container for all parsed data from an mzIdentML file."""

    mzid_file: MzidFile
    peptides: list[Peptide]
    peptide_modifications: list[PeptideModification]
    peptide_evidence: list[PeptideEvidence]
    psms: list[PeptideSpectrumMatch]
    psm_peptide_evidence_junctions: list[PSMPeptideEvidence]


class ParsedMztabData(NamedTuple):
    peptides: list[dict]
    psms: list[dict]
