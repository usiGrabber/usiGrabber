"""
evidence.txt Parser Module

Parses evidence.txt files and populates the database with:
- Peptides (one per row of evidence.txt file, not deduplicated)
- Peptide Modifications (UNIMOD-based)
- Peptide Evidence (just UniProt accessions)
- Peptide Spectrum Matches (PSMs)
- PSM-PeptideEvidence junction records

Uses retrieve_refs=False to avoid handling deduplication in the code.
"""

# from usigrabber.file_parser.mzid.models import ParsedMzidData
from usigrabber.file_parser.evidence_txt.parser import import_evidence_txt, parse_evidence_txt_file

# Public API
__all__ = [
    # "ParsedMzidData",
    "parse_evidence_txt_file",
    "import_evidence_txt",
]
