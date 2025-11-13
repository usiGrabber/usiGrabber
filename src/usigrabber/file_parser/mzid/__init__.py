"""
mzID Parser Module

Parses mzIdentML files using pyteomics and populates the database with:
- Peptides (one per mzID Peptide element, not deduplicated)
- Peptide Modifications (UNIMOD-based)
- Peptide Evidence (protein mappings)
- Peptide Spectrum Matches (PSMs)
- PSM-PeptideEvidence junction records

Uses retrieve_refs=False to avoid handling deduplication in the code.
"""

from usigrabber.file_parser.mzid.models import ParsedMzidData
from usigrabber.file_parser.mzid.parser import import_mzid, parse_mzid_file

# Public API
__all__ = [
    "ParsedMzidData",
    "parse_mzid_file",
    "import_mzid",
]
