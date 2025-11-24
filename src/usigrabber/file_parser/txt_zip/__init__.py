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
from usigrabber.file_parser.txt_zip.parser import import_all_txt_zip, parse_txt_zip

# Public API
__all__ = [
    # "ParsedMzidData",
    "parse_txt_zip",
    "import_all_txt_zip",
]
