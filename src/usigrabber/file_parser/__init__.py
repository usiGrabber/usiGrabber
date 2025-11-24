"""
File Parser Module

Provides parsers for various proteomics file formats.
"""

from usigrabber.file_parser.errors import (
    FileParserError,
    MzidImportError,
    MzidParseError,
    TxtZipImportError,
    TxtZipParseError,
)
from usigrabber.file_parser.models import ImportStats
from usigrabber.file_parser.mzid import import_mzid
from usigrabber.file_parser.txt_zip import import_all_txt_zip

__all__ = [
    # Main functions
    "import_mzid",
    "import_all_txt_zip",
    # Models
    "ImportStats",
    # Exceptions
    "FileParserError",
    "MzidParseError",
    "MzidImportError",
    "TxtZipImportError",
    "TxtZipParseError",
]
