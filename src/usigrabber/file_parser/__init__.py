"""
File Parser Module

Provides parsers for various proteomics file formats.
"""

from usigrabber.file_parser.errors import (
    FileParserError,
    MzidImportError,
    MzidParseError,
)
from usigrabber.file_parser.models import ImportStats
from usigrabber.file_parser.mzid import import_mzid
from usigrabber.file_parser.mztab import import_mztab

__all__ = [
    # Main functions
    "import_mzid",
    "import_mztab",
    # Models
    "ImportStats",
    # Exceptions
    "FileParserError",
    "MzidParseError",
    "MzidImportError",
]
