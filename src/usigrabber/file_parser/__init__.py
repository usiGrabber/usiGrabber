"""
File Parser Module

Provides parsers for various proteomics file formats.
"""

from usigrabber.file_parser.errors import (
    DatabaseError,
    FileParserError,
    FileReadError,
    MzidImportError,
    MzidParseError,
    ParseError,
    ValidationError,
)
from usigrabber.file_parser.models import ImportStats
from usigrabber.file_parser.mzid_db_parser import import_mzid

__all__ = [
    # Main functions
    "import_mzid",
    # Models
    "ImportStats",
    # Exceptions
    "FileParserError",
    "FileReadError",
    "ParseError",
    "ValidationError",
    "DatabaseError",
    "MzidParseError",
    "MzidImportError",
]
