"""
File Parser Exceptions

Custom exception hierarchy for file parsing and import operations.
"""


class FileParserError(Exception):
    """Base exception for all file parser errors."""

    pass


class ImportError(Exception):
    """Base exception for all import errors."""

    pass


# mzID-specific errors
class MzidParseError(FileParserError):
    """Failed to parse mzIdentML file."""

    pass


class MzidImportError(ImportError):
    """Failed to import mzIdentML data to database."""

    pass


# evidence.txt-specific errors
class EvidenceTxtParseError(FileParserError):
    """Failed to parse evidence.txt file."""

    pass


class EvidenceTxtImportError(ImportError):
    """Failed to import evidence.txt data to database."""

    pass
