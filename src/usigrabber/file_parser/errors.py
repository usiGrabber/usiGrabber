"""
File Parser Exceptions

Custom exception hierarchy for file parsing and import operations.
"""


class FileParserError(Exception):
    """Base exception for all file parser errors."""

    pass


class FileImportError(Exception):
    """Base exception for all import errors."""

    pass


class UnsupportedDatabaseError(Exception):
    """Exception for unsupported database dialects."""

    pass


# mzID-specific errors
class MzidParseError(FileParserError):
    """Failed to parse mzIdentML file."""

    pass


class MzidImportError(FileImportError):
    """Failed to import mzIdentML data to database."""

    pass


# mzTab-specific errors
class MztabParseError(FileParserError):
    """Failed to parse mzTab file."""

    pass


class MztabImportError(FileImportError):
    """Failed to import mzTab data to database."""

    pass


# evidence.txt-specific errors
class TxtZipParseError(FileParserError):
    """Failed to parse evidence.txt file."""

    pass


class TxtZipImportError(ImportError):
    """Failed to import evidence.txt data to database."""

    pass
