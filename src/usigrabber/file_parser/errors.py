"""
File Parser Exceptions

Custom exception hierarchy for file parsing and import operations.
"""


class FileParserError(Exception):
    """Base exception for all file parser errors."""

    pass


class FileReadError(FileParserError):
    """Failed to read or access file."""

    pass


class ParseError(FileParserError):
    """Failed to parse file contents."""

    pass


class ValidationError(FileParserError):
    """Data validation failed."""

    pass


class DatabaseError(FileParserError):
    """Database operation failed during import."""

    pass


# mzID-specific errors
class MzidParseError(ParseError):
    """Failed to parse mzIdentML file."""

    pass


class MzidImportError(DatabaseError):
    """Failed to import mzIdentML data to database."""

    pass
