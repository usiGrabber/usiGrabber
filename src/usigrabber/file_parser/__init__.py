"""
File Parser Module

Provides a unified interface for importing proteomics files.
"""

from usigrabber.file_parser.base import BaseFileParser, get_parser_for_extension, register_parser
from usigrabber.file_parser.errors import FileParserError
from usigrabber.file_parser.models import ImportStats

__all__ = [
    # Base parser and registry
    "BaseFileParser",
    "register_parser",
    "get_parser_for_extension",
    # Shared models
    "ImportStats",
    # Exceptions
    "FileParserError",
]


def import_file(engine, path, project_accession):
    """
    Generic file import function.

    Automatically selects the appropriate parser based on file extension.
    """
    from pathlib import Path

    if not isinstance(path, Path):
        path = Path(path)

    parser = get_parser_for_extension(path.suffix)
    if not parser:
        raise FileParserError(f"No parser registered for file extension '{path.suffix}'")

    return parser.import_file(engine, path, project_accession)
