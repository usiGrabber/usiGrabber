"""
File Parser Module

Provides a unified interface for importing proteomics files.
"""

from usigrabber.file_parser.base import BaseFileParser, get_parser_for_extension, register_parser
from usigrabber.file_parser.errors import FileParserError
from usigrabber.file_parser.helpers import log_info
from usigrabber.file_parser.models import ImportStats
from usigrabber.file_parser.mzid.parser import MzidFileParser  # noqa: F401
from usigrabber.file_parser.mztab.parser import MztabFileParser  # noqa: F401
from usigrabber.file_parser.txt_zip.parser import TxtZipFileParser  # noqa: F401

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


def import_files(engine, paths, project_accession, logger) -> bool:
    """
    Generic file import function for multiple files.

    Automatically selects the appropriate parser based on file extensions.
    """
    from pathlib import Path

    exception_occurred = False

    for path in paths:
        if not isinstance(path, Path):
            path = Path(path)

    file_ext = paths[0].suffix

    if file_ext == ".txt":
        try:
            file_stats = import_file(engine, paths, file_ext, project_accession)
            if file_stats.psm_count:
                log_info(logger, file_stats, file_ext)
        except FileParserError as e:
            logger.error(
                f"Failed to import '{file_ext}' files: {e}",
                exc_info=True,
                stack_info=True,
                extra={
                    "ext": str(file_ext),
                    "project_accession": project_accession,
                },
            )
            exception_occurred = True
    else:
        for path in paths:
            try:
                file_stats = import_file(engine, [path], file_ext, project_accession)
                if file_stats.psm_count:
                    log_info(logger, file_stats, path.name)
            except FileParserError as e:
                logger.error(
                    f"Failed to import '{file_ext}' files: {e}",
                    exc_info=True,
                    stack_info=True,
                    extra={
                        "ext": str(file_ext),
                        "project_accession": project_accession,
                    },
                )
                exception_occurred = True

    return not exception_occurred


def import_file(engine, path_list, file_ext, project_accession) -> ImportStats:
    """
    Generic file import function.

    Automatically selects the appropriate parser based on file extension.
    """

    parser = get_parser_for_extension(file_ext)
    if not parser:
        raise FileParserError(f"No parser registered for file extension '{file_ext}'")

    return (
        parser.import_file(engine, path_list[0], project_accession)
        if len(path_list) == 1
        else parser.import_file(engine, path_list, project_accession)
    )
