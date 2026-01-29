"""
File Parser Module

Provides a unified interface for importing proteomics files.
"""

import asyncio
from logging import Logger
from pathlib import Path

from aioftp import StatusCodeError
from sqlalchemy import Engine

from usigrabber.backends.base import FileMetadata
from usigrabber.file_parser.base import BaseFileParser, get_parser_for_extension, register_parser
from usigrabber.file_parser.errors import FileParserError
from usigrabber.file_parser.helpers import get_txt_triples, log_info
from usigrabber.file_parser.models import ImportStats
from usigrabber.file_parser.mzid.parser import MzidFileParser  # noqa: F401
from usigrabber.file_parser.mztab.parser import MztabFileParser  # noqa: F401
from usigrabber.file_parser.txt_zip.parser import TxtZipFileParser  # noqa: F401
from usigrabber.utils.file import PARALLEL_DOWNLOADS, download_ftp_with_semaphore, extract_archive

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


async def import_files(
    engine: Engine,
    ftp_paths: list[str],
    file_ext: str,
    project_accession: str,
    tmp_dir: Path,
    logger: Logger,
    raw_files: list[FileMetadata],
) -> bool:
    """
    Generic file import function for multiple files.

    Automatically selects the appropriate parser based on file extensions.
    """
    exception_occurred = False

    sem = asyncio.Semaphore(PARALLEL_DOWNLOADS)

    # files to handle in bulk
    if file_ext == ".txt":
        paths = await asyncio.gather(
            *[
                download_ftp_with_semaphore(
                    semaphore=sem,
                    url=path,
                    out_dir=tmp_dir,
                )
                for path in ftp_paths
            ],
            return_exceptions=True,
        )
        paths: list[Path] = [path for path in paths if isinstance(path, Path)]
        if not paths:
            logger.error(
                f"No valid txt files were downloaded for processing {file_ext} in project "
                f"{project_accession}."
            )
            exception_occurred = True
        else:
            relevant_paths = extract_relevant_paths(paths, file_ext)
            txt_triplets = get_txt_triples(relevant_paths)
            for triplet in txt_triplets:
                try:
                    file_stats = import_file(engine, triplet, file_ext, project_accession)
                    if file_stats.psm_count:
                        log_info(logger, file_stats, file_ext)
                except FileParserError as e:
                    logger.error(
                        f"Failed to import '{file_ext}' files: {e}",
                        exc_info=True,
                        stack_info=True,
                        extra={
                            "ext": file_ext,
                            "project_accession": project_accession,
                        },
                    )
                    exception_occurred = True
                    continue
    # files to handle individually
    else:
        path_coros = [
            download_ftp_with_semaphore(
                semaphore=sem,
                url=path,
                out_dir=tmp_dir,
            )
            for path in ftp_paths
        ]
        for coro in asyncio.as_completed(path_coros):
            try:
                path = await coro
                relevant_paths = extract_relevant_paths([path], file_ext)
                for path in relevant_paths:
                    file_stats = import_file(engine, path, file_ext, project_accession)
                    if file_stats.psm_count:
                        log_info(logger, file_stats, Path(path).name)
            except FileParserError as e:
                logger.error(
                    f"Failed to import '{file_ext}' files: {e}",
                    exc_info=True,
                    stack_info=True,
                    extra={
                        "ext": file_ext,
                        "project_accession": project_accession,
                    },
                )
                exception_occurred = True
                continue
            except StatusCodeError as e:
                logger.error(
                    f"Failed to download file from FTP: {e}",
                    exc_info=True,
                    stack_info=True,
                    extra={
                        "ext": file_ext,
                        "project_accession": project_accession,
                    },
                )
                exception_occurred = True
                continue

    return not exception_occurred


def import_file(engine, path, file_ext, project_accession) -> ImportStats:
    """
    Generic file import function.

    Automatically selects the appropriate parser based on file extension.
    """
    parser = get_parser_for_extension(file_ext)
    if not parser:
        raise FileParserError(f"No parser registered for file extension '{file_ext}'")

    return parser.import_file(engine, path, project_accession)


def extract_relevant_paths(paths: list[Path], file_ext: str) -> list[Path]:
    relevant_paths = []

    for p in paths:
        extract_dir = p.stem + "_extracted"
        extracted_files = extract_archive(archive_path=p, extract_to=p.parent / extract_dir)
        for f in extracted_files:
            ext = f.suffix
            if ext == file_ext:
                relevant_paths.append(f)

    return relevant_paths
