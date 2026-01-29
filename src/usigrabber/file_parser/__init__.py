"""
File Parser Module

Provides a unified interface for importing proteomics files.
"""

import asyncio
import logging
from pathlib import Path

from aioftp import StatusCodeError
from sqlalchemy import Engine

from usigrabber.file_parser.base import BaseFileParser, get_parser_for_extension, register_parser
from usigrabber.file_parser.errors import FileParserError
from usigrabber.file_parser.helpers import get_txt_triples, log_info
from usigrabber.file_parser.models import ImportStats
from usigrabber.file_parser.mzid.parser import MzidFileParser  # noqa: F401
from usigrabber.file_parser.mztab.parser import MztabFileParser  # noqa: F401
from usigrabber.file_parser.txt_zip.parser import TxtZipFileParser  # noqa: F401
from usigrabber.utils.checksum import md5_checksum
from usigrabber.utils.context import context_file_id
from usigrabber.utils.file import PARALLEL_DOWNLOADS, download_ftp_with_semaphore, extract_archive

logger = logging.getLogger(__name__)

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
) -> None:
    """
    Generic file import function for multiple files.

    Automatically selects the appropriate parser based on file extensions.
    """

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
            logger.warning(
                f"No valid txt files were downloaded for processing {file_ext} in project "
                f"{project_accession}."
            )
        else:
            relevant_paths = extract_relevant_paths(paths, file_ext)
            txt_triplets = get_txt_triples(relevant_paths)
            for triplet in txt_triplets:
                import_file(engine, triplet, file_ext, project_accession)
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
                path: Path = await coro
                checksum = md5_checksum(path)  # generate checksum before extraction
                relevant_paths = extract_relevant_paths([path], file_ext)
                for path in relevant_paths:
                    import_file(engine, path, file_ext, checksum, project_accession)
            except StatusCodeError as e:
                logger.error(
                    f"Failed to download file from FTP: {e}",
                    exc_info=True,
                    stack_info=True,
                    extra={
                        "ext": str(file_ext),
                    },
                )
            except Exception:
                # Raise any other exception because import_file should capture exceptions and any other exception is unexpected
                raise


def import_file(
    engine: Engine,
    path: Path | tuple[Path, Path, Path],
    file_ext: str,
    checksum: str,
    project_accession: str,
) -> None:
    """
    Generic file import function.

    Automatically selects the appropriate parser based on file extension.
    Correctly sets the context file_id for logging

    :param engine: Database engine for persisting data.
    :param path: Path to the file or tuple of paths (for txt triplets).
    :param file_ext: File extension to determine the parser.
    :param checksum: MD5 checksum of the file as it was downloaded from the source (pre-extraction).
    :param project_accession: Project accession identifier.
    """

    parser = get_parser_for_extension(file_ext)
    if not parser:
        # This is an important error which should be surfaced and the list of allowed files for parsing should be changed or the parser changed.
        # This will cause the project to fail but because its important we are surfacing the error here!
        raise FileParserError(f"No parser registered for file extension '{file_ext}'")

    file_id = parser.get_file_id(path)
    file_id_context_token = context_file_id.set(file_id)

    try:
        file_stats = parser.import_file(engine, path, checksum, project_accession)
        if file_stats.psm_count:
            log_info(logger, file_stats, file_ext)
    except FileParserError as e:
        logger.error(
            f"Failed to import '{file_ext}' files: {e}",
            exc_info=True,
            stack_info=True,
            extra={
                "ext": str(file_ext),
            },
        )

    finally:
        context_file_id.reset(file_id_context_token)


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
