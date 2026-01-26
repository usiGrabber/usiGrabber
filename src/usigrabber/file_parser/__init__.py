"""
File Parser Module

Provides a unified interface for importing proteomics files.
"""

import asyncio
import logging
import traceback as tb
from pathlib import Path

from sqlalchemy import Engine
from sqlmodel import Session

from usigrabber.db.schema import DownloadedFile
from usigrabber.file_parser.base import BaseFileParser, get_parser_for_extension, register_parser
from usigrabber.file_parser.errors import FileParserError
from usigrabber.file_parser.helpers import get_txt_triples, log_info
from usigrabber.file_parser.models import ImportStats
from usigrabber.file_parser.mzid.parser import MzidFileParser  # noqa: F401
from usigrabber.file_parser.mztab.parser import MztabFileParser  # noqa: F401
from usigrabber.file_parser.txt_zip.parser import TxtZipFileParser  # noqa: F401
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
    engine: Engine, ftp_paths: list[str], file_ext, project_accession, tmp_dir
) -> None:
    """
    Generic file import function for multiple files.

    Automatically selects the appropriate parser based on file extensions.
    """

    sem = asyncio.Semaphore(PARALLEL_DOWNLOADS)

    # files to handle in bulk
    if file_ext == ".txt":
        results = await asyncio.gather(
            *[
                _download_and_track(
                    engine=engine,
                    semaphore=sem,
                    url=path,
                    out_dir=tmp_dir,
                    project_accession=project_accession,
                )
                for path in ftp_paths
            ],
        )
        downloads = [r for r in results if r is not None]
        if not downloads:
            logger.warning(
                f"No valid txt files were downloaded for processing {file_ext} in project "
                f"{project_accession}."
            )
        else:
            relevant_paths = _extract_relevant_paths(engine, downloads, file_ext, project_accession)
            txt_triplets = get_txt_triples(relevant_paths)
            for triplet in txt_triplets:
                import_file(engine, triplet, file_ext, project_accession)
    # files to handle individually
    else:
        path_coros = [
            _download_and_track(
                engine=engine,
                semaphore=sem,
                url=path,
                out_dir=tmp_dir,
                project_accession=project_accession,
            )
            for path in ftp_paths
        ]
        for coro in asyncio.as_completed(path_coros):
            result = await coro
            if result is None:
                continue
            relevant_paths = _extract_relevant_paths(engine, [result], file_ext, project_accession)
            for p in relevant_paths:
                import_file(engine, p, file_ext, project_accession)


async def _download_and_track(
    engine: Engine,
    semaphore: asyncio.Semaphore,
    url: str,
    out_dir: Path,
    project_accession: str,
) -> tuple[Path, str] | None:
    """Download a file and track failure in DownloadedFile table.

    Returns (local_path, file_path) on success, None on failure.
    file_path is the URL path without the FTP prefix, used as DownloadedFile.file_path.
    """
    # Extract path without FTP prefix (e.g., "pride/data/archive/2023/01/PXD000001/file.mzid.gz")
    file_path = _extract_file_path(url)

    try:
        path = await download_ftp_with_semaphore(
            semaphore=semaphore,
            url=url,
            out_dir=out_dir,
        )
        return (path, file_path)
    except Exception as e:
        logger.error(
            f"Failed to download file from FTP: {e}",
            exc_info=True,
            extra={"ftp_url": url},
        )
        _record_download_error(engine, project_accession, file_path, e)
        return None


def _extract_file_path(url: str) -> str:
    """Extract file path from FTP URL, removing the protocol and host."""
    # ftp://ftp.pride.ebi.ac.uk/pride/data/archive/2023/01/PXD000001/file.mzid.gz
    # -> pride/data/archive/2023/01/PXD000001/file.mzid.gz
    if "://" in url:
        url = url.split("://", 1)[1]  # Remove protocol
    if "/" in url:
        url = url.split("/", 1)[1]  # Remove host
    return url


def _record_download_error(
    engine: Engine,
    project_accession: str,
    file_path: str,
    error: Exception,
) -> None:
    """Record download/extraction failure in DownloadedFile table."""
    with Session(engine) as session:
        record = DownloadedFile(
            file_path=file_path,
            project_accession=project_accession,
            is_successful=False,
            error_message=str(error),
            traceback=tb.format_exc(),
        )
        session.add(record)
        session.commit()


def import_file(engine, path: Path | tuple[Path, Path, Path], file_ext, project_accession) -> None:
    """
    Generic file import function.

    Automatically selects the appropriate parser based on file extension.
    Correctly sets the context file_id for logging
    """

    parser = get_parser_for_extension(file_ext)
    if not parser:
        # This is an important error which should be surfaced and the list of allowed files for parsing should be changed or the parser changed.
        # This will cause the project to fail but because its important we are surfacing the error here!
        raise FileParserError(f"No parser registered for file extension '{file_ext}'")

    file_id = parser.get_file_id(path)
    file_id_context_token = context_file_id.set(file_id)

    try:
        file_stats = parser.import_file(engine, path, project_accession)
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


def _extract_relevant_paths(
    engine: Engine,
    downloads: list[tuple[Path, str]],
    file_ext: str,
    project_accession: str,
) -> list[Path]:
    """Extract archives and return paths matching file_ext. Records failures to DB."""
    relevant_paths = []

    for local_path, file_path in downloads:
        extract_dir = local_path.stem + "_extracted"
        try:
            extracted_files = extract_archive(
                archive_path=local_path, extract_to=local_path.parent / extract_dir
            )
            for f in extracted_files:
                if f.suffix == file_ext:
                    relevant_paths.append(f)
        except Exception as e:
            logger.error(
                f"Failed to extract archive {local_path.name}: {e}",
                exc_info=True,
            )
            _record_download_error(engine, project_accession, file_path, e)

    return relevant_paths
