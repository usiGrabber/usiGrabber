"""
File Parser Module

Provides a unified interface for importing proteomics files.
"""

import asyncio
import logging
import os
import traceback as tb
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import Engine
from sqlmodel import Session

from usigrabber.backends.base import FileMetadata
from usigrabber.db.schema import DownloadedFile
from usigrabber.file_parser.base import BaseFileParser, get_parser_for_extension, register_parser
from usigrabber.file_parser.errors import FileParserError
from usigrabber.file_parser.helpers import get_txt_triples, log_info
from usigrabber.file_parser.models import ImportStats

# Import to register parsers
from usigrabber.file_parser.mzid.parser import MzidFileParser  # noqa: F401
from usigrabber.file_parser.mztab.parser import MztabFileParser  # noqa: F401
from usigrabber.file_parser.txt_zip.parser import TxtZipFileParser  # noqa: F401
from usigrabber.utils.checksum import md5_checksum
from usigrabber.utils.context import context_file_id
from usigrabber.utils.file import PARALLEL_DOWNLOADS, download_ftp_with_semaphore, extract_archive
from usigrabber.utils.job_id import get_job_id

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
    "import_file",
]


@dataclass
class DownloadResult:
    """Result of download + extraction."""

    file_name: str
    start_time: float
    end_time: float
    local_path: Path | None = None
    extracted_paths: list[Path] | None = None
    file_size: int | None = None
    checksum: str | None = None
    error_message: str | None = None
    traceback_str: str | None = None

    @property
    def is_successful(self) -> bool:
        return self.error_message is None


async def import_files(
    engine: Engine,
    ftp_paths: list[str],
    file_ext: str,
    project_accession: str,
    tmp_dir: Path,
    raw_files: list[FileMetadata],
) -> None:
    """Download, extract, and import proteomics files."""
    sem = asyncio.Semaphore(PARALLEL_DOWNLOADS)

    # Download and extract all files in parallel
    results = await asyncio.gather(
        *[
            _download_and_extract(sem, url, tmp_dir, file_ext, engine, project_accession)
            for url in ftp_paths
        ],
        return_exceptions=True,  # One failure won't cancel others
    )

    successful_paths: list[Path] = []
    for result in results:
        if isinstance(result, DownloadResult) and result.is_successful and result.extracted_paths:
            successful_paths.extend(result.extracted_paths)

    if not successful_paths:
        logger.warning(
            f"No valid {file_ext} files were downloaded for project {project_accession}."
        )
        return

    # Import the files
    if file_ext == ".txt":
        triplets = get_txt_triples(successful_paths)
        for triplet in triplets:
            import_file(engine, triplet, file_ext, project_accession, raw_files)
    else:
        for path in successful_paths:
            import_file(engine, path, file_ext, project_accession, raw_files)


async def _download_and_extract(
    sem: asyncio.Semaphore,
    url: str,
    out_dir: Path,
    file_ext: str,
    engine: Engine,
    project_accession: str,
) -> DownloadResult:
    """Download and extract a file.
    Returns DownloadResult or raises asyncio.CancelledError
    """
    import time

    file_name: str = os.path.basename(url)
    start_time = time.time()

    download_result = None
    try:
        local_path = await download_ftp_with_semaphore(sem, url, out_dir)
        file_size = local_path.stat().st_size
        checksum = md5_checksum(local_path)
        extract_dir = local_path.stem + "_extracted"
        extracted = extract_archive(local_path, local_path.parent / extract_dir)
        relevant = [f for f in extracted if f.suffix == file_ext]
        download_result = DownloadResult(
            file_name=file_name,
            start_time=start_time,
            end_time=time.time(),
            local_path=local_path,
            extracted_paths=relevant,
            file_size=file_size,
            checksum=checksum,
        )
        return download_result
    except TimeoutError:
        logger.error(f"Download of {url} timed out.", exc_info=True)
        download_result = DownloadResult(
            file_name=file_name,
            start_time=start_time,
            end_time=time.time(),
            error_message="Download timed out",
            traceback_str=tb.format_exc(),
        )
        return download_result
    except asyncio.CancelledError:
        download_result = DownloadResult(
            file_name=file_name,
            start_time=start_time,
            end_time=time.time(),
            error_message="Task was cancelled",
            traceback_str=tb.format_exc(),
        )
        raise  # Re-raise so user cancellation still works
    except Exception as e:
        logger.error(f"Failed to download/extract {url}: {e}", exc_info=True)
        download_result = DownloadResult(
            file_name=file_name,
            start_time=start_time,
            end_time=time.time(),
            error_message=str(e),
            traceback_str=tb.format_exc(),
        )
        return download_result
    finally:
        assert download_result, "download results needs to be set in both try and except cases"
        from datetime import datetime

        with Session(engine) as session:
            record = DownloadedFile(
                project_accession=project_accession,
                file_name=download_result.file_name,
                file_size=download_result.file_size,
                checksum=download_result.checksum,
                start_time=datetime.fromtimestamp(download_result.start_time),
                end_time=datetime.fromtimestamp(download_result.end_time),
                is_successful=download_result.is_successful,
                error_message=download_result.error_message,
                traceback=download_result.traceback_str,
                worker_pid=os.getpid(),
                job_id=get_job_id(),
            )
            session.add(record)
            session.commit()


def import_file(
    engine: Engine,
    path: Path | tuple[Path, Path, Path],
    file_ext: str,
    project_accession: str,
    raw_files: list[FileMetadata],
) -> None:
    """Import a file using the appropriate parser. Errors are tracked in ImportedFile."""
    parser = get_parser_for_extension(file_ext)
    if not parser:
        raise FileParserError(f"No parser registered for file extension '{file_ext}'")

    file_id = parser.get_file_id(path)
    token = context_file_id.set(file_id)

    try:
        stats = parser.import_file(engine, path, project_accession, raw_files)
        if stats.psm_count:
            log_info(logger, stats, file_ext)
    except Exception as e:
        logger.error(f"Failed to import '{file_ext}' file: {e}", exc_info=True)
    finally:
        context_file_id.reset(token)
