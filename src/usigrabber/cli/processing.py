"""Shared project processing logic for build and profile commands."""

import asyncio
import logging
import os
from asyncio.taskgroups import TaskGroup
from pathlib import Path

from sqlalchemy import Engine

from usigrabber.backends import BackendEnum
from usigrabber.backends.base import FileMetadata
from usigrabber.file_parser import MzidImportError, MzidParseError, import_mzid
from usigrabber.utils.file import (
    download_ftp,
    extract_archive,
    temporary_path,
)

logger = logging.getLogger(__name__)

# empty string for folders (no extension)
FILETYPE_ALLOWLIST = {".mzid", ""}
PARALLEL_DOWNLOADS = int(os.getenv("PARALLEL_DOWNLOADS", "10"))


async def process_and_log_mzid_file(
    engine: Engine, mzid_file: Path, project_accession: str
) -> None:
    try:
        stats = await asyncio.to_thread(lambda: import_mzid(engine, mzid_file, project_accession))
        duration_str = (
            f"{stats.duration_seconds:.1f}s" if stats.duration_seconds is not None else "N/A"
        )
        logger.info(
            f"Imported {stats.psm_count:,} PSMs from '{mzid_file.name}' ({duration_str})",
            extra={
                "event": "mzid_imported",
                "project_accession": project_accession,
                "mzid_file": mzid_file.name,
                "psm_count": stats.psm_count,
                "parse_time": stats.duration_seconds,
            },
        )
    except MzidParseError as e:
        logger.warning(
            f"Skipping malformed mzID file '{mzid_file.name}': {e}",
            extra={
                "event": "mzid_parse_error",
                "error_type": "MzidParseError",
                "mzid_file": str(mzid_file),
                "project_accession": project_accession,
            },
        )
        return
    except MzidImportError as e:
        logger.error(
            f"Failed to import mzID file '{mzid_file.name}': {e}",
            exc_info=True,
            stack_info=True,
            extra={
                "event": "mzid_import_error",
                "error_type": "MzidImportError",
                "mzid_file": str(mzid_file),
                "project_accession": project_accession,
            },
        )
        raise


async def process_file(
    engine: Engine, file: FileMetadata, tmp_dir: Path, project_accession: str, idx: int
) -> None:
    file_url = file["filepath"]
    filename = os.path.basename(file_url)

    # Find actual file extension, without archives
    file_base, file_ext = os.path.splitext(filename)
    while file_ext in {".zip", ".gz", ".tar", ".rar", ".7z"}:
        file_base, file_ext = os.path.splitext(file_base)

    if file_ext not in FILETYPE_ALLOWLIST:
        logger.debug(
            "Skipping file %s with unsupported extension %s.",
            filename,
            file_ext,
        )
        return

    try:
        downloaded_path = await download_ftp(
            url=file["filepath"],
            out_dir=tmp_dir / project_accession / str(idx),
        )
    except Exception:
        logger.error(f"Failed to download file: {file['filepath']}", exc_info=True)
        return

    extracted_files = await asyncio.to_thread(
        lambda: extract_archive(
            archive_path=downloaded_path, extract_to=downloaded_path.parent / "extracted"
        )
    )

    interesting_files: dict[str, list[Path]] = {ext: [] for ext in FILETYPE_ALLOWLIST}
    for f in extracted_files:
        ext = os.path.splitext(str(f))[1]
        if ext in FILETYPE_ALLOWLIST:
            interesting_files[ext].append(f)

    # Process mzID files
    async with TaskGroup() as tg:
        for mzid_file in interesting_files[".mzid"]:
            tg.create_task(process_and_log_mzid_file(engine, mzid_file, project_accession))


async def process_project(
    engine: Engine,
    project: dict,
    backend: BackendEnum,
) -> None:
    """
    Process a single project through the full pipeline.

    Args:
        session: Database session
        project: Project metadata dictionary
        backend: Backend enum
    """
    backend_impl = backend.value
    project_accession = project["accession"]

    logger.info(
        f"Processing project {project_accession}",
        extra={
            "event": "project_start",
            "project_accession": project_accession,
            "backend": backend.name,
        },
    )

    # Check submission type
    if project.get("submissionType") != "COMPLETE":
        logger.info(
            f"Skipping {project_accession} because it is not COMPLETE",
            extra={
                "event": "project_skipped",
                "project_accession": project_accession,
                "reason": "not_complete",
                "backend": backend.name,
                "submission_type": project.get("submissionType"),
            },
        )
        return

    # Save project metadata
    try:
        await backend_impl.dump_project_to_db(engine, project)
        logger.info(
            f"Project {project_accession} metadata saved to DB",
            extra={
                "event": "project_metadata_saved",
                "project_accession": project_accession,
            },
        )
    except Exception as e:
        logger.error(
            f"Failed to save project {project_accession} metadata",
            exc_info=True,
            extra={
                "event": "project_metadata_error",
                "project_accession": project_accession,
                "error_type": type(e).__name__,
                "backend": backend.name,
            },
        )
        raise

    # Download and process files
    files = await backend_impl.get_files_for_project(project["accession"])

    async with TaskGroup() as tg:
        with temporary_path() as tmp_dir:
            # idx is relevant such that files not overwrite each other!! Pay attention with multiple loops!
            for idx, file in enumerate(files["result"]):
                tg.create_task(process_file(engine, file, tmp_dir, project_accession, idx))

    logger.info(
        f"Project {project_accession} completed successfully",
        extra={
            "event": "project_completed",
            "project_accession": project_accession,
            "backend": backend.name,
        },
    )
