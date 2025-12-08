import asyncio
import datetime
import logging
import os
import warnings
from pathlib import Path
from typing import Annotated

import typer
from sqlalchemy import exc as sa_exc
from sqlalchemy import inspect
from sqlmodel import Session, select

from usigrabber.backends import BackendEnum
from usigrabber.backends.base import FileMetadata
from usigrabber.cli import app
from usigrabber.db import Project, create_db_and_tables, load_db_engine
from usigrabber.db.cli import reset as db_reset
from usigrabber.file_parser import import_file
from usigrabber.file_parser.errors import FileParserError
from usigrabber.utils import get_cache_dir
from usigrabber.utils.file import (
    download_ftp,
    download_ftp_with_semamphore,
    extract_archive,
    temporary_path,
)

CACHE_DIR = get_cache_dir()
STANDARD_BACKENDS = [enum for enum in BackendEnum]
logger = logging.getLogger(__name__)

# empty string for folders (no extension)
FILETYPE_ALLOWLIST = {".mzid", "", ".mztab"}

MAX_FILESIZE_BYTES = 5 * 1024**3  # 5 GB
PARALLEL_DOWNLOADS = int(os.getenv("PARALLEL_DOWNLOADS", "10"))


@app.command()
def build(
    debug: Annotated[
        bool,
        typer.Option(help="Run in debug mode with verbose output.", envvar="DEBUG"),
    ] = False,
    reset: Annotated[bool, typer.Option(help="Reset the database before building.")] = False,
    backends: Annotated[
        list[BackendEnum],
        typer.Option(help="Set of backends to fetch data from."),
    ] = STANDARD_BACKENDS,
    no_ontology: Annotated[
        bool,
        typer.Option(
            help="Disable ontology lookup.",
            envvar="NO_ONTOLOGY",
        ),
    ] = False,
    cache_dir: Annotated[
        Path,
        typer.Option(
            help="Path to the cache dir.",
            envvar="CACHE_DIR",
            exists=True,
            dir_okay=True,
            file_okay=False,
            writable=True,
            readable=True,
            resolve_path=True,
        ),
    ] = CACHE_DIR,
):
    # mute SQLAlchemy warnings from pyteomics library
    with warnings.catch_warnings(action="ignore", category=sa_exc.SAWarning):
        asyncio.run(async_build(debug, reset, backends, no_ontology, cache_dir))


async def async_build(
    debug: bool = False,
    reset: bool = False,
    backends: list[BackendEnum] = STANDARD_BACKENDS,
    no_ontology: bool = False,
    cache_dir: Path = CACHE_DIR,
) -> None:
    """Build USI database."""

    if os.name == "nt":
        raise RuntimeError(
            "Windows is not supported. This application requires the 'sed' command-line utility, "
            "which is available by default on most Linux and macOS systems."
        )

    logger.info("Building database.")

    os.environ["CACHE_DIR"] = str(cache_dir)

    if no_ontology:
        os.environ["NO_ONTOLOGY"] = "1"

    if os.getenv("NO_ONTOLOGY"):
        logger.warning("Ontology lookup is disabled.")

    if debug:
        os.environ["DEBUG"] = "1"

    if os.getenv("DEBUG"):
        logger.info("Running in DEBUG mode.")

    if reset:
        logger.info("Resetting database before build.")
        db_reset(force=True)

    # WORKFLOW
    build_start_time = asyncio.get_event_loop().time()

    # set up database connection
    db_engine = load_db_engine()
    inspector = inspect(db_engine)
    if len(inspector.get_table_names()) == 0:
        logger.info("No preexisting database found. Database will be initialized.")
        create_db_and_tables(db_engine)

    # get all existing project accessions in the database
    with Session(db_engine) as session:
        statement = select(Project.accession)
        existing_accessions: set[str] = set(session.exec(statement).all())

    logger.info(
        "Found %s existing accessions in the database.",
        len(existing_accessions),
    )

    for backend_enum in backends:
        backend = backend_enum.value
        logger.debug("Fetching data from backend: %s", backend_enum.name)

        imported = errors = 0
        error_projects = []
        async for project in backend.get_new_projects(existing_accessions):
            # TODO: support other submission types
            if project.get("submissionType") != "COMPLETE":
                continue

            with Session(db_engine) as session:
                try:
                    await backend.dump_project_to_db(session, project)
                    session.commit()
                except Exception as e:
                    errors += 1
                    error_projects.append((project.get("accession"), str(e)))
                    session.rollback()
                    continue

            imported += 1
            # download files
            files = backend.get_files_for_project(project["accession"])

            with temporary_path() as tmp_dir:
                # process files
                if files["result"]:
                    files_to_be_downloaded: list[FileMetadata] = []

                    # filter files based on extension allowlist
                    for file in files["result"]:
                        # parse filename from file url
                        file_url = file["filepath"]
                        filename = os.path.basename(file_url)

                        if file["file_size"] > MAX_FILESIZE_BYTES:
                            logger.warning(
                                "Skipping file '%s' in project %s due "
                                + "to size (%.2f GiB > %.2f GiB).",
                                filename,
                                project["accession"],
                                file["file_size"] / (1024**3),
                                MAX_FILESIZE_BYTES / (1024**3),
                            )
                            continue

                        # find actual file extension, without archives
                        file_base, file_ext = os.path.splitext(filename)
                        while file_ext in {".zip", ".gz", ".tar", ".rar", ".7z"}:
                            file_base, file_ext = os.path.splitext(file_base)

                        if file_ext not in FILETYPE_ALLOWLIST:
                            logger.debug(
                                "Skipping file %s with unsupported extension %s.",
                                filename,
                                file_ext,
                            )
                            continue

                        files_to_be_downloaded.append(file)

                    if len(files_to_be_downloaded) == 0:
                        logger.warning(
                            "Found result files for project %s, but none match "
                            "the supported file types.",
                            project["accession"],
                        )
                        continue

                    # download all matching files asynchronously (limit concurrency)
                    sem = asyncio.Semaphore(PARALLEL_DOWNLOADS)
                    path_coros = [
                        download_ftp_with_semamphore(
                            semaphore=sem,
                            url=file["filepath"],
                            out_dir=tmp_dir / project["accession"] / str(idx),
                        )
                        for idx, file in enumerate(files_to_be_downloaded)
                    ]

                    for fut in asyncio.as_completed(path_coros):
                        try:
                            path = await fut
                        except Exception as e:
                            logger.error(
                                "Error while downloading file for project %s: %s",
                                project["accession"],
                                e,
                                exc_info=True,
                            )
                            continue

                        # contains all files extracted from archive
                        extracted_files = extract_archive(
                            archive_path=path, extract_to=path.parent / "extracted"
                        )

                        interesting_files: dict[str, list[Path]] = {
                            ext: [] for ext in FILETYPE_ALLOWLIST
                        }
                        for f in extracted_files:
                            ext = os.path.splitext(str(f))[1]
                            if ext in FILETYPE_ALLOWLIST:
                                interesting_files[ext].append(f)

                        for _, flist in interesting_files.items():
                            for file in flist:
                                try:
                                    stats = import_file(
                                        db_engine,
                                        file,
                                        project["accession"],
                                    )

                                    parsing_duration = stats.format_duration(stats.parsing_duration)
                                    persist_duration = stats.format_duration(
                                        stats.persisting_duration
                                    )
                                    total_duration = stats.format_duration(stats.duration_seconds)

                                    logger.info(
                                        f"Imported {stats.psm_count:,} PSMs from '{file.name}'"
                                        f"(parsing: {parsing_duration} | "
                                        f"persisting: {persist_duration} | "
                                        f"total: {total_duration}).",
                                        extra={
                                            "project_accession": project["accession"],
                                            "file_name": file.name,
                                            "duration": {
                                                "parsing": stats.parsing_duration,
                                                "persist": stats.persisting_duration,
                                                "total": stats.duration_seconds,
                                            },
                                        },
                                    )
                                except FileParserError as e:
                                    logger.error(
                                        f"Failed to import file '{file.name}': {e}",
                                        exc_info=True,
                                        stack_info=True,
                                        extra={
                                            "file": str(file),
                                            "project_accession": project["accession"],
                                        },
                                    )
                                    errors += 1
                                    continue

                elif files["search"]:
                    # TODO: support search files
                    continue
                else:
                    logger.warning(
                        "No results/search files found for project '%s' from backend %s.",
                        project["accession"],
                        backend_enum.name,
                    )

            # TODO: set "complete" flag for project

        if imported > 0 or errors > 0:
            logger.info(
                "Finished importing from backend %s.",
                backend_enum.name,
            )
            logger.info(
                "Successfully imported %s projects, encountered %s errors (%.1f%%).",
                imported,
                errors,
                (imported / (imported + errors) * 100),
            )
            if error_projects:
                logger.warning("\nFailed Projects:")
                for accession, error in error_projects[:10]:  # Show first 10
                    logger.warning("  • %s: %s", accession, error[:80])
                if len(error_projects) > 10:
                    logger.warning("  ... and %d more", len(error_projects) - 10)

    build_duration = asyncio.get_event_loop().time() - build_start_time
    logger.info(
        "Database build process completed in %s.", str(datetime.timedelta(seconds=build_duration))
    )
    logger.info(
        "FTP download statistics: %s",
        download_ftp.statistics,  # type: ignore
        extra=download_ftp.statistics,  # type: ignore
    )
