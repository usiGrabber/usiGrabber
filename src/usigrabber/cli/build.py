import asyncio
import logging
import os
from pathlib import Path
from typing import Annotated

import typer
from sqlalchemy import inspect
from sqlmodel import Session, select

from usigrabber.backends import BackendEnum
from usigrabber.cli import app
from usigrabber.db import Project, create_db_and_tables, load_db_engine
from usigrabber.db.cli import reset as db_reset
from usigrabber.file_parser import MzidImportError, MzidParseError, import_mzid, import_mztab
from usigrabber.utils import get_cache_dir
from usigrabber.utils.file import download_ftp, extract_archive, temporary_path

CACHE_DIR = get_cache_dir()
STANDARD_BACKENDS = [enum for enum in BackendEnum]
logger = logging.getLogger(__name__)

# empty string for folders (no extension)
FILETYPE_WHITELIST = {".mzid", "", ".mztab"}


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
    asyncio.run(async_build(debug, reset, backends, no_ontology, cache_dir))


async def async_build(
    debug: bool = False,
    reset: bool = False,
    backends: list[BackendEnum] = STANDARD_BACKENDS,
    no_ontology: bool = False,
    cache_dir: Path = CACHE_DIR,
) -> None:
    """Build USI database."""

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
        with Session(db_engine) as session:
            async for project in backend.get_new_projects(existing_accessions):
                # TODO: support other submission types
                if project.get("submissionType") != "COMPLETE":
                    continue

                try:
                    await backend.dump_project_to_db(session, project)
                except Exception as e:
                    errors += 1
                    error_projects.append((project.get("accession"), str(e)))
                    session.rollback()
                    continue

                session.commit()
                imported += 1
                # download files
                files = backend.get_files_for_project(project["accession"])

                # process files
                if files["result"]:
                    for file in files["result"]:
                        # parse filename from file url
                        file_url = file["filepath"]
                        filename = os.path.basename(file_url)

                        # find actual file extension, without archives
                        file_base, file_ext = os.path.splitext(filename)
                        while file_ext in {".zip", ".gz", ".tar", ".rar", ".7z"}:
                            file_base, file_ext = os.path.splitext(file_base)

                        if file_ext not in FILETYPE_WHITELIST:
                            logger.debug(
                                f"Skipping file {filename} with unsupported extension {file_ext}."
                            )
                            continue

                        logger.debug(
                            f"Processing result file {filename} "
                            + f"({file['file_size'] / (1024 * 1024):,.2f} MB)"
                        )

                        with temporary_path() as tmp_dir:
                            try:
                                path = await download_ftp(
                                    url=file_url,
                                    out_dir=tmp_dir,
                                    file_name=filename,
                                )
                            except Exception:
                                logger.error(
                                    "Failed to download file %s for project %s.",
                                    filename,
                                    project["accession"],
                                    exc_info=True,
                                )
                                continue

                            # contains all files extracted from archive
                            extracted_files = extract_archive(
                                archive_path=path, extract_to=tmp_dir / "extracted"
                            )

                            interesting_files: dict[str, list[Path]] = {
                                ext: [] for ext in FILETYPE_WHITELIST
                            }
                            for f in extracted_files:
                                ext = os.path.splitext(str(f))[1]
                                if ext in FILETYPE_WHITELIST:
                                    interesting_files[ext].append(f)

                            for mztab_file in interesting_files[".mztab"]:
                                import_mztab(mztab_file, project["accession"])

                            # access files based on priority
                            for mzid_file in interesting_files[".mzid"]:
                                # Process mzID file
                                try:
                                    stats = import_mzid(mzid_file, project["accession"])
                                    duration_str = (
                                        f"{stats.duration_seconds:.1f}s"
                                        if stats.duration_seconds is not None
                                        else "N/A"
                                    )
                                    logger.info(
                                        f"Imported {stats.psm_count:,} PSMs from {mzid_file.name} "
                                        f"({duration_str})"
                                    )
                                except MzidParseError as e:
                                    logger.warning(
                                        f"Skipping malformed mzID file {mzid_file.name}: {e}"
                                    )
                                    continue
                                except MzidImportError as e:
                                    logger.error(
                                        f"Failed to import mzID file {mzid_file.name}: {e}",
                                        exc_info=True,
                                        stack_info=True,
                                        extra={
                                            "mzid_file": str(mzid_file),
                                            "project_accession": project["accession"],
                                        },
                                    )
                                    errors += 1
                                    continue
                            # TODO: add processing for other file types here

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
            logger.info("Finished importing from backend %s.", backend_enum.name)
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
