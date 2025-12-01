import asyncio
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
from usigrabber.cli import app
from usigrabber.db import Project, create_db_and_tables, load_db_engine
from usigrabber.db.cli import reset as db_reset
from usigrabber.file_parser import (
    MzidImportError,
    MzidParseError,
    import_all_txt_zip,
    import_mzid,
)
from usigrabber.utils import get_cache_dir
from usigrabber.utils.file import (
    get_interesting_files,
    temporary_path,
)

CACHE_DIR = get_cache_dir()
STANDARD_BACKENDS = [enum for enum in BackendEnum]
logger = logging.getLogger(__name__)


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
        with Session(db_engine) as session, temporary_path() as tmp_dir_default:
            async for project in backend.get_new_projects(existing_accessions):
                tmp_dir = Path(tmp_dir_default / project["accession"])
                tmp_dir.mkdir()
                fully_processed: bool = False

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

                processed_files = 0
                main_source_type = None

                # process files
                if files["result"]:
                    interesting_files = await get_interesting_files(
                        files["result"], project["accession"], tmp_dir
                    )

                    # access files based on priority

                    # Process mzID file
                    if not main_source_type and interesting_files[".mzid"]:
                        main_source_type = ".mzid"
                        for mzid_file in interesting_files[".mzid"]:
                            try:
                                stats = import_mzid(db_engine, mzid_file, project["accession"])
                                duration_str = (
                                    f"{stats.duration_seconds:.1f}s"
                                    if stats.duration_seconds is not None
                                    else "N/A"
                                )
                                logger.info(
                                    f"Imported {stats.psm_count:,} PSMs from {mzid_file.name} "
                                    f"({duration_str})"
                                )
                                processed_files += 1
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
                        if processed_files == len(interesting_files[".mzid"]):
                            fully_processed = True
                            continue

                    # Process mzTab file
                    # TODO: uncomment, if mztab parser is implemented!
                    # if not main_source_type and interesting_files[".mzTab"]:
                    # main_source_type = ".mztab"
                    # for mztab_file in interesting_files[".mzTab"]:
                    # logger.debug(f"Found mzTab: {mztab_file}, but cannot yet parse it")
                    # try:
                    #     parse(mztab_file)
                    #     processed_files += 1
                    # except Exception as e:
                    #     pass
                    # if processed_files == len(interesting_files[".mztab"]):
                    #     fully_processed = True
                    #     continue

                if files["search"] and not fully_processed:
                    interesting_files = await get_interesting_files(
                        files["search"], project["accession"], tmp_dir
                    )

                    # Process maxQuant txt output
                    if not main_source_type and interesting_files[".txt"]:
                        main_source_type = ".txt"
                        processed_files, errors, fully_processed = import_all_txt_zip(
                            db_engine,
                            interesting_files[".txt"],
                            project["accession"],
                            errors,
                        )
                        if fully_processed:
                            continue

                if files["other"] and not fully_processed:
                    interesting_files = await get_interesting_files(
                        files["other"], project["accession"], tmp_dir
                    )

                    # Process maxQuant txt output
                    if not main_source_type and interesting_files[".txt"]:
                        main_source_type = ".txt"
                        processed_files, errors, fully_processed = import_all_txt_zip(
                            db_engine,
                            interesting_files[".txt"],
                            project["accession"],
                            errors,
                        )
                        if fully_processed:
                            continue

                if not (files["result"] or files["search"] or files["other"]):
                    logger.warning(
                        f"No files found for project '{project['accession']}'"
                        f"from backend {backend_enum.name}.",
                    )
                elif not main_source_type:
                    logger.warning(
                        f"No file could be parsed for project '{project['accession']}'"
                        f"from backend {backend_enum.name}.",
                    )
                elif not fully_processed:
                    logger.warning(
                        f"'{main_source_type}' files could not be completely parsed for project "
                        f"'{project['accession']}' from backend {backend_enum.name}.",
                    )

                    # TODO: set "complete=fully_processed" flag for project

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
