import asyncio
import datetime
import logging
import multiprocessing
import os
import traceback
import warnings
from pathlib import Path
from typing import Annotated, Any, Literal

import typer
from pydantic import BaseModel
from sqlalchemy import Engine, inspect
from sqlalchemy import exc as sa_exc
from sqlmodel import Session, col, delete, select

from usigrabber.backends import BackendEnum
from usigrabber.backends.base import Files
from usigrabber.cli import app
from usigrabber.db import create_db_and_tables, load_db_engine
from usigrabber.db.cli import reset as db_reset
from usigrabber.db.schema import DownloadedFile, Project
from usigrabber.file_parser import import_files
from usigrabber.utils import get_cache_dir
from usigrabber.utils.context import context_project_accession
from usigrabber.utils.file import get_interesting_files, temporary_path
from usigrabber.utils.setup import setup_logger

FileCategory = Literal["result", "search", "other"]
FILE_CATEGORIES: list[FileCategory] = ["result", "search", "other"]

CACHE_DIR = get_cache_dir()
STANDARD_BACKENDS = [enum for enum in BackendEnum]

logger = logging.getLogger(__name__)


class OntologyConfiguration(BaseModel):
    skip_ontos: bool
    ontology_workers: int


class BuildConfiguration(BaseModel):
    cache_dir: Path
    ontologies: OntologyConfiguration
    max_workers: int


@app.command()
def build(
    projects_file: Annotated[
        Path | None,
        typer.Option(
            help="Path to a JSON file with sampled projects for testing. If not provided, projects are fetched from the PRIDE API.",
            envvar="PROJECTS_FILE",
            exists=True,
            dir_okay=False,
            file_okay=True,
            readable=True,
            resolve_path=True,
        ),
    ] = None,
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
    ontology_workers: Annotated[
        int,
        typer.Option(
            help="Number of workers for ontology processing in multiprocessing mode.",
            envvar="ONTOLOGY_WORKERS",
            min=1,
        ),
    ] = 1,
    cache_dir: Annotated[
        Path,
        typer.Option(
            help="Path to the cache dir.",
            envvar="CACHE_DIR",
            dir_okay=True,
            file_okay=False,
            resolve_path=True,
        ),
    ] = CACHE_DIR,
    max_workers: int | None = 3,
):
    cache_dir.mkdir(parents=True, exist_ok=True)
    setup_logger(is_main_process=True)

    if os.name == "nt":
        raise RuntimeError(
            "Windows is not supported. This application requires the 'sed' command-line utility, "
            "which is available by default on most Linux and macOS systems."
        )
    if reset:
        logger.info("Resetting database before build.")
        db_reset(force=True)

    os.environ["CACHE_DIR"] = str(cache_dir)

    if no_ontology:
        os.environ["NO_ONTOLOGY"] = "1"
    if os.getenv("NO_ONTOLOGY"):
        logger.warning("Ontology lookup is disabled.")
    if projects_file:
        os.environ["PROJECTS_FILE"] = str(projects_file)
    if os.getenv("PROJECTS_FILE"):
        logger.info(f"Using projects file: {os.getenv('PROJECTS_FILE')}")

    config = BuildConfiguration(
        cache_dir=cache_dir,
        ontologies=OntologyConfiguration(
            skip_ontos=no_ontology,
            ontology_workers=ontology_workers,
        ),
        max_workers=max_workers or multiprocessing.cpu_count(),
    )

    # set up database connection
    db_engine = load_db_engine()
    inspector = inspect(db_engine)
    if len(inspector.get_table_names()) == 0:
        logger.info("No preexisting database found. Database will be initialized.")
        create_db_and_tables(db_engine)

    asyncio.run(build_all_projects(backends, config, db_engine))


async def build_all_projects(
    backends: list[BackendEnum],
    config: BuildConfiguration,
    engine: Engine,
):
    from usigrabber.parallelism.main import (
        build_all_projects_in_process_pool,
        build_all_projects_in_single_process,
    )

    if config.max_workers == 1:
        await build_all_projects_in_single_process(backends, engine=engine)
    else:
        await build_all_projects_in_process_pool(backends, config)


async def build_project(
    backend_enum: BackendEnum, project: dict[str, Any], db_engine: None | Engine = None
) -> None:
    """Build USI database."""
    from usigrabber.parallelism.main import db_engine as worker_db_engine

    backend = backend_enum.value
    project_accession = backend.get_project_accession(project)
    token = context_project_accession.set(project_accession)

    logger.info(
        f"Building {project_accession}",
        extra={
            "event": "project_started",
            "project_accession": project_accession,
            "backend": backend_enum.name,
        },
    )

    # Use worker db_engine if available (multiprocessing), otherwise load a new one
    if db_engine:
        engine = db_engine
    elif worker_db_engine is not None:
        engine = worker_db_engine
    else:
        raise ValueError(
            "DB engine must be passed in single process mode or loaded per individual worker with the ProcessPool"
        )

    with Session(engine) as session:
        project_exists = (
            session.exec(select(Project).where(Project.accession == project_accession)).first()
            is not None
        )
        if not project_exists:
            # create project entry in db
            await backend.dump_project_to_db(session, project)
            session.commit()

    error: None | str = None
    traceback_str: None | str = None

    try:
        # download files
        files = await get_filtered_files(project_accession, backend_enum, engine)
        with temporary_path() as tmp_dir:
            main_source_type = None
            for category in FILE_CATEGORIES:
                if not files[category] or main_source_type is not None:
                    continue

                interesting_ftp_paths, file_ext = await get_interesting_files(
                    files[category], project["accession"]
                )

                if not interesting_ftp_paths:
                    continue

                main_source_type = file_ext
                await import_files(
                    engine,
                    interesting_ftp_paths,
                    file_ext,
                    project["accession"],
                    tmp_dir,
                    files["raw"],
                )
    except Exception as e:
        error = str(e)
        traceback_str = traceback.format_exc()
        raise e
    finally:
        context_project_accession.reset(token)
        with Session(engine) as session:
            db_project = session.get(Project, project_accession)
            assert db_project, (
                f"There must exist a project with accession {project_accession} in the db"
            )

            db_project.error_message = error
            db_project.traceback = traceback_str
            db_project.end_time = datetime.datetime.now()
            session.commit()


def build_project_sync(backend_enum: BackendEnum, project: dict[str, Any]) -> None:
    with warnings.catch_warnings(action="ignore", category=sa_exc.SAWarning):
        asyncio.run(build_project(backend_enum, project))


async def get_filtered_files(
    project_accession: str,
    backend_enum: BackendEnum,
    engine: Engine,
) -> Files:
    """
    Returns a filtered list of files to be downloaded for a given project,
    based on the files already present in the database and their status (successful/unsuccessful).
    """

    backend = backend_enum.value
    files = await backend.get_files_for_project(project_accession)
    new_files: Files = {"result": [], "search": [], "other": [], "raw": []}
    with Session(engine) as session:
        delete_files = []
        # get all existing files for this project
        statement = select(DownloadedFile).where(
            DownloadedFile.project_accession == project_accession,
        )
        db_files = session.exec(statement).all()

        if len(db_files) == 0:
            logger.info(
                f"No existing files found for project {project_accession}. "
                "All files will be downloaded."
            )
            new_files = files
        else:
            # dict mapping filename -> checksum for all existing files
            # enables faster access by avoiding O(n²) loops when checking which files need to be downloaded
            db_file_lookup: dict[str, tuple[Any, ...]] = {
                db_file.file_name: (
                    db_file.is_successful,
                    db_file.checksum,
                    db_file.error_message,
                    db_file.id,
                )
                for db_file in db_files
            }

            for category in FILE_CATEGORIES:
                for file in files[category]:
                    filename = Path(file["filepath"]).name

                    # decide if file needs to be downloaded or can be skipped
                    db_file = db_file_lookup.get(filename)
                    if db_file is not None:
                        # check if unsuccessful
                        if not db_file[0]:
                            if not db_file[2]:
                                logger.warning(
                                    "No error message for unsuccessful file '%s' in project %s. "
                                    "It will be skipped.",
                                    filename,
                                    project_accession,
                                )
                                continue

                            # check if it is a retryable error
                            if db_file[2].startswith(
                                (
                                    "[Errno 111]",  # Connect call failed
                                    "[Errno 28]",  # No space left on device
                                    "Download timed out",
                                    "Waiting for ('2xx',) but got 426 [' Failure writing network stream.']",
                                    "Task was cancelled",
                                )
                            ):
                                logger.info(
                                    f"Retrying download of file '{filename}' in project {project_accession} due to previous error: {db_file[2]}"
                                )
                                new_files[category].append(file)
                                # mark for deletion to allow re-download
                                delete_files.append(db_file[3])
                        else:
                            # file is marked as successful in the db
                            if db_file[1] == file["checksum"]:
                                # file is already in the db and checksum matches, skip it
                                continue

                            logger.warning(
                                f"Checksum verification for '{filename}' failed. "
                                "It will still be skipped for now.",
                                extra={
                                    "event": "checksum_mismatch",
                                    "project_accession": project_accession,
                                    "backend": backend_enum.name,
                                },
                            )
                            continue
                    else:
                        # file is not in the db -> download it
                        new_files[category].append(file)

            # delete files with retryable errors to allow re-download
            session.exec(delete(DownloadedFile).where(col(DownloadedFile.id).in_(delete_files)))
            session.commit()

    # copy raw files to new_files
    new_files["raw"] = files["raw"]
    return new_files
