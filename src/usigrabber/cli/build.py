import asyncio
import logging
import multiprocessing
import os
import warnings
from pathlib import Path
from typing import Annotated, Any

import typer
from pydantic import BaseModel
from sqlalchemy import Engine, inspect
from sqlalchemy import exc as sa_exc
from sqlmodel import Session, select

from usigrabber.backends import BackendEnum
from usigrabber.cli import app
from usigrabber.db import create_db_and_tables, load_db_engine
from usigrabber.db.cli import reset as db_reset
from usigrabber.db.schema import Project
from usigrabber.file_parser import import_files
from usigrabber.utils import get_cache_dir
from usigrabber.utils.context import context_project_accession
from usigrabber.utils.file import get_interesting_files, temporary_path
from usigrabber.utils.setup import setup_logger

FILE_CATEGORIES = ["result", "search", "other"]

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
            exists=True,
            dir_okay=True,
            file_okay=False,
            writable=True,
            readable=True,
            resolve_path=True,
        ),
    ] = CACHE_DIR,
    max_workers: int | None = 1,
):
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

    with Session(db_engine) as session:
        statement = select(Project.accession)
        existing_accessions: set[str] = set(session.exec(statement).all())
        logger.info(f"Found {len(existing_accessions)} existing projects.")

    asyncio.run(build_all_projects(backends, config, existing_accessions, db_engine))


async def build_all_projects(
    backends: list[BackendEnum],
    config: BuildConfiguration,
    existing_accessions: set[str],
    engine: Engine,
):
    from usigrabber.parallelism.main import (
        build_all_projects_in_process_pool,
        build_all_projects_in_single_process,
    )

    if config.max_workers == 1:
        await build_all_projects_in_single_process(backends, existing_accessions, engine=engine)
    else:
        await build_all_projects_in_process_pool(backends, config, existing_accessions)


async def build_project(
    backend_enum: BackendEnum, project: dict[str, Any], db_engine: None | Engine = None
) -> None:
    """Build USI database."""
    from usigrabber.parallelism.main import db_engine as worker_db_engine

    backend = backend_enum.value
    project_accession = backend.get_project_accession(project)
    token = context_project_accession.set(project_accession)

    try:
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
            await backend.dump_project_to_db(session, project)
            session.commit()

        # download files
        files = backend.get_files_for_project(project["accession"])
        with temporary_path() as tmp_dir:
            is_fully_processed: bool = False
            main_source_type = None
            for category in FILE_CATEGORIES:
                if not files[category] or is_fully_processed or main_source_type is not None:
                    continue

                interesting_ftp_paths, file_ext = await get_interesting_files(
                    files[category], project["accession"]
                )

                if not interesting_ftp_paths:
                    continue

                main_source_type = file_ext
                is_fully_processed = await import_files(
                    engine,
                    interesting_ftp_paths,
                    file_ext,
                    project["accession"],
                    tmp_dir,
                )
    except Exception as e:
        raise e
    finally:
        context_project_accession.reset(token)


def build_project_sync(backend_enum: BackendEnum, project: dict[str, Any]) -> None:
    with warnings.catch_warnings(action="ignore", category=sa_exc.SAWarning):
        asyncio.run(build_project(backend_enum, project))
