import asyncio
import logging
import multiprocessing
import os
import warnings
from pathlib import Path
from typing import Annotated, Any

import typer
from pydantic import BaseModel
from sqlalchemy import exc as sa_exc
from sqlalchemy import inspect
from sqlmodel import Session

from usigrabber.backends import BackendEnum
from usigrabber.cli import app
from usigrabber.db import create_db_and_tables, load_db_engine
from usigrabber.db.cli import reset as db_reset
from usigrabber.file_parser import import_files
from usigrabber.file_parser.errors import FileParserError
from usigrabber.utils import get_cache_dir
from usigrabber.utils.file import (
    get_interesting_files,
    temporary_path,
)

CACHE_DIR = get_cache_dir()
STANDARD_BACKENDS = [enum for enum in BackendEnum]
FILE_CATEGORIES = ["result", "search", "other"]

logger = logging.getLogger(__name__)


class ObservabilityConfiguration(BaseModel):
    debug: bool


class OntologyConfiguration(BaseModel):
    skip_ontos: bool


class BuildConfiguration(BaseModel):
    observability: ObservabilityConfiguration
    cache_dir: Path
    ontologies: OntologyConfiguration
    max_workers: int


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
    max_workers: int | None = 1,
):
    if reset:
        logger.info("Resetting database before build.")
        db_reset(force=True)

    os.environ["CACHE_DIR"] = str(cache_dir)

    if no_ontology:
        os.environ["NO_ONTOLOGY"] = "1"
    if os.getenv("NO_ONTOLOGY"):
        logger.warning("Ontology lookup is disabled.")
    if debug:
        os.environ["DEBUG"] = "1"
    if os.getenv("DEBUG"):
        logger.info("Running in DEBUG mode.")

    config = BuildConfiguration(
        observability=ObservabilityConfiguration(debug=debug),
        cache_dir=cache_dir,
        ontologies=OntologyConfiguration(skip_ontos=no_ontology),
        max_workers=max_workers or multiprocessing.cpu_count(),
    )

    db_engine = load_db_engine()
    inspector = inspect(db_engine)
    if len(inspector.get_table_names()) == 0:
        logger.info("No preexisting database found. Database will be initialized.")
        create_db_and_tables(db_engine)

    asyncio.run(build_all_projects(backends, config))


async def build_all_projects(backends: list[BackendEnum], config: BuildConfiguration):
    from usigrabber.parallelism.main import (
        build_all_projects_in_process_pool,
        build_all_projects_in_single_process,
    )

    if config.max_workers == 1:
        await build_all_projects_in_single_process(backends, config)
    else:
        await build_all_projects_in_process_pool(backends, config)


async def build_project(
    backend_enum: BackendEnum,
    project: dict[str, Any],
) -> None:
    """Build USI database."""
    from usigrabber.parallelism.main import db_engine as worker_db_engine

    backend = backend_enum.value
    project_accession = backend.get_project_accession(project)
    logger.info(
        f"Building {project_accession}",
        extra={
            "event": "project_started",
            "project_accession": project_accession,
            "backend": backend_enum.name,
        },
    )

    # Use worker db_engine if available (multiprocessing), otherwise load a new one
    engine = worker_db_engine if worker_db_engine is not None else load_db_engine()

    with Session(engine) as session:
        await backend.dump_project_to_db(session, project)
        session.commit()

    # download files
    files = backend.get_files_for_project(project["accession"])

    with temporary_path() as tmp_dir:
        for category in FILE_CATEGORIES:
            if files.get(category):
                interesting_files = await get_interesting_files(
                    files[category], project["accession"], tmp_dir
                )

                for ext, flist in interesting_files.items():
                    if flist:
                        try:
                            import_files(
                                engine,
                                flist,
                                project["accession"],
                                logger,
                            )
                        except FileParserError as e:
                            logger.error(
                                f"Failed to import '{ext}' files: {e}",
                                exc_info=True,
                                stack_info=True,
                                extra={
                                    "ext": str(ext),
                                    "project_accession": project["accession"],
                                },
                            )


def build_project_sync(backend_enum: BackendEnum, project: dict[str, Any]) -> None:
    with warnings.catch_warnings(action="ignore", category=sa_exc.SAWarning):
        asyncio.run(build_project(backend_enum, project))
