import asyncio
import logging
import multiprocessing
import os
import time
import warnings
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer
from pydantic import BaseModel
from pyinstrument import Profiler
from sqlalchemy import exc as sa_exc
from sqlalchemy import inspect

from usigrabber.backends import BackendEnum
from usigrabber.cli import app
from usigrabber.cli.processing import process_project
from usigrabber.db import create_db_and_tables, load_db_engine
from usigrabber.db.cli import reset as db_reset
from usigrabber.utils import get_cache_dir
from usigrabber.utils.setup import system_setup

CACHE_DIR = get_cache_dir()
STANDARD_BACKENDS = [enum for enum in BackendEnum]
logger = logging.getLogger(__name__)


db_engine = None  # This is loaded once per worker


def init_worker():
    global db_engine
    system_setup(is_main_process=False)
    # Dispose of any existing engine from parent process
    if db_engine is not None:
        db_engine.dispose()
    # Create fresh engine for this worker process
    db_engine = load_db_engine()
    logger.info(f"Worker {os.getpid()} initialized with engine: {db_engine}")


class ObservabilityConfiguration(BaseModel):
    debug: bool


class OntologyConfiguration(BaseModel):
    skip_ontos: bool


class BuildConfiguration(BaseModel):
    observability: ObservabilityConfiguration
    cache_dir: Path
    ontologies: OntologyConfiguration


class SubBuildStatus(StrEnum):
    FAILED = "Failed"
    SUCCESSFUL = "Successful"
    BACKEND_DONE = "Backend fully processed"


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
    max_workers: int | None = None,
    # max_workers: Annotated[int | None, typer.Option("Number of processes to use. None uses all CPU cores")] = None,
    projects_per_worker: int = 1,
):
    if reset:
        logger.info("Resetting database before build.")
        db_reset(force=True)

    config = BuildConfiguration(
        observability=ObservabilityConfiguration(debug=debug),
        cache_dir=cache_dir,
        ontologies=OntologyConfiguration(skip_ontos=no_ontology),
    )

    # Don't make this global! This would break and python processes
    multiprocessing.set_start_method("spawn")

    if max_workers == 1:
        global db_engine
        db_engine = load_db_engine()
        for backend in backends:
            project_offset = 0
            while True:
                status = build_backend_partially(
                    backend, project_offset, projects_per_worker, config
                )
                project_offset += projects_per_worker
                if status == SubBuildStatus.BACKEND_DONE:
                    logger.info(f"Backend {backend.name} done")
                    break
    else:
        backend_queue = deque(backends)
        futures = set()
        project_offset = 0
        current_backend: None | BackendEnum = backend_queue.popleft()
        max_workers = max_workers or multiprocessing.cpu_count()
        with ProcessPoolExecutor(max_workers=max_workers, initializer=init_worker) as executor:
            # --- Fill the worker slots initially ---
            while len(futures) < max_workers and current_backend:
                fut: Future[SubBuildStatus] = executor.submit(
                    build_backend_partially,
                    current_backend,
                    project_offset,
                    projects_per_worker,
                    config,
                )
                project_offset += projects_per_worker
                futures.add(fut)

            # --- Main loop: process results as they come in ---
            FUTURE_TIMEOUT = 30
            while futures:
                done, futures = wait(futures, return_when=FIRST_COMPLETED, timeout=FUTURE_TIMEOUT)
                if not done:
                    logger.info(f"No futures completed in {FUTURE_TIMEOUT}s")
                    continue
                for finished in done:
                    try:
                        status = finished.result()
                        logger.info(f"Project finished: {status}")
                        # success_pbar.update()
                    except Exception as e:
                        logger.error(f"Error occured: {e}")
                        status = SubBuildStatus.FAILED
                        # failures_pbar.update()

                    if status == SubBuildStatus.BACKEND_DONE:
                        if len(backend_queue) == 0:
                            current_backend = None
                            project_offset = 0
                        else:
                            current_backend = backend_queue.popleft()
                            project_offset = 0
                            logger.info(f"Working on new backend: {current_backend.name}")

                    if current_backend:
                        fut = executor.submit(
                            build_backend_partially,
                            current_backend,
                            project_offset,
                            projects_per_worker,
                            config,
                        )
                        project_offset += projects_per_worker
                        logger.info(
                            f"Submitting task with {projects_per_worker} projects at offset {project_offset} for backend {current_backend.name}"
                        )
                        futures.add(fut)

                # loop restarts to wait for next finished future

            logger.info("All work completed.")


def build_backend_partially(
    backend: BackendEnum, project_start: int, project_limit: int | None, config: BuildConfiguration
) -> SubBuildStatus:
    pid = os.getpid()
    logger.info(f"⏰ {time.time():.2f} - Worker {pid} START: {backend.name} offset={project_start}")
    with warnings.catch_warnings(action="ignore", category=sa_exc.SAWarning), Profiler() as p:
        result = asyncio.run(async_build(backend, project_start, project_limit, config))
    p.write_html(f"logs/profiles/{project_start}-{project_limit}.html")
    logger.info(
        f"⏰ {time.time():.2f} - Worker {pid} DONE: {backend.name} offset={project_start} status={result}"
    )
    return result


async def async_build(
    backend_enum: BackendEnum,
    project_start: int,
    project_limit: int | None,
    config: BuildConfiguration,
) -> SubBuildStatus:
    """Build USI database."""
    global db_engine
    logger.info("Building database.")

    os.environ["CACHE_DIR"] = str(config.cache_dir)

    if config.ontologies.skip_ontos:
        os.environ["NO_ONTOLOGY"] = "1"

    if os.getenv("NO_ONTOLOGY"):
        logger.warning("Ontology lookup is disabled.")

    if config.observability.debug:
        os.environ["DEBUG"] = "1"

    if os.getenv("DEBUG"):
        logger.info("Running in DEBUG mode.")

    # set up database connection
    assert db_engine is not None, "DB ENGINE NEEDS TO BE INITIALIZED IN WORKER"

    # Verify engine is actually an Engine object
    from sqlalchemy.engine.base import Engine as SQLAlchemyEngine

    if not isinstance(db_engine, SQLAlchemyEngine):
        error_msg = f"db_engine is not an Engine! Type: {type(db_engine)}, Value: {db_engine}"
        logger.error(error_msg)
        raise TypeError(error_msg)

    inspector = inspect(db_engine)

    if len(inspector.get_table_names()) == 0:
        logger.info("No preexisting database found. Database will be initialized.")
        create_db_and_tables(db_engine)

    logger.info(f"Fetching {project_start}, limit: {project_limit} on {backend_enum.name}")

    backend = backend_enum.value
    imported = errors = 0
    processed_projects = 0
    error_projects = []

    logger.info(
        f"DB pool status at session start - size: {db_engine.pool.size()}, "
        f"checked_in: {db_engine.pool.checkedin()}, "
        f"checked_out: {db_engine.pool.checkedout()}, "
        f"overflow: {db_engine.pool.overflow()}",
        extra={
            "event": "db_pool_status_start",
            "pool_size": db_engine.pool.size(),
            "checked_in": db_engine.pool.checkedin(),
            "checked_out": db_engine.pool.checkedout(),
            "overflow": db_engine.pool.overflow(),
        },
    )

    async for project in backend.get_projects(project_start, project_limit):
        project_accession = project.get("accession", "unknown")
        try:
            # Use shared processing logic
            await process_project(db_engine, project, backend_enum)
            imported += 1
        except Exception as e:
            errors += 1
            error_projects.append((project_accession, str(e)))
            logger.error(
                f"Project {project_accession} failed with error",
                exc_info=True,
                extra={
                    "event": "project_failed",
                    "project_accession": project_accession,
                    "error_type": type(e).__name__,
                    "backend": backend_enum.name,
                },
            )
            # Don't re-raise - continue with next project
        finally:
            processed_projects += 1

    logger.info(
        f"DB pool status at session end - size: {db_engine.pool.size()}, "
        f"checked_in: {db_engine.pool.checkedin()}, "
        f"checked_out: {db_engine.pool.checkedout()}, "
        f"overflow: {db_engine.pool.overflow()}",
        extra={
            "event": "db_pool_status_end",
            "pool_size": db_engine.pool.size(),
            "checked_in": db_engine.pool.checkedin(),
            "checked_out": db_engine.pool.checkedout(),
            "overflow": db_engine.pool.overflow(),
        },
    )

    if processed_projects == 0:
        logger.info(f"No projects were processed. Returning status: {SubBuildStatus.BACKEND_DONE}")
        return SubBuildStatus.BACKEND_DONE
    return SubBuildStatus.SUCCESSFUL
