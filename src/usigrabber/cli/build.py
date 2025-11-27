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

import enlighten
import typer
from pydantic import BaseModel
from sqlalchemy import exc as sa_exc
from sqlalchemy import inspect
from sqlmodel import Session

from usigrabber.backends import BackendEnum
from usigrabber.backends.base import FileMetadata
from usigrabber.cli import app
from usigrabber.db import create_db_and_tables, load_db_engine
from usigrabber.db.cli import reset as db_reset
from usigrabber.file_parser import MzidImportError, MzidParseError, import_mzid
from usigrabber.utils import get_cache_dir
from usigrabber.utils.file import (
    download_ftp_with_semamphore,
    extract_archive,
    temporary_path,
)
from usigrabber.utils.setup import system_setup

CACHE_DIR = get_cache_dir()
STANDARD_BACKENDS = [enum for enum in BackendEnum]
logger = logging.getLogger(__name__)

# empty string for folders (no extension)
FILETYPE_ALLOWLIST = {".mzid", ""}

PARALLEL_DOWNLOADS = int(os.getenv("PARALLEL_DOWNLOADS", "10"))


db_engine = None  # This is loaded once per worker


def init_worker():
    global db_engine
    system_setup(is_main_process=False)
    db_engine = load_db_engine()  # created ONCE per worker


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
    if max_workers is None:
        max_workers = multiprocessing.cpu_count()

    logger.info(f"Using {max_workers} workers. Main process: {os.getpid()}")

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
        manager = enlighten.get_manager()
        success_pbar = manager.counter(total=None, desc="Fetching projects", color="green")
        failures_pbar = success_pbar.add_subcounter("red")

        backend_queue = deque(backends)
        futures = set()
        project_offset = 0
        current_backend: None | BackendEnum = backend_queue.popleft()
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
            while futures:
                done, futures = wait(futures, return_when=FIRST_COMPLETED, timeout=30)
                for finished in done:
                    try:
                        status = finished.result()
                        success_pbar.update()
                    except Exception as e:
                        logger.exception(e)
                        status = SubBuildStatus.FAILED
                        failures_pbar.update()

                    if status == SubBuildStatus.BACKEND_DONE:
                        if len(backend_queue) == 0:
                            current_backend = None
                            project_offset = 0
                        else:
                            current_backend = backend_queue.popleft()

                    if current_backend:
                        fut = executor.submit(
                            build_backend_partially,
                            current_backend,
                            project_offset,
                            projects_per_worker,
                            config,
                        )
                        project_offset += projects_per_worker
                        futures.add(fut)

                # loop restarts to wait for next finished future

        logger.info("All work completed.")


def build_backend_partially(
    backend: BackendEnum, project_start: int, project_limit: int | None, config: BuildConfiguration
) -> SubBuildStatus:
    pid = os.getpid()
    logger.info(f"⏰ {time.time():.2f} - Worker {pid} START: {backend.name} offset={project_start}")
    with warnings.catch_warnings(action="ignore", category=sa_exc.SAWarning):
        result = asyncio.run(async_build(backend, project_start, project_limit, config))

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
    inspector = inspect(db_engine)

    if len(inspector.get_table_names()) == 0:
        logger.info("No preexisting database found. Database will be initialized.")
        create_db_and_tables(db_engine)

    logger.info(f"Fetching {project_start}, limit: {project_limit} on {backend_enum.name}")

    backend = backend_enum.value
    imported = errors = 0
    processed_projects = 0
    error_projects = []

    with Session(db_engine) as session:
        async for project in backend.get_projects(project_start, project_limit):
            try:
                logger.info(f"{project}")
                # TODO: support other submission types

                if project.get("submissionType") != "COMPLETE":
                    logger.info(f"Skipping {project['accession']} because it is not COMPLETE")
                    continue

                try:
                    await backend.dump_project_to_db(session, project)
                except Exception as e:
                    errors += 1
                    error_projects.append((project.get("accession"), str(e)))
                    session.rollback()
                    continue

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
                        paths = await asyncio.gather(
                            *[
                                download_ftp_with_semamphore(
                                    semaphore=sem,
                                    url=file["filepath"],
                                    out_dir=tmp_dir / project["accession"] / str(idx),
                                )
                                for idx, file in enumerate(files_to_be_downloaded)
                            ],
                            return_exceptions=True,
                        )

                        for idx, path in enumerate(paths):
                            filename = os.path.basename(files_to_be_downloaded[idx]["filepath"])
                            if isinstance(path, BaseException):
                                logger.error(
                                    "Failed to download file %s for project %s.",
                                    filename,
                                    project["accession"],
                                    exc_info=True,
                                )
                                continue

                            filesize_in_mb = files_to_be_downloaded[idx]["file_size"] / (
                                1024 * 1024
                            )
                            logger.debug(
                                f"Processing result file '{filename}' "
                                + f"({filesize_in_mb:,.2f} MB)"
                            )

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

                            # access files based on priority
                            for mzid_file in interesting_files[".mzid"]:
                                # Process mzID file
                                try:
                                    stats = import_mzid(session, mzid_file, project["accession"])
                                    duration_str = (
                                        f"{stats.duration_seconds:.1f}s"
                                        if stats.duration_seconds is not None
                                        else "N/A"
                                    )
                                    logger.info(
                                        f"Imported {stats.psm_count:,} PSMs from '{mzid_file.name}'"
                                        f" ({duration_str})"
                                    )
                                except MzidParseError as e:
                                    logger.warning(
                                        f"Skipping malformed mzID file '{mzid_file.name}': {e}",
                                        extra={
                                            "mzid_file": str(mzid_file),
                                            "project_accession": project["accession"],
                                        },
                                    )
                                    continue
                                except MzidImportError as e:
                                    logger.error(
                                        f"Failed to import mzID file '{mzid_file.name}': {e}",
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

                session.commit()
                imported += 1
            except Exception as e:
                logger.exception(e)
                session.rollback()
            finally:
                processed_projects += 1

            # TODO: set "complete" flag for project
    if processed_projects == 0:
        logger.info(f"No projects were processed. Returning status: {SubBuildStatus.BACKEND_DONE}")
        return SubBuildStatus.BACKEND_DONE
    return SubBuildStatus.SUCCESSFUL
