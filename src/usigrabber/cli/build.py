import asyncio
import logging
import multiprocessing
import os
import threading
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
from usigrabber.utils.logging_helpers.aggregator.dashboard import (
    DashboardDisplay,
    create_default_dashboard,
)
from usigrabber.utils.logging_helpers.aggregator.running_aggregator import (
    RunningLogAggregator,
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


def run_dashboard_monitor(
    aggregator: RunningLogAggregator, display: DashboardDisplay, stop_event: threading.Event
):
    """
    Background thread that continuously updates and displays metrics.

    Args:
        aggregator: The log aggregator to read metrics from
        display: The dashboard display to render metrics
        stop_event: Event to signal when to stop monitoring
    """
    while not stop_event.is_set():
        try:
            # Update metrics from log files
            aggregator.update()

            # Get all metrics and display
            metrics = aggregator.get_all_metrics()
            display.display(metrics)

            # Wait before next update
            stop_event.wait(display.refresh_interval)
        except Exception as e:
            logger.error(f"Error in dashboard monitor: {e}", exc_info=True)
            time.sleep(display.refresh_interval)


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
    enable_dashboard: Annotated[
        bool,
        typer.Option(help="Enable live metrics dashboard."),
    ] = True,
    dashboard_refresh: Annotated[
        float,
        typer.Option(help="Dashboard refresh interval in seconds."),
    ] = 2.0,
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

    # Set up dashboard monitoring in main process
    dashboard_thread = None
    stop_dashboard = threading.Event()

    if enable_dashboard and max_workers > 1:
        # Initialize aggregator and dashboard
        log_dir = Path(os.getenv("LOGGING_DIR", "logs"))
        aggregator = RunningLogAggregator(log_dir=str(log_dir))

        # Create default pipelines and categories with renderers
        pipelines, categories = create_default_dashboard()

        # Register pipelines
        for pipeline in pipelines:
            aggregator.register_pipeline(pipeline)

        # Start dashboard in background thread
        display = DashboardDisplay(
            categories=categories, refresh_interval=dashboard_refresh, width=100
        )
        dashboard_thread = threading.Thread(
            target=run_dashboard_monitor,
            args=(aggregator, display, stop_dashboard),
            daemon=True,
        )
        dashboard_thread.start()
        logger.info("Live dashboard monitoring started")

    try:
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
            # manager = enlighten.get_manager()
            # success_pbar = manager.counter(total=None, desc="Fetching projects", color="green")
            # failures_pbar = success_pbar.add_subcounter("red")

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
                            logger.info(f"Project finished: {status}")
                            # success_pbar.update()
                        except Exception as e:
                            logger.exception(e)
                            status = SubBuildStatus.FAILED
                            # failures_pbar.update()

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
    finally:
        # Stop dashboard monitoring
        if dashboard_thread is not None:
            logger.info("Stopping dashboard monitoring...")
            stop_dashboard.set()
            dashboard_thread.join(timeout=5)


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
                project_accession = project.get("accession", "unknown")
                logger.info(
                    f"Processing project {project_accession}",
                    extra={
                        "event": "project_start",
                        "project_accession": project_accession,
                        "backend": backend_enum.name,
                    },
                )
                # TODO: support other submission types

                if project.get("submissionType") != "COMPLETE":
                    logger.info(
                        f"Skipping {project_accession} because it is not COMPLETE",
                        extra={
                            "event": "project_skipped",
                            "project_accession": project_accession,
                            "reason": "not_complete",
                            "submission_type": project.get("submissionType"),
                        },
                    )
                    continue

                try:
                    await backend.dump_project_to_db(session, project)
                    logger.debug(
                        f"Project {project_accession} metadata saved to DB",
                        extra={
                            "event": "project_metadata_saved",
                            "project_accession": project_accession,
                        },
                    )
                except Exception as e:
                    errors += 1
                    error_projects.append((project.get("accession"), str(e)))
                    logger.error(
                        f"Failed to save project {project_accession} metadata",
                        exc_info=True,
                        extra={
                            "event": "project_metadata_error",
                            "project_accession": project_accession,
                            "error_type": type(e).__name__,
                        },
                    )
                    session.rollback()
                    continue

                # download files
                files = await backend.get_files_for_project(project["accession"])

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
                                        f" ({duration_str})",
                                        extra={
                                            "event": "mzid_imported",
                                            "project_accession": project["accession"],
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
                                            "event": "mzid_import_error",
                                            "error_type": "MzidImportError",
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
                logger.info(
                    f"Project {project_accession} completed successfully",
                    extra={
                        "event": "project_completed",
                        "project_accession": project_accession,
                        "backend": backend_enum.name,
                    },
                )
            except Exception as e:
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
                raise e
            finally:
                processed_projects += 1

            # TODO: set "complete" flag for project
    if processed_projects == 0:
        logger.info(f"No projects were processed. Returning status: {SubBuildStatus.BACKEND_DONE}")
        return SubBuildStatus.BACKEND_DONE
    return SubBuildStatus.SUCCESSFUL
