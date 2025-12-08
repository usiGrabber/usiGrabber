import logging
import multiprocessing
import os
from collections import deque
from collections.abc import AsyncGenerator
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, as_completed, wait
from typing import TYPE_CHECKING, Any

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from usigrabber.backends import BackendEnum
from usigrabber.db.engine import load_db_engine
from usigrabber.utils.setup import system_setup

db_engine = None  # This is loaded once per worker
ontology_helper = None  # This is loaded once per ontology worker

if TYPE_CHECKING:
    from usigrabber.cli.build import BuildConfiguration

logger = logging.getLogger(__name__)


def init_worker():
    global db_engine
    system_setup(is_main_process=False)
    # Dispose of any existing engine from parent process
    if db_engine is not None:
        db_engine.dispose()
    # Create fresh engine for this worker process
    db_engine = load_db_engine()
    logger.info(f"Worker {os.getpid()} initialized with engine: {db_engine}")


def init_ontology_worker():
    """Initialize worker with both DB engine and ontology helper."""
    global db_engine, ontology_helper
    from ontology_resolver.ontology_helper import OntologyHelper

    logger.info(f"Ontology worker {os.getpid()} is starting")
    system_setup(is_main_process=False)
    # Dispose of any existing engine from parent process
    if db_engine is not None:
        db_engine.dispose()
    # Create fresh engine for this worker process
    db_engine = load_db_engine()
    # Load ontology helper (this will take time on first load)
    ontology_helper = OntologyHelper()
    logger.info(f"Ontology worker {os.getpid()} initialized with engine and ontology helper")


async def iterate_projects(
    backends: list[BackendEnum],
) -> AsyncGenerator[tuple[dict[str, Any], BackendEnum], None]:
    backend_queue = deque(backends)
    while len(backend_queue) > 0:
        current_backend = backend_queue.popleft()
        async for project in current_backend.value.get_new_projects(existing_accessions=set()):
            yield project, current_backend


async def build_all_projects_in_single_process(
    backends: list[BackendEnum], config: "BuildConfiguration"
) -> None:
    from usigrabber.cli.build import build_project

    async for project, backend in iterate_projects(backends):
        await build_project(backend, project)
    logger.info("Done")


def resolve_ontologies_for_project_sync(backend_enum: BackendEnum, project: dict[str, Any]) -> None:
    """Resolve and add ontologies for a single project (runs in ontology worker)."""
    import asyncio
    import warnings

    from sqlalchemy import exc as sa_exc
    from sqlmodel import Session

    project_accession = backend_enum.value.get_project_accession(project)
    logger.info(f"Resolving ontologies for {project_accession}")

    # Use worker db_engine
    from usigrabber.parallelism.main import db_engine as worker_db_engine

    engine = worker_db_engine if worker_db_engine is not None else load_db_engine()

    with (
        warnings.catch_warnings(action="ignore", category=sa_exc.SAWarning),
        Session(engine) as session,
    ):
        # Call the backend's ontology parsing method
        asyncio.run(
            backend_enum.value._parse_and_add_cv_params(project_accession, session, project)
        )
        session.commit()

    logger.info(f"Completed ontology resolution for {project_accession}")


async def build_all_projects_in_process_pool(
    backends: list[BackendEnum], config: "BuildConfiguration"
) -> None:
    from usigrabber.cli.build import build_project_sync

    # Don't make this global! This would break and python processes
    multiprocessing.set_start_method("spawn")

    # Set environment variable to skip ontologies in main build phase
    os.environ["SKIP_ONTOLOGY_IN_MAIN_BUILD"] = "1"

    # Track project states for ontology processing
    completed_queue: deque[tuple[str, BackendEnum]] = (
        deque()
    )  # Queue of (accession, backend) for ontology processing
    failed_projects: set[str] = set()
    main_future_to_data: dict[
        Future, tuple[str, BackendEnum]
    ] = {}  # Future -> (accession, backend)
    onto_future_to_accession: dict[Future, str] = {}

    main_futures: set[Future] = set()
    onto_futures: set[Future] = set()
    max_workers = config.max_workers or multiprocessing.cpu_count()
    ontology_workers = config.ontologies.ontology_workers

    total_successful = 0
    total_errors = 0
    ontology_successful = 0
    ontology_errors = 0

    with logging_redirect_tqdm():
        # Progress bars
        pbar = tqdm(desc="Building projects", unit=" projects", position=0)
        onto_pbar = tqdm(desc="Resolving ontologies", unit=" projects", position=1)

        with (
            ProcessPoolExecutor(max_workers=max_workers, initializer=init_worker) as main_executor,
            ProcessPoolExecutor(
                max_workers=ontology_workers, initializer=init_ontology_worker
            ) as onto_executor,
        ):
            async for project, current_backend in iterate_projects(backends):
                accession = current_backend.value.get_project_accession(project)

                # Submit to main build queue
                if len(main_futures) < max_workers:
                    fut: Future[None] = main_executor.submit(
                        build_project_sync, current_backend, project
                    )
                    main_futures.add(fut)
                    main_future_to_data[fut] = (accession, current_backend)
                else:
                    # Wait for at least one main future to complete
                    done, main_futures = wait(main_futures, return_when=FIRST_COMPLETED)
                    for finished in done:
                        acc, backend = main_future_to_data.pop(finished)
                        try:
                            finished.result()
                            # Push to ontology queue on success
                            if not config.ontologies.skip_ontos:
                                completed_queue.append((acc, backend))
                            total_successful += 1
                        except Exception as e:
                            failed_projects.add(acc)
                            logger.error(f"Error occurred in {acc}: {e}")
                            total_errors += 1
                        finally:
                            pbar.update(1)

                    # Now submit the current project
                    fut = main_executor.submit(build_project_sync, current_backend, project)
                    main_futures.add(fut)
                    main_future_to_data[fut] = (accession, current_backend)

                # Process ontology queue if we have capacity
                if not config.ontologies.skip_ontos:
                    while len(completed_queue) > 0 and len(onto_futures) < ontology_workers:
                        acc, backend = completed_queue.popleft()
                        # Retrieve project details again
                        try:
                            project_data = await backend.value.get_project(acc)
                            fut_onto: Future[None] = onto_executor.submit(
                                resolve_ontologies_for_project_sync, backend, project_data
                            )
                            onto_futures.add(fut_onto)
                            onto_future_to_accession[fut_onto] = acc
                        except Exception as e:
                            logger.error(
                                f"Failed to retrieve project {acc} for ontology processing: {e}"
                            )
                            ontology_errors += 1

                # Check for completed ontology futures
                if len(onto_futures) > 0:
                    done_onto, onto_futures = wait(
                        onto_futures, timeout=0, return_when=FIRST_COMPLETED
                    )
                    for finished in done_onto:
                        acc = onto_future_to_accession.pop(finished)
                        try:
                            finished.result()
                            ontology_successful += 1
                        except Exception as e:
                            logger.error(f"Error resolving ontologies for {acc}: {e}")
                            ontology_errors += 1
                        finally:
                            onto_pbar.update(1)

            # Process remaining main futures
            logger.info(
                f"All projects submitted to workers: Waiting for the remaining {len(main_futures)} projects to complete"
            )
            for finished in as_completed(main_futures):
                acc, backend = main_future_to_data.pop(finished)
                try:
                    finished.result()
                    # Push to ontology queue on success
                    if not config.ontologies.skip_ontos:
                        completed_queue.append((acc, backend))
                    total_successful += 1
                except Exception as e:
                    failed_projects.add(acc)
                    logger.error(f"Error occurred in {acc}: {e}")
                    total_errors += 1
                finally:
                    pbar.update(1)

            # Process remaining items in ontology queue
            if not config.ontologies.skip_ontos:
                logger.info(
                    f"Main loop complete. Processing remaining {len(completed_queue)} projects for ontology resolution"
                )
                while len(completed_queue) > 0:
                    # Submit up to ontology_workers at a time
                    while len(completed_queue) > 0 and len(onto_futures) < ontology_workers:
                        acc, backend = completed_queue.popleft()
                        try:
                            project_data = await backend.value.get_project(acc)
                            fut_onto = onto_executor.submit(
                                resolve_ontologies_for_project_sync, backend, project_data
                            )
                            onto_futures.add(fut_onto)
                            onto_future_to_accession[fut_onto] = acc
                        except Exception as e:
                            logger.error(
                                f"Failed to retrieve project {acc} for ontology processing: {e}"
                            )
                            ontology_errors += 1

                    # Wait for at least one to complete
                    if len(onto_futures) > 0:
                        done_onto, onto_futures = wait(onto_futures, return_when=FIRST_COMPLETED)
                        for finished in done_onto:
                            acc = onto_future_to_accession.pop(finished)
                            try:
                                finished.result()
                                ontology_successful += 1
                            except Exception as e:
                                logger.error(f"Error resolving ontologies for {acc}: {e}")
                                ontology_errors += 1
                            finally:
                                onto_pbar.update(1)

                # Wait for final ontology futures
                logger.info(f"Waiting for final {len(onto_futures)} ontology tasks to complete")
                for finished in as_completed(onto_futures):
                    acc = onto_future_to_accession.pop(finished)
                    try:
                        finished.result()
                        ontology_successful += 1
                    except Exception as e:
                        logger.error(f"Error resolving ontologies for {acc}: {e}")
                        ontology_errors += 1
                    finally:
                        onto_pbar.update(1)

        pbar.close()
        onto_pbar.close()

    logger.info(
        f"Build complete: {total_successful} successful, {total_errors} failed. "
        f"Ontologies: {ontology_successful} successful, {ontology_errors} failed"
    )
