import logging
import multiprocessing
import os
from collections import deque
from collections.abc import AsyncGenerator, Callable
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import Engine
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from usigrabber.backends import BackendEnum
from usigrabber.db.engine import load_db_engine
from usigrabber.utils.setup import setup_logger

db_engine = None  # This is loaded once per worker
ontology_helper = None  # This is loaded once per ontology worker

if TYPE_CHECKING:
    from usigrabber.cli.build import BuildConfiguration

logger = logging.getLogger(__name__)

mp_context = multiprocessing.get_context("spawn")


def init_worker() -> None:
    global db_engine
    setup_logger(is_main_process=False)
    # Dispose of any existing engine from parent process
    if db_engine is not None:
        db_engine.dispose()
    # Create fresh engine for this worker process
    db_engine = load_db_engine()
    logger.info(f"Worker {os.getpid()} initialized with engine: {db_engine}")


def init_ontology_worker() -> None:
    """Initialize worker with both DB engine and ontology helper."""
    global db_engine, ontology_helper
    from ontology_resolver.ontology_helper import OntologyHelper

    logger.info(f"Ontology worker {os.getpid()} is starting")
    setup_logger(is_main_process=False)
    # Dispose of any existing engine from parent process
    if db_engine is not None:
        db_engine.dispose()
    # Create fresh engine for this worker process
    db_engine = load_db_engine()
    # Load ontology helper (this will take time on first load)
    ontology_helper = OntologyHelper()
    logger.info(f"Ontology worker {os.getpid()} initialized with engine and ontology helper")


async def iterate_projects(
    backends: list[BackendEnum], existing_accessions: set[str]
) -> AsyncGenerator[tuple[dict[str, Any], BackendEnum], None]:
    backend_queue = deque(backends)
    while len(backend_queue) > 0:
        current_backend = backend_queue.popleft()
        async for project in current_backend.value.get_new_projects(
            existing_accessions=existing_accessions
        ):
            yield project, current_backend


async def build_all_projects_in_single_process(
    backends: list[BackendEnum],
    config: "BuildConfiguration",
    existing_accessions: set[str],
    engine: Engine,
) -> None:
    from usigrabber.cli.build import build_project

    async for project, backend in iterate_projects(backends, existing_accessions):
        try:
            await build_project(backend, project, engine)
        except Exception as e:
            logger.error(
                f"Error building project {project['accession']}: {e}",
                exc_info=True,
                extra={"project_accession": project["accession"], "backend": backend.name},
            )
    logger.info("Done finishing all projects")


def resolve_ontologies_for_project_sync(backend_enum: BackendEnum, project_accession: str) -> None:
    """Resolve and add ontologies for a single project (runs in ontology worker)."""
    import asyncio
    import warnings

    from sqlalchemy import exc as sa_exc

    logger.info(f"Resolving ontologies for {project_accession}")

    # Use worker db_engine
    from usigrabber.parallelism.main import db_engine as worker_db_engine

    engine = worker_db_engine if worker_db_engine is not None else load_db_engine()

    with (
        warnings.catch_warnings(action="ignore", category=sa_exc.SAWarning),
    ):
        # Call the backend's ontology parsing method
        asyncio.run(
            backend_enum.value._parse_and_add_cv_params(
                project_accession, engine, backend_enum.value
            )
        )

    logger.info(f"Completed ontology resolution for {project_accession}")


@dataclass
class FutureContext:
    config: "BuildConfiguration"
    project_accession: str
    backend: BackendEnum
    increment_main_progress_bar: Callable[[], Any]
    increment_onto_progress_bar: Callable[[], Any]
    onto_executor: ProcessPoolExecutor
    future_holder: "FutureHolder"


class WorkFuture:
    def __init__(self, future: Future, context: FutureContext) -> None:
        self.future = future
        self._context = context

    def get_and_process_result(self):
        raise NotImplementedError()


class FutureHolder:
    """
    Holds a number of futures and useses a strategy pattern to execute future side effects and track succcess
    """

    def __init__(self) -> None:
        self._futures: dict[Future, WorkFuture] = {}

    def add(self, future_wrapper: WorkFuture) -> None:
        self._futures[future_wrapper.future] = future_wrapper

    def count_of(self, future_type: type[WorkFuture]) -> int:
        return sum(1 for future in self._futures.values() if isinstance(future, future_type))

    def process_first_completed(self):
        """
        Processes the first completed future.
        """
        futures = list(self._futures.keys())
        done_futures, waiting_futures = wait(futures, return_when=FIRST_COMPLETED)

        for future in done_futures:
            future_wrapper = self._futures[future]
            future_wrapper.get_and_process_result()
            self._futures.pop(future)

    def process_all_completed(self):
        """
        Waits until all futures are completed and processes their results.

        We don't use the native ALL_COMPLETED because a processed future might cause a new future to be added to the holder.
        """
        while len(self._futures) > 0:
            self.process_first_completed()


class OntologyWorkFuture(WorkFuture):
    def get_and_process_result(self):
        try:
            self.future.result()
        except Exception as e:
            logger.error(
                f"Error for ontologies occurred in {self._context.project_accession} for backend {self._context.backend.name}: {e}",
                exc_info=True,
                extra={"project_accession": self._context.project_accession},
            )
        finally:
            self._context.increment_onto_progress_bar()


class MainWorkFuture(WorkFuture):
    def get_and_process_result(self):
        try:
            self.future.result()
        except Exception as e:
            logger.error(
                f"Error occurred in {self._context.project_accession} for backend {self._context.backend.name}: {e}",
                exc_info=True,
                extra={"project_accession": self._context.project_accession},
            )
        finally:
            self._context.increment_main_progress_bar()
            if not self._context.config.ontologies.skip_ontos:
                onto_future = self._context.onto_executor.submit(
                    resolve_ontologies_for_project_sync,
                    self._context.backend,
                    self._context.project_accession,
                )
                onto_future_wrapper = OntologyWorkFuture(onto_future, self._context)
                self._context.future_holder.add(onto_future_wrapper)


async def build_all_projects_in_process_pool(
    backends: list[BackendEnum], config: "BuildConfiguration", existing_accessions: set[str]
) -> None:
    from usigrabber.cli.build import build_project_sync

    # Set environment variable to skip ontologies in main build phase
    os.environ["IS_IN_MULTIPROCESSING_MODE"] = "1"

    max_workers = config.max_workers
    ontology_workers = config.ontologies.ontology_workers

    logger.info(f"Using {max_workers} workers for main build phase")
    logger.info(f"Using {ontology_workers} workers for ontology resolution\n")

    with logging_redirect_tqdm():
        # Progress bars
        pbar = tqdm(desc="Building projects", unit=" projects", position=0)
        onto_pbar = tqdm(desc="Resolving ontologies", unit=" projects", position=1)
        future_holder: FutureHolder = FutureHolder()

        with (
            ProcessPoolExecutor(
                max_workers=max_workers, initializer=init_worker, mp_context=mp_context
            ) as main_executor,
            ProcessPoolExecutor(
                max_workers=ontology_workers,
                initializer=init_ontology_worker,
                mp_context=mp_context,
            ) as onto_executor,
        ):
            async for project, current_backend in iterate_projects(backends, existing_accessions):
                future_context = FutureContext(
                    config=config,
                    project_accession=current_backend.value.get_project_accession(project),
                    backend=current_backend,
                    increment_main_progress_bar=lambda: pbar.update(),
                    increment_onto_progress_bar=lambda: onto_pbar.update(),
                    onto_executor=onto_executor,
                    future_holder=future_holder,
                )
                if future_holder.count_of(MainWorkFuture) >= max_workers:
                    future_holder.process_first_completed()

                fut: Future[None] = main_executor.submit(
                    build_project_sync, current_backend, project
                )
                future_wrapper = MainWorkFuture(fut, future_context)
                future_holder.add(future_wrapper)

            logger.info(
                f"All projects submitted to workers: Waiting for the remaining {future_holder.count_of(MainWorkFuture)} projects to complete"
            )
            future_holder.process_all_completed()
        pbar.close()
        onto_pbar.close()
