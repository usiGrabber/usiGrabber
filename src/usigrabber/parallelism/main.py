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


async def build_all_projects_in_process_pool(
    backends: list[BackendEnum], config: "BuildConfiguration"
) -> None:
    from usigrabber.cli.build import build_project_sync

    # Don't make this global! This would break and python processes
    multiprocessing.set_start_method("spawn")

    futures = set()
    max_workers = config.max_workers or multiprocessing.cpu_count()

    total_successful = 0
    total_errors = 0

    with logging_redirect_tqdm():
        # Main progress bar for total projects
        pbar = tqdm(desc="Building projects", unit=" projects", position=0)

        with ProcessPoolExecutor(max_workers=max_workers, initializer=init_worker) as executor:
            async for project, current_backend in iterate_projects(backends):
                if len(futures) < max_workers:
                    fut: Future[None] = executor.submit(
                        build_project_sync, current_backend, project
                    )
                    futures.add(fut)
                else:
                    done, futures = wait(futures, return_when=FIRST_COMPLETED)
                    for finished in done:
                        try:
                            finished.result()
                            total_successful += 1
                        except Exception as e:
                            logger.error(f"Error occurred: {e}")
                            total_errors += 1
                        finally:
                            pbar.update(1)

            # Process remaining futures
            logger.info(
                f"All projects submitted to workers: Waiting for the remaining {len(futures)} projects to complete"
            )
            for finished in as_completed(futures):
                try:
                    finished.result()
                    total_successful += 1
                except Exception as e:
                    logger.error(f"Error occurred: {e}")
                    total_errors += 1
                finally:
                    pbar.update(1)

        pbar.close()
