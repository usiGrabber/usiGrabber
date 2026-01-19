import multiprocessing
import os
import random
import time

from usigrabber.parallelism_new.logging_helpers import MergingLoggerAdapter, WorkerLogging
from usigrabber.parallelism_new.pride import Project

# 1. The Placeholder
# This variable lives separately in every single worker process's memory.
_shared_queue = None


# 2. The Injector (Initializer)
def init_worker_queue(q: multiprocessing.Queue) -> None:
    """
    This function is called by the Pool when the process starts.
    It receives the queue from the main process and saves it locally.
    """
    global _shared_queue
    _shared_queue = q
    # Debug print to confirm it worked
    print(f"[{os.getpid()}] Worker module initialized with queue.")


# 3. The Task
def do_work(project: Project) -> None:
    """
    This function just assumes _shared_queue is already set.
    """
    if _shared_queue is None:
        raise RuntimeError("Queue was not initialized! Did you set the initializer?")

    logger = WorkerLogging.get_logger(_shared_queue, project.project_accession)

    # Simulate work
    time.sleep(0.1)

    for file in project.files:
        file_logger = MergingLoggerAdapter(logger, {"file_name": file})
        file_logger.info("Processing file %s for project %s", file, project.project_accession)

    if random.random() < 0.33:
        logger.debug("Doing some debugging.")
        logger.error(
            "Project %s Encountered an issue.",
            project.project_accession,
            extra={"extra_info": "Simulated error for demonstration."},
        )
    logger.info("Completed work for project %s", project.project_accession)
