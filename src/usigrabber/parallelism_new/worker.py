import multiprocessing
import os
import time

from usigrabber.parallelism_new.logging_helpers import WorkerLogging

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
def do_work(job_id: int) -> int:
    """
    This function just assumes _shared_queue is already set.
    """
    if _shared_queue is None:
        raise RuntimeError("Queue was not initialized! Did you set the initializer?")

    logger = WorkerLogging.get_logger(_shared_queue, job_id)

    # Simulate work
    time.sleep(0.1)
    result = job_id * 10

    logger.info("Completed work for job %d, result=%d", job_id, result)
    if job_id % 3 == 0:
        logger.error(
            "Job %d encountered an issue.",
            job_id,
            extra={"extra_info": "Simulated error for demonstration."},
        )

    return result
