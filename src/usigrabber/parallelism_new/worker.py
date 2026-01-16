import logging
import logging.config
import logging.handlers
import multiprocessing
import os
import time

# 1. The Placeholder
# This variable lives separately in every single worker process's memory.
_shared_queue = None


class MultiprocessingContextFilter(logging.Filter):
    def __init__(self, job_id: int):
        super().__init__()
        self.job_id = job_id

    def filter(self, record):
        # Get the current process identity
        proc = multiprocessing.current_process()
        record.process_name = proc.name
        # record.pid = proc.pid
        record.worker_type = "child" if proc.name != "MainProcess" else "manager"
        # record.job_id = self.job_id

        return True


def configure_logging_dict(q: multiprocessing.Queue, job_id: int) -> dict:
    # The worker process configuration is just a QueueHandler attached to the
    # root logger, which allows all messages to be sent to the queue.
    # We disable existing loggers to disable the "setup" logger used in the
    # parent process. This is needed on POSIX because the logger will
    # be there in the child following a fork().
    return {
        "version": 1,
        "disable_existing_loggers": True,
        # "filters": {"mp_filter": {"()": MultiprocessingContextFilter, "job_id": job_id}},
        "handlers": {
            "queue": {
                "class": "logging.handlers.QueueHandler",
                "queue": q,
                # "filters": ["mp_filter"],
            }
        },
        "root": {"handlers": ["queue"], "level": "DEBUG"},
    }


def get_logger(q: multiprocessing.Queue, job_id: int) -> logging.LoggerAdapter:
    pid = os.getpid()
    logging.config.dictConfig(configure_logging_dict(q, job_id))
    _logger = logging.getLogger(f"worker.{job_id}")
    logger = logging.LoggerAdapter(_logger, {"job_id": job_id, "pid": pid})
    return logger


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

    # pid = os.getpid()
    # logging.config.dictConfig(configure_logging_dict(_shared_queue, job_id))
    # _logger = logging.getLogger(f"worker.{job_id}")
    # logger = logging.LoggerAdapter(_logger, {"job_id": job_id, "pid": pid})
    logger = get_logger(_shared_queue, job_id)

    # Simulate work
    time.sleep(0.1)
    result = job_id * 10

    if job_id % 100 == 0:
        logger.info("Completed work for job %d, result=%d", job_id, result)
        logger.error("Job %d encountered an issue.", job_id)

    return result
