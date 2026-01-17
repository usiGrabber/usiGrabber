import logging
import logging.config
import logging.handlers
import multiprocessing

from usigrabber.parallelism_new.logging_helpers import ListenerLogging


def listener_process(queue: multiprocessing.Queue) -> None:
    logging.config.dictConfig(ListenerLogging.configure_logging_dict(queue))
    while True:
        try:
            record: logging.LogRecord = queue.get()
            if record is None:  # We send None to signal stop
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except Exception:
            import sys
            import traceback

            print("Whoops! Problem:", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
