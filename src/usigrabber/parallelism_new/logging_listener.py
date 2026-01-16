import logging
import logging.config
import logging.handlers
import multiprocessing
from pathlib import Path

LOG_PATH = Path("logs")
LOG_PATH.mkdir(exist_ok=True)


def configure_logging_dict(q: multiprocessing.Queue) -> dict:
    # The listener process configuration shows that the full flexibility of
    # logging configuration is available to dispatch events to handlers however
    # you want.
    # We disable existing loggers to disable the "setup" logger used in the
    # parent process. This is needed on POSIX because the logger will
    # be there in the child following a fork().
    return {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "detailed": {
                "class": "logging.Formatter",
                "format": "%(asctime)s %(name)-10s %(job_id)-5s %(levelname)-8s %(pid) -10s %(message)s",
            },
            "simple": {
                "class": "logging.Formatter",
                "format": "%(name)-10s %(job_id)-5s %(levelname)-8s %(pid)-10s %(message)s",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "simple",
                "level": "WARNING",
            },
            "file": {
                "class": "logging.FileHandler",
                "filename": LOG_PATH / "mplog.log",
                "mode": "w",
                "formatter": "detailed",
            },
            "foofile": {
                "class": "logging.FileHandler",
                "filename": LOG_PATH / "mplog-foo.log",
                "mode": "w",
                "formatter": "detailed",
            },
            "errors": {
                "class": "logging.FileHandler",
                "filename": LOG_PATH / "mplog-errors.log",
                "mode": "w",
                "formatter": "detailed",
                "level": "ERROR",
            },
        },
        "loggers": {"foo": {"handlers": ["foofile"]}},
        "root": {"handlers": ["console", "file", "errors"], "level": "DEBUG"},
    }


def listener_process(queue: multiprocessing.Queue) -> None:
    logging.config.dictConfig(configure_logging_dict(queue))
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
