import logging
import logging.config
import multiprocessing
import os
from pathlib import Path


def get_bool_env(key: str, default: bool = False) -> bool:
    value = os.getenv(key, str(default)).lower()
    return value in ("1", "true", "yes", "on")


APP_NAME = "usiGrabber"
USE_PLAIN_LOGGING = get_bool_env("USIGRABBER_USE_PLAIN_LOGGING", False)
CONSOLE_LOG_LEVEL = "DEBUG"

# Set up logging directory
LOG_PATH = Path("logs/logging-mp")
LOG_PATH.mkdir(exist_ok=True, parents=True)


class MergingLoggerAdapter(logging.LoggerAdapter):
    """
    A `LoggerAdapter` that merges its 'extra' dict with per-call 'extra' dicts.

    This is necessary because the standard LoggerAdapter simply overwrites
    the 'extra' dict on a per-call basis, which means extra info passed to
    individual logs is lost.
    """

    def process(self, msg, kwargs):
        # 1. Get the 'extra' passed to the log call, or an empty dict if none exists
        logging_call_extra = kwargs.get("extra", {})

        # 2. Copy the adapter's context so we don't mutate the original
        merged_extra = getattr(self, "extra", {}).copy()

        # 3. Update with the call-specific extra
        # (This ensures local message vars take precedence over job vars)
        merged_extra.update(logging_call_extra)

        # 4. Put the merged dict back into kwargs
        kwargs["extra"] = merged_extra

        return msg, kwargs


class WorkerLogging:
    @classmethod
    def configure_logging_dict(cls, q: multiprocessing.Queue, job_id: int) -> dict:
        # The worker process configuration is just a QueueHandler attached to the
        # root logger, which allows all messages to be sent to the queue.
        # We disable existing loggers to disable the "setup" logger used in the
        # parent process. This is needed on POSIX because the logger will
        # be there in the child following a fork().

        return {
            "version": 1,
            "disable_existing_loggers": True,
            "handlers": {
                "queue": {
                    "class": "logging.handlers.QueueHandler",
                    "queue": q,
                }
            },
            "root": {"handlers": ["queue"], "level": "DEBUG"},
        }

    @classmethod
    def get_logger(cls, q: multiprocessing.Queue, job_id: int) -> logging.LoggerAdapter:
        pid = os.getpid()
        logging.config.dictConfig(cls.configure_logging_dict(q, job_id))
        return MergingLoggerAdapter(
            logging.getLogger(f"{APP_NAME}.worker.{job_id}"),
            {"job_id": job_id, "pid": pid},
        )


class ListenerLogging:
    @classmethod
    def configure_logging_dict(cls, q: multiprocessing.Queue) -> dict:
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
                    "format": "%(asctime)s %(job_id)d %(levelname)s %(message)s",
                },
                "simple": {
                    "class": "logging.Formatter",
                    "format": "%(name)-10s %(levelname)-8s %(message)s %(extra_info)s",
                },
                "json": {
                    "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
                    # The format string determines the keys in the JSON output
                    "format": "%(asctime)s %(levelname)s %(message)s %(pid)d %(job_id)d",
                },
            },
            "handlers": {
                # console handler using rich for pretty printing
                "console": {
                    "()": "rich.logging.RichHandler",
                    "rich_tracebacks": True,
                    "markup": True,
                    "level": CONSOLE_LOG_LEVEL,
                },
                # unused console handler without rich
                "console_plain": {
                    "class": "logging.StreamHandler",
                    "formatter": "simple",
                    "level": CONSOLE_LOG_LEVEL,
                },
                # file handler for all logs
                "file": {
                    "class": "logging.FileHandler",
                    "filename": LOG_PATH / "mplog.log",
                    "mode": "w",
                    "formatter": "detailed",
                    "level": "INFO",
                },
                # file handler for specific worker
                "worker3": {
                    "class": "logging.FileHandler",
                    "filename": LOG_PATH / "mplog-worker3.log",
                    "mode": "w",
                    "formatter": "detailed",
                    "level": "DEBUG",  # detailed logs for specific worker
                },
                # json file handler for all logs
                "file_json": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "formatter": "json",
                    "filename": LOG_PATH / "mplog.json",
                    "maxBytes": 1 * (1024**2),  # 1 MB
                    "backupCount": 5,
                    "level": "INFO",
                },
                "errors": {
                    "class": "logging.FileHandler",
                    "filename": LOG_PATH / "mplog-errors.log",
                    "mode": "w",
                    "formatter": "detailed",
                    "level": "ERROR",
                },
            },
            # configure root logger
            "root": {
                "handlers": [
                    "console" if not USE_PLAIN_LOGGING else "console_plain",
                    "file",
                    "errors",
                    "file_json",
                ],
                # "level": "INFO",  # configure levels on individual handlers instead
            },
            "loggers": {
                f"{APP_NAME}.worker.3": {"handlers": ["worker3"]},
            },
        }
