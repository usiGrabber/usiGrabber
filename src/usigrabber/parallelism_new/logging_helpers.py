import logging
import logging.config
import multiprocessing
import os
from pathlib import Path

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
            logging.getLogger(f"worker.{job_id}"),
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
                    # "format": "%(asctime)s %(name)-10s %(job_id)-5s %(levelname)-8s %(pid) -10s %(message)s",
                    "format": "%(asctime)s %(name)-10s %(levelname)-8s %(message)s",
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
                "jsonfile": {
                    "class": "logging.FileHandler",
                    "filename": LOG_PATH / "mplog.json",
                    "mode": "w",
                    "formatter": "json",
                },
                "file_json": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "formatter": "json",
                    "filename": LOG_PATH / "mplog.json",
                    "maxBytes": 10 * (1024**2),  # 10 MB
                    "backupCount": 5,
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
            "root": {"handlers": ["console", "file", "errors", "jsonfile"], "level": "DEBUG"},
        }
