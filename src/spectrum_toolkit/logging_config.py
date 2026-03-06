"""Logging configuration for the PSM to MGF pipeline."""

import logging
import logging.config
import logging.handlers
import os
from pathlib import Path

CONSOLE_LOG_LEVEL = os.getenv("CONSOLE_LOG_LEVEL", "INFO").upper()
USE_PLAIN_LOGGING = os.getenv("USE_PLAIN_LOGGING", "false").lower() in ["1", "true", "yes", "on"]
LOG_DIR = Path("./logs/modification-prediction")


def worker_log_configurer(queue):
    """
    This runs once inside every new worker process.
    It replaces all complex handlers with a single QueueHandler.
    """
    h = logging.handlers.QueueHandler(queue)
    root = logging.getLogger()

    # Crucial: Reset existing handlers in the worker so we don't duplicate
    # or try to open files that are locked by the main process.
    if root.handlers:
        root.handlers.clear()

    root.addHandler(h)

    # Set level to match the main process or capture everything
    root.setLevel(logging.DEBUG)


def get_logging_config(logging_dir: Path = LOG_DIR) -> dict:
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "detailed": {
                "class": "logging.Formatter",
                "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
            },
            "simple": {
                "class": "logging.Formatter",
                "format": "%(name)-10s %(levelname)-8s %(message)s",
            },
            # "json": {
            #     "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
            #     # The format string determines the keys in the JSON output
            #     "format": "%(asctime)s %(levelname)s %(message)s %(pid)d %(project_accession)s",
            # },
        },
        "handlers": {
            # console handler using rich for pretty printing
            "console": {
                "()": "rich.logging.RichHandler",
                "rich_tracebacks": True,
                "markup": True,
                "level": CONSOLE_LOG_LEVEL,
            },
            # simple stream handler without rich
            "console_plain": {
                "class": "logging.StreamHandler",
                "formatter": "simple",
                "level": CONSOLE_LOG_LEVEL,
            },
            # file handler for all logs, human readable
            "file": {
                "class": "logging.FileHandler",
                "filename": str(logging_dir / "pipeline.log"),
                "mode": "w",
                "formatter": "detailed",
                "level": "INFO",
            },
            # json file handler for all logs, only this should be sent to loki
            # "file_json": {
            #     "class": "logging.handlers.RotatingFileHandler",
            #     "formatter": "json",
            #     "filename": str(logging_dir / "usigrabber.json"),
            #     "maxBytes": 1 * (1024**2),  # 1 MB
            #     "backupCount": 5,
            #     "level": "INFO",  # debugging should only be done on machine
            # },
            # quick way to inspect errors, human readable
            "errors": {
                "class": "logging.FileHandler",
                "filename": str(logging_dir / "errors.log"),
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
                # "file_json",
            ],
            "level": "DEBUG",  # configure levels on individual handlers instead
        },
        "loggers": {
            # specific logger, not needed for now
            "mod-prediction": {
                "level": "DEBUG",
                "propagate": True,
            },
            "db-fetcher": {
                "level": "DEBUG",
                "propagate": True,
            },
            "export-mgf": {
                "level": "DEBUG",
                "propagate": True,
            },
            "urllib3": {
                "level": "WARNING",
                "propagate": False,
            },
            "asyncio": {
                "level": "WARNING",
                "propagate": False,
            },
        },
    }


def setup_logging(log_dir: Path | None = None) -> None:
    """
    Configure logging to write to both console and files.
    Logs are written to the logs directory.
    """

    log_dir = log_dir or LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.config.dictConfig(get_logging_config(log_dir))

    logging.info(f"Logging initialized. Logfiles will be written to '{log_dir}'")
