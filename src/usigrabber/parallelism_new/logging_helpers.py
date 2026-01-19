import logging
import logging.config
import multiprocessing
import os
import shutil
from pathlib import Path


class Environment:
    @staticmethod
    def parse_bool(key: str, default: bool = False) -> bool:
        value = os.getenv(key, str(default)).lower()
        return value in ("1", "true", "yes", "on")

    @staticmethod
    def parse_int(key: str, default: int = 0) -> int:
        value = os.getenv(key, str(default))
        return int(value)

    @staticmethod
    def parse_str(key: str, default: str = "") -> str:
        return os.getenv(key, default)

    @staticmethod
    def parse_path(key: str, default: Path | None = None) -> Path:
        if default is None:
            default = Path.cwd()

        value = Environment.parse_str(key, "")
        return Path(value) if value else default


APP_NAME = "usiGrabber"
USE_PLAIN_LOGGING = Environment.parse_bool("USIGRABBER_USE_PLAIN_LOGGING", False)
CONSOLE_LOG_LEVEL = "WARNING"

# Set up logging directory
NUM_BACKUPS = Environment.parse_int("USIGRABBER_LOG_NUM_BACKUPS", 5)
LOG_ROOT = Environment.parse_path("USIGRABBER_LOG_DIR", Path("logs/logging-mp"))
LOG_ROOT.mkdir(exist_ok=True, parents=True)

LOG_PATH = LOG_ROOT / "0"
HUMAN_READABLE_LOG_PATH = LOG_PATH / "human.log"


COMPRESSION_FORMATS = [f[0] for f in shutil.get_archive_formats()]
ARCHIVE_FORMAT = "zstdtar" if "zstdtar" in COMPRESSION_FORMATS else "gztar"
ARCHIVE_FILE_EXT = ".tar.zst" if ARCHIVE_FORMAT == "zstdtar" else ".tar.gz"


def create_archive(src: Path) -> Path:
    """
    Create a compressed archive of the given directory and remove the original.
    Returns the path to the created archive.

    :param src: The source directory to archive.
    :return: Path to the created archive.

    Example usage:
    ---
    >>> create_archive(Path("/dir/to/logs"))
    Path('/dir/to/logs.tar.zst')
    """

    archive = shutil.make_archive(str(src), ARCHIVE_FORMAT, root_dir=src)
    print(f"Archived log directory {src} to {archive}")
    shutil.rmtree(src)
    return Path(archive)


def rotate_log_dirs() -> None:
    if LOG_PATH.exists():
        # Archive the existing log directory
        create_archive(LOG_PATH)

        # Rotate existing log directories
        for i in range(NUM_BACKUPS - 1, -1, -1):
            src = LOG_ROOT / f"{i}{ARCHIVE_FILE_EXT}"
            dst = LOG_ROOT / f"{i + 1}{ARCHIVE_FILE_EXT}"
            if src.exists():
                src.rename(dst)

    LOG_PATH.mkdir(exist_ok=True)

    # create symlink in root logging to human readable log of latest run
    symlink_path = LOG_ROOT / "latest.log"
    if symlink_path.exists():
        symlink_path.unlink()

    if not symlink_path.is_symlink():
        symlink_path.symlink_to(HUMAN_READABLE_LOG_PATH.absolute())


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
    def configure_logging_dict(cls, q: multiprocessing.Queue) -> dict:
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
    def get_logger(cls, q: multiprocessing.Queue, project_accession: str) -> logging.LoggerAdapter:
        pid = os.getpid()
        logging.config.dictConfig(cls.configure_logging_dict(q))
        return MergingLoggerAdapter(
            logging.getLogger(f"{APP_NAME}.worker.{project_accession}"),
            {"project_accession": project_accession, "pid": pid},
        )


class ListenerLogging:
    @classmethod
    def configure_logging(cls, q: multiprocessing.Queue) -> None:
        """Configure the logging dictionary for the listener process."""

        rotate_log_dirs()

        config = {
            "version": 1,
            # disable the "setup" logger used in the parent process
            "disable_existing_loggers": True,
            "formatters": {
                "detailed": {
                    "class": "logging.Formatter",
                    "format": "%(asctime)s %(project_accession)s %(levelname)s %(message)s",
                },
                "simple": {
                    "class": "logging.Formatter",
                    "format": "%(name)-10s %(levelname)-8s %(message)s %(extra_info)s",
                },
                "json": {
                    "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
                    # The format string determines the keys in the JSON output
                    "format": "%(asctime)s %(levelname)s %(message)s %(pid)d %(project_accession)s",
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
                # simple stream handler without rich
                "console_plain": {
                    "class": "logging.StreamHandler",
                    "formatter": "simple",
                    "level": CONSOLE_LOG_LEVEL,
                },
                # file handler for all logs, human readable
                "file": {
                    "class": "logging.FileHandler",
                    "filename": HUMAN_READABLE_LOG_PATH,
                    "mode": "w",
                    "formatter": "detailed",
                    "level": "INFO",
                },
                # file handler for specific worker
                "worker3": {
                    "class": "logging.FileHandler",
                    "filename": LOG_PATH / "worker3.log",
                    "mode": "w",
                    "formatter": "detailed",
                    "level": "DEBUG",  # detailed logs for specific worker
                },
                # json file handler for all logs, only this should be sent to loki
                "file_json": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "formatter": "json",
                    "filename": LOG_PATH / "usigrabber.json",
                    "maxBytes": 1 * (1024**2),  # 1 MB
                    "backupCount": 5,
                    "level": "INFO",  # debugging should only be done on machine
                },
                # quick way to inspect errors, human readable
                "errors": {
                    "class": "logging.FileHandler",
                    "filename": LOG_PATH / "errors.log",
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
                # specific logger, not needed for now
                # f"{APP_NAME}.worker.3": {"handlers": ["worker3"]},
            },
        }

        logging.config.dictConfig(config)
