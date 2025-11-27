# myapp/system_setup.py
import logging
import os
import sys
from logging import FileHandler
from pathlib import Path

from dotenv import load_dotenv

from usigrabber.utils import get_cache_dir
from usigrabber.utils.logging_helpers.filters import ExponentialBackoffFilter


class ConsoleFilter(logging.Filter):
    """Only allow records with 'to_console=True' to go to the console"""

    def filter(self, record):
        return getattr(record, "to_console", False)


def system_setup(is_main_process: bool, logger_name: str | None = None):
    """
    - Setups logger
    - Loads env variables

    If you use "" for the logger_name we configure the root logger and
    every log message will be formatted.

    If you only want to format our logs, use "usigrabber" as the name.
    """

    from usigrabber.utils.logging_helpers.formatter import CustomColorFormatter, JsonFormatter

    load_dotenv()

    logging_dir = Path(os.getenv("LOGGING_DIR", "logs"))

    logging_dir.mkdir(exist_ok=True)

    # create necessary directories
    cache_dir = get_cache_dir()
    cache_dir.mkdir(exist_ok=True, parents=True)

    logging_dir = Path(os.getenv("LOGGING_DIR", "logs"))
    logging_dir.mkdir(exist_ok=True)

    # overwrite root logger, should only be called in application code
    logger = logging.getLogger(logger_name if logger_name else "")
    LOGLEVEL = os.getenv("LOGLEVEL", "INFO").upper()
    logger.setLevel(
        level=LOGLEVEL
    )  # overwrite root logger, should only be called in application code
    logger = logging.getLogger(logger_name)

    if logger.hasHandlers():
        logger.handlers.clear()

    # mute noisy libraries
    for child in ["sqlalchemy", "aioftp", "urllib3"]:
        logging.getLogger(child).setLevel("WARNING")

    terminal_handler = logging.StreamHandler(sys.stdout)
    terminal_handler.setLevel(os.getenv("LOGLEVEL", "INFO").upper())
    terminal_handler.setFormatter(CustomColorFormatter(use_colors=True))
    if not is_main_process:
        terminal_handler.addFilter(ConsoleFilter())
    terminal_handler.addFilter(ExponentialBackoffFilter())

    # Handler for plain text file output (without colors)
    process_suffix = "main" if is_main_process else os.getpid()
    file_handler = FileHandler(logging_dir / f"application-{process_suffix}.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(CustomColorFormatter(use_colors=False))
    file_handler.addFilter(ExponentialBackoffFilter())

    json_handler = FileHandler(
        filename=logging_dir / f"application-{process_suffix}.jsonl",
    )
    json_handler.setLevel(logging.DEBUG)
    json_handler.setFormatter(JsonFormatter())

    # force new log files per run
    # this will create one empty log in the beginning but that's acceptable

    logger.addHandler(terminal_handler)
    logger.addHandler(file_handler)
    logger.addHandler(json_handler)

    logging.info(f"Setting up logging on worker: {process_suffix}")
