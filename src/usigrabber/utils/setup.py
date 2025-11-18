# myapp/system_setup.py
import logging
import os
import sys
import threading
from pathlib import Path

from dotenv import load_dotenv

from usigrabber.utils.logging_helpers.filters import ExponentialBackoffFilter

_setup_done = False
_setup_lock = threading.Lock()


def system_setup(logger_name: str):
    """
    - Setups logger
    - Loads env variables

    If you use "" for the logger_name we configure the root logger and
    every log message will be formatted.

    If you only want to format our logs, use "usigrabber" as the name.
    """

    global _setup_done, _setup_lock
    if _setup_done:
        return

    with _setup_lock:
        if _setup_done:
            return  # another thread did it

        from usigrabber.utils.logging_helpers.formatter import CustomColorFormatter, JsonFormatter

        load_dotenv()

        logging_dir = Path(os.environ.get("LOGGING_DIR", "logs"))

        logging_dir.mkdir(exist_ok=True)

        # TODO: Remove this later: https://rednafi.com/python/no-hijack-root-logger/
        logger = logging.getLogger(logger_name)

        logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())
        if logger.hasHandlers():
            logger.handlers.clear()

        terminal_handler = logging.StreamHandler(sys.stdout)
        terminal_handler.setLevel(logging.INFO)
        terminal_handler.setFormatter(CustomColorFormatter(use_colors=True))
        terminal_handler.addFilter(ExponentialBackoffFilter())

        # Handler for plain text file output (without colors)
        file_handler = logging.FileHandler(logging_dir / "application.log", mode="w")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(CustomColorFormatter(use_colors=False))
        file_handler.addFilter(ExponentialBackoffFilter())

        # Handler for JSON file output (remains unchanged)
        json_handler = logging.FileHandler(logging_dir / "application.jsonl", mode="w")
        json_handler.setLevel(logging.DEBUG)
        json_handler.setFormatter(JsonFormatter())

        logger.addHandler(terminal_handler)
        logger.addHandler(file_handler)
        logger.addHandler(json_handler)
