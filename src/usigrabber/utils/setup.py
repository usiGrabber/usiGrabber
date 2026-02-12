# myapp/system_setup.py
import logging
import os
import shutil
import sys
from logging import FileHandler
from pathlib import Path

from usigrabber.utils import get_cache_dir
from usigrabber.utils.job_id import get_job_id


def setup_logger(is_main_process: bool, logger_name: str | None = None):
    """
    Set up custom logger.

    :param is_main_process: Whether this is the main process.
    :param logger_name: Name of the logger to configure.
        If left to None, the root logger is used and
        every log message will be formatted.
        Use `usigrabber` to only format logs from usigrabber.
    """

    from usigrabber.utils.logging_helpers.formatter import CustomColorFormatter, JsonFormatter
    from usigrabber.utils.logging_helpers.resource_monitor import start_resource_monitoring

    base_logging_dir = Path(os.getenv("LOGGING_DIR", "logs"))
    main_run_logging_dir = base_logging_dir / "0"
    second_logging_dir = base_logging_dir / "1"

    # Copy existing log in a backoff folder
    # Do it only once in the main process!!
    if is_main_process and main_run_logging_dir.exists():
        if second_logging_dir.exists():
            shutil.rmtree(second_logging_dir)
        shutil.copytree(main_run_logging_dir, second_logging_dir)
        shutil.rmtree(main_run_logging_dir)
    main_run_logging_dir.mkdir(exist_ok=True, parents=True)

    # create necessary directories
    cache_dir = get_cache_dir()
    cache_dir.mkdir(exist_ok=True, parents=True)

    # overwrite root logger, should only be called in application code
    logger = logging.getLogger(logger_name if logger_name else "")
    LOGLEVEL = os.getenv("LOGLEVEL", "DEBUG").upper()
    logger.setLevel(level=LOGLEVEL)

    # mute noisy libraries
    for child in [
        "sqlalchemy",
        "aioftp",
        "urllib3",
        "aiosqlite",
        "aiohttp_client_cache",
        "aiohttp_retry",
    ]:
        logging.getLogger(child).setLevel("WARNING")

    terminal_handler = logging.StreamHandler(sys.stdout)
    terminal_handler.setLevel(os.getenv("LOGLEVEL", "INFO").upper())
    terminal_handler.setFormatter(CustomColorFormatter(use_colors=True))

    # Handler for plain text file output (without colors)
    process_suffix = "main" if is_main_process else os.getpid()
    file_handler = FileHandler(main_run_logging_dir / f"application-{process_suffix}.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(CustomColorFormatter(use_colors=False))

    # Deactivate the filter because it causes some confusion
    # that might not be expected for a person that doesn't know that this exists!
    # file_handler.addFilter(ExponentialBackoffFilter())

    json_handler = FileHandler(
        filename=main_run_logging_dir / f"application-{process_suffix}.jsonl",
    )
    json_handler.setLevel(logging.DEBUG)
    json_handler.setFormatter(JsonFormatter())

    # force new log files per run
    # this will create one empty log in the beginning but that's acceptable
    if is_main_process:
        logger.addHandler(terminal_handler)
    logger.addHandler(file_handler)
    logger.addHandler(json_handler)

    if os.environ.get("LOKI_URL"):
        from usigrabber.utils.logging_helpers.loki_handler import LokiHandler

        job_id = get_job_id()
        loki_url = f"http://{os.environ.get('LOKI_URL')}/loki/api/v1/push"
        loki_handler = LokiHandler(
            url=loki_url,
            tags={
                "app": os.environ.get("APP_ID", "usigrabber"),
                "process": str(process_suffix),
                "job_id": job_id,
            },
            batch_size=100,
            flush_interval=5.0,
            include_metadata=True,  # Include log level, file, line, etc. in logs
            use_structured_metadata=False,  # Append as JSON (Loki structured metadata disabled)
        )
        loki_handler.setLevel(logging.DEBUG)
        logger.addHandler(loki_handler)
        logger.info(f"Loki handler configured for {loki_url}")
    else:
        logger.warning("Env variable LOKI_URL not set. Only saving logs locally.")

    logger.info(f"Setup logging on worker: {process_suffix}")

    # Start resource monitoring in background thread if enabled (only in worker processes)
    if not is_main_process:
        enable_resource_monitoring = (
            os.getenv("ENABLE_RESOURCE_MONITORING", "true").lower() == "true"
        )
        resource_interval = float(os.getenv("RESOURCE_MONITORING_INTERVAL", "60"))

        if enable_resource_monitoring:
            start_resource_monitoring(
                interval_seconds=resource_interval,
                logger_name=logger_name if logger_name else "usigrabber",
            )
