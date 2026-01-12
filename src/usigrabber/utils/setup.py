import logging
import os
import random
import shutil
import sys
import time
from logging import FileHandler
from pathlib import Path

from usigrabber.utils import get_cache_dir

LOCAL_JOB_ID = f"local-{random.randint(0, 10**6)}"


def nfs_safe_remove(path: Path):
    """
    Safely remove a directory on NFS by renaming it first.
    If the removal fails due to open handles, the path is at least clear
    for new files, and the trash dir is left behind (nfs silly rename).
    """
    if not path.exists():
        return

    # Generate a unique trash name
    timestamp = int(time.time() * 1000)
    trash_path = path.parent / f".trash_{path.name}_{timestamp}"

    try:
        # 1. Rename the blocking directory to get it out of the way
        path.rename(trash_path)
    except OSError as e:
        # If we can't rename, we can't proceed. This is a hard error.
        logging.warning(f"Could not rename {path} to {trash_path}: {e}")
        return

    try:
        # 2. Try to delete the renamed directory
        shutil.rmtree(trash_path, ignore_errors=True)
    except Exception:
        # If deletion fails (e.g. NFS locks), it's fine.
        # The trash folder is hidden and no longer blocking the main app.
        pass


def setup_logger(is_main_process: bool, logger_name: str | None = None):
    """
    Set up custom logger with NFS-safe rotation.
    """
    # ... imports inside function as per your original code ...
    from usigrabber.utils.logging_helpers.formatter import CustomColorFormatter, JsonFormatter
    from usigrabber.utils.logging_helpers.resource_monitor import start_resource_monitoring

    base_logging_dir = Path(os.getenv("LOGGING_DIR", "logs"))
    main_run_logging_dir = base_logging_dir / "0"
    second_logging_dir = base_logging_dir / "1"

    # --- CHANGED: NFS-Safe Rotation Logic ---
    if is_main_process:
        # 1. Clear the backup slot (logs/1) using the safe remove
        if second_logging_dir.exists():
            nfs_safe_remove(second_logging_dir)

        # 2. If we have a previous run (logs/0), move it to logs/1
        if main_run_logging_dir.exists():
            try:
                # We rename logs/0 -> logs/1 directly.
                # This is faster and safer than copy+delete on NFS.
                main_run_logging_dir.rename(second_logging_dir)
            except OSError:
                # Fallback if rename fails (e.g. across different mounts)
                shutil.copytree(main_run_logging_dir, second_logging_dir, dirs_exist_ok=True)
                nfs_safe_remove(main_run_logging_dir)

    # 3. Ensure the main log dir is fresh and exists
    # Even if previous steps failed, main_run_logging_dir is likely renamed or deleted
    # If it still exists with locks, nfs_safe_remove below ensures it is moved aside.
    if is_main_process and main_run_logging_dir.exists():
        nfs_safe_remove(main_run_logging_dir)

    main_run_logging_dir.mkdir(exist_ok=True, parents=True)
    # ----------------------------------------

    # create necessary directories
    cache_dir = get_cache_dir()
    cache_dir.mkdir(exist_ok=True, parents=True)

    # overwrite root logger
    logger = logging.getLogger(logger_name if logger_name else "")
    LOGLEVEL = os.getenv("LOGLEVEL", "DEBUG").upper()
    logger.setLevel(level=LOGLEVEL)

    # mute noisy libraries
    for child in ["sqlalchemy", "aioftp", "urllib3"]:
        logging.getLogger(child).setLevel("WARNING")

    terminal_handler = logging.StreamHandler(sys.stdout)
    terminal_handler.setLevel(os.getenv("LOGLEVEL", "INFO").upper())
    terminal_handler.setFormatter(CustomColorFormatter(use_colors=True))

    process_suffix = "main" if is_main_process else os.getpid()

    # Check if directory exists before creating handlers to be safe
    if not main_run_logging_dir.exists():
        main_run_logging_dir.mkdir(parents=True, exist_ok=True)

    file_handler = FileHandler(main_run_logging_dir / f"application-{process_suffix}.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(CustomColorFormatter(use_colors=False))

    json_handler = FileHandler(
        filename=main_run_logging_dir / f"application-{process_suffix}.jsonl",
    )
    json_handler.setLevel(logging.DEBUG)
    json_handler.setFormatter(JsonFormatter())

    if is_main_process:
        logger.addHandler(terminal_handler)
    logger.addHandler(file_handler)
    logger.addHandler(json_handler)

    if os.environ.get("LOKI_URL"):
        from usigrabber.utils.logging_helpers.loki_handler import LokiHandler

        if os.environ.get("SLURM_JOB_ID"):
            job_id = f"slurm-{os.environ.get('SLURM_JOB_ID')}"
        else:
            job_id = LOCAL_JOB_ID

        loki_url = f"http://{os.environ.get('LOKI_URL')}/loki/api/v1/push"
        loki_handler = LokiHandler(
            url=loki_url,
            tags={"app": "usigrabber", "process": str(process_suffix), "job_id": job_id},
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
