"""CLI commands for the web dashboard."""

import logging
import threading
import time
from pathlib import Path
from typing import Annotated

import typer
import uvicorn
from dotenv import load_dotenv

from usigrabber.cli import app
from usigrabber.utils.logging_helpers.aggregator.dashboard import create_default_dashboard
from usigrabber.utils.logging_helpers.aggregator.running_aggregator import (
    RunningLogAggregator,
)
from usigrabber.web.app import update_metrics

logger = logging.getLogger(__name__)


def update_metrics_loop(aggregator: RunningLogAggregator, stop_event: threading.Event):
    """
    Background thread that continuously updates metrics.

    Args:
        aggregator: The log aggregator to read metrics from
        stop_event: Event to signal when to stop monitoring
    """
    from usigrabber.web.app import update_log_files, update_stats

    while not stop_event.is_set():
        try:
            # Update metrics from log files
            aggregator.update()

            # Get all metrics and update the web app
            metrics = aggregator.get_all_metrics()
            update_metrics(metrics)

            # Get all stats and update the web app
            stats = aggregator.get_all_stats()
            update_stats(stats)

            # Update log file information
            log_files = aggregator.get_loaded_log_files()
            update_log_files(log_files)

            # Wait before next update
            stop_event.wait(2.0)
        except Exception as e:
            logger.error(f"Error updating metrics: {e}", exc_info=True)
            time.sleep(2.0)


@app.command()
def serve_dashboard(
    host: Annotated[
        str,
        typer.Option(help="Host to bind the server to."),
    ] = "127.0.0.1",
    port: Annotated[
        int,
        typer.Option(help="Port to bind the server to."),
    ] = 8000,
    log_dir: Annotated[
        Path,
        typer.Option(
            help="Path to the log directory.",
            envvar="LOGGING_DIR",
            exists=True,
            dir_okay=True,
            file_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ] = Path("logs"),
    reload: Annotated[
        bool,
        typer.Option(help="Enable auto-reload on code changes."),
    ] = False,
):
    """
    Start the web dashboard server.

    The dashboard provides a real-time HTML interface for monitoring analytics metrics.
    """
    # Load environment variables
    load_dotenv()

    logger.info(f"Starting dashboard server on http://{host}:{port}")
    logger.info(f"Reading logs from: {log_dir}")

    # Initialize aggregator
    aggregator = RunningLogAggregator(log_dir=str(log_dir))

    # Create default pipelines
    pipelines, _ = create_default_dashboard()

    # Register pipelines
    for pipeline in pipelines:
        aggregator.register_pipeline(pipeline)

    # Start metrics update thread
    stop_event = threading.Event()
    metrics_thread = threading.Thread(
        target=update_metrics_loop,
        args=(aggregator, stop_event),
        daemon=True,
    )
    metrics_thread.start()
    logger.info("Metrics update thread started")

    try:
        # Start the server
        uvicorn.run(
            "usigrabber.web.app:app",
            host=host,
            port=port,
            reload=reload,
            log_level="info",
        )
    finally:
        logger.info("Stopping metrics update thread...")
        stop_event.set()
        metrics_thread.join(timeout=5)
