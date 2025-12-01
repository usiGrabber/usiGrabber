"""CLI command for profiling usigrabber performance."""

import json
import os
import time
import warnings
from pathlib import Path
from typing import Annotated

import typer
from pydantic import BaseModel
from pyinstrument import Profiler
from sqlalchemy import exc as sa_exc
from sqlalchemy.engine.base import Engine
from sqlmodel import inspect

from usigrabber.backends import BackendEnum
from usigrabber.cli import app
from usigrabber.cli.build import (
    BuildConfiguration,
    ObservabilityConfiguration,
    OntologyConfiguration,
)
from usigrabber.cli.processing import process_project
from usigrabber.db import create_db_and_tables, load_db_engine
from usigrabber.db.cli import reset as db_reset
from usigrabber.utils import get_cache_dir, logger
from usigrabber.utils.logging_helpers.aggregator.dashboard import create_default_dashboard
from usigrabber.utils.logging_helpers.aggregator.renderers import CategoryRenderer
from usigrabber.utils.logging_helpers.aggregator.running_aggregator import RunningLogAggregator
from usigrabber.utils.setup import system_setup

CACHE_DIR = get_cache_dir()


class ProfileConfig(BaseModel):
    """Configuration for the profiling run."""

    project_accession: str
    backend: str
    cache_dir: str
    skip_ontology: bool
    debug: bool


class ProfileMetrics(BaseModel):
    """Metrics collected during profiling."""

    total_time_seconds: float
    total_memory_mb: float | None
    config: ProfileConfig


@app.command()
def profile(
    project_accession: Annotated[
        str,
        typer.Argument(help="Project accession to profile (e.g., PXD000001)"),
    ],
    backend: Annotated[
        BackendEnum,
        typer.Option(help="Backend to use for fetching the project."),
    ] = BackendEnum.PRIDE,
    reset: Annotated[
        bool,
        typer.Option(help="Reset the database before profiling."),
    ] = False,
    debug: Annotated[
        bool,
        typer.Option(help="Run in debug mode with verbose output.", envvar="DEBUG"),
    ] = False,
    no_ontology: Annotated[
        bool,
        typer.Option(
            help="Disable ontology lookup.",
            envvar="NO_ONTOLOGY",
        ),
    ] = False,
    cache_dir: Annotated[
        Path,
        typer.Option(
            help="Path to the cache dir.",
            envvar="CACHE_DIR",
            exists=True,
            dir_okay=True,
            file_okay=False,
            writable=True,
            readable=True,
            resolve_path=True,
        ),
    ] = CACHE_DIR,
    output_dir: Annotated[
        Path,
        typer.Option(
            help="Directory to write profiling outputs.",
            dir_okay=True,
            file_okay=False,
            writable=True,
            resolve_path=True,
        ),
    ] = Path("profile_results"),
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
):
    """
    Profile usigrabber on a single project to benchmark performance.

    This command runs the full processing pipeline on a single project in single-threaded mode,
    collecting memory and CPU profiling data, and exports metrics for optimization analysis.

    Example:
        usigrabber profile PXD000001
        usigrabber profile PXD000001 --reset --no-ontology
    """
    # Reset database if requested
    if reset:
        logger.info("Resetting database before profiling.")
        db_reset(force=True)

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Initialize system
    system_setup(is_main_process=True)

    config = BuildConfiguration(
        observability=ObservabilityConfiguration(debug=debug),
        cache_dir=cache_dir,
        ontologies=OntologyConfiguration(skip_ontos=no_ontology),
    )

    logger.info(f"Starting profile run for project {project_accession}")
    logger.info(f"Backend: {backend.name}")
    logger.info(f"Output directory: {output_dir}")

    # Initialize database
    db_engine = load_db_engine()
    inspector = inspect(db_engine)

    logger.info(f"Engine: {db_engine.url}")
    if len(inspector.get_table_names()) == 0:
        logger.info("No preexisting database found. Database will be initialized.")
        create_db_and_tables(db_engine)

    # Start timing and profiling
    start_time = time.time()
    profiler = Profiler(async_mode="enabled")

    with profiler:
        # Run the actual processing
        run_profile(
            backend=backend,
            project_accession=project_accession,
            config=config,
            db_engine=db_engine,
        )

    # Stop timing
    end_time = time.time()
    total_time = end_time - start_time

    # Save pyinstrument profile
    pyinstrument_html = output_dir / f"{project_accession}_pyinstrument.html"
    profiler.write_html(str(pyinstrument_html))
    logger.info(f"Pyinstrument profile saved to {pyinstrument_html}")

    # Export static dashboard view
    dashboard_html = output_dir / f"{project_accession}_dashboard.html"
    export_static_dashboard(log_dir, dashboard_html)
    logger.info(f"Dashboard snapshot saved to {dashboard_html}")

    # Generate metrics JSON
    profile_config = ProfileConfig(
        project_accession=project_accession,
        backend=backend.name,
        cache_dir=str(cache_dir),
        skip_ontology=no_ontology,
        debug=debug,
    )

    metrics = ProfileMetrics(
        total_time_seconds=total_time,
        total_memory_mb=None,  # Will be filled by memray if used
        config=profile_config,
    )

    metrics_json = output_dir / f"{project_accession}_metrics.json"
    with open(metrics_json, "w") as f:
        json.dump(metrics.model_dump(), f, indent=2)

    logger.info(f"Metrics saved to {metrics_json}")
    logger.info(f"Total execution time: {total_time:.2f}s")
    logger.info("Profile run complete!")

    typer.echo(f"\n{'=' * 60}")
    typer.echo(f"Profile Results for {project_accession}")
    typer.echo(f"{'=' * 60}")
    typer.echo(f"Total Time: {total_time:.2f}s")
    typer.echo("\nOutputs:")
    typer.echo(f"  - Pyinstrument: {pyinstrument_html}")
    typer.echo(f"  - Dashboard:    {dashboard_html}")
    typer.echo(f"  - Metrics JSON: {metrics_json}")
    typer.echo(f"{'=' * 60}\n")


def run_profile(
    backend: BackendEnum,
    project_accession: str,
    config: BuildConfiguration,
    db_engine: Engine,
) -> None:
    """
    Profile a single project through the full processing pipeline.

    Args:
        backend: Backend to use
        project_accession: Project accession to profile
        config: Build configuration
        db_engine: Database engine
    """
    logger.info(f"Fetching project {project_accession} from {backend.name}")

    # Set environment variables
    os.environ["CACHE_DIR"] = str(config.cache_dir)

    if config.ontologies.skip_ontos:
        os.environ["NO_ONTOLOGY"] = "1"

    if os.getenv("NO_ONTOLOGY"):
        logger.warning("Ontology lookup is disabled.")

    if config.observability.debug:
        os.environ["DEBUG"] = "1"

    if os.getenv("DEBUG"):
        logger.info("Running in DEBUG mode.")

    backend_impl = backend.value

    with warnings.catch_warnings(action="ignore", category=sa_exc.SAWarning):
        # Fetch the specific project - use sync wrapper for async call
        import asyncio

        project = asyncio.run(backend_impl.get_project_by_accession(project_accession))

        # Process the project using shared logic
        asyncio.run(process_project(db_engine, project, backend))


def export_static_dashboard(log_dir: Path, output_file: Path) -> None:
    """
    Export a static HTML snapshot of the dashboard metrics.

    Args:
        log_dir: Directory containing log files
        output_file: Path to write the static HTML dashboard
    """
    # Initialize aggregator
    aggregator = RunningLogAggregator(log_dir=str(log_dir))

    # Create default pipelines and categories
    pipelines, categories = create_default_dashboard()

    # Register pipelines
    for pipeline in pipelines:
        aggregator.register_pipeline(pipeline)

    # Update metrics from log files
    aggregator.update()

    # Get all metrics
    all_metrics = aggregator.get_all_metrics()

    # Render HTML
    html_content = render_static_dashboard(categories, all_metrics)

    # Write to file
    with open(output_file, "w") as f:
        f.write(html_content)


def render_static_dashboard(categories: list[CategoryRenderer], all_metrics: dict) -> str:
    """
    Render the dashboard as a static HTML page.

    Args:
        categories: List of category renderers
        all_metrics: Metrics data from the aggregator

    Returns:
        HTML string
    """
    html_parts = [
        "<!DOCTYPE html>",
        "<html>",
        "<head>",
        "    <meta charset='utf-8'>",
        "    <title>USI Grabber - Profile Dashboard</title>",
        "    <style>",
        "        body {",
        "            font-family: 'Courier New', monospace;",
        "            background-color: #1e1e1e;",
        "            color: #d4d4d4;",
        "            padding: 20px;",
        "            margin: 0;",
        "        }",
        "        .container {",
        "            max-width: 1200px;",
        "            margin: 0 auto;",
        "        }",
        "        h1 {",
        "            text-align: center;",
        "            color: #4ec9b0;",
        "        }",
        "        .divider {",
        "            border-top: 2px solid #4ec9b0;",
        "            margin: 20px 0;",
        "        }",
        "        pre {",
        "            background-color: #252526;",
        "            padding: 15px;",
        "            border-radius: 5px;",
        "            overflow-x: auto;",
        "            white-space: pre;",
        "        }",
        "        .timestamp {",
        "            text-align: center;",
        "            color: #858585;",
        "            margin-top: 20px;",
        "        }",
        "    </style>",
        "</head>",
        "<body>",
        "    <div class='container'>",
        "        <h1>📊 USI Grabber - Profile Dashboard</h1>",
        "        <div class='divider'></div>",
        "        <pre>",
    ]

    # Render each category
    for category in categories:
        category_output = category.render(all_metrics)
        html_parts.append(category_output)
        html_parts.append("\n")

    html_parts.extend(
        [
            "        </pre>",
            "        <div class='divider'></div>",
            f"        <div class='timestamp'>Generated at {time.strftime('%Y-%m-%d %H:%M:%S')}</div>",
            "    </div>",
            "</body>",
            "</html>",
        ]
    )

    return "\n".join(html_parts)
