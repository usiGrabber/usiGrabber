import asyncio
import logging
import os
from pathlib import Path
from typing import Annotated

import typer
from rich.progress import Progress, SpinnerColumn, TextColumn
from sqlalchemy import inspect
from sqlmodel import Session, select

from usigrabber.backends import BackendEnum
from usigrabber.cli import app
from usigrabber.db import Project, create_db_and_tables, load_db_engine
from usigrabber.file_parser import MzidImportError, MzidParseError, import_mzid
from usigrabber.utils.file import download_ftp, extract_archive, temporary_path

STANDARD_BACKENDS = [enum for enum in BackendEnum]

logger = logging.getLogger(__name__)


@app.command()
def build(
    data_dir: Annotated[
        Path,
        typer.Option(
            help="Path to the USI data directory.",
            envvar="UG_DATA_DIR",
            exists=True,
            dir_okay=True,
            file_okay=False,
            writable=True,
            readable=True,
            resolve_path=True,
        ),
    ] = Path("./data"),
    backends: Annotated[
        list[BackendEnum],
        typer.Option(help="Set of backends to fetch data from."),
    ] = STANDARD_BACKENDS,
    debug: Annotated[
        bool,
        typer.Option(help="Run in debug mode with verbose output.", envvar="DEBUG"),
    ] = False,
):
    asyncio.run(async_build(data_dir, backends, debug))


async def async_build(
    data_dir: Path = Path("./data"),
    backends: list[BackendEnum] = STANDARD_BACKENDS,
    debug: bool = False,
) -> None:
    """Build USI database."""

    logger.info("Building database.")

    os.environ["UG_DATA_DIR"] = str(data_dir)

    if debug:
        os.environ["DEBUG"] = "1"

    if os.getenv("DEBUG"):
        logger.info("Running in DEBUG mode.")

    # WORKFLOW

    # set up database connection
    db_engine = load_db_engine()
    inspector = inspect(db_engine)
    if len(inspector.get_table_names()) == 0:
        logger.info("No preexisting database found. Database will be initialized.")
        create_db_and_tables(db_engine)

    # get all existing project accessions in the database
    with Session(db_engine) as session:
        # TODO: filter for "complete" projects only
        statement = select(Project.accession)
        existing_accessions: set[str] = set(session.exec(statement).all())

    logger.info(
        "Found %s existing accessions in the database.",
        len(existing_accessions),
    )

    for backend_enum in backends:
        backend = backend_enum.value
        logger.debug("Fetching data from backend: %s", backend_enum.name)

        imported = errors = 0
        completed = 0
        error_projects = []
        with (
            Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=typer.echo(),
            ) as progress,
            Session(db_engine) as session,
        ):
            task = progress.add_task(
                f"Importing projects... {completed}/?",
                completed=0,
            )
            async for project in backend.get_new_projects(existing_accessions):
                # TODO: support other submission types
                if project.get("submissionType") != "COMPLETE":
                    continue

                try:
                    await backend.dump_project_to_db(session, project)
                except Exception as e:
                    errors += 1
                    error_projects.append((project.get("accession"), str(e)))
                    session.rollback()
                    progress.update(
                        task,
                        description=f"Importing projects... {completed}/?",
                        completed=imported + errors,
                    )
                    continue

                session.commit()
                imported += 1
                completed = imported + errors
                progress.update(
                    task,
                    description=f"Importing projects... {completed}/?",
                    completed=imported + errors,
                )

                # download files
                files = backend.get_files_for_project(project["accession"])

                # process files
                if files["result"]:
                    for file in files["result"]:
                        # project_data["psms"].append(list(backend.process_result_file(file)))
                        # parse filename from file url
                        file_url = file["filepath"]
                        filename = os.path.basename(file_url)

                        logger.debug(
                            f"Processing result file {filename} "
                            + f"({file['file_size'] / (1024 * 1024):,.2f} MB)"
                        )

                        # extract name and extension
                        file_name, ext = os.path.splitext(filename)

                        with temporary_path() as tmp_dir:
                            path = await download_ftp(
                                url=file_url, out_dir=tmp_dir, file_name=filename
                            )

                            if path is None or not path.exists():
                                logger.error(
                                    "Failed to download file %s for project %s.",
                                    filename,
                                    project["accession"],
                                )
                                continue

                            extracted_files = extract_archive(archive_path=path, extract_to=tmp_dir)

                            filetypes = set()
                            for f in extracted_files:
                                str_f = str(f)
                                ext = os.path.splitext(str_f)[1]
                                filetypes.add(ext)

                            # access files based on priority
                            if ".mzid" in filetypes:
                                # Process mzID file
                                try:
                                    stats = import_mzid(path, project["accession"])
                                    duration_str = (
                                        f"{stats.duration_seconds:.1f}s"
                                        if stats.duration_seconds is not None
                                        else "N/A"
                                    )
                                    logger.info(
                                        f"Imported {stats.psm_count:,} PSMs from {path.name} "
                                        f"({duration_str})"
                                    )
                                except MzidParseError as e:
                                    logger.warning(f"Skipping malformed mzID file {path.name}: {e}")
                                    continue
                                except MzidImportError as e:
                                    logger.error(
                                        f"Failed to import mzID file {path.name}: {e}",
                                        exc_info=True,
                                        stack_info=True,
                                        extra={
                                            "mzid_file": str(path),
                                            "project_accession": project["accession"],
                                        },
                                    )
                                    errors += 1
                                    continue
                            # elif '.mztab' in filetypes:
                            #     # parse mztab files
                            else:
                                print("No known file types found.")
                                # make sure project is not flagged as complete
                                return

                elif files["search"]:
                    # TODO: support search files
                    continue
                else:
                    logger.warning(
                        "No results/search files found for project '%s' from backend %s.",
                        project["accession"],
                        backend_enum.name,
                    )

                # TODO: set "complete" flag for project

        if imported > 0 or errors > 0:
            logger.info("Finished importing from backend %s.", backend_enum.name)
            logger.info(
                "Successfully imported %s projects, encountered %s errors (%.1f%%).",
                imported,
                errors,
                (imported / (imported + errors) * 100),
            )
            if error_projects:
                logger.warning("\nFailed Projects:")
                for accession, error in error_projects[:10]:  # Show first 10
                    logger.warning("  • %s: %s", accession, error[:80])
                if len(error_projects) > 10:
                    logger.warning("  ... and %d more", len(error_projects) - 10)
