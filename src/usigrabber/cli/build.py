import asyncio
import logging
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.progress import Progress, SpinnerColumn, TextColumn
from sqlalchemy import inspect
from sqlmodel import Session, select

from usigrabber.backends import BackendEnum
from usigrabber.cli import app
from usigrabber.db import Project, create_db_and_tables, load_db_engine
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
        statement = select(Project.accession)
        accessions: Sequence[str] = session.exec(statement).all()

    logger.info(f"Found {len(accessions)} existing accessions in the database.")

    for backend_enum in backends:
        backend = backend_enum.value
        logger.info(f"Fetching data from backend: {backend_enum.name}")

        backend_accessions = backend.get_all_project_accessions()

        # filter accessions to only new ones
        new_accessions: list[str] = []
        for accession in backend_accessions:
            if accession not in accessions:
                new_accessions.append(accession)

        len_new_accessions = len(new_accessions)
        logger.info(f"Found {len_new_accessions} new accessions from backend {backend_enum.name}.")

        imported = errors = 0
        completed = imported + errors
        error_projects = []
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
        ) as progress:
            task = progress.add_task(
                f"Importing projects... {completed}/{len_new_accessions}",
                completed=0,
                total=len_new_accessions,
            )
            with Session(db_engine) as session:
                for accession in new_accessions:
                    metadata: dict[str, Any] = backend.get_metadata_for_project(accession)

                    # TODO: support other submission types
                    if metadata.get("submissionType") != "COMPLETE":
                        continue

                    try:
                        await backend.dump_project_to_db(session, metadata)
                    except Exception as e:
                        errors += 1
                        error_projects.append((metadata.get("accession"), str(e)))
                        session.rollback()

                    session.commit()
                    imported += 1
                    completed = imported + errors
                    progress.update(
                        task,
                        description=f"Importing projects... {completed}/{len_new_accessions}",
                        completed=imported + errors,
                    )

                    # download files
                    files = backend.get_files_for_project(accession)

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
                            file_name, ext = filename.split(".", maxsplit=1)

                            with temporary_path() as tmp_dir:
                                # download file
                                # TODO: make async throughout
                                path = await download_ftp(
                                    file_url, out_dir=tmp_dir, file_name=filename
                                )

                                # optional: extract if archived
                                if ext in {".gz", ".zip", ".tar"}:
                                    extract_archive(path, extract_to=tmp_dir)
                                    path = tmp_dir / (file_name + ".mzid")  # assume mzid inside

                                assert path.exists(), (
                                    f"Expected extracted file {path} does not exist."
                                )

                                # process file
                                # TODO: implement interface
                                # mzid_data = mzid_parser.handle(project, path)
                                # dump_mzid_to_db(session, project.accession, mzid_data)

                    elif files["search"]:
                        # TODO: support search files
                        continue
                    else:
                        logger.warning(
                            "No results/search files found for accession %s from backend %s.",
                            accession,
                            backend_enum.name,
                        )

                    # TODO: set "complete" flag for project

        if new_accessions:
            logger.info(
                f"Finished importing from backend {backend_enum.name}. "
                + f"\nSuccessfully imported {imported} projects, "
                + f"encountered {errors} errors."
                + f"Success rate: {(imported / len(new_accessions) * 100):.1f}%"
            )
            if error_projects:
                logger.info("\nFailed Projects:")
                for accession, error in error_projects[:10]:  # Show first 10
                    logger.info(f"  • {accession}: {error[:80]}")
                if len(error_projects) > 10:
                    logger.info(f"  ... and {len(error_projects) - 10} more")
