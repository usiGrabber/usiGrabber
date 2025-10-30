import os
from pathlib import Path
from typing import Annotated, Any

import typer

from usigrabber.backends import BackendEnum

app = typer.Typer()


@app.command()
def search(
    sql_string: Annotated[
        str | None, typer.Argument(help="SQL query string to search the USI database.")
    ] = None,
    sql_file: Annotated[
        typer.FileText | None,
        typer.Option(help="SQL file which includes the search query."),
    ] = None,
) -> None:
    """Search USI database with SQL query."""

    if sql_string:
        pass
    elif sql_file:
        sql_string = sql_file.read()
    else:
        typer.echo("Please provide either an SQL string or a SQL file.")
        raise typer.Exit(code=1)

    # Here you would add the logic to perform the search using the SQL string.
    typer.echo(f"Searching with SQL: {sql_string}")
    raise NotImplementedError("Search functionality is not yet implemented.")


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
    ] = [enum for enum in BackendEnum],  # noqa: B006
) -> None:
    """Build USI database."""
    typer.echo("Building USI database...")
    os.environ["UG_DATA_DIR"] = str(data_dir)

    print([backend.name for backend in backends])

    # WORKFLOW

    # get all existing project accessions in the database
    accessions: list[str] = []

    # gather backends to fetch

    for backend_enum in backends:
        backend = backend_enum.value
        typer.echo(f"Fetching data from backend: {backend_enum.name}")

        backend_accessions = backend.get_all_project_accessions()

        # filter accessions to only new ones
        new_accessions = []
        for accession in backend_accessions:
            if accession not in accessions:
                # if satisfies filter criteria
                new_accessions.append(accession)

        typer.echo(
            f"Found {len(new_accessions)} new accessions from backend {backend_enum.name}."
        )

        for accession in new_accessions:
            # fetch metadata
            metadata: dict[str, Any] = backend.get_metadata_for_project(accession)

            # download files
            files: list[dict[str, Any]] = backend.get_files_for_project(accession)

            # process files

            # dump project to database


def main() -> None:
    app()


if __name__ == "__main__":
    main()
