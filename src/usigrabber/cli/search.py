from typing import Annotated

import typer

from usigrabber.cli import app


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
