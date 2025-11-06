import os
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Any

import typer
from sqlalchemy import inspect
from sqlmodel import Session as Session
from sqlmodel import select

from usigrabber.backends import BackendEnum
from usigrabber.db import (
    Project,
    ProjectCountry,
    ProjectKeyword,
    ProjectTag,
    Reference,
    create_db_and_tables,
    load_db_engine,
)
from usigrabber.db.schema import (
    ProjectAffiliation,
    ProjectOtherOmicsLink,
)

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
    is_test: Annotated[
        bool,
        typer.Option(help="Run in test mode with limited data."),
    ] = False,
) -> None:
    """Build USI database."""
    typer.echo("Building database.")
    os.environ["UG_DATA_DIR"] = str(data_dir)

    # WORKFLOW
    # i want to get all accessions from the project table from the database
    db_engine = load_db_engine()
    inspector = inspect(db_engine)
    if len(inspector.get_table_names()) == 0:
        typer.echo("No preexisting database found. Database will be initialized.")
        create_db_and_tables(db_engine)

    # get all existing project accessions in the database
    with Session(db_engine) as session:
        statement = select(Project.accession)
        accessions = session.exec(statement).all()
    # gather backends to fetch

    typer.echo(f"Found {len(accessions)} existing accessions in the database.")

    for backend_enum in backends:
        backend = backend_enum.value
        typer.echo(f"Fetching data from backend: {backend_enum.name}")

        backend_accessions = backend.get_all_project_accessions(is_test=is_test)

        # filter accessions to only new ones
        new_accessions = []
        for accession in backend_accessions:
            if accession not in accessions:
                # if satisfies filter criteria
                new_accessions.append(accession)

        len_new_accessions = len(new_accessions)

        typer.echo(
            message=f"Found {len_new_accessions} new "
            + f"accessions from backend {backend_enum.name}."
        )

        from rich.progress import Progress, SpinnerColumn, TextColumn

        imported = errors = 0
        completed = imported + errors
        error_projects = []
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=typer.echo(),
        ) as progress:
            task = progress.add_task(
                f"Importing projects... {completed}/{len_new_accessions}",
                completed=0,
                total=len_new_accessions,
            )
            for accession in new_accessions:
                with Session(db_engine) as session:
                    metadata: dict[str, Any] = backend.get_metadata_for_project(accession)
                    try:
                        import_project(session, metadata)
                    except Exception as e:
                        errors += 1
                        error_projects.append((metadata.get("accession", "unknown"), str(e)))
                        session.rollback()

                    # Now collect all files for the project and parse them into the db
                    # files = backend.get_files_for_project(accession)

                    session.commit()
                    imported += 1
                    completed = imported + errors
                    progress.update(
                        task,
                        description=f"Importing projects... {completed}/{len_new_accessions}",
                        completed=imported + errors,
                    )

        if new_accessions:
            typer.echo(
                message=f"Finished importing from backend {backend_enum.name}. "
                + f"\nSuccessfully imported {imported} projects, "
                + f"encountered {errors} errors."
                + f"Success rate: {(imported / len(new_accessions) * 100):.1f}%"
            )
            if error_projects:
                typer.echo("\nFailed Projects:")
                for accession, error in error_projects[:10]:  # Show first 10
                    typer.echo(f"  • {accession}: {error[:80]}")
                if len(error_projects) > 10:
                    typer.echo(f"  ... and {len(error_projects) - 10} more")


# to-do: move this code to a separate module
### start
def import_project(session: Session, project_data: dict[str, Any]) -> None:
    # 1. Create Project
    project = Project(
        accession=project_data["accession"],
        title=project_data["title"],
        projectDescription=project_data.get("projectDescription"),
        sampleProcessingProtocol=project_data.get("sampleProcessingProtocol"),
        dataProcessingProtocol=project_data.get("dataProcessingProtocol"),
        doi=project_data.get("doi"),
        submissionType=project_data["submissionType"],
        license=project_data.get("license"),
        submissionDate=parse_date(project_data.get("submissionDate")),
        publicationDate=parse_date(project_data.get("publicationDate")),
        totalFileDownloads=project_data.get("totalFileDownloads", 0),
        sampleAttributes=project_data.get("sampleAttributes"),
        additionalAttributes=project_data.get("additionalAttributes"),
    )
    session.add(project)

    # 2. References
    for ref_data in project_data.get("references", []):
        reference = Reference(
            project_accession=project.accession,
            referenceLine=ref_data.get("referenceLine"),
            pubmedID=ref_data.get("pubmedID"),
            doi=ref_data.get("doi"),
        )
        session.add(reference)

    # 3. Keywords
    for keyword in project_data.get("keywords", []):
        if keyword:  # Skip empty strings
            session.add(ProjectKeyword(project_accession=project.accession, keyword=keyword))

    # 4. Tags
    for tag in project_data.get("projectTags", []):
        if tag:
            session.add(ProjectTag(project_accession=project.accession, tag=tag))

    # 5. Countries
    for country in project_data.get("countries", []):
        if country:
            session.add(ProjectCountry(project_accession=project.accession, country=country))

    # 6. Affiliations
    for affiliation in project_data.get("affiliations", []):
        if affiliation:
            session.add(
                ProjectAffiliation(project_accession=project.accession, affiliation=affiliation)
            )

    # 7. Other Omics Links
    for link in project_data.get("otherOmicsLinks", []):
        if link:
            session.add(ProjectOtherOmicsLink(project_accession=project.accession, link=link))


def parse_date(date_str: str | None) -> date | None:
    """Parse date string in YYYY-MM-DD format."""
    if not date_str:
        return None
    try:
        # Handle both date-only and datetime formats
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.date()
    except (ValueError, AttributeError):
        return None


### end


def main() -> None:
    app()


if __name__ == "__main__":
    main()
