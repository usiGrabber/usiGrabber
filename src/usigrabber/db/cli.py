"""CLI commands for database management."""

from urllib.parse import urlparse

import typer
from rich.console import Console
from rich.table import Table
from sqlalchemy import func, inspect, select
from sqlalchemy.orm import Session

from usigrabber.db import (
    CvParam,
    MzidFile,
    PeptideEvidence,
    PeptideSpectrumMatch,
    Project,
    ProjectCountry,
    ProjectKeyword,
    ProjectTag,
    PSMPeptideEvidence,
    Reference,
    create_db_and_tables,
    load_db_engine,
    seed_minimal_data,
)
from usigrabber.db.schema import Base, Modification, ModifiedPeptide

app = typer.Typer(help="Database management commands")
console = Console()


@app.command()
def init(echo_sql: bool = False):
    """
    Initialize database by creating all tables.

    Example: python -m usigrabber.db.cli init
    """
    console.print("Initializing database...", style="bold blue")

    engine = load_db_engine(debug_sql=echo_sql)
    create_db_and_tables(engine)

    # Count tables created
    inspector = inspect(engine)
    table_count = len(inspector.get_table_names())

    console.print(f"Created {table_count} tables successfully!", style="bold green")


@app.command()
def seed(echo_sql: bool = False):
    """
    Seed database with minimal sample data.

    Creates sample projects, contacts, and relationships for development.

    Example: python -m usigrabber.db.cli seed
    """
    console.print("Seeding database with sample data...", style="bold blue")

    engine = load_db_engine(debug_sql=echo_sql)

    # Check if tables exist
    inspector = inspect(engine)
    if len(inspector.get_table_names()) == 0:
        console.print(
            "Warning: No tables found. Run 'init' first or use 'reset'.", style="bold yellow"
        )
        raise typer.Exit(1)

    seed_minimal_data(engine)
    console.print("Database seeded successfully!", style="bold green")


@app.command()
def reset(
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation prompt"),
    echo_sql: bool = False,
):
    """
    Reset database: drop all tables, recreate, and seed with sample data.

    Example: python -m usigrabber.db.cli reset --force
    """
    if not force:
        confirm = typer.confirm("Warning: This will DELETE all data. Continue?")
        if not confirm:
            console.print("Reset cancelled.", style="yellow")
            raise typer.Exit(0)

    console.print("Resetting database...", style="bold blue")

    engine = load_db_engine(debug_sql=echo_sql)

    console.print("  - Dropping all tables...")
    Base.metadata.drop_all(engine)

    # Recreate tables
    console.print("  - Creating tables...")
    create_db_and_tables(engine)

    console.print("Database reset complete!", style="bold green")


@app.command()
def info(echo_sql: bool = False):
    """
    Display database information and statistics.

    Shows table counts, record counts, and database location.

    Example: python -m usigrabber.db.cli info
    """
    engine = load_db_engine(debug_sql=echo_sql)

    console.print("\nDatabase Information", style="bold blue")

    db_url = str(engine.url)
    # PostgreSQL: show connection info
    parsed = urlparse(db_url)
    console.print("Type: PostgreSQL")
    console.print(f"Host: {parsed.hostname}")
    console.print(f"Port: {parsed.port}")
    console.print(f"Database: {parsed.path.lstrip('/')}")
    console.print(f"User: {parsed.username}")

    # Check if tables exist
    inspector = inspect(engine)
    table_names = inspector.get_table_names()

    if len(table_names) == 0:
        console.print("\nWarning: No tables found. Run 'init' to create schema.", style="yellow")
        raise typer.Exit(0)

    console.print(f"\nTotal tables: {len(table_names)}")

    # Get record counts for main tables
    with Session(engine) as session:
        counts = {
            "Projects": session.execute(select(func.count()).select_from(Project)).scalar_one(),
            "References": session.execute(select(func.count()).select_from(Reference)).scalar_one(),
            "Keywords": session.execute(
                select(func.count()).select_from(ProjectKeyword)
            ).scalar_one(),
            "Tags": session.execute(select(func.count()).select_from(ProjectTag)).scalar_one(),
            "Countries": session.execute(
                select(func.count()).select_from(ProjectCountry)
            ).scalar_one(),
            "MzID Files": session.execute(select(func.count()).select_from(MzidFile)).scalar_one(),
            "PSMs": session.execute(
                select(func.count()).select_from(PeptideSpectrumMatch)
            ).scalar_one(),
            "Modified Peptides": session.execute(
                select(func.count()).select_from(ModifiedPeptide)
            ).scalar_one(),
            "Modifications": session.execute(
                select(func.count()).select_from(Modification)
            ).scalar_one(),
            "Peptide Evidence": session.execute(
                select(func.count()).select_from(PeptideEvidence)
            ).scalar_one(),
            "PSM-Peptide Evidence Links": session.execute(
                select(func.count()).select_from(PSMPeptideEvidence)
            ).scalar_one(),
            "CV Parameters": session.execute(
                select(func.count()).select_from(CvParam)
            ).scalar_one(),
        }

    # Create table
    table = Table(title="\nRecord Counts")
    table.add_column("Table", style="cyan", no_wrap=True)
    table.add_column("Count", style="magenta", justify="right")

    for table_name, count in counts.items():
        table.add_row(table_name, str(count))

    console.print(table)

    # Show sample project if available
    with Session(engine) as session:
        project = session.execute(select(Project).limit(1)).scalar_one_or_none()
        if project:
            console.print("\nSample Project:", style="bold")
            console.print(f"  - {project.accession}: {project.title}")
            console.print(f"  - Submission: {project.submission_date}")
            console.print(f"  - Downloads: {project.total_file_downloads}")


@app.command()
def drop(
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation prompt"),
    echo_sql: bool = False,
):
    """
    Drop all database tables (WARNING: deletes all data).

    Example: python -m usigrabber.db.cli drop --force
    """
    if not force:
        confirm = typer.confirm("Warning: This will DELETE all tables and data. Continue?")
        if not confirm:
            console.print("Drop cancelled.", style="yellow")
            raise typer.Exit(0)

    console.print("Dropping all tables...", style="bold red")

    engine = load_db_engine(debug_sql=echo_sql)

    Base.metadata.drop_all(engine)

    console.print("All tables dropped.", style="bold green")


if __name__ == "__main__":
    app()
