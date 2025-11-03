"""CLI commands for database management."""

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table
from sqlalchemy import inspect
from sqlmodel import Session, select

from usigrabber.db import (
	Project,
	ProjectCountry,
	ProjectKeyword,
	ProjectTag,
	Reference,
	create_db_and_tables,
	load_db_engine,
	seed_minimal_data,
)

app = typer.Typer(help="Database management commands")
console = Console()


@app.command()
def init(echo_sql: bool = False):
	"""
	Initialize database by creating all tables.

	Example: python -m usigrabber.db.cli init
	"""
	console.print("🗄️  Initializing database...", style="bold blue")

	engine = load_db_engine(debug_sql=echo_sql)
	create_db_and_tables(engine)

	# Count tables created
	inspector = inspect(engine)
	table_count = len(inspector.get_table_names())

	console.print(f"✅ Created {table_count} tables successfully!", style="bold green")


@app.command()
def seed(echo_sql: bool = False):
	"""
	Seed database with minimal sample data.

	Creates sample projects, contacts, and relationships for development.

	Example: python -m usigrabber.db.cli seed
	"""
	console.print("🌱 Seeding database with sample data...", style="bold blue")

	engine = load_db_engine(debug_sql=echo_sql)

	# Check if tables exist
	inspector = inspect(engine)
	if len(inspector.get_table_names()) == 0:
		console.print("⚠️  No tables found. Run 'init' first or use 'reset'.", style="bold yellow")
		raise typer.Exit(1)

	seed_minimal_data(engine)
	console.print("✅ Database seeded successfully!", style="bold green")


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
		confirm = typer.confirm("⚠️  This will DELETE all data. Continue?")
		if not confirm:
			console.print("❌ Reset cancelled.", style="yellow")
			raise typer.Exit(0)

	console.print("🔄 Resetting database...", style="bold blue")

	engine = load_db_engine(debug_sql=echo_sql)

	# Drop all tables
	from sqlmodel import SQLModel

	console.print("  - Dropping all tables...")
	SQLModel.metadata.drop_all(engine)

	# Recreate tables
	console.print("  - Creating tables...")
	create_db_and_tables(engine)

	# Seed data
	console.print("  - Seeding sample data...")
	seed_minimal_data(engine)

	console.print("✅ Database reset complete!", style="bold green")


@app.command()
def info(echo_sql: bool = False):
	"""
	Display database information and statistics.

	Shows table counts, record counts, and database location.

	Example: python -m usigrabber.db.cli info
	"""
	engine = load_db_engine(debug_sql=echo_sql)

	# Get database path
	db_path = str(engine.url).replace("sqlite:///", "")
	db_exists = Path(db_path).exists()

	console.print("\n📊 Database Information", style="bold blue")
	console.print(f"Location: {db_path}")
	console.print(f"Exists: {'✅ Yes' if db_exists else '❌ No'}")

	if not db_exists:
		console.print(
			"\n⚠️  Database file not found. Run 'init' to create it.",
			style="yellow",
		)
		raise typer.Exit(0)

	# Get file size
	file_size_bytes = Path(db_path).stat().st_size
	file_size_mb = file_size_bytes / (1024 * 1024)
	console.print(f"Size: {file_size_mb:.2f} MB")

	# Check if tables exist
	inspector = inspect(engine)
	table_names = inspector.get_table_names()

	if len(table_names) == 0:
		console.print("\n⚠️  No tables found. Run 'init' to create schema.", style="yellow")
		raise typer.Exit(0)

	console.print(f"\nTotal tables: {len(table_names)}")

	# Get record counts for main tables
	with Session(engine) as session:
		from sqlalchemy import func

		counts = {
			"Projects": session.exec(select(func.count()).select_from(Project)).one(),
			"References": session.exec(select(func.count()).select_from(Reference)).one(),
			"Keywords": session.exec(select(func.count()).select_from(ProjectKeyword)).one(),
			"Tags": session.exec(select(func.count()).select_from(ProjectTag)).one(),
			"Countries": session.exec(select(func.count()).select_from(ProjectCountry)).one(),
		}

	# Create table
	table = Table(title="\n📋 Record Counts")
	table.add_column("Table", style="cyan", no_wrap=True)
	table.add_column("Count", style="magenta", justify="right")

	for table_name, count in counts.items():
		table.add_row(table_name, str(count))

	console.print(table)

	# Show sample project if available
	with Session(engine) as session:
		project = session.exec(select(Project).limit(1)).first()
		if project:
			console.print("\n🔬 Sample Project:", style="bold")
			console.print(f"  • {project.accession}: {project.title}")
			console.print(f"  • Submission: {project.submission_date}")
			console.print(f"  • Downloads: {project.total_file_downloads}")


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
		confirm = typer.confirm("⚠️  This will DELETE all tables and data. Continue?")
		if not confirm:
			console.print("❌ Drop cancelled.", style="yellow")
			raise typer.Exit(0)

	console.print("🗑️  Dropping all tables...", style="bold red")

	engine = load_db_engine(debug_sql=echo_sql)

	from sqlmodel import SQLModel

	SQLModel.metadata.drop_all(engine)

	console.print("✅ All tables dropped.", style="bold green")


if __name__ == "__main__":
	app()
