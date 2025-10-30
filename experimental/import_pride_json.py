#!/usr/bin/env python3
"""
Import PRIDE projects from pride_projects_all.json into the database.

This script processes the JSON file and imports all projects with their
relationships into the database. It handles:
- Deduplication of contacts and CV parameters
- Batch processing for performance
- Progress tracking
- Error handling

Usage:
    python scripts/import_pride_json.py pride_projects_all.json
"""

import json
import sys
from datetime import date, datetime
from pathlib import Path

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from sqlmodel import Session

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from usigrabber.db import (
	CvParam,
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
	ProjectDisease,
	ProjectExperimentType,
	ProjectIdentifiedPTM,
	ProjectInstrument,
	ProjectOrganism,
	ProjectOrganismPart,
	ProjectOtherOmicsLink,
	ProjectQuantificationMethod,
	ProjectSoftware,
)

console = Console()


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


def get_or_create_cvparam(session: Session, cv_data: dict, param_type: str, cv_cache: dict) -> int:
	"""Get or create a CV parameter, using cache to avoid duplicates."""
	accession = cv_data.get("accession", "")
	name = cv_data.get("name", "")
	cache_key = f"{param_type}:{accession}:{name}"

	# Check cache
	if cache_key in cv_cache:
		return cv_cache[cache_key]

	# Check database
	from sqlmodel import select

	statement = (
		select(CvParam)
		.where(CvParam.param_type == param_type)
		.where(CvParam.accession == accession)
		.where(CvParam.name == name)
	)
	existing = session.exec(statement).first()

	if existing:
		assert existing.id is not None
		cv_cache[cache_key] = existing.id
		return existing.id

	# Create new
	cv_param = CvParam(
		accession=accession,
		cvLabel=cv_data.get("cvLabel"),
		name=name,
		value=cv_data.get("value"),
		param_type=param_type,
	)
	session.add(cv_param)
	session.flush()
	assert cv_param.id is not None
	cv_cache[cache_key] = cv_param.id
	return cv_param.id


def import_project(session: Session, project_data: dict, cv_cache: dict):
	"""Import a single project with all its relationships."""

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

	# 8. CV Parameters (instruments, software, organisms, etc.)
	cv_mappings = [
		("instruments", "instrument", ProjectInstrument),
		("softwares", "software", ProjectSoftware),
		("experimentTypes", "experiment_type", ProjectExperimentType),
		("quantificationMethods", "quantification_method", ProjectQuantificationMethod),
		("organisms", "organism", ProjectOrganism),
		("organismParts", "organism_part", ProjectOrganismPart),
		("diseases", "disease", ProjectDisease),
		("identifiedPTMStrings", "identified_ptm", ProjectIdentifiedPTM),
	]

	for json_key, param_type, junction_class in cv_mappings:
		for cv_data in project_data.get(json_key, []):
			cv_id = get_or_create_cvparam(session, cv_data, param_type, cv_cache)
			session.add(junction_class(project_accession=project.accession, cv_param_id=cv_id))


def import_pride_json(json_file: str, batch_size: int = 100):
	"""Import PRIDE projects from JSON file into database."""

	console.print(f"\n🔬 Importing PRIDE projects from: {json_file}", style="bold blue")

	# Load JSON
	console.print("📖 Loading JSON file...")
	with open(json_file) as f:
		projects_data = json.load(f)

	total_projects = len(projects_data)
	console.print(f"✓ Found {total_projects:,} projects\n")

	# Initialize database
	engine = load_db_engine()

	# Check if database exists
	from sqlalchemy import inspect

	inspector = inspect(engine)
	if len(inspector.get_table_names()) == 0:
		console.print("🗄️  Creating database tables...", style="yellow")
		create_db_and_tables(engine)

	# Caches to avoid duplicates
	cv_cache = {}

	# Import in batches
	imported = 0
	errors = 0
	error_projects = []

	with Progress(
		SpinnerColumn(),
		TextColumn("[progress.description]{task.description}"),
		console=console,
	) as progress:
		task = progress.add_task(f"Importing projects... 0/{total_projects}", total=total_projects)

		with Session(engine) as session:
			for i, project_data in enumerate(projects_data):
				try:
					import_project(session, project_data, cv_cache)
					imported += 1

					# Commit in batches
					if (i + 1) % batch_size == 0:
						session.commit()
						description = (
							f"Importing projects... {i + 1}/{total_projects} "
							f"(✓ {imported}, ✗ {errors})"
						)
						progress.update(
							task,
							advance=batch_size,
							description=description,
						)

				except Exception as e:
					errors += 1
					error_projects.append((project_data.get("accession", "unknown"), str(e)))
					session.rollback()
					cv_cache.clear()

					# Continue with next project
					if (i + 1) % batch_size == 0:
						description = (
							f"Importing projects... {i + 1}/{total_projects} "
							f"(✓ {imported}, ✗ {errors})"
						)
						progress.update(
							task,
							advance=batch_size,
							description=description,
						)

			# Final commit
			session.commit()
			progress.update(task, completed=total_projects)

	# Summary
	console.print("\n" + "=" * 60)
	console.print("📊 Import Summary", style="bold green")
	console.print("=" * 60)
	console.print(f"✅ Successfully imported: {imported:,} projects")
	console.print(f"❌ Errors: {errors:,} projects")
	console.print(f"📈 Success rate: {(imported / total_projects * 100):.1f}%")

	if error_projects:
		console.print("\n⚠️  Failed Projects:", style="yellow")
		for accession, error in error_projects[:10]:  # Show first 10
			console.print(f"  • {accession}: {error[:80]}")
		if len(error_projects) > 10:
			console.print(f"  ... and {len(error_projects) - 10} more")

	console.print("\n✅ Import complete!", style="bold green")


if __name__ == "__main__":
	import_pride_json("experimental/pride_projects_all.json")
