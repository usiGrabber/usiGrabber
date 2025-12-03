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
    uv run python experimental/import_pride_json.py pride_projects_all.json
"""

import asyncio
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from time import time

from attr import dataclass
from ontology_resolver.ontology_helper import OntologyHelper
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from sqlmodel import Session, and_, or_, select

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
from usigrabber.db.schema import ProjectAffiliation, ProjectOtherOmicsLink

logger = logging.Logger(__name__)
# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


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


async def import_project(session: Session, project_data: dict):
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

    session.flush()
    await process_cv_data(session, project, project_data=project_data)


@dataclass
class RawCvParam:
    name: str
    value: str | None = None


def add_cv_params_to_project(session: Session, project: Project, all_cvs: list[RawCvParam]):
    """
    cvs: list of (name, value) tuples
    """
    if not all_cvs:
        return

    MAX_QUERY_SIZE = 200
    for i in range(0, len(all_cvs), MAX_QUERY_SIZE):
        cvs = all_cvs[i * MAX_QUERY_SIZE : (i + 1) * MAX_QUERY_SIZE]
        # Disable autoflush to avoid SQLite "database is locked"
        filters = []
        for cv in cvs:
            if cv.value is None:
                filters.append(and_(CvParam.accession == cv.name, CvParam.value.is_(None)))  # pyright: ignore[reportAttributeAccessIssue, reportOptionalMemberAccess]
            else:
                filters.append(and_(CvParam.accession == cv.name, CvParam.value == cv.value))

        statement = select(CvParam).where(or_(*filters))
        existing = session.exec(statement)
        existing_params = list(existing.all())

        # Map existing for fast lookup
        existing_map = {(cv.accession, cv.value): cv for cv in existing_params}

        # Step 3: Prepare CvParams to add
        new_cvs = []
        for cv in cvs:
            name, value = cv.name, cv.value
            key = (name, value)
            if key not in existing_map:
                cv_param = CvParam(accession=name, value=value)
                session.add(cv_param)

                existing_map[key] = cv_param
                new_cvs.append(cv_param)
        session.flush()  # Assigns ID

        # Step 4: Attach all CVs to project if not already linked
        for cv_param in existing_map.values():
            if cv_param not in project.cv_params:
                project.cv_params.append(cv_param)


async def process_cv_data(session: Session, project: Project, project_data: dict):
    cv_data_keys = [
        "instruments",
        "softwares",
        "experimentTypes",
        "quantificationMethods",
        "organisms",
        "organismParts",
        "diseases",
        "identifiedPTMStrings",
    ]

    ontology_helper = OntologyHelper()
    cv_row_data: list[RawCvParam] = []
    start_time_supper_classes = time()
    for json_key in cv_data_keys:
        for cv_data in project_data.get(json_key, []):
            cv_term: str | None = None
            cv_term_value = None
            supperclass_cv_terms: list[str] = []

            if cv_data.get("@type") == "Tuple":
                pass
            elif cv_data.get("@type") == "CvParam":
                cv_term = cv_data.get("accession")
                assert isinstance(cv_term, str)
                try:
                    superclasses = await ontology_helper.get_superclasses(cv_term)
                    supperclass_cv_terms = [x.id for x in superclasses[1:]]
                    cv_term_value = cv_data.get("value", None)
                except Exception as e:
                    logger.error(f"Failed to resolve super classes for term {cv_term}:", e)

            if cv_term is not None:
                cv_row_data.append(RawCvParam(name=cv_term, value=cv_term_value))
                for x in supperclass_cv_terms:
                    cv_row_data.append(RawCvParam(name=x))

    print(f"Resolved supperclasses in {time() - start_time_supper_classes}s")
    start_time_db = time()
    add_cv_params_to_project(session, project, cv_row_data)
    print(f"Wrote to db in {time() - start_time_db}s")
    session.flush()


async def import_pride_json(json_file: str, batch_size: int = 1):
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
                await import_project(session, project_data)
                imported += 1

                # Commit in batches
                if (i + 1) % batch_size == 0:
                    session.commit()
                    description = (
                        f"Importing projects... {i + 1}/{total_projects} (✓ {imported}, ✗ {errors})"
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
    asyncio.run(import_pride_json("experimental/pride_projects_all.json"))
