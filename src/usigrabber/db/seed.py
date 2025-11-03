"""Seed database with minimal sample data for development."""

from datetime import date

from sqlalchemy.engine.base import Engine
from sqlmodel import Session

from usigrabber.db.schema import (
	Project,
	ProjectCountry,
	ProjectKeyword,
	ProjectTag,
	Reference,
)


def seed_minimal_data(engine: Engine) -> None:
	"""
	Seed database with minimal sample PRIDE project data.

	Creates:
	- 2 projects with relationships
	- 2 references
	- Keywords, tags, countries
	"""

	with Session(engine) as session:
		# 1. Create Projects
		project1 = Project(
			accession="PXD000001",
			title="Proteomics Analysis of Human Cancer Cell Lines",
			projectDescription="Comprehensive proteomics study of human cancer ...",
			sampleProcessingProtocol="Cells lysed, proteins extracted and ...",
			dataProcessingProtocol="Data analyzed with MaxQuant and Perseus.",
			submissionType="COMPLETE",
			submissionDate=date(2023, 1, 15),
			publicationDate=date(2023, 6, 1),
			totalFileDownloads=523,
		)

		project2 = Project(
			accession="PXD000002",
			title="Mouse Brain Development Proteome",
			projectDescription="Temporal proteomics analysis of mouse brain ...",
			submissionType="PARTIAL",
			submissionDate=date(2023, 3, 10),
			publicationDate=date(2023, 8, 15),
			totalFileDownloads=187,
		)

		session.add_all([project1, project2])
		session.flush()

		# 3. Create References
		references = [
			Reference(
				project_accession="PXD000001",
				referenceLine="Smith J, et al. Proteomics of cancer cells. Nature.",
				pubmedID=12345678,
				doi="10.1038/nature.2023.001",
			),
			Reference(
				project_accession="PXD000002",
				referenceLine="Doe J, et al. Brain development proteome. Cell. 2023",
				pubmedID=87654321,
				doi="10.1016/cell.2023.002",
			),
		]
		session.add_all(references)

		# 4. Create Keywords
		keywords = [
			ProjectKeyword(project_accession="PXD000001", keyword="cancer"),
			ProjectKeyword(project_accession="PXD000001", keyword="proteomics"),
			ProjectKeyword(project_accession="PXD000001", keyword="human"),
			ProjectKeyword(project_accession="PXD000002", keyword="brain"),
			ProjectKeyword(project_accession="PXD000002", keyword="development"),
			ProjectKeyword(project_accession="PXD000002", keyword="mouse"),
		]
		session.add_all(keywords)

		# 5. Create Tags
		tags = [
			ProjectTag(project_accession="PXD000001", tag="Biological"),
			ProjectTag(project_accession="PXD000001", tag="Medical"),
			ProjectTag(project_accession="PXD000002", tag="Biological"),
		]
		session.add_all(tags)

		# 6. Create Countries
		countries = [
			ProjectCountry(project_accession="PXD000001", country="United States"),
			ProjectCountry(project_accession="PXD000001", country="United Kingdom"),
			ProjectCountry(project_accession="PXD000002", country="United Kingdom"),
			ProjectCountry(project_accession="PXD000002", country="Spain"),
		]
		session.add_all(countries)

		session.commit()


if __name__ == "__main__":
	from usigrabber.db import create_db_and_tables, load_db_engine

	print("Creating database and seeding with sample data...")
	engine = load_db_engine()
	create_db_and_tables(engine)
	seed_minimal_data(engine)
	print("✅ Database seeded successfully!")
