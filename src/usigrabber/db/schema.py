from datetime import date

from sqlmodel import JSON, Column, Field, Relationship, SQLModel

# ============================================================================
# Core Tables
# ============================================================================


class Project(SQLModel, table=True):
	"""Main project table storing PRIDE project information."""

	__tablename__ = "projects"

	accession: str = Field(primary_key=True, index=True)
	title: str
	project_description: str | None = Field(default=None, alias="projectDescription")
	sample_processing_protocol: str | None = Field(default=None, alias="sampleProcessingProtocol")
	data_processing_protocol: str | None = Field(default=None, alias="dataProcessingProtocol")
	doi: str | None = None
	submission_type: str = Field(alias="submissionType")
	license: str | None = None
	submission_date: date | None = Field(default=None, alias="submissionDate")
	publication_date: date | None = Field(default=None, alias="publicationDate")
	total_file_downloads: int = Field(default=0, alias="totalFileDownloads")

	# Complex nested data stored as JSON
	sample_attributes: dict | None = Field(
		default=None, sa_column=Column(JSON), alias="sampleAttributes"
	)
	additional_attributes: dict | None = Field(
		default=None, sa_column=Column(JSON), alias="additionalAttributes"
	)

	# Relationships
	references: list["Reference"] = Relationship(back_populates="project")
	keywords: list["ProjectKeyword"] = Relationship(back_populates="project")
	project_tags: list["ProjectTag"] = Relationship(back_populates="project")
	countries: list["ProjectCountry"] = Relationship(back_populates="project")
	affiliations: list["ProjectAffiliation"] = Relationship(back_populates="project")
	other_omics_links: list["ProjectOtherOmicsLink"] = Relationship(back_populates="project")


class Reference(SQLModel, table=True):
	"""Publication references for projects."""

	__tablename__ = "references"

	id: int | None = Field(default=None, primary_key=True)
	project_accession: str = Field(foreign_key="projects.accession", index=True)
	reference_line: str | None = Field(default=None, alias="referenceLine")
	pubmed_id: int | None = Field(default=None, alias="pubmedID")
	doi: str | None = None

	# Relationships
	project: Project | None = Relationship(back_populates="references")


# ============================================================================
# Simple Array Tables
# ============================================================================


class ProjectKeyword(SQLModel, table=True):
	"""Project keywords."""

	__tablename__ = "project_keywords"

	id: int | None = Field(default=None, primary_key=True)
	project_accession: str = Field(foreign_key="projects.accession", index=True)
	keyword: str

	# Relationships
	project: Project | None = Relationship(back_populates="keywords")


class ProjectTag(SQLModel, table=True):
	"""Project tags."""

	__tablename__ = "project_tags"

	id: int | None = Field(default=None, primary_key=True)
	project_accession: str = Field(foreign_key="projects.accession", index=True)
	tag: str

	# Relationships
	project: Project | None = Relationship(back_populates="project_tags")


class ProjectCountry(SQLModel, table=True):
	"""Project countries."""

	__tablename__ = "project_countries"

	id: int | None = Field(default=None, primary_key=True)
	project_accession: str = Field(foreign_key="projects.accession", index=True)
	country: str

	# Relationships
	project: Project | None = Relationship(back_populates="countries")


class ProjectAffiliation(SQLModel, table=True):
	"""Project affiliations."""

	__tablename__ = "project_affiliations"

	id: int | None = Field(default=None, primary_key=True)
	project_accession: str = Field(foreign_key="projects.accession", index=True)
	affiliation: str

	# Relationships
	project: Project | None = Relationship(back_populates="affiliations")


class ProjectOtherOmicsLink(SQLModel, table=True):
	"""Project other omics links."""

	__tablename__ = "project_other_omics_links"

	id: int | None = Field(default=None, primary_key=True)
	project_accession: str = Field(foreign_key="projects.accession", index=True)
	link: str

	# Relationships
	project: Project | None = Relationship(back_populates="other_omics_links")


# ============================================================================
# Database Initialization
# ============================================================================


def create_db_and_tables(engine):
	"""Create all database tables."""
	SQLModel.metadata.create_all(engine)
