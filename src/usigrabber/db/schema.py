from datetime import date

from sqlmodel import JSON, Column, Field, Relationship, SQLModel

# ============================================================================
# Junction Tables (Many-to-Many)
# ============================================================================


# class ProjectInstrument(SQLModel, table=True):
# 	"""Junction table for project instruments."""

# 	__tablename__ = "project_instruments"

# 	project_accession: str = Field(foreign_key="projects.accession", primary_key=True, index=True)
# 	cv_param_id: int = Field(foreign_key="cv_params.id", primary_key=True, index=True)


# class ProjectSoftware(SQLModel, table=True):
# 	"""Junction table for project software."""

# 	__tablename__ = "project_softwares"

# 	project_accession: str = Field(foreign_key="projects.accession", primary_key=True, index=True)
# 	cv_param_id: int = Field(foreign_key="cv_params.id", primary_key=True, index=True)


# class ProjectExperimentType(SQLModel, table=True):
# 	"""Junction table for project experiment types."""

# 	__tablename__ = "project_experiment_types"

# 	project_accession: str = Field(foreign_key="projects.accession", primary_key=True, index=True)
# 	cv_param_id: int = Field(foreign_key="cv_params.id", primary_key=True, index=True)


# class ProjectQuantificationMethod(SQLModel, table=True):
# 	"""Junction table for project quantification methods."""

# 	__tablename__ = "project_quantification_methods"

# 	project_accession: str = Field(foreign_key="projects.accession", primary_key=True, index=True)
# 	cv_param_id: int = Field(foreign_key="cv_params.id", primary_key=True, index=True)


# class ProjectOrganism(SQLModel, table=True):
# 	"""Junction table for project organisms."""

# 	__tablename__ = "project_organisms"

# 	project_accession: str = Field(foreign_key="projects.accession", primary_key=True, index=True)
# 	cv_param_id: int = Field(foreign_key="cv_params.id", primary_key=True, index=True)


# class ProjectOrganismPart(SQLModel, table=True):
# 	"""Junction table for project organism parts."""

# 	__tablename__ = "project_organism_parts"

# 	project_accession: str = Field(foreign_key="projects.accession", primary_key=True, index=True)
# 	cv_param_id: int = Field(foreign_key="cv_params.id", primary_key=True, index=True)


# class ProjectDisease(SQLModel, table=True):
# 	"""Junction table for project diseases."""

# 	__tablename__ = "project_diseases"

# 	project_accession: str = Field(foreign_key="projects.accession", primary_key=True, index=True)
# 	cv_param_id: int = Field(foreign_key="cv_params.id", primary_key=True, index=True)


# class ProjectIdentifiedPTM(SQLModel, table=True):
# 	"""Junction table for project identified PTMs."""

# 	__tablename__ = "project_identified_ptms"

# 	project_accession: str = Field(foreign_key="projects.accession", primary_key=True, index=True)
# 	cv_param_id: int = Field(foreign_key="cv_params.id", primary_key=True, index=True)


# ============================================================================
# Core Tables
# ============================================================================


class CvJunctionTable(SQLModel, table=True):
	__tablename__ = "project_cv_params"

	cv_param_id: int = Field(foreign_key="cv_params.id", primary_key=True, index=True)
	project_accession: str = Field(foreign_key="projects.accession", primary_key=True, index=True)


class CvParam(SQLModel, table=True):
	__tablename__ = "cv_params"

	id: int | None = Field(default=None, primary_key=True)
	name: str
	value: str | None = Field(default=None)

	projects: list["Project"] = Relationship(back_populates="cv_params", link_model=CvJunctionTable)


# class CvParam(SQLModel, table=True):
# 	"""Controlled vocabulary parameter (for instruments, organisms, diseases, etc.)."""

# 	__tablename__ = "cv_params"

# 	id: int | None = Field(default=None, primary_key=True)
# 	accession: str = Field(index=True)
# 	cv_label: str | None = Field(default=None, alias="cvLabel")
# 	name: str
# 	value: str | None = None
# 	param_type: str = Field(index=True)  # 'instrument', 'software', 'organism', 'disease', etc.

# 	# Relationships
# 	projects_as_instrument: list["Project"] = Relationship(
# 		back_populates="instruments",
# 		link_model=ProjectInstrument,
# 	)
# 	projects_as_software: list["Project"] = Relationship(
# 		back_populates="softwares",
# 		link_model=ProjectSoftware,
# 	)
# 	projects_as_experiment_type: list["Project"] = Relationship(
# 		back_populates="experiment_types",
# 		link_model=ProjectExperimentType,
# 	)
# 	projects_as_quantification_method: list["Project"] = Relationship(
# 		back_populates="quantification_methods",
# 		link_model=ProjectQuantificationMethod,
# 	)
# 	projects_as_organism: list["Project"] = Relationship(
# 		back_populates="organisms",
# 		link_model=ProjectOrganism,
# 	)
# 	projects_as_organism_part: list["Project"] = Relationship(
# 		back_populates="organism_parts",
# 		link_model=ProjectOrganismPart,
# 	)
# 	projects_as_disease: list["Project"] = Relationship(
# 		back_populates="diseases",
# 		link_model=ProjectDisease,
# 	)
# 	projects_as_identified_ptm: list["Project"] = Relationship(
# 		back_populates="identified_ptms",
# 		link_model=ProjectIdentifiedPTM,
# 	)


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

	cv_params: list[CvParam] = Relationship(back_populates="projects", link_model=CvJunctionTable)

	# Many-to-many relationships with CvParam
	# instruments: list[CvParam] = Relationship(
	# 	back_populates="projects_as_instrument",
	# 	link_model=ProjectInstrument,
	# )
	# softwares: list[CvParam] = Relationship(
	# 	back_populates="projects_as_software",
	# 	link_model=ProjectSoftware,
	# )
	# experiment_types: list[CvParam] = Relationship(
	# 	back_populates="projects_as_experiment_type",
	# 	link_model=ProjectExperimentType,
	# )
	# quantification_methods: list[CvParam] = Relationship(
	# 	back_populates="projects_as_quantification_method",
	# 	link_model=ProjectQuantificationMethod,
	# )
	# organisms: list[CvParam] = Relationship(
	# 	back_populates="projects_as_organism",
	# 	link_model=ProjectOrganism,
	# )
	# organism_parts: list[CvParam] = Relationship(
	# 	back_populates="projects_as_organism_part",
	# 	link_model=ProjectOrganismPart,
	# )
	# diseases: list[CvParam] = Relationship(
	# 	back_populates="projects_as_disease",
	# 	link_model=ProjectDisease,
	# )
	# identified_ptms: list[CvParam] = Relationship(
	# 	back_populates="projects_as_identified_ptm",
	# 	link_model=ProjectIdentifiedPTM,
	# )


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
