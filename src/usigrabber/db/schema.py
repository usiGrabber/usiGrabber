from datetime import date, datetime

from sqlmodel import JSON, Column, Field, Relationship, SQLModel

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
    mzid_files: list["MzidFile"] = Relationship(back_populates="project")
    peptide_spectrum_matches: list["PeptideSpectrumMatch"] = Relationship(back_populates="project")

    cv_params: list[CvParam] = Relationship(back_populates="projects", link_model=CvJunctionTable)


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
# mzID Data Tables
# ============================================================================


class MzidFile(SQLModel, table=True):
    """Metadata per mzID file - OPTIONAL table for provenance tracking."""

    __tablename__ = "mzid_files"

    id: int | None = Field(default=None, primary_key=True)
    project_accession: str = Field(foreign_key="projects.accession", index=True)
    file_name: str
    file_path: str | None = None
    software_name: str | None = None
    software_version: str | None = None
    search_database_name: str | None = None
    protocol_parameters: dict | None = Field(
        default=None,
        sa_column=Column(JSON),
        description="Enzyme, tolerances, modifications - stored as JSON",
    )
    threshold_type: str | None = Field(
        default=None,
        index=True,
        description="Type of threshold used (e.g., 'FDR', 'q-value', 'e-value', 'Mascot:identity')",
    )
    threshold_value: float | None = Field(
        default=None,
        index=True,
        description="Threshold value (e.g., 0.01 for 1% FDR)",
    )
    creation_date: datetime | None = None

    # Relationships
    project: Project | None = Relationship(back_populates="mzid_files")
    peptide_spectrum_matches: list["PeptideSpectrumMatch"] = Relationship(
        back_populates="mzid_file"
    )


class Peptide(SQLModel, table=True):
    """Peptide sequences."""

    __tablename__ = "peptides"

    id: int | None = Field(default=None, primary_key=True)
    sequence: str = Field(index=True, description="Peptide sequence")
    length: int = Field(description="Computed sequence length")

    # Relationships
    peptide_spectrum_matches: list["PeptideSpectrumMatch"] = Relationship(back_populates="peptide")
    peptide_modifications: list["PeptideModification"] = Relationship(back_populates="peptide")


class PeptideModification(SQLModel, table=True):
    """Junction table: which modifications occur at which positions in each peptide."""

    __tablename__ = "peptide_modifications"

    id: int | None = Field(default=None, primary_key=True)
    peptide_id: int = Field(foreign_key="peptides.id", index=True)
    unimod_id: int = Field(description="Unimod id, e.g., '35' for 'UNIMOD:35' accession")
    position: int = Field(description="Position in the peptide sequence (1-indexed)")
    modified_residue: str = Field(description="The specific amino acid that was modified")

    # Relationships
    peptide: Peptide | None = Relationship(back_populates="peptide_modifications")


class PeptideSpectrumMatch(SQLModel, table=True):
    """Core PSM data. Links to Project, MzidFile, Peptide."""

    __tablename__ = "peptide_spectrum_matches"

    id: int | None = Field(default=None, primary_key=True)
    project_accession: str = Field(foreign_key="projects.accession", index=True)
    mzid_file_id: int | None = Field(
        default=None,
        foreign_key="mzid_files.id",
        index=True,
        description="Optional: can be NULL for non-mzID sources",
    )
    peptide_id: int = Field(foreign_key="peptides.id", index=True)
    spectrum_id: str = Field(index=True, description="Spectrum identifier/index")
    charge_state: int
    experimental_mz: float = Field(description="Experimental m/z value")
    calculated_mz: float = Field(description="Calculated m/z value")
    score_values: dict | None = Field(
        default=None,
        sa_column=Column(JSON),
        description="MS-GF+ score, FDR, e-value, etc. as JSON",
    )
    rank: int | None = Field(default=None, description="Rank of this PSM for the spectrum")
    pass_threshold: bool = Field(
        description=(
            "Whether PSM passes quality threshold. Based on file-level threshold "
            "(see mzid_file.threshold_type and threshold_value) or per-spectrum "
            "dynamic thresholds (e.g., Mascot identity/homology). Value comes from "
            "mzID passThreshold attribute."
        )
    )

    # Relationships
    project: Project | None = Relationship(back_populates="peptide_spectrum_matches")
    mzid_file: MzidFile | None = Relationship(back_populates="peptide_spectrum_matches")
    peptide: Peptide | None = Relationship(back_populates="peptide_spectrum_matches")
    psm_peptide_evidences: list["PSMPeptideEvidence"] = Relationship(back_populates="psm")


class PeptideEvidence(SQLModel, table=True):
    """Peptide-to-protein mappings - where peptides appear in proteins."""

    __tablename__ = "peptide_evidence"

    id: int | None = Field(default=None, primary_key=True)
    protein_accession: str | None = Field(default=None, description="Protein accession.")
    is_decoy: bool = Field(default=False, description="Whether the protein is a decoy")
    start_position: int | None = Field(
        default=None, description="Start position in protein sequence"
    )
    end_position: int | None = Field(default=None, description="End position in protein sequence")
    pre_residue: str | None = Field(
        default=None, max_length=1, description="Flanking amino acid before"
    )
    post_residue: str | None = Field(
        default=None, max_length=1, description="Flanking amino acid after"
    )

    # Relationships
    psm_peptide_evidences: list["PSMPeptideEvidence"] = Relationship(
        back_populates="peptide_evidence"
    )


class PSMPeptideEvidence(SQLModel, table=True):
    """Junction table linking PSMs to their protein evidence.

    In mzID files, PeptideEvidenceRef elements appear inside SpectrumIdentificationItem
    (PSM) elements, establishing a many-to-many relationship. This allows:
    - One PSM to map to multiple proteins (shared peptides)
    - One protein mapping to be referenced by multiple PSMs
    """

    __tablename__ = "psm_peptide_evidence"

    id: int | None = Field(default=None, primary_key=True)
    psm_id: int = Field(foreign_key="peptide_spectrum_matches.id", index=True)
    peptide_evidence_id: int = Field(foreign_key="peptide_evidence.id", index=True)

    # Relationships
    psm: "PeptideSpectrumMatch" = Relationship(back_populates="psm_peptide_evidences")
    peptide_evidence: "PeptideEvidence" = Relationship(back_populates="psm_peptide_evidences")


# ============================================================================
# Database Initialization
# ============================================================================


def create_db_and_tables(engine):
    """Create all database tables."""
    SQLModel.metadata.create_all(engine)
