import uuid
from datetime import date, datetime
from enum import Enum

import psycopg
from psycopg.adapt import Dumper
from psycopg.pq import Format
from sqlalchemy import CHAR, CheckConstraint, UniqueConstraint
from sqlmodel import JSON, Column, Field, Relationship, SQLModel

from usigrabber.utils.uuid import UUID as UUID7
from usigrabber.utils.uuid import uuid7


class UUID7Dumper(Dumper):
    # Tell psycopg this dumper produces binary data (Format.BINARY = 1)
    format = Format.BINARY

    def dump(self, obj) -> bytes:
        return obj.bytes


# since we are not yet using stdlib UUID, we need to register a custom dumper for our own class
psycopg.adapters.register_dumper(UUID7, UUID7Dumper)


class IndexType(str, Enum):
    """Type of spectrum index for USI specification."""

    scan = "scan"
    index = "index"
    nativeId = "nativeId"
    trace = "trace"


# ============================================================================
# Core Tables
# ============================================================================


class CvJunctionTable(SQLModel, table=True):
    __tablename__ = "project_cv_params"

    cv_param_id: int = Field(foreign_key="cv_params.id", primary_key=True)
    project_accession: str = Field(foreign_key="projects.accession", primary_key=True)


class CvParam(SQLModel, table=True):
    __tablename__ = "cv_params"

    id: int | None = Field(default=None, primary_key=True)
    accession: str
    value: str | None = Field(default=None)

    projects: list["Project"] = Relationship(back_populates="cv_params", link_model=CvJunctionTable)


class Project(SQLModel, table=True):
    """Main project table storing PRIDE project information."""

    __tablename__ = "projects"

    accession: str = Field(primary_key=True)
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
    fully_processed: bool = Field(
        default=False, description="Flag indicating if project is fully processed or not"
    )

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
    project_accession: str = Field(foreign_key="projects.accession")
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
    project_accession: str = Field(foreign_key="projects.accession")
    keyword: str

    # Relationships
    project: Project | None = Relationship(back_populates="keywords")


class ProjectTag(SQLModel, table=True):
    """Project tags."""

    __tablename__ = "project_tags"

    id: int | None = Field(default=None, primary_key=True)
    project_accession: str = Field(foreign_key="projects.accession")
    tag: str

    # Relationships
    project: Project | None = Relationship(back_populates="project_tags")


class ProjectCountry(SQLModel, table=True):
    """Project countries."""

    __tablename__ = "project_countries"

    id: int | None = Field(default=None, primary_key=True)
    project_accession: str = Field(foreign_key="projects.accession")
    country: str

    # Relationships
    project: Project | None = Relationship(back_populates="countries")


class ProjectAffiliation(SQLModel, table=True):
    """Project affiliations."""

    __tablename__ = "project_affiliations"

    id: int | None = Field(default=None, primary_key=True)
    project_accession: str = Field(foreign_key="projects.accession")
    affiliation: str

    # Relationships
    project: Project | None = Relationship(back_populates="affiliations")


class ProjectOtherOmicsLink(SQLModel, table=True):
    """Project other omics links."""

    __tablename__ = "project_other_omics_links"

    id: int | None = Field(default=None, primary_key=True)
    project_accession: str = Field(foreign_key="projects.accession")
    link: str

    # Relationships
    project: Project | None = Relationship(back_populates="other_omics_links")


# ============================================================================
# File specific Data Tables
# ============================================================================


class MzidFile(SQLModel, table=True):
    """Metadata per mzID file - OPTIONAL table for provenance tracking."""

    __tablename__ = "mzid_files"

    id: uuid.UUID = Field(default_factory=uuid7, primary_key=True)
    project_accession: str = Field(foreign_key="projects.accession")
    file_name: str
    file_path: str | None = None
    checksum: str = Field(
        sa_column=Column(CHAR(32), nullable=False),
        description="MD5 checksum of the mzID file",
    )
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
        description="Type of threshold used (e.g., 'FDR', 'q-value', 'e-value', 'Mascot:identity')",
    )
    threshold_value: float | None = Field(
        default=None,
        description="Threshold value (e.g., 0.01 for 1% FDR)",
    )
    creation_date: datetime | None = None

    # Relationships
    project: Project | None = Relationship(back_populates="mzid_files")
    peptide_spectrum_matches: list["PeptideSpectrumMatch"] = Relationship(
        back_populates="mzid_file"
    )


class ImportedFile(SQLModel, table=True):
    __tablename__ = "imported_files"

    id: int | None = Field(default=None, primary_key=True)
    project_accession: str
    file_id: str
    format: str

    psm_count: int | None = Field(default=None)
    start_time: datetime = Field(
        default_factory=datetime.now,
    )
    end_time: datetime | None = Field(default=None)
    is_processed_successfully: bool | None = Field(default=None)
    error_message: str | None = Field(default=None)
    traceback: str | None = Field(default=None)
    worker_pid: int
    checksum: str = Field(
        sa_column=Column(CHAR(32), nullable=False),
        description="MD5 checksum of the mzID file",
    )

    __table_args__ = (
        UniqueConstraint("file_id", "project_accession", name="unique_file_constraint"),
    )


# ============================================================================
# General Data Tables
# ============================================================================


class ModifiedPeptideModificationJunction(SQLModel, table=True):
    __tablename__ = "modified_peptide_modification_junction"

    modified_peptide_id: uuid.UUID = Field(foreign_key="modified_peptides.id", primary_key=True)
    modification_id: uuid.UUID = Field(foreign_key="modifications.id", primary_key=True)


class ModifiedPeptide(SQLModel, table=True):
    """Modified peptide sequence. Links to 0 to many Modifications."""

    __tablename__ = "modified_peptides"

    id: uuid.UUID = Field(
        primary_key=True, description="Deterministic UUID based on sequence and modifications"
    )
    peptide_sequence: str = Field(description="Peptide sequence without modifications")

    # Relationships
    peptide_spectrum_matches: list["PeptideSpectrumMatch"] = Relationship(
        back_populates="modified_peptide"
    )
    modifications: list["Modification"] = Relationship(
        back_populates="modified_peptides", link_model=ModifiedPeptideModificationJunction
    )


class Modification(SQLModel, table=True):
    """Modification with a location and modified residue."""

    __tablename__ = "modifications"

    id: uuid.UUID = Field(primary_key=True)
    unimod_id: int | None = Field(description="Unimod id, e.g., '35' for 'UNIMOD:35' accession")
    name: str | None = Field(
        description="Modification name. Used as fallback if UNIMOD id not available."
    )
    location: int | None = Field(description="Location in the peptide sequence (1-indexed)")
    modified_residue: str | None = Field(description="The specific amino acid that was modified")

    modified_peptides: list["ModifiedPeptide"] = Relationship(
        back_populates="modifications", link_model=ModifiedPeptideModificationJunction
    )

    # constraint
    __table_args__ = (
        UniqueConstraint(
            "unimod_id", "name", "location", "modified_residue", name="uix_mod_unique"
        ),
        CheckConstraint(
            "(unimod_id IS NULL) OR (name IS NULL)", name="chk_mod_name_or_unimodid_null"
        ),
    )


class PeptideSpectrumMatch(SQLModel, table=True):
    """Core PSM data. Links to Project, MzidFile, Peptide."""

    __tablename__ = "peptide_spectrum_matches"

    id: uuid.UUID = Field(default_factory=uuid7, primary_key=True)
    project_accession: str = Field(foreign_key="projects.accession")
    mzid_file_id: uuid.UUID | None = Field(
        default=None,
        foreign_key="mzid_files.id",
        description="Optional: can be NULL for non-mzID sources",
    )
    modified_peptide_id: uuid.UUID = Field(foreign_key="modified_peptides.id")
    spectrum_id: str | None = Field(description="Spectrum identifier/index")
    charge_state: int | None
    experimental_mz: float | None = Field(description="Experimental m/z value")
    calculated_mz: float | None = Field(description="Calculated m/z value")
    score_values: dict | None = Field(
        default=None,
        sa_column=Column(JSON),
        description="MS-GF+ score, FDR, e-value, etc. as JSON",
    )
    rank: int | None = Field(default=None, description="Rank of this PSM for the spectrum")
    pass_threshold: bool | None = Field(
        description=(
            "Whether PSM passes quality threshold. Based on file-level threshold "
            "(see mzid_file.threshold_type and threshold_value) or per-spectrum "
            "dynamic thresholds (e.g., Mascot identity/homology). Value comes from "
            "mzID passThreshold attribute."
        )
    )
    index_type: IndexType | None = Field(
        default=None,
        description="Type of spectrum index. Part of USI specification.",
    )
    index_number: int | None = Field(
        default=None,
        description="Spectrum index number. Part of USI specification.",
    )
    ms_run: str | None = Field(
        default=None,
        description="MS run identifier from raw file name. Part of USI specification.",
    )
    ms_run_ext: str | None = Field(
        default=None,
        description="File extension of MS run. Part of USI specification.",
    )
    is_usi_validated: bool | None = Field(
        default=None,
        description="USI validation status: True (valid), False (invalid), None (not validated)",
    )

    # Relationships
    project: Project | None = Relationship(back_populates="peptide_spectrum_matches")
    mzid_file: MzidFile | None = Relationship(back_populates="peptide_spectrum_matches")
    modified_peptide: ModifiedPeptide | None = Relationship(
        back_populates="peptide_spectrum_matches"
    )
    psm_peptide_evidences: list["PSMPeptideEvidence"] = Relationship(back_populates="psm")
    search_modifications: list["SearchModification"] | None = Relationship(
        back_populates="peptide_spectrum_match"
    )


class PeptideEvidence(SQLModel, table=True):
    """Peptide-to-protein mappings - where peptides appear in proteins."""

    __tablename__ = "peptide_evidence"

    id: uuid.UUID = Field(default_factory=uuid7, primary_key=True)
    protein_accession: str | None = Field(default=None, description="Protein accession.")
    is_decoy: bool | None = Field(default=None, description="Whether the protein is a decoy")
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

    id: uuid.UUID = Field(default_factory=uuid7, primary_key=True)
    psm_id: uuid.UUID = Field(foreign_key="peptide_spectrum_matches.id")
    peptide_evidence_id: uuid.UUID = Field(foreign_key="peptide_evidence.id")

    # Relationships
    psm: "PeptideSpectrumMatch" = Relationship(back_populates="psm_peptide_evidences")
    peptide_evidence: "PeptideEvidence" = Relationship(back_populates="psm_peptide_evidences")


class SearchModification(SQLModel, table=True):
    __tablename__ = "search_modifications"

    id: uuid.UUID = Field(default_factory=uuid7, primary_key=True)
    psm_id: uuid.UUID = Field(foreign_key="peptide_spectrum_matches.id")
    unimod_id: int = Field()

    # Relationships
    peptide_spectrum_match: "PeptideSpectrumMatch" = Relationship(
        back_populates="search_modifications"
    )


# ============================================================================
# Database Initialization
# ============================================================================


def create_db_and_tables(engine):
    """Create all database tables."""
    SQLModel.metadata.create_all(engine)
