import uuid
from datetime import date, datetime
from enum import Enum
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class IndexType(str, Enum):
    """Type of spectrum index for USI specification."""

    scan = "scan"
    index = "index"
    nativeId = "nativeId"
    trace = "trace"


# ============================================================================
# Base Class
# ============================================================================


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""

    type_annotation_map = {
        dict: JSON,
        dict[str, Any]: JSON,
    }


# ============================================================================
# Core Tables
# ============================================================================


class CvJunctionTable(Base):
    __tablename__ = "project_cv_params"

    cv_param_id: Mapped[int] = mapped_column(ForeignKey("cv_params.id"), primary_key=True)
    project_accession: Mapped[str] = mapped_column(
        ForeignKey("projects.accession"), primary_key=True
    )


class CvParam(Base):
    __tablename__ = "cv_params"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    accession: Mapped[str] = mapped_column(String, nullable=False)
    value: Mapped[str | None] = mapped_column(String, default=None)

    projects: Mapped[list["Project"]] = relationship(
        back_populates="cv_params", secondary="project_cv_params"
    )


class Project(Base):
    """Main project table storing PRIDE project information."""

    __tablename__ = "projects"

    accession: Mapped[str] = mapped_column(String, primary_key=True)
    title: Mapped[str] = mapped_column(String, nullable=False)
    project_description: Mapped[str | None] = mapped_column(Text, default=None)
    sample_processing_protocol: Mapped[str | None] = mapped_column(Text, default=None)
    data_processing_protocol: Mapped[str | None] = mapped_column(Text, default=None)
    doi: Mapped[str | None] = mapped_column(String, default=None)
    submission_type: Mapped[str] = mapped_column(String, nullable=False)
    license: Mapped[str | None] = mapped_column(String, default=None)
    submission_date: Mapped[date | None] = mapped_column(Date, default=None)
    publication_date: Mapped[date | None] = mapped_column(Date, default=None)
    total_file_downloads: Mapped[int] = mapped_column(Integer, default=0)
    fully_processed: Mapped[bool] = mapped_column(Boolean, default=False)

    # Complex nested data stored as JSON
    sample_attributes: Mapped[dict | None] = mapped_column(JSON, default=None)
    additional_attributes: Mapped[dict | None] = mapped_column(JSON, default=None)

    # Relationships
    references: Mapped[list["Reference"]] = relationship(back_populates="project")
    keywords: Mapped[list["ProjectKeyword"]] = relationship(back_populates="project")
    project_tags: Mapped[list["ProjectTag"]] = relationship(back_populates="project")
    countries: Mapped[list["ProjectCountry"]] = relationship(back_populates="project")
    affiliations: Mapped[list["ProjectAffiliation"]] = relationship(back_populates="project")
    other_omics_links: Mapped[list["ProjectOtherOmicsLink"]] = relationship(
        back_populates="project"
    )
    mzid_files: Mapped[list["MzidFile"]] = relationship(back_populates="project")
    peptide_spectrum_matches: Mapped[list["PeptideSpectrumMatch"]] = relationship(
        back_populates="project"
    )

    cv_params: Mapped[list[CvParam]] = relationship(
        back_populates="projects", secondary="project_cv_params"
    )


class Reference(Base):
    """Publication references for projects."""

    __tablename__ = "references"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    project_accession: Mapped[str] = mapped_column(ForeignKey("projects.accession"))
    reference_line: Mapped[str | None] = mapped_column(Text, default=None)
    pubmed_id: Mapped[int | None] = mapped_column(Integer, default=None)
    doi: Mapped[str | None] = mapped_column(String, default=None)

    # Relationships
    project: Mapped[Project | None] = relationship(back_populates="references")


# ============================================================================
# Simple Array Tables
# ============================================================================


class ProjectKeyword(Base):
    """Project keywords."""

    __tablename__ = "project_keywords"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    project_accession: Mapped[str] = mapped_column(ForeignKey("projects.accession"))
    keyword: Mapped[str] = mapped_column(String, nullable=False)

    # Relationships
    project: Mapped[Project | None] = relationship(back_populates="keywords")


class ProjectTag(Base):
    """Project tags."""

    __tablename__ = "project_tags"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    project_accession: Mapped[str] = mapped_column(ForeignKey("projects.accession"))
    tag: Mapped[str] = mapped_column(String, nullable=False)

    # Relationships
    project: Mapped[Project | None] = relationship(back_populates="project_tags")


class ProjectCountry(Base):
    """Project countries."""

    __tablename__ = "project_countries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    project_accession: Mapped[str] = mapped_column(ForeignKey("projects.accession"))
    country: Mapped[str] = mapped_column(String, nullable=False)

    # Relationships
    project: Mapped[Project | None] = relationship(back_populates="countries")


class ProjectAffiliation(Base):
    """Project affiliations."""

    __tablename__ = "project_affiliations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    project_accession: Mapped[str] = mapped_column(ForeignKey("projects.accession"))
    affiliation: Mapped[str] = mapped_column(String, nullable=False)

    # Relationships
    project: Mapped[Project | None] = relationship(back_populates="affiliations")


class ProjectOtherOmicsLink(Base):
    """Project other omics links."""

    __tablename__ = "project_other_omics_links"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    project_accession: Mapped[str] = mapped_column(ForeignKey("projects.accession"))
    link: Mapped[str] = mapped_column(String, nullable=False)

    # Relationships
    project: Mapped[Project | None] = relationship(back_populates="other_omics_links")


# ============================================================================
# File specific Data Tables
# ============================================================================


class MzidFile(Base):
    """Metadata per mzID file - OPTIONAL table for provenance tracking."""

    __tablename__ = "mzid_files"

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    project_accession: Mapped[str] = mapped_column(ForeignKey("projects.accession"))
    file_name: Mapped[str] = mapped_column(String, nullable=False)
    file_path: Mapped[str | None] = mapped_column(String, default=None)
    software_name: Mapped[str | None] = mapped_column(String, default=None)
    software_version: Mapped[str | None] = mapped_column(String, default=None)
    search_database_name: Mapped[str | None] = mapped_column(String, default=None)
    protocol_parameters: Mapped[dict | None] = mapped_column(JSON, default=None)
    threshold_type: Mapped[str | None] = mapped_column(String, default=None)
    threshold_value: Mapped[float | None] = mapped_column(Float, default=None)
    creation_date: Mapped[datetime | None] = mapped_column(DateTime, default=None)

    # Relationships
    project: Mapped[Project | None] = relationship(back_populates="mzid_files")
    peptide_spectrum_matches: Mapped[list["PeptideSpectrumMatch"]] = relationship(
        back_populates="mzid_file"
    )


# ============================================================================
# General Data Tables
# ============================================================================


class ModifiedPeptideModificationJunction(Base):
    __tablename__ = "modified_peptide_modification_junction"

    modified_peptide_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("modified_peptides.id"), primary_key=True
    )
    modification_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("modifications.id"), primary_key=True
    )


class ModifiedPeptide(Base):
    """Modified peptide sequence. Links to 0 to many Modifications."""

    __tablename__ = "modified_peptides"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    peptide_sequence: Mapped[str] = mapped_column(String, nullable=False)

    # Relationships
    peptide_spectrum_matches: Mapped[list["PeptideSpectrumMatch"]] = relationship(
        back_populates="modified_peptide"
    )
    modifications: Mapped[list["Modification"]] = relationship(
        back_populates="modified_peptides",
        secondary="modified_peptide_modification_junction",
    )


class Modification(Base):
    """Modification with a location and modified residue."""

    __tablename__ = "modifications"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    unimod_id: Mapped[int | None] = mapped_column(Integer, default=None)
    name: Mapped[str | None] = mapped_column(String, default=None)
    location: Mapped[int | None] = mapped_column(Integer, default=None)
    modified_residue: Mapped[str | None] = mapped_column(String, default=None)

    modified_peptides: Mapped[list["ModifiedPeptide"]] = relationship(
        back_populates="modifications",
        secondary="modified_peptide_modification_junction",
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


class PeptideSpectrumMatch(Base):
    """Core PSM data. Links to Project, MzidFile, Peptide."""

    __tablename__ = "peptide_spectrum_matches"

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    project_accession: Mapped[str] = mapped_column(ForeignKey("projects.accession"))
    mzid_file_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("mzid_files.id"),
        default=None,
    )
    modified_peptide_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("modified_peptides.id")
    )
    spectrum_id: Mapped[str | None] = mapped_column(String, default=None)
    charge_state: Mapped[int | None] = mapped_column(Integer, default=None)
    experimental_mz: Mapped[float | None] = mapped_column(Float, default=None)
    calculated_mz: Mapped[float | None] = mapped_column(Float, default=None)
    score_values: Mapped[dict | None] = mapped_column(JSON, default=None)
    rank: Mapped[int | None] = mapped_column(Integer, default=None)
    pass_threshold: Mapped[bool | None] = mapped_column(Boolean, default=None)
    index_type: Mapped[IndexType | None] = mapped_column(default=None)
    index_number: Mapped[int | None] = mapped_column(Integer, default=None)
    ms_run: Mapped[str | None] = mapped_column(String, default=None)
    ms_run_ext: Mapped[str | None] = mapped_column(String, default=None)
    is_usi_validated: Mapped[bool | None] = mapped_column(Boolean, default=None)

    # Relationships
    project: Mapped[Project | None] = relationship(back_populates="peptide_spectrum_matches")
    mzid_file: Mapped[MzidFile | None] = relationship(back_populates="peptide_spectrum_matches")
    modified_peptide: Mapped[ModifiedPeptide | None] = relationship(
        back_populates="peptide_spectrum_matches"
    )
    psm_peptide_evidences: Mapped[list["PSMPeptideEvidence"]] = relationship(back_populates="psm")
    search_modifications: Mapped[list["SearchModification"] | None] = relationship(
        back_populates="peptide_spectrum_match"
    )


class PeptideEvidence(Base):
    """Peptide-to-protein mappings - where peptides appear in proteins."""

    __tablename__ = "peptide_evidence"

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    protein_accession: Mapped[str | None] = mapped_column(String, default=None)
    is_decoy: Mapped[bool | None] = mapped_column(Boolean, default=None)
    start_position: Mapped[int | None] = mapped_column(Integer, default=None)
    end_position: Mapped[int | None] = mapped_column(Integer, default=None)
    pre_residue: Mapped[str | None] = mapped_column(String(1), default=None)
    post_residue: Mapped[str | None] = mapped_column(String(1), default=None)

    # Relationships
    psm_peptide_evidences: Mapped[list["PSMPeptideEvidence"]] = relationship(
        back_populates="peptide_evidence"
    )


class PSMPeptideEvidence(Base):
    """Junction table linking PSMs to their protein evidence.

    In mzID files, PeptideEvidenceRef elements appear inside SpectrumIdentificationItem
    (PSM) elements, establishing a many-to-many relationship. This allows:
    - One PSM to map to multiple proteins (shared peptides)
    - One protein mapping to be referenced by multiple PSMs
    """

    __tablename__ = "psm_peptide_evidence"

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    psm_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("peptide_spectrum_matches.id")
    )
    peptide_evidence_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("peptide_evidence.id")
    )

    # Relationships
    psm: Mapped["PeptideSpectrumMatch"] = relationship(back_populates="psm_peptide_evidences")
    peptide_evidence: Mapped["PeptideEvidence"] = relationship(
        back_populates="psm_peptide_evidences"
    )


class SearchModification(Base):
    __tablename__ = "search_modifications"

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    psm_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("peptide_spectrum_matches.id")
    )
    unimod_id: Mapped[int] = mapped_column(Integer, nullable=False)

    # Relationships
    peptide_spectrum_match: Mapped["PeptideSpectrumMatch"] = relationship(
        back_populates="search_modifications"
    )


# ============================================================================
# Database Initialization
# ============================================================================


def create_db_and_tables(engine):
    """Create all database tables."""
    Base.metadata.create_all(engine)
