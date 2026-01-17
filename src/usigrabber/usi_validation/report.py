"""
USI validation report generation with comprehensive statistics and PSM details.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from usigrabber.db.schema import MzidFile, PeptideSpectrumMatch, Project
from usigrabber.utils import logger
from usigrabber.utils.usi import build_usi


@dataclass
class PSMDetail:
    """Individual PSM validation details."""

    psm_id: str
    usi: str | None
    is_valid: bool
    spectrum_id: str | None
    charge_state: int | None
    peptide_sequence: str | None
    error: str | None = None


@dataclass
class MzidFileStats:
    """Statistics for a single mzID file."""

    file_id: str
    file_name: str
    psms_validated: int
    psms_valid: int
    psms_invalid: int
    success_rate: float
    psms: list[PSMDetail] = field(default_factory=list)


@dataclass
class ProjectStats:
    """Statistics for a project with mzID files."""

    accession: str
    title: str
    psms_validated: int
    psms_valid: int
    psms_invalid: int
    success_rate: float
    mzid_files: list[MzidFileStats] = field(default_factory=list)


@dataclass
class ValidationReport:
    """Complete validation report with metadata, summary, and details."""

    metadata: dict[str, Any]
    summary: dict[str, Any]
    projects: list[ProjectStats]


def generate_report(
    session: Session,
    sample_size: int,
    requests_per_second: float,
) -> ValidationReport:
    """
    Generate a comprehensive USI validation report from database.

    Queries all PSMs with validation status (is_usi_validated IS NOT NULL),
    aggregates statistics by project and mzID file, and includes individual
    PSM details.

    Args:
        session: SQLModel database session
        sample_size: Sample size used during validation
        requests_per_second: API rate limit used during validation

    Returns:
        ValidationReport with complete statistics and details

    Example:
        >>> report = generate_report(session, sample_size=5, requests_per_second=20.0)
        >>> print(f"Total validated: {report.summary['total_psms_validated']}")
    """
    logger.info("Generating USI validation report...")

    # Count total mzID files and projects
    total_mzid_files = session.execute(select(func.count()).select_from(MzidFile)).scalar_one()
    total_projects = session.execute(select(func.count()).select_from(Project)).scalar_one()

    # Query all validated PSMs (relationships will be lazy-loaded as needed)
    validated_psms = (
        session.execute(
            select(PeptideSpectrumMatch).where(
                PeptideSpectrumMatch.is_usi_validated.is_not(None)  # type: ignore
            )
        )
        .scalars()
        .all()
    )

    logger.info(f"Found {len(validated_psms)} validated PSMs")

    # Aggregate by project and mzID file
    project_map: dict[str, ProjectStats] = {}

    for psm in validated_psms:
        if not psm.project:
            continue

        project_acc = psm.project.accession

        # Initialize project stats if needed
        if project_acc not in project_map:
            project_map[project_acc] = ProjectStats(
                accession=project_acc,
                title=psm.project.title,
                psms_validated=0,
                psms_valid=0,
                psms_invalid=0,
                success_rate=0.0,
                mzid_files=[],
            )

        project_stats = project_map[project_acc]

        # Find or create mzID file stats
        mzid_file_id = str(psm.mzid_file_id) if psm.mzid_file_id else "unknown"
        mzid_file_name = psm.mzid_file.file_name if psm.mzid_file else "unknown"

        mzid_stats = next(
            (mf for mf in project_stats.mzid_files if mf.file_id == mzid_file_id), None
        )

        if mzid_stats is None:
            mzid_stats = MzidFileStats(
                file_id=mzid_file_id,
                file_name=mzid_file_name,
                psms_validated=0,
                psms_valid=0,
                psms_invalid=0,
                success_rate=0.0,
                psms=[],
            )
            project_stats.mzid_files.append(mzid_stats)

        # Build USI and PSM detail
        usi = build_usi(psm)
        # is_usi_validated should never be None here due to the query filter,
        # but handle it defensively
        is_valid = psm.is_usi_validated if psm.is_usi_validated is not None else False

        error_msg = None
        if usi is None:
            missing = []
            if not psm.project:
                missing.append("project")
            if not psm.ms_run:
                missing.append("ms_run")
            if not psm.index_type:
                missing.append("index_type")
            if psm.index_number is None:
                missing.append("index_number")
            error_msg = f"Missing required USI fields: {', '.join(missing)}"

        psm_detail = PSMDetail(
            psm_id=str(psm.id),
            usi=usi,
            is_valid=is_valid,
            spectrum_id=psm.spectrum_id,
            charge_state=psm.charge_state,
            peptide_sequence=(
                psm.modified_peptide.peptide_sequence if psm.modified_peptide else None
            ),
            error=error_msg,
        )

        # Update stats
        mzid_stats.psms_validated += 1
        mzid_stats.psms.append(psm_detail)
        if is_valid:
            mzid_stats.psms_valid += 1
        else:
            mzid_stats.psms_invalid += 1

        project_stats.psms_validated += 1
        if is_valid:
            project_stats.psms_valid += 1
        else:
            project_stats.psms_invalid += 1

    # Calculate success rates and sort
    for project_stats in project_map.values():
        if project_stats.psms_validated > 0:
            project_stats.success_rate = project_stats.psms_valid / project_stats.psms_validated

        for mzid_stats in project_stats.mzid_files:
            if mzid_stats.psms_validated > 0:
                mzid_stats.success_rate = mzid_stats.psms_valid / mzid_stats.psms_validated

        # Sort mzID files within project: failed mzID files first (by number of failures)
        project_stats.mzid_files.sort(key=lambda mf: mf.psms_invalid, reverse=True)

    # Sort projects: failed projects first (by number of failures)
    sorted_projects = sorted(project_map.values(), key=lambda p: p.psms_invalid, reverse=True)

    # Build summary
    total_validated = sum(p.psms_validated for p in project_map.values())
    total_valid = sum(p.psms_valid for p in project_map.values())
    total_invalid = sum(p.psms_invalid for p in project_map.values())
    success_rate = total_valid / total_validated if total_validated > 0 else 0.0

    metadata = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "sample_size": sample_size,
        "requests_per_second": requests_per_second,
        "total_mzid_files": total_mzid_files,
        "total_projects": total_projects,
    }

    summary = {
        "total_psms_validated": total_validated,
        "total_psms_valid": total_valid,
        "total_psms_invalid": total_invalid,
        "success_rate": round(success_rate, 4),
    }

    report = ValidationReport(metadata=metadata, summary=summary, projects=sorted_projects)

    logger.info(
        f"Report generated: {total_validated} PSMs validated, "
        f"{total_valid} valid ({success_rate * 100:.1f}% success rate)"
    )

    return report


def save_report(report: ValidationReport, output_path: Path) -> None:
    """
    Save validation report to JSON file with pretty formatting.

    Args:
        report: ValidationReport to save
        output_path: Path to output JSON file

    Example:
        >>> save_report(report, Path("usi_validation_report.json"))
    """

    # Convert dataclasses to dicts
    def to_dict(obj: Any) -> Any:
        if isinstance(obj, (ProjectStats, MzidFileStats, PSMDetail)):
            return {k: to_dict(v) for k, v in obj.__dict__.items()}
        elif isinstance(obj, list):
            return [to_dict(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: to_dict(v) for k, v in obj.items()}
        else:
            return obj

    report_dict = {
        "metadata": report.metadata,
        "summary": report.summary,
        "projects": to_dict(report.projects),
    }

    with open(output_path, "w") as f:
        json.dump(report_dict, f, indent=2)

    logger.info(f"Report saved to {output_path}")
