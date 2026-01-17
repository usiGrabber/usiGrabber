"""
CLI command for USI validation.

Validates USI strings for peptide spectrum matches by sampling PSMs from each
mzID file and checking against available backends.
"""

import asyncio
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from usigrabber.backends.project_exchange import ProjectExchange
from usigrabber.cli import app
from usigrabber.db import load_db_engine
from usigrabber.db.schema import MzidFile, PeptideSpectrumMatch
from usigrabber.usi_validation.report import generate_report, save_report
from usigrabber.usi_validation.validator import validate_psms_batch
from usigrabber.utils import logger

console = Console()


@app.command()
def validate_usi(
    sample_size: Annotated[int, typer.Option(help="Number of PSMs to sample per mzID file")] = 5,
    requests_per_second: Annotated[
        float, typer.Option(help="API request rate limit (requests per second)")
    ] = 20.0,
    output: Annotated[Path, typer.Option(help="Output JSON report path")] = Path(
        "usi_validation_report.json"
    ),
    project_filter: Annotated[
        str | None,
        typer.Option(help="Comma-separated list of project accessions to validate"),
    ] = None,
    dry_run: Annotated[bool, typer.Option(help="Preview sampling without validation")] = False,
) -> None:
    """
    Validate USI strings for peptide spectrum matches.

    Samples PSMs from each mzID file, constructs USI strings, validates them
    against the PRIDE backend, and generates a comprehensive JSON report.

    Automatically resumes from previous runs by only validating PSMs where
    is_usi_validated IS NULL.

    Example:
        usigrabber validate-usi --sample-size 10 --requests-per-second 15.0
    """
    console.print("\n🔍 USI Validation", style="bold blue")
    console.print(
        f"Sample size: {sample_size} PSMs per file | Rate limit: {requests_per_second} req/s\n"
    )

    engine = load_db_engine()

    # Parse project filter
    project_accessions = None
    if project_filter:
        project_accessions = [p.strip() for p in project_filter.split(",")]
        console.print(f"Filtering projects: {', '.join(project_accessions)}\n")

    with Session(engine) as session:
        # Query mzID files (optionally filtered by project)
        query = select(MzidFile)
        if project_accessions:
            query = query.where(MzidFile.project_accession.in_(project_accessions))  # type: ignore

        mzid_files = session.execute(query).scalars().all()

        if not mzid_files:
            console.print("❌ No mzID files found matching criteria", style="yellow")
            raise typer.Exit(1)

        console.print(f"Found {len(mzid_files)} mzID files to process\n")

        # Statistics
        total_psms_to_validate = 0
        total_already_validated = 0
        files_processed = 0

        for mzid_file in mzid_files:
            # Query unvalidated PSMs for this file
            psm_query = (
                select(PeptideSpectrumMatch)
                .where(PeptideSpectrumMatch.mzid_file_id == mzid_file.id)
                .where(PeptideSpectrumMatch.is_usi_validated.is_(None))  # type: ignore
            )

            # Count total, validated, and unvalidated
            total_count = session.execute(
                select(func.count())
                .select_from(PeptideSpectrumMatch)
                .where(PeptideSpectrumMatch.mzid_file_id == mzid_file.id)
            ).scalar_one()

            validated_count = session.execute(
                select(func.count())
                .select_from(PeptideSpectrumMatch)
                .where(PeptideSpectrumMatch.mzid_file_id == mzid_file.id)
                .where(PeptideSpectrumMatch.is_usi_validated.is_not(None))  # type: ignore
            ).scalar_one()

            unvalidated_count = session.execute(
                select(func.count())
                .select_from(PeptideSpectrumMatch)
                .where(PeptideSpectrumMatch.mzid_file_id == mzid_file.id)
                .where(PeptideSpectrumMatch.is_usi_validated.is_(None))  # type: ignore
            ).scalar_one()

            # Skip if we already have sample_size validated PSMs for this file
            if validated_count >= sample_size:
                console.print(
                    f"⏭️  Skipping {mzid_file.file_name}: already has {validated_count} validated PSMs (target: {sample_size})",
                    style="dim",
                )
                total_already_validated += validated_count
                continue

            if unvalidated_count == 0:
                console.print(
                    f"⏭️  Skipping {mzid_file.file_name}: no unvalidated PSMs remaining",
                    style="dim",
                )
                total_already_validated += total_count
                continue

            # Calculate how many more we need to reach sample_size
            remaining_needed = sample_size - validated_count
            sample_limit = min(remaining_needed, unvalidated_count)

            psms_to_validate = (
                session.execute(
                    psm_query.order_by(func.random()).limit(sample_limit)  # type: ignore
                )
                .scalars()
                .all()
            )

            console.print(
                f"📄 {mzid_file.file_name}: sampling {len(psms_to_validate)} PSMs "
                f"({validated_count} already validated, target: {sample_size})",
                style="cyan",
            )

            if dry_run:
                total_psms_to_validate += len(psms_to_validate)
                files_processed += 1
                continue

            # Find which backends have this project
            available_backends = asyncio.run(
                ProjectExchange.get_backends_for_project(mzid_file.project_accession)
            )

            if not available_backends:
                console.print(
                    f"⚠️  Skipping {mzid_file.file_name}: project {mzid_file.project_accession} not found in any backend",
                    style="yellow",
                )
                continue

            # Use the first available backend for validation
            backend_enum = available_backends[0]
            backend_class = backend_enum.value
            console.print(
                f"  Using {backend_enum.name} backend for validation",
                style="dim",
            )

            # Validate batch
            validation_results = validate_psms_batch(
                psms_to_validate, backend_class, requests_per_second
            )

            # Update database - modify PSMs that are already tracked by the session
            updated_count = 0
            for psm in psms_to_validate:
                if psm.id in validation_results:
                    psm.is_usi_validated = validation_results[psm.id]
                    updated_count += 1
                    logger.debug(
                        f"Setting is_usi_validated={validation_results[psm.id]} for PSM {psm.id}"
                    )

            # Flush changes to database and commit the transaction
            session.flush()  # Ensure changes are sent to DB
            session.commit()
            logger.info(f"Committed {updated_count} PSM updates to database")

            # Verify updates persisted by querying one back
            if updated_count > 0 and psms_to_validate:
                first_psm_id = psms_to_validate[0].id
                verification = session.execute(
                    select(PeptideSpectrumMatch).where(PeptideSpectrumMatch.id == first_psm_id)
                ).scalar_one_or_none()
                if verification:
                    logger.info(
                        f"Verification: PSM {first_psm_id} is_usi_validated = {verification.is_usi_validated}"
                    )

            console.print(
                f"  ✓ Updated {updated_count} PSMs in database",
                style="green",
            )

            total_psms_to_validate += len(psms_to_validate)
            files_processed += 1

        # Summary
        console.print("\n" + "=" * 60)
        if dry_run:
            console.print("📊 Dry Run Summary", style="bold yellow")
            console.print(f"  Files to process: {files_processed}")
            console.print(f"  PSMs to validate: {total_psms_to_validate}")
            console.print(f"  Already validated: {total_already_validated}")
            console.print("\nNo validation performed (dry run mode)", style="dim")
            raise typer.Exit(0)

        # Generate report
        console.print("\n📊 Generating validation report...", style="bold")
        report = generate_report(session, sample_size, requests_per_second)

        # Save report
        save_report(report, output)

        # Display summary table
        table = Table(title="Validation Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", justify="right", style="magenta")

        table.add_row("Files Processed", str(files_processed))
        table.add_row("PSMs Validated (this run)", str(total_psms_to_validate))
        table.add_row(
            "Total PSMs Validated (all time)", str(report.summary["total_psms_validated"])
        )
        table.add_row("Valid PSMs", str(report.summary["total_psms_valid"]))
        table.add_row("Invalid PSMs", str(report.summary["total_psms_invalid"]))
        table.add_row("Success Rate", f"{report.summary['success_rate'] * 100:.1f}%")

        console.print(table)
        console.print(f"\n✅ Report saved to: {output.absolute()}", style="green")
