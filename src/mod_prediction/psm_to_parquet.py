"""Enrich PSM data by downloading spectra from PRIDE.

Supports both CSV and Parquet input files.
"""

import argparse
import logging
import sys
import time
from logging import Logger
from pathlib import Path
from typing import NamedTuple

import pandas as pd
from pyteomics.usi import USI

from mod_prediction.logging_config import setup_logging
from mod_prediction.models import EnrichedPSM
from mod_prediction.parquet_utils import (
    aggregate_modifications_per_psm,
    read_psm_data,
    write_batch_parquet,
)
from mod_prediction.pride_fetcher import download_spectra

setup_logging()
logger: Logger = logging.getLogger()

class PSMRow(NamedTuple):
    """PSM data from parquet row."""

    psm_id: str
    project_accession: str
    ms_run: str
    index_type: str
    index_number: int
    charge_state: int
    peptide_sequence: str


def create_usi_from_row(row: PSMRow) -> USI:
    """
    Create USI from parquet row data.

    Args:
        row: PSMRow with required fields

    Returns:
        USI object
    """
    return USI(
        protocol="mzspec",
        dataset=row.project_accession,
        datafile=row.ms_run,
        scan_identifier_type=row.index_type,
        scan_identifier=str(row.index_number),
        interpretation=f"{row.peptide_sequence}/{row.charge_state}",
    )


def extract_usis_and_psms(
    df: pd.DataFrame,
) -> tuple[list[USI], dict[str, dict]]:
    """
    Extract USIs and PSM data from DataFrame.

    Args:
        df: DataFrame with aggregated PSM data (one row per psm_id)

    Returns:
        Tuple of (list of USI objects, dict mapping USI string to PSM row dict)
    """
    logger.info("Extracting USIs from parquet rows...")
    usis = []
    psm_data = {}

    for _, row in df.iterrows():
        try:
            psm_row = PSMRow(
                psm_id=str(row["psm_id"]),
                project_accession=str(row["project_accession"]),
                ms_run=str(row["ms_run"]),
                index_type=str(row["index_type"]),
                index_number=int(row["index_number"]),
                charge_state=int(row["charge_state"]),
                peptide_sequence=str(row["peptide_sequence"]),
            )
            usi = create_usi_from_row(psm_row)
            usis.append(usi)

            # Store PSM data for later merging (convert to dict and replace NaN with None)
            row_dict = row.to_dict()
            # Replace any NaN values with None for Pydantic compatibility
            for key, value in row_dict.items():
                try:
                    if pd.isna(value):
                        row_dict[key] = None
                except (TypeError, ValueError):
                    # Skip array-like values that can't be checked with isna()
                    pass
            psm_data[str(usi)] = row_dict

        except Exception as e:
            logger.warning(f"Failed to create USI from row: {e}")
            continue

    logger.info(f"Created {len(usis)} USIs from {len(df)} rows")
    return usis, psm_data


def process_batch(
    batch_usis: list[USI],
    psm_data: dict[str, dict],
    output_dir: Path,
    batch_num: int,
    failed_files: set[tuple[str, str]],
    max_workers: int = 5,
) -> int:
    """
    Process a batch of USIs: download spectra and merge with PSM data.

    Args:
        batch_usis: List of USIs to process in this batch
        psm_data: Dict mapping USI string to PSM row dict
        output_dir: Path to output directory for batch parquet files
        batch_num: Batch number for logging and filename
        failed_files: Set of (project_accession, ms_run) tuples that have failed
        max_workers: Number of parallel download threads (default: 5)

    Returns:
        Number of enriched PSMs successfully written
    """
    logger.info(f"Processing batch {batch_num} with {len(batch_usis)} USIs...")

    # Track batch processing time
    batch_start_time = time.time()

    # Download spectra
    spectra = download_spectra(batch_usis, failed_files, max_workers=max_workers)

    if not spectra:
        logger.warning(f"No spectra downloaded for batch {batch_num}")
        return 0

    logger.info(f"Merging {len(spectra)} spectra with PSM metadata...")
    enriched_psms = []

    for usi, spectrum in spectra:
        try:
            usi_str = str(usi)
            if usi_str not in psm_data:
                logger.warning(f"No PSM data found for USI: {usi_str}")
                continue

            # Get PSM data
            psm_row = psm_data[usi_str]

            # Create enriched PSM with spectrum data
            enriched_psm = EnrichedPSM(
                psm_id=psm_row["psm_id"],
                project_accession=psm_row["project_accession"],
                spectrum_id=psm_row["spectrum_id"],
                charge_state=psm_row["charge_state"],
                experimental_mz=psm_row["experimental_mz"],
                calculated_mz=psm_row["calculated_mz"],
                pass_threshold=psm_row["pass_threshold"],
                rank=psm_row["rank"],
                ms_run=psm_row["ms_run"],
                index_number=psm_row["index_number"],
                index_type=psm_row["index_type"],
                peptide_sequence=psm_row["peptide_sequence"],
                modified_peptide_id=psm_row["modified_peptide_id"],
                unimod_ids=psm_row["unimod_ids"],
                locations=psm_row["locations"],
                modified_residues=psm_row["modified_residues"],
                mz_array=spectrum.mz_array.tolist(),
                intensity_array=spectrum.intensity_array.tolist(),
            )
            enriched_psms.append(enriched_psm)

        except Exception as e:
            logger.warning(f"Failed to create enriched PSM for {usi}: {e}")
            continue

    if not enriched_psms:
        logger.warning(f"No enriched PSMs created for batch {batch_num}")
        return 0

    # Write batch to individual parquet file
    file_name = f"batch_{batch_num:04d}"
    write_batch_parquet(enriched_psms, output_dir, file_name)

    # Calculate and log performance metrics
    batch_duration = time.time() - batch_start_time
    batch_duration_minutes = batch_duration / 60
    if batch_duration_minutes > 0:
        spectra_per_minute = len(enriched_psms) / batch_duration_minutes
    else:
        spectra_per_minute = 0

    logger.info(
        f"Batch {batch_num} complete: {len(enriched_psms)} enriched PSMs written "
        f"in {batch_duration:.1f}s ({spectra_per_minute:.1f} spectra/min)"
    )
    return len(enriched_psms)


def main():
    parser = argparse.ArgumentParser(
        description="Enrich PSM data by downloading spectra from PRIDE. "
        "Supports CSV and Parquet input files."
    )
    parser.add_argument(
        "input_file", type=Path, help="Path to input CSV or Parquet file with PSM data"
    )
    parser.add_argument(
        "output_dir",
        type=Path,
        help="Path to output directory for enriched PSM batch parquet files",
    )
    parser.add_argument(
        "-n",
        "--nrows",
        type=int,
        default=None,
        help="Number of rows to read from input (default: all rows)",
    )
    parser.add_argument(
        "-b",
        "--batch-size",
        type=int,
        default=1000,
        help="Maximum number of USIs to process per batch (default: 1000)",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=5,
        help="Number of parallel download threads (default: 5)",
    )

    args = parser.parse_args()

    # Validate workers is positive
    if args.workers <= 0:
        parser.error("workers must be a positive integer")

    # Validate input file exists
    if not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    if args.batch_size > 10000:
        logger.warning("Large batch size may lead to performance issues.")

    # Create output directory if it doesn't exist
    args.output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Starting PSM enrichment pipeline with PRIDE spectrum download...")
    logger.info(f"Input: {args.input_file}")
    logger.info(f"Output directory: {args.output_dir}")
    logger.info(f"Batch size: {args.batch_size} USIs per batch")
    logger.info(f"Parallel workers: {args.workers}")

    # Read PSM data (supports CSV and Parquet)
    df = read_psm_data(args.input_file)

    if args.nrows:
        logger.info(f"Limiting to first {args.nrows} rows")
        df = df.head(args.nrows)

    # Aggregate modifications per psm_id
    df_aggregated = aggregate_modifications_per_psm(df)

    # Extract USIs and PSM data
    usis, psm_data = extract_usis_and_psms(df_aggregated)

    if not usis:
        logger.error("No USIs extracted. Exiting.")
        sys.exit(1)

    # Process USIs in batches
    total_enriched = 0
    num_batches = (len(usis) + args.batch_size - 1) // args.batch_size
    failed_files: set[tuple[str, str]] = set()

    for i in range(0, len(usis), args.batch_size):
        batch_num = (i // args.batch_size) + 1
        batch_usis = usis[i : i + args.batch_size]

        enriched_count = process_batch(
            batch_usis,
            psm_data,
            args.output_dir,
            batch_num,
            failed_files,
            max_workers=args.workers,
        )
        total_enriched += enriched_count

        logger.info(
            f"Progress: {batch_num}/{num_batches} batches, "
            f"{total_enriched} total enriched PSMs written"
        )

    logger.info("Pipeline complete!")
    logger.info(f"Total enriched PSMs written: {total_enriched}")
    logger.info(f"Output directory: {args.output_dir}")


if __name__ == "__main__":
    main()
