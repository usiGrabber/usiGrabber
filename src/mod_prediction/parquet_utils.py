"""Utilities for working with Parquet files in the PSM workflow."""

import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from mod_prediction.models import EnrichedPSM

logger = logging.getLogger(__name__)


def aggregate_modifications_per_psm(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate modification data per PSM, combining multiple rows into arrays.

    Groups rows by psm_id and aggregates modification fields (unimod_id, location,
    modified_residue) into lists. All other PSM fields are taken from the first row
    in each group.

    Args:
        df: DataFrame with columns including psm_id, unimod_id, location, modified_residue

    Returns:
        DataFrame with one row per psm_id and modification arrays
    """
    logger.debug(f"Aggregating {len(df)} rows by psm_id...")

    # Check if modification columns exist
    has_mods = all(col in df.columns for col in ["unimod_id", "location", "modified_residue"])

    if has_mods:
        # Group by psm_id and aggregate
        agg_dict = {
            # Keep first value for all non-modification columns
            "project_accession": "first",
            "spectrum_id": "first",
            "charge_state": "first",
            "experimental_mz": "first",
            "calculated_mz": "first",
            "pass_threshold": "first",
            "rank": "first",
            "ms_run": "first",
            "index_number": "first",
            "index_type": "first",
            "peptide_sequence": "first",
            "modified_peptide_id": "first",
            # Aggregate modifications into lists
            "unimod_id": list,
            "location": list,
            "modified_residue": list,
        }
    else:
        # No modifications - just deduplicate
        logger.info("No modification columns found, deduplicating on psm_id only")
        agg_dict = {
            "project_accession": "first",
            "spectrum_id": "first",
            "charge_state": "first",
            "experimental_mz": "first",
            "calculated_mz": "first",
            "pass_threshold": "first",
            "rank": "first",
            "ms_run": "first",
            "index_number": "first",
            "index_type": "first",
            "peptide_sequence": "first",
            "modified_peptide_id": "first",
        }

    aggregated: pd.DataFrame = df.groupby("psm_id", as_index=False).agg(agg_dict)

    if has_mods:
        # Rename modification columns to match model
        aggregated = aggregated.rename(
            columns={
                "unimod_id": "unimod_ids",
                "location": "locations",
                "modified_residue": "modified_residues",
            }
        )
    else:
        # Add empty modification arrays
        aggregated["unimod_ids"] = [[] for _ in range(len(aggregated))]
        aggregated["locations"] = [[] for _ in range(len(aggregated))]
        aggregated["modified_residues"] = [[] for _ in range(len(aggregated))]

    initial_rows = len(df)
    final_rows = len(aggregated)
    logger.info(f"Aggregated {initial_rows} rows into {final_rows} unique PSMs")

    return aggregated


def read_psm_parquet(parquet_path: Path) -> pd.DataFrame:
    """
    Read PSM parquet file with proper type handling.

    Args:
        parquet_path: Path to parquet file

    Returns:
        DataFrame with PSM data
    """
    logger.debug("Reading parquet file: %s", parquet_path)

    # Read with pyarrow to preserve types
    table = pq.read_table(parquet_path)
    df = table.to_pandas()

    logger.debug("Read %d rows with %d columns", len(df), len(df.columns))
    return df


def read_psm_data(input_path: Path) -> pd.DataFrame:
    """
    Read PSM data from either CSV or Parquet file.

    Args:
        input_path: Path to CSV or Parquet file

    Returns:
        DataFrame with PSM data
    """
    if input_path.suffix.lower() == ".csv":
        logger.debug("Reading CSV file: %s", input_path)
        df = pd.read_csv(input_path)
    elif input_path.suffix.lower() == ".parquet":
        logger.debug("Reading Parquet file: %s", input_path)
        df = read_psm_parquet(input_path)
    else:
        raise ValueError(
            f"Unsupported file format: {input_path.suffix}. Please use .csv or .parquet files."
        )
    # Convert NaN to None for nullable fields (pandas uses NaN, Pydantic expects None)
    df = df.where(pd.notna(df), None)
    logger.debug("Read %d rows with %d columns", len(df), len(df.columns))
    return df


def write_batch_parquet(enriched_psms: list[EnrichedPSM], output_dir: Path, file_name: str) -> Path:
    """
    Write enriched PSM data to a batch parquet file in an output directory.

    Creates a parquet file named batch_{batch_num:04d}.parquet with proper schema
    including list columns for modifications and spectrum arrays.

    Args:
        enriched_psms: List of EnrichedPSM objects
        output_dir: Path to output directory (will be created if doesn't exist)
        file_name: filename of the file without extension

    Returns:
        Path to the created batch file
    """
    if not enriched_psms:
        logger.warning("No enriched PSMs to write for %s", file_name)
        return output_dir / f"{file_name}.parquet"

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create batch filename
    batch_path = output_dir / f"{file_name}.parquet"

    logger.debug("Writing %d enriched PSMs to %s", len(enriched_psms), batch_path)

    # Convert to list of dicts
    records = [psm.model_dump() for psm in enriched_psms]
    df = pd.DataFrame(records)

    # Define schema with explicit list types and nullable fields
    schema = pa.schema(
        [
            pa.field("psm_id", pa.string(), nullable=False),
            pa.field("project_accession", pa.string(), nullable=False),
            pa.field("spectrum_id", pa.string(), nullable=True),
            pa.field("charge_state", pa.int32(), nullable=True),
            pa.field("experimental_mz", pa.float64(), nullable=True),
            pa.field("calculated_mz", pa.float64(), nullable=True),
            pa.field("pass_threshold", pa.bool_(), nullable=True),
            pa.field("rank", pa.int32(), nullable=True),
            pa.field("ms_run", pa.string(), nullable=False),
            pa.field("index_number", pa.int32(), nullable=False),
            pa.field("index_type", pa.string(), nullable=False),
            pa.field("peptide_sequence", pa.string(), nullable=True),
            pa.field("modified_peptide_id", pa.string(), nullable=True),
            pa.field("unimod_ids", pa.list_(pa.int32()), nullable=False),
            pa.field("locations", pa.list_(pa.int32()), nullable=False),
            pa.field("modified_residues", pa.list_(pa.string()), nullable=True),
            pa.field("mz_array", pa.list_(pa.float64()), nullable=False),
            pa.field("intensity_array", pa.list_(pa.float64()), nullable=False),
        ]
    )

    # Convert to pyarrow table
    table = pa.Table.from_pandas(df, schema=schema)

    # Write to parquet
    pq.write_table(table, batch_path)
    logger.debug("Successfully wrote %d records to '%s'", len(enriched_psms), batch_path)

    return batch_path
