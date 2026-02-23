"""Utilities for working with Parquet files in the PSM workflow."""

import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from mod_prediction.models import EnrichedPSM

logger = logging.getLogger(__name__)


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
            pa.field("charge_state", pa.int32(), nullable=True),
            pa.field("ms_run", pa.string(), nullable=False),
            pa.field("index_number", pa.int32(), nullable=False),
            pa.field("index_type", pa.string(), nullable=False),
            pa.field("peptide_sequence", pa.string(), nullable=True),
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
