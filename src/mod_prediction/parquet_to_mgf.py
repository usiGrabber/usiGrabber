"""Convert enriched PSM parquet files to MGF format."""

import argparse
import logging
import logging.handlers
import multiprocessing
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Literal

import pyarrow.dataset as ds
from dotenv import load_dotenv
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

from mod_prediction.logging_config import setup_logging, worker_log_configurer
from mod_prediction.mgf import append_mgf, spectrum_from_parquet_row, write_mgf

load_dotenv()

NUM_WORKERS = int(os.getenv("NUM_WORKERS", multiprocessing.cpu_count() - 1))

logger = logging.getLogger("export-mgf")


def convert_parquet_to_mgf(
    parquet_path: Path,
    output_path: Path,
    batch_size: int = 1000,
    nrows: int | None = None,
) -> int:
    """
    Convert enriched PSM parquet file or directory to MGF format.

    Args:
        parquet_path: Path to input enriched parquet file or directory
        output_path: Path to output MGF file
        batch_size: Number of spectra to process per batch (for memory efficiency)
        nrows: Number of rows to convert (default: all rows)

    Returns:
        Number of spectra written to MGF file
    """

    # Validate required columns
    required_columns = ["index_number", "charge_state", "mz_array", "intensity_array"]

    if not output_path.suffix.lower() == ".mgf":
        raise ValueError(f"Output file must have .mgf extension: {output_path}")

    # Delete existing output file if it exists
    if output_path.exists():
        logger.warning(f"Removing existing output file: {output_path}")
        output_path.unlink()

    # Create dataset for memory-efficient batch processing
    if parquet_path.is_dir():
        logger.debug(f"Reading enriched PSM data from directory: {parquet_path}")
        dataset = ds.dataset(str(parquet_path), format="parquet")
    else:
        logger.debug(f"Reading enriched PSM data from file: {parquet_path}")
        dataset = ds.dataset(str(parquet_path), format="parquet")

    # Validate columns from schema
    schema_fields = {field.name for field in dataset.schema}
    missing_columns = [col for col in required_columns if col not in schema_fields]
    if missing_columns:
        raise ValueError(
            f"Missing required columns in parquet file: {missing_columns}. "
            f"This file may not be an enriched PSM parquet with spectrum data."
        )

    # Process in memory-efficient batches
    total_written = 0
    batch_num = 0
    is_first_batch = True

    for batch in dataset.to_batches(batch_size=batch_size):
        batch_num += 1
        batch_df = batch.to_pandas()

        # Apply row limit if specified
        if nrows and total_written + len(batch_df) > nrows:
            batch_df = batch_df.iloc[: nrows - total_written]

        logger.debug(f"Processing batch {batch_num} ({len(batch_df)} spectra)...")

        # Convert batch to MGF spectra
        mgf_spectra = []
        for _, row in batch_df.iterrows():
            try:
                mgf_spectrum = spectrum_from_parquet_row(row.to_dict())
                mgf_spectra.append(mgf_spectrum)
            except Exception as e:
                logger.warning(f"Failed to convert row to MGF: {e}")
                continue

        if not mgf_spectra:
            logger.warning(f"No spectra converted in batch {batch_num}")
        else:
            # Write or append to file
            if is_first_batch:
                write_mgf(mgf_spectra, output_path)
                is_first_batch = False
            else:
                append_mgf(mgf_spectra, output_path)

            total_written += len(mgf_spectra)
            logger.debug(
                f"Batch {batch_num} complete: {len(mgf_spectra)} spectra, "
                f"{total_written} total written"
            )

        # Stop if we've reached nrows limit
        if nrows and total_written >= nrows:
            logger.debug(f"Reached row limit of {nrows}")
            break

    return total_written


def convert_parallel(
    input_dir: Path,
    output_dir: Path,
    *,
    batch_size: int = 1000,
    nrows: int | None = None,
    num_workers: int = NUM_WORKERS,
) -> int:
    if not input_dir.is_dir():
        raise ValueError(f"Input path must be a directory for parallel conversion: {input_dir}")

    files = list(input_dir.glob("*.parquet"))[:10]
    total_files = len(files)
    logger.info(f"Found {total_files:,} parquet files for conversion in {input_dir}")

    logger.info(f"Running parallel conversion with {num_workers} workers")

    total_spectra_written = 0

    with multiprocessing.Manager() as m:
        log_queue = m.Queue()

        with (
            ProcessPoolExecutor(
                max_workers=num_workers, initializer=worker_log_configurer, initargs=(log_queue,)
            ) as executor,
            Progress(
                SpinnerColumn(),  # Adds a spinning icon
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),  # This will pulse back and forth
                MofNCompleteColumn(),  # <--- This shows the current count
                TimeElapsedColumn(),  # <--- Shows how long it's been running
                transient=True,  # Optional: clears the bar from screen when done
            ) as progress,
        ):
            task = progress.add_task("Converting parquet files to MGF", total=total_files)

            # This thread runs in the main process, pulling logs from the queue
            # and writing them to your elaborate handlers.
            root_logger = logging.getLogger()
            listener = logging.handlers.QueueListener(
                log_queue, *root_logger.handlers, respect_handler_level=True
            )
            listener.start()

            try:
                futures = {
                    executor.submit(
                        convert_parquet_to_mgf,
                        file,
                        output_dir / f"{file.stem}.mgf",
                        batch_size,
                        nrows,
                    ): file
                    for file in files
                }
                for future in as_completed(futures):
                    n = futures[future]
                    try:
                        result = future.result()
                        logger.debug(f"Finished conversion of '{n.name}': {result} spectra written")
                        total_spectra_written += result
                    except Exception as e:
                        logger.error(f"Error converting '{n.name}': {e}")
                    finally:
                        progress.advance(task)
            finally:
                listener.stop()

    return total_spectra_written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert enriched PSM parquet file or directory to MGF format"
    )
    parser.add_argument(
        "input",
        type=Path,
        help="Path to input enriched parquet file or directory (output from psm_to_parquet.py)",
    )
    parser.add_argument(
        "output",
        type=Path,
        help=(
            "If the input is a directory, this should also be a directory where "
            "output .mgf files are exported to. Otherwise, this should "
            "be the path to the output .mgf file (e.g. 'output.mgf')"
        ),
    )
    parser.add_argument(
        "-n",
        "--nrows",
        type=int,
        default=None,
        help="Number of rows to convert (default: all rows)",
    )
    parser.add_argument(
        "-b",
        "--batch-size",
        type=int,
        default=1000,
        help="Number of spectra to process per batch (default: 1000)",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=NUM_WORKERS,
        help=f"Number of parallel workers to use for directory conversion (default: {NUM_WORKERS})",
    )

    args = parser.parse_args()

    # Validate input file or directory exists
    if not args.input.exists():
        logger.error(f"Input path not found: {args.input}")
        sys.exit(1)

    # Create output directory if it doesn't exist
    args.output.parent.mkdir(parents=True, exist_ok=True)

    return args


def main():
    setup_logging()

    args = parse_args()

    input_type: Literal["directory", "file"] = "directory" if args.input.is_dir() else "file"
    logger.info("Starting parquet to MGF conversion...")
    logger.info(f"Input {input_type}: {args.input}")
    logger.info(f"Output: {args.output}")
    logger.info(f"Batch size: {args.batch_size:,}")

    total_spectra = 0

    # convert single file
    if input_type == "file":
        try:
            total_spectra = convert_parquet_to_mgf(
                args.input,
                args.output,
                batch_size=args.batch_size,
                nrows=args.nrows,
            )

        except Exception as e:
            logger.error(f"Conversion failed: {e}")
            sys.exit(1)
    else:
        # convert multiple files in a directory in parallel
        total_spectra = convert_parallel(
            args.input,
            args.output,
            batch_size=args.batch_size,
            nrows=args.nrows,
            num_workers=args.workers,
        )

    logger.info("Conversion complete!")
    logger.info(f"Total spectra written: {total_spectra:,}")
    logger.info(f"Output file: '{args.output}'")


if __name__ == "__main__":
    main()
