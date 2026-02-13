"""
Query SQL files and export results to both CSV and Parquet formats.
"""

import argparse
import logging
import os
from pathlib import Path
from typing import LiteralString, cast
from uuid import UUID

import pandas as pd
import psycopg
import pyarrow.parquet as pq
from dotenv import load_dotenv
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

from mod_prediction.logging_config import setup_logging

logger = logging.getLogger("db-fetcher")


def sql_to_file(
    sql_file: Path,
    output_base: Path,
    connection_string: str,
    chunksize: int = 100_000,
    no_parquet: bool = False,
) -> None:
    """
    Export SQL query results to both CSV and Parquet files in chunks.

    Args:
        sql_file: Path to SQL file containing the query
        output_base: Base path for output files (extensions will be added)
        connection_string: PostgreSQL connection string
        chunksize: Number of rows per chunk (default: 100,000)
    """
    # Read SQL query and cast to LiteralString for type checker
    query = cast(LiteralString, sql_file.read_text())

    # Generate output filenames
    csv_file = output_base.with_suffix(".csv")
    parquet_file = output_base.with_suffix(".parquet")

    # Create parent directory if it doesn't exist
    output_base.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Executing query from {sql_file.name}...")
    logger.info("Will export to:")
    logger.info(f"  CSV: {csv_file}")
    logger.info(f"  Parquet: {parquet_file}")

    parquet_writer: pq.ParquetWriter | None = None
    first_chunk = True
    total_rows = 0

    try:
        logger.info("Connecting to database...")
        with psycopg.connect(connection_string) as conn, conn.cursor() as cursor:
            logger.info("Connected to database, executing query...")
            cursor.execute(query)
            logger.info("Query executed, fetching results...")

            # Fetch column names
            columns: list[str] = (
                [desc[0] for desc in cursor.description] if cursor.description else []
            )

            with Progress(
                SpinnerColumn(),  # Adds a spinning icon
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),  # This will pulse back and forth
                # transient=True          # Optional: clears the bar from screen when done
                MofNCompleteColumn(),  # <--- This shows the current count
                TimeElapsedColumn(),  # <--- Shows how long it's been running
            ) as progress:
                task = progress.add_task("Writing rows to file", total=None)

                while True:
                    rows = cursor.fetchmany(chunksize)
                    if not rows:
                        break

                    # Convert to pandas DataFrame
                    chunk = pd.DataFrame(rows, columns=pd.Index(columns))

                    # Convert UUID columns to strings for compatibility
                    for col in chunk.columns:
                        if len(chunk[col]) > 0 and isinstance(chunk[col].iloc[0], UUID):
                            chunk[col] = chunk[col].astype(str)

                    total_rows += len(chunk)

                    # Write to CSV
                    chunk.to_csv(
                        csv_file, mode="w" if first_chunk else "a", header=first_chunk, index=False
                    )

                    # Write to Parquet
                    if not no_parquet:
                        table = pa.Table.from_pandas(chunk, preserve_index=False)
                        if parquet_writer is None:
                            parquet_writer = pq.ParquetWriter(parquet_file, table.schema)
                        parquet_writer.write_table(table)

                    first_chunk = False

                    progress.advance(task, len(chunk))

        logger.info(f"Exported {total_rows:,} rows to:")
        logger.info(f"  {csv_file}")
        if not no_parquet:
            logger.info(f"  {parquet_file}")

    finally:
        if parquet_writer is not None:
            parquet_writer.close()


def main() -> None:
    """CLI entry point for SQL to CSV and Parquet conversion."""
    setup_logging()
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Export SQL query results to both CSV and Parquet formats"
    )
    parser.add_argument(
        "sql_file",
        type=Path,
        help="Path to SQL file containing the query",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help=(
            "Base output path (extensions .csv and .parquet will be added, default: <sql_filename>)"
        ),
    )
    parser.add_argument(
        "-c",
        "--chunksize",
        type=int,
        default=100_000,
        help="Number of rows per chunk (default: 100,000)",
    )
    parser.add_argument(
        "--no-parquet",
        action="store_true",
        default=False,
        help="Do not export to Parquet format",
    )

    args = parser.parse_args()

    # Determine output base path (without extension)
    if args.output is None:
        args.output = args.sql_file.with_suffix("")
    else:
        # Remove any existing extension to use as base
        args.output = args.output.with_suffix("")

    # Get connection string
    connection_string = os.getenv("DATABASE_URL", "")
    if not connection_string:
        logger.error("No database connection string provided. Set DATABASE_URL env variable.")
        return

    sql_to_file(
        sql_file=args.sql_file,
        output_base=args.output,
        connection_string=connection_string,
        chunksize=args.chunksize,
        no_parquet=args.no_parquet,
    )


if __name__ == "__main__":
    main()
