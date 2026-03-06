"""
Query SQL files and export results to both CSV and Parquet formats.
"""

import argparse
import logging
import os
from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID

import pandas as pd
import pyarrow as pa
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
from sqlalchemy import create_engine, text

from spectrum_toolkit.logging_config import setup_logging

logger = logging.getLogger("db-fetcher")


def build_postgres_url() -> str:
    """
    Build PostgreSQL connection URL from environment variables.
    Information from DB_URL takes precedence over individual components.
    """

    url = urlparse(os.environ.get("DB_URL", ""))
    assert url.scheme in ("postgresql", "postgres", "postgresql+psycopg"), (
        "DB_URL must start with postgresql://, postgres://, or postgresql+psycopg://."
    )

    # user and pwd can also be empty if connection is anonymous
    user = url.username or os.environ.get("POSTGRES_USER")
    pwd = url.password or os.environ.get("POSTGRES_PASSWORD")
    assert user is not None, "Postgres user must be set via DB_URL or POSTGRES_USER"
    assert pwd is not None, "Postgres password must be set via DB_URL or POSTGRES_PASSWORD"

    host = url.hostname or os.environ.get("POSTGRES_HOST", "localhost")
    port = url.port or os.environ.get("POSTGRES_PORT", "5432")
    name = url.path.lstrip("/") or os.environ.get("POSTGRES_DB", "usigrabber")

    # Construct the final URL
    # we want to force psycopg(3) as the driver, as the default is the predecessor (psycopg2)
    return f"postgresql+psycopg://{user}:{pwd}@{host}:{port}/{name}"


def sql_to_file(
    sql_file: Path,
    output_base: Path,
    connection_string: str,
    chunksize: int = 100_000,
    no_parquet: bool = False,
) -> None:
    """
    Export SQL query results to both CSV and Parquet files in chunks.

    Uses SQLAlchemy with stream_results=True for server-side cursors, which
    keeps memory usage low even for long-running queries that return many rows.

    Args:
        sql_file: Path to SQL file containing the query
        output_base: Base path for output files (extensions will be added)
        connection_string: SQLAlchemy-compatible database URL
        chunksize: Number of rows per chunk (default: 100,000)
        no_parquet: Skip Parquet output
    """
    query = sql_file.read_text()

    # Generate output filenames
    csv_file = output_base.with_suffix(".csv")
    parquet_file = output_base.with_suffix(".parquet")

    # Create parent directory if it doesn't exist
    output_base.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Executing query from {sql_file.name}...")
    logger.info("Will export to:")
    logger.info(f"  CSV: {csv_file}")
    if not no_parquet:
        logger.info(f"  Parquet: {parquet_file}")

    parquet_writer: pq.ParquetWriter | None = None
    first_chunk = True
    total_rows = 0

    engine = create_engine(connection_string)

    try:
        logger.info("Connecting to database...")
        # stream_results=True uses a server-side cursor so rows are fetched
        # lazily; the full result set is never buffered client-side.
        with engine.connect().execution_options(stream_results=True) as conn:
            logger.info("Connected to database, executing query...")
            result = conn.execute(text(query))
            logger.info("Query executed, streaming results...")

            columns: list[str] = list(result.keys())

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeElapsedColumn(),
            ) as progress:
                task = progress.add_task("Writing rows to file", total=None)

                for partition in result.partitions(chunksize):
                    # Convert to pandas DataFrame
                    chunk = pd.DataFrame(partition, columns=pd.Index(columns))

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
        engine.dispose()


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
    try:
        connection_string = build_postgres_url()
    except AssertionError as e:
        logger.error("Failed to build database connection URL: %s", e)
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
