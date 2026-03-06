import argparse
import logging
import os
import tarfile
from pathlib import Path
from urllib.parse import urlparse

import duckdb
from dotenv import load_dotenv
from rich.progress import track

from spectrum_toolkit.logging_config import setup_logging

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger("parquet_export")

# === DB CREDENTIALS ===
DB_URL = os.getenv("DB_URL")
parsed_url = urlparse(DB_URL)
DB_NAME = parsed_url.path[1:]  # Remove leading slash
DB_USER = os.getenv("POSTGRES_USER") or parsed_url.username
DB_PASS = os.getenv("POSTGRES_PASSWORD") or parsed_url.password
DB_HOST = parsed_url.hostname
DB_PORT = parsed_url.port or 5432
SCHEMA = "public"
PG_URI = f"dbname={DB_NAME} user={DB_USER} password={DB_PASS} host={DB_HOST} port={DB_PORT}"
# ---------------------------------------------------

OUTPUT_DIR = Path("parquet_exports")


def duckdb_con() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection and attach the PostgreSQL database."""
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{PG_URI}' AS pg (TYPE POSTGRES);")
    return con


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export PostgreSQL tables to Parquet files using DuckDB."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help=f"Directory to save Parquet files (default: {OUTPUT_DIR})",
    )

    parser.add_argument(
        "--file-size",
        type=int,
        default=75,
        help="Target file size in GB for Parquet files (default: 75GB). Adjust based on your system's memory and performance characteristics.",
    )

    return parser.parse_args()


def main() -> None:
    setup_logging(Path(__file__).parent / "logs")

    args = parse_args()
    output_dir = args.output_dir

    # check if output directory exists
    if not output_dir.exists():
        logger.warning(f"Output directory '{output_dir}' does not exist. Creating it now...")
        output_dir.mkdir(parents=True, exist_ok=True)

    # check if output directory is empty
    if any(output_dir.iterdir()):
        raise FileExistsError(
            f"Output directory '{output_dir}' is not empty. Please clear it before running the export to avoid overwriting existing files."
        )

    logger.info("Initializing DuckDB and connecting to PostgreSQL...")

    # 1. Connect to DuckDB and load the Postgres extension
    con = duckdb_con()

    # CRITICAL: Force 1 thread.
    # This ensures a linear stream from Postgres and prevents Out-Of-Memory errors
    # when DuckDB calculates chunk boundaries on massive tables.
    con.execute("SET threads = 1;")

    # 3. Fetch all table names dynamically from the information_schema
    tables_query = f"""
        SELECT table_name
        FROM pg.information_schema.tables
        WHERE table_schema = '{SCHEMA}'
        AND table_type = 'BASE TABLE';
    """
    tables = con.execute(tables_query).fetchall()

    # 4. Loop through the tables and export them
    if not tables:
        logger.warning(
            f"No tables found in schema '{SCHEMA}'. Check your connection or schema name."
        )
        return

    logger.info(f"Found {len(tables)} tables. Starting export to Parquet...")

    for table_tuple in track(tables, description="Exporting tables to Parquet"):
        table_name = table_tuple[0]
        output_file = output_dir / f"{table_name}"

        logger.info(f"Exporting '{table_name}'...")

        try:
            # The COPY command streams the table directly to disk
            con.execute(f"""
                COPY pg.{table_name}
                TO '{output_file}'
                (FORMAT PARQUET, CODEC 'ZSTD', FILE_SIZE_BYTES '{args.file_size}GB');
            """)
        except Exception as e:
            logger.error(f"Failed to export {table_name}: {e}")

    logger.info("Export process complete!")


def _create_tarball(folder_list, output_file):
    """Helper to create the tar archive."""
    with tarfile.open(output_file, "w") as tar:
        for folder in folder_list:
            # arcname ensures the internal structure starts at the folder name
            tar.add(folder, arcname=folder.name)
    logger.info(f"✅ Created {output_file}")


def bundle_exports() -> None:
    """Groups small folders into one archive and large folders into individual ones."""
    setup_logging(Path(__file__).parent / "logs")

    parser = argparse.ArgumentParser(
        description="Bundle exported Parquet folders into tar archives. "
        "Large tables are archived individually, while small tables are grouped together."
    )
    parser.add_argument(
        "source_dir",
        type=Path,
        default=OUTPUT_DIR,
        help=f"Directory where the exported Parquet files are stored (default: {OUTPUT_DIR})",
    )

    parser.add_argument(
        "dest_dir",
        type=Path,
        default=OUTPUT_DIR.parent / "bundled-export",
        help=f"Directory where the bundled archives are stored (default: {OUTPUT_DIR.parent / 'bundled-export'})",
    )

    parser.add_argument(
        "--size-threshold",
        type=int,
        default=20,
        help="Size threshold in GB to determine if a folder is considered 'large' (default: 20GB). Adjust based on your needs.",
    )

    args = parser.parse_args()

    source_path = Path(args.source_dir)
    if not source_path.exists() or not source_path.is_dir():
        logger.error(f"Source directory '{source_path}' does not exist or is not a directory.")
        exit(1)

    dest_path = Path(args.dest_dir)
    dest_path.mkdir(parents=True, exist_ok=True)

    GB_LIMIT = args.size_threshold * (2**30)  # User-defined threshold in bytes
    small_folders: list[Path] = []

    # 1. Analyze folders
    for entry in source_path.iterdir():
        if entry.is_dir():
            # Calculate folder size
            folder_size = sum(f.stat().st_size for f in entry.glob("**/*") if f.is_file())

            if folder_size >= GB_LIMIT:
                logger.info(f"📦 Archiving Large Table: {entry.name} ({folder_size / 1e9:.2f} GB)")
                _create_tarball([entry], dest_path / f"{entry.name}.tar")
            else:
                small_folders.append(entry)

    # 2. Bundle small folders
    if small_folders:
        logger.info(
            f"📦 Bundling {len(small_folders)} small tables into small_tables_collection.tar"
        )
        _create_tarball(small_folders, dest_path / "small_tables_collection.tar")


if __name__ == "__main__":
    main()
