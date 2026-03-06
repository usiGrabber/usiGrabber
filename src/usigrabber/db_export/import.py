import argparse
import logging
from pathlib import Path

import duckdb
from rich.progress import track

from spectrum_toolkit.logging_config import setup_logging
from usigrabber.db_export.export import OUTPUT_DIR, duckdb_con

# --- UPDATE WITH YOUR RESTORE DB CREDENTIALS ---
DB_NAME = "testdb_restore"  # The new, empty database
DB_USER = "your_user"
DB_PASS = "your_password"
DB_HOST = "127.0.0.1"
TABLE_NAME = "peptide_spectrum_matches"
INPUT_DIR = f"{TABLE_NAME}_chunks"
# ----------------------------------------------

logger = logging.getLogger("parquet_import")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import Parquet files back into a PostgreSQL table."
    )

    parser.add_argument(
        "--input-dir",
        type=OUTPUT_DIR.__class__,
        default=OUTPUT_DIR,
        help=f"Directory where Parquet files are stored (default: {OUTPUT_DIR})",
    )
    return parser.parse_args()


def import_table(con: duckdb.DuckDBPyConnection, table_name: str, table_dir: Path) -> None:
    logger.info(f"Importing '{table_name}'...")

    # This command reads all parquet files in the folder and inserts them
    # directly into the Postgres table.
    con.execute(f"COPY pg.{table_name} FROM '{table_dir}/*.parquet';")

    logger.debug(f"Finished importing '{table_dir}'.")


def main() -> None:
    setup_logging(Path(__file__).parent / "logs")

    args = parse_args()
    input_dir: Path = args.input_dir

    logging.info("Connecting to PostgreSQL")
    con = duckdb_con()

    tables = list(input_dir.iterdir())  # List all subdirectories (one per table)
    for table_dir in track(tables, description="Importing tables"):
        import_table(con, table_dir.name, table_dir)

    logging.info("Import complete!")


if __name__ == "__main__":
    main()
