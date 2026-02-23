import argparse
import logging
from pathlib import Path

from mod_prediction.logging_config import setup_logging
from usigrabber.db_export.export import OUTPUT_DIR, duckdb_con

logger = logging.getLogger("validate_db_export")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate that the number of rows in the exported Parquet files matches the original PostgreSQL table."
    )

    parser.add_argument(
        "--table-name",
        type=str,
        default="projects",
        help="Name of the table to validate (e.g. 'projects')",
    )

    parser.add_argument(
        "--output-dir",
        type=OUTPUT_DIR.__class__,
        default=OUTPUT_DIR,
        help=f"Directory where Parquet files are stored (default: {OUTPUT_DIR})",
    )
    return parser.parse_args()


def main() -> int:
    setup_logging(Path(__file__).parent / "logs")

    args = parse_args()
    output_dir = args.output_dir
    table_name = args.table_name

    table_dir = output_dir / table_name

    if not table_dir.exists():
        logger.error(f"Expected output directory '{table_dir}' does not exist.")
        return 1

    logger.info("Starting validation process...")

    # 1. Connect to DuckDB and attach Postgres
    con = duckdb_con()

    # 2. Count the rows in the original PostgreSQL table
    logger.info(f"1/2: Counting rows in PostgreSQL table '{table_name}'...")
    # fetchone()[0] simply extracts the raw number from the query result
    pg_count = con.execute(f"SELECT COUNT(*) FROM pg.{table_name}").fetchone()[0]

    # 3. Count the rows across ALL exported Parquet files simultaneously
    logger.info(f"2/2: Counting rows in Parquet chunks inside '{table_dir}'...")
    # The *.parquet wildcard tells DuckDB to union all files in the folder automatically
    pq_count = con.execute(f"SELECT COUNT(*) FROM '{table_dir}/*.parquet'").fetchone()[0]

    # 4. Print the comparison
    logger.info("=" * 40)
    logger.info("VALIDATION RESULTS")
    logger.info("=" * 40)
    logger.info(f"PostgreSQL Rows: {pg_count:,}")
    logger.info(f"Parquet Rows:    {pq_count:,}")
    logger.info("-" * 40)

    if pg_count == pq_count:
        logger.info("✅ SUCCESS: Row counts match perfectly. No data was dropped!")
        return 0

    difference = abs(pg_count - pq_count)
    logger.error(f"❌ ERROR: Row counts DO NOT match. Difference of {difference:,} rows.")
    return 1


if __name__ == "__main__":
    exit(main())
