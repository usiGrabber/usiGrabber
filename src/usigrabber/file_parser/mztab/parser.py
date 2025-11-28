# logger
import logging
from pathlib import Path

from pyteomics import mztab
from pyteomics.auxiliary import PyteomicsError
from sqlalchemy import insert

from usigrabber.db.engine import load_db_engine
from usigrabber.db.schema import Peptide, PeptideSpectrumMatch
from usigrabber.file_parser.models import ImportStats
from usigrabber.file_parser.mzid.models import ParsedMztabData
from usigrabber.file_parser.mztab.parsing_functions import extract_mztab_data

logger = logging.getLogger(__name__)


def parse_mztab_file(mztab_path: Path, project_accession: str) -> ParsedMztabData:
    """
    Parse an mzTab file and extract relevant data.

    Args:
        mztab_path (Path): Path to the mzTab file.
        project_accession (str): Project accession identifier.

    Returns:
        ParsedMztabData: Parsed data from the mzTab file.
    """
    # Validate file exists
    if not mztab_path.exists():
        error_msg = f"File not found: {mztab_path}"
        logger.error(error_msg)

    logger.debug(f"Parsing mzTab file: {mztab_path.name}")

    try:
        file = mztab.MzTab(str(mztab_path))
        logger.debug("\nPhase 1: Parsing spectrum identification results...")
        psm_batch, peptide_batch = extract_mztab_data(file, project_accession)

        return ParsedMztabData(
            psms=psm_batch,
            peptides=peptide_batch,
        )

    except PyteomicsError as e:
        error_msg = f"Failed to parse mzID file: {e}"
        logger.error(error_msg, exc_info=True)


def import_mztab(mztab_path: Path, project_accession: str) -> ImportStats:
    stats = ImportStats(
        file_name=mztab_path.name,
        project_accession=project_accession,
    )
    logger.debug(f"Importing mzTab file: {mztab_path.name}")

    try:
        parsed = parse_mztab_file(mztab_path, project_accession)
        stats.mark_parsing_complete()

        psm_rows = parsed.psms
        peptide_rows = parsed.peptides

        engine = load_db_engine()
        logger.info(f"Using DB: {engine.url}")

        # Optional: massive SQLite speed boost
        with engine.begin() as conn:
            conn.exec_driver_sql("PRAGMA synchronous = OFF;")
            conn.exec_driver_sql("PRAGMA journal_mode = MEMORY;")

            # Bulk insert
            if peptide_rows:
                conn.execute(insert(Peptide), peptide_rows)

            if psm_rows:
                conn.execute(insert(PeptideSpectrumMatch), psm_rows)

        stats.peptide_count = len(peptide_rows)
        stats.psm_count = len(psm_rows)

        stats.mark_complete()
        logger.debug(stats.summary())

        return stats

    except Exception as e:
        logger.error(f"Import failed: {e}", exc_info=True)
        stats.mark_failed(str(e))
        raise
