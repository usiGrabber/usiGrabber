"""
mzID Parser Orchestration

Main parsing and import functions that orchestrate the parsing process
and handle database operations.
"""

import logging
from pathlib import Path

from pyteomics import mzid
from pyteomics.auxiliary import PyteomicsError
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session

from usigrabber.db.engine import load_db_engine
from usigrabber.file_parser.errors import MzidImportError, MzidParseError
from usigrabber.file_parser.models import ImportStats
from usigrabber.file_parser.mzid.models import ParsedMzidData
from usigrabber.file_parser.mzid.parsing_functions import (
    link_modifications,
    parse_db_sequences,
    parse_mzid_metadata,
    parse_peptide_evidence,
    parse_peptides,
    parse_psms,
)

logger = logging.getLogger(__name__)


def parse_mzid_file(mzid_path: Path, project_accession: str) -> ParsedMzidData:
    """
    Parse an mzIdentML file and return all parsed data structures.

    This function performs pure parsing with no database operations.

    Args:
            mzid_path: Path to the mzIdentML file
            project_accession: PRIDE project accession

    Returns:
            ParsedMzidData containing all parsed records

    Raises:
            MzidParseError: If file cannot be read or parsed
    """
    # Validate file exists
    if not mzid_path.exists():
        error_msg = f"File not found: {mzid_path}"
        logger.error(error_msg)
        raise MzidParseError(error_msg)

    logger.info(f"Parsing mzID file: {mzid_path.name}")

    try:
        # Parse mzID file with retrieve_refs=False
        with mzid.MzIdentML(str(mzid_path), retrieve_refs=False) as reader:
            mzid_file = parse_mzid_metadata(reader, mzid_path, project_accession)

            # Phase 1: Parse DB sequences
            logger.debug("\nPhase 1: Parsing database sequences...")
            db_sequence_map = parse_db_sequences(reader)

            # Phase 2: Parse peptides
            logger.debug("\nPhase 2: Parsing peptides...")
            peptide_id_map, peptide_mods, peptides_batch = parse_peptides(reader)

            # Phase 3: Parse peptide evidence
            logger.debug("\nPhase 3: Parsing peptide evidence...")
            pe_id_map, peptide_evidence_batch = parse_peptide_evidence(reader, db_sequence_map)

            # Phase 4: Parse PSMs
            logger.debug("\nPhase 4: Parsing spectrum identification results...")
            psm_batch, junction_batch = parse_psms(
                reader,
                project_accession,
                mzid_file.id,
                peptide_id_map,
                pe_id_map,
            )

            # Phase 5: Link modifications
            logger.debug("\nPhase 5: Linking peptide modifications...")
            mod_batch = link_modifications(peptide_mods)

            # Return all parsed data
            parsed_data = ParsedMzidData(
                mzid_file=mzid_file,
                peptides=peptides_batch,
                peptide_modifications=mod_batch,
                peptide_evidence=peptide_evidence_batch,
                psms=psm_batch,
                psm_peptide_evidence_junctions=junction_batch,
            )

            logger.debug(
                f"Parsing complete: {len(peptides_batch)} peptides, "
                f"{len(mod_batch)} modifications, "
                f"{len(peptide_evidence_batch)} evidence, "
                f"{len(psm_batch)} PSMs"
            )

            return parsed_data

    except PyteomicsError as e:
        error_msg = f"Failed to parse mzID file: {e}"
        logger.error(error_msg, exc_info=True)
        raise MzidParseError(error_msg) from e


def import_mzid(mzid_path: Path, project_accession: str) -> ImportStats:
    """
    Import an mzIdentML file into the database.

    This function orchestrates parsing and database persistence:
    1. Parses the mzID file using parse_mzid_file()
    2. Persists all parsed data to the database in a single transaction

    Args:
            mzid_path: Path to the mzIdentML file
            project_accession: PRIDE project accession

    Returns:
            ImportStats object containing import statistics and status

    Raises:
            MzidParseError: If file cannot be read or parsed
            MzidImportError: If database operations fail
    """
    # Initialize stats tracker
    stats = ImportStats(
        file_name=mzid_path.name,
        project_accession=project_accession,
    )

    logger.info(f"Importing mzID file: {mzid_path.name}")

    try:
        # Step 1: Parse the mzID file (pure parsing, no DB operations)
        parsed_data = parse_mzid_file(mzid_path, project_accession)

        # Step 2: Persist everything to the database
        engine = load_db_engine()
        with Session(engine) as session:
            session.add(parsed_data.mzid_file)
            session.add_all(parsed_data.peptides)
            session.add_all(parsed_data.peptide_evidence)
            session.add_all(parsed_data.psms)
            session.add_all(parsed_data.psm_peptide_evidence_junctions)
            session.add_all(parsed_data.peptide_modifications)
            session.commit()

        # Update stats
        stats.peptide_count = len(parsed_data.peptides)
        stats.modification_count = len(parsed_data.peptide_modifications)
        stats.peptide_evidence_count = len(parsed_data.peptide_evidence)
        stats.psm_count = len(parsed_data.psms)
        stats.mark_complete()

        logger.info(stats.summary())
        return stats

    except MzidParseError as e:
        # Re-raise parsing errors with updated stats
        stats.mark_failed(str(e))
        raise
    except SQLAlchemyError as e:
        error_msg = f"Database error during import: {e}"
        logger.error(error_msg, exc_info=True)
        stats.mark_failed(error_msg)
        raise MzidImportError(error_msg) from e
    except Exception as e:
        error_msg = f"Unexpected error during import: {e}"
        logger.error(error_msg, exc_info=True)
        stats.mark_failed(error_msg)
        raise MzidImportError(error_msg) from e
