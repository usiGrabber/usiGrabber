"""
mzID Parser Orchestration

Main parsing and import functions that orchestrate the parsing process
and handle database operations.
"""

import logging
from pathlib import Path

from pyteomics import mzid
from pyteomics.auxiliary import PyteomicsError
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session

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
    parse_psms_streaming,
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

    try:
        # Parse mzID file with retrieve_refs=False
        with mzid.MzIdentML(str(mzid_path), retrieve_refs=False) as reader:
            mzid_file = parse_mzid_metadata(reader, mzid_path, project_accession)

            # Phase 1: Parse DB sequences
            logger.debug("Phase 1: Parsing database sequences...")
            db_sequence_map = parse_db_sequences(reader)

            # Phase 2: Parse peptides
            logger.debug("Phase 2: Parsing peptides...")
            peptide_id_map, peptide_mods, peptides_batch = parse_peptides(reader)

            # Phase 3: Parse peptide evidence
            logger.debug("Phase 3: Parsing peptide evidence...")
            pe_id_map, peptide_evidence_batch = parse_peptide_evidence(reader, db_sequence_map)

            # Phase 4: Parse PSMs
            logger.debug("Phase 4: Parsing spectrum identification results...")
            psm_batch, junction_batch = parse_psms(
                reader,
                project_accession,
                mzid_file.id,
                peptide_id_map,
                pe_id_map,
            )

            # Phase 5: Link modifications
            logger.debug("Phase 5: Linking peptide modifications...")
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

            return parsed_data

    except PyteomicsError as e:
        error_msg = f"Failed to parse mzID file: {e}"
        logger.error(error_msg, exc_info=True)
        raise MzidParseError(error_msg) from e


def import_mzid(engine: Engine, mzid_path: Path, project_accession: str) -> ImportStats:
    """
    Import an mzIdentML file into the database using streaming batch processing.

    This function orchestrates parsing and database persistence:
    1. Parses metadata, peptides, and peptide evidence fully (typically small)
    2. Streams PSMs in batches to handle arbitrarily large files

    Memory efficiency: Only holds one batch of PSMs in memory at a time,
    enabling processing of arbitrarily large mzID files.

    Args:
            session: The database session to use for the import.
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

    # Validate file exists
    if not mzid_path.exists():
        error_msg = f"File not found: {mzid_path}"
        logger.error(error_msg)
        raise MzidParseError(error_msg)

    # Log file size
    file_size_bytes = mzid_path.stat().st_size
    file_size_mb = file_size_bytes / (1024 * 1024)
    logger.info(
        f"Importing mzID file (streaming mode): '{mzid_path.name}' ({file_size_mb:.2f} MB)",
        extra={
            "event": "mzid_import_start",
            "file_name": mzid_path.name,
            "file_size_bytes": file_size_bytes,
            "file_size_mb": file_size_mb,
            "project_accession": project_accession,
        },
    )

    try:
        # Debug: Verify engine type before creating session
        from sqlalchemy.engine.base import Engine as SQLAlchemyEngine

        if not isinstance(engine, SQLAlchemyEngine):
            error_msg = f"Expected Engine but got {type(engine).__name__}: {engine}"
            logger.error(error_msg)
            raise TypeError(error_msg)

        # Explicitly pass bind parameter to ensure engine is used correctly
        with Session(bind=engine) as session:
            # Parse mzID file with retrieve_refs=False
            with mzid.MzIdentML(str(mzid_path), retrieve_refs=False) as reader:
                # Phase 1: Parse metadata
                mzid_file = parse_mzid_metadata(reader, mzid_path, project_accession)
                session.add(mzid_file)
                session.flush()  # Get mzid_file.id

                # Phase 2: Parse DB sequences
                logger.debug("Phase 1: Parsing database sequences...")
                db_sequence_map = parse_db_sequences(reader)

                # Phase 3: Parse peptides
                logger.debug("Phase 2: Parsing peptides...")
                peptide_id_map, peptide_mods, peptides_batch = parse_peptides(reader)

                session.add_all(peptides_batch)
                session.flush()  # Get peptide IDs

                # Phase 4: Parse peptide modifications
                logger.debug("Phase 3: Linking peptide modifications...")
                mod_batch = link_modifications(peptide_mods)
                session.add_all(mod_batch)
                session.flush()

                # Phase 5: Parse peptide evidence
                logger.debug("Phase 4: Parsing peptide evidence...")
                pe_id_map, peptide_evidence_batch = parse_peptide_evidence(reader, db_sequence_map)

                session.add_all(peptide_evidence_batch)
                session.flush()

                # Update stats for completed phases
                stats.mark_parsing_complete()
                stats.peptide_count = len(peptide_id_map)
                stats.modification_count = len(mod_batch) if "mod_batch" in locals() else 0
                stats.peptide_evidence_count = len(pe_id_map)

                # Phase 6: Stream PSMs in batches
                logger.debug("Phase 5: Streaming PSMs...")
                BATCH_SIZE = 10000
                total_psms = 0
                total_junctions = 0

                for psm_batch, junction_batch in parse_psms_streaming(
                    reader,
                    project_accession,
                    mzid_file.id,
                    peptide_id_map,
                    pe_id_map,
                    batch_size=BATCH_SIZE,
                ):
                    # Add batch to session
                    session.add_all(psm_batch)
                    session.add_all(junction_batch)
                    session.flush()

                    # Track totals
                    total_psms += len(psm_batch)
                    total_junctions += len(junction_batch)

                    # Expunge objects to remove from identity map and free memory
                    session.expunge_all()

                    logger.debug(
                        f"Processed batch: {len(psm_batch)} PSMs, {len(junction_batch)} junctions "
                        f"(total: {total_psms} PSMs, {total_junctions} junctions)"
                    )

                stats.psm_count = total_psms
                stats.mark_complete()

                logger.info(
                    f"Completed import: {total_psms:,} PSMs, {total_junctions:,} junctions",
                    extra={"fileStats": stats.dict_summary()},
                )
                return stats

    except PyteomicsError as e:
        error_msg = f"Failed to parse mzID file: {e}"
        logger.error(error_msg, exc_info=True)
        stats.mark_failed(error_msg)
        raise MzidParseError(error_msg) from e
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
