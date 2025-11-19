import logging
from os import listdir
from os.path import isfile, join
from pathlib import Path

import pandas as pd
from pandas.core.frame import DataFrame
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session

from usigrabber.db.engine import load_db_engine
from usigrabber.file_parser.errors import TxtZipImportError, TxtZipParseError
from usigrabber.file_parser.models import ImportStats
from usigrabber.file_parser.txt_zip.models import ParsedTxtZipData
from usigrabber.file_parser.txt_zip.parsing_functions import (
    link_modifications,
    parse_peptide_evidence,
    parse_peptides,
    parse_psms,
)

logger = logging.getLogger(__name__)


def parse_txt_zip(txt_dir_path: Path, project_accession: str) -> ParsedTxtZipData:
    if not txt_dir_path.exists():
        error_msg = f"Directory not found: {txt_dir_path}"
        logger.error(error_msg)
        raise TxtZipParseError(error_msg)

    logger.info(f"Parsing txt directory: {txt_dir_path.name}")

    try:
        evidence = summary = peptides = None
        files = [f for f in listdir(txt_dir_path) if isfile(join(txt_dir_path, f))]
        for file in files:
            filepath = txt_dir_path / file
            if file == "evidence.txt":
                evidence: DataFrame = pd.read_csv(filepath, sep="\t")
            elif file == "summary.txt":
                summary: DataFrame = pd.read_csv(filepath, sep="\t")
            elif file == "peptides.txt":
                peptides: DataFrame = pd.read_csv(filepath, sep="\t")

        assert evidence is not None, "evidence.txt file is required"

        # Phase 1:
        logger.debug("\nPhase 1: Parsing peptides...")
        peptide_id_map, peptide_mods, peptides_batch = parse_peptides(evidence, peptides)

        logger.debug("\nPhase 2: Parsing peptide evidence...")
        if peptides is not None:
            pe_id_map, peptide_evidence_batch = parse_peptide_evidence(peptides)

        logger.debug("\nPhase 3: Parsing spectrum identification results...")
        psm_batch, junction_batch = parse_psms(
            evidence,
            summary,
            project_accession,
            peptide_id_map,
            pe_id_map,
        )  # spectrum id?

        logger.debug("\nPhase 4: Linking peptide modifications...")
        mod_batch = link_modifications(peptide_mods)

        # Return all parsed data
        parsed_data = ParsedTxtZipData(
            peptides=peptides_batch,
            peptide_modifications=mod_batch,
            peptide_evidence=peptide_evidence_batch,
            psms=psm_batch,
            psm_peptide_evidence_junctions=junction_batch,
        )

        return parsed_data

    except Exception as e:
        error_msg = f"Failed to parse evidence.txt file: {e}"
        logger.error(error_msg, exc_info=True)
        raise TxtZipParseError(error_msg) from e


def import_txt_zip(txt_dir_path: Path, project_accession: str):
    # Initialize stats tracker
    stats = ImportStats(
        file_name=txt_dir_path.name,
        project_accession=project_accession,
    )

    logger.info(f"Importing txt directory: {txt_dir_path.name}")

    try:
        # Step 1: Parse the mzID file (pure parsing, no DB operations)
        parsed_data = parse_txt_zip(txt_dir_path, project_accession)

        stats.mark_parsing_complete()

        # Step 2: Persist everything to the database
        engine = load_db_engine()
        with Session(engine) as session:
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

        logger.debug(stats.summary())
        return stats

    except TxtZipParseError as e:
        # Re-raise parsing errors with updated stats
        stats.mark_failed(str(e))
        raise
    except SQLAlchemyError as e:
        error_msg = f"Database error during import: {e}"
        logger.error(error_msg, exc_info=True)
        stats.mark_failed(error_msg)
        raise TxtZipImportError(error_msg) from e
    except Exception as e:
        error_msg = f"Unexpected error during import: {e}"
        logger.error(error_msg, exc_info=True)
        stats.mark_failed(error_msg)
        raise TxtZipImportError(error_msg) from e
