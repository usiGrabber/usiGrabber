import logging
from pathlib import Path

import pandas as pd
from pandas.core.frame import DataFrame
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session

from usigrabber.file_parser.errors import TxtZipImportError, TxtZipParseError
from usigrabber.file_parser.models import ImportStats
from usigrabber.file_parser.txt_zip.helpers import get_txt_triples
from usigrabber.file_parser.txt_zip.models import ParsedTxtZipData
from usigrabber.file_parser.txt_zip.parsing_functions import (
    link_modifications,
    parse_peptide_evidence,
    parse_peptides,
    parse_psms,
)

logger = logging.getLogger(__name__)


def parse_txt_zip(
    evidence_path: Path, summary_path: Path, peptides_path: Path, project_accession: str
) -> ParsedTxtZipData:
    """
    Parse maxquant output txt.zip files by a triple of evidence, summary and peptides
    .txt-files and return all parsed data structures.

    This function performs pure parsing with no database operations.

    Args:
            evidence_path: Path to the evidence.txt file
            summary_path: Path to the summary.txt file
            peptides_path: Path to the peptides.txt file
            project_accession: PRIDE project accession

    Returns:
            ParsedTxtZipData containing all parsed records

    Raises:
            TxtZipParseError: If files cannot be read or parsed
    """

    logger.info(
        f"Parsing txt files: {evidence_path.name}, {summary_path.name}, {peptides_path.name}"
    )

    try:
        evidence: DataFrame = pd.read_csv(evidence_path, sep="\t")
        summary: DataFrame = pd.read_csv(summary_path, sep="\t")
        peptides: DataFrame = pd.read_csv(peptides_path, sep="\t")

        evidence = evidence.get(
            [
                "Sequence",
                "Modifications",
                "Modified sequence",
                "Raw file",
                "Charge",
                "m/z",
                "Mass",
                "MS/MS scan number",
            ],
            default=pd.DataFrame(),
        )
        evidence = evidence[evidence["MS/MS scan number"].notna()]
        peptides = peptides.get(
            [
                "Sequence",
                "Amino acid before",
                "Amino acid after",
                "Proteins",
                "Leading razor protein",
                "Start position",
                "End position",
            ],
            default=pd.DataFrame(),
        )
        summary = summary.get(
            ["Raw file", "Variable modifications", "Fixed modifications"],
            default=pd.DataFrame(),
        )

        logger.debug("Phase 1: Parsing peptides...")
        peptide_id_map, peptide_mods, peptides_batch = parse_peptides(evidence, peptides)

        logger.debug("Phase 2: Parsing peptide evidence...")
        pe_id_map, peptide_evidence_batch = parse_peptide_evidence(peptides)

        logger.debug("Phase 3: Parsing spectrum identification results...")
        psm_batch, junction_batch, search_mod_batch = parse_psms(
            evidence,
            summary,
            project_accession,
            peptide_id_map,
            pe_id_map,
        )

        logger.debug("Phase 4: Linking peptide modifications...")
        mod_batch = link_modifications(peptide_mods)

        # Return all parsed data
        parsed_data = ParsedTxtZipData(
            peptides=peptides_batch,
            peptide_modifications=mod_batch,
            peptide_evidence=peptide_evidence_batch,
            psms=psm_batch,
            psm_peptide_evidence_junctions=junction_batch,
            search_modifications=search_mod_batch,
        )

        return parsed_data

    except Exception as e:
        error_msg = f"Failed to parse a file from txt.zip: {e}"
        logger.error(error_msg, exc_info=True)
        raise TxtZipParseError(error_msg) from e


def import_txt_zip(
    engine: Engine,
    evidence_path: Path,
    summary_path: Path,
    peptides_path: Path,
    project_accession: str,
):
    """
    Import an maxquant output txt.zip file into the database.

    This function orchestrates parsing and database persistence:
    1. Parses the interesting txt files using parse_txt_zip()
    2. Persists all parsed data to the database in a single transaction

    Args:
            engine: the running db_engine
            evidence_path: Path to the evidence.txt file
            summary_path: Path to the summary.txt file
            peptides_path: Path to the peptides.txt file
            project_accession: PRIDE project accession

    Returns:
            ImportStats object containing import statistics and status

    Raises:
            TxtZipParseError: If a file cannot be read or parsed
            TxtZipImportError: If database operations fail
    """
    # Initialize stats tracker
    stats = ImportStats(
        file_name=evidence_path.name,
        project_accession=project_accession,
    )

    logger.info(
        f"Importing txt files: {evidence_path.name}, {summary_path.name}, {peptides_path.name}"
    )

    try:
        # Step 1: Parse the mzID file (pure parsing, no DB operations)
        parsed_data = parse_txt_zip(evidence_path, summary_path, peptides_path, project_accession)

        stats.mark_parsing_complete()

        # Step 2: Persist everything to the database
        with Session(engine) as session:
            session.add_all(parsed_data.peptides)
            session.add_all(parsed_data.peptide_evidence)
            session.add_all(parsed_data.psms)
            session.add_all(parsed_data.psm_peptide_evidence_junctions)
            session.add_all(parsed_data.peptide_modifications)
            session.add_all(parsed_data.search_modifications)
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


def import_all_txt_zip(
    engine: Engine,
    files: list[Path],
    project_accession: str,
    errors: int,
) -> tuple[int, int, bool]:
    """
    Import all available txt.zip files into the database as it is possible
    to encounter multiple in one project.

    This function orchestrates import_txt_zip for all available, valid triplets
    of interesting .txt-files

    Args:
            engine: the running db_engine
            files: all files currently marked as interesting
            project_accession: PRIDE project accession
            errors: the number of already encountered errors

    Returns:
            processed_files: the number of already processed files
            errors: the number of already encountered errors
            fully_processed: flag, whether the project is already fully processed

    Raises:
            TxtZipParseError: If a file cannot be read or parsed
            TxtZipImportError: If database operations fail
    """
    txt_triples = get_txt_triples(files)
    processed_files = 0
    fully_processed: bool = False
    for triple in txt_triples:
        evidence_path, summary_path, peptides_path = triple
        try:
            stats = import_txt_zip(
                engine,
                evidence_path,
                summary_path,
                peptides_path,
                project_accession,
            )
            duration_str = (
                f"{stats.duration_seconds:.1f}s" if stats.duration_seconds is not None else "N/A"
            )
            logger.info(
                f"Imported {stats.psm_count:,} PSMs from {evidence_path.name} ({duration_str})"
            )
            processed_files += 1
        except TxtZipParseError as e:
            logger.warning(f"Skipping malformed txt file {evidence_path.name}: {e}")
            continue
        except TxtZipImportError as e:
            logger.error(
                f"Failed to import txt file {evidence_path.name}: {e}",
                exc_info=True,
                stack_info=True,
                extra={
                    "txt_evidence_file": str(evidence_path),
                    "txt_summary_file": str(summary_path),
                    "txt_peptides_file": str(peptides_path),
                    "project_accession": project_accession,
                },
            )
            errors += 1
            continue

    if processed_files == len(txt_triples):
        fully_processed = True
    return processed_files, errors, fully_processed
