import logging
from pathlib import Path

import pandas as pd
from pandas import DataFrame
from sqlalchemy import insert
from sqlalchemy.engine import Engine

from usigrabber.db.schema import (
    Modification,
    ModifiedPeptide,
    ModifiedPeptideModificationJunction,
    PeptideEvidence,
    PeptideSpectrumMatch,
    PSMPeptideEvidence,
    SearchModification,
)
from usigrabber.file_parser.base import BaseFileParser, register_parser
from usigrabber.file_parser.errors import (
    TxtZipImportError,
    TxtZipParseError,
)
from usigrabber.file_parser.helpers import get_db_insert_function
from usigrabber.file_parser.models import ImportStats, ParsedTxtZipData
from usigrabber.file_parser.txt_zip.parsing_functions import (
    parse_peptide_evidence,
    parse_peptides_and_modifications,
    parse_psms,
)

logger = logging.getLogger(__name__)


@register_parser
class TxtZipFileParser(BaseFileParser):
    @property
    def file_extensions(self) -> set[str]:
        return {".txt", ""}

    @property
    def format_name(self) -> str:
        return "txt"

    def parse_file(self, path, project_accession: str) -> ParsedTxtZipData:
        """Parse txt.zip files by triples of evidence, summary and peptides .txt-files.
        Args:
                path: triple of Paths to the txt files
                project_accession: PRIDE project accession
        Returns:
                List of ParsedTxtZipData containing all parsed records
        """
        evidence_path, summary_path, peptides_path = (
            path if isinstance(path, tuple) else (path, path, path)
        )
        try:
            parsed_data = parse_txt_zip(
                evidence_path,
                summary_path,
                peptides_path,
                project_accession,
            )
            return parsed_data

        except TxtZipParseError as e:
            error_msg = f"Failed to parse txt files: {e}"
            logger.error(error_msg, exc_info=True)
            raise TxtZipParseError(error_msg) from e

    def persist(self, engine: Engine, parsed: ParsedTxtZipData, stats: ImportStats):
        """Persist parsed txt data to the database with debug logging."""
        logger.debug(f"Persisting txt data to database for file '{stats.file_name}'")
        import time

        try:
            insert_func = get_db_insert_function(engine)
            with engine.begin() as conn:
                if parsed.modified_peptides:
                    # Sort by primary key to minimize deadlocks
                    sorted_peptides = sorted(parsed.modified_peptides, key=lambda x: x["id"])
                    # Use INSERT OR IGNORE (SQLite) or INSERT ON CONFLICT DO NOTHING (PostgreSQL)
                    # for cross-file deduplication based on primary key
                    stmt = insert_func(ModifiedPeptide).on_conflict_do_nothing()
                    db_start = time.time()
                    conn.execute(stmt, sorted_peptides)
                    db_time = time.time() - db_start
                    stats.peptide_count = len(sorted_peptides)
                    logger.info(f"[{stats.file_name}] ModifiedPeptide: {db_time:.3f}s total")

                if parsed.modifications:
                    # Sort by primary key to minimize deadlocks
                    sorted_modifications = sorted(parsed.modifications, key=lambda x: x["id"])
                    # Use INSERT OR IGNORE (SQLite) or INSERT ON CONFLICT DO NOTHING (PostgreSQL)
                    # for cross-file deduplication based on unique constraint
                    stmt = insert_func(Modification).on_conflict_do_nothing()
                    db_start = time.time()
                    conn.execute(stmt, sorted_modifications)
                    db_time = time.time() - db_start
                    stats.modification_count = len(sorted_modifications)
                    logger.info(f"[{stats.file_name}] Modification: {db_time:.3f}s total")

                if parsed.modified_peptide_modification_junctions:
                    # Sort by composite primary key to minimize deadlocks
                    sorted_junctions = sorted(
                        parsed.modified_peptide_modification_junctions,
                        key=lambda x: (x["modified_peptide_id"], x["modification_id"]),
                    )
                    # Use INSERT OR IGNORE (SQLite) or INSERT ON CONFLICT DO NOTHING (PostgreSQL)
                    # for cross-file deduplication based on composite primary key
                    stmt = insert_func(ModifiedPeptideModificationJunction).on_conflict_do_nothing()
                    db_start = time.time()
                    conn.execute(stmt, sorted_junctions)
                    db_time = time.time() - db_start
                    logger.info(
                        f"[{stats.file_name}] ModifiedPeptideModificationJunction: {db_time:.3f}s total"
                    )

                if parsed.peptide_evidence:
                    # Sort by primary key to minimize deadlocks
                    sorted_evidence = sorted(parsed.peptide_evidence, key=lambda x: x["id"])
                    db_start = time.time()
                    conn.execute(insert(PeptideEvidence), sorted_evidence)
                    db_time = time.time() - db_start
                    stats.peptide_evidence_count = len(sorted_evidence)
                    logger.info(f"[{stats.file_name}] PeptideEvidence: {db_time:.3f}s total ")

                if parsed.psms:
                    # Sort by primary key to minimize deadlocks
                    sorted_psms = sorted(parsed.psms, key=lambda x: x["id"])
                    db_start = time.time()
                    conn.execute(insert(PeptideSpectrumMatch), sorted_psms)
                    db_time = time.time() - db_start
                    stats.psm_count = len(sorted_psms)
                    logger.info(f"[{stats.file_name}] PeptideSpectrumMatch: {db_time:.3f}s total")

                if parsed.psm_peptide_evidence_junctions:
                    # Sort by primary key to minimize deadlocks
                    sorted_pe_junctions = sorted(
                        parsed.psm_peptide_evidence_junctions, key=lambda x: x["id"]
                    )
                    db_start = time.time()
                    conn.execute(insert(PSMPeptideEvidence), sorted_pe_junctions)
                    db_time = time.time() - db_start
                    logger.info(f"[{stats.file_name}] PSMPeptideEvidence: {db_time:.3f}s total")

                if parsed.search_modifications:
                    sorted_search_mods = sorted(
                        parsed.search_modifications, key=lambda x: x["psm_id"]
                    )
                    stmt = insert_func(SearchModification).on_conflict_do_nothing()
                    db_start = time.time()
                    conn.execute(
                        stmt,
                        sorted_search_mods,
                    )
                    db_time = time.time() - db_start
                    logger.info(f"[{stats.file_name}] SearchModification: {db_time:.3f}s total")
            logger.debug(f"Successfully imported mzID data for file '{stats.file_name}'")
        except Exception as e:
            error_msg = f"Database import failed for file '{stats.file_name}': {e}"
            logger.error(error_msg, exc_info=True)
            raise TxtZipImportError(error_msg) from e


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

    logger.debug(
        f"Parsing txt files: {evidence_path.name}, {summary_path.name}, {peptides_path.name}"
    )

    try:
        evidence: DataFrame = pd.read_csv(  # pyright: ignore[reportCallIssue]
            filepath_or_buffer=evidence_path,
            sep="\t",
            usecols=[  # pyright: ignore[reportArgumentType]
                "Sequence",
                "Modifications",
                "Modified sequence",
                "Raw file",
                "Charge",
                "m/z",
                "Mass",
                "MS/MS scan number",
            ],
        )
        summary: DataFrame = pd.read_csv(  # pyright: ignore[reportCallIssue]
            summary_path,
            sep="\t",
            usecols=[  # pyright: ignore[reportArgumentType]
                "Raw file",
                "Variable modifications",
                "Fixed modifications",
            ],
        )
        peptides: DataFrame = pd.read_csv(  # pyright: ignore[reportCallIssue]
            peptides_path,
            sep="\t",
            usecols=[  # pyright: ignore[reportArgumentType]
                "Sequence",
                "Amino acid before",
                "Amino acid after",
                "Proteins",
                "Leading razor protein",
                "Start position",
                "End position",
            ],
        )

        evidence = evidence[evidence["MS/MS scan number"].notna()]

        logger.debug("Phase 1: Parsing peptides and modifications...")
        peptide_id_map, peptides_batch, mod_batch, mod_junction_batch = (
            parse_peptides_and_modifications(evidence)
        )

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

        # Return all parsed data
        parsed_data = ParsedTxtZipData(
            modified_peptides=peptides_batch,
            modifications=mod_batch,
            modified_peptide_modification_junctions=mod_junction_batch,
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
