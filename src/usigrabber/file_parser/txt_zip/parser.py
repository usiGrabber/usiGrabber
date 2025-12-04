import logging
from pathlib import Path

import pandas as pd
from pandas.core.frame import DataFrame
from sqlalchemy import insert
from sqlalchemy.engine import Engine
from sqlmodel import Session

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
from usigrabber.file_parser.models import ImportStats, ParsedTxtZipData
from usigrabber.file_parser.txt_zip.parsing_functions import (
    parse_peptide_evidence,
    parse_peptides,
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
        try:
            with Session(engine) as session:
                if parsed.modified_peptides:
                    session.execute(insert(ModifiedPeptide), parsed.modified_peptides)
                    stats.peptide_count = len(parsed.modified_peptides)
                if parsed.modifications:
                    session.execute(insert(Modification), parsed.modifications)
                    stats.modification_count = len(parsed.modifications)
                if parsed.modified_peptide_modification_junctions:
                    session.execute(
                        insert(ModifiedPeptideModificationJunction),
                        parsed.modified_peptide_modification_junctions,
                    )
                if parsed.peptide_evidence:
                    session.execute(insert(PeptideEvidence), parsed.peptide_evidence)
                    stats.peptide_evidence_count = len(parsed.peptide_evidence)
                if parsed.psms:
                    session.execute(insert(PeptideSpectrumMatch), parsed.psms)
                    stats.psm_count = len(parsed.psms)
                if parsed.psm_peptide_evidence_junctions:
                    session.execute(
                        insert(PSMPeptideEvidence), parsed.psm_peptide_evidence_junctions
                    )
                if parsed.search_modifications:
                    session.execute(insert(SearchModification), parsed.search_modifications)
                session.commit()
            logger.debug(
                f"Successfully imported txt data for txt.zip from '{stats.project_accession}'"
            )
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

        logger.debug("Phase 1: Parsing peptides and modifications...")
        peptide_id_map, modified_peptides_batch, modifications_batch, mod_junction_batch = (
            parse_peptides(evidence, peptides)
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
            modified_peptides=modified_peptides_batch,
            modifications=modifications_batch,
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
