import logging
from pathlib import Path

import pandas as pd

from usigrabber.file_parser.errors import EvidenceTxtParseError
from usigrabber.file_parser.evidence_txt.models import ParsedEvidenceTxtData
from usigrabber.file_parser.evidence_txt.parsing_functions import (
    link_modifications,
    parse_db_sequences,
    parse_peptide_evidence,
    parse_peptides,
    parse_psms,
)

logger = logging.getLogger(__name__)


def import_evidence_txt(evidence_txt_path: Path, project_accession: str) -> ParsedEvidenceTxtData:
    if not evidence_txt_path.exists():
        error_msg = f"File not found: {evidence_txt_path}"
        logger.error(error_msg)
        raise EvidenceTxtParseError(error_msg)

    logger.info(f"Parsing mzID file: {evidence_txt_path.name}")

    try:
        # Parse evidence.txt file with retrieve_refs=False
        df = pd.read_csv(evidence_txt_path, sep="\t")

        # Phase 1: Parse DB sequences
        logger.debug("\nPhase 1: Parsing database sequences...")
        db_sequence_map = parse_db_sequences(df)

        # Phase 2: Parse peptides
        logger.debug("\nPhase 2: Parsing peptides...")
        peptide_id_map, peptide_mods, peptides_batch = parse_peptides(df)

        # Phase 3: Parse peptide evidence
        logger.debug("\nPhase 3: Parsing peptide evidence...")
        pe_id_map, peptide_evidence_batch = parse_peptide_evidence(df, db_sequence_map)

        # Phase 4: Parse PSMs
        logger.debug("\nPhase 4: Parsing spectrum identification results...")
        psm_batch, junction_batch = parse_psms(
            df,
            project_accession,
            # mzid_file.id,
            peptide_id_map,
            pe_id_map,
        )

        # Phase 5: Link modifications
        logger.debug("\nPhase 5: Linking peptide modifications...")
        mod_batch = link_modifications(peptide_mods)

        # Return all parsed data
        parsed_data = ParsedEvidenceTxtData(
            # evidence_txt_file = evidence_txt_file,
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
        raise EvidenceTxtParseError(error_msg) from e


def parse_evidence_txt_file():
    pass
