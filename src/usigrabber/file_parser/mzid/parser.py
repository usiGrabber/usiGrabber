# file_parser/mzid/parser.py
import logging
from pathlib import Path

from pyteomics import mzid
from pyteomics.auxiliary import PyteomicsError
from sqlalchemy import insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine
from sqlmodel import Session

from usigrabber.db.schema import (
    Modification,
    ModifiedPeptide,
    ModifiedPeptideModificationJunction,
    PeptideEvidence,
    PeptideSpectrumMatch,
    PSMPeptideEvidence,
)
from usigrabber.file_parser.base import BaseFileParser, register_parser
from usigrabber.file_parser.errors import MzidImportError, MzidParseError
from usigrabber.file_parser.models import ImportStats, ParsedMzidData
from usigrabber.file_parser.mzid.parsing_functions import (
    parse_db_sequences,
    parse_mzid_metadata,
    parse_peptide_evidence,
    parse_peptides_and_modifications,
    parse_psms,
    parse_spectra_data,
)

logger = logging.getLogger(__name__)


@register_parser
class MzidFileParser(BaseFileParser):
    @property
    def file_extensions(self) -> set[str]:
        return {".mzid"}

    @property
    def format_name(self) -> str:
        return "mzIdentML"

    def parse_file(self, path, project_accession: str) -> ParsedMzidData:
        """Parse the mzID file into ParsedMzidData."""
        path = path if isinstance(path, Path) else path[0]
        logger.debug(f"Parsing mzID file: '{path.name}'")

        if not path.exists():
            error_msg = f"File not found: {path}"
            logger.error(error_msg)
            raise MzidParseError(error_msg)

        try:
            spectra_data_map = parse_spectra_data(path)
            if len(spectra_data_map) == 0:
                raise MzidParseError(f"No SpectraData found in mzID file {path.name}")

            with mzid.MzIdentML(str(path), retrieve_refs=False) as reader:
                logger.debug("Phase 0: Parsing file metadata...")
                mzid_file = parse_mzid_metadata(reader, path, project_accession)

                logger.debug("Phase 1: Parsing database sequences...")
                db_seq_map = parse_db_sequences(reader)

                logger.debug("Phase 2: Parsing peptides and modifications...")
                peptide_id_map, peptides_batch, mod_batch, mod_junction_batch = (
                    parse_peptides_and_modifications(reader)
                )

                logger.debug("Phase 3: Parsing peptide evidence...")
                pe_id_map, peptide_evidence_batch = parse_peptide_evidence(reader, db_seq_map)

                logger.debug("Phase 4: Parsing spectrum identification results (PSMs)...")
                psm_batch, junction_batch = parse_psms(
                    reader,
                    project_accession,
                    mzid_file.id,
                    peptide_id_map,
                    pe_id_map,
                    spectra_data_map,
                )

                parsed_data = ParsedMzidData(
                    mzid_file=mzid_file,
                    modified_peptides=peptides_batch,
                    modifications=mod_batch,
                    modified_peptide_modification_junctions=mod_junction_batch,
                    peptide_evidence=peptide_evidence_batch,
                    psms=psm_batch,
                    psm_peptide_evidence_junctions=junction_batch,
                )

                logger.debug(f"Successfully parsed '{path.name}'")
                return parsed_data

        except PyteomicsError as e:
            error_msg = f"Failed to parse mzID file: {e}"
            logger.error(error_msg, exc_info=True)
            raise MzidParseError(error_msg) from e

    def persist(self, engine: Engine, parsed: ParsedMzidData, stats: ImportStats):
        """Persist parsed mzID data to the database with debug logging."""
        logger.debug(f"Persisting mzID data to database for file '{stats.file_name}'")

        # Detect database type to use appropriate insert dialect
        db_dialect = engine.dialect.name
        is_postgresql = db_dialect == "postgresql"

        # Select appropriate insert function based on database type
        insert_func = pg_insert if is_postgresql else sqlite_insert

        try:
            with Session(engine) as session:
                session.add(parsed.mzid_file)
                session.commit()
                if parsed.modified_peptides:
                    # Use INSERT OR IGNORE (SQLite) or INSERT ON CONFLICT DO NOTHING (PostgreSQL)
                    # for cross-file deduplication based on primary key
                    stmt = insert_func(ModifiedPeptide).on_conflict_do_nothing()
                    session.execute(stmt, parsed.modified_peptides)
                    stats.peptide_count = len(parsed.modified_peptides)
                if parsed.modifications:
                    # Use INSERT OR IGNORE (SQLite) or INSERT ON CONFLICT DO NOTHING (PostgreSQL)
                    # for cross-file deduplication based on unique constraint
                    stmt = insert_func(Modification).on_conflict_do_nothing()
                    session.execute(stmt, parsed.modifications)
                    stats.modification_count = len(parsed.modifications)
                if parsed.modified_peptide_modification_junctions:
                    # Use INSERT OR IGNORE (SQLite) or INSERT ON CONFLICT DO NOTHING (PostgreSQL)
                    # for cross-file deduplication based on composite primary key
                    stmt = insert_func(ModifiedPeptideModificationJunction).on_conflict_do_nothing()
                    session.execute(stmt, parsed.modified_peptide_modification_junctions)
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
                session.commit()
            logger.debug(f"Successfully imported mzID data for file '{stats.file_name}'")
        except Exception as e:
            error_msg = f"Database import failed for file '{stats.file_name}': {e}"
            logger.error(error_msg, exc_info=True)
            raise MzidImportError(error_msg) from e
