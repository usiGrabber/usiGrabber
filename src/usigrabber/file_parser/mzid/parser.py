# file_parser/mzid/parser.py
import logging
from pathlib import Path

from pyteomics import mzid
from pyteomics.auxiliary import PyteomicsError
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
from usigrabber.file_parser.errors import MzidImportError, MzidParseError
from usigrabber.file_parser.helpers import get_db_insert_function
from usigrabber.file_parser.models import (
    ImportStats,
    ParsedMzidData,
)
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

    def get_file_id(self, path: Path | tuple[Path, Path, Path]) -> str:
        path = path if isinstance(path, Path) else path[0]
        return str(path)

    @property
    def format_name(self) -> str:
        return "mzIdentML"

    def parse_file(self, path, project_accession: str) -> ParsedMzidData:
        """Parse the mzID file into ParsedMzidData."""
        if isinstance(path, tuple):
            logger.warning(
                "mzID file parser called with tuple of paths: %s. We are only using the first path!",
                path,
            )
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

            # NOTE:
            # pyteomics may fall back to a non-indexed forward stream for iterfind calls
            # where no byte-offset index exists (e.g. empty DBSequence/Peptide sections).
            # In those cases, one pass can exhaust the underlying stream and subsequent
            # passes can fail with `XMLSyntaxError: no element found (line 0)`.
            # We avoid that by resetting the reader between parsing phases.
            with mzid.MzIdentML(str(path), retrieve_refs=False) as reader:
                logger.debug("Phase 0: Parsing file metadata...")
                mzid_file = parse_mzid_metadata(reader, path, project_accession)

                logger.debug("Phase 1: Parsing database sequences...")
                reader.reset()
                db_seq_map = parse_db_sequences(reader)

                logger.debug("Phase 2: Parsing peptides and modifications...")
                reader.reset()
                peptide_id_map, peptides_batch, mod_batch, mod_junction_batch = (
                    parse_peptides_and_modifications(reader)
                )

                logger.debug("Phase 3: Parsing peptide evidence...")
                reader.reset()
                pe_id_map, peptide_evidence_batch = parse_peptide_evidence(reader, db_seq_map)

                logger.debug("Phase 4: Parsing spectrum identification results (PSMs)...")
                reader.reset()
                psm_batch, junction_batch, search_mod_batch = parse_psms(
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
                search_modifications=search_mod_batch,
            )

            logger.debug(f"Successfully parsed '{path.name}'")
            return parsed_data

        except PyteomicsError as e:
            error_msg = f"Failed to parse mzID file: {e}"
            logger.error(error_msg, exc_info=True)
            raise MzidParseError(error_msg) from e

    def persist(self, engine: Engine, parsed: ParsedMzidData, stats: ImportStats):
        """Persist parsed mzID data to the database with debug logging."""
        import time

        logger.debug(f"Persisting mzID data to database for file '{stats.file_name}'")

        try:
            insert_func = get_db_insert_function(engine)
            with Session(engine) as session:
                session.add(parsed.mzid_file)
                session.commit()

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
                    logger.info(f"[{stats.file_name}] PeptideEvidence: {db_time:.3f}s total")

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
                    sorted_search_mods = sorted(parsed.search_modifications, key=lambda x: x["id"])
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
            raise MzidImportError(error_msg) from e
