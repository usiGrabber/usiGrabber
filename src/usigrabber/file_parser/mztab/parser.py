import logging
from pathlib import Path

from pyteomics import mztab
from pyteomics.auxiliary import PyteomicsError
from sqlalchemy import insert
from sqlalchemy.engine.base import Engine

from usigrabber.db.schema import ModifiedPeptide, PeptideSpectrumMatch
from usigrabber.file_parser.base import BaseFileParser, register_parser
from usigrabber.file_parser.errors import MztabImportError, MztabParseError
from usigrabber.file_parser.helpers import get_db_insert_function
from usigrabber.file_parser.models import ImportStats, ParsedMztabData
from usigrabber.file_parser.mztab.parsing_functions import extract_mztab_data

logger = logging.getLogger(__name__)


@register_parser
class MztabFileParser(BaseFileParser):
    @property
    def file_extensions(self) -> set[str]:
        return {".mzTab"}

    @property
    def format_name(self) -> str:
        return "mzTab"

    def parse_file(self, path, project_accession: str) -> ParsedMztabData:
        path = path if isinstance(path, Path) else path[0]
        if not path.exists():
            raise MztabParseError(f"File not found: {path}")

        logger.debug(f"Parsing mzTab file: {path.name}")
        try:
            mz_file = mztab.MzTab(str(path))
            psm_rows, peptide_rows = extract_mztab_data(mz_file, project_accession)
            return ParsedMztabData(psms=psm_rows, modified_peptides=peptide_rows)
        except PyteomicsError as e:
            raise MztabParseError(f"Failed to parse mzTab file '{path}': {e}") from e

    def persist(self, engine: Engine, parsed: ParsedMztabData, stats: ImportStats):
        """Persist parsed mzTab data to the database with debug logging."""
        logger.debug(f"Persisting mzTab data to database for file '{stats.file_name}'")

        insert_func = get_db_insert_function(engine)

        try:
            with engine.begin() as conn:
                if parsed.modified_peptides:
                    # Use INSERT OR IGNORE (SQLite) or INSERT ON CONFLICT DO NOTHING (PostgreSQL)
                    # for cross-file deduplication based on primary key
                    stmt = insert_func(ModifiedPeptide).on_conflict_do_nothing()
                    conn.execute(stmt, parsed.modified_peptides)
                    stats.peptide_count = len(parsed.modified_peptides)
                if parsed.psms:
                    conn.execute(insert(PeptideSpectrumMatch), parsed.psms)
                    stats.psm_count = len(parsed.psms)
            logger.debug(f"Successfully imported mzTab data for file '{stats.file_name}'")
        except Exception as e:
            raise MztabImportError(f"Failed to persist mzTab data: {e}") from e
