import logging
from pathlib import Path

from pyteomics import mztab
from pyteomics.auxiliary import PyteomicsError
from sqlalchemy import insert
from sqlalchemy.engine.base import Engine

from usigrabber.db.schema import Peptide, PeptideSpectrumMatch
from usigrabber.file_parser.base import BaseFileParser, register_parser
from usigrabber.file_parser.errors import MztabImportError, MztabParseError
from usigrabber.file_parser.models import ImportStats, ParsedMztabData
from usigrabber.file_parser.parsing_functions import extract_mztab_data

logger = logging.getLogger(__name__)


@register_parser
class MztabParser(BaseFileParser):
    file_extensions = {".mztab"}
    format_name = "mzTab"

    def parse_file(self, path: Path, project_accession: str) -> ParsedMztabData:
        if not path.exists():
            raise MztabParseError(f"File not found: {path}")

        logger.debug(f"Parsing mzTab file: {path.name}")
        try:
            mz_file = mztab.MzTab(str(path))
            psm_rows, peptide_rows = extract_mztab_data(mz_file, project_accession)
            return ParsedMztabData(psms=psm_rows, peptides=peptide_rows)
        except PyteomicsError as e:
            raise MztabParseError(f"Failed to parse mzTab file '{path}': {e}") from e

    def persist(self, engine: Engine, parsed: ParsedMztabData, stats: ImportStats):
        try:
            with engine.begin() as conn:
                if parsed.peptides:
                    conn.execute(insert(Peptide), parsed.peptides)
                if parsed.psms:
                    conn.execute(insert(PeptideSpectrumMatch), parsed.psms)

            stats.peptide_count = len(parsed.peptides)
            stats.psm_count = len(parsed.psms)
        except Exception as e:
            raise MztabImportError(f"Failed to persist mzTab data: {e}") from e
