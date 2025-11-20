# logger
import logging
from pathlib import Path

from pyteomics import mztab
from pyteomics.auxiliary import PyteomicsError

logger = logging.getLogger(__name__)


def parse_mztab_file(mztab_path: Path, project_accession: str) -> None:
    """
    Parse an mzTab file and extract relevant data.

    Args:
        mztab_path (Path): Path to the mzTab file.
        project_accession (str): Project accession identifier.

    Returns:
        ParsedMztabData: Parsed data from the mzTab file.
    """
    # Validate file exists
    if not mztab_path.exists():
        error_msg = f"File not found: {mztab_path}"
        logger.error(error_msg)

    logger.debug(f"Parsing mzTab file: {mztab_path.name}")

    try:
        file = mztab.MzTab(str(mztab_path))
        # Extract relevant data
        breakpoint()
        del file

    except PyteomicsError as e:
        error_msg = f"Failed to parse mzID file: {e}"
        logger.error(error_msg, exc_info=True)

    return None


def import_mztab(mzid_path: Path, project_accession: str) -> None:
    parsed_data = parse_mztab_file(mzid_path, project_accession)
    print(parsed_data)
