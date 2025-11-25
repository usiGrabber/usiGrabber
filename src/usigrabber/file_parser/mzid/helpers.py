"""
mzID Helper Functions

Pure parsing functions with no database dependencies.
These functions extract and transform data from mzIdentML elements.
"""

import logging
from typing import Any

from usigrabber.db.schema import IndexType
from usigrabber.utils import lookup_unimod_id_by_name

logger = logging.getLogger(__name__)


def extract_unimod_id_and_name(mod_data: dict) -> tuple[int | None, str | None]:
    """
    Extract UNIMOD ID and name from modification data.

    Args:
            mod_data: Modification dictionary

    Returns:
            UNIMOD ID as integer, or None if not found. Also returns modification name.

    """
    # Check if cvParam exists
    cv_params = mod_data.get("cvParam")

    mod_name = mod_data.get("name")
    uid: int | None = None

    if cv_params:
        # cvParam can be a list or a single dict
        if not isinstance(cv_params, list):
            cv_params = [cv_params]

        # Look for UNIMOD accession
        for param in cv_params:
            accession = param.get("accession", "")
            if "UNIMOD:" in accession and len(accession) > 7:
                try:
                    # Extract number from "UNIMOD:35" format
                    return int(accession.split(":")[-1]), mod_name
                except (ValueError, IndexError):
                    pass
            name = param.get("name", "")
            if name:
                # Fallback: resolve by modification name
                uid = lookup_unimod_id_by_name(name)

    if mod_name:
        # Fallback: resolve by modification name
        uid = lookup_unimod_id_by_name(mod_name)

    if uid is None:
        logger.debug("No UNIMOD ID found for modification: %s", mod_data)
    return uid, mod_name


def extract_score_values(sii: dict) -> dict[str, Any]:
    """
    Extract score values from a SpectrumIdentificationItem.

    Args:
            sii: SpectrumIdentificationItem dictionary

    Returns:
            Dictionary of score names to values
    """
    score_values: dict[str, Any] = {}

    for key, value in sii.items():
        # Include fields that look like scores, expectation values, etc.
        key_lower = key.lower()
        if any(term in key_lower for term in ["score", "value", "fdr", "qvalue", "expect"]):
            # Skip fields that are not actual score values
            if key in ["peptide_ref", "PeptideEvidenceRef", "passThreshold"]:
                continue
            score_values[key] = value

    return score_values if score_values else {}


def parse_modification_location(mod: dict) -> tuple[int | None, str | None]:
    """
    Extract modification location and residue information.

    Args:
            mod: Modification dictionary

    Returns:
            Tuple of (location, residues_string)
    """
    location = mod.get("location")
    residues = mod.get("residues")

    # Convert residues to string if it's a list
    if isinstance(residues, list):
        residues = "".join(residues)

    return location, residues


def normalize_residues(residues: Any) -> str:
    """
    Normalize residue representation to string.

    Args:
            residues: Residues in various formats (string, list, etc.)

    Returns:
            Normalized residue string
    """
    if isinstance(residues, list):
        return "".join(residues)
    return str(residues) if residues else ""


def extract_usi_fields(sir: dict) -> tuple[IndexType | None, int | None, str | None]:
    """
    Extract USI-related fields from SpectrumIdentificationResult.

    Parses spectrum title (MS:1000796) or scan number (MS:1001115) cvParams to extract:
    - index_type: Type of spectrum index (scan, index, nativeId, or trace)
    - index_number: Spectrum index number
    - ms_run: MS run identifier from raw file name

    Args:
            sir: SpectrumIdentificationResult dictionary

    Returns:
            Tuple of (index_type, index_number, ms_run)
    """
    index_type: IndexType | None = None
    index_number: int | None = None
    ms_run: str | None = None

    # First try to parse from spectrumID attribute
    spectrum_id = sir.get("spectrumID", "")
    if spectrum_id:
        # Check for "index=" pattern
        if spectrum_id.startswith("index="):
            try:
                index_type = IndexType.index
                index_number = int(spectrum_id.split("=")[1])
            except (ValueError, IndexError):
                pass
        # Check for "scan=" pattern
        elif spectrum_id.startswith("scan="):
            try:
                index_type = IndexType.scan
                index_number = int(spectrum_id.split("=")[1])
            except (ValueError, IndexError):
                pass
        else:
            # Fallback: try to parse integer directly
            try:
                index_number = int(spectrum_id)
                index_type = IndexType.scan
            except ValueError:
                pass

    spectrum_title: str | None = sir.get("spectrum title")
    scan_number_value: str | None = sir.get("scan number(s)")

    # Parse spectrum title (MS:1000796)
    if spectrum_title:
        # Example format:
        # "OTE0019_York_060813_JH16.3285.3285.2 File:\"OTE0019_York_060813_JH16.raw\",
        #  NativeID:\"controllerType=0 controllerNumber=1 scan=3285\""

        # Extract scan number from NativeID if present (overwrites existing spectrum id)
        if "scan=" in spectrum_title:
            try:
                scan_part = spectrum_title.split("scan=")[1]
                scan_num_str = scan_part.split('"')[0].split()[0]
                index_number = int(scan_num_str)
                index_type = IndexType.scan
            except (ValueError, IndexError):
                pass

        # Extract MS run from File field
        if 'File:"' in spectrum_title or "File:&quot;" in spectrum_title:
            try:
                # Handle both " and &quot; encodings
                file_part = spectrum_title.split("File:")[1]
                if file_part.startswith("&quot;"):
                    file_name = file_part.split("&quot;")[1]
                else:
                    file_name = file_part.split('"')[1]
                # Remove .raw extension
                ms_run = file_name.replace(".raw", "").replace(".RAW", "")
            except IndexError:
                pass

    # Parse scan number(s) (MS:1001115)
    if scan_number_value and index_type is None:
        try:
            index_type = IndexType.scan
            index_number = int(scan_number_value)
        except (ValueError, TypeError):
            pass

    return index_type, index_number, ms_run
