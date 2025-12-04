"""
mzID Helper Functions

Pure parsing functions with no database dependencies.
These functions extract and transform data from mzIdentML elements.
"""

import logging
import re
import subprocess
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

from usigrabber.db.schema import IndexType
from usigrabber.utils import get_unimod_db

logger = logging.getLogger(__name__)


def extract_unimod_id_or_name(mod_data: dict) -> tuple[int | None, str | None]:
    """
    Extract UNIMOD ID and name from modification data.

    Args:
            mod_data: Modification dictionary

    Returns:
            UNIMOD ID as integer, or None if not found.
            Returns modification name if unimod ID not found and name is available.

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
                    return int(accession.split(":")[-1]), None
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
        return None, mod_name
    return uid, None


@lru_cache(maxsize=420)
def lookup_unimod_id_by_name(mod_name: str) -> int | None:
    """
    Lookup UNIMOD ID by modification name with caching.

    Args:
            mod_name: Name of the modification

    Returns:
            UNIMOD ID as integer, or None if not found
    """
    try:
        mod = get_unimod_db().get(mod_name, False)
        if mod is not None:
            return int(cast(int, mod.id))
    except KeyError:
        pass

    return None


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
    Extract modification position and residue information.

    Args:
            mod: Modification dictionary

    Returns:
            Tuple of (position, residues_string)
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


def extract_index_type_and_number(sir: dict) -> tuple[IndexType | None, int | None]:
    """
    Extract USI-related fields from SpectrumIdentificationResult.

    Parses spectrum title (MS:1000796) or scan number (MS:1001115) cvParams to extract:
    - index_type: Type of spectrum index (scan, index, nativeId, or trace)
    - index_number: Spectrum index number

    Args:
        sir: SpectrumIdentificationResult dictionary

    Returns:
        Tuple of (index_type, index_number)
    """
    index_type: IndexType | None = None
    index_number: int | None = None

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
    if spectrum_title and "scan=" in spectrum_title:
        # Example format:
        # "OTE0019_York_060813_JH16.3285.3285.2 File:\"OTE0019_York_060813_JH16.raw\",
        #  NativeID:\"controllerType=0 controllerNumber=1 scan=3285\""

        # Extract scan number from NativeID if present (overwrites existing spectrum id)
        try:
            match = re.search(r"scan=(\d+)", spectrum_title)
            if match:
                index_number = int(match.group(1))
            index_type = IndexType.scan
        except (ValueError, IndexError):
            pass

    # Parse scan number(s) (MS:1001115)
    if scan_number_value and index_type is None:
        try:
            index_number = int(scan_number_value)
            index_type = IndexType.scan
        except (ValueError, TypeError):
            pass

    return index_type, index_number


SPECTRUM_ID_FORMAT_MAPPING = {
    "MS:1000774": IndexType.index,
    "MS:1000776": IndexType.scan,
    "MS:1000768": IndexType.scan,
    "MS:1001530": IndexType.nativeId,
}

# Set to track already logged exceptions
# WARNING: not thread-safe, but acceptable for this use case
exceptions = set()


def get_spectrum_id_format(cv_param: str) -> IndexType | None:
    """
    Map SpectrumIDFormat accession to human-readable format.

    Args:
        cv_param: SpectrumIDFormat cvParam accession

    Returns:
        Human-readable format string or None if not found
    """
    id_format = SPECTRUM_ID_FORMAT_MAPPING.get(cv_param)
    if not id_format:
        if cv_param not in exceptions:
            # only print each exception once
            logger.warning("Unknown SpectrumIDFormat accession: %s", cv_param)
            exceptions.add(cv_param)
        return None
    return id_format


def extract_xml_subtree(xml_path: Path, tag: str) -> str:
    """
    Use sed to extract XML subtree from a file and return as string.
    """
    cmd = ["sed", "-n", rf"/<{tag}>/,/<\/{tag}>/p", str(xml_path)]
    proc = subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        check=True,
    )
    return proc.stdout.strip()
