"""
mzID Helper Functions

Pure parsing functions with no database dependencies.
These functions extract and transform data from mzIdentML elements.
"""

import logging
from functools import lru_cache
from typing import Any, cast

from usigrabber.utils import get_unimod_db

logger = logging.getLogger(__name__)


def extract_unimod_id_and_name(mod_data: dict) -> tuple[int | None, str | None]:
    """
    Extract UNIMOD ID from modification cvParam data.

    Args:
            mod_data: Modification dictionary containing cvParam information

    Returns:
            UNIMOD ID as integer, or None if not found
    """
    # Check if cvParam exists
    cv_params = mod_data.get("cvParam")

    mod_name = mod_data.get("name")

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
    return uid


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

    return None, mod_name


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
