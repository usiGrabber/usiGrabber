"""
mzID Helper Functions

Pure parsing functions with no database dependencies.
These functions extract and transform data from mzIdentML elements.
"""

import logging
from typing import Any

from usigrabber.utils import get_unimod_db

logger = logging.getLogger(__name__)


def extract_unimod_id(mod_data: dict) -> int | None:
    """
    Extract UNIMOD ID from modification cvParam data.

    Args:
            mod_data: Modification dictionary containing cvParam information

    Returns:
            UNIMOD ID as integer, or None if not found
    """
    # Check if cvParam exists
    cv_params = mod_data.get("cvParam")

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
                    return int(accession.split(":")[-1])
                except (ValueError, IndexError):
                    continue

    # Fallback: resolve by modification name
    unimod_db = get_unimod_db()
    try:
        mod = unimod_db.get(mod_data.get("name", ""), False)
        if mod is not None:
            return int(mod.id)  # type: ignore[arg-type]
    except KeyError:
        return None

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


def parse_modification_location(mod: dict) -> tuple[int, str]:
    """
    Extract modification location and residue information.

    Args:
            mod: Modification dictionary

    Returns:
            Tuple of (location, residues_string)
    """
    location = mod.get("location", 0)
    residues = mod.get("residues", "")

    # Convert residues to string if it's a list
    if isinstance(residues, list):
        residues = "".join(residues)

    return location, residues or ""


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
