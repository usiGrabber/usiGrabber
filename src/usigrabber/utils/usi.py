"""
USI (Universal Spectrum Identifier) utility functions.

This module provides utilities for building and working with USI strings
according to the PSI specification.
"""

from usigrabber.db.schema import PeptideSpectrumMatch


def build_usi(psm: PeptideSpectrumMatch) -> str | None:
    """
    Build a USI (Universal Spectrum Identifier) string from a PeptideSpectrumMatch.

    USI format: mzspec:{project}:{ms_run}:{index_type}:{index_number}:{sequence}/{charge}

    Args:
        psm: PeptideSpectrumMatch object with USI-related fields populated

    Returns:
        USI string if all required fields are present, None otherwise

    Example:
        >>> # Assuming psm has all required fields
        >>> build_usi(psm)
        'mzspec:PXD006066:file_name:scan:12345:PEPTIDEK/2'
    """
    # Check if all required fields are present
    if not psm.project:
        return None

    if not psm.ms_run:
        return None

    if not psm.index_type:
        return None

    if psm.index_number is None:
        return None

    if not psm.modified_peptide or not psm.modified_peptide.peptide_sequence:
        return None

    if psm.charge_state is None:
        return None

    # Build USI string
    # Format: mzspec:{project}:{ms_run}:{index_type}:{index_number}:{sequence}/{charge}
    usi = (
        f"mzspec:{psm.project.accession}:{psm.ms_run}:"
        f"{psm.index_type.value}:{psm.index_number}:"
        f"{psm.modified_peptide.peptide_sequence}/{psm.charge_state}"
    )

    return usi
