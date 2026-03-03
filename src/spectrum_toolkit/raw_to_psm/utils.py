import json
import logging
from collections.abc import Iterator
from pathlib import Path

from spectrum_toolkit.models import Spectrum

logger = logging.getLogger(__name__)


class NonRetryableError(Exception):
    """Base class for non-retryable errors."""

    pass


class ThermoRawFileParserError(NonRetryableError):
    """Custom exception for ThermoRawFileParser errors."""

    pass


class ChargeMismatchError(NonRetryableError):
    """Exception raised when there is a charge state mismatch."""

    pass


THERMO_CHUNK_SIZE = 10_000  # avoid hitting "Argument list too long" error


def format_scan_ranges(scan_numbers: list[int]) -> Iterator[str]:
    """
    Format scan numbers into ThermoRawFileParser's expected format.

    Converts a list of scan numbers into a compact range notation.
    e.g., [1, 2, 3, 5, 7, 8, 9, 12] → "1-3, 5, 7-9, 12"

    Args:
        scan_numbers: List of integer scan numbers

    Returns:
        Formatted scan string for ThermoRawFileParser
    """
    if not scan_numbers:
        return ""

    # Sort and deduplicate
    number_of_scans: int = len(scan_numbers)
    sorted_scans: list[int] = sorted(set[int](scan_numbers))
    number_of_sorted_scans: int = len(sorted_scans)
    if number_of_scans != number_of_sorted_scans:
        logger.error(
            f"Duplicate scan numbers found: original count {number_of_scans}, unique count {number_of_sorted_scans}"
        )

    # Group consecutive scans
    ranges = []
    start = sorted_scans[0]
    end = sorted_scans[0]

    for scan in sorted_scans[1:]:
        if scan == end + 1:
            # Continue the range
            end = scan
        else:
            # End current range and start new one
            if start == end:
                ranges.append(str(start))
            else:
                ranges.append(f"{start}-{end}")
            start = scan
            end = scan

    # Add final range
    if start == end:
        ranges.append(str(start))
    else:
        ranges.append(f"{start}-{end}")

    # yield ranges in chunks of 10k range strings to avoid "Argument list too long" error
    for i in range(0, len(ranges), THERMO_CHUNK_SIZE):
        yield ",".join(ranges[i : i + THERMO_CHUNK_SIZE])


def extract_charge_state_from_attributes(attributes: list) -> int | None:
    """
    Extract charge state from spectrum attributes.

    Looks for attribute with accession "MS:1000041" (charge state).

    Args:
        attributes: List of PyteomicsAttribute dicts

    Returns:
        Charge state as int, or None if not found
    """
    for attr in attributes:
        if isinstance(attr, dict) and attr.get("accession") == "MS:1000041":
            try:
                charge: int = int(attr.get("value", -1))
                if charge > 0:
                    return charge
                else:
                    return None
            except (ValueError, TypeError):
                logger.warning(f"Invalid charge state value: {attr.get('value')}")
                return None
    return None


def json_to_spectra(json_file: Path) -> list[Spectrum]:
    """
    Convert JSON output from ThermoRawFileParser query to Spectrum objects.

    Args:
        json_file: Path to JSON file from query command

    Returns:
        List of Spectrum objects
    """
    try:
        with open(json_file) as f:
            data = json.load(f)

        spectra = []
        for spectrum_data in data:
            # Extract m/z and intensity arrays
            mzs = spectrum_data.get("mzs", [])
            intensities = spectrum_data.get("intensities", [])
            attributes: list = spectrum_data.get("attributes", [])

            # Create Spectrum object using alias names from JSON
            spectrum: Spectrum = Spectrum(
                mzs=mzs,
                intensities=intensities,
                attributes=attributes,
            )
            spectra.append(spectrum)

        logger.debug("Converted %d spectra from JSON file", len(spectra))
        return spectra

    except Exception as e:
        logger.error(f"Failed to convert JSON to spectra: {e}")
        return []
