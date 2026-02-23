"""MGF (Mascot Generic Format) file operations."""

import logging
from pathlib import Path
from typing import Any

import numpy as np
from pyteomics import mgf as pyteomics_mgf

from mod_prediction.models import MGFParams, MGFSpectrum

logger = logging.getLogger(__name__)


def write_mgf(spectra: list[MGFSpectrum], output_path: Path) -> None:
    """
    Write spectra to MGF file.

    Args:
            spectra: List of MGFSpectrum objects
            output_path: Output file path

    Raises:
            ValueError: If spectra list is empty
            IOError: If file cannot be written
    """
    if not spectra:
        raise ValueError("Cannot write empty spectra list to MGF file")

    logger.debug(f"Writing {len(spectra)} spectra to {output_path}...")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Convert MGFSpectrum objects to dicts for pyteomics
    spectra_dicts = [spectrum.to_dict() for spectrum in spectra]

    pyteomics_mgf.write(spectra_dicts, output=str(output_path))

    file_size = output_path.stat().st_size
    logger.debug(f"Wrote MGF file: {output_path} ({file_size:,} bytes)")


def append_mgf(spectra: list[MGFSpectrum], output_path: Path) -> None:
    """
    Append spectra to existing MGF file (or create new if doesn't exist).

    Args:
            spectra: List of MGFSpectrum objects to append
            output_path: Output file path

    Raises:
            ValueError: If spectra list is empty
            IOError: If file cannot be written
    """
    if not spectra:
        raise ValueError("Cannot append empty spectra list to MGF file")

    logger.debug(f"Appending {len(spectra)} spectra to {output_path}...")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Convert MGFSpectrum objects to dicts for pyteomics
    spectra_dicts = [spectrum.to_dict() for spectrum in spectra]

    # Open in append mode
    with open(output_path, "a") as f:
        pyteomics_mgf.write(spectra_dicts, output=f)

    file_size = output_path.stat().st_size
    logger.debug(f"Appended to MGF file: {output_path} ({file_size:,} bytes)")


def spectrum_from_parquet_row(row: dict[str, Any]) -> MGFSpectrum:
    """
    Convert enriched PSM parquet row to MGF spectrum format.

    Args:
        row: Dictionary containing enriched PSM data with keys:
            - index_number: Scan number
            - charge_state: Charge state
            - mz_array: List of m/z values
            - intensity_array: List of intensity values

    Returns:
        MGFSpectrum ready to be written to MGF file
    """
    scan_id = str(row["index_number"])
    charge = int(row["charge_state"])

    # Build MGF params
    params: MGFParams = {
        "title": f"controllerType=0 controllerNumber=1 scan={scan_id}",
        "scans": scan_id,
        "charge": [charge],
    }

    # Convert lists to numpy arrays if needed
    mz_array = row["mz_array"]
    intensity_array = row["intensity_array"]

    if not isinstance(mz_array, np.ndarray):
        mz_array = np.array(mz_array)
    if not isinstance(intensity_array, np.ndarray):
        intensity_array = np.array(intensity_array)

    return MGFSpectrum(
        **{
            "m/z array": mz_array,
            "intensity array": intensity_array,
            "params": params,
        }
    )
