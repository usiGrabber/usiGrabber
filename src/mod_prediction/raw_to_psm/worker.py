import csv
import logging
import subprocess
import tempfile
import time
from multiprocessing.synchronize import Lock
from pathlib import Path
from typing import Any

import pandas as pd

from mod_prediction.models import EnrichedPSM, Spectrum
from mod_prediction.parquet_to_mgf import convert_parquet_to_mgf
from mod_prediction.parquet_utils import write_batch_parquet
from mod_prediction.raw_to_psm.utils import (
    ChargeMismatchError,
    ThermoRawFileParserError,
    extract_charge_state_from_attributes,
    format_scan_ranges,
    json_to_spectra,
)

THERMO_PARSER_PATH: Path = (
    Path(__file__).parent.parent.parent.parent / "thermo" / "ThermoRawFileParser"
)
logger = logging.getLogger(__name__)


def extract_spectra_with_parser(
    raw_file_path: Path,
    scan_numbers: list[int],
) -> list[dict[str, Any]]:
    """
    Extract specific scans from raw file using ThermoRawFileParser query command.

    Extracts only the requested scans and converts JSON output to spectrum objects.
    Scan numbers are matched by position order with the returned spectra.

    Args:
        raw_file_path: Path to .raw file
        scan_numbers: List of scan numbers to extract (in sorted order)

    Returns:
        List of spectrum dicts with scan numbers, or None if extraction failed
    """

    if not scan_numbers:
        raise ValueError("No scan numbers provided for extraction")

    if not raw_file_path.exists():
        raise FileNotFoundError(f"Raw file does not exist: {raw_file_path}")
    if not raw_file_path.is_file():
        raise ValueError(f"Raw file path is not a file: {raw_file_path}")

    logger.debug(
        "[%s] Extracting %d unique scans from '%s'...",
        raw_file_path.name,
        len(scan_numbers),
        raw_file_path.name,
    )

    spectra: list[Spectrum] = []

    # Create temporary directory for JSON output
    with tempfile.TemporaryDirectory() as temp_dir:
        # Format scan numbers for ThermoRawFileParser
        for i, formatted_scans in enumerate(format_scan_ranges(scan_numbers)):
            logger.debug("[%s] Extracting batch %d...", raw_file_path.name, i + 1)

            temp_json = Path(temp_dir) / f"{i + 1}.json"

            # Run ThermoRawFileParser query command to extract JSON
            cmd = [
                str(THERMO_PARSER_PATH),
                "query",
                "-i",
                str(raw_file_path),
                "-n",
                formatted_scans,
                "-b",
                str(temp_json),
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3000,
            )

            if result.returncode != 0:
                raise ThermoRawFileParserError(
                    "ThermoRawFileParser query failed "
                    f"with return code {result.returncode}: {result.stderr}"
                )

            if not temp_json.exists():
                raise ThermoRawFileParserError(f"No JSON output file found at {temp_json}")

            # Convert JSON to Spectrum objects
            spectra.extend(json_to_spectra(temp_json))

            temp_json.unlink(missing_ok=True)

        if len(spectra) == 0:
            raise ValueError("Failed to convert JSON spectra")

        if len(spectra) != len(scan_numbers):
            raise ValueError(
                f"Requested {len(scan_numbers)} unique scans but extracted {len(spectra)} spectra"
            )

        # Convert spectrum data to DataFrame format (using aliases for column names)
        # Match scan numbers by position order (ThermoRawFileParser maintains order)
        spectrum_dicts = []
        for i, spectrum in enumerate(spectra):
            spectrum_dict = spectrum.model_dump(by_alias=True)
            # Assign scan number by position from the requested list
            spectrum_dict["scan_number"] = scan_numbers[i]
            spectrum_dicts.append(spectrum_dict)

    logger.info("[%s] Successfully extracted %d spectra", raw_file_path.name, len(spectrum_dicts))

    return spectrum_dicts


def write_charge_mismatch(
    charge_mismatch_file: Path,
    psm_id: str,
    spectrum_charge: int | None,
    psm_charge: int | None,
    project_accession: str,
    ms_run: str,
    scan_number: int,
) -> None:
    """
    Write a charge mismatch record to CSV file.

    Args:
        charge_mismatch_file: Path to charge_mismatches.csv file
        psm_id: PSM identifier
        spectrum_charge: Charge state from spectrum attributes
        psm_charge: Charge state from PSM data
        project_accession: PRIDE project accession
        ms_run: MS run name
        scan_number: Scan number
    """

    # Check if file exists to determine if we need to write header
    file_exists = charge_mismatch_file.exists()

    try:
        with open(charge_mismatch_file, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(
                    [
                        "psm_id",
                        "project_accession",
                        "ms_run",
                        "scan_number",
                        "spectrum_charge",
                        "psm_charge",
                    ]
                )
            writer.writerow(
                [
                    psm_id,
                    project_accession,
                    ms_run,
                    scan_number,
                    spectrum_charge,
                    psm_charge,
                ]
            )
    except Exception as e:
        logger.error(f"Failed to write charge mismatch record: {e}")


def extract_and_export(
    project_accession: str,
    ms_run: str,
    raw_file_path: Path,
    scan_numbers: list[int],
    output_dir: Path,
    chunk_df: pd.DataFrame,
    charge_mismatch_lock: Lock,
    no_validate_charge: bool = False,
    convert_to_mgf: bool = False,
) -> float:
    raw_file_name = raw_file_path.name

    # ===== EXTRACT =====
    extraction_start = time.time()
    spectrum_dicts = extract_spectra_with_parser(raw_file_path, scan_numbers)
    extraction_duration = time.time() - extraction_start
    logger.debug(
        "Extraction of scans from '%s' took: %.2f seconds",
        raw_file_name,
        extraction_duration,
    )

    # Convert to DataFrame
    spectra = pd.DataFrame(spectrum_dicts)

    # ===== MERGE AND VALIDATE =====
    enriched_psms = []
    charge_mismatch_file = output_dir.parent / "charge_mismatches.csv"

    for _, spectrum_row in spectra.iterrows():
        scan_number = spectrum_row["scan_number"]

        # Find matching PSMs for this spectrum
        matching_psms = chunk_df[chunk_df["index_number"] == scan_number]

        if matching_psms.empty:
            logger.warning(f"No PSM found for scan {scan_number}")
            continue

        # Extract charge state from spectrum attributes
        spectrum_attributes = spectrum_row.get("attributes") or []
        spectrum_charge = extract_charge_state_from_attributes(spectrum_attributes)

        # Create EnrichedPSM for each matching PSM
        for _, psm_row in matching_psms.iterrows():
            try:
                # Convert Series to dict and replace NaN with None for Pydantic compatibility
                psm_dict = psm_row.to_dict()
                for key, value in psm_dict.items():
                    try:
                        if pd.isna(value):
                            psm_dict[key] = None
                    except (TypeError, ValueError):
                        # Skip array-like values that can't be checked with isna()
                        pass

                # Validate charge state matches
                psm_charge = psm_dict.get("charge_state")
                has_charge_mismatch = (
                    spectrum_charge is not None
                    and psm_charge is not None
                    and spectrum_charge != psm_charge
                )
                if has_charge_mismatch and not no_validate_charge:
                    with charge_mismatch_lock:
                        write_charge_mismatch(
                            charge_mismatch_file,
                            psm_dict["psm_id"],
                            spectrum_charge,
                            psm_charge,
                            project_accession,
                            ms_run,
                            int(scan_number),
                        )
                    raise ChargeMismatchError(
                        f"Charge state mismatch for {project_accession}/{ms_run}: "
                        f"PSM {psm_dict['psm_id']}, scan {int(scan_number)}, "
                        f"spectrum={spectrum_charge}, psm={psm_charge}. "
                        f"Skipping entire file."
                    )

                enriched_psm: EnrichedPSM = EnrichedPSM(
                    psm_id=psm_dict["psm_id"],
                    project_accession=project_accession,
                    spectrum_id=psm_dict["spectrum_id"],
                    charge_state=psm_dict["charge_state"],
                    experimental_mz=psm_dict["experimental_mz"],
                    calculated_mz=psm_dict["calculated_mz"],
                    pass_threshold=psm_dict["pass_threshold"],
                    rank=psm_dict["rank"],
                    ms_run=ms_run,
                    index_number=psm_dict["index_number"],
                    index_type=psm_dict["index_type"],
                    peptide_sequence=psm_dict["peptide_sequence"],
                    modified_peptide_id=psm_dict["modified_peptide_id"],
                    unimod_ids=psm_dict["unimod_ids"],
                    locations=psm_dict["locations"],
                    modified_residues=psm_dict["modified_residues"],
                    mz_array=spectrum_row["mzs"].tolist(),
                    intensity_array=spectrum_row["intensities"].tolist(),
                )
                enriched_psms.append(enriched_psm)
            except ChargeMismatchError:
                raise  # re-raise to skip entire file on charge mismatch
            except Exception as e:
                logger.warning(
                    f"Failed to create EnrichedPSM for PSM {psm_row.get('psm_id', 'unknown')}: {e}"
                )
                continue

    if not enriched_psms:
        logger.warning(f"No enriched PSMs created for {project_accession}/{ms_run}")
        raise ValueError("No enriched PSMs created")

    # Write to output parquet file
    output_filename = f"{project_accession}_{ms_run}"
    parquet_path = write_batch_parquet(enriched_psms, output_dir, output_filename)

    if convert_to_mgf:
        convert_parquet_to_mgf(
            parquet_path=parquet_path,
            output_path=output_dir.parent / "mgf_output" / f"{output_filename}.mgf",
            batch_size=5000,
        )

    return extraction_duration
