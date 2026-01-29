import argparse
import asyncio
import csv
import random
import re
import shutil
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pyteomics.usi import USI, proxi

# Adjust imports to match your project structure
from usigrabber.backends.pride import PrideBackend
from usigrabber.file_parser.mzid.parser import MzidFileParser
from usigrabber.utils.file import download_ftp, extract_archive, temporary_path

# Retry configuration
MAX_RETRIES = 3
INITIAL_BACKOFF = 2.0  # seconds
BACKOFF_MULTIPLIER = 2.0


class PyteomicsAttribute(BaseModel):
    """Attribute in pyteomics PROXI response."""

    model_config = ConfigDict(extra="allow")

    accession: str
    name: str
    value: str | int | float | None = None


class PyteomicsSpectrum(BaseModel):
    """Pyteomics PROXI response format with numpy array support."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    usi: str
    status: str
    attributes: list[PyteomicsAttribute]
    mz_array: np.ndarray = Field(alias="m/z array")
    intensity_array: np.ndarray = Field(alias="intensity array")

    @field_validator("mz_array", "intensity_array", mode="before")
    @classmethod
    def validate_numpy_array(cls, v: Any) -> np.ndarray:
        """Ensure arrays are numpy arrays."""
        if isinstance(v, np.ndarray):
            return v
        if isinstance(v, list):
            return np.array(v)
        raise ValueError(f"Expected numpy array or list, got {type(v)}")

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Ensure status is READABLE."""
        if v != "READABLE":
            raise ValueError(f"Expected status 'READABLE', got '{v}'")
        return v

    def model_post_init(self, __context: Any) -> None:
        """Validate array lengths match."""
        if len(self.mz_array) != len(self.intensity_array):
            raise ValueError(
                f"Array length mismatch: "
                f"m/z array={len(self.mz_array)}, intensity array={len(self.intensity_array)}"
            )


def setup_args():
    """Configures and parses command line arguments."""
    parser = argparse.ArgumentParser(
        description="Process a CSV file with accession and ms_runs columns."
    )
    parser.add_argument("input_file", type=Path, help="Path to the input CSV file")
    parser.add_argument(
        "-o", "--output", type=Path, default=None, help="Path to save the processed CSV"
    )
    return parser.parse_args()


def find_matching_file(files_dict, ms_run):
    """
    Searches for a file path that contains both the ms_run string and '.mzid'.
    Returns the first matching file object, or None if not found.
    """
    # Combine 'result' and 'other' lists safely
    all_files = files_dict.get("result", []) + files_dict.get("other", [])

    # Pattern: ms_run ... anything ... .mzid (case-insensitive)
    pattern = re.compile(rf"{re.escape(ms_run)}.*\.mzid", re.IGNORECASE)

    for f in all_files:
        filepath = f.get("filepath", "")
        if pattern.search(filepath):
            return f

    return None


async def process_row(row):
    """
    Async function to process a single row: find file, download, extract.
    """
    pxd = row.get("pxd", "")
    ms_run = row.get("ms_run", "")

    if not pxd or not ms_run:
        print(f"Skipping invalid row: {row}")
        return row

    # 1. Query PRIDE for file list
    try:
        files = PrideBackend.get_files_for_project(pxd)
    except Exception as e:
        print(f"Error fetching files for {pxd}: {e}")
        return row

    # 2. Find the specific mzid file
    matched_file = find_matching_file(files, ms_run)

    if matched_file is None:
        print(f"No matching .mzid file found for PXD: {pxd}, MS Run: {ms_run}")
        return row

    print(f"Found matching file for {pxd}: {matched_file['filepath']}")

    # 3. Download and Extract in a temp directory
    # 'temporary_path' is a sync context manager, so we use 'with' (not 'async with')
    # inside the async function.
    with temporary_path() as tmp_dir:
        print(f"Created temp dir: {tmp_dir}")

        try:
            # await the async download function
            downloaded_path = await download_ftp(matched_file["filepath"], tmp_dir)
            print(f"Downloaded: {downloaded_path.name}")

            # extract_archive is sync, so just call it
            extract_dir = tmp_dir / "extracted"
            extracted_files = extract_archive(downloaded_path, extract_dir)

            for file in extracted_files:
                print(f"Extracted: {file.name}")

                file_parser = MzidFileParser()
                parsed_data = file_parser.parse_file(file, pxd)

                usis = generate_random_usis(parsed_data, count=5)
                matches, mismatches, download_errors = validate_usi_list(usis)

                print(f"USI Validation Results for {file.name}:")
                print(f"Matches: {len(matches)}")
                print(f"Mismatches: {len(mismatches)}")
                print(f"Download Errors: {len(download_errors)}")

                save_path = Path("mzid_issues") / f"{pxd}_{ms_run}.mzid"
                save_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(file, save_path)

                # if mismatches is > 0:
                # save mzid file for further inspection
                if len(mismatches) > 0 or len(download_errors) > 0:
                    print(f"Saved problematic mzid to: {save_path}")
                else:
                    print(f"Saved sane mzid to: {save_path}")
                print("Generated USIs:")
                for usi in usis:
                    print(f"\t{usi}")

                # validate generated USIs with

                input("Press any key to continue...")

                # --- YOUR PROCESSING LOGIC HERE ---
                # e.g., usis = extract_usis(file)
                # If you need to keep the file, copy it out of tmp_dir here.

        except Exception as e:
            print(f"  Error downloading/extracting {pxd}: {e}")

    return row


def get_attribute(data, target_name):
    """Helper to safely extract attributes from spectrum objects or dicts."""
    raw_attributes = getattr(data, "attributes", None) or data.get("attributes", [])
    for item in raw_attributes:
        name = item.get("name") if isinstance(item, dict) else getattr(item, "name", None)
        if name == target_name:
            return item.get("value") if isinstance(item, dict) else getattr(item, "value", None)
    return None


def validate_usi_list(usi_list):
    """
    Returns tuple: (matches, mismatches, download_errors)
    """
    matches, mismatches, errors = [], [], []

    for usi in usi_list:
        try:
            # Parse USI charge
            usi_charge = str(usi.split("/")[-1])

            # Fetch Spectrum
            spectrum = download_spectrum(usi)
            if not spectrum:
                errors.append(usi)
                continue

            # Get PRIDE charge
            pride_charge = get_attribute(spectrum, "charge state")
            if pride_charge is None:
                errors.append(usi)
                continue

            # Compare
            if str(pride_charge) == usi_charge:
                matches.append(usi)
            else:
                mismatches.append(usi)

        except Exception:
            errors.append(usi)

    return matches, mismatches, errors


def download_spectrum(usi: USI) -> PyteomicsSpectrum | None:
    """Download single spectrum from PROXI API with exponential backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            spectrum_data = proxi(usi, backend="pride")
            if not isinstance(spectrum_data, dict):
                raise ValueError("Response is not a dictionary")
            return PyteomicsSpectrum.model_validate(spectrum_data)

        except Exception as e:
            if "Internal Server Error" in str(e):
                print(f"Internal Server error for '{usi}', not retrying.")
                break
            if attempt < MAX_RETRIES - 1:
                time.sleep(INITIAL_BACKOFF * (BACKOFF_MULTIPLIER**attempt))
            else:
                print(f"All {MAX_RETRIES} retries exhausted for {usi}: {e}")
    return None


def generate_random_usis(parsed_data, count=20) -> list[str]:
    """
    Generates a list of random USIs from the parsed mzIdentML data.
    Assumes parsed_data contains dictionaries.
    Format: mzspec:{accession}:{ms_run}:{index_type}:{index_number}:{Peptide}/{charge}
    """
    psms = parsed_data.psms
    peptides = parsed_data.modified_peptides

    # 1. Create a lookup dictionary: Peptide UUID -> Peptide Sequence String
    # CHANGE: Accessed via ['id'] and ['peptide_sequence']
    peptide_lookup = {p["id"]: p["peptide_sequence"] for p in peptides}

    if not psms:
        print("No PSMs found in this file.")
        return []

    # 2. Sample random PSMs
    sample_size = min(count, len(psms))
    sampled_psms = random.sample(psms, sample_size)

    generated_usis = []

    print(f"\n--- Generating {sample_size} Random USIs ---\n")

    for psm in sampled_psms:
        try:
            # 3. Extract fields using Dictionary Access
            pxd = psm["project_accession"]
            ms_run = psm["ms_run"]
            index_num = psm["index_number"]
            charge = psm["charge_state"]

            # Handle Index Type (Enum vs String)
            raw_index_type = psm["index_type"]
            # If it's an Enum object (has .value), use that; otherwise use it directly
            index_type = (
                raw_index_type.value if hasattr(raw_index_type, "value") else raw_index_type
            )

            # 4. Get Peptide Sequence
            pep_id = psm["modified_peptide_id"]
            peptide_seq = peptide_lookup.get(pep_id, "UNKNOWN")

            # 5. Construct USI
            usi = f"mzspec:{pxd}:{ms_run}:{index_type}:{index_num}:{peptide_seq}/{charge}"

            generated_usis.append(usi)

        except KeyError as e:
            print(f"Skipping a USI due to missing key: {e}")
        except Exception as e:
            print(f"Error processing USI: {e}")

    return generated_usis


async def main():
    args = setup_args()

    # 1. Validation
    if not args.input_file.exists():
        print(f"Error: The file '{args.input_file}' was not found.")
        sys.exit(1)

    # 2. Read the File
    print(f"Reading from: {args.input_file}...")

    rows = []
    with open(args.input_file, newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)  # Read all into memory to avoid keeping file open during async ops

    # 3. Process Data
    # We iterate sequentially here. If you wanted parallel downloads,
    # you would use asyncio.gather(), but sequential is safer for FTP limits.
    for row in rows:
        await process_row(row)


if __name__ == "__main__":
    # Start the async event loop
    asyncio.run(main())
