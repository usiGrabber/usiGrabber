#!/usr/bin/env python3
"""
Read pride_files_all.json and extract the first 100 mzid file entries.
"""

import json


def extract_mzid_entries(
    input_file: str = "experimental/pride_files_all.json", max_entries: int = 100
):
    """Extract first N mzid file entries from PRIDE files JSON."""
    print(f"Reading {input_file}...")

    with open(input_file) as f:
        files_data = json.load(f)

    print(f"Total files in dataset: {len(files_data)}")

    # Filter for mzid files
    mzid_files = []
    for file_info in files_data:
        file_name = file_info.get("fileName", "")

        if file_name.endswith(".mzid"):
            mzid_files.append(file_info)

            if len(mzid_files) >= max_entries:
                break

    print(f"Found {len(mzid_files)} mzid files (limited to {max_entries})")

    # Save to new JSON file
    output_file = "experimental/pride_mzid_files_100.json"
    with open(output_file, "w") as f:
        json.dump(mzid_files, f, indent=2)

    print(f"Saved to {output_file}")

    # Print summary of first few entries
    print("\nFirst 5 entries:")
    for idx, file_info in enumerate(mzid_files[:5], 1):
        file_name = file_info.get("fileName", "UNKNOWN")
        project_accession = file_info.get("projectAccession", "UNKNOWN")
        file_size = file_info.get("fileSizeBytes", 0)
        print(f"  {idx}. {file_name}")
        print(f"     Project: {project_accession}, Size: {file_size / 1024 / 1024:.2f} MB")


if __name__ == "__main__":
    extract_mzid_entries()
