#!/usr/bin/env python3
"""
Download the first 5 small mzid files from PRIDE Archive using pride_files_all.json.
Prints filename and project accession for each downloaded file.
"""

import json

import requests


def get_ftp_url(file_info: dict) -> str | None:
    """Extract FTP download URL from publicFileLocations."""
    public_locations = file_info.get("publicFileLocations", [])
    for location in public_locations:
        if location.get("name") == "FTP Protocol":
            ftp_url = location.get("value", "")
            # Convert ftp:// to https:// for easier downloading
            if ftp_url.startswith("ftp://"):
                return ftp_url.replace("ftp://", "https://")
    return None


def download_small_mzid_files(
    input_file: str = "experimental/pride_files_all.json",
    max_files: int = 5,
    max_size_mb: float = 10.0,
):
    """
    Download small mzid files from PRIDE Archive.

    Args:
            input_file: Path to pride_files_all.json
            max_files: Maximum number of files to download
            max_size_mb: Maximum file size in MB to consider
    """
    max_size_bytes = max_size_mb * 1024 * 1024

    print(f"Reading file metadata from {input_file}...")
    print(f"Looking for mzid files smaller than {max_size_mb} MB\n")

    with open(input_file) as f:
        files_data = json.load(f)

    print(f"Total files in dataset: {len(files_data)}")

    # Filter for small mzid files
    mzid_files = []
    for file_info in files_data:
        file_name = file_info.get("fileName", "")
        file_size = file_info.get("fileSizeBytes", 0)

        if file_name.endswith(".mzid") and file_size > 0 and file_size <= max_size_bytes:
            mzid_files.append(file_info)

            if len(mzid_files) >= max_files:
                break

    print(f"Found {len(mzid_files)} suitable mzid files\n")

    # Download and print info for each file
    for idx, file_info in enumerate(mzid_files, 1):
        file_name = file_info["fileName"]
        project_accessions = file_info.get("projectAccessions", [])
        project_accession = project_accessions[0] if project_accessions else "UNKNOWN"
        file_size = file_info.get("fileSizeBytes", 0)
        download_url = get_ftp_url(file_info)

        print(f"[{idx}/{len(mzid_files)}] Downloading: {file_name}")
        print(f"  Project: {project_accession}")
        print(f"  Size: {file_size / 1024 / 1024:.2f} MB")

        if download_url:
            print(f"  URL: {download_url}")
            try:
                file_response = requests.get(download_url, timeout=120)
                file_response.raise_for_status()

                # Save to experimental directory
                output_path = f"experimental/{file_name}"
                with open(output_path, "wb") as f:
                    f.write(file_response.content)

                print(f"  Saved to: {output_path}")
                print("  ✓ Success\n")

            except Exception as e:
                print(f"  ✗ Error downloading: {e}\n")
        else:
            print("  ✗ No download URL available\n")


if __name__ == "__main__":
    download_small_mzid_files("data/all_files.json")
