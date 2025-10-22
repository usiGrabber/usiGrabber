import pandas as pd
import requests
from pathlib import Path
import ijson
from tqdm import tqdm
import csv
from pyteomics import mzid

from usigrabber.main import get_files_of_category, download_ftp

def download_all_files(json_path: Path):
    """ Download all files metadata from PRIDE Archive API and save to a JSON file."""
    url = "https://www.ebi.ac.uk/pride/ws/archive/v3/files/all"

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_bytes = int(r.headers.get("content-length", 0))
        with open(json_path, "wb") as f, tqdm(
            total=total_bytes, unit="B", unit_scale=True, unit_divisor=1024, desc="Downloading"
        ) as pbar:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))

    print(f"All files downloaded to {json_path}")

def create_csv(json_path: Path, csv_path: Path):
    """ Parse PRIDE Archive all files JSON and create a filtered CSV of RESULT files. """

    with open(json_path, "r", encoding="utf-8") as in_f, open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["accession", "project_accession", "file", "size"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # iterate through JSON items
        for file_item in ijson.items(in_f, "item"):
            if file_item["fileCategory"]["value"] != "RESULT":
                continue
            
            accession = file_item["accession"]
            if len(file_item["projectAccessions"]) > 1:
                print(f"Warning: multiple project accessions found for file {file_item['accession']}, using the first one.")
            project_accession = file_item["projectAccessions"][0]  # assuming only one accession
            filesize = file_item["fileSizeBytes"]
            file_url = None
            for file_loc in file_item["publicFileLocations"]:
                if file_loc["name"] == "FTP Protocol":
                    file_url = file_loc["value"]
                    break

            writer.writerow({
                "accession": accession,
                "project_accession": project_accession,
                "file": file_url,
                "size": filesize,
            })

    print(f"CSV file created at {csv_path}")


def histogram_of_file_categories(json_path: Path):
    """ Generate and print a histogram of file categories from the JSON file. """
    category_counts = {}

    with open(json_path, "r", encoding="utf-8") as in_f:
        for file_item in ijson.items(in_f, "item"):
            category = file_item["fileCategory"]["value"]
            category_counts[category] = category_counts.get(category, 0) + 1

    print("Histogram of file categories:")
    for category, count in category_counts.items():
        print(f"{category}: {count}")


if __name__ == "__main__":
    json_path = Path.cwd() / "all_files.json"
    # download_all_files(json_path)

    csv_path = Path.cwd() / "result_files.csv"
    # create_csv(json_path, csv_path)

    # histogram_of_file_categories(json_path)

    SAMPLE_ACCESSION = "PXD001357"
    root_path = Path.cwd() / "project_archive"
    project_path = root_path / SAMPLE_ACCESSION
    if not project_path.exists():
        result_files = get_files_of_category(SAMPLE_ACCESSION, category="RESULT")
        download_ftp(result_files[0], out_dir=project_path)

        # extract archive

    