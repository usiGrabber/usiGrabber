import pandas as pd
import requests
from pathlib import Path
import ijson
from tqdm import tqdm
import csv

def download_all_files(path: Path):
    url = "https://www.ebi.ac.uk/pride/ws/archive/v3/files/all"

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_bytes = int(r.headers.get("content-length", 0))
        with open(path, "wb") as f, tqdm(
            total=total_bytes, unit="B", unit_scale=True, unit_divisor=1024, desc="Downloading"
        ) as pbar:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))

    print(f"All files downloaded to {path}")

def create_csv(in_path: Path, out_path: Path):

    with open(in_path, "r", encoding="utf-8") as in_f, open(out_path, "w", newline="", encoding="utf-8") as csvfile:
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

    print(f"CSV file created at {out_path}")


# def find_result_files


if __name__ == "__main__":
    json_path = Path.cwd() / "all_files.json"
    # download_all_files(json_path)

    csv_path = Path.cwd() / "result_files.csv"
    create_csv(json_path, csv_path)