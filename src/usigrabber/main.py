import logging
import os
import tarfile
import urllib.parse
import urllib.request
from collections.abc import Generator
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SAMPLE_ACCESSION = "PXD014174"
SAMPLE_ACCESSION = "PXD069312"  # yannicks project
# url = "ftp://ftp.pride.ebi.ac.uk/pride/data/archive/2020/03/PXD014174/txt.tar.gz"

BASE_URL = "https://www.ebi.ac.uk/pride/ws/archive/v3"


def check_availability(accession: str) -> bool:
    url = f"{BASE_URL}/status/{accession}"
    with requests.get(url) as response:
        response.raise_for_status()
        return response.text == "PUBLIC"


def get_files_of_category(accession: str, category: str = "SEARCH") -> list[str]:
    url = f"{BASE_URL}/projects/{accession}/files"
    with requests.get(url) as response:
        if response.status_code == 200:
            files_info = response.json()
            files = []
            for file_info in files_info:
                if file_info["fileCategory"]["value"] == category:
                    for download_link in file_info["publicFileLocations"]:
                        if download_link["name"] == "FTP Protocol":
                            files.append(download_link["value"])
                            break

            return files
        else:
            logger.error("Error: %s %s", response.status_code, response.reason)
            return []
            # raise ValueError(f"Could not retrieve files for accession {accession}")


def download_ftp(url: str, out_dir: Path, file_name: str | None = None) -> None:
    # create directory
    out_dir.mkdir(parents=True)

    parsed = urllib.parse.urlparse(url)
    filename = file_name or os.path.basename(parsed.path)
    out_path = out_dir / filename

    def _reporthook(block_num, block_size, total_size):
        if total_size > 0:
            downloaded = block_num * block_size
            pct = downloaded / total_size * 100
            downloaded = min(downloaded, total_size)
            downloaded_mb = downloaded / (1024 * 1024)
            total_mb = total_size / (1024 * 1024)
            print(
                f"\rDownloading {filename}: {pct:5.1f}% ({downloaded_mb:5.1f}MB/{total_mb:5.1f}MB)",
                end="",
            )
        else:
            downloaded_mb = (block_num * block_size) / (1024 * 1024)
            print(f"\rDownloading {filename}: {downloaded_mb:5.1f}MB", end="")

    urllib.request.urlretrieve(url, filename=str(out_path), reporthook=_reporthook)
    print("\n")
    logger.debug("Saved to %s", out_path)


def extract_archive(archive_path: Path, extract_to: Path):
    with tarfile.open(archive_path, "r:gz") as tar:
        tar.extractall(path=extract_to)
    logger.debug("Extracted %s to %s", archive_path, extract_to)


def generate_usis(project_path: Path) -> Generator[str, None, None]:
    evidence_file = project_path / "evidence.txt"
    if not evidence_file.exists():
        raise FileNotFoundError(f"Evidence file {evidence_file} does not exist.")

    with evidence_file.open("r") as f:
        df = pd.read_csv(f, sep="\t")

        for index, row in df.iterrows():
            raw_file: str = row["Raw file"]
            scan_number: int = int(row["MS/MS scan number"])
            charge: int = int(row["Charge"])
            mod_seq: str = row["Modified sequence"].replace("_", "")

            # parse modifications
            # find ( and ) pairs

            # mods now contains parsed modifications with start/end positions, names and letters

            # mod_seq = row["Sequence"]
            usi = f"mzspec:{SAMPLE_ACCESSION}:{raw_file}:scan:{scan_number}:{mod_seq}/{charge}"
            yield usi


if __name__ == "__main__":
    root_path = Path.cwd() / "project_archive"
    project_path = root_path / SAMPLE_ACCESSION

    if not project_path.exists():
        os.mkdir(project_path)

        # check availability
        if check_availability(SAMPLE_ACCESSION):
            print(f"Accession {SAMPLE_ACCESSION} is public.")
        else:
            print(f"Accession {SAMPLE_ACCESSION} is not public.")
            exit(1)

        # get search files
        search_files = get_files_of_category(SAMPLE_ACCESSION)
        print(f"Search files for accession {SAMPLE_ACCESSION}:")
        for f in search_files:
            print(f" - {f}")
            download_ftp(f, project_path)
            archive_filename = os.path.basename(urllib.parse.urlparse(f).path)
            extract_archive(project_path / archive_filename, project_path)

    # generate USIs
    print("Generated USIs:")
    count = 0
    for usi in generate_usis(project_path):
        if "(" in usi:
            count += 1
            print(usi)

        if count >= 20:
            break

    print(f"Total USIs generated: {count}")
