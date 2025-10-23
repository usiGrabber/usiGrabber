import os
import urllib.parse
from collections.abc import Generator
from pathlib import Path

import pandas as pd

from usigrabber.pride import PRIDE
from usigrabber.utils.file import download_ftp

# SAMPLE_ACCESSION = "PXD014174"
SAMPLE_ACCESSION = "PXD069312"  # yannicks project
# url = "ftp://ftp.pride.ebi.ac.uk/pride/data/archive/2020/03/PXD014174/txt.tar.gz"

BASE_URL = "https://www.ebi.ac.uk/pride/ws/archive/v3"


def maxquant_generate_usis(project_path: Path) -> Generator[str, None, None]:
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
        if PRIDE.check_availability(SAMPLE_ACCESSION):
            print(f"Accession {SAMPLE_ACCESSION} is public.")
        else:
            print(f"Accession {SAMPLE_ACCESSION} is not public.")
            exit(1)

        # get search files
        search_files = PRIDE.get_files_of_category(SAMPLE_ACCESSION)
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
