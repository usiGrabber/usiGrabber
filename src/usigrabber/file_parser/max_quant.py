from collections.abc import Generator
from pathlib import Path

import pandas as pd
from pyteomics.usi import USI

from usigrabber.utils import data_directory_path, logger


def maxquant_generate_usis(
    project_accession: str, project_path: Path
) -> Generator[USI, None, None]:
    evidence_file = project_path / "evidence.txt"
    if not evidence_file.exists():
        raise FileNotFoundError(f"Evidence file {evidence_file} does not exist.")

    with evidence_file.open("r") as f:
        df = pd.read_csv(f, sep="\t")

        found_invalid_scan_no = False

        for index, row in df.iterrows():
            raw_file: str = str(row["Raw file"])
            try:
                scan_number: int = int(row["MS/MS scan number"])
            except ValueError:
                if not found_invalid_scan_no:
                    # TODO: investigate why some scan numbers are NaN
                    # maybe because this isn't a scan but one
                    # of (index, nativeId, trace)?
                    logger.warning(
                        f"Invalid scan number '{row['MS/MS scan number']}' "
                        + f"at row {index}. Skipping."
                    )
                found_invalid_scan_no = True
                continue
            charge: int = int(row["Charge"])
            if row["Modifications"] == "Unmodified":
                continue
            mod_seq: str = str(row["Modified sequence"]).replace("_", "")

            # TODO: this is currently incorrect, as we expect
            # mods to be in UniMod format
            usi = USI(
                project_accession,
                raw_file,
                "scan",
                str(scan_number),
                mod_seq,
                charge,
            )

            yield usi


if __name__ == "__main__":
    SAMPLE_ACCESSION = "PXD014174"
    # SAMPLE_ACCESSION = "PXD069312"  # yannicks project

    root_path = data_directory_path() / "project_archive"
    project_path = root_path / SAMPLE_ACCESSION

    # generate USIs
    print("Generated USIs:")
    count = 0
    for usi in maxquant_generate_usis(
        project_accession=SAMPLE_ACCESSION, project_path=project_path
    ):
        if "(" in usi:
            count += 1
            print(usi)

        if count >= 3:
            break
