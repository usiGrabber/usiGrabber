import csv
from pathlib import Path

import requests
from tqdm import tqdm

from usigrabber.utils import iter_json, logger, project_root_path


class PRIDE:
    BASE_URL = "https://www.ebi.ac.uk/pride/ws/archive/v3"

    @classmethod
    def all_files_to_json(cls, json_path: Path) -> None:
        """
        Download all files metadata from PRIDE Archive API and save to a JSON file.
        """
        url = f"{PRIDE.BASE_URL}/files/all"
        logger.debug("Downloading all files metadata from '%s' to '%s'", url, json_path)

        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total_bytes = int(r.headers.get("content-length", 0))
            with (
                open(json_path, "wb") as f,
                tqdm(
                    total=total_bytes,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc="Downloading",
                ) as pbar,
            ):
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

        logger.debug("All files downloaded to %s", json_path)

    @classmethod
    def filter_result_files_to_csv(cls, json_path: Path, csv_path: Path) -> None:
        """
        Parse PRIDE Archive all files JSON (`PRIDE.all_files`) and create a filtered CSV of RESULT files.
        """

        with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["accession", "project_accession", "file", "size"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            # iterate through JSON items
            for file_item in iter_json(json_path):
                if file_item["fileCategory"]["value"] != "RESULT":
                    continue

                accession = file_item["accession"]
                if len(file_item["projectAccessions"]) > 1:
                    logger.warning(
                        "Multiple project accessions found for file %s, using the first one.",
                        file_item["accession"],
                    )

                # assuming only one accession
                project_accession = file_item["projectAccessions"][0]
                filesize = file_item["fileSizeBytes"]
                file_url = None
                for file_loc in file_item["publicFileLocations"]:
                    if file_loc["name"] == "FTP Protocol":
                        file_url = file_loc["value"]
                        break

                writer.writerow(
                    {
                        "accession": accession,
                        "project_accession": project_accession,
                        "file": file_url,
                        "size": filesize,
                    }
                )

        logger.debug("CSV file created at %s", csv_path)


class MZID:
    pass


def histogram_of_file_categories(json_path: Path) -> dict[str, int]:
    """
    Generate and print a histogram of file categories from the JSON file.
    """
    logger.debug("Generating histogram of file categories from %s", json_path)
    category_counts = {}

    with open(json_path, encoding="utf-8") as in_f:
        for file_item in ijson.items(in_f, "item"):
            category = file_item["fileCategory"]["value"]
            category_counts[category] = category_counts.get(category, 0) + 1

    return category_counts


if __name__ == "__main__":
    project_root = project_root_path()
    json_path = project_root / "data" / "files" / "all_files.json"
    PRIDE.all_files_to_json(json_path)

    csv_path = project_root / "data" / "files" / "result_files.csv"
    PRIDE.filter_result_files_to_csv(json_path, csv_path)

    # hist = histogram_of_file_categories(json_path)
    # for category, count in hist.items():
    #     print(f"{category}: {count}")

    # SAMPLE_ACCESSION = "PXD001357"
    # root_path = project_root / "data" / "project_archive"
    # project_path = root_path / SAMPLE_ACCESSION
    # if not project_path.exists():
    #     result_files = get_files_of_category(SAMPLE_ACCESSION, category="RESULT")
    #     download_ftp(result_files[0], out_dir=project_path)

    # extract archive
