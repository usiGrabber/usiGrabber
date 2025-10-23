import csv

import requests
from tqdm import tqdm

from usigrabber.utils import data_directory_path, iter_json, logger


class PRIDE:
    BASE_URL = "https://www.ebi.ac.uk/pride/ws/archive/v3"
    JSON_PATH = data_directory_path() / "files" / "all_files.json"
    JSON_EXISTS = JSON_PATH.exists()
    RESULT_CSV_PATH = data_directory_path() / "files" / "result_files.csv"
    CSV_EXISTS = RESULT_CSV_PATH.exists()

    @classmethod
    def all_files_to_json(cls) -> None:
        """
        Download all files metadata from PRIDE Archive API and save to a JSON file.
        """
        if cls.JSON_EXISTS:
            logger.debug(
                "All files JSON already exists at %s. Remove to re-run.", cls.JSON_PATH
            )
            return

        url = f"{PRIDE.BASE_URL}/files/all"
        logger.debug(
            "Downloading all files metadata from '%s' to '%s'", url, cls.JSON_PATH
        )

        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total_bytes = int(r.headers.get("content-length", 0))
            with (
                open(cls.JSON_PATH, "wb") as f,
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

        logger.debug("All files downloaded to %s", cls.JSON_PATH)

    @classmethod
    def filter_result_files_to_csv(cls) -> None:
        """
        Parse PRIDE Archive all files JSON (`PRIDE.all_files`) and create a filtered CSV of RESULT files.
        """

        with open(cls.RESULT_CSV_PATH, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["accession", "project_accession", "file", "size"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            # iterate through JSON items
            for file_item in iter_json(cls.JSON_PATH):
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

        logger.debug("CSV file created at %s", cls.RESULT_CSV_PATH)

    @classmethod
    def histogram_of_file_categories(cls) -> dict[str, int]:
        """
        Generate and print a histogram of file categories from the JSON file.
        """
        if not cls.JSON_EXISTS:
            raise FileNotFoundError(
                f"JSON file {cls.JSON_PATH} does not exist. Run `PRIDE.all_files_to_json()` first."
            )

        logger.debug("Generating histogram of file categories from %s", cls.JSON_PATH)
        category_counts = {}

        for file_item in iter_json(cls.JSON_PATH):
            category = file_item["fileCategory"]["value"]
            category_counts[category] = category_counts.get(category, 0) + 1

        return category_counts

    @classmethod
    def check_availability(cls, accession: str) -> bool:
        url = f"{cls.BASE_URL}/status/{accession}"
        with requests.get(url) as response:
            response.raise_for_status()
            return response.text == "PUBLIC"

    @classmethod
    def get_files_for_project(
        cls,
        accession: str,
    ) -> list[dict]:
        url = f"{cls.BASE_URL}/projects/{accession}/files/all"
        with requests.get(url) as response:
            if response.status_code == 200:
                files_info = response.json()
                return files_info
            else:
                logger.error(
                    "Could not retrieve files for accession %s: %s %s",
                    accession,
                    response.status_code,
                    response.reason,
                )
                return []

    @classmethod
    def get_files_of_category(
        cls, accession: str, category: str = "SEARCH"
    ) -> list[str]:
        url = f"{cls.BASE_URL}/projects/{accession}/files"
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
                logger.error(
                    "Could not retrieve files for accession %s: %s %s",
                    accession,
                    response.status_code,
                    response.reason,
                )
                return []


if __name__ == "__main__":
    # PRIDE.all_files_to_json()
    # PRIDE.filter_result_files_to_csv()

    # hist = histogram_of_file_categories(json_path)
    # for category, count in hist.items():
    #     print(f"{category}: {count}")

    SAMPLE_ACCESSION = "PXD001357"
    # root_path = project_root / "data" / "project_archive"
    # project_path = root_path / SAMPLE_ACCESSION
    # if not project_path.exists():
    #     result_files = get_files_of_category(SAMPLE_ACCESSION, category="RESULT")
    #     download_ftp(result_files[0], out_dir=project_path)

    # extract archive
