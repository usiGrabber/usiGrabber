from typing import Any

import requests

from usigrabber.backends.base import BaseBackend, FileMetadata, Files
from usigrabber.utils import DATA_DIR, logger


class PrideBackend(BaseBackend):
    BASE_URL: str = "https://www.ebi.ac.uk/pride/ws/archive/v3"

    @classmethod
    def check_availability(cls, accession: str) -> bool:
        url = f"{cls.BASE_URL}/status/{accession}"
        with requests.get(url) as response:
            response.raise_for_status()
            return response.text == "PUBLIC"

    @classmethod
    def get_sample_projects(cls) -> list[str]:
        # read from DATA_DIR/files/sample_projects.json
        with open(DATA_DIR / "files" / "sample_projects.json", encoding="utf-8") as f:
            import json

            project_metadata = json.load(f)
            accessions = [project["accession"] for project in project_metadata]
            return accessions

    @classmethod
    def get_all_project_accessions(cls, is_test: bool = False) -> list[str]:
        if is_test:
            return cls.get_sample_projects()

        url = f"{cls.BASE_URL}/projects/all"
        with requests.get(url) as response:
            if response.status_code == 200:
                projects_info = response.json()
                accessions = [project["accession"] for project in projects_info]
                return accessions
            else:
                logger.error(
                    "Could not retrieve project accessions: %s %s",
                    response.status_code,
                    response.reason,
                )
                return []

    @classmethod
    def get_files_for_project(
        cls,
        project_accession: str,
    ) -> Files:
        url = f"{cls.BASE_URL}/projects/{project_accession}/files/all"
        with requests.get(url) as response:
            if response.status_code == 200:
                files_info = response.json()
                return Files(
                    search=[
                        FileMetadata(**file_info)
                        for file_info in files_info
                        if file_info["category"] == "SEARCH"
                    ],
                    result=[
                        FileMetadata(**file_info)
                        for file_info in files_info
                        if file_info["category"] == "RESULT"
                    ],
                )
            else:
                logger.error(
                    "Could not retrieve files for accession %s: %s %s",
                    project_accession,
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

    @classmethod
    def get_metadata_for_project(
        cls,
        project_accession: str,
    ) -> dict[str, Any]:
        url = f"{cls.BASE_URL}/projects/{project_accession}"
        with requests.get(url) as response:
            response.raise_for_status()
            metadata = response.json()
            return metadata

    @classmethod
    def process_file(cls, file: dict[str, Any], metadata: dict[str, Any]) -> None:
        # Example processing: just log the file info
        logger.info(
            "Processing file %s for project %s",
            file.get("fileName", "unknown"),
            metadata.get("accession", "unknown"),
        )


if __name__ == "__main__":
    SAMPLE_ACCESSION = "PXD001357"
    # print(PrideBackend.get_metadata_for_project(SAMPLE_ACCESSION))
    # print(PrideBackend.get_files_for_project(SAMPLE_ACCESSION))
    # print(PrideBackend.get_all_project_accessions(is_test=True))
