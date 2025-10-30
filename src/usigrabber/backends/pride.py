from typing import Any

import requests

from usigrabber.backends.base import BaseBackend
from usigrabber.utils import logger


class PrideBackend(BaseBackend):
    BASE_URL: str = "https://www.ebi.ac.uk/pride/ws/archive/v3"

    @classmethod
    def check_availability(cls, accession: str) -> bool:
        url = f"{cls.BASE_URL}/status/{accession}"
        with requests.get(url) as response:
            response.raise_for_status()
            return response.text == "PUBLIC"

    @classmethod
    def get_files_for_project(
        cls,
        project_accession: str,
    ) -> list[dict[str, Any]]:
        url = f"{cls.BASE_URL}/projects/{project_accession}/files/all"
        with requests.get(url) as response:
            if response.status_code == 200:
                files_info = response.json()
                return files_info
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
