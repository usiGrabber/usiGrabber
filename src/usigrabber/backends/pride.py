import os
from collections.abc import Generator
from typing import Any

import requests
from pyteomics import mzid

from usigrabber.backends.base import (
    PSM,
    BaseBackend,
    FileMetadata,
    Files,
    Ref,
    ScanIdentifierType,
)
from usigrabber.utils import DATA_DIR, logger
from usigrabber.utils.file import download_ftp, extract_archive, temporary_path


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
        # read from DATA_DIR/files/sampled_projects.json
        with open(DATA_DIR / "files" / "sampled_projects.json", encoding="utf-8") as f:
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
                search_files = []
                result_files = []
                for file_info in files_info:
                    category = file_info["fileCategory"]["value"]
                    ftp_link = None
                    for loc in file_info.get("publicFileLocations", []):
                        if loc.get("name") == "FTP Protocol":
                            ftp_link = loc.get("value")
                            break
                    if not ftp_link:
                        logger.warning(
                            "No FTP link found for file %s in project %s. Skipping.",
                            file_info.get("fileName"),
                            project_accession,
                        )
                        continue

                    file = FileMetadata(
                        {
                            "filepath": ftp_link,
                            "category": category,
                            "file_size": file_info.get("fileSizeBytes"),
                        }
                    )

                    if category == "SEARCH":
                        search_files.append(file)
                    elif category == "RESULT":
                        result_files.append(file)

                return Files(search=search_files, result=result_files)
            else:
                logger.error(
                    "Could not retrieve files for accession %s: %s %s",
                    project_accession,
                    response.status_code,
                    response.reason,
                )
                return Files(search=[], result=[])

    @classmethod
    def get_files_of_category(cls, accession: str, category: str = "SEARCH") -> list[str]:
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
    def process_result_file(cls, file: FileMetadata) -> Generator[PSM, None, None]:
        # parse filename from file url
        file_url = file["filepath"]
        filename = os.path.basename(file_url)

        logger.debug(
            f"Processing result file {filename} " + f"({file['file_size'] / (1024 * 1024):,.2f} MB)"
        )

        # extract name and extension
        file_parts = filename.split(".")
        filename = file_parts[0]
        ext = file_parts[-1]

        with temporary_path() as tmp_dir:
            # download file
            path = download_ftp(file_url, out_dir=tmp_dir, file_name=filename)

            # optional: extract if archived
            if ext in {".gz", ".zip", ".tar"}:
                extract_archive(path, extract_to=tmp_dir)
                path = tmp_dir / (filename + ".mzid")  # assume mzid inside

            with mzid.read(source=str(path)) as reader:
                for i, psm in enumerate(reader):
                    psm: dict[str, Any]
                    sids = psm.get("SpectrumIdentificationItem", [])
                    if len(sids) != 1:
                        logger.warning(
                            "PSM %d in file %s has %d SpectrumIdentificationItems, "
                            + "expected 1. Skipping.",
                            psm.get("spectrumID", i),
                            file["filepath"],
                            len(sids),
                        )
                        continue

                    sid: dict[str, Any] = sids[0]

                    yield PSM(
                        datafile=file["filepath"],
                        scan_identifier_type=ScanIdentifierType.SCAN,
                        scan_identifier="",
                        peptide_sequence=psm.get("PeptideSequence", ""),
                        charge=psm.get("chargeState", ""),
                        experimental_mass_to_charge=sid.get("experimentalMassToCharge", 0),
                        retention_time=psm.get("retention time(s)", None),
                        refs=[
                            Ref(
                                start=ref.get("start", 0),
                                end=ref.get("end", 0),
                                pre=ref.get("pre", ""),
                                post=ref.get("post", ""),
                                is_decoy=ref.get("isDecoy", False),
                                protein=ref.get("accession", ""),
                            )
                            for ref in sid.get("PeptideEvidenceRef", [])
                        ],
                        modifications=[
                            {"name": mod["name"], "position": mod["location"]}
                            for mod in psm.get("Modifications", [])
                        ],
                    )


if __name__ == "__main__":
    SAMPLE_ACCESSION = "PXD001357"
    # print(PrideBackend.get_metadata_for_project(SAMPLE_ACCESSION))
    # print(PrideBackend.get_files_for_project(SAMPLE_ACCESSION))
    # print(PrideBackend.get_all_project_accessions(is_test=True))
