import json
from typing import Any

import requests
from sqlmodel import Session

from usigrabber.backends.base import BaseBackend, FileMetadata, Files
from usigrabber.db import Project, ProjectCountry, ProjectKeyword, ProjectTag, Reference
from usigrabber.db.schema import ProjectAffiliation, ProjectOtherOmicsLink
from usigrabber.utils import DATA_DIR, logger, parse_date


class PrideBackend(BaseBackend):
    BASE_URL: str = "https://www.ebi.ac.uk/pride/ws/archive/v3"
    SAMPLED_PROJECTS_PATH = DATA_DIR / "files" / "sampled_projects.json"

    @classmethod
    def check_availability(cls, accession: str) -> bool:
        url = f"{cls.BASE_URL}/status/{accession}"
        with requests.get(url) as response:
            response.raise_for_status()
            return response.text == "PUBLIC"

    @classmethod
    def get_sample_projects(cls) -> list[str]:
        if not cls.SAMPLED_PROJECTS_PATH.exists():
            raise FileNotFoundError(
                f"Sampled projects file not found at {cls.SAMPLED_PROJECTS_PATH}"
            )
        # read from DATA_DIR/files/sampled_projects.json
        with open(cls.SAMPLED_PROJECTS_PATH, encoding="utf-8") as f:
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
        is_test: bool = False,
    ) -> dict[str, Any]:
        if is_test:
            if not cls.SAMPLED_PROJECTS_PATH.exists():
                raise FileNotFoundError(
                    f"Sampled projects file not found at {cls.SAMPLED_PROJECTS_PATH}"
                )

            with open(DATA_DIR / "files" / "sampled_projects.json", encoding="utf-8") as f:
                project_metadata = json.load(f)
                for project in project_metadata:
                    if project["accession"] == project_accession:
                        return project

        url = f"{cls.BASE_URL}/projects/{project_accession}"
        with requests.get(url) as response:
            response.raise_for_status()
            return response.json()

    @classmethod
    def dump_project_to_db(cls, session: Session, project_data: dict[str, Any]) -> None:
        # 1. Create Project
        project = Project(
            accession=project_data["accession"],
            title=project_data["title"],
            projectDescription=project_data.get("projectDescription"),
            sampleProcessingProtocol=project_data.get("sampleProcessingProtocol"),
            dataProcessingProtocol=project_data.get("dataProcessingProtocol"),
            doi=project_data.get("doi"),
            submissionType=project_data["submissionType"],
            license=project_data.get("license"),
            submissionDate=parse_date(project_data.get("submissionDate")),
            publicationDate=parse_date(project_data.get("publicationDate")),
            totalFileDownloads=project_data.get("totalFileDownloads", 0),
            sampleAttributes=project_data.get("sampleAttributes"),
            additionalAttributes=project_data.get("additionalAttributes"),
        )
        session.add(project)

        # 2. References
        for ref_data in project_data.get("references", []):
            reference = Reference(
                project_accession=project.accession,
                referenceLine=ref_data.get("referenceLine"),
                pubmedID=ref_data.get("pubmedID"),
                doi=ref_data.get("doi"),
            )
            session.add(reference)

        # 3. Keywords
        for keyword in project_data.get("keywords", []):
            if keyword:  # Skip empty strings
                session.add(ProjectKeyword(project_accession=project.accession, keyword=keyword))

        # 4. Tags
        for tag in project_data.get("projectTags", []):
            if tag:
                session.add(ProjectTag(project_accession=project.accession, tag=tag))

        # 5. Countries
        for country in project_data.get("countries", []):
            if country:
                session.add(ProjectCountry(project_accession=project.accession, country=country))

        # 6. Affiliations
        for affiliation in project_data.get("affiliations", []):
            if affiliation:
                session.add(
                    ProjectAffiliation(project_accession=project.accession, affiliation=affiliation)
                )

        # 7. Other Omics Links
        for link in project_data.get("otherOmicsLinks", []):
            if link:
                session.add(ProjectOtherOmicsLink(project_accession=project.accession, link=link))
