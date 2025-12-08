import os
from collections.abc import AsyncGenerator
from typing import Any

import ijson
import requests
from async_http_client import AsyncHttpClient
from ontology_resolver.ontology_helper import OntologyHelper
from sqlmodel import Session

from usigrabber.backends.base import BaseBackend, FileMetadata, Files
from usigrabber.cv_parameters.cv_engine import CVInjector, CVParam, CVTuple
from usigrabber.db import Project, ProjectCountry, ProjectKeyword, ProjectTag, Reference
from usigrabber.db.schema import ProjectAffiliation, ProjectOtherOmicsLink
from usigrabber.utils import get_cache_dir, logger, parse_date


def parse_cv_param(cv_data: dict) -> CVParam | None:
    """Parse and validate a CV parameter from PRIDE project data.

    Args:
            cv_data: Dictionary containing CV parameter fields (accession, value)

    Returns:
            CVParam if validation succeeds, None otherwise
    """
    cv_accession = cv_data.get("accession")
    if not isinstance(cv_accession, str):
        logger.error(f"Pride CV Param accession is malformed: {cv_data}")
        return None

    cv_value = cv_data.get("value")
    return CVParam(accession=cv_accession, value=cv_value)


def parse_cv_tuple(cv_data: dict) -> CVTuple | None:
    """Parse and validate a CV tuple from PRIDE project data.

    Args:
            cv_data: Dictionary containing 'key' and 'value' fields with CV parameters

    Returns:
            CVTuple if validation succeeds, None otherwise
    """
    if "key" not in cv_data or not isinstance(cv_data["key"], dict):
        logger.warning(f"Pride CV Tuple (key) is malformed: {cv_data}")
        return None
    if "value" not in cv_data or not isinstance(cv_data["value"], dict):
        logger.warning(f"Pride CV Tuple (value) is malformed: {cv_data}")
        return None

    if "accession" not in cv_data["key"]:
        logger.warning(f"Pride CV Tuple key missing accession: {cv_data}")
        return None
    if "accession" not in cv_data["value"]:
        logger.warning(f"Pride CV Tuple value missing accession: {cv_data}")
        return None

    key = CVParam(accession=cv_data["key"]["accession"], value=cv_data["key"].get("value"))
    value = CVParam(
        accession=cv_data["value"]["accession"],
        value=cv_data["value"].get("value"),
    )

    return CVTuple(key, value)


class PrideBackend(BaseBackend):
    BASE_URL: str = "https://www.ebi.ac.uk/pride/ws/archive/v3"

    @staticmethod
    def _convert_ftp_to_http(ftp_url: str) -> str:
        """Convert FTP URL to HTTP URL for PRIDE archive.

        Converts from: ftp://ftp.pride.ebi.ac.uk/pride/data/archive/<year>/<month>/<accession>/<filename>
        To: https://ftp.pride.ebi.ac.uk/pride/data/archive/<year>/<month>/<accession>/<filename>
        """
        if ftp_url.startswith("ftp://ftp.pride.ebi.ac.uk"):
            return ftp_url.replace("ftp://", "https://", 1)
        return ftp_url

    @classmethod
    def check_availability(cls, accession: str) -> bool:
        url = f"{cls.BASE_URL}/status/{accession}"
        with requests.get(url) as response:
            response.raise_for_status()
            return response.text == "PUBLIC"

    @classmethod
    def get_project_accession(cls, project: dict[str, Any]) -> str:
        return project["accession"]

    @classmethod
    async def get_project(cls, project_accession: str) -> dict[str, Any]:
        projects = cls.get_new_projects(existing_accessions=set())
        total_searched_projects = 0
        async for project in projects:
            total_searched_projects += 1
            if project["accession"] == project_accession:
                return project
        raise ValueError(
            f"No project found for accession: {project_accession} in {total_searched_projects} projects"
        )

    @classmethod
    def is_project_complete(cls, project: dict[str, Any]) -> bool:
        return project.get("submissionType") == "COMPLETE"

    @classmethod
    async def get_new_projects(
        cls,
        existing_accessions: set[str],
    ) -> AsyncGenerator[dict[str, Any], None]:
        file_path = get_cache_dir() / "pride" / "all_projects.json"
        is_debug = os.getenv("DEBUG")
        if is_debug:
            file_name = os.getenv("DEBUG_PROJECTS_FILENAME", "sampled_projects.json")
            file_path = get_cache_dir() / "pride" / file_name
        # if file doesnt exist, download it
        if not file_path.exists():
            if is_debug:
                raise FileNotFoundError(f"Debug projects file: {file_path} not found.")

            url = f"{cls.BASE_URL}/projects/all"

            async with AsyncHttpClient() as client:
                await client.stream_file(
                    url,
                    download_file_name=file_path,
                )

        with open(file_path, encoding="utf-8") as in_f:
            for project in ijson.items(in_f, "item"):
                if project["accession"] not in existing_accessions:
                    yield project

    @classmethod
    def get_files_for_project(
        cls,
        project_accession: str,
    ) -> Files:
        url = f"{cls.BASE_URL}/projects/{project_accession}/files/all"
        with requests.get(url) as response:
            if response.status_code == 200:
                files_info = response.json()
                search_files: list[FileMetadata] = []
                result_files: list[FileMetadata] = []
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

                    # Convert FTP URL to HTTP URL
                    http_link = cls._convert_ftp_to_http(ftp_link)

                    file = FileMetadata(
                        {
                            "filepath": http_link,
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
    async def _parse_and_add_cv_params(
        cls, project_accession: str, session: Session, project_data: dict
    ) -> None:
        ontology_helper = OntologyHelper()

        cv_data_keys = [
            "instruments",
            "softwares",
            "experimentTypes",
            "quantificationMethods",
            "organisms",
            "organismParts",
            "diseases",
            "identifiedPTMStrings",
        ]

        async with CVInjector(project_accession, session) as injector:
            for json_key in cv_data_keys:
                for cv_data in project_data.get(json_key, []):
                    if cv_data.get("@type") == "Tuple":
                        cv_tuple = parse_cv_tuple(cv_data)
                        if cv_tuple is None:
                            continue
                        await injector.add(cv_tuple)

                    elif cv_data.get("@type") == "CvParam":
                        cv_param = parse_cv_param(cv_data)
                        if cv_param is None:
                            continue
                        await injector.add(cv_param)

                        superclass_cv_accessions: list[str] = []
                        try:
                            superclasses = await ontology_helper.get_superclasses(
                                cv_param.accession
                            )
                            superclass_cv_accessions = [x.id for x in superclasses[1:]]
                        except Exception:
                            logger.error(
                                f"Failed to resolve super classes for accession "
                                f"{cv_param.accession}:",
                                exc_info=True,
                            )

                        for x in superclass_cv_accessions:
                            await injector.add(CVParam(accession=x))

    @classmethod
    async def dump_project_to_db(cls, session: Session, project_data: dict[str, Any]) -> None:
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

        # 8. CV Params
        # Skip ontologies if NO_ONTOLOGY is set or if we're in main build phase of multiprocessing
        # (ontologies will be resolved in separate pass)
        if not os.getenv("NO_ONTOLOGY") and not os.getenv("IS_IN_MULTIPROCESSING_MODE"):
            await cls._parse_and_add_cv_params(project.accession, session, project_data)
