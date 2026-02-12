import asyncio
import io
import os
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError

import ijson
import pandas as pd
import requests
from async_http_client import AsyncHttpClient
from ontology_resolver.ontology_helper import OntologyHelper
from pyteomics.usi import PRIDEBackend
from sqlalchemy.engine import Engine
from sqlmodel import Session
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from usigrabber.backends.base import BaseBackend, FileMetadata, Files
from usigrabber.cv_parameters.cv_engine import CVInjector, CVParam, CVTuple
from usigrabber.cv_parameters.instrument_cleaner import clean_instruments
from usigrabber.db import Project, ProjectCountry, ProjectKeyword, ProjectTag, Reference
from usigrabber.db.schema import ProjectAffiliation, ProjectOtherOmicsLink
from usigrabber.utils import get_cache_dir, logger, parse_date
from usigrabber.utils.job_id import get_job_id


def parse_cv_param(cv_data: dict, ontology_helper: OntologyHelper) -> CVParam | None:
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

    cv_accession = ontology_helper.sanitize_accession(cv_accession)

    cv_value = cv_data.get("value")
    return CVParam(accession=cv_accession, value=cv_value)


def parse_cv_tuple(cv_data: dict, ontology_helper: OntologyHelper) -> CVTuple | None:
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

    if "accession" not in cv_data["key"] or not isinstance(cv_data["key"]["accession"], str):
        logger.warning(f"Pride CV Tuple (key.accession) is missing or malformed: {cv_data}")
        return None

    if "accession" not in cv_data["value"] or not isinstance(cv_data["value"]["accession"], str):
        logger.warning(f"Pride CV Tuple (value.accession) is missing or malformed: {cv_data}")
        return None

    key = CVParam(
        accession=ontology_helper.sanitize_accession(cv_data["key"]["accession"]),
        value=cv_data["key"].get("value"),
    )
    value = CVParam(
        accession=ontology_helper.sanitize_accession(cv_data["value"]["accession"]),
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
        projects = cls.get_projects()
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
    async def get_projects(cls) -> AsyncGenerator[dict[str, Any], None]:
        projects_file = os.getenv("PROJECTS_FILE")
        if projects_file:
            file_path = Path(projects_file)
            if not file_path.exists():
                raise FileNotFoundError(f"Projects file not found: {file_path}")
        else:
            file_path = get_cache_dir() / "pride" / "all_projects.json"
        # if file doesnt exist, download it
        if not file_path.exists():
            # Ensure parent directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)

            url = f"{cls.BASE_URL}/projects/all"

            async with AsyncHttpClient() as client:
                await client.stream_file(
                    url,
                    download_file_name=file_path,
                )

        with open(file_path, encoding="utf-8") as in_f:
            for project in ijson.items(in_f, "item"):
                yield project

    @classmethod
    async def get_files_for_project(
        cls,
        project_accession: str,
    ) -> Files:
        async with AsyncHttpClient() as client:
            files_response, checksums_response = await asyncio.gather(
                client.get_response(f"{cls.BASE_URL}/projects/{project_accession}/files/all"),
                client.get_response(f"{cls.BASE_URL}/files/checksum/{project_accession}"),
            )
            if files_response.ok and checksums_response.ok:
                files_info: dict = await files_response.json()
                checksums_raw = await checksums_response.text()
                checksums = pd.read_csv(
                    io.StringIO(checksums_raw),
                    sep="\t",
                )
                search_files: list[FileMetadata] = []
                result_files: list[FileMetadata] = []
                other_files: list[FileMetadata] = []
                raw_files: list[FileMetadata] = []
                for file_info in files_info:
                    filename: str = file_info["fileName"]

                    # strip possible extensions
                    fn_stripped: str = filename
                    for ext in [".gz", ".rar", ".zip"]:
                        fn_stripped = fn_stripped.removesuffix(ext)

                    # only parse files we can actually handle
                    # other files might not have checksums
                    suffix = Path(fn_stripped).suffix.lower()
                    if suffix not in [".raw", ".mzid"]:
                        # filename doesnt include a valid extension, skip it
                        continue

                    category = file_info["fileCategory"]["value"]
                    ftp_url: str | None = None
                    for loc in file_info.get("publicFileLocations", []):
                        if loc.get("name") == "FTP Protocol":
                            ftp_url = loc.get("value")
                            break
                    if not ftp_url:
                        logger.warning(
                            "No FTP link found for file %s in project %s. Skipping.",
                            filename,
                            project_accession,
                        )
                        continue

                    try:
                        checksum = checksums.loc[
                            checksums["File-Name"] == filename, "File-MD5Checksum"
                        ].values[0]
                    except IndexError:
                        logger.warning(f"No checksum found for file '{filename}'")
                        continue

                    file = FileMetadata(
                        {
                            "filepath": ftp_url,
                            "category": category,
                            "file_size": file_info.get("fileSizeBytes"),
                            "checksum": checksum,
                        }
                    )

                    if category == "SEARCH":
                        search_files.append(file)
                    elif category == "RESULT":
                        result_files.append(file)
                    elif category == "OTHER":
                        other_files.append(file)
                    elif category == "RAW":
                        raw_files.append(file)

                return Files(
                    search=search_files, result=result_files, other=other_files, raw=raw_files
                )
            else:
                logger.error(
                    "Could not retrieve files for accession %s: Files: (%s, %s), Checksums: (%s, %s)",
                    project_accession,
                    files_response.status,
                    files_response.reason,
                    checksums_response.status,
                    checksums_response.reason,
                )
                return Files(search=[], result=[], other=[], raw=[])

    @classmethod
    async def _parse_and_add_cv_params(
        cls, project_accession: str, engine: Engine, backend: type[BaseBackend]
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

        project_data = await backend.get_project(project_accession)

        # Clean instrument data to handle MS:1000031 cases
        if "instruments" in project_data:
            project_data["instruments"] = await clean_instruments(
                project_data.get("instruments", [])
            )

        async with CVInjector(project_accession, engine) as injector:
            for json_key in cv_data_keys:
                for cv_data in project_data.get(json_key, []):
                    if cv_data.get("@type") == "Tuple":
                        cv_tuple = parse_cv_tuple(cv_data, ontology_helper)
                        if cv_tuple is None:
                            continue
                        await injector.add(cv_tuple)

                    elif cv_data.get("@type") == "CvParam":
                        cv_param = parse_cv_param(cv_data, ontology_helper)
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
            job_id=get_job_id(),
            worker_pid=os.getpid(),
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
            logger.warning("Getting ontos in single processing mode is currently not supported.")

    @classmethod
    def validate_usi(cls, usi: str) -> bool:
        """
        Validate a USI by checking if the spectrum exists in PRIDE archive.

        Uses pyteomics.usi.PRIDEBackend to retrieve spectrum data. Implements
        retry logic with exponential backoff for rate limiting and network errors.

        Args:
            usi: Universal Spectrum Identifier string

        Returns:
            True if spectrum exists and can be retrieved, False otherwise
        """

        @retry(
            retry=retry_if_exception_type((HTTPError, URLError)),
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=1, max=10),
            reraise=False,
        )
        def _validate_with_retry(usi_str: str) -> bool:
            """Inner function with retry logic."""
            try:
                # Attempt to retrieve the spectrum
                backend = PRIDEBackend()
                spectrum = backend.get(usi_str)

                # If we got here, the spectrum was found
                return spectrum is not None

            except HTTPError as e:
                # HTTP 404 or 500 means spectrum not found - don't retry
                if e.code in (404, 500):
                    logger.debug(f"Spectrum not found for USI {usi_str}: HTTP {e.code}")
                    return False
                # HTTP 429 or other errors - let retry logic handle it
                logger.warning(f"HTTP error {e.code} for USI {usi_str}, retrying...")
                raise

            except URLError as e:
                # Network errors - retry
                logger.warning(f"Network error for USI {usi_str}: {e}, retrying...")
                raise

            except Exception as e:
                # Other errors - don't retry, just return False
                logger.error(f"Unexpected error validating USI {usi_str}: {e}", exc_info=True)
                return False

        try:
            return _validate_with_retry(usi)
        except Exception:
            # If retries exhausted or other error, return False
            logger.error(f"Failed to validate USI {usi} after retries")
            return False
