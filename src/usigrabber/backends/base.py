from abc import ABC, abstractmethod
from collections.abc import Iterable
from enum import Enum
from typing import Any, TypedDict


class FileMetadata(TypedDict):
    filepath: str
    file_size: int
    category: str


class Files(TypedDict):
    search: list[FileMetadata]
    result: list[FileMetadata]


class ScanIdentifierType(str, Enum):
    SCAN = "scan"
    INDEX = "index"
    NATIVEID = "nativeId"
    TRACE = "trace"


class Ref(TypedDict):
    start: int
    end: int
    pre: str
    post: str
    is_decoy: bool
    protein: str


class Modification(TypedDict):
    position: int
    name: str


class PSM(TypedDict):
    datafile: str
    scan_identifier_type: ScanIdentifierType
    scan_identifier: str
    peptide_sequence: str
    charge: int
    experimental_mass_to_charge: float
    retention_time: float | None
    refs: list[Ref]
    modifications: list[Modification]


class BaseBackend(ABC):
    @classmethod
    @abstractmethod
    def get_all_project_accessions(cls, is_test: bool) -> list[str]:
        """
        Retrieve all project accessions from the backend.

        :return: A list of project accession strings.
        """
        ...

    @classmethod
    @abstractmethod
    def get_metadata_for_project(cls, project_accession: str) -> dict[str, Any]:
        """
        Retrieve metadata for a specific project.

        :return: A dictionary containing project metadata.
        """
        ...

    @classmethod
    @abstractmethod
    def get_files_for_project(cls, project_accession: str) -> Files:
        """
        Retrieve file information for a specific project.

        :return: A list of dictionaries, each containing file information.
        """
        ...

    @classmethod
    @abstractmethod
    def process_result_file(cls, file: FileMetadata) -> Iterable[PSM]:
        """
        Process a result file.

        :param file: A dictionary containing file information.
        """
        ...

    @classmethod
    @abstractmethod
    def process_search_file(cls, file: FileMetadata) -> None:
        """
        Process a search file.

        :param file: A dictionary containing file information.
        """
        ...
