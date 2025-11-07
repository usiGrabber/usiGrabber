from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any, TypedDict


class FileMetadata(TypedDict):
    filepath: str
    file_size: int
    category: str


class Files(TypedDict):
    search: list[FileMetadata]
    result: list[FileMetadata]


class BaseBackend(ABC):
    @classmethod
    @abstractmethod
    def get_all_project_accessions(cls) -> list[str]:
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
    def iterate_new_projects(
        cls,
        existing_accessions: set[str],
        is_test: bool = False,
    ) -> Iterable[dict[str, Any]]:
        """
        Iterate over all projects that are not present in `existing_accessions`.

        :param existing_accessions: A set of existing project accessions to skip.
        :param is_test: Whether to operate in test mode.
        :yield: A dictionary containing project metadata for each new project.
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
