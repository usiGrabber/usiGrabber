from abc import ABC, abstractmethod
from typing import Any


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
    def get_files_for_project(cls, project_accession: str) -> list[dict[str, Any]]:
        """
        Retrieve file information for a specific project.

        :return: A list of dictionaries, each containing file information.
        """
        ...
