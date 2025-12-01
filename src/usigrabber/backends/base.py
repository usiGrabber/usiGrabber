from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from typing import Any, TypedDict

from sqlalchemy import Engine


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
    async def get_new_projects(
        cls,
        existing_accessions: set[str],
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Iterate over all projects that are not present in `existing_accessions`.
        :param existing_accessions: A set of existing project accessions to skip.
        :param is_test: Whether to operate in test mode.
        :yield: A dictionary containing project metadata for each new project.
        """
        # This is an abstract method and should not be called.
        raise NotImplementedError

        yield
        ...

    @classmethod
    @abstractmethod
    async def get_projects(
        cls, offset: int, limit: None | int
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Iterates over a project of all projects

        The limit is the number of projects to get (not offset + limit!)

        If the limit is None fetch all projects starting from the offset
        """

    @classmethod
    @abstractmethod
    async def get_project_by_accession(cls, accession: str) -> dict[str, Any]:
        """
        Retrieve a single project by its accession.

        :param accession: The project accession to retrieve.
        :return: A dictionary containing project metadata.
        """
        ...

    @classmethod
    @abstractmethod
    async def get_files_for_project(cls, project_accession: str) -> Files:
        """
        Retrieve file information for a specific project.

        :return: A list of dictionaries, each containing file information.
        """
        ...

    @classmethod
    @abstractmethod
    async def dump_project_to_db(cls, engine: Engine, project_data: dict[str, Any]) -> None:
        """
        Dump project data into the database.
        """
        ...
