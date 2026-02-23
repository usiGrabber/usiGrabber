from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from typing import Any, TypedDict

from sqlmodel import Session


class FileMetadata(TypedDict):
    filepath: str
    file_size: int
    category: str
    checksum: str


class Files(TypedDict):
    search: list[FileMetadata]
    result: list[FileMetadata]
    other: list[FileMetadata]
    raw: list[FileMetadata]


class BaseBackend(ABC):
    @classmethod
    @abstractmethod
    async def get_project(cls, project_accession: str) -> dict[str, Any]:
        """
        Raises exception if project is not found
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def get_project_accession(cls, project: dict[str, Any]) -> str:
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def is_project_complete(cls, project: dict[str, Any]) -> bool:
        """
        This checks if a project provides (roughly) the same data as a Pride "complete" project
        """

        raise NotImplementedError()

    @classmethod
    @abstractmethod
    async def get_projects(cls) -> AsyncGenerator[dict[str, Any], None]:
        """
        Iterate over all projects.

        :yield: A dictionary containing project metadata for each new project.
        """
        # This is an abstract method and should not be called.
        raise NotImplementedError()

        yield
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
    async def dump_project_to_db(cls, session: Session, project_data: dict[str, Any]) -> None:
        """
        Dump project data into the database.
        """
        ...
