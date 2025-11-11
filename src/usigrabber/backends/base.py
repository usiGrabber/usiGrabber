from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from typing import Any, TypedDict

from sqlmodel import Session


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
    def get_files_for_project(cls, project_accession: str) -> Files:
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
