# usigrabber/file_parser/base.py

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from sqlalchemy.engine import Engine

from usigrabber.file_parser.models import (  # Import models
    ImportStats,
)

PARSER_REGISTRY = {}


def register_parser(cls):
    """Decorator to auto-register parsers by extensions."""
    instance = cls()
    for ext in instance.file_extensions:
        PARSER_REGISTRY[ext.lower()] = instance
    return cls


def get_parser_for_extension(ext: str):
    return PARSER_REGISTRY.get(ext.lower())


class BaseFileParser(ABC):
    @property
    @abstractmethod
    def file_extensions(self) -> set[str]:
        """
        Returns the set of file extensions that this parser can handle.
        """
        pass

    @property
    @abstractmethod
    def format_name(self) -> str:
        """
        Returns the name of the file format this parser handles.
        """
        pass

    @abstractmethod
    def parse_file(self, path: Path | list[Path], project_accession: str) -> list[Any]:
        """
        Parse the given file and return the parsed data structure.
        """
        pass

    @abstractmethod
    def persist(self, engine: Engine, parsed, stats: ImportStats):
        """
        Persist the parsed data into the database using the provided engine.
        """
        pass

    def import_file(
        self, engine: Engine, path: Path | list[Path], project_accession: str
    ) -> ImportStats:
        path_name = path[0].name if isinstance(path, list) else path.name
        stats = ImportStats(file_name=path_name, project_accession=project_accession)
        try:
            parsed_data_list = self.parse_file(path, project_accession)
            stats.mark_parsing_complete()
            for parsed in parsed_data_list:
                self.persist(engine, parsed, stats)
            stats.mark_complete()
            return stats
        except Exception as e:
            stats.mark_failed(str(e))
            raise
