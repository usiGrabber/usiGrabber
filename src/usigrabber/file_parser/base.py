# usigrabber/file_parser/base.py

from abc import ABC, abstractmethod
from pathlib import Path

from sqlalchemy.engine import Engine

from usigrabber.file_parser.models import ImportStats

PARSER_REGISTRY = {}


def register_parser(cls):
    """Decorator to auto-register parsers by extensions."""
    instance = cls()
    for ext in cls.file_extensions:
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
    def parse_file(self, path: Path, project_accession: str):
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

    def import_file(self, engine: Engine, path: Path, project_accession: str) -> ImportStats:
        stats = ImportStats(file_name=path.name, project_accession=project_accession)
        try:
            parsed = self.parse_file(path, project_accession)
            stats.mark_parsing_complete()
            self.persist(engine, parsed, stats)
            stats.mark_complete()
            return stats
        except Exception as e:
            stats.mark_failed(str(e))
            raise
