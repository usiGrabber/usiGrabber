# usigrabber/file_parser/base.py

import logging
from abc import ABC, abstractmethod
from pathlib import Path

from sqlalchemy.engine import Engine

from usigrabber.backends.base import FileMetadata
from usigrabber.file_parser.models import (  # Import models
    ImportStats,
    ParsedMzidData,
    ParsedMztabData,
    ParsedTxtZipData,
)

PARSER_REGISTRY: dict[str, "BaseFileParser"] = {}

logger = logging.getLogger(__name__)


def register_parser[T: BaseFileParser](cls: type[T]) -> type[T]:
    """Decorator to auto-register parsers by extensions."""
    instance = cls()
    for ext in instance.file_extensions:
        PARSER_REGISTRY[ext.lower()] = instance
    return cls


def get_parser_for_extension(ext: str) -> "BaseFileParser | None":
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
    def parse_file(
        self, path: Path | tuple[Path, Path, Path], project_accession: str
    ) -> ParsedTxtZipData | ParsedMzidData | ParsedMztabData:
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
        self,
        engine: Engine,
        path: Path | tuple[Path, Path, Path],
        project_accession: str,
        raw_files: list[FileMetadata],
    ) -> ImportStats:
        path_name = path[0].name if isinstance(path, tuple) else path.name
        stats = ImportStats(file_name=path_name, project_accession=project_accession)
        try:
            parsed_data = self.parse_file(path, project_accession)
            stats.mark_parsing_complete()
            self.persist(engine, parsed_data, stats)
            stats.mark_complete()
            return stats
        except Exception as e:
            stats.mark_failed(str(e))
            raise
