# usigrabber/file_parser/base.py

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from sqlalchemy.engine import Engine

from usigrabber.file_parser.models import ImportStats, now

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
    def parse_file(self, path: Path, project_accession: str) -> Any:
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

            persist_duration = (stats.end_time or now()) - (stats.parsing_complete_time or now())
            logger.debug(
                "Persisted data from file '%s' for project in %s",
                path.name,
                str(persist_duration).split(".")[0],
            )
            return stats
        except Exception as e:
            stats.mark_failed(str(e))
            raise
