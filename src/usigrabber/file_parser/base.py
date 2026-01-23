# usigrabber/file_parser/base.py

import datetime
import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path

from sqlalchemy.engine import Engine

# from usigrabber.db.schema import ImportedFile
from sqlalchemy.orm.session import Session

from usigrabber.db.schema import ImportedFile
from usigrabber.file_parser.models import (  # Import models
    ImportStats,
    ParsedMzidData,
    ParsedMztabData,
    ParsedTxtZipData,
)
from usigrabber.utils.checksum import md5_checksum

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
    def get_file_id(self, path: Path | tuple[Path, Path, Path]) -> str:
        """
        Returns a single str id that uniquely identifies this file in the context of the project
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
    ) -> ImportStats:
        path_name = path[0].name if isinstance(path, tuple) else path.name
        stats = ImportStats(file_name=path_name, project_accession=project_accession)
        with Session(engine) as session:
            file_info = ImportedFile(
                project_accession=project_accession,
                file_id=self.get_file_id(path),
                checksum=md5_checksum(path),
                format=self.format_name,
                worker_pid=os.getpid(),
            )
            session.add(file_info)
            session.commit()

        is_processed_successfully = False
        error_message = None
        psm_count = None

        try:
            parsed_data = self.parse_file(path, project_accession)
            stats.mark_parsing_complete()
            self.persist(engine, parsed_data, stats)
            stats.mark_complete()
            is_processed_successfully = True
            psm_count = stats.psm_count
            return stats
        except Exception as e:
            error_message = str(e)
            stats.mark_failed(str(e))
            raise
        finally:
            with Session(engine) as session:
                file_info = session.get(ImportedFile, file_info.id)
                assert file_info, ""

                file_info.psm_count = psm_count
                file_info.is_processed_successfully = is_processed_successfully
                file_info.error_message = error_message
                file_info.end_time = datetime.datetime.now()
                session.commit()
