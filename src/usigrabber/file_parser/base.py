# usigrabber/file_parser/base.py

import datetime
import logging
import os
import traceback
from abc import ABC, abstractmethod
from pathlib import Path

from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session

from usigrabber.backends.base import FileMetadata
from usigrabber.db.schema import ImportedFile
from usigrabber.file_parser.errors import MsRunNameValidationError
from usigrabber.file_parser.models import (
    ImportStats,
    ParsedMzidData,
    ParsedMztabData,
    ParsedTxtZipData,
)
from usigrabber.utils.checksum import md5_checksum
from usigrabber.utils.job_id import get_job_id

PARSER_REGISTRY: dict[str, "BaseFileParser"] = {}

logger = logging.getLogger(__name__)


def register_parser[T: BaseFileParser](cls: type[T]) -> type[T]:
    """Decorator to auto-register parsers by extensions."""
    instance = cls()
    logger.info("Registering file parser for extensions: %s", instance.file_extensions)
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

    def validate_ms_run_names(
        self,
        parsed_data: ParsedTxtZipData | ParsedMzidData | ParsedMztabData,
        raw_files: list[FileMetadata],
    ) -> bool:
        """
        Validate that all ms_run names in the parsed data correspond to actual raw files.
        Checks if a raw file with extension .raw (not case sensitive) and the ms_run name exists.
        """
        if not parsed_data.psms:
            return True

        # Create a set of raw file names (without extension) in lowercase
        # Skip all raw files that do not end with .raw or .RAW
        raw_file_names_lower = {
            Path(rf["filepath"]).stem.lower()
            for rf in raw_files
            if rf.get("filepath") and rf["filepath"].lower().endswith(".raw")
        }

        if not raw_file_names_lower:
            logger.warning("No raw files available for ms_run validation")
            return False

        # Check all PSMs for ms_run validation
        for psm in parsed_data.psms:
            ms_runs = psm.get("ms_run", "") or ""
            for ms_run in ms_runs.split("|"):
                if ms_run.lower() in raw_file_names_lower:
                    psm["ms_run"] = ms_run  # set to the valid ms_run
            else:
                logger.warning(
                    f"PSM has ms_run '{ms_run}' that doesn't match any available raw files"
                )
                return False

        return True

    def import_file(
        self,
        engine: Engine,
        path: Path | tuple[Path, Path, Path],
        project_accession: str,
        raw_files: list[FileMetadata],
    ) -> ImportStats:
        path_name = path[0].name if isinstance(path, tuple) else path.name
        stats = ImportStats(file_name=path_name, project_accession=project_accession)
        file_id = self.get_file_id(path)
        imported_file_id = None

        with Session(engine) as session:
            file_info = ImportedFile(
                project_accession=project_accession,
                file_id=file_id,
                checksum=md5_checksum(path),
                format=self.format_name,
                worker_pid=os.getpid(),
                job_id=get_job_id(),
            )
            session.add(file_info)
            session.commit()
            imported_file_id = file_info.id
            assert imported_file_id, "Imported file must has been given an id by the db!"

        is_processed_successfully = False
        error_message = None
        traceback_str = None
        psm_count = None

        try:
            parsed_data = self.parse_file(path, project_accession)
            stats.mark_parsing_complete()

            ms_run_valid = self.validate_ms_run_names(parsed_data, raw_files)
            if not ms_run_valid:
                raise MsRunNameValidationError("MS run name validation failed.")

            self.persist(engine, parsed_data, stats)
            stats.mark_complete()
            is_processed_successfully = True
            psm_count = stats.psm_count
            return stats
        except Exception as e:
            error_message = str(e)
            traceback_str = traceback.format_exc()
            stats.mark_failed(str(e))
            raise
        finally:
            with Session(engine) as session:
                file_info = session.get(ImportedFile, imported_file_id)
                assert file_info, "This id must exist!"

                file_info.psm_count = psm_count
                file_info.is_processed_successfully = is_processed_successfully
                file_info.error_message = error_message
                file_info.traceback = traceback_str
                file_info.end_time = datetime.datetime.now()
                session.commit()
