import re
from abc import ABC, abstractmethod
from collections.abc import Generator
from pathlib import Path

from pyteomics import usi


class USIGenerator(ABC):
    @classmethod
    @abstractmethod
    def generate_usis(
        cls, project_accession: str, file_path: Path
    ) -> Generator[str, None, None]:
        """
        Generate unique spectrum identifiers (USIs) from a file path.

        :param project_accession: The accession of the project.
        :param file_path: The path to the file to parse.
        :return: A generator yielding USI strings.
        """
        ...

    @classmethod
    def validate(cls, usi: str) -> bool:
        """
        Validate the format of a USI string.

        :param usi: The USI string to validate.
        :return: True if valid, False otherwise.
        """
        pattern = r"^mzspec:[^:]+:[^:]+:[^:]+:[^:]+:[^:]+$"
        return bool(re.match(pattern, usi))

    @classmethod
    def look_up(cls, usi_str: str):
        # TODO: move this to the correct location (maybe in pride.py or its super class?)
        parsed_usi = usi.USI.parse(usi_str)
        return usi.PRIDEBackend().get(parsed_usi)
