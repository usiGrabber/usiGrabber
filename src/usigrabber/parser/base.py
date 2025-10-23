from abc import ABC, abstractmethod
from collections.abc import Generator
from pathlib import Path


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
