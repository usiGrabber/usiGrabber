from collections.abc import Generator
from dataclasses import dataclass


@dataclass
class Project:
    project_accession: str
    files: list[str]


def dummy_generator(n: int = 100) -> Generator[Project]:
    """Example generator of work items."""
    for i in range(n):
        yield Project(
            project_accession=f"PXD{i:05d}",
            files=[f"file_{j}.txt" for j in range(5)],
        )
