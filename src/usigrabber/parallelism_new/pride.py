from collections.abc import Generator
from dataclasses import dataclass


@dataclass
class Project:
    project_accession: str


def dummy_generator() -> Generator[Project]:
    """Example generator of work items."""
    for i in range(1000):
        yield Project(project_accession=f"PXD{i:05d}")
